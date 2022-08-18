use std::{
    collections::HashMap,
    env,
    fs::{self, DirBuilder, File},
    io::{Error, Write},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
};

use clap::Parser;

use futures::executor;
use prompts::{
    confirm::ConfirmPrompt,
    text::{Style, TextPrompt},
    Prompt,
};
use regex::Regex;
use sequoia_openpgp::{cert::CertBuilder, serialize::Marshal};
use serde_derive::{Deserialize, Serialize};

use path_absolutize::*;

const CONFIG_VER: u8 = 1;

const GPG_KEY_ID_REGEX: &str = r"(?im)^\s*([0-9A-F]+)$";

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[clap(long)]
    setup: bool,

    #[clap(long, short)]
    backup: bool,

    #[clap(long, short)]
    start: bool,

    #[clap(long, short = 'p')]
    add_path: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
struct Config {
    config_ver: u8,
    key_id: String,
    paths: Vec<String>,
}

impl Config {
    fn save(&self, path: &PathBuf) -> Result<(), String> {
        let config_out = toml::to_string_pretty(self).expect_res("Failed to serialize config")?;
        let mut config_file = File::create(path).expect_res("Failed to get config file")?;
        write!(&mut config_file, "{}", config_out).expect_res("Failed to write config")?;
        Ok(())
    }
}

enum AddPathStatus {
    Skip,
    Save,
    Error,
}

impl AddPathStatus {
    fn is_error(&self) -> bool {
        matches!(self, AddPathStatus::Error)
    }
}

enum PathExistStatus<'a> {
    Exact,
    NotExist,
    SymLinkExist(&'a PathBuf),
    CanonicalExist(PathBuf),
}

trait QuietStatus {
    fn quiet_status(&mut self) -> Result<ExitStatus, Error>;
}

impl QuietStatus for Command {
    fn quiet_status(&mut self) -> Result<ExitStatus, Error> {
        self.stdout(Stdio::null()).stderr(Stdio::null()).status()
    }
}

trait ResultStringErr<T> {
    fn expect_res(self, msg: &str) -> Result<T, String>;
}

impl<T, E> ResultStringErr<T> for Result<T, E> {
    fn expect_res(self, msg: &str) -> Result<T, String> {
        self.map_err(|_| msg.to_string())
    }
}

impl<T> ResultStringErr<T> for Option<T> {
    fn expect_res(self, msg: &str) -> Result<T, String> {
        match self {
            Some(v) => Ok(v),
            _ => Err(msg.to_string()),
        }
    }
}

trait SyncRunPrompt<T> {
    fn run_sync(&mut self) -> Result<T, String>;
}

impl SyncRunPrompt<String> for TextPrompt {
    fn run_sync(&mut self) -> Result<String, String> {
        match executor::block_on(self.run()) {
            Ok(Some(p)) => Ok(p),
            Err(_) => input_error(),
            _ => aborted(),
        }
    }
}

impl SyncRunPrompt<()> for ConfirmPrompt {
    fn run_sync(&mut self) -> Result<(), String> {
        match executor::block_on(self.run()) {
            Ok(Some(true)) => Ok(()),
            Err(_) => input_error(),
            _ => aborted(),
        }
    }
}

trait PathBufOf {
    fn of(path: &str) -> Self;
}

impl PathBufOf for PathBuf {
    fn of(path: &str) -> Self {
        Path::new(path).to_path_buf()
    }
}

fn aborted<T>() -> Result<T, String> {
    err("Aborted")
}

fn input_error<T>() -> Result<T, String> {
    err("Error reading input")
}

fn err<T>(err: &str) -> Result<T, String> {
    Err(err.to_string())
}

fn get_cwd() -> Result<PathBuf, String> {
    let cur_dir = env::current_dir().expect_res("Could not get current directory")?;
    let mut path: PathBuf;
    if let Ok(pwd) = env::var("PWD") {
        path = PathBuf::of(&pwd);
        let canon_path = path
            .canonicalize()
            .expect_res("Could not get canonical PWD")?;
        if canon_path != cur_dir {
            path = cur_dir;
        }
    } else {
        path = cur_dir;
    }
    Ok(path)
}

fn get_canon_paths(paths: &[String]) -> HashMap<PathBuf, PathBuf> {
    let mut canon_paths: HashMap<PathBuf, PathBuf> = HashMap::new();
    for p in paths {
        if let Ok(canon) = fs::canonicalize(p) {
            let p_path = Path::new(p);
            if canon != p_path {
                canon_paths.insert(canon, p_path.to_path_buf());
            }
        }
    }
    canon_paths
}

fn get_canon_path(path: &Path) -> Option<PathBuf> {
    if let Ok(canon) = path.canonicalize() {
        return Some(canon);
    }
    if let Some(parent) = path.parent() {
        if let Some(mut parent_canon) = get_canon_path(parent) {
            if let Some(file_name) = parent.file_name() {
                parent_canon.push(file_name);
                return Some(parent_canon);
            }
        }
    }
    None
}

fn get_path_exists<'a>(
    paths: &[String],
    canon_paths: &'a HashMap<PathBuf, PathBuf>,
    path: &PathBuf,
) -> PathExistStatus<'a> {
    if let Some(symlink) = canon_paths.get(path) {
        return PathExistStatus::SymLinkExist(symlink);
    }
    if paths.contains(&path.to_string_lossy().to_string()) {
        return PathExistStatus::Exact;
    }
    if let Some(canon) = get_canon_path(path) {
        if &canon != path && paths.contains(&canon.to_string_lossy().to_string()) {
            return PathExistStatus::CanonicalExist(canon);
        }
    }
    PathExistStatus::NotExist
}

fn main() -> Result<(), String> {
    let args = Args::parse();

    let mut config_dir = dirs::config_dir().expect_res("No config directory")?;
    config_dir.push("disc-up");
    let config_path = config_dir.join("config.toml");
    let key_path = config_dir.join("backupkey.gpg");

    let mut config: Option<Config> = None;
    if config_path.exists() {
        let config_file =
            fs::read_to_string(&config_path).expect_res("Failed to read config file")?;
        if let Ok(config_file) = toml::from_str(&config_file) {
            config = Some(config_file);
        } else {
            println!("Warning: Failed to parse config, resetting config.");
        }
    }

    if args.setup {
        if config.is_none() {
            if !config_dir.exists() {
                DirBuilder::new()
                    .recursive(true)
                    .create(config_dir)
                    .expect_res("Failed to create config directory")?;
            }
        } else {
            println!("Config already exists.");
            let mut confirm = ConfirmPrompt::new(
                "Overwrite config? - This will replace your existing key backup!",
            )
            .set_initial(false);

            confirm.run_sync()?;
        }

        let mut key_name_prompt = TextPrompt::new("Enter a User ID (name) for your backup key:")
            .with_validator(|v| {
                if v.is_empty() {
                    Err("Enter a non-empty User ID".to_string())
                } else {
                    Ok(())
                }
            });

        let key_name = key_name_prompt.run_sync()?;

        let passwd = loop {
            let mut passwd_prompt = TextPrompt::new("Enter password for backup key:")
                .with_style(Style::Password)
                .with_validator(|v| {
                    if v.is_empty() {
                        err("A password is required")
                    } else {
                        Ok(())
                    }
                });

            let passwd = passwd_prompt.run_sync()?;

            let mut passwd_confirm_prompt =
                TextPrompt::new("Confirm password:").with_style(Style::Password);
            let passwd_confirm = passwd_confirm_prompt.run_sync()?;

            if passwd == passwd_confirm {
                break passwd;
            }

            println!("Passwords do not match");
        };

        let (backup_key, _) = CertBuilder::new()
            .add_userid(key_name)
            .add_storage_encryption_subkey()
            .set_password(Some(passwd.clone().into()))
            .generate()
            .expect_res("Failed to generate backup key")?;

        let mut key_file = File::create(&key_path).expect_res("Failed to get key file")?;

        backup_key
            .as_tsk()
            .export(&mut key_file)
            .expect_res("Failed to export backup key")?;

        let key_path = key_path.to_string_lossy();
        let mut gpg_child = Command::new("gpg")
            .args([
                "--batch",
                "--pinentry-mode",
                "loopback",
                "--passphrase-fd",
                "0",
                "--import-options",
                "import-show",
                "--import",
                &key_path,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect_res("Failed to spawn gpg child")?;

        let mut gpg_stdin = gpg_child
            .stdin
            .take()
            .expect_res("Failed to get gpg child stdin")?;

        writeln!(&mut gpg_stdin, "{}", passwd)
            .expect_res("Failed to write password to gpg stdin")?;

        let gpg_output = gpg_child
            .wait_with_output()
            .expect_res("Failed to add key to GPG")?;
        let gpg_output = String::from_utf8_lossy(&gpg_output.stdout);
        let key_regex = Regex::new(GPG_KEY_ID_REGEX).unwrap();
        let key_id = key_regex
            .captures(&gpg_output)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_string())
            .expect_res("Failed to get GPG key ID")?;

        config = Some(Config {
            config_ver: CONFIG_VER,
            key_id,
            paths: Vec::new(),
        });

        config.as_ref().unwrap().save(&config_path)?;
    } else if config.is_none() {
        return err("Disc-Up is not setup, please run with --setup");
    } else {
        let status = Command::new("gpg")
            .args(["-k", config.as_ref().unwrap().key_id.as_str()])
            .quiet_status()
            .expect_res("Failed to execute GPG")?;

        if !status.success() {
            return err("Couldn't find backup key in GPG keyring");
        }
    }

    let mut config = config.unwrap();

    if let Some(add_paths) = args.add_path {
        let mut status = AddPathStatus::Skip;
        let mut new_paths: Vec<(Option<String>, String)> = Vec::new();
        let cwd = get_cwd()?;
        let existing_canon_paths = get_canon_paths(&config.paths);
        for p in add_paths {
            let path = Path::new(&p);
            match path.absolutize_from(&cwd) {
                Ok(abs) => {
                    if status.is_error() {
                        break;
                    }

                    let mut already_added: Option<PathBuf> = None;
                    let abs = abs.to_path_buf();
                    match get_path_exists(&config.paths, &existing_canon_paths, &abs) {
                        PathExistStatus::Exact => already_added = Some(abs.clone()),
                        PathExistStatus::CanonicalExist(canon_path) => {
                            already_added = Some(canon_path);
                        }
                        PathExistStatus::SymLinkExist(symlink_path) => {
                            let symlink_path = symlink_path.to_string_lossy().to_string();
                            config.paths.retain(|p| p != &symlink_path);
                            let abs = abs.to_string_lossy().to_string();
                            config.paths.push(abs.clone());
                            new_paths.push((Some(symlink_path), abs));
                            status = AddPathStatus::Save;
                        }
                        PathExistStatus::NotExist => {
                            let abs = abs.to_string_lossy().to_string();
                            config.paths.push(abs.clone());
                            new_paths.push((None, abs));
                            status = AddPathStatus::Save;
                        }
                    }

                    if let Some(already_added) = already_added {
                        println!(
                            "Skipping: Path \"{}\" already added",
                            already_added.display()
                        );
                    }
                }
                _ => {
                    println!("Error: Path \"{}\" could not be resolved", p);
                    status = AddPathStatus::Error;
                }
            };
        }

        match status {
            AddPathStatus::Error => return Ok(()),
            AddPathStatus::Save => {
                config.save(&config_path)?;

                new_paths.iter().for_each(|(replace, with)| {
                    if let Some(replace) = replace {
                        println!("Replaced: {} path, with: {}", replace, with);
                    } else {
                        println!("Added: \"{}\"", with);
                    }
                });
            }
            _ => {}
        }
    }

    Ok(())
}
