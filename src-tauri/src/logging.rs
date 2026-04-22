use anyhow::{Context, Result};
use chrono::Local;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use tauri::{AppHandle, Manager};
use tracing_subscriber::{
    fmt::{self, MakeWriter},
    prelude::*,
    EnvFilter,
};

const DEFAULT_BASE_NAME: &str = "downloader";
const CONFIG_FILE_NAME: &str = "logging.json";

#[derive(Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub max_file_size_bytes: u64,
    pub max_total_size_bytes: u64,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            max_file_size_bytes: 1 * 1024 * 1024,
            max_total_size_bytes: 10 * 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct LogManager {
    inner: Arc<LogInner>,
}

struct LogInner {
    dir: PathBuf,
    base_name: String,
    state: Mutex<FileState>,
    config: RwLock<LoggingConfig>,
    config_path: PathBuf,
}

struct FileState {
    file: Option<File>,
    size: u64,
}

impl LogManager {
    pub fn setup(app: &AppHandle) -> Result<Self> {
        static INIT: OnceLock<()> = OnceLock::new();

        let path_resolver = app.path();
        let log_dir = path_resolver
            .app_log_dir()
            .context("failed to resolve app log dir")?;
        let config_dir = path_resolver
            .app_config_dir()
            .context("failed to resolve app config dir")?;

        fs::create_dir_all(&log_dir)
            .with_context(|| format!("failed to create log directory {}", log_dir.display()))?;
        fs::create_dir_all(&config_dir).with_context(|| {
            format!("failed to create config directory {}", config_dir.display())
        })?;

        let config_path = config_dir.join(CONFIG_FILE_NAME);
        let config = if config_path.exists() {
            let bytes = fs::read(&config_path)
                .with_context(|| format!("failed to read {}", config_path.display()))?;
            serde_json::from_slice(&bytes).unwrap_or_default()
        } else {
            LoggingConfig::default()
        };

        let manager = Self::new(log_dir.clone(), config_path, config)?;

        INIT.get_or_init(|| {
            // setup tracing subscriber once
            let env_filter = EnvFilter::from_default_env().add_directive(
                "downloader=info"
                    .parse()
                    .unwrap_or_else(|_| "downloader=info".parse().unwrap()),
            );

            let console_layer = fmt::layer().with_target(false);
            let file_layer = fmt::layer()
                .with_target(true)
                .with_ansi(false)
                .with_writer(manager.clone());

            let _ = tracing_subscriber::registry()
                .with(env_filter)
                .with(console_layer)
                .with(file_layer)
                .try_init();
        });

        Ok(manager)
    }

    fn new(dir: PathBuf, config_path: PathBuf, config: LoggingConfig) -> Result<Self> {
        let base_name = DEFAULT_BASE_NAME.to_string();
        let log_path = dir.join(format!("{base_name}.log"));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("failed to open log file {}", log_path.display()))?;
        let size = file.metadata().map(|m| m.len()).unwrap_or(0);
        let state = FileState {
            file: Some(file),
            size,
        };

        let inner = LogInner {
            dir,
            base_name,
            state: Mutex::new(state),
            config: RwLock::new(config),
            config_path,
        };

        let manager = Self {
            inner: Arc::new(inner),
        };

        manager
            .inner
            .persist_config()
            .context("failed to persist logging config")?;
        manager
            .inner
            .prune_with_config(manager.inner.config.read().clone())?;

        Ok(manager)
    }

    pub fn log_directory(&self) -> PathBuf {
        self.inner.dir.clone()
    }

    pub fn config(&self) -> LoggingConfig {
        self.inner.config.read().clone()
    }

    pub fn update_config(&self, config: LoggingConfig) -> Result<()> {
        let sanitized = self.inner.sanitize_config(config);
        {
            let mut guard = self.inner.config.write();
            *guard = sanitized.clone();
        }
        self.inner.persist_config()?;
        self.inner.prune_with_config(sanitized)?;
        Ok(())
    }

    pub fn list_logs(&self) -> Result<Vec<LogFileEntry>> {
        self.inner.list_logs()
    }

    pub fn read_log(&self, file_name: &str, limit: Option<u64>) -> Result<String> {
        self.inner.read_log(file_name, limit)
    }
}

impl LogInner {
    fn log_path(&self) -> PathBuf {
        self.dir.join(format!("{}.log", self.base_name))
    }

    fn sanitize_config(&self, mut config: LoggingConfig) -> LoggingConfig {
        const MIN_FILE_SIZE: u64 = 64 * 1024;
        const MIN_TOTAL_SIZE: u64 = 256 * 1024;
        if config.max_file_size_bytes < MIN_FILE_SIZE {
            config.max_file_size_bytes = MIN_FILE_SIZE;
        }
        if config.max_total_size_bytes < config.max_file_size_bytes {
            config.max_total_size_bytes = config.max_file_size_bytes;
        }
        if config.max_total_size_bytes < MIN_TOTAL_SIZE {
            config.max_total_size_bytes = MIN_TOTAL_SIZE;
        }
        config
    }

    fn persist_config(&self) -> Result<()> {
        let config = self.config.read().clone();
        let json = serde_json::to_vec_pretty(&config)?;
        fs::write(&self.config_path, json)
            .with_context(|| format!("failed to write {}", self.config_path.display()))?;
        Ok(())
    }

    fn rotate_locked(&self, state: &mut FileState, config: &LoggingConfig) -> io::Result<()> {
        let current_path = self.log_path();
        if let Some(mut file) = state.file.take() {
            let _ = file.flush();
        }
        if state.size > 0 && current_path.exists() {
            let timestamp = Local::now().format("%Y%m%d-%H%M%S");
            let mut counter = 0;
            let rotated_path = loop {
                let suffix = if counter == 0 {
                    format!("{}-{}", self.base_name, timestamp)
                } else {
                    format!("{}-{}-{}", self.base_name, timestamp, counter)
                };
                let candidate = self.dir.join(format!("{suffix}.log"));
                if !candidate.exists() {
                    break candidate;
                }
                counter += 1;
            };
            fs::rename(&current_path, rotated_path)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&current_path)?;
        state.file = Some(file);
        state.size = 0;

        self.prune_with_config(config.clone())?;
        Ok(())
    }

    fn prune_with_config(&self, config: LoggingConfig) -> io::Result<()> {
        let mut entries: Vec<_> = fs::read_dir(&self.dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if !path.is_file() {
                    return None;
                }
                let name = path.file_name()?.to_string_lossy().to_string();
                if !name.starts_with(self.base_name.as_str()) || !name.ends_with(".log") {
                    return None;
                }
                let metadata = entry.metadata().ok()?;
                let modified = metadata.modified().ok()?;
                Some(LogFileInternal {
                    path,
                    name,
                    size: metadata.len(),
                    modified,
                })
            })
            .collect();

        entries.sort_by_key(|entry| Reverse(entry.modified));

        let current_name = format!("{}.log", self.base_name);
        let max_files = ((config.max_total_size_bytes + config.max_file_size_bytes - 1)
            / config.max_file_size_bytes)
            .max(1) as usize;
        let mut total = 0u64;

        let mut kept = Vec::new();
        for entry in entries.into_iter() {
            if entry.name == current_name {
                total += entry.size;
                continue;
            }
            kept.push(entry);
        }

        total += kept.iter().map(|entry| entry.size).sum::<u64>();

        let max_rotated = max_files.saturating_sub(1);

        let mut idx = kept.len();
        while (total > config.max_total_size_bytes || kept.len() > max_rotated) && idx > 0 {
            idx -= 1;
            let removed = kept.remove(idx);
            total -= removed.size;
            if let Err(err) = fs::remove_file(&removed.path) {
                eprintln!(
                    "failed to remove log file {}: {err}",
                    removed.path.display()
                );
            }
        }

        Ok(())
    }

    fn list_logs(&self) -> Result<Vec<LogFileEntry>> {
        let mut files: Vec<LogFileInternal> = Vec::new();
        let current_path = self.log_path();
        let current_meta = fs::metadata(&current_path).ok();
        let current_size = {
            let state = self.state.lock();
            state.size
        };
        if let Some(meta) = current_meta {
            let modified = meta
                .modified()
                .unwrap_or_else(|_| std::time::SystemTime::UNIX_EPOCH);
            files.push(LogFileInternal {
                path: current_path.clone(),
                name: current_path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string(),
                size: current_size,
                modified,
            });
        }

        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() || path == current_path {
                continue;
            }
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if !name.starts_with(self.base_name.as_str()) || !name.ends_with(".log") {
                continue;
            }
            let metadata = entry.metadata()?;
            let modified = metadata
                .modified()
                .unwrap_or_else(|_| std::time::SystemTime::UNIX_EPOCH);
            files.push(LogFileInternal {
                path,
                name,
                size: metadata.len(),
                modified,
            });
        }

        files.sort_by_key(|entry| Reverse(entry.modified));

        Ok(files
            .into_iter()
            .map(|entry| LogFileEntry {
                name: entry.name,
                size: entry.size,
                modified: Some(
                    chrono::DateTime::<chrono::Local>::from(entry.modified).to_rfc3339(),
                ),
            })
            .collect())
    }

    fn read_log(&self, file_name: &str, limit: Option<u64>) -> Result<String> {
        if file_name.contains('/') || file_name.contains('\\') {
            anyhow::bail!("invalid file name");
        }
        let path = self.dir.join(file_name);
        if !path.exists() {
            anyhow::bail!("log file not found");
        }
        let mut content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if let Some(limit) = limit {
            if limit > 0 && content.len() as u64 > limit {
                let len = content.len();
                let start = len.saturating_sub(limit as usize);
                content = content[start..].to_string();
            }
        }
        Ok(content)
    }
}

struct LogFileInternal {
    path: PathBuf,
    name: String,
    size: u64,
    modified: std::time::SystemTime,
}

#[derive(Clone, Serialize)]
pub struct LogFileEntry {
    pub name: String,
    pub size: u64,
    pub modified: Option<String>,
}

impl<'a> MakeWriter<'a> for LogManager {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriter {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Clone)]
pub struct LogWriter {
    inner: Arc<LogInner>,
}

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut state = self.inner.state.lock();
        if state.file.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(self.inner.log_path())?;
            state.file = Some(file);
            state.size = 0;
        }
        if let Some(file) = state.file.as_mut() {
            file.write_all(buf)?;
        }
        state.size = state.size.saturating_add(buf.len() as u64);
        let config = self.inner.config.read().clone();
        if state.size >= config.max_file_size_bytes {
            self.inner.rotate_locked(&mut state, &config)?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut state = self.inner.state.lock();
        if let Some(file) = state.file.as_mut() {
            file.flush()
        } else {
            Ok(())
        }
    }
}

pub fn list_log_files_for_ui(manager: &LogManager) -> Result<Vec<LogFileInfo>> {
    let entries = manager.list_logs()?;
    Ok(entries
        .into_iter()
        .map(|entry| LogFileInfo {
            name: entry.name,
            size: entry.size,
            modified: entry.modified,
        })
        .collect())
}

#[derive(Serialize)]
pub struct LogFileInfo {
    pub name: String,
    pub size: u64,
    pub modified: Option<String>,
}
