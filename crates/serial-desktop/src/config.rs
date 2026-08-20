use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};

pub const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:3210";
const MAX_INPUT_HISTORY: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ThemePreference {
    #[default]
    System,
    Dark,
    Light,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DesktopConfig {
    pub endpoint: String,
    /// Empty means resolve `serial` next to this executable. The token is
    /// deliberately not persisted; loopback personal mode needs none.
    pub local_program: String,
    pub auto_start_local: bool,
    pub theme: ThemePreference,
    pub selected_slot: Option<String>,
    pub drafts: BTreeMap<String, String>,
    pub input_history: BTreeMap<String, Vec<String>>,
}

impl Default for DesktopConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_ENDPOINT.into(),
            local_program: String::new(),
            auto_start_local: true,
            theme: ThemePreference::System,
            selected_slot: None,
            drafts: BTreeMap::new(),
            input_history: BTreeMap::new(),
        }
    }
}

impl DesktopConfig {
    pub fn remember_input(&mut self, slot_id: &str, value: String) {
        let value = value.trim_end_matches(['\r', '\n']).to_string();
        if value.is_empty() {
            return;
        }
        let history = self.input_history.entry(slot_id.to_string()).or_default();
        if history.last() != Some(&value) {
            history.push(value);
        }
        if history.len() > MAX_INPUT_HISTORY {
            history.drain(..history.len() - MAX_INPUT_HISTORY);
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConfigStore {
    path: PathBuf,
}

impl ConfigStore {
    pub fn discover() -> Result<Self> {
        let project = ProjectDirs::from("dev", "serial-platform", "serial-desktop")
            .context("cannot resolve the desktop application configuration directory")?;
        Ok(Self {
            path: project.config_dir().join("desktop.toml"),
        })
    }

    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<DesktopConfig> {
        match fs::read_to_string(&self.path) {
            Ok(contents) => toml::from_str(&contents)
                .with_context(|| format!("parse desktop config {}", self.path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(DesktopConfig::default())
            }
            Err(error) => {
                Err(error).with_context(|| format!("read desktop config {}", self.path.display()))
            }
        }
    }

    pub fn save(&self, config: &DesktopConfig) -> Result<()> {
        let parent = self
            .path
            .parent()
            .context("desktop config path has no parent directory")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("create desktop config directory {}", parent.display()))?;
        let encoded = toml::to_string_pretty(config).context("encode desktop config")?;
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options
            .open(&self.path)
            .with_context(|| format!("open desktop config {}", self.path.display()))?;
        file.write_all(encoded.as_bytes())
            .with_context(|| format!("write desktop config {}", self.path.display()))?;
        file.sync_all()
            .with_context(|| format!("sync desktop config {}", self.path.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_round_trip_preserves_theme_draft_and_history_without_a_token_field() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::at(temporary.path().join("desktop.toml"));
        let mut config = DesktopConfig {
            theme: ThemePreference::Dark,
            selected_slot: Some("dut-1".into()),
            ..DesktopConfig::default()
        };
        config.drafts.insert("dut-1".into(), "version".into());
        config.remember_input("dut-1", "version\r".into());

        store.save(&config).unwrap();
        let loaded = store.load().unwrap();
        let raw = fs::read_to_string(store.path()).unwrap();

        assert_eq!(loaded.theme, ThemePreference::Dark);
        assert_eq!(loaded.drafts["dut-1"], "version");
        assert_eq!(loaded.input_history["dut-1"], ["version"]);
        assert!(!raw.contains("token"));
    }

    #[test]
    fn input_history_is_deduplicated_and_bounded() {
        let mut config = DesktopConfig::default();
        for index in 0..=MAX_INPUT_HISTORY {
            config.remember_input("slot", format!("cmd-{index}"));
        }
        config.remember_input("slot", format!("cmd-{MAX_INPUT_HISTORY}"));

        let history = &config.input_history["slot"];
        assert_eq!(history.len(), MAX_INPUT_HISTORY);
        assert_eq!(history.first().unwrap(), "cmd-1");
        assert_eq!(history.last().unwrap(), &format!("cmd-{MAX_INPUT_HISTORY}"));
    }
}
