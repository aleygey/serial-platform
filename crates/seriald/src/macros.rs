//! Shared Macro Script catalog. Saving never opens or writes a serial port.
use crate::config::atomic_write;
use serde::{Deserialize, Serialize};
use serial_macro::{Limits, Program, Value as ScriptValue, ValueType};
use serial_protocol::*;
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

const MAX_MACROS: usize = 512;
const MAX_CATALOG_BYTES: usize = 40 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum MacroError {
    #[error("invalid macro: {0}")]
    Invalid(String),
    #[error("{0}")]
    Script(#[from] serial_macro::Error),
    #[error("macro {0} does not exist")]
    NotFound(String),
    #[error("macro {id} revision conflict: expected {expected:?}, current {actual}")]
    Conflict {
        id: String,
        expected: Option<u64>,
        actual: u64,
    },
    #[error("macro storage: {0}")]
    Storage(#[from] std::io::Error),
    #[error("macro catalog codec: {0}")]
    Codec(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct MacroCatalog {
    path: PathBuf,
    state: Arc<Mutex<Option<Catalog>>>,
}
#[derive(Clone, Default, Serialize, Deserialize)]
struct Catalog {
    revision: u64,
    macros: BTreeMap<String, MacroDefinition>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedMacro {
    pub spec: MacroRunSpec,
    pub definition: Option<MacroDefinition>,
    pub program: Program,
    pub arguments: BTreeMap<String, ScriptValue>,
    pub description: String,
}

impl MacroCatalog {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            state: Arc::new(Mutex::new(None)),
        }
    }
    fn load(&self) -> Result<Catalog, MacroError> {
        match fs::symlink_metadata(&self.path) {
            Ok(metadata) => {
                if !metadata.file_type().is_file() || metadata.len() > MAX_CATALOG_BYTES as u64 {
                    return Err(MacroError::Invalid(
                        "catalog is not a bounded regular file".into(),
                    ));
                }
                let state: Catalog = serde_json::from_slice(&fs::read(&self.path)?)?;
                if state.macros.len() > MAX_MACROS {
                    return Err(MacroError::Invalid("catalog capacity exceeded".into()));
                }
                for (id, definition) in &state.macros {
                    if id != &definition.id || definition.language_version != MACRO_LANGUAGE_VERSION
                    {
                        return Err(MacroError::Invalid(
                            "catalog identity/language version mismatch".into(),
                        ));
                    }
                    validate_definition(definition)?;
                }
                Ok(state)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Catalog::default()),
            Err(e) => Err(e.into()),
        }
    }
    fn with_catalog<T>(
        &self,
        f: impl FnOnce(&mut Catalog) -> Result<T, MacroError>,
    ) -> Result<T, MacroError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.is_none() {
            *state = Some(self.load()?);
        }
        f(state.as_mut().expect("initialized catalog"))
    }
    pub fn list(&self, query: MacroListQuery) -> Result<MacroListResponse, MacroError> {
        if query.id.is_some() && (query.query.is_some() || query.offset.is_some()) {
            return Err(MacroError::Invalid(
                "id lookup cannot be combined with search/pagination".into(),
            ));
        }
        self.with_catalog(|state| {
            if let Some(id) = query.id {
                let definition = state
                    .macros
                    .get(&id)
                    .ok_or_else(|| MacroError::NotFound(id.clone()))?;
                return Ok(MacroListResponse {
                    catalog_revision: state.revision,
                    macros: vec![definition.into()],
                    definition: Some(definition.clone()),
                    total: 1,
                    next_offset: None,
                });
            }
            let text = query.query.unwrap_or_default().to_lowercase();
            let candidates: Vec<_> = state
                .macros
                .values()
                .filter(|entry| {
                    (query.include_drafts || entry.shared)
                        && (text.is_empty()
                            || format!("{} {} {}", entry.id, entry.name, entry.description)
                                .to_lowercase()
                                .contains(&text))
                })
                .collect();
            let total = candidates.len();
            let offset = query.offset.unwrap_or(0);
            let limit = query.limit.unwrap_or(50).clamp(1, 100);
            let macros: Vec<_> = candidates
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(MacroSummary::from)
                .collect();
            let end = offset.saturating_add(macros.len());
            Ok(MacroListResponse {
                catalog_revision: state.revision,
                macros,
                definition: None,
                total,
                next_offset: (end < total).then_some(end),
            })
        })
    }
    pub fn save(&self, request: MacroSaveRequest) -> Result<MacroSaveResponse, MacroError> {
        self.with_catalog(|state| {
            let previous = state.macros.get(&request.id);
            let revision = match previous {
                Some(previous) if request.expected_revision == Some(previous.revision) => previous
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| MacroError::Invalid("revision exhausted".into()))?,
                Some(previous) => {
                    return Err(MacroError::Conflict {
                        id: request.id,
                        expected: request.expected_revision,
                        actual: previous.revision,
                    });
                }
                None if request.expected_revision.is_some() => {
                    return Err(MacroError::NotFound(request.id));
                }
                None if state.macros.len() >= MAX_MACROS => {
                    return Err(MacroError::Invalid("macro catalog is full".into()));
                }
                None => 1,
            };
            let definition = MacroDefinition {
                id: request.id,
                name: request.name,
                description: request.description,
                language_version: MACRO_LANGUAGE_VERSION,
                revision,
                parameters: request.parameters,
                script: request.script,
                shared: request
                    .shared
                    .unwrap_or_else(|| previous.is_some_and(|entry| entry.shared)),
                applies_to: request.applies_to,
                updated_at_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
            };
            validate_definition(&definition)?;
            let mut next = state.clone();
            next.revision = next
                .revision
                .checked_add(1)
                .ok_or_else(|| MacroError::Invalid("catalog revision exhausted".into()))?;
            next.macros
                .insert(definition.id.clone(), definition.clone());
            let bytes = serde_json::to_vec_pretty(&next)?;
            if bytes.len() > MAX_CATALOG_BYTES {
                return Err(MacroError::Invalid("catalog size limit".into()));
            }
            atomic_write(&self.path, &bytes)?;
            *state = next;
            Ok(MacroSaveResponse {
                catalog_revision: state.revision,
                definition,
            })
        })
    }

    pub(crate) fn prepare(&self, spec: MacroRunSpec) -> Result<PreparedMacro, MacroError> {
        if !(1..=MAX_MACRO_TIMEOUT_SECONDS).contains(&spec.timeout_seconds) {
            return Err(MacroError::Invalid(format!(
                "timeout_seconds must be 1..={MAX_MACRO_TIMEOUT_SECONDS}"
            )));
        }
        let definition = match (&spec.macro_id, &spec.script, spec.revision) {
            (Some(id), None, Some(revision)) => Some(self.with_catalog(|state| {
                let definition = state
                    .macros
                    .get(id)
                    .ok_or_else(|| MacroError::NotFound(id.clone()))?;
                if definition.revision != revision {
                    return Err(MacroError::Conflict {
                        id: id.clone(),
                        expected: Some(revision),
                        actual: definition.revision,
                    });
                }
                Ok(definition.clone())
            })?),
            (None, Some(_), None) if spec.args.is_empty() => None,
            _ => {
                return Err(MacroError::Invalid(
                    "choose macro_id+revision+args OR inline script without args/revision".into(),
                ));
            }
        };
        let empty = BTreeMap::new();
        let parameters = definition
            .as_ref()
            .map_or(&empty, |entry| &entry.parameters);
        let source = definition
            .as_ref()
            .map(|entry| entry.script.as_str())
            .or(spec.script.as_deref())
            .expect("validated source");
        let description = spec
            .description
            .clone()
            .or_else(|| definition.as_ref().map(|d| d.description.clone()))
            .unwrap_or_else(|| "Run inline Macro Script v1".into());
        validate_label("description", &description, 256)?;
        let types = parameter_types(parameters)?;
        let program = serial_macro::compile(source, &types, Limits::default())?;
        let arguments = arguments(parameters, &spec.args)?;
        Ok(PreparedMacro {
            spec,
            definition,
            program,
            arguments,
            description,
        })
    }
}

fn validate_label(field: &str, value: &str, maximum: usize) -> Result<(), MacroError> {
    if value.trim() != value
        || value.is_empty()
        || value.len() > maximum
        || value.chars().any(char::is_control)
    {
        Err(MacroError::Invalid(format!(
            "{field} must be nonempty, trimmed, control-free and at most {maximum} bytes"
        )))
    } else {
        Ok(())
    }
}

fn validate_definition(definition: &MacroDefinition) -> Result<(), MacroError> {
    if definition.id.is_empty()
        || definition.id.len() > 80
        || !definition
            .id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_-".contains(&b))
    {
        return Err(MacroError::Invalid(
            "id must use 1..80 lowercase letters, digits, _ or -".into(),
        ));
    }
    validate_label("name", &definition.name, 128)?;
    validate_label("description", &definition.description, 256)?;
    if let Some(applies) = &definition.applies_to {
        validate_label("applies_to.model_family", &applies.model_family, 128)?;
        if applies.model_names.len() > 128 {
            return Err(MacroError::Invalid("too many model names".into()));
        }
        for name in &applies.model_names {
            validate_label("model_name", name, 128)?;
        }
    }
    let types = parameter_types(&definition.parameters)?;
    serial_macro::compile(&definition.script, &types, Limits::default())?;
    Ok(())
}

fn parameter_types(
    parameters: &BTreeMap<String, MacroParameter>,
) -> Result<BTreeMap<String, ValueType>, MacroError> {
    if parameters.len() > 32 {
        return Err(MacroError::Invalid("at most 32 parameters".into()));
    }
    parameters
        .iter()
        .map(|(name, parameter)| {
            if name.is_empty()
                || name.len() > 64
                || !name.bytes().enumerate().all(|(i, b)| {
                    b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit())
                })
            {
                return Err(MacroError::Invalid(format!(
                    "invalid parameter name {name}"
                )));
            }
            if (parameter.minimum.is_some() || parameter.maximum.is_some())
                && parameter.value_type != MacroParameterType::Integer
            {
                return Err(MacroError::Invalid(format!(
                    "{name}: bounds require integer type"
                )));
            }
            if parameter
                .minimum
                .zip(parameter.maximum)
                .is_some_and(|(a, b)| a > b)
            {
                return Err(MacroError::Invalid(format!(
                    "{name}: minimum exceeds maximum"
                )));
            }
            if let Some(value) = &parameter.default {
                argument(name, parameter, value)?;
            }
            if let Some(description) = &parameter.description {
                validate_label(&format!("parameters.{name}.description"), description, 1024)?;
            }
            Ok((
                name.clone(),
                match parameter.value_type {
                    MacroParameterType::String => ValueType::String,
                    MacroParameterType::Integer => ValueType::Integer,
                    MacroParameterType::Boolean => ValueType::Boolean,
                },
            ))
        })
        .collect()
}

fn argument(
    name: &str,
    parameter: &MacroParameter,
    value: &serde_json::Value,
) -> Result<ScriptValue, MacroError> {
    let invalid = || {
        MacroError::Invalid(format!(
            "args.{name}: value violates declared type or bounds"
        ))
    };
    match parameter.value_type {
        MacroParameterType::String => value
            .as_str()
            .filter(|v| v.len() <= 4096)
            .map(|v| ScriptValue::String(v.into()))
            .ok_or_else(invalid),
        MacroParameterType::Boolean => value
            .as_bool()
            .map(ScriptValue::Boolean)
            .ok_or_else(invalid),
        MacroParameterType::Integer => value
            .as_i64()
            .filter(|v| {
                parameter.minimum.is_none_or(|min| *v >= min)
                    && parameter.maximum.is_none_or(|max| *v <= max)
            })
            .map(ScriptValue::Integer)
            .ok_or_else(invalid),
    }
}
fn arguments(
    parameters: &BTreeMap<String, MacroParameter>,
    args: &BTreeMap<String, serde_json::Value>,
) -> Result<BTreeMap<String, ScriptValue>, MacroError> {
    if let Some(name) = args.keys().find(|name| !parameters.contains_key(*name)) {
        return Err(MacroError::Invalid(format!("unknown args.{name}")));
    }
    parameters
        .iter()
        .map(|(name, parameter)| {
            let value = args
                .get(name)
                .or(parameter.default.as_ref())
                .ok_or_else(|| MacroError::Invalid(format!("missing args.{name}")))?;
            Ok((name.clone(), argument(name, parameter, value)?))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> MacroSaveRequest {
        MacroSaveRequest {
            id: "test".into(),
            name: "Test".into(),
            description: "Test macro".into(),
            parameters: BTreeMap::new(),
            script: "cmd(\"help\");".into(),
            expected_revision: None,
            shared: None,
            applies_to: None,
        }
    }
    #[test]
    fn drafts_explicit_sharing_revisions_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("macros.json");
        let catalog = MacroCatalog::new(path.clone());
        catalog.save(request()).unwrap();
        assert_eq!(catalog.list(Default::default()).unwrap().total, 0);
        assert!(matches!(
            catalog.save(request()),
            Err(MacroError::Conflict { .. })
        ));
        let mut edit = request();
        edit.expected_revision = Some(1);
        edit.shared = Some(true);
        catalog.save(edit).unwrap();
        let mut edit = request();
        edit.expected_revision = Some(2);
        catalog.save(edit).unwrap();
        let list = MacroCatalog::new(path).list(Default::default()).unwrap();
        assert_eq!(list.total, 1);
        assert_eq!(list.macros[0].revision, 3);
        assert!(list.macros[0].shared);
    }
    #[test]
    fn inline_is_not_saved_and_bad_scripts_fail() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = MacroCatalog::new(dir.path().join("macros.json"));
        let spec: MacroRunSpec =
            serde_json::from_value(serde_json::json!({"script":"cmd(\"help\");"})).unwrap();
        catalog.prepare(spec).unwrap();
        assert_eq!(catalog.list(Default::default()).unwrap().total, 0);
        let mut invalid = request();
        invalid.script = "os.exec(\"rm\");".into();
        assert!(catalog.save(invalid).is_err());
    }
}
