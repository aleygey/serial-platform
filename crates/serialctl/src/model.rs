use std::{
    collections::{HashMap, HashSet},
    io::{self, IsTerminal, Write as _},
    path::PathBuf,
};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand, ValueEnum};
use crossterm::{
    cursor::{Hide, Show},
    event::{Event, KeyCode, KeyEventKind, read},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use serial_protocol::{
    DeviceModel, DeviceModelListResponse, ModelConfirmationMethod, SetSlotDeviceModelRequest,
};

use crate::{api::ApiClient, config, display::safe_inline, i18n::tr};

#[derive(Debug, Args)]
pub struct ModelArgs {
    /// Read the one-time administrator credential from this file for catalog changes.
    #[arg(long, global = true)]
    admin_token_file: Option<PathBuf>,

    #[command(subcommand)]
    command: ModelCommand,
}

#[derive(Debug, Subcommand)]
enum ModelCommand {
    /// List the flat model catalog and current Slot bindings.
    List(OutputArgs),
    /// Print the model hierarchy with child indentation.
    Tree(OutputArgs),
    /// Add one model or family node.
    Add(AddArgs),
    /// Update one model node without changing its stable ID.
    Update(UpdateArgs),
    /// Bind a Slot to an existing model, or atomically create and bind one.
    Attach(AttachArgs),
    /// Remove the current model binding from a Slot.
    Detach(DetachArgs),
}

#[derive(Debug, Clone, Args)]
struct OutputArgs {
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct AddArgs {
    /// Stable model ID. When omitted in a TTY, it is derived from the name.
    #[arg(long)]
    id: Option<String>,
    /// Human-readable model or family name.
    #[arg(long)]
    name: Option<String>,
    /// Stable ID of the parent model/family.
    #[arg(long)]
    parent: Option<String>,
    /// Alternate model name. Repeat for multiple aliases.
    #[arg(long = "alias")]
    aliases: Vec<String>,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct UpdateArgs {
    /// Stable model ID to update.
    id: String,
    #[arg(long)]
    name: Option<String>,
    #[arg(long, conflicts_with = "clear_parent")]
    parent: Option<String>,
    #[arg(long)]
    clear_parent: bool,
    /// Replace all aliases. Repeat for multiple values.
    #[arg(long = "alias", conflicts_with = "clear_aliases")]
    aliases: Vec<String>,
    #[arg(long)]
    clear_aliases: bool,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ConfirmationArg {
    Serial,
    Telnet,
    Web,
    Human,
    Other,
}

impl From<ConfirmationArg> for ModelConfirmationMethod {
    fn from(value: ConfirmationArg) -> Self {
        match value {
            ConfirmationArg::Serial => Self::Serial,
            ConfirmationArg::Telnet => Self::Telnet,
            ConfirmationArg::Web => Self::Web,
            ConfirmationArg::Human => Self::Human,
            ConfirmationArg::Other => Self::Other,
        }
    }
}

#[derive(Debug, Clone, Args)]
struct AttachArgs {
    #[arg(long)]
    slot: String,
    /// Existing stable model ID. Omit in a TTY to choose with Up/Down.
    #[arg(long)]
    model: Option<String>,
    /// Create the missing model and bind it in one daemon transaction.
    #[arg(long)]
    create_if_missing: bool,
    /// Model name used with --create-if-missing.
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    parent: Option<String>,
    #[arg(long = "alias")]
    aliases: Vec<String>,
    /// How the physical DUT identity was confirmed. Interactive use defaults
    /// to human confirmation; scripts must state this explicitly.
    #[arg(long, value_enum)]
    confirm_via: Option<ConfirmationArg>,
    /// Short evidence note, for example the serial identity command used.
    #[arg(long)]
    note: Option<String>,
    /// Require this exact current model before replacing the binding.
    #[arg(long, conflicts_with = "expect_unbound")]
    expected_current: Option<String>,
    /// Require the Slot to be currently unbound.
    #[arg(long)]
    expect_unbound: bool,
    /// Force the Up/Down selector even when --model was supplied.
    #[arg(long, conflicts_with = "json")]
    interactive: bool,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct DetachArgs {
    #[arg(long)]
    slot: String,
    /// Require this exact current model before detaching.
    #[arg(long, conflicts_with = "expect_unbound")]
    expected_current: Option<String>,
    /// Require the Slot to be already unbound (useful for idempotent scripts).
    #[arg(long)]
    expect_unbound: bool,
    #[arg(long)]
    json: bool,
}

pub async fn run(api: &ApiClient, args: ModelArgs) -> Result<()> {
    let ModelArgs {
        admin_token_file,
        command,
    } = args;
    match command {
        ModelCommand::List(args) => list(api, args).await,
        ModelCommand::Tree(args) => tree(api, args).await,
        ModelCommand::Add(args) => {
            let admin = admin_api(api, admin_token_file.as_deref(), args.json).await?;
            add(&admin, args).await
        }
        ModelCommand::Update(args) => {
            let admin = admin_api(api, admin_token_file.as_deref(), args.json).await?;
            update(&admin, args).await
        }
        ModelCommand::Attach(args) => attach(api, args).await,
        ModelCommand::Detach(args) => detach(api, args).await,
    }
}

async fn admin_api(
    api: &ApiClient,
    token_file: Option<&std::path::Path>,
    json: bool,
) -> Result<ApiClient> {
    let authentication_required = match api.health().await {
        Ok(health) => health.auth_required,
        Err(error) if crate::api::is_unauthorized(&error) => true,
        Err(error) => return Err(error),
    };
    if !authentication_required {
        return Ok(api.clone());
    }
    let token = match token_file {
        Some(path) => config::read_token_if_present(path)?
            .with_context(|| format!("administrator token file {} is empty", path.display()))?,
        None if !json && io::stdin().is_terminal() && io::stdout().is_terminal() => {
            rpassword::prompt_password(tr("i.admin.prompt"))?
                .trim()
                .to_owned()
        }
        None => bail!(
            "--admin-token-file is required for non-interactive model catalog changes; the administrator credential is never saved"
        ),
    };
    if token.is_empty() {
        bail!(tr("i.admin.required"));
    }
    ApiClient::new(api.endpoint().to_owned(), Some(token))
}

async fn list(api: &ApiClient, args: OutputArgs) -> Result<()> {
    let catalog = api.device_models().await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&catalog)?);
        return Ok(());
    }
    let bindings = bindings_by_model(&catalog);
    println!(
        "{:<24} {:<28} {:<24} ALIASES / SLOTS",
        "ID", "NAME", "PARENT"
    );
    for model in &catalog.models {
        let slots = bindings
            .get(model.id.as_str())
            .map(|slots| slots.join(","))
            .unwrap_or_else(|| "-".into());
        let aliases = if model.aliases.is_empty() {
            "-".into()
        } else {
            model.aliases.join(",")
        };
        println!(
            "{:<24} {:<28} {:<24} {} / {}",
            safe_inline(&model.id),
            safe_inline(&model.name),
            safe_inline(model.parent_id.as_deref().unwrap_or("-")),
            safe_inline(&aliases),
            safe_inline(&slots),
        );
    }
    if catalog.models.is_empty() {
        println!("(no device models configured)");
    }
    Ok(())
}

async fn tree(api: &ApiClient, args: OutputArgs) -> Result<()> {
    let catalog = api.device_models().await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&catalog)?);
        return Ok(());
    }
    let bindings = bindings_by_model(&catalog);
    for row in all_tree_rows(&catalog.models) {
        let model = &catalog.models[row.index];
        let slots = bindings
            .get(model.id.as_str())
            .map(|slots| format!("  [Slots: {}]", slots.join(", ")))
            .unwrap_or_default();
        println!(
            "{}{}  ({}){}",
            "  ".repeat(row.depth),
            safe_inline(&model.name),
            safe_inline(&model.id),
            slots,
        );
    }
    if catalog.models.is_empty() {
        println!("(no device models configured)");
    }
    Ok(())
}

async fn add(api: &ApiClient, mut args: AddArgs) -> Result<()> {
    let interactive = !args.json && io::stdin().is_terminal() && io::stdout().is_terminal();
    if args.name.is_none() && interactive {
        args.name = Some(prompt("Model name")?);
    }
    let name = args
        .name
        .context("--name is required for non-interactive model add")?;
    let id = match args.id {
        Some(id) => id,
        None if interactive => {
            let proposed = normalize_model_id(&name);
            prompt_with_default("Stable model ID", &proposed)?
        }
        None => bail!("--id is required for non-interactive model add"),
    };
    let mut catalog = api.device_models().await?;
    if catalog.models.iter().any(|model| model.id == id) {
        bail!("device model {:?} already exists", safe_inline(&id));
    }
    catalog.models.push(DeviceModel {
        id,
        name,
        parent_id: args.parent,
        aliases: args.aliases,
    });
    let response = api
        .configure_device_models(catalog.models, Some(catalog.config_revision))
        .await?;
    print_catalog_response(&response, args.json)
}

async fn update(api: &ApiClient, args: UpdateArgs) -> Result<()> {
    if args.name.is_none()
        && args.parent.is_none()
        && !args.clear_parent
        && args.aliases.is_empty()
        && !args.clear_aliases
    {
        bail!("model update requires at least one changed field");
    }
    let mut catalog = api.device_models().await?;
    let model = catalog
        .models
        .iter_mut()
        .find(|model| model.id == args.id)
        .with_context(|| format!("unknown device model {:?}", safe_inline(&args.id)))?;
    if let Some(name) = args.name {
        model.name = name;
    }
    if let Some(parent) = args.parent {
        model.parent_id = Some(parent);
    } else if args.clear_parent {
        model.parent_id = None;
    }
    if args.clear_aliases {
        model.aliases.clear();
    } else if !args.aliases.is_empty() {
        model.aliases = args.aliases;
    }
    let response = api
        .configure_device_models(catalog.models, Some(catalog.config_revision))
        .await?;
    print_catalog_response(&response, args.json)
}

async fn attach(api: &ApiClient, args: AttachArgs) -> Result<()> {
    let interactive_terminal = io::stdin().is_terminal() && io::stdout().is_terminal();
    let confirmation_method = match args.confirm_via {
        Some(method) => method.into(),
        None if !args.json && interactive_terminal => ModelConfirmationMethod::Human,
        None => bail!(
            "--confirm-via is required for non-interactive model attachment; state how the physical DUT identity was verified"
        ),
    };
    if args.create_if_missing && args.model.is_none() {
        bail!("--create-if-missing requires --model");
    }
    if args.create_if_missing && args.name.is_none() {
        bail!("--create-if-missing requires --name");
    }
    if args.create_if_missing && args.interactive {
        bail!("--interactive cannot be combined with --create-if-missing");
    }
    if !args.create_if_missing
        && (args.name.is_some() || args.parent.is_some() || !args.aliases.is_empty())
    {
        bail!("--name, --parent and --alias require --create-if-missing");
    }
    let catalog = api.device_models().await?;
    let should_select = args.interactive || args.model.is_none();
    let model_id = if should_select {
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            bail!("--model is required when stdin/stdout is not a terminal");
        }
        if catalog.models.is_empty() {
            bail!(
                "no device models are configured; use `serialctl model add` or --create-if-missing"
            );
        }
        interactive_select_model(&catalog.models, args.model.as_deref())?
    } else {
        args.model.clone().expect("checked above")
    };
    if !args.create_if_missing && !catalog.models.iter().any(|model| model.id == model_id) {
        bail!("unknown device model {:?}", safe_inline(&model_id));
    }
    let current = catalog
        .bindings
        .iter()
        .find(|binding| binding.slot_id == args.slot)
        .map(|binding| binding.model_id.clone());
    let expected_current =
        explicit_or_observed_guard(args.expected_current, args.expect_unbound, current);
    let request = SetSlotDeviceModelRequest {
        model_id: Some(model_id),
        create_if_missing: args.create_if_missing,
        update_existing: false,
        name: args.name,
        parent_id: args.parent,
        clear_parent: false,
        aliases: args.aliases,
        clear_aliases: false,
        confirmation_method: Some(confirmation_method),
        note: args.note,
        source: workflow_source(args.json),
        expected_revision: Some(catalog.config_revision),
        expected_current: Some(expected_current),
    };
    let response = api.set_slot_device_model(&args.slot, &request).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else if let Some(binding) = response.binding {
        println!(
            "Slot {} now uses model {} at configuration revision {}{}",
            safe_inline(&binding.slot_id),
            safe_inline(&binding.model_id),
            response.config_revision,
            if response.created { " (created)" } else { "" },
        );
    }
    Ok(())
}

async fn detach(api: &ApiClient, args: DetachArgs) -> Result<()> {
    let catalog = api.device_models().await?;
    let current = catalog
        .bindings
        .iter()
        .find(|binding| binding.slot_id == args.slot)
        .map(|binding| binding.model_id.clone());
    let expected_current =
        explicit_or_observed_guard(args.expected_current, args.expect_unbound, current);
    let request = SetSlotDeviceModelRequest {
        model_id: None,
        create_if_missing: false,
        update_existing: false,
        name: None,
        parent_id: None,
        clear_parent: false,
        aliases: Vec::new(),
        clear_aliases: false,
        confirmation_method: None,
        note: None,
        source: workflow_source(args.json),
        expected_revision: Some(catalog.config_revision),
        expected_current: Some(expected_current),
    };
    let response = api.set_slot_device_model(&args.slot, &request).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        println!(
            "Slot {} model binding removed at configuration revision {}",
            safe_inline(&args.slot),
            response.config_revision,
        );
    }
    Ok(())
}

fn explicit_or_observed_guard(
    expected_current: Option<String>,
    expect_unbound: bool,
    observed: Option<String>,
) -> Option<String> {
    if expect_unbound {
        None
    } else {
        expected_current.or(observed)
    }
}

fn workflow_source(json: bool) -> String {
    if !json && io::stdin().is_terminal() && io::stdout().is_terminal() {
        "human:serialctl".into()
    } else {
        "script:serialctl".into()
    }
}

fn print_catalog_response(
    response: &serial_protocol::ConfigureDeviceModelsResponse,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(response)?);
    } else {
        println!(
            "Saved {} device model(s) at configuration revision {}",
            response.models.len(),
            response.config_revision,
        );
    }
    Ok(())
}

fn bindings_by_model(catalog: &DeviceModelListResponse) -> HashMap<&str, Vec<String>> {
    let mut bindings: HashMap<&str, Vec<String>> = HashMap::new();
    for binding in &catalog.bindings {
        bindings
            .entry(binding.model_id.as_str())
            .or_default()
            .push(binding.slot_id.clone());
    }
    bindings
}

#[derive(Debug, Clone, Copy)]
struct TreeRow {
    index: usize,
    depth: usize,
}

fn all_tree_rows(models: &[DeviceModel]) -> Vec<TreeRow> {
    let expanded = models
        .iter()
        .map(|model| model.id.clone())
        .collect::<HashSet<_>>();
    visible_tree_rows(models, &expanded)
}

fn visible_tree_rows(models: &[DeviceModel], expanded: &HashSet<String>) -> Vec<TreeRow> {
    fn visit(
        models: &[DeviceModel],
        parent: Option<&str>,
        depth: usize,
        expanded: &HashSet<String>,
        rows: &mut Vec<TreeRow>,
    ) {
        for (index, model) in models
            .iter()
            .enumerate()
            .filter(|(_, model)| model.parent_id.as_deref() == parent)
        {
            rows.push(TreeRow { index, depth });
            if expanded.contains(&model.id) {
                visit(models, Some(&model.id), depth + 1, expanded, rows);
            }
        }
    }

    let mut rows = Vec::new();
    visit(models, None, 0, expanded, &mut rows);
    rows
}

fn interactive_select_model(models: &[DeviceModel], preferred: Option<&str>) -> Result<String> {
    let mut expanded = HashSet::new();
    if let Some(preferred) = preferred {
        expand_ancestors(models, preferred, &mut expanded);
    }
    let mut rows = visible_tree_rows(models, &expanded);
    if rows.is_empty() {
        bail!("device model catalog has no root nodes");
    }
    let mut selected = preferred
        .and_then(|id| rows.iter().position(|row| models[row.index].id == id))
        .unwrap_or(0);
    let _guard = SelectorTerminalGuard::enter()?;

    loop {
        rows = visible_tree_rows(models, &expanded);
        selected = selected.min(rows.len().saturating_sub(1));
        render_selector(models, &rows, &expanded, selected)?;
        let Event::Key(key) = read().context("read model selector key")? else {
            continue;
        };
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            continue;
        }
        match key.code {
            KeyCode::Up => selected = selected.saturating_sub(1),
            KeyCode::Down => selected = (selected + 1).min(rows.len() - 1),
            KeyCode::Home => selected = 0,
            KeyCode::End => selected = rows.len() - 1,
            KeyCode::Right => {
                let model = &models[rows[selected].index];
                if has_children(models, &model.id) {
                    expanded.insert(model.id.clone());
                }
            }
            KeyCode::Left => {
                let model = &models[rows[selected].index];
                if expanded.remove(&model.id) {
                    continue;
                }
                if let Some(parent) = model.parent_id.as_deref() {
                    rows = visible_tree_rows(models, &expanded);
                    if let Some(index) = rows.iter().position(|row| models[row.index].id == parent)
                    {
                        selected = index;
                    }
                }
            }
            KeyCode::Enter => {
                let model = &models[rows[selected].index];
                if has_children(models, &model.id) && !expanded.contains(&model.id) {
                    expanded.insert(model.id.clone());
                } else {
                    return Ok(model.id.clone());
                }
            }
            KeyCode::Esc | KeyCode::Char('q' | 'Q') => bail!("model selection cancelled"),
            _ => {}
        }
    }
}

fn render_selector(
    models: &[DeviceModel],
    rows: &[TreeRow],
    expanded: &HashSet<String>,
    selected: usize,
) -> Result<()> {
    let mut stdout = io::stdout();
    execute!(
        stdout,
        crossterm::cursor::MoveTo(0, 0),
        crossterm::terminal::Clear(crossterm::terminal::ClearType::All)
    )?;
    writeln!(
        stdout,
        "Select device model — ↑/↓ move, Enter expand/select, ← collapse, Esc cancel"
    )?;
    writeln!(stdout)?;
    for (position, row) in rows.iter().enumerate() {
        let model = &models[row.index];
        let branch = if has_children(models, &model.id) {
            if expanded.contains(&model.id) {
                "▾"
            } else {
                "▸"
            }
        } else {
            "•"
        };
        writeln!(
            stdout,
            "{} {}{} {}  ({})",
            if position == selected { ">" } else { " " },
            "  ".repeat(row.depth),
            branch,
            safe_inline(&model.name),
            safe_inline(&model.id),
        )?;
    }
    stdout.flush()?;
    Ok(())
}

fn expand_ancestors(models: &[DeviceModel], id: &str, expanded: &mut HashSet<String>) {
    let by_id = models
        .iter()
        .map(|model| (model.id.as_str(), model))
        .collect::<HashMap<_, _>>();
    let mut current = by_id.get(id).and_then(|model| model.parent_id.as_deref());
    while let Some(parent) = current {
        if !expanded.insert(parent.to_owned()) {
            break;
        }
        current = by_id
            .get(parent)
            .and_then(|model| model.parent_id.as_deref());
    }
}

fn has_children(models: &[DeviceModel], id: &str) -> bool {
    models
        .iter()
        .any(|model| model.parent_id.as_deref() == Some(id))
}

struct SelectorTerminalGuard;

impl SelectorTerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode()?;
        if let Err(error) = execute!(io::stdout(), EnterAlternateScreen, Hide) {
            let _ = disable_raw_mode();
            return Err(error.into());
        }
        Ok(Self)
    }
}

impl Drop for SelectorTerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), Show, LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}

fn prompt(label: &str) -> Result<String> {
    print!("{label}: ");
    io::stdout().flush()?;
    let mut value = String::new();
    io::stdin().read_line(&mut value)?;
    let value = value.trim().to_owned();
    if value.is_empty() {
        bail!("{label} is required");
    }
    Ok(value)
}

fn prompt_with_default(label: &str, default: &str) -> Result<String> {
    print!("{label} [{default}]: ");
    io::stdout().flush()?;
    let mut value = String::new();
    io::stdin().read_line(&mut value)?;
    let value = value.trim();
    Ok(if value.is_empty() {
        default.to_owned()
    } else {
        value.to_owned()
    })
}

fn normalize_model_id(name: &str) -> String {
    let trimmed = name.trim();
    let mut contains_non_ascii_name_character = false;
    let normalized = name
        .trim()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                contains_non_ascii_name_character |=
                    !character.is_ascii() && character.is_alphanumeric();
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_owned();
    if !contains_non_ascii_name_character {
        return if normalized.is_empty() {
            "model".into()
        } else {
            normalized
        };
    }

    // Unicode names cannot be represented directly in the stable ASCII ID.
    // Keep any readable ASCII prefix and append a deterministic FNV-1a digest
    // so distinct Chinese model names do not all collapse to `model`.
    let digest = trimmed
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    if normalized.is_empty() {
        format!("model-{digest:016x}")
    } else {
        format!("{normalized}-{digest:016x}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn models() -> Vec<DeviceModel> {
        vec![
            DeviceModel {
                id: "tl-as7230-family".into(),
                name: "TL-AS7230".into(),
                parent_id: None,
                aliases: Vec::new(),
            },
            DeviceModel {
                id: "tl-as7230".into(),
                name: "TL-AS7230".into(),
                parent_id: Some("tl-as7230-family".into()),
                aliases: Vec::new(),
            },
            DeviceModel {
                id: "tl-as7230-w".into(),
                name: "TL-AS7230-W".into(),
                parent_id: Some("tl-as7230-family".into()),
                aliases: vec!["7230W".into()],
            },
        ]
    }

    #[test]
    fn collapsed_tree_only_shows_roots_and_expansion_indents_children() {
        let models = models();
        let mut expanded = HashSet::new();
        let collapsed = visible_tree_rows(&models, &expanded);
        assert_eq!(collapsed.len(), 1);
        assert_eq!(collapsed[0].depth, 0);
        expanded.insert("tl-as7230-family".into());
        let visible = visible_tree_rows(&models, &expanded);
        assert_eq!(visible.len(), 3);
        assert_eq!(visible[1].depth, 1);
        assert_eq!(visible[2].depth, 1);
    }

    #[test]
    fn expected_current_defaults_to_the_observed_binding() {
        assert_eq!(
            explicit_or_observed_guard(None, false, Some("old".into())),
            Some("old".into())
        );
        assert_eq!(
            explicit_or_observed_guard(Some("required".into()), false, Some("old".into())),
            Some("required".into())
        );
        assert_eq!(
            explicit_or_observed_guard(Some("required".into()), true, Some("old".into())),
            None
        );
    }

    #[test]
    fn model_id_normalization_is_stable() {
        assert_eq!(normalize_model_id(" TL-AS7230 W "), "tl-as7230-w");
        let chinese = normalize_model_id("样机");
        assert_eq!(chinese, "model-b732c7b1f9c3983c");
        assert_eq!(chinese, normalize_model_id("  样机  "));
        assert_ne!(chinese, normalize_model_id("另一台样机"));
        assert_ne!(
            normalize_model_id("TL-样机"),
            normalize_model_id("TL-样机二")
        );
    }
}
