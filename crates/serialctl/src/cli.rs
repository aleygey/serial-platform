use std::{
    io::IsTerminal,
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use chrono::DateTime;
use clap::{Args, Parser, Subcommand};
use serial_protocol::{
    Direction, EventKind, EventQuery, PROTOCOL_VERSION, SerialSettings, SlotConfig,
    apply_transport_profile,
};

use crate::{
    api::ApiClient,
    config::{self, LoadedConfig},
    display::{
        format_event_plain, format_wall_time_local, pad_display, safe_inline, trigger_status_label,
    },
    i18n::{self, Lang, tr, trf},
    profile::{self, SAFE_TRANSPORT_NAME},
    tui, ws,
};

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:3210";

#[derive(Debug, Parser)]
#[command(
    name = "serialctl",
    version,
    about = "Human console and administration client for seriald"
)]
pub struct Cli {
    /// seriald base URL. The saved value is used when omitted.
    #[arg(long, global = true, env = "SERIALD_ENDPOINT")]
    endpoint: Option<String>,

    /// Read the bearer token from this file. Tokens are never accepted inline.
    #[arg(long, global = true, env = "SERIALD_TOKEN_FILE")]
    token_file: Option<PathBuf>,

    /// Override the serialctl configuration file.
    #[arg(long, global = true, env = "SERIALCTL_CONFIG")]
    config: Option<PathBuf>,

    /// Override the UI language (en or zh). The saved value is used when omitted.
    #[arg(long, global = true, env = "SERIALCTL_LANG", value_parser = parse_lang)]
    lang: Option<Lang>,

    /// Open this Slot initially in interactive mode.
    #[arg(long)]
    initial_slot: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Discover serial ports, configure Slots and save the daily credential.
    #[command(alias = "init")]
    Setup(SetupArgs),
    /// Manage reusable UART and DUT Profiles.
    Profile(profile::ProfileArgs),
    /// Print the daemon and Slot state.
    Status(OutputArgs),
    /// Diagnose the saved client connection, ports, stream, storage, or Slot state.
    Doctor(DoctorArgs),
    /// List retained Slot/epoch journal archives.
    Archives(ArchivesArgs),
    /// Query durable serial timeline events.
    Logs(LogsArgs),
}

#[derive(Debug, Clone, Args)]
pub struct SetupArgs {
    /// Serial port on the seriald host. Repeat for multiple Slots.
    #[arg(long)]
    port: Vec<String>,

    /// Display name matching each --port by position.
    #[arg(long)]
    display_name: Vec<String>,

    /// Stable Slot ID matching each --port by position.
    #[arg(long)]
    slot_id: Vec<String>,

    /// Transport Profile applied to every selected port.
    #[arg(long)]
    transport: Option<String>,

    /// Device Profile applied to every selected port.
    #[arg(long, conflicts_with = "generic")]
    device: Option<String>,

    /// Use no DUT-specific Profile.
    #[arg(long)]
    generic: bool,

    /// Read the one-time setup administrator credential from this file.
    #[arg(long)]
    admin_token_file: Option<PathBuf>,

    /// Read the daily operator credential to save from this file.
    #[arg(long)]
    operator_token_file: Option<PathBuf>,

    /// Destination for the validated daily operator credential.
    #[arg(long)]
    save_operator_token_file: Option<PathBuf>,

    /// Remove existing Slots whose ports were not selected. The safe default is to keep them.
    #[arg(long)]
    delete_omitted: bool,

    /// Emit one machine-readable result. Requires all non-default inputs as flags/files.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
pub struct OutputArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct DoctorArgs {
    #[command(subcommand)]
    command: Option<DoctorCommand>,

    /// Emit machine-readable JSON for the default connection check.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Subcommand)]
pub enum DoctorCommand {
    /// Verify client configuration, HTTP reachability, authentication and daemon identity.
    Connection(OutputArgs),
    /// Explain whether one configured serial endpoint can be opened.
    Port(DoctorSlotArgs),
    /// Compare an independent live subscription with authoritative offsets and the journal.
    Stream(DoctorStreamArgs),
    /// Report retained journal usage and logging health.
    Storage(OutputArgs),
    /// Print the authoritative Slot state, control, Run, Trigger and effective settings.
    State(DoctorSlotArgs),
}

#[derive(Debug, Args)]
pub struct DoctorSlotArgs {
    #[arg(long)]
    pub slot: String,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct DoctorStreamArgs {
    #[arg(long)]
    pub slot: String,
    /// Observation duration in seconds (1..=60).
    #[arg(long, default_value_t = 10, value_parser = parse_doctor_duration)]
    pub duration: u64,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ArchivesArgs {
    /// Only list archives for this Slot.
    #[arg(long)]
    slot: Option<String>,

    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
pub struct LogsArgs {
    /// Slot to query. Defaults to the last selected Slot.
    #[arg(long)]
    slot: Option<String>,

    /// Only return events whose decoded data contains this text.
    #[arg(long, conflicts_with = "regex")]
    contains: Option<String>,

    /// Only return events whose bounded decoded byte stream matches this regular expression.
    #[arg(long, conflicts_with = "contains")]
    regex: Option<String>,

    /// Return events strictly after this sequence.
    #[arg(long)]
    after_seq: Option<u64>,

    /// Return events through this sequence, inclusively.
    #[arg(long)]
    through_seq: Option<u64>,

    /// Only return events strictly after this RFC3339 timestamp.
    #[arg(long, value_parser = parse_rfc3339_ns)]
    after_time: Option<i64>,

    /// Only return events strictly before this RFC3339 timestamp.
    #[arg(long, value_parser = parse_rfc3339_ns)]
    before_time: Option<i64>,

    /// Pin the query to one daemon epoch when continuing an archived cursor.
    #[arg(long)]
    epoch: Option<uuid::Uuid>,

    /// Only return events from this Run.
    #[arg(long)]
    run: Option<uuid::Uuid>,

    /// Only return events from this operation.
    #[arg(long)]
    operation: Option<uuid::Uuid>,

    /// Only return events attributed to this actor ID.
    #[arg(long)]
    actor: Option<String>,

    /// Only return this event kind (for example rx, tx, or serial-closed).
    #[arg(long, value_parser = parse_event_kind)]
    kind: Option<EventKind>,

    /// Only return rx, tx, or non-byte control events.
    #[arg(long, value_parser = parse_direction)]
    direction: Option<Direction>,

    /// Maximum number of events returned.
    #[arg(long, default_value_t = 200, value_parser = parse_log_limit)]
    limit: usize,

    /// Emit the full query response as JSON.
    #[arg(long)]
    json: bool,
}

pub async fn run(cli: Cli) -> Result<()> {
    let loaded = LoadedConfig::load(cli.config.clone())?;
    i18n::set_lang(cli.lang.or(loaded.config.language).unwrap_or_default());
    validate_cli_scope(&cli)?;

    if let Some(Command::Setup(args)) = &cli.command {
        return run_setup(loaded, cli.endpoint, cli.token_file, args.clone()).await;
    }

    let resolved = loaded.resolve(cli.endpoint, cli.token_file)?;
    let api = ApiClient::new(resolved.endpoint.clone(), resolved.token.clone())?;

    match cli.command {
        None => {
            if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
                bail!(tr("m.terminal.required"));
            }
            tui::run(
                api,
                loaded,
                cli.initial_slot.or(resolved.last_slot),
                resolved.endpoint,
                resolved.token,
            )
            .await
        }
        Some(Command::Status(args)) => run_status(&api, args).await,
        Some(Command::Doctor(args)) => run_doctor(&api, &loaded, &resolved, args).await,
        Some(Command::Profile(args)) => profile::run(&api, args).await,
        Some(Command::Archives(args)) => run_archives(&api, args).await,
        Some(Command::Logs(args)) => run_logs(&api, resolved.last_slot, args).await,
        Some(Command::Setup(_)) => unreachable!("handled before resolving configuration"),
    }
}

fn validate_cli_scope(cli: &Cli) -> Result<()> {
    if cli.initial_slot.is_some() && cli.command.is_some() {
        bail!(tr("m.scope.error"));
    }
    Ok(())
}

async fn run_status(api: &ApiClient, args: OutputArgs) -> Result<()> {
    let status = api.status().await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&status)?);
        return Ok(());
    }

    println!(
        "{}",
        trf(
            "m.status.header",
            &[
                &status.server_id.to_string(),
                &status.daemon_epoch.to_string(),
                &status.slots.len().to_string(),
            ]
        )
    );
    for slot in status.slots {
        let baud_rate = slot
            .effective_transport
            .map(|transport| transport.baud_rate)
            .unwrap_or(slot.config.settings.baud_rate);
        let control = slot
            .control
            .as_ref()
            .map(|lease| lease.owner.label.as_str())
            .unwrap_or("-");
        println!(
            "{} {:<10?} {:<8?} {} {:>7} baud  {}",
            pad_display(&safe_inline(&slot.config.display_name), 16),
            slot.session_state,
            slot.target_activity,
            pad_display(&safe_inline(&slot.config.port), 8),
            baud_rate,
            trf("m.status.control", &[&safe_inline(control)])
        );
        if let Some(reason) = slot.state_reason {
            println!("{}", trf("m.status.reason", &[&safe_inline(&reason)]));
        }
        if let Some(trigger) = slot.active_trigger {
            println!(
                "{}",
                trf(
                    "m.status.trigger",
                    &[
                        &trigger.id.to_string(),
                        trigger_status_label(trigger.status),
                        &trigger.fires_confirmed.to_string(),
                    ],
                )
            );
        }
    }
    Ok(())
}

#[derive(serde::Serialize)]
struct DoctorReport<'a> {
    config_path: String,
    endpoint: &'a str,
    token_configured: bool,
    daemon_status: &'a str,
    server_id: String,
    daemon_epoch: String,
    protocol_version: u16,
    protocol_compatible: bool,
    uptime_ms: u64,
    slots: usize,
    online_slots: usize,
}

async fn run_doctor(
    api: &ApiClient,
    loaded: &LoadedConfig,
    resolved: &config::ResolvedConfig,
    args: DoctorArgs,
) -> Result<()> {
    match args.command {
        None => run_doctor_connection(api, loaded, resolved, OutputArgs { json: args.json }).await,
        Some(DoctorCommand::Connection(args)) => {
            run_doctor_connection(api, loaded, resolved, args).await
        }
        Some(DoctorCommand::Port(args)) => crate::doctor::port(api, args).await,
        Some(DoctorCommand::Stream(args)) => crate::doctor::stream(api, args).await,
        Some(DoctorCommand::Storage(args)) => crate::doctor::storage(api, args).await,
        Some(DoctorCommand::State(args)) => crate::doctor::state(api, args).await,
    }
}

async fn run_doctor_connection(
    api: &ApiClient,
    loaded: &LoadedConfig,
    resolved: &config::ResolvedConfig,
    args: OutputArgs,
) -> Result<()> {
    let health = api.health().await.context("daemon health check failed")?;
    let status = api.status().await.context("daemon status request failed")?;
    let online_slots = status
        .slots
        .iter()
        .filter(|slot| slot.session_state == serial_protocol::SessionState::Online)
        .count();
    let report = DoctorReport {
        config_path: loaded.path.display().to_string(),
        endpoint: &resolved.endpoint,
        token_configured: resolved.token.is_some(),
        daemon_status: &health.status,
        server_id: health.server_id.to_string(),
        daemon_epoch: health.daemon_epoch.to_string(),
        protocol_version: health.protocol_version,
        protocol_compatible: health.protocol_version == PROTOCOL_VERSION
            && status.protocol_version == PROTOCOL_VERSION,
        uptime_ms: health.uptime_ms,
        slots: status.slots.len(),
        online_slots,
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "{} {}",
            pad_display(tr("m.doctor.config"), 12),
            report.config_path
        );
        println!(
            "{} {}",
            pad_display(tr("m.doctor.endpoint"), 12),
            report.endpoint
        );
        println!(
            "{} {}",
            pad_display(tr("m.doctor.token"), 12),
            if report.token_configured {
                tr("m.token.configured")
            } else {
                tr("m.token.missing")
            }
        );
        println!(
            "{} {}",
            pad_display(tr("m.doctor.daemon"), 12),
            report.daemon_status
        );
        println!(
            "{} {}",
            pad_display(tr("m.doctor.server"), 12),
            report.server_id
        );
        println!(
            "{} {}",
            pad_display(tr("m.doctor.epoch"), 12),
            report.daemon_epoch
        );
        println!(
            "{} {} ({})",
            pad_display(tr("m.doctor.protocol"), 12),
            report.protocol_version,
            if report.protocol_compatible {
                tr("m.doctor.protocol.compatible")
            } else {
                tr("m.doctor.protocol.mismatch")
            }
        );
        println!(
            "{} {}",
            pad_display(tr("m.doctor.uptime"), 12),
            trf("m.uptime.ms", &[&report.uptime_ms.to_string()])
        );
        println!(
            "{} {}",
            pad_display(tr("m.doctor.slots"), 12),
            trf(
                "m.doctor.slots.value",
                &[&report.slots.to_string(), &report.online_slots.to_string()]
            )
        );
    }
    Ok(())
}

async fn run_archives(api: &ApiClient, args: ArchivesArgs) -> Result<()> {
    let response = api.archives(args.slot.as_deref()).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }

    if response.archives.is_empty() {
        println!("{}", tr("m.archives.none"));
    }
    for archive in response.archives {
        println!(
            "{}",
            trf(
                "m.archives.line",
                &[
                    &pad_display(&safe_inline(&archive.slot_id), 16),
                    &archive.epoch.to_string(),
                    &format_wall_time_local(archive.first_segment_wall_time_ns),
                    &format_wall_time_local(archive.last_segment_wall_time_ns),
                    &archive.first_seq.to_string(),
                    &archive.last_seq.to_string(),
                    &format_byte_size(archive.total_bytes),
                    &archive.segment_count.to_string(),
                    if archive.has_open_segment {
                        tr("m.archives.open")
                    } else {
                        ""
                    },
                ]
            )
        );
    }
    if response.truncated {
        eprintln!("{}", tr("m.archives.truncated"));
    }
    Ok(())
}

async fn run_logs(api: &ApiClient, last_slot: Option<String>, args: LogsArgs) -> Result<()> {
    if let (Some(after), Some(before)) = (args.after_time, args.before_time)
        && after >= before
    {
        bail!(tr("m.logs.time.order"));
    }
    if let (Some(after), Some(through)) = (args.after_seq, args.through_seq)
        && after > through
    {
        bail!(tr("m.logs.seq.order"));
    }
    let query_spans_epoch = args.run.is_none()
        && args.operation.is_none()
        && args.after_seq.is_none()
        && args.through_seq.is_none()
        && args.after_time.is_none()
        && args.before_time.is_none();
    if query_spans_epoch && !args.json {
        eprintln!("{}", tr("m.logs.span.warn"));
    }
    if args.regex.is_some()
        && api.configuration_status().await?.protocol_version != Some(PROTOCOL_VERSION)
    {
        bail!(
            "regular-expression log queries require seriald WebSocket protocol \
             {PROTOCOL_VERSION}; use the matching v0.4 release or --contains"
        );
    }
    let slot_id = match args.slot.or(last_slot) {
        Some(slot) => slot,
        None => api
            .status()
            .await?
            .slots
            .into_iter()
            .next()
            .map(|slot| slot.config.id)
            .context(tr("st.no.slot"))?,
    };
    let response = api
        .events(
            &slot_id,
            &EventQuery {
                epoch: args.epoch,
                after_seq: args.after_seq,
                through_seq: args.through_seq,
                before_wall_time_ns: args.before_time,
                after_wall_time_ns: args.after_time,
                direction: args.direction,
                kind: args.kind,
                actor_id: args.actor,
                run_id: args.run,
                operation_id: args.operation,
                contains: args.contains,
                regex: args.regex,
                limit_events: Some(args.limit),
                limit_bytes: Some(2 * 1024 * 1024),
            },
        )
        .await?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }

    for event in &response.events {
        println!("{}", format_event_plain(event));
    }
    if response.truncated {
        if let Some(cursor) = &response.next_cursor {
            eprintln!(
                "{}",
                trf(
                    "m.logs.truncated",
                    &[&cursor.epoch.to_string(), &cursor.after_seq.to_string()]
                )
            );
        } else {
            eprintln!("{}", tr("m.logs.truncated.nocursor"));
        }
    }
    for gap in response.gaps {
        eprintln!(
            "{}",
            trf(
                "m.logs.gap",
                &[
                    &gap.first_seq.to_string(),
                    &gap.last_seq.to_string(),
                    &format!("{:?}", gap.reason),
                    &gap.epoch.to_string(),
                ]
            )
        );
    }
    Ok(())
}

async fn run_setup(
    mut loaded: LoadedConfig,
    endpoint_override: Option<String>,
    token_file_override: Option<PathBuf>,
    args: SetupArgs,
) -> Result<()> {
    let interactive =
        std::io::stdin().is_terminal() && std::io::stdout().is_terminal() && !args.json;
    if token_file_override.is_some() {
        bail!(
            "--token-file is a daily read credential for normal commands and is not accepted by \
             setup; use --operator-token-file for input and --save-operator-token-file for the \
             validated destination"
        );
    }

    let saved_endpoint = loaded
        .config
        .endpoint
        .clone()
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
    let endpoint = match endpoint_override {
        Some(endpoint) => endpoint,
        None if interactive => prompt_with_default(tr("i.endpoint"), &saved_endpoint)?,
        None => saved_endpoint,
    };

    let token_file = args
        .save_operator_token_file
        .clone()
        .or_else(|| loaded.config.token_file.clone())
        .unwrap_or_else(|| loaded.default_token_path());
    ensure_distinct_credential_paths(
        args.admin_token_file.as_deref(),
        args.operator_token_file.as_deref(),
        &token_file,
    )?;
    let existing_operator_token = config::read_token_if_present(&token_file)?;
    if existing_operator_token.is_some() && !args.json {
        println!("{}", tr("i.token.notice"));
    }
    let admin_token = match args.admin_token_file.as_deref() {
        Some(path) => config::read_token_if_present(path)?
            .with_context(|| format!("administrator token file {} is empty", path.display()))?,
        None if interactive => rpassword::prompt_password(tr("i.admin.prompt"))?
            .trim()
            .to_owned(),
        None => bail!("--admin-token-file is required in non-interactive setup"),
    };
    if admin_token.is_empty() {
        bail!(tr("i.admin.required"));
    }

    let admin_api = ApiClient::new(endpoint.clone(), Some(admin_token.clone()))?;
    let health = admin_api.health().await.context(tr("i.unreachable"))?;
    if health.protocol_version != PROTOCOL_VERSION {
        bail!(
            "seriald protocol {} is incompatible with this client (expected {}); install all \
             v0.4 components from the same release package",
            health.protocol_version,
            PROTOCOL_VERSION
        );
    }
    let current = admin_api
        .configuration_status()
        .await
        .context(tr("i.status.fail"))?;
    let config_revision = current.config_revision;
    let existing_slots = current
        .slots
        .into_iter()
        .map(|slot| slot.config)
        .collect::<Vec<_>>();
    if !args.json {
        println!(
            "{}",
            trf(
                "i.connected",
                &[
                    &health.server_id.to_string(),
                    &health.daemon_epoch.to_string()
                ]
            )
        );
    }

    let ports = admin_api.ports().await?;
    if ports.is_empty() {
        bail!(tr("i.no.ports"));
    }
    if !args.json {
        println!("{}", tr("i.ports.header"));
        for (index, port) in ports.iter().enumerate() {
            let detail = port
                .product
                .as_deref()
                .or(port.manufacturer.as_deref())
                .unwrap_or(&port.port_type);
            println!(
                "  {:>2}. {} {}",
                index + 1,
                pad_display(&safe_inline(&port.name), 10),
                safe_inline(detail)
            );
        }
    }
    let existing_selection = ports
        .iter()
        .enumerate()
        .filter(|(_, port)| {
            existing_slots
                .iter()
                .any(|slot| same_serial_port(&slot.port, &port.name))
        })
        .map(|(index, _)| (index + 1).to_string())
        .collect::<Vec<_>>();
    let default_selection = if existing_selection.is_empty() {
        (1..=ports.len().min(2))
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    } else {
        existing_selection.join(",")
    };
    let selected = if args.port.is_empty() {
        if !interactive {
            bail!("at least one --port is required in non-interactive setup");
        }
        let selection = prompt_with_default(tr("i.select.ports"), &default_selection)?;
        parse_selection(&selection, ports.len())?
    } else {
        args.port
            .iter()
            .map(|requested| {
                ports
                    .iter()
                    .position(|port| same_serial_port(&port.name, requested))
                    .with_context(|| {
                        format!("serial port {requested:?} was not discovered on the seriald host")
                    })
            })
            .collect::<Result<Vec<_>>>()?
    };
    validate_parallel_values("--display-name", &args.display_name, selected.len())?;
    validate_parallel_values("--slot-id", &args.slot_id, selected.len())?;

    let (transport_catalog, transport_server_managed) =
        profile::load_transport_catalog(&admin_api).await?;
    let device_catalog = profile::load_device_catalog(&admin_api).await?;

    if !args.json {
        println!("{}", tr("i.profile.note"));
        println!(
            "Transport Profiles: {}",
            transport_catalog
                .profiles
                .iter()
                .map(|profile| profile.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        println!(
            "Device Profiles: Generic{}",
            if device_catalog.profiles.is_empty() {
                String::new()
            } else {
                format!(
                    ", {}",
                    device_catalog
                        .profiles
                        .iter()
                        .map(|profile| profile.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        );
    }
    if !existing_slots.is_empty() && !args.json {
        println!("{}", tr("i.existing.keep"));
    }
    let mut slots = Vec::with_capacity(selected.len());
    for (slot_index, port_index) in selected.into_iter().enumerate() {
        let port = &ports[port_index];
        let existing = existing_slots
            .iter()
            .find(|slot| same_serial_port(&slot.port, &port.name))
            .cloned();
        let preserve_existing_transport = existing.is_some()
            && args.transport.is_none()
            && (!interactive || !transport_server_managed);
        let default_name = existing
            .as_ref()
            .map(|slot| slot.display_name.clone())
            .unwrap_or_else(|| port.name.clone());
        let display_name = match args.display_name.get(slot_index) {
            Some(name) => name.clone(),
            None if interactive => prompt_with_default(
                &trf("i.slot.name", &[&safe_inline(&port.name)]),
                &default_name,
            )?,
            None => default_name,
        };
        let default_id = existing
            .as_ref()
            .map(|slot| slot.id.clone())
            .unwrap_or_else(|| normalize_slot_id(&display_name, slot_index + 1));
        let entered_id = match args.slot_id.get(slot_index) {
            Some(id) => id.clone(),
            None if interactive => {
                prompt_with_default(&trf("i.slot.id", &[&safe_inline(&port.name)]), &default_id)?
            }
            None => default_id,
        };
        let base_id = normalize_slot_id(&entered_id, slot_index + 1);
        let mut id = base_id.clone();
        let mut suffix = 2;
        while slots.iter().any(|slot: &SlotConfig| slot.id == id) {
            id = format!("{base_id}-{suffix}");
            suffix += 1;
        }
        let mut slot = existing.unwrap_or_else(|| SlotConfig {
            id: id.clone(),
            display_name: display_name.clone(),
            port: port.name.clone(),
            profile: SAFE_TRANSPORT_NAME.into(),
            enabled: true,
            settings: SerialSettings::default(),
            device_profile: None,
        });
        let selected_transport = if preserve_existing_transport {
            None
        } else {
            let default_transport = if transport_catalog
                .profiles
                .iter()
                .any(|profile| profile.name == slot.profile)
            {
                slot.profile.clone()
            } else {
                transport_catalog
                    .profiles
                    .first()
                    .map(|profile| profile.name.clone())
                    .unwrap_or_else(|| SAFE_TRANSPORT_NAME.into())
            };
            let transport_name = match args.transport.as_deref() {
                Some(name) => name.to_owned(),
                None if interactive => {
                    prompt_with_default("Transport Profile", &default_transport)?
                }
                None => default_transport,
            };
            Some(
                transport_catalog
                    .profiles
                    .iter()
                    .find(|profile| profile.name == transport_name)
                    .with_context(|| format!("unknown Transport Profile {transport_name:?}"))?,
            )
        };
        let default_device = slot.device_profile.as_deref().unwrap_or("Generic");
        let device_name = match args.device.as_deref() {
            Some(name) => name.to_owned(),
            None if args.generic => "Generic".into(),
            None if interactive => prompt_with_default("Device Profile", default_device)?,
            None => default_device.to_owned(),
        };
        let device_profile = if device_name.eq_ignore_ascii_case("generic") {
            None
        } else {
            device_catalog
                .profiles
                .iter()
                .find(|profile| profile.name == device_name)
                .with_context(|| format!("unknown Device Profile {device_name:?}"))?;
            Some(device_name)
        };

        slot.id = id;
        slot.display_name = display_name;
        slot.port = port.name.clone();
        if let Some(transport) = selected_transport {
            slot.profile = transport.name.clone();
            slot.settings = apply_transport_profile(&slot.settings, Some(transport));
        }
        slot.device_profile = device_profile;
        slots.push(slot);
    }

    let omitted_existing = unselected_existing_slots(&existing_slots, &slots);
    if !omitted_existing.is_empty() {
        if !args.json {
            println!("{}", tr("i.omitted.header"));
            for slot in &omitted_existing {
                println!(
                    "{}",
                    trf(
                        "i.omitted.note",
                        &[&safe_inline(&slot.display_name), &safe_inline(&slot.port)]
                    )
                );
            }
        }
        let delete = args.delete_omitted
            || (interactive && prompt_yes_no_default_no(tr("i.omitted.delete"))?);
        if delete {
            if !args.json {
                println!(
                    "{}",
                    trf("i.omitted.deleting", &[&omitted_existing.len().to_string()])
                );
            }
        } else {
            if !args.json {
                println!(
                    "{}",
                    trf("i.omitted.keeping", &[&omitted_existing.len().to_string()])
                );
            }
            slots.extend(omitted_existing);
        }
    }

    // Validate the lower-privilege daily credential before the only daemon
    // mutation. A bad token must not leave setup reporting failure after the
    // Slot configuration has already changed.
    let operator_token = match args.operator_token_file.as_deref() {
        Some(path) => config::read_token_if_present(path)?
            .with_context(|| format!("operator token file {} is empty", path.display()))?,
        None if interactive => {
            let operator_prompt = if existing_operator_token.is_some() {
                tr("i.operator.keep")
            } else {
                tr("i.operator.required.prompt")
            };
            let entered = rpassword::prompt_password(operator_prompt)?;
            let entered = entered.trim().to_owned();
            if entered.is_empty() {
                existing_operator_token.context(tr("i.operator.required"))?
            } else {
                entered
            }
        }
        None => existing_operator_token.context(
            "an existing daily token or --operator-token-file is required in non-interactive setup",
        )?,
    };
    let operator_api = ApiClient::new(endpoint.clone(), Some(operator_token.clone()))?;
    operator_api.status().await.context(tr("i.operator.fail"))?;
    let daily_role = ws::probe_role(&endpoint, &operator_token)
        .await
        .context(tr("i.role.fail"))?;
    if daily_role != serial_protocol::Role::Operator {
        bail!(trf("i.role.wrong", &[&format!("{daily_role:?}")]));
    }

    let configured = admin_api
        .configure_slots(slots, config_revision)
        .await
        .map_err(|error| {
            if crate::api::is_conflict(&error) {
                anyhow::anyhow!(
                    "configuration changed while setup was open; rerun setup to reload the current Slot list"
                )
            } else {
                error
            }
        })?;
    if !args.json {
        println!(
            "{}",
            trf("i.configured", &[&configured.slots.len().to_string()])
        );
        for slot in &configured.slots {
            println!(
                "  {} → {} ({:?})",
                safe_inline(&slot.config.display_name),
                safe_inline(&slot.config.port),
                slot.session_state
            );
        }
    }

    // Destroy every owner of the one-time setup credential before persisting
    // the already validated lower-privilege daily credential.
    drop(admin_api);
    drop(admin_token);
    drop(operator_api);

    config::write_token(&token_file, &operator_token)?;
    loaded.config.token_file = Some(token_file);
    loaded.config.endpoint = Some(endpoint.clone());
    loaded.config.last_slot = configured.slots.first().map(|slot| slot.config.id.clone());
    loaded.save()?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "config_path": loaded.path,
                "endpoint": endpoint,
                "config_revision": configured.config_revision,
                "slots": configured.slots,
                "operator_token_saved": true,
            }))?
        );
    } else {
        println!("{}", trf("i.saved", &[&loaded.path.display().to_string()]));
        println!("{}", tr("i.open.console"));
    }
    Ok(())
}

fn validate_parallel_values(label: &str, values: &[String], expected: usize) -> Result<()> {
    if !values.is_empty() && values.len() != expected {
        bail!(
            "{label} must be omitted or repeated exactly once per --port (expected {expected}, got {})",
            values.len()
        );
    }
    Ok(())
}

fn ensure_distinct_credential_paths(
    admin_input: Option<&Path>,
    operator_input: Option<&Path>,
    operator_destination: &Path,
) -> Result<()> {
    let mut paths = vec![(
        "--save-operator-token-file (or the saved daily token path)",
        operator_destination,
    )];
    if let Some(path) = admin_input {
        paths.push(("--admin-token-file", path));
    }
    if let Some(path) = operator_input {
        paths.push(("--operator-token-file", path));
    }

    let normalized = paths
        .iter()
        .map(|(label, path)| Ok((*label, credential_path_key(path)?)))
        .collect::<Result<Vec<_>>>()?;
    for left in 0..normalized.len() {
        for right in left + 1..normalized.len() {
            if normalized[left].1 == normalized[right].1 {
                bail!(
                    "{} and {} must be different files; setup never overwrites an input credential",
                    normalized[left].0,
                    normalized[right].0
                );
            }
        }
    }
    Ok(())
}

fn credential_path_key(path: &Path) -> Result<String> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("cannot resolve credential file path")?
            .join(path)
    };
    let canonical = std::fs::canonicalize(&absolute).unwrap_or_else(|_| {
        let mut normalized = PathBuf::new();
        for component in absolute.components() {
            match component {
                Component::CurDir => {}
                Component::ParentDir => {
                    normalized.pop();
                }
                other => normalized.push(other.as_os_str()),
            }
        }
        normalized
    });
    let key = canonical.to_string_lossy().to_string();
    Ok(if cfg!(windows) {
        key.to_ascii_lowercase()
    } else {
        key
    })
}

fn prompt_with_default(label: &str, default: &str) -> Result<String> {
    use std::io::Write;

    print!("{label} [{default}]: ");
    std::io::stdout().flush()?;
    let mut value = String::new();
    std::io::stdin().read_line(&mut value)?;
    let value = value.trim();
    Ok(if value.is_empty() {
        default.to_string()
    } else {
        value.to_string()
    })
}

fn prompt_yes_no_default_no(label: &str) -> Result<bool> {
    use std::io::Write;

    print!("{label} [y/N]: ");
    std::io::stdout().flush()?;
    let mut value = String::new();
    std::io::stdin().read_line(&mut value)?;
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "n" | "no" => Ok(false),
        "y" | "yes" => Ok(true),
        _ => bail!(tr("i.delete.confirm")),
    }
}

fn unselected_existing_slots(existing: &[SlotConfig], selected: &[SlotConfig]) -> Vec<SlotConfig> {
    existing
        .iter()
        .filter(|existing| {
            !selected
                .iter()
                .any(|selected| same_serial_port(&existing.port, &selected.port))
        })
        .cloned()
        .collect()
}

fn parse_selection(value: &str, port_count: usize) -> Result<Vec<usize>> {
    let mut selected = Vec::new();
    for item in value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let number: usize = item
            .parse()
            .with_context(|| trf("i.invalid.selection", &[item]))?;
        if number == 0 || number > port_count {
            bail!(trf(
                "i.selection.range",
                &[&number.to_string(), &port_count.to_string()]
            ));
        }
        let index = number - 1;
        if !selected.contains(&index) {
            selected.push(index);
        }
    }
    if selected.is_empty() {
        bail!(tr("i.selection.empty"));
    }
    Ok(selected)
}

fn normalize_slot_id(display_name: &str, fallback_index: usize) -> String {
    let normalized = display_name
        .trim()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    if normalized.is_empty() {
        format!("slot-{fallback_index}")
    } else {
        normalized
    }
}

fn same_serial_port(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    match (windows_com_name(left), windows_com_name(right)) {
        (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
        _ => false,
    }
}

fn windows_com_name(port: &str) -> Option<&str> {
    let port = port.strip_prefix(r"\\.\").unwrap_or(port);
    let bytes = port.as_bytes();
    (bytes.len() > 3
        && bytes[..3].eq_ignore_ascii_case(b"COM")
        && bytes[3..].iter().all(u8::is_ascii_digit))
    .then_some(port)
}

fn parse_log_limit(value: &str) -> Result<usize, String> {
    let limit = value
        .parse::<usize>()
        .map_err(|_| tr("m.limit.int").to_string())?;
    if (1..=10_000).contains(&limit) {
        Ok(limit)
    } else {
        Err(tr("m.limit.range").into())
    }
}

fn parse_doctor_duration(value: &str) -> Result<u64, String> {
    let value = value
        .parse::<u64>()
        .map_err(|_| "duration must be an integer number of seconds".to_string())?;
    if !(1..=60).contains(&value) {
        return Err("duration must be between 1 and 60 seconds".into());
    }
    Ok(value)
}

fn parse_lang(value: &str) -> Result<Lang, String> {
    Lang::parse(value).ok_or_else(|| format!("unknown language `{value}`; use en or zh"))
}

fn parse_rfc3339_ns(value: &str) -> Result<i64, String> {
    DateTime::parse_from_rfc3339(value)
        .map_err(|error| trf("m.time.invalid", &[value, &error.to_string()]))?
        .timestamp_nanos_opt()
        .ok_or_else(|| trf("m.time.range", &[value]))
}

fn parse_direction(value: &str) -> Result<Direction, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "rx" => Ok(Direction::Rx),
        "tx" => Ok(Direction::Tx),
        "none" => Ok(Direction::None),
        _ => Err(trf("m.direction.unknown", &[value])),
    }
}

fn format_byte_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn parse_event_kind(value: &str) -> Result<EventKind, String> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "rx" => Ok(EventKind::Rx),
        "tx" => Ok(EventKind::Tx),
        "serial_opening" => Ok(EventKind::SerialOpening),
        "serial_opened" => Ok(EventKind::SerialOpened),
        "serial_open_failed" => Ok(EventKind::SerialOpenFailed),
        "serial_closed" => Ok(EventKind::SerialClosed),
        "slot_reconfigured" => Ok(EventKind::SlotReconfigured),
        "slot_removed" => Ok(EventKind::SlotRemoved),
        "control_granted" => Ok(EventKind::ControlGranted),
        "control_released" => Ok(EventKind::ControlReleased),
        "control_revoked" => Ok(EventKind::ControlRevoked),
        "control_expired" => Ok(EventKind::ControlExpired),
        "run_started" => Ok(EventKind::RunStarted),
        "run_ended" => Ok(EventKind::RunEnded),
        "run_aborted" => Ok(EventKind::RunAborted),
        "trigger_started" => Ok(EventKind::TriggerStarted),
        "trigger_completed" => Ok(EventKind::TriggerCompleted),
        "trigger_cancelled" => Ok(EventKind::TriggerCancelled),
        "trigger_failed" => Ok(EventKind::TriggerFailed),
        "break" => Ok(EventKind::Break),
        "checkpoint" => Ok(EventKind::Checkpoint),
        "logging_degraded" => Ok(EventKind::LoggingDegraded),
        "gap" => Ok(EventKind::Gap),
        _ => Err(trf("m.kind.unknown", &[value])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configured_slot(id: &str, port: &str) -> SlotConfig {
        SlotConfig {
            id: id.into(),
            display_name: id.into(),
            port: port.into(),
            profile: "generic-115200".into(),
            enabled: true,
            settings: SerialSettings::default(),
            device_profile: None,
        }
    }

    #[test]
    fn selection_is_one_based_and_deduplicated() {
        assert_eq!(parse_selection("2, 1,2", 3).unwrap(), vec![1, 0]);
        assert!(parse_selection("0", 3).is_err());
        assert!(parse_selection("4", 3).is_err());
    }

    #[test]
    fn slot_ids_are_safe_and_stable() {
        assert_eq!(
            normalize_slot_id("Station A / Port 1", 1),
            "station-a---port-1"
        );
        assert_eq!(normalize_slot_id("串口一", 2), "slot-2");
    }

    #[test]
    fn windows_com_ports_match_without_case_but_unix_paths_do_not() {
        assert!(same_serial_port("COM12", "com12"));
        assert!(same_serial_port(r"\\.\COM12", r"\\.\com12"));
        assert!(same_serial_port(r"\\.\COM12", "com12"));
        assert!(!same_serial_port("/dev/ttyUSB0", "/dev/ttyusb0"));
    }

    #[test]
    fn log_event_kind_accepts_cli_and_protocol_spelling() {
        assert_eq!(
            parse_event_kind("serial-closed").unwrap(),
            EventKind::SerialClosed
        );
        assert_eq!(
            parse_event_kind("run_started").unwrap(),
            EventKind::RunStarted
        );
        assert_eq!(
            parse_event_kind("trigger-started").unwrap(),
            EventKind::TriggerStarted
        );
        assert_eq!(
            parse_event_kind("trigger_completed").unwrap(),
            EventKind::TriggerCompleted
        );
        assert_eq!(
            parse_event_kind("trigger-cancelled").unwrap(),
            EventKind::TriggerCancelled
        );
        assert_eq!(
            parse_event_kind("trigger_failed").unwrap(),
            EventKind::TriggerFailed
        );
        assert_eq!(parse_event_kind("break").unwrap(), EventKind::Break);
        assert!(parse_event_kind("not-an-event").is_err());
    }

    #[test]
    fn log_time_parser_requires_valid_rfc3339_and_preserves_nanoseconds() {
        assert_eq!(
            parse_rfc3339_ns("1970-01-01T08:00:01.123456789+08:00").unwrap(),
            1_123_456_789
        );
        assert!(parse_rfc3339_ns("2026-07-19 12:30:00").is_err());
        assert!(parse_rfc3339_ns("not-a-time").is_err());
    }

    #[test]
    fn log_direction_is_explicit_and_bounded() {
        assert_eq!(parse_direction("rx").unwrap(), Direction::Rx);
        assert_eq!(parse_direction("TX").unwrap(), Direction::Tx);
        assert_eq!(parse_direction("none").unwrap(), Direction::None);
        assert!(parse_direction("both").is_err());
    }

    #[test]
    fn archive_sizes_use_compact_binary_units() {
        assert_eq!(format_byte_size(999), "999 B");
        assert_eq!(format_byte_size(1536), "1.50 KiB");
        assert_eq!(format_byte_size(2 * 1024 * 1024), "2.00 MiB");
    }

    #[test]
    fn init_preserves_existing_slots_not_selected_in_the_current_scan() {
        let existing = vec![
            configured_slot("slot-1", "COM3"),
            configured_slot("slot-2", "COM4"),
        ];
        let selected = vec![configured_slot("slot-1", "com3")];

        let omitted = unselected_existing_slots(&existing, &selected);
        assert_eq!(omitted.len(), 1);
        assert_eq!(omitted[0].id, "slot-2");
    }

    #[test]
    fn initial_slot_is_scoped_to_the_interactive_console() {
        let interactive = Cli::try_parse_from(["serialctl", "--initial-slot", "slot-2"])
            .expect("interactive initial Slot should parse");
        assert_eq!(interactive.initial_slot.as_deref(), Some("slot-2"));
        assert!(validate_cli_scope(&interactive).is_ok());

        let status = Cli::try_parse_from(["serialctl", "--initial-slot", "slot-2", "status"])
            .expect("root options may syntactically precede a subcommand");
        assert!(validate_cli_scope(&status).is_err());

        let logs = Cli::try_parse_from(["serialctl", "logs", "--slot", "slot-1"])
            .expect("logs retains its own Slot filter");
        assert!(matches!(
            logs.command,
            Some(Command::Logs(LogsArgs { slot: Some(ref slot), .. })) if slot == "slot-1"
        ));
    }

    #[test]
    fn setup_keeps_init_as_a_compatible_alias() {
        for command in ["setup", "init"] {
            let parsed = Cli::try_parse_from([
                "serialctl",
                command,
                "--port",
                "COM3",
                "--admin-token-file",
                "admin.token",
                "--operator-token-file",
                "operator.token",
            ])
            .unwrap();
            assert!(matches!(parsed.command, Some(Command::Setup(_))));
        }
    }

    #[test]
    fn setup_never_overwrites_an_input_credential() {
        let same = Path::new("same-token-file");
        assert!(ensure_distinct_credential_paths(Some(same), None, same).is_err());
        assert!(ensure_distinct_credential_paths(None, Some(same), same).is_err());
        assert!(
            ensure_distinct_credential_paths(
                Some(Path::new("admin-token")),
                Some(Path::new("operator-input")),
                Path::new("operator-output"),
            )
            .is_ok()
        );
    }

    #[test]
    fn profile_and_tiered_doctor_commands_parse_without_implicit_io() {
        let profile = Cli::try_parse_from([
            "serialctl",
            "profile",
            "transport",
            "create",
            "--name",
            "uart-921600",
            "--baud-rate",
            "921600",
            "--json",
        ])
        .unwrap();
        assert!(matches!(profile.command, Some(Command::Profile(_))));

        let doctor = Cli::try_parse_from([
            "serialctl",
            "doctor",
            "stream",
            "--slot",
            "dut-1",
            "--duration",
            "5",
            "--json",
        ])
        .unwrap();
        assert!(matches!(
            doctor.command,
            Some(Command::Doctor(DoctorArgs {
                command: Some(DoctorCommand::Stream(DoctorStreamArgs { duration: 5, .. })),
                ..
            }))
        ));
        assert!(
            Cli::try_parse_from([
                "serialctl",
                "doctor",
                "stream",
                "--slot",
                "dut-1",
                "--duration",
                "0",
            ])
            .is_err()
        );
    }

    #[test]
    fn logs_accepts_one_bounded_text_filter() {
        assert!(Cli::try_parse_from(["serialctl", "logs", "--regex", r"panic|oops"]).is_ok());
        assert!(
            Cli::try_parse_from([
                "serialctl",
                "logs",
                "--contains",
                "panic",
                "--regex",
                "oops",
            ])
            .is_err()
        );
    }
}
