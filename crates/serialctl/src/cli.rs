use std::{io::IsTerminal, path::PathBuf};

use anyhow::{Context, Result, bail};
use chrono::DateTime;
use clap::{Args, Parser, Subcommand};
use serial_protocol::{Direction, EventKind, EventQuery, PROTOCOL_VERSION, SlotConfig};

use crate::{
    api::ApiClient,
    config::{self, LoadedConfig},
    display::{
        format_event_plain, format_wall_time_local, gap_reason_label, pad_display, safe_inline,
        session_state_label, target_activity_label, trigger_status_label,
    },
    i18n::{self, Lang, tr, trf},
    profile, tui,
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

    /// Override the serialctl configuration file.
    #[arg(long, global = true, env = "SERIALCTL_CONFIG")]
    config: Option<PathBuf>,

    /// Override the UI language (en or zh). The saved value is used when omitted.
    #[arg(long, global = true, env = "SERIALCTL_LANG", value_parser = parse_lang)]
    lang: Option<Lang>,

    /// Open this serial port initially in interactive mode.
    #[arg(long)]
    initial_port: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Discover and configure serial ports.
    Setup(SetupArgs),
    /// Manage reusable Serial and Model Profiles.
    Profile(profile::ProfileArgs),
    /// Print the daemon and serial-port state.
    Status(OutputArgs),
    /// Diagnose the saved client connection, ports, stream, storage, or port state.
    Doctor(DoctorArgs),
    /// List retained port/epoch journal archives.
    Archives(ArchivesArgs),
    /// Query durable serial timeline events.
    Logs(LogsArgs),
}

#[derive(Debug, Clone, Args)]
pub struct SetupArgs {
    /// Serial port on the seriald host. Repeat for multiple ports.
    #[arg(long)]
    port: Vec<String>,

    /// Transport Profile applied to every selected port.
    #[arg(long)]
    transport: Option<String>,

    /// Model Profile applied to every selected port.
    #[arg(long)]
    model: Option<String>,

    /// Remove existing configured ports that were not selected.
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
    /// Verify client configuration, HTTP reachability and daemon identity.
    Connection(OutputArgs),
    /// Explain whether one configured serial endpoint can be opened.
    Port(DoctorSlotArgs),
    /// Compare an independent live subscription with authoritative offsets and the journal.
    Stream(DoctorStreamArgs),
    /// Report retained journal usage and logging health.
    Storage(OutputArgs),
    /// Print the authoritative port state, control, Run, Trigger and effective settings.
    State(DoctorSlotArgs),
}

#[derive(Debug, Args)]
pub struct DoctorSlotArgs {
    #[arg(long)]
    pub port: String,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct DoctorStreamArgs {
    #[arg(long)]
    pub port: String,
    /// Observation duration in seconds (1..=60).
    #[arg(long, default_value_t = 10, value_parser = parse_doctor_duration)]
    pub duration: u64,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ArchivesArgs {
    /// Only list archives for this port.
    #[arg(long)]
    port: Option<String>,

    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
pub struct LogsArgs {
    /// Port to query. Defaults to the last selected port.
    #[arg(long)]
    port: Option<String>,

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
        return run_setup(loaded, cli.endpoint, args.clone()).await;
    }

    let resolved = loaded.resolve(cli.endpoint)?;
    let api = ApiClient::new(resolved.endpoint.clone())?;

    match cli.command {
        None => {
            if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
                bail!(tr("m.terminal.required"));
            }
            tui::run(
                api,
                loaded,
                cli.initial_port.or(resolved.last_port),
                resolved.endpoint,
            )
            .await
        }
        Some(Command::Status(args)) => run_status(&api, args).await,
        Some(Command::Doctor(args)) => run_doctor(&api, &loaded, &resolved, args).await,
        Some(Command::Profile(args)) => profile::run(&api, args).await,
        Some(Command::Archives(args)) => run_archives(&api, args).await,
        Some(Command::Logs(args)) => run_logs(&api, resolved.last_port, args).await,
        Some(Command::Setup(_)) => unreachable!("handled before resolving configuration"),
    }
}

fn validate_cli_scope(cli: &Cli) -> Result<()> {
    if cli.initial_port.is_some() && cli.command.is_some() {
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
                &status.ports.len().to_string(),
            ]
        )
    );
    for port in status.ports {
        let baud_rate = port
            .effective_transport
            .map(|transport| transport.baud_rate)
            .unwrap_or(115_200);
        let control = port
            .control
            .as_ref()
            .map(|lease| lease.owner.label.as_str())
            .unwrap_or("-");
        println!(
            "{} {:<10} {:<8} {} {:>7} baud  {}",
            pad_display(&safe_inline(&port.config.port), 16),
            session_state_label(port.session_state),
            target_activity_label(port.target_activity),
            pad_display(&safe_inline(&port.config.port), 8),
            baud_rate,
            trf("m.status.control", &[&safe_inline(control)])
        );
        if let Some(reason) = port.state_reason {
            println!("{}", trf("m.status.reason", &[&safe_inline(&reason)]));
        }
        if let Some(trigger) = port.active_trigger {
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
    daemon_status: &'a str,
    server_id: String,
    daemon_epoch: String,
    protocol_version: u16,
    protocol_compatible: bool,
    uptime_ms: u64,
    ports: usize,
    online_ports: usize,
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
    let online_ports = status
        .ports
        .iter()
        .filter(|slot| slot.session_state == serial_protocol::SessionState::Online)
        .count();
    let report = DoctorReport {
        config_path: loaded.path.display().to_string(),
        endpoint: &resolved.endpoint,
        daemon_status: &health.status,
        server_id: health.server_id.to_string(),
        daemon_epoch: health.daemon_epoch.to_string(),
        protocol_version: health.protocol_version,
        protocol_compatible: health.protocol_version == PROTOCOL_VERSION
            && status.protocol_version == PROTOCOL_VERSION,
        uptime_ms: health.uptime_ms,
        ports: status.ports.len(),
        online_ports,
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
            pad_display(tr("m.doctor.ports"), 12),
            trf(
                "m.doctor.ports.value",
                &[&report.ports.to_string(), &report.online_ports.to_string()]
            )
        );
    }
    Ok(())
}

async fn run_archives(api: &ApiClient, args: ArchivesArgs) -> Result<()> {
    let response = api.archives(args.port.as_deref()).await?;
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
                    &pad_display(&safe_inline(&archive.port), 16),
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

async fn run_logs(api: &ApiClient, last_port: Option<String>, args: LogsArgs) -> Result<()> {
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
             {PROTOCOL_VERSION}; use matching-version components or --contains"
        );
    }
    let port = match args.port.or(last_port) {
        Some(port) => port,
        None => api
            .status()
            .await?
            .ports
            .into_iter()
            .next()
            .map(|port| port.config.port)
            .context(tr("st.no.port"))?,
    };
    let response = api
        .events(
            &port,
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
                    gap_reason_label(gap.reason),
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
    args: SetupArgs,
) -> Result<()> {
    let interactive =
        std::io::stdin().is_terminal() && std::io::stdout().is_terminal() && !args.json;
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

    let api = ApiClient::new(endpoint.clone())?;
    let health = api.health().await.context(tr("i.unreachable"))?;
    if health.protocol_version != PROTOCOL_VERSION {
        bail!(
            "seriald protocol {} is incompatible with this client (expected {})",
            health.protocol_version,
            PROTOCOL_VERSION
        );
    }
    let current = api
        .configuration_status()
        .await
        .context(tr("i.status.fail"))?;
    let existing_ports = current
        .ports
        .into_iter()
        .map(|port| port.config)
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

    let discovered = api.ports().await?;
    if discovered.is_empty() {
        bail!(tr("i.no.ports"));
    }
    if !args.json {
        println!("{}", tr("i.ports.header"));
        for (index, port) in discovered.iter().enumerate() {
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

    let existing_selection = discovered
        .iter()
        .enumerate()
        .filter(|(_, port)| {
            existing_ports
                .iter()
                .any(|configured| same_serial_port(&configured.port, &port.name))
        })
        .map(|(index, _)| (index + 1).to_string())
        .collect::<Vec<_>>();
    let default_selection = if existing_selection.is_empty() {
        "1".to_string()
    } else {
        existing_selection.join(",")
    };
    let selected = if args.port.is_empty() {
        if !interactive {
            bail!("at least one --port is required in non-interactive setup");
        }
        let selection = prompt_with_default(tr("i.select.ports"), &default_selection)?;
        parse_selection(&selection, discovered.len())?
    } else {
        args.port
            .iter()
            .map(|requested| {
                discovered
                    .iter()
                    .position(|port| same_serial_port(&port.name, requested))
                    .with_context(|| {
                        format!("serial port {requested:?} was not discovered on the seriald host")
                    })
            })
            .collect::<Result<Vec<_>>>()?
    };

    let (transport_catalog, _) = profile::load_transport_catalog(&api).await?;
    let model_catalog = profile::load_model_catalog(&api).await?;
    if !args.json {
        println!("串口 Profile：配置波特率、数据位、校验位、停止位和流控");
        println!("机型 Profile：配置机型名、Shell/U-Boot 提示符、换行和发送节奏");
    }

    let mut configured_ports = Vec::with_capacity(selected.len());
    for port_index in selected {
        let discovered_port = &discovered[port_index];
        let existing = existing_ports
            .iter()
            .find(|configured| same_serial_port(&configured.port, &discovered_port.name));

        let default_transport = existing.and_then(|port| port.transport_profile.clone());
        let transport_profile = match args.transport.as_deref() {
            Some(name) => Some(name.to_owned()),
            None if interactive => {
                let default = default_transport.as_deref().unwrap_or("none");
                let chosen =
                    prompt_with_default("串口 Profile（none 表示默认 115200 8N1）", default)?;
                (!chosen.eq_ignore_ascii_case("none")).then_some(chosen)
            }
            None => default_transport,
        };
        if let Some(name) = transport_profile.as_deref() {
            transport_catalog
                .profiles
                .iter()
                .find(|profile| profile.name == name)
                .with_context(|| format!("unknown serial Profile {name:?}"))?;
        }

        let default_model = existing.and_then(|port| port.model_profile.clone());
        let model_profile = match args.model.as_deref() {
            Some(name) => Some(name.to_owned()),
            None if interactive => {
                let default = default_model.as_deref().unwrap_or("none");
                let chosen = prompt_with_default("机型 Profile（none 表示不绑定）", default)?;
                (!chosen.eq_ignore_ascii_case("none")).then_some(chosen)
            }
            None => default_model,
        };
        if let Some(name) = model_profile.as_deref() {
            model_catalog
                .profiles
                .iter()
                .find(|profile| profile.name == name)
                .with_context(|| format!("unknown model Profile {name:?}"))?;
        }

        configured_ports.push(SlotConfig {
            port: discovered_port.name.clone(),
            transport_profile,
            model_profile,
            enabled: existing.is_none_or(|port| port.enabled),
        });
    }

    let omitted = unselected_existing_ports(&existing_ports, &configured_ports);
    if !omitted.is_empty() {
        if !args.json {
            println!("{}", tr("i.omitted.header"));
            for port in &omitted {
                println!("  {}", safe_inline(&port.port));
            }
        }
        let delete = args.delete_omitted
            || (interactive && prompt_yes_no_default_no(tr("i.omitted.delete"))?);
        if !delete {
            configured_ports.extend(omitted);
        }
    }

    let configured = api
        .configure_ports(configured_ports, current.config_revision)
        .await
        .map_err(|error| {
            if crate::api::is_conflict(&error) {
                anyhow::anyhow!(
                    "configuration changed while setup was open; rerun setup to reload the current port list"
                )
            } else {
                error
            }
        })?;

    loaded.config.endpoint = Some(endpoint.clone());
    loaded.config.last_port = configured
        .ports
        .first()
        .map(|port| port.config.port.clone());
    loaded.save()?;

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "config_path": loaded.path,
                "endpoint": endpoint,
                "config_revision": configured.config_revision,
                "ports": configured.ports,
            }))?
        );
    } else {
        println!(
            "{}",
            trf("i.configured", &[&configured.ports.len().to_string()])
        );
        for port in &configured.ports {
            println!(
                "  {} ({})",
                safe_inline(&port.config.port),
                session_state_label(port.session_state)
            );
        }
        println!("{}", trf("i.saved", &[&loaded.path.display().to_string()]));
        println!("{}", tr("i.open.console"));
    }
    Ok(())
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

fn unselected_existing_ports(existing: &[SlotConfig], selected: &[SlotConfig]) -> Vec<SlotConfig> {
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
        "port_reconfigured" => Ok(EventKind::PortReconfigured),
        "port_removed" => Ok(EventKind::PortRemoved),
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

    fn configured_port(port: &str) -> SlotConfig {
        SlotConfig {
            port: port.into(),
            transport_profile: None,
            model_profile: None,
            enabled: true,
        }
    }

    #[test]
    fn selection_is_one_based_and_deduplicated() {
        assert_eq!(parse_selection("2, 1,2", 3).unwrap(), vec![1, 0]);
        assert!(parse_selection("0", 3).is_err());
        assert!(parse_selection("4", 3).is_err());
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
    fn setup_preserves_existing_ports_not_selected_in_the_current_scan() {
        let existing = vec![configured_port("COM3"), configured_port("COM4")];
        let selected = vec![configured_port("com3")];

        let omitted = unselected_existing_ports(&existing, &selected);
        assert_eq!(omitted.len(), 1);
        assert_eq!(omitted[0].port, "COM4");
    }

    #[test]
    fn initial_port_is_scoped_to_the_interactive_console() {
        let interactive = Cli::try_parse_from(["serialctl", "--initial-port", "COM4"])
            .expect("interactive initial port should parse");
        assert_eq!(interactive.initial_port.as_deref(), Some("COM4"));
        assert!(validate_cli_scope(&interactive).is_ok());

        let status = Cli::try_parse_from(["serialctl", "--initial-port", "COM4", "status"])
            .expect("root options may syntactically precede a subcommand");
        assert!(validate_cli_scope(&status).is_err());

        let logs = Cli::try_parse_from(["serialctl", "logs", "--port", "COM3"])
            .expect("logs retains its own port filter");
        assert!(matches!(
            logs.command,
            Some(Command::Logs(LogsArgs { port: Some(ref port), .. })) if port == "COM3"
        ));
    }

    #[test]
    fn setup_is_the_single_configuration_command() {
        let parsed = Cli::try_parse_from(["serialctl", "setup", "--port", "COM3"]).unwrap();
        assert!(matches!(parsed.command, Some(Command::Setup(_))));
        assert!(Cli::try_parse_from(["serialctl", "init", "--port", "COM3"]).is_err());
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
            "--port",
            "COM4",
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
                "--port",
                "COM4",
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
