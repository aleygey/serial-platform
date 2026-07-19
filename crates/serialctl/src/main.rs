mod api;
mod config;
mod display;
mod tui;
mod ws;

use std::{io::IsTerminal, path::PathBuf, process::ExitCode};

use anyhow::{Context, Result, bail};
use chrono::DateTime;
use clap::{Args, Parser, Subcommand};
use serial_protocol::{Direction, EventKind, EventQuery, SerialSettings, SlotConfig};

use crate::{
    api::ApiClient,
    config::LoadedConfig,
    display::{format_event_plain, format_wall_time_local, safe_inline},
};

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:3210";

#[derive(Debug, Parser)]
#[command(
    name = "serialctl",
    version,
    about = "Human console and administration client for seriald"
)]
struct Cli {
    /// seriald base URL. The saved value is used when omitted.
    #[arg(long, global = true, env = "SERIALD_ENDPOINT")]
    endpoint: Option<String>,

    /// Read the bearer token from this file. Tokens are never accepted inline.
    #[arg(long, global = true, env = "SERIALD_TOKEN_FILE")]
    token_file: Option<PathBuf>,

    /// Override the serialctl configuration file.
    #[arg(long, global = true, env = "SERIALCTL_CONFIG")]
    config: Option<PathBuf>,

    /// Open this Slot initially in interactive mode.
    #[arg(long)]
    initial_slot: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Discover serial ports on the seriald host and configure Slots.
    Init,
    /// Print the daemon and Slot state.
    Status(OutputArgs),
    /// Diagnose the saved client connection and daemon state.
    Doctor(OutputArgs),
    /// List retained Slot/epoch journal archives.
    Archives(ArchivesArgs),
    /// Query durable serial timeline events.
    Logs(LogsArgs),
}

#[derive(Debug, Args)]
struct OutputArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ArchivesArgs {
    /// Only list archives for this Slot.
    #[arg(long)]
    slot: Option<String>,

    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct LogsArgs {
    /// Slot to query. Defaults to the last selected Slot.
    #[arg(long)]
    slot: Option<String>,

    /// Only return events whose decoded data contains this text.
    #[arg(long)]
    contains: Option<String>,

    /// Return events strictly after this sequence.
    #[arg(long)]
    after_seq: Option<u64>,

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

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("serialctl: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    validate_cli_scope(&cli)?;
    let loaded = LoadedConfig::load(cli.config.clone())?;

    if matches!(cli.command, Some(Command::Init)) {
        return run_init(loaded, cli.endpoint, cli.token_file).await;
    }

    let resolved = loaded.resolve(cli.endpoint, cli.token_file)?;
    let api = ApiClient::new(resolved.endpoint.clone(), resolved.token.clone())?;

    match cli.command {
        None => {
            if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
                bail!(
                    "interactive mode requires a terminal; use `serialctl status --json` or `serialctl logs --json`"
                );
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
        Some(Command::Archives(args)) => run_archives(&api, args).await,
        Some(Command::Logs(args)) => run_logs(&api, resolved.last_slot, args).await,
        Some(Command::Init) => unreachable!("handled before resolving configuration"),
    }
}

fn validate_cli_scope(cli: &Cli) -> Result<()> {
    if cli.initial_slot.is_some() && cli.command.is_some() {
        bail!("--initial-slot applies only to the interactive `serialctl` console");
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
        "seriald {}  epoch {}  {} Slot(s)",
        status.server_id,
        status.daemon_epoch,
        status.slots.len()
    );
    for slot in status.slots {
        let control = slot
            .control
            .as_ref()
            .map(|lease| lease.owner.label.as_str())
            .unwrap_or("-");
        println!(
            "{:<16} {:<10?} {:<8?} {:<8} {:>7} baud  control: {}",
            safe_inline(&slot.config.display_name),
            slot.session_state,
            slot.target_activity,
            safe_inline(&slot.config.port),
            slot.config.settings.baud_rate,
            safe_inline(control)
        );
        if let Some(reason) = slot.state_reason {
            println!("  reason: {}", safe_inline(&reason));
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
    uptime_ms: u64,
    slots: usize,
    online_slots: usize,
}

async fn run_doctor(
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
        uptime_ms: health.uptime_ms,
        slots: status.slots.len(),
        online_slots,
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("config       {}", report.config_path);
        println!("endpoint     {}", report.endpoint);
        println!(
            "token        {}",
            if report.token_configured {
                "configured"
            } else {
                "not configured"
            }
        );
        println!("daemon       {}", report.daemon_status);
        println!("server       {}", report.server_id);
        println!("epoch        {}", report.daemon_epoch);
        println!("uptime       {} ms", report.uptime_ms);
        println!(
            "slots        {} total, {} online",
            report.slots, report.online_slots
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
        println!("No retained serial archives found.");
    }
    for archive in response.archives {
        println!(
            "{:<16} {}  segment-open {} .. {}  seq {}..={}  {}  {} segment(s){}",
            safe_inline(&archive.slot_id),
            archive.epoch,
            format_wall_time_local(archive.first_segment_wall_time_ns),
            format_wall_time_local(archive.last_segment_wall_time_ns),
            archive.first_seq,
            archive.last_seq,
            format_byte_size(archive.total_bytes),
            archive.segment_count,
            if archive.has_open_segment {
                "  [open]"
            } else {
                ""
            }
        );
    }
    if response.truncated {
        eprintln!(
            "archive catalog is incomplete because its bounded scan skipped unreadable entries or reached the response limit"
        );
    }
    Ok(())
}

async fn run_logs(api: &ApiClient, last_slot: Option<String>, args: LogsArgs) -> Result<()> {
    if let (Some(after), Some(before)) = (args.after_time, args.before_time)
        && after >= before
    {
        bail!("--after-time must be earlier than --before-time");
    }
    let query_spans_epoch = args.run.is_none()
        && args.operation.is_none()
        && args.after_seq.is_none()
        && args.after_time.is_none()
        && args.before_time.is_none();
    if query_spans_epoch && !args.json {
        eprintln!(
            "warning: this query spans the entire selected daemon epoch and may include older test cycles; --contains only filters that global range, so narrow it with --run, --operation, --after-seq, or --after-time/--before-time"
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
            .context("no Slot is configured; run `serialctl init`")?,
    };
    let response = api
        .events(
            &slot_id,
            &EventQuery {
                epoch: args.epoch,
                after_seq: args.after_seq,
                before_wall_time_ns: args.before_time,
                after_wall_time_ns: args.after_time,
                direction: args.direction,
                kind: args.kind,
                actor_id: args.actor,
                run_id: args.run,
                operation_id: args.operation,
                contains: args.contains,
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
                "results truncated; repeat the same filters with --epoch {} --after-seq {}",
                cursor.epoch, cursor.after_seq
            );
        } else {
            eprintln!("results truncated without a continuation cursor");
        }
    }
    for gap in response.gaps {
        eprintln!(
            "gap {}..={} ({:?}, epoch {})",
            gap.first_seq, gap.last_seq, gap.reason, gap.epoch
        );
    }
    Ok(())
}

async fn run_init(
    mut loaded: LoadedConfig,
    endpoint_override: Option<String>,
    token_file_override: Option<PathBuf>,
) -> Result<()> {
    ensure_interactive()?;

    let saved_endpoint = loaded
        .config
        .endpoint
        .clone()
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
    let endpoint =
        endpoint_override.unwrap_or(prompt_with_default("seriald endpoint", &saved_endpoint)?);

    let token_file = token_file_override
        .or_else(|| loaded.config.token_file.clone())
        .unwrap_or_else(|| loaded.default_token_path());
    let existing_operator_token = config::read_token_if_present(&token_file)?;
    if existing_operator_token.is_some() {
        println!(
            "The saved token is treated as the daily operator token; setup still requires a separate admin token."
        );
    }
    let admin_token = rpassword::prompt_password(
        "seriald admin bearer token (required for setup; never saved): ",
    )?;
    let admin_token = admin_token.trim().to_string();
    if admin_token.is_empty() {
        bail!(
            "an admin bearer token is required; seriald v1 does not support disabled authentication"
        );
    }

    let admin_api = ApiClient::new(endpoint.clone(), Some(admin_token.clone()))?;
    let health = admin_api.health().await.context(
        "cannot reach seriald; start seriald on Windows and verify the host-only endpoint",
    )?;
    let current = admin_api
        .status()
        .await
        .context("cannot read existing Slot configuration; verify the admin token")?;
    let existing_slots = current
        .slots
        .into_iter()
        .map(|slot| slot.config)
        .collect::<Vec<_>>();
    println!(
        "Connected to seriald {} (epoch {}).",
        health.server_id, health.daemon_epoch
    );

    let ports = admin_api.ports().await?;
    if ports.is_empty() {
        bail!("seriald found no serial ports on its host");
    }
    println!("\nSerial ports discovered on the seriald host:");
    for (index, port) in ports.iter().enumerate() {
        let detail = port
            .product
            .as_deref()
            .or(port.manufacturer.as_deref())
            .unwrap_or(&port.port_type);
        println!(
            "  {:>2}. {:<10} {}",
            index + 1,
            safe_inline(&port.name),
            safe_inline(detail)
        );
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
    let selection = prompt_with_default(
        "Select ports for the complete Slot set (comma-separated numbers)",
        &default_selection,
    )?;
    let selected = parse_selection(&selection, ports.len())?;

    println!(
        "\nNew ports use: 115200 8N1, no flow control, DTR/RTS low, TX EOL \\r, echo on, U-Boot prompt `SigmaStar #`, probe disabled, auto-open."
    );
    if !existing_slots.is_empty() {
        println!("Previously configured ports keep their Profile and serial settings.");
    }
    let mut slots = Vec::with_capacity(selected.len());
    for (slot_index, port_index) in selected.into_iter().enumerate() {
        let port = &ports[port_index];
        let existing = existing_slots
            .iter()
            .find(|slot| same_serial_port(&slot.port, &port.name))
            .cloned();
        let default_name = existing
            .as_ref()
            .map(|slot| slot.display_name.clone())
            .unwrap_or_else(|| format!("slot-{}", slot_index + 1));
        let display_name = prompt_with_default(
            &format!("Slot name for {}", safe_inline(&port.name)),
            &default_name,
        )?;
        let default_id = existing
            .as_ref()
            .map(|slot| slot.id.clone())
            .unwrap_or_else(|| normalize_slot_id(&display_name, slot_index + 1));
        let entered_id = prompt_with_default(
            &format!("Slot ID for {}", safe_inline(&port.name)),
            &default_id,
        )?;
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
            profile: "generic-115200".into(),
            enabled: true,
            settings: SerialSettings::default(),
        });
        slot.id = id;
        slot.display_name = display_name;
        slot.port = port.name.clone();
        slots.push(slot);
    }

    let omitted_existing = unselected_existing_slots(&existing_slots, &slots);
    if !omitted_existing.is_empty() {
        println!("\nExisting Slots not selected in this scan:");
        for slot in &omitted_existing {
            println!(
                "  {} → {} (kept by default, including when the COM port is temporarily absent)",
                safe_inline(&slot.display_name),
                safe_inline(&slot.port)
            );
        }
        let delete = prompt_yes_no_default_no(
            "Explicitly delete these omitted Slots from seriald configuration?",
        )?;
        if delete {
            println!(
                "Deleting {} explicitly omitted Slot(s).",
                omitted_existing.len()
            );
        } else {
            println!("Keeping {} existing Slot(s).", omitted_existing.len());
            slots.extend(omitted_existing);
        }
    }

    let configured = admin_api.configure_slots(slots).await?;
    println!("\nConfigured {} Slot(s):", configured.slots.len());
    for slot in &configured.slots {
        println!(
            "  {} → {} ({:?})",
            safe_inline(&slot.config.display_name),
            safe_inline(&slot.config.port),
            slot.session_state
        );
    }

    // Destroy every owner of the setup credential before asking for the
    // lower-privilege daily credential. The admin token is never persisted.
    drop(admin_api);
    drop(admin_token);

    let operator_prompt = if existing_operator_token.is_some() {
        "seriald operator bearer token for daily use (leave empty to keep the saved token): "
    } else {
        "seriald operator bearer token for daily use (required; saved locally): "
    };
    let entered_operator_token = rpassword::prompt_password(operator_prompt)?;
    let entered_operator_token = entered_operator_token.trim().to_string();
    let operator_token = if entered_operator_token.is_empty() {
        existing_operator_token.context(
            "an operator bearer token is required for the daily console; the admin token is not saved",
        )?
    } else {
        entered_operator_token
    };
    let operator_api = ApiClient::new(endpoint.clone(), Some(operator_token.clone()))?;
    operator_api.status().await.context(
        "the operator token could not read daemon status; the token file was not changed",
    )?;
    let daily_role = ws::probe_role(&endpoint, &operator_token)
        .await
        .context("the daily token role could not be verified; the token file was not changed")?;
    if daily_role != serial_protocol::Role::Operator {
        bail!(
            "the daily token has role {daily_role:?}; an operator token is required and the token file was not changed"
        );
    }

    config::write_token(&token_file, &operator_token)?;
    loaded.config.token_file = Some(token_file);
    loaded.config.endpoint = Some(endpoint);
    loaded.config.last_slot = configured.slots.first().map(|slot| slot.config.id.clone());
    loaded.save()?;
    println!(
        "Saved serialctl configuration to {}.",
        loaded.path.display()
    );
    println!("Run `serialctl` to open the multi-Slot console.");
    Ok(())
}

fn ensure_interactive() -> Result<()> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        bail!("this command requires an interactive terminal");
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
        _ => bail!("enter `y` to delete the omitted Slots or `n` to keep them"),
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
            .with_context(|| format!("invalid port selection `{item}`"))?;
        if number == 0 || number > port_count {
            bail!("port selection {number} is outside 1..={port_count}");
        }
        let index = number - 1;
        if !selected.contains(&index) {
            selected.push(index);
        }
    }
    if selected.is_empty() {
        bail!("select at least one serial port");
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
        .map_err(|_| "limit must be a positive integer".to_string())?;
    if (1..=10_000).contains(&limit) {
        Ok(limit)
    } else {
        Err("limit must be between 1 and 10000".into())
    }
}

fn parse_rfc3339_ns(value: &str) -> Result<i64, String> {
    DateTime::parse_from_rfc3339(value)
        .map_err(|error| {
            format!(
                "invalid RFC3339 timestamp `{value}`: {error}; include a timezone, for example 2026-07-19T12:30:00+08:00"
            )
        })?
        .timestamp_nanos_opt()
        .ok_or_else(|| format!("RFC3339 timestamp `{value}` is outside the nanosecond range"))
}

fn parse_direction(value: &str) -> Result<Direction, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "rx" => Ok(Direction::Rx),
        "tx" => Ok(Direction::Tx),
        "none" => Ok(Direction::None),
        _ => Err(format!("unknown direction `{value}`; use rx, tx, or none")),
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
        "checkpoint" => Ok(EventKind::Checkpoint),
        "logging_degraded" => Ok(EventKind::LoggingDegraded),
        "gap" => Ok(EventKind::Gap),
        _ => Err(format!(
            "unknown event kind `{value}`; use rx, tx, serial-opened, serial-closed, run-started, checkpoint, or another protocol event kind"
        )),
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
}
