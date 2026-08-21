use std::{
    fs,
    io::{self, IsTerminal as _, Write as _},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serial_protocol::{
    DataBits, EchoMode, FlowControl, ModelProfile, Parity, SlotConfig, StopBits, TransportProfile,
};

use crate::{
    api::{ApiClient, ProfileCatalog, is_conflict},
    display::safe_inline,
};

pub const SAFE_TRANSPORT_NAME: &str = "generic-115200";

#[cfg(test)]
fn safe_transport_profile() -> TransportProfile {
    TransportProfile {
        name: SAFE_TRANSPORT_NAME.into(),
        baud_rate: 115_200,
        data_bits: DataBits::Eight,
        parity: Parity::None,
        stop_bits: StopBits::One,
        flow_control: FlowControl::None,
        dtr: false,
        rts: false,
        auto_open: true,
    }
}

#[derive(Debug, Args)]
pub struct ProfileArgs {
    #[command(subcommand)]
    command: ProfileCommand,
}

#[derive(Debug, Subcommand)]
enum ProfileCommand {
    /// Manage reusable physical UART settings.
    Transport {
        #[command(subcommand)]
        command: TransportCommand,
    },
    /// Manage reusable DUT shell/U-Boot behavior and write pacing.
    Model {
        #[command(subcommand)]
        command: ModelCommand,
    },
    /// Attach one or both profiles to a serial port.
    Attach(ProfileAttachArgs),
    /// Remove one or both profile bindings from a serial port.
    Detach(ProfileDetachArgs),
}

#[derive(Debug, Subcommand)]
enum TransportCommand {
    /// List available UART transport profiles.
    List(OutputArgs),
    /// Show one UART transport profile.
    Show(ShowArgs),
    /// Create a UART transport profile from explicit values or prompts.
    Create(TransportCreateArgs),
    /// Update selected fields of an existing UART transport profile.
    Update(TransportUpdateArgs),
    /// Copy a UART transport profile and optionally override fields.
    Clone(TransportCloneArgs),
    /// Import one profile or a profile catalog from TOML/JSON.
    Import(ImportArgs),
    /// Export one profile as TOML or JSON.
    Export(ExportArgs),
    /// Delete an unused UART transport profile.
    Delete(DeleteArgs),
}

#[derive(Debug, Subcommand)]
enum ModelCommand {
    /// List available DUT behavior profiles. Generic means unbound.
    List(OutputArgs),
    /// Show one DUT behavior profile.
    Show(ShowArgs),
    /// Create a DUT behavior profile from explicit values or prompts.
    Create(ModelCreateArgs),
    /// Update selected fields of an existing DUT behavior profile.
    Update(ModelUpdateArgs),
    /// Copy a DUT behavior profile and optionally override fields.
    Clone(ModelCloneArgs),
    /// Import one profile or a profile catalog from TOML/JSON.
    Import(ImportArgs),
    /// Export one profile as TOML or JSON.
    Export(ExportArgs),
    /// Delete an unused DUT behavior profile.
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
struct OutputArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct ShowArgs {
    /// Exact profile name.
    name: String,
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct ImportArgs {
    /// TOML or JSON file containing one profile or a `profiles` catalog.
    file: PathBuf,
    /// Replace an existing profile with the same name.
    #[arg(long)]
    replace: bool,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ExportFormat {
    Toml,
    Json,
}

#[derive(Debug, Clone, Args)]
struct ExportArgs {
    /// Exact profile name.
    name: String,
    /// Destination path, or `-` for stdout.
    #[arg(long, default_value = "-")]
    output: PathBuf,
    #[arg(long, value_enum)]
    format: Option<ExportFormat>,
    /// Replace an existing destination file.
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Clone, Args)]
struct DeleteArgs {
    /// Exact profile name.
    name: String,
    /// Required in non-interactive use.
    #[arg(long)]
    yes: bool,
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DataBitsArg {
    Five,
    Six,
    Seven,
    Eight,
}

impl From<DataBitsArg> for DataBits {
    fn from(value: DataBitsArg) -> Self {
        match value {
            DataBitsArg::Five => Self::Five,
            DataBitsArg::Six => Self::Six,
            DataBitsArg::Seven => Self::Seven,
            DataBitsArg::Eight => Self::Eight,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ParityArg {
    None,
    Odd,
    Even,
}

impl From<ParityArg> for Parity {
    fn from(value: ParityArg) -> Self {
        match value {
            ParityArg::None => Self::None,
            ParityArg::Odd => Self::Odd,
            ParityArg::Even => Self::Even,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum StopBitsArg {
    One,
    Two,
}

impl From<StopBitsArg> for StopBits {
    fn from(value: StopBitsArg) -> Self {
        match value {
            StopBitsArg::One => Self::One,
            StopBitsArg::Two => Self::Two,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum FlowControlArg {
    None,
    Software,
    Hardware,
}

impl From<FlowControlArg> for FlowControl {
    fn from(value: FlowControlArg) -> Self {
        match value {
            FlowControlArg::None => Self::None,
            FlowControlArg::Software => Self::Software,
            FlowControlArg::Hardware => Self::Hardware,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum EchoArg {
    On,
    Off,
    Auto,
}

impl From<EchoArg> for EchoMode {
    fn from(value: EchoArg) -> Self {
        match value {
            EchoArg::On => Self::On,
            EchoArg::Off => Self::Off,
            EchoArg::Auto => Self::Auto,
        }
    }
}

#[derive(Debug, Clone, Args)]
struct TransportCreateArgs {
    /// Unique profile name.
    #[arg(long)]
    name: Option<String>,
    /// UART baud rate.
    #[arg(long, default_value_t = 115_200)]
    baud_rate: u32,
    /// UART data bits.
    #[arg(long, value_enum, default_value_t = DataBitsArg::Eight)]
    data_bits: DataBitsArg,
    /// UART parity.
    #[arg(long, value_enum, default_value_t = ParityArg::None)]
    parity: ParityArg,
    /// UART stop bits.
    #[arg(long, value_enum, default_value_t = StopBitsArg::One)]
    stop_bits: StopBitsArg,
    /// UART flow control.
    #[arg(long, value_enum, default_value_t = FlowControlArg::None)]
    flow_control: FlowControlArg,
    /// Assert DTR while the port is open.
    #[arg(long)]
    dtr: bool,
    /// Assert RTS while the port is open.
    #[arg(long)]
    rts: bool,
    /// Keep the configured port open and reconnect after disconnects.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    auto_open: bool,
    /// Prompt for omitted values. A TTY already prompts for the required name.
    #[arg(long, conflicts_with = "json")]
    interactive: bool,
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct TransportCloneArgs {
    source: String,
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    baud_rate: Option<u32>,
    #[arg(long, value_enum)]
    data_bits: Option<DataBitsArg>,
    #[arg(long, value_enum)]
    parity: Option<ParityArg>,
    #[arg(long, value_enum)]
    stop_bits: Option<StopBitsArg>,
    #[arg(long, value_enum)]
    flow_control: Option<FlowControlArg>,
    #[arg(long)]
    dtr: Option<bool>,
    #[arg(long)]
    rts: Option<bool>,
    #[arg(long)]
    auto_open: Option<bool>,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct TransportUpdateArgs {
    /// Exact profile name. Profile identity cannot be renamed in place.
    name: String,
    #[arg(long)]
    baud_rate: Option<u32>,
    #[arg(long, value_enum)]
    data_bits: Option<DataBitsArg>,
    #[arg(long, value_enum)]
    parity: Option<ParityArg>,
    #[arg(long, value_enum)]
    stop_bits: Option<StopBitsArg>,
    #[arg(long, value_enum)]
    flow_control: Option<FlowControlArg>,
    /// Set whether DTR is asserted while the port is open.
    #[arg(long)]
    dtr: Option<bool>,
    /// Set whether RTS is asserted while the port is open.
    #[arg(long)]
    rts: Option<bool>,
    /// Set whether seriald keeps the configured port open.
    #[arg(long)]
    auto_open: Option<bool>,
    /// Prompt for every field, using the current profile as defaults.
    #[arg(long, conflicts_with = "json")]
    interactive: bool,
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct ModelCreateArgs {
    /// Unique DUT profile name.
    #[arg(long)]
    name: Option<String>,
    /// Shell prompt used to delimit completed commands.
    #[arg(long)]
    shell_prompt: Option<String>,
    /// U-Boot prompt used to delimit completed commands.
    #[arg(long)]
    uboot_prompt: Option<String>,
    /// `none`, `cr`, `lf`, or `crlf`.
    #[arg(long, value_parser = parse_eol)]
    write_eol: Option<String>,
    #[arg(long, value_enum)]
    echo: Option<EchoArg>,
    /// Maximum bytes sent in each paced write chunk.
    #[arg(long, value_parser = parse_positive_u32)]
    write_chunk_size: Option<u32>,
    /// Delay between paced write chunks.
    #[arg(long)]
    write_chunk_delay_ms: Option<u64>,
    /// Prompt for all model behavior fields.
    #[arg(long, conflicts_with = "json")]
    interactive: bool,
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct ModelCloneArgs {
    source: String,
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    shell_prompt: Option<String>,
    #[arg(long)]
    uboot_prompt: Option<String>,
    #[arg(long, value_parser = parse_eol)]
    write_eol: Option<String>,
    #[arg(long, value_enum)]
    echo: Option<EchoArg>,
    #[arg(long, value_parser = parse_positive_u32)]
    write_chunk_size: Option<u32>,
    #[arg(long)]
    write_chunk_delay_ms: Option<u64>,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct ModelUpdateArgs {
    /// Exact profile name. Profile identity cannot be renamed in place.
    name: String,
    #[arg(long, conflicts_with = "clear_shell_prompt")]
    shell_prompt: Option<String>,
    /// Clear the shell prompt for this Model Profile.
    #[arg(long)]
    clear_shell_prompt: bool,
    #[arg(long, conflicts_with = "clear_uboot_prompt")]
    uboot_prompt: Option<String>,
    /// Clear the U-Boot prompt for this Model Profile.
    #[arg(long)]
    clear_uboot_prompt: bool,
    /// `none`, `cr`, `lf`, or `crlf`.
    #[arg(
        long,
        value_parser = parse_eol,
        conflicts_with = "inherit_write_eol"
    )]
    write_eol: Option<String>,
    /// Remove the EOL override so the Port value is inherited.
    #[arg(long)]
    inherit_write_eol: bool,
    #[arg(long, value_enum, conflicts_with = "inherit_echo")]
    echo: Option<EchoArg>,
    /// Remove the echo override so the Port value is inherited.
    #[arg(long)]
    inherit_echo: bool,
    #[arg(
        long,
        value_parser = parse_positive_u32,
        conflicts_with = "inherit_write_chunk_size"
    )]
    write_chunk_size: Option<u32>,
    /// Remove the write-chunk-size override.
    #[arg(long)]
    inherit_write_chunk_size: bool,
    #[arg(long, conflicts_with = "inherit_write_chunk_delay")]
    write_chunk_delay_ms: Option<u64>,
    /// Remove the inter-chunk-delay override.
    #[arg(long)]
    inherit_write_chunk_delay: bool,
    /// Prompt for every field, using the current profile as defaults.
    #[arg(long, conflicts_with = "json")]
    interactive: bool,
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct ProfileAttachArgs {
    /// Serial port to update.
    #[arg(long)]
    port: String,
    /// UART transport profile name.
    #[arg(long)]
    transport: Option<String>,
    /// DUT behavior profile name.
    #[arg(long, conflicts_with = "generic")]
    model: Option<String>,
    /// Attach no DUT-specific profile.
    #[arg(long)]
    generic: bool,
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct ProfileDetachArgs {
    /// Serial port to update.
    #[arg(long)]
    port: String,
    /// Reset the physical UART profile to the safe 115200/8N1 baseline.
    #[arg(long)]
    transport: bool,
    /// Detach DUT-specific behavior. This is the default when no kind is set.
    #[arg(long)]
    model: bool,
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

pub async fn run(api: &ApiClient, args: ProfileArgs) -> Result<()> {
    let ProfileArgs { command } = args;
    command.validate_local()?;
    match command {
        ProfileCommand::Transport { command } => run_transport(api, command).await,
        ProfileCommand::Model { command } => run_model(api, command).await,
        ProfileCommand::Attach(args) => attach(api, args).await,
        ProfileCommand::Detach(args) => detach(api, args).await,
    }
}

impl ProfileCommand {
    fn validate_local(&self) -> Result<()> {
        match self {
            Self::Transport {
                command: TransportCommand::Update(args),
            } if !args.interactive && !args.has_changes() => bail!(
                "transport update requires at least one field flag or --interactive; no changes were requested"
            ),
            Self::Model {
                command: ModelCommand::Update(args),
            } if !args.interactive && !args.has_changes() => bail!(
                "model update requires at least one field flag, one --clear-*/--inherit-* flag, or --interactive; no changes were requested"
            ),
            _ => Ok(()),
        }
    }
}

pub async fn load_transport_catalog(
    api: &ApiClient,
) -> Result<(ProfileCatalog<TransportProfile>, bool)> {
    Ok((api.transport_profiles().await?, true))
}

pub async fn load_model_catalog(api: &ApiClient) -> Result<ProfileCatalog<ModelProfile>> {
    api.model_profiles().await
}

async fn run_transport(api: &ApiClient, command: TransportCommand) -> Result<()> {
    match command {
        TransportCommand::List(args) => {
            let (catalog, server_managed) = load_transport_catalog(api).await?;
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "profiles": catalog.profiles,
                        "config_revision": catalog.config_revision,
                        "server_managed": server_managed,
                    }))?
                );
            } else {
                for profile in catalog.profiles {
                    println!(
                        "{:<24} {} {:?}{:?}{:?} flow={:?} dtr={} rts={} auto_open={}",
                        safe_inline(&profile.name),
                        profile.baud_rate,
                        profile.data_bits,
                        profile.parity,
                        profile.stop_bits,
                        profile.flow_control,
                        profile.dtr,
                        profile.rts,
                        profile.auto_open
                    );
                }
                if !server_managed {
                    eprintln!(
                        "seriald does not expose the transport Profile API; only the built-in safe baseline is available"
                    );
                }
            }
            Ok(())
        }
        TransportCommand::Show(args) => {
            let (catalog, _) = load_transport_catalog(api).await?;
            let profile = find_named(&catalog.profiles, &args.name)?;
            print_profile(profile, args.json)
        }
        TransportCommand::Create(mut args) => {
            if args.interactive {
                populate_transport_interactively(&mut args)?;
            }
            let name = required_name(args.name, "Transport Profile name", !args.json)?;
            let profile = TransportProfile {
                name,
                baud_rate: args.baud_rate,
                data_bits: args.data_bits.into(),
                parity: args.parity.into(),
                stop_bits: args.stop_bits.into(),
                flow_control: args.flow_control.into(),
                dtr: args.dtr,
                rts: args.rts,
                auto_open: args.auto_open,
            };
            validate_transport(&profile)?;
            upsert_transport(api, vec![profile], false, args.json).await
        }
        TransportCommand::Update(args) => update_transport(api, args).await,
        TransportCommand::Clone(args) => {
            let (catalog, _) = load_transport_catalog(api).await?;
            let mut profile = find_named(&catalog.profiles, &args.source)?.clone();
            profile.name = required_name(args.name, "New Transport Profile name", !args.json)?;
            if let Some(value) = args.baud_rate {
                profile.baud_rate = value;
            }
            if let Some(value) = args.data_bits {
                profile.data_bits = value.into();
            }
            if let Some(value) = args.parity {
                profile.parity = value.into();
            }
            if let Some(value) = args.stop_bits {
                profile.stop_bits = value.into();
            }
            if let Some(value) = args.flow_control {
                profile.flow_control = value.into();
            }
            if let Some(value) = args.dtr {
                profile.dtr = value;
            }
            if let Some(value) = args.rts {
                profile.rts = value;
            }
            if let Some(value) = args.auto_open {
                profile.auto_open = value;
            }
            validate_transport(&profile)?;
            upsert_transport(api, vec![profile], false, args.json).await
        }
        TransportCommand::Import(args) => {
            let profiles = read_profiles::<TransportProfile>(&args.file)?;
            for profile in &profiles {
                validate_transport(profile)?;
            }
            upsert_transport(api, profiles, args.replace, args.json).await
        }
        TransportCommand::Export(args) => {
            let (catalog, _) = load_transport_catalog(api).await?;
            let profile = find_named(&catalog.profiles, &args.name)?;
            export_profile(profile, &args)
        }
        TransportCommand::Delete(args) => delete_transport(api, args).await,
    }
}

async fn run_model(api: &ApiClient, command: ModelCommand) -> Result<()> {
    match command {
        ModelCommand::List(args) => {
            let catalog = load_model_catalog(api).await?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&catalog)?);
            } else {
                println!(
                    "{:<24} {:<16} {:<16} {:<8} {:<8} pacing",
                    "NAME", "SHELL", "U-BOOT", "EOL", "ECHO"
                );
                println!(
                    "{:<24} {:<16} {:<16} {:<8} {:<8} inherited",
                    "Generic (unbound)", "-", "-", "\\r", "on"
                );
                for profile in catalog.profiles {
                    println!(
                        "{:<24} {:<16} {:<16} {:<8} {:<8} {}/{}ms",
                        safe_inline(&profile.name),
                        safe_inline(profile.shell_prompt.as_deref().unwrap_or("-")),
                        safe_inline(profile.uboot_prompt.as_deref().unwrap_or("-")),
                        printable_eol(profile.write_eol.as_deref()),
                        profile
                            .echo
                            .map(|value| format!("{value:?}"))
                            .unwrap_or_else(|| "-".into()),
                        profile
                            .write_chunk_size
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "-".into()),
                        profile
                            .write_chunk_delay_ms
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "-".into()),
                    );
                }
            }
            Ok(())
        }
        ModelCommand::Show(args) => {
            if args.name.eq_ignore_ascii_case("generic") {
                if args.json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "name": "Generic",
                            "attached_profile": null,
                            "description": "No DUT-specific overrides"
                        }))?
                    );
                } else {
                    println!("Generic: no DUT-specific overrides are attached");
                }
                return Ok(());
            }
            let catalog = load_model_catalog(api).await?;
            print_profile(find_named(&catalog.profiles, &args.name)?, args.json)
        }
        ModelCommand::Create(mut args) => {
            if args.interactive {
                populate_model_interactively(&mut args)?;
            }
            let profile = ModelProfile {
                name: required_name(args.name, "Model Profile name", !args.json)?,
                shell_prompt: args.shell_prompt,
                uboot_prompt: args.uboot_prompt,
                write_eol: args.write_eol,
                echo: args.echo.map(Into::into),
                write_chunk_size: args.write_chunk_size,
                write_chunk_delay_ms: args.write_chunk_delay_ms,
            };
            validate_model(&profile)?;
            upsert_model(api, vec![profile], false, args.json).await
        }
        ModelCommand::Update(args) => update_model(api, args).await,
        ModelCommand::Clone(args) => {
            let catalog = load_model_catalog(api).await?;
            let mut profile = find_named(&catalog.profiles, &args.source)?.clone();
            profile.name = required_name(args.name, "New Model Profile name", !args.json)?;
            if let Some(value) = args.shell_prompt {
                profile.shell_prompt = Some(value);
            }
            if let Some(value) = args.uboot_prompt {
                profile.uboot_prompt = Some(value);
            }
            if let Some(value) = args.write_eol {
                profile.write_eol = Some(value);
            }
            if let Some(value) = args.echo {
                profile.echo = Some(value.into());
            }
            if let Some(value) = args.write_chunk_size {
                profile.write_chunk_size = Some(value);
            }
            if let Some(value) = args.write_chunk_delay_ms {
                profile.write_chunk_delay_ms = Some(value);
            }
            validate_model(&profile)?;
            upsert_model(api, vec![profile], false, args.json).await
        }
        ModelCommand::Import(args) => {
            let profiles = read_profiles::<ModelProfile>(&args.file)?;
            for profile in &profiles {
                validate_model(profile)?;
            }
            upsert_model(api, profiles, args.replace, args.json).await
        }
        ModelCommand::Export(args) => {
            let catalog = load_model_catalog(api).await?;
            export_profile(find_named(&catalog.profiles, &args.name)?, &args)
        }
        ModelCommand::Delete(args) => delete_model(api, args).await,
    }
}

async fn update_transport(api: &ApiClient, args: TransportUpdateArgs) -> Result<()> {
    let (mut catalog, server_managed) = load_transport_catalog(api).await?;
    if !server_managed {
        bail!(
            "this seriald does not support managed Transport Profiles; upgrade seriald before updating one"
        );
    }
    let index = catalog
        .profiles
        .iter()
        .position(|profile| profile.name == args.name)
        .with_context(|| format!("unknown Profile {:?}", safe_inline(&args.name)))?;
    let mut profile = catalog.profiles[index].clone();
    apply_transport_update(&mut profile, &args);
    if args.interactive {
        populate_transport_profile_interactively(&mut profile)?;
    }
    validate_transport(&profile)?;
    catalog.profiles[index] = profile;

    let response = api
        .configure_transport_profiles(catalog.profiles, catalog.config_revision)
        .await
        .map_err(revision_error)?;
    print_catalog_result(&response, args.json)
}

async fn update_model(api: &ApiClient, args: ModelUpdateArgs) -> Result<()> {
    let mut catalog = load_model_catalog(api).await?;
    let index = catalog
        .profiles
        .iter()
        .position(|profile| profile.name == args.name)
        .with_context(|| format!("unknown Profile {:?}", safe_inline(&args.name)))?;
    let mut profile = catalog.profiles[index].clone();
    apply_model_update(&mut profile, &args);
    if args.interactive {
        populate_model_profile_interactively(&mut profile)?;
    }
    validate_model(&profile)?;
    if catalog.config_revision.is_none()
        && (profile.write_chunk_size.is_some() || profile.write_chunk_delay_ms.is_some())
    {
        bail!(
            "this seriald predates managed Model Profile pacing; upgrade seriald before saving write_chunk_size or write_chunk_delay_ms"
        );
    }
    catalog.profiles[index] = profile;

    let response = api
        .configure_model_profiles(catalog.profiles, catalog.config_revision)
        .await
        .map_err(revision_error)?;
    print_catalog_result(&response, args.json)
}

impl TransportUpdateArgs {
    fn has_changes(&self) -> bool {
        self.baud_rate.is_some()
            || self.data_bits.is_some()
            || self.parity.is_some()
            || self.stop_bits.is_some()
            || self.flow_control.is_some()
            || self.dtr.is_some()
            || self.rts.is_some()
            || self.auto_open.is_some()
    }
}

fn apply_transport_update(profile: &mut TransportProfile, args: &TransportUpdateArgs) {
    if let Some(value) = args.baud_rate {
        profile.baud_rate = value;
    }
    if let Some(value) = args.data_bits {
        profile.data_bits = value.into();
    }
    if let Some(value) = args.parity {
        profile.parity = value.into();
    }
    if let Some(value) = args.stop_bits {
        profile.stop_bits = value.into();
    }
    if let Some(value) = args.flow_control {
        profile.flow_control = value.into();
    }
    if let Some(value) = args.dtr {
        profile.dtr = value;
    }
    if let Some(value) = args.rts {
        profile.rts = value;
    }
    if let Some(value) = args.auto_open {
        profile.auto_open = value;
    }
}

impl ModelUpdateArgs {
    fn has_changes(&self) -> bool {
        self.shell_prompt.is_some()
            || self.clear_shell_prompt
            || self.uboot_prompt.is_some()
            || self.clear_uboot_prompt
            || self.write_eol.is_some()
            || self.inherit_write_eol
            || self.echo.is_some()
            || self.inherit_echo
            || self.write_chunk_size.is_some()
            || self.inherit_write_chunk_size
            || self.write_chunk_delay_ms.is_some()
            || self.inherit_write_chunk_delay
    }
}

fn apply_model_update(profile: &mut ModelProfile, args: &ModelUpdateArgs) {
    if let Some(value) = &args.shell_prompt {
        profile.shell_prompt = Some(value.clone());
    } else if args.clear_shell_prompt {
        profile.shell_prompt = None;
    }
    if let Some(value) = &args.uboot_prompt {
        profile.uboot_prompt = Some(value.clone());
    } else if args.clear_uboot_prompt {
        profile.uboot_prompt = None;
    }
    if let Some(value) = &args.write_eol {
        profile.write_eol = Some(value.clone());
    } else if args.inherit_write_eol {
        profile.write_eol = None;
    }
    if let Some(value) = args.echo {
        profile.echo = Some(value.into());
    } else if args.inherit_echo {
        profile.echo = None;
    }
    if let Some(value) = args.write_chunk_size {
        profile.write_chunk_size = Some(value);
    } else if args.inherit_write_chunk_size {
        profile.write_chunk_size = None;
    }
    if let Some(value) = args.write_chunk_delay_ms {
        profile.write_chunk_delay_ms = Some(value);
    } else if args.inherit_write_chunk_delay {
        profile.write_chunk_delay_ms = None;
    }
}

async fn upsert_transport(
    api: &ApiClient,
    incoming: Vec<TransportProfile>,
    replace: bool,
    json: bool,
) -> Result<()> {
    let (mut catalog, server_managed) = load_transport_catalog(api).await?;
    if !server_managed {
        bail!(
            "this seriald does not support managed Transport Profiles; upgrade seriald before creating or importing one"
        );
    }
    merge_profiles(&mut catalog.profiles, incoming, replace)?;
    let response = api
        .configure_transport_profiles(catalog.profiles, catalog.config_revision)
        .await
        .map_err(revision_error)?;
    print_catalog_result(&response, json)
}

async fn upsert_model(
    api: &ApiClient,
    incoming: Vec<ModelProfile>,
    replace: bool,
    json: bool,
) -> Result<()> {
    let mut catalog = load_model_catalog(api).await?;
    if catalog.config_revision.is_none()
        && incoming.iter().any(|profile| {
            profile.write_chunk_size.is_some() || profile.write_chunk_delay_ms.is_some()
        })
    {
        bail!(
            "this seriald predates managed Model Profile pacing; upgrade seriald before saving write_chunk_size or write_chunk_delay_ms"
        );
    }
    merge_profiles(&mut catalog.profiles, incoming, replace)?;
    let response = api
        .configure_model_profiles(catalog.profiles, catalog.config_revision)
        .await
        .map_err(revision_error)?;
    print_catalog_result(&response, json)
}

async fn delete_transport(api: &ApiClient, args: DeleteArgs) -> Result<()> {
    if args.name == SAFE_TRANSPORT_NAME {
        bail!("the built-in safe Transport Profile cannot be deleted");
    }
    confirm_delete(&args.name, args.yes, !args.json)?;
    let status = api.configuration_status().await?;
    let used = status
        .ports
        .iter()
        .filter(|slot| slot.config.transport_profile.as_deref() == Some(args.name.as_str()))
        .map(|slot| slot.config.port.as_str())
        .collect::<Vec<_>>();
    if !used.is_empty() {
        let used = used
            .into_iter()
            .map(safe_inline)
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "Transport Profile {:?} is attached to Port(s) {}; attach another profile first",
            safe_inline(&args.name),
            used
        );
    }
    let (mut catalog, server_managed) = load_transport_catalog(api).await?;
    if !server_managed {
        bail!("this seriald does not support managed Transport Profiles");
    }
    remove_named(&mut catalog.profiles, &args.name)?;
    let response = api
        .configure_transport_profiles(catalog.profiles, status.config_revision)
        .await
        .map_err(revision_error)?;
    print_catalog_result(&response, args.json)
}

async fn delete_model(api: &ApiClient, args: DeleteArgs) -> Result<()> {
    confirm_delete(&args.name, args.yes, !args.json)?;
    let status = api.configuration_status().await?;
    let used = status
        .ports
        .iter()
        .filter(|slot| slot.config.model_profile.as_deref() == Some(args.name.as_str()))
        .map(|slot| slot.config.port.as_str())
        .collect::<Vec<_>>();
    if !used.is_empty() {
        let used = used
            .into_iter()
            .map(safe_inline)
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "Model Profile {:?} is attached to Port(s) {}; detach it first",
            safe_inline(&args.name),
            used
        );
    }
    let mut catalog = load_model_catalog(api).await?;
    remove_named(&mut catalog.profiles, &args.name)?;
    let response = api
        .configure_model_profiles(catalog.profiles, status.config_revision)
        .await
        .map_err(revision_error)?;
    print_catalog_result(&response, args.json)
}

async fn attach(api: &ApiClient, args: ProfileAttachArgs) -> Result<()> {
    if args.transport.is_none() && args.model.is_none() && !args.generic {
        bail!("attach requires --transport, --model, or --generic");
    }
    let status = api.configuration_status().await?;
    let mut ports = status
        .ports
        .into_iter()
        .map(|slot| slot.config)
        .collect::<Vec<_>>();
    let slot = find_port_mut(&mut ports, &args.port)?;

    if let Some(name) = args.transport {
        let (catalog, _) = load_transport_catalog(api).await?;
        let selected = find_named(&catalog.profiles, &name)?;
        slot.transport_profile = Some(selected.name.clone());
    }
    if args.generic {
        slot.model_profile = None;
    } else if let Some(name) = args.model {
        if name.eq_ignore_ascii_case("generic") {
            slot.model_profile = None;
        } else {
            let catalog = load_model_catalog(api).await?;
            find_named(&catalog.profiles, &name)?;
            slot.model_profile = Some(name);
        }
    }

    let response = api
        .configure_ports(ports, status.config_revision)
        .await
        .map_err(revision_error)?;
    print_slot_result(
        &response.ports,
        &args.port,
        response.config_revision,
        args.json,
    )
}

async fn detach(api: &ApiClient, args: ProfileDetachArgs) -> Result<()> {
    let status = api.configuration_status().await?;
    let mut ports = status
        .ports
        .into_iter()
        .map(|slot| slot.config)
        .collect::<Vec<_>>();
    let slot = find_port_mut(&mut ports, &args.port)?;
    let detach_model = args.model || !args.transport;
    if detach_model {
        slot.model_profile = None;
    }
    if args.transport {
        slot.transport_profile = None;
    }
    let response = api
        .configure_ports(ports, status.config_revision)
        .await
        .map_err(revision_error)?;
    print_slot_result(
        &response.ports,
        &args.port,
        response.config_revision,
        args.json,
    )
}

fn print_slot_result(
    ports: &[serial_protocol::SlotSnapshot],
    port: &str,
    revision: u64,
    json: bool,
) -> Result<()> {
    let slot = ports
        .iter()
        .find(|slot| slot.config.port == port)
        .with_context(|| {
            format!(
                "seriald did not return configured port {:?}",
                safe_inline(port)
            )
        })?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "port": slot,
                "config_revision": revision,
            }))?
        );
    } else {
        println!(
            "Port {}: transport={}, model={}",
            safe_inline(&slot.config.port),
            safe_inline(
                slot.config
                    .transport_profile
                    .as_deref()
                    .unwrap_or("115200 8N1")
            ),
            safe_inline(slot.config.model_profile.as_deref().unwrap_or("Generic"))
        );
    }
    Ok(())
}

fn find_port_mut<'a>(ports: &'a mut [SlotConfig], port: &str) -> Result<&'a mut SlotConfig> {
    ports
        .iter_mut()
        .find(|slot| slot.port == port)
        .with_context(|| format!("unknown serial port {:?}", safe_inline(port)))
}

fn revision_error(error: anyhow::Error) -> anyhow::Error {
    if is_conflict(&error) {
        anyhow::anyhow!(
            "configuration changed on another client; reload the Profile/port state and retry ({error})"
        )
    } else {
        error
    }
}

fn print_catalog_result<T: Serialize>(catalog: &ProfileCatalog<T>, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(catalog)?);
    } else {
        println!(
            "Saved {} Profile(s) at configuration revision {}",
            catalog.profiles.len(),
            catalog
                .config_revision
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".into())
        );
    }
    Ok(())
}

fn print_profile<T: Serialize + std::fmt::Debug>(profile: &T, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(profile)?);
    } else {
        println!("{}", toml::to_string_pretty(profile)?);
    }
    Ok(())
}

fn validate_transport(profile: &TransportProfile) -> Result<()> {
    validate_name(&profile.name)?;
    if profile.baud_rate == 0 {
        bail!("baud rate must be greater than zero");
    }
    Ok(())
}

fn validate_model(profile: &ModelProfile) -> Result<()> {
    validate_name(&profile.name)?;
    if profile
        .write_chunk_size
        .is_some_and(|chunk_size| chunk_size == 0)
    {
        bail!("write chunk size must be greater than zero");
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.trim() != name {
        bail!("Profile name must be non-empty and must not have surrounding whitespace");
    }
    if name.eq_ignore_ascii_case("generic") {
        bail!("Generic is reserved for an unbound Model Profile");
    }
    Ok(())
}

fn populate_transport_interactively(args: &mut TransportCreateArgs) -> Result<()> {
    ensure_tty()?;
    args.name = Some(match args.name.take() {
        Some(name) => prompt_with_default("Transport Profile name", &name)?,
        None => prompt("Transport Profile name")?,
    });
    args.baud_rate = prompt_with_default("Baud rate", &args.baud_rate.to_string())?
        .parse()
        .context("baud rate must be a positive integer")?;
    args.data_bits = prompt_enum("Data bits", args.data_bits)?;
    args.parity = prompt_enum("Parity", args.parity)?;
    args.stop_bits = prompt_enum("Stop bits", args.stop_bits)?;
    args.flow_control = prompt_enum("Flow control", args.flow_control)?;
    args.dtr = prompt_bool("Assert DTR", args.dtr)?;
    args.rts = prompt_bool("Assert RTS", args.rts)?;
    args.auto_open = prompt_bool("Open automatically", args.auto_open)?;
    Ok(())
}

fn populate_transport_profile_interactively(profile: &mut TransportProfile) -> Result<()> {
    ensure_tty()?;
    profile.baud_rate = prompt_with_default("Baud rate", &profile.baud_rate.to_string())?
        .parse()
        .context("baud rate must be a positive integer")?;
    profile.data_bits = prompt_enum("Data bits", data_bits_arg(&profile.data_bits))?.into();
    profile.parity = prompt_enum("Parity", parity_arg(&profile.parity))?.into();
    profile.stop_bits = prompt_enum("Stop bits", stop_bits_arg(&profile.stop_bits))?.into();
    profile.flow_control =
        prompt_enum("Flow control", flow_control_arg(&profile.flow_control))?.into();
    profile.dtr = prompt_bool("Assert DTR", profile.dtr)?;
    profile.rts = prompt_bool("Assert RTS", profile.rts)?;
    profile.auto_open = prompt_bool("Open automatically", profile.auto_open)?;
    Ok(())
}

fn populate_model_interactively(args: &mut ModelCreateArgs) -> Result<()> {
    ensure_tty()?;
    args.name = Some(match args.name.take() {
        Some(name) => prompt_with_default("Model Profile name", &name)?,
        None => prompt("Model Profile name")?,
    });
    args.shell_prompt = prompt_optional_prompt("Shell prompt", args.shell_prompt.take())?;
    args.uboot_prompt = prompt_optional_prompt("U-Boot prompt", args.uboot_prompt.take())?;

    let current_eol = args
        .write_eol
        .as_deref()
        .map(|value| printable_eol(Some(value)))
        .unwrap_or("inherit");
    let entered_eol = prompt_with_default("Write EOL (inherit/none/cr/lf/crlf)", current_eol)?;
    args.write_eol = if entered_eol.eq_ignore_ascii_case("inherit") {
        None
    } else {
        Some(parse_eol(&entered_eol).map_err(anyhow::Error::msg)?)
    };

    let current_echo = args
        .echo
        .map(|value| format!("{value:?}").to_ascii_lowercase())
        .unwrap_or_else(|| "inherit".into());
    let entered_echo = prompt_with_default("Echo (inherit/on/off/auto)", &current_echo)?;
    args.echo = if entered_echo.eq_ignore_ascii_case("inherit") {
        None
    } else {
        Some(EchoArg::from_str(&entered_echo, true).map_err(anyhow::Error::msg)?)
    };

    args.write_chunk_size = prompt_optional_number(
        "Write chunk size (inherit or positive bytes)",
        args.write_chunk_size,
        true,
    )?
    .map(|value| value as u32);
    args.write_chunk_delay_ms = prompt_optional_number(
        "Inter-chunk delay (inherit or milliseconds)",
        args.write_chunk_delay_ms,
        false,
    )?;
    Ok(())
}

fn populate_model_profile_interactively(profile: &mut ModelProfile) -> Result<()> {
    ensure_tty()?;
    profile.shell_prompt = prompt_optional_prompt("Shell prompt", profile.shell_prompt.take())?;
    profile.uboot_prompt = prompt_optional_prompt("U-Boot prompt", profile.uboot_prompt.take())?;

    let current_eol = profile
        .write_eol
        .as_deref()
        .map(|value| printable_eol(Some(value)))
        .unwrap_or("inherit");
    let entered_eol = prompt_with_default("Write EOL (inherit/none/cr/lf/crlf)", current_eol)?;
    profile.write_eol = if entered_eol.eq_ignore_ascii_case("inherit") {
        None
    } else {
        Some(parse_eol(&entered_eol).map_err(anyhow::Error::msg)?)
    };

    let current_echo = profile
        .echo
        .as_ref()
        .map(|value| format!("{:?}", echo_arg(value)).to_ascii_lowercase())
        .unwrap_or_else(|| "inherit".into());
    let entered_echo = prompt_with_default("Echo (inherit/on/off/auto)", &current_echo)?;
    profile.echo = if entered_echo.eq_ignore_ascii_case("inherit") {
        None
    } else {
        Some(
            EchoArg::from_str(&entered_echo, true)
                .map_err(anyhow::Error::msg)?
                .into(),
        )
    };

    profile.write_chunk_size = prompt_optional_number(
        "Write chunk size (inherit or positive bytes)",
        profile.write_chunk_size,
        true,
    )?
    .map(|value| value as u32);
    profile.write_chunk_delay_ms = prompt_optional_number(
        "Inter-chunk delay (inherit or milliseconds)",
        profile.write_chunk_delay_ms,
        false,
    )?;
    Ok(())
}

fn data_bits_arg(value: &DataBits) -> DataBitsArg {
    match value {
        DataBits::Five => DataBitsArg::Five,
        DataBits::Six => DataBitsArg::Six,
        DataBits::Seven => DataBitsArg::Seven,
        DataBits::Eight => DataBitsArg::Eight,
    }
}

fn parity_arg(value: &Parity) -> ParityArg {
    match value {
        Parity::None => ParityArg::None,
        Parity::Odd => ParityArg::Odd,
        Parity::Even => ParityArg::Even,
    }
}

fn stop_bits_arg(value: &StopBits) -> StopBitsArg {
    match value {
        StopBits::One => StopBitsArg::One,
        StopBits::Two => StopBitsArg::Two,
    }
}

fn flow_control_arg(value: &FlowControl) -> FlowControlArg {
    match value {
        FlowControl::None => FlowControlArg::None,
        FlowControl::Software => FlowControlArg::Software,
        FlowControl::Hardware => FlowControlArg::Hardware,
    }
}

fn echo_arg(value: &EchoMode) -> EchoArg {
    match value {
        EchoMode::On => EchoArg::On,
        EchoMode::Off => EchoArg::Off,
        EchoMode::Auto => EchoArg::Auto,
    }
}

fn ensure_tty() -> Result<()> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        bail!("--interactive requires a terminal; provide complete flags for automation");
    }
    Ok(())
}

fn prompt_with_default(label: &str, default: &str) -> Result<String> {
    print!("{} [{}]: ", safe_inline(label), safe_inline(default));
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

fn prompt_optional_prompt(label: &str, current: Option<String>) -> Result<Option<String>> {
    let default = current.as_deref().unwrap_or("unset");
    let entered = prompt_with_default(&format!("{label} (unset for no prompt)"), default)?;
    Ok((!entered.eq_ignore_ascii_case("unset")).then_some(entered))
}

fn prompt_optional_number(
    label: &str,
    current: Option<impl Into<u64> + Copy>,
    positive: bool,
) -> Result<Option<u64>> {
    let default = current
        .map(|value| value.into().to_string())
        .unwrap_or_else(|| "inherit".into());
    let entered = prompt_with_default(label, &default)?;
    if entered.eq_ignore_ascii_case("inherit") {
        return Ok(None);
    }
    let value = entered
        .parse::<u64>()
        .with_context(|| format!("{label} must be an integer or inherit"))?;
    if positive && value == 0 {
        bail!("{label} must be greater than zero");
    }
    Ok(Some(value))
}

fn prompt_enum<T>(label: &str, current: T) -> Result<T>
where
    T: ValueEnum + Clone + std::fmt::Debug,
{
    let default = format!("{current:?}").to_ascii_lowercase();
    let value = prompt_with_default(label, &default)?;
    T::from_str(&value, true).map_err(anyhow::Error::msg)
}

fn prompt_bool(label: &str, current: bool) -> Result<bool> {
    let value = prompt_with_default(label, if current { "yes" } else { "no" })?;
    match value.to_ascii_lowercase().as_str() {
        "y" | "yes" | "true" | "1" => Ok(true),
        "n" | "no" | "false" | "0" => Ok(false),
        _ => bail!("{label} must be yes or no"),
    }
}

fn required_name(value: Option<String>, label: &str, allow_prompt: bool) -> Result<String> {
    let value = match value {
        Some(value) => value,
        None if allow_prompt && io::stdin().is_terminal() && io::stdout().is_terminal() => {
            prompt(label)?
        }
        None => bail!("--name is required in non-interactive use"),
    };
    validate_name(&value)?;
    Ok(value)
}

fn prompt(label: &str) -> Result<String> {
    print!("{}: ", safe_inline(label));
    io::stdout().flush()?;
    let mut value = String::new();
    io::stdin().read_line(&mut value)?;
    Ok(value.trim().to_owned())
}

fn confirm_delete(name: &str, yes: bool, allow_prompt: bool) -> Result<()> {
    if yes {
        return Ok(());
    }
    if !allow_prompt || !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        bail!("--yes is required to delete a Profile in non-interactive use");
    }
    print!("Delete Profile {:?}? [y/N]: ", safe_inline(name));
    io::stdout().flush()?;
    let mut value = String::new();
    io::stdin().read_line(&mut value)?;
    if !matches!(value.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        bail!("Profile deletion cancelled");
    }
    Ok(())
}

fn merge_profiles<T: Named>(existing: &mut Vec<T>, incoming: Vec<T>, replace: bool) -> Result<()> {
    for profile in incoming {
        if let Some(index) = existing
            .iter()
            .position(|candidate| candidate.name() == profile.name())
        {
            if !replace {
                bail!(
                    "Profile {:?} already exists; use --replace to update it",
                    safe_inline(profile.name())
                );
            }
            existing[index] = profile;
        } else {
            existing.push(profile);
        }
    }
    existing.sort_by(|left, right| left.name().cmp(right.name()));
    Ok(())
}

fn remove_named<T: Named>(profiles: &mut Vec<T>, name: &str) -> Result<()> {
    let previous = profiles.len();
    profiles.retain(|profile| profile.name() != name);
    if profiles.len() == previous {
        bail!("unknown Profile {:?}", safe_inline(name));
    }
    Ok(())
}

fn find_named<'a, T: Named>(profiles: &'a [T], name: &str) -> Result<&'a T> {
    profiles
        .iter()
        .find(|profile| profile.name() == name)
        .with_context(|| format!("unknown Profile {:?}", safe_inline(name)))
}

trait Named {
    fn name(&self) -> &str;
}

impl Named for TransportProfile {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Named for ModelProfile {
    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Deserialize)]
struct CatalogFile<T> {
    profiles: Vec<T>,
}

fn read_profiles<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    let parse_json = || -> Result<Vec<T>> {
        if let Ok(profile) = serde_json::from_str::<T>(&contents) {
            return Ok(vec![profile]);
        }
        Ok(serde_json::from_str::<CatalogFile<T>>(&contents)?.profiles)
    };
    let parse_toml = || -> Result<Vec<T>> {
        if let Ok(profile) = toml::from_str::<T>(&contents) {
            return Ok(vec![profile]);
        }
        Ok(toml::from_str::<CatalogFile<T>>(&contents)?.profiles)
    };
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("json") => parse_json(),
        Some("toml") => parse_toml(),
        _ => parse_json().or_else(|_| parse_toml()),
    }
    .with_context(|| {
        format!(
            "{} is not a valid Profile JSON/TOML document",
            path.display()
        )
    })
}

fn export_profile<T: Serialize>(profile: &T, args: &ExportArgs) -> Result<()> {
    let format = args.format.unwrap_or_else(|| {
        if args
            .output
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
        {
            ExportFormat::Json
        } else {
            ExportFormat::Toml
        }
    });
    let encoded = match format {
        ExportFormat::Toml => toml::to_string_pretty(profile)?,
        ExportFormat::Json => serde_json::to_string_pretty(profile)? + "\n",
    };
    if args.output == Path::new("-") {
        print!("{encoded}");
        return Ok(());
    }
    if args.output.exists() && !args.force {
        bail!(
            "{} already exists; use --force to replace it",
            args.output.display()
        );
    }
    fs::write(&args.output, encoded)
        .with_context(|| format!("cannot write {}", args.output.display()))
}

fn parse_eol(value: &str) -> Result<String, String> {
    match value.to_ascii_lowercase().as_str() {
        "none" | "" => Ok(String::new()),
        "cr" | "\\r" => Ok("\r".into()),
        "lf" | "\\n" => Ok("\n".into()),
        "crlf" | "\\r\\n" => Ok("\r\n".into()),
        _ => Err("expected none, cr, lf, or crlf".into()),
    }
}

fn printable_eol(value: Option<&str>) -> &'static str {
    match value {
        Some("") => "none",
        Some("\r") => "\\r",
        Some("\n") => "\\n",
        Some("\r\n") => "\\r\\n",
        Some(_) => "custom",
        None => "-",
    }
}

fn parse_positive_u32(value: &str) -> Result<u32, String> {
    let value = value
        .parse::<u32>()
        .map_err(|_| "expected a positive integer".to_string())?;
    if value == 0 {
        return Err("expected a positive integer".into());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use serial_protocol::apply_transport_profile;

    #[derive(Debug, Parser)]
    struct ProfileParser {
        #[command(subcommand)]
        command: ProfileCommand,
    }

    #[test]
    fn safe_transport_is_115200_8n1_without_flow_control() {
        let profile = safe_transport_profile();
        assert_eq!(profile.baud_rate, 115_200);
        assert_eq!(profile.data_bits, DataBits::Eight);
        assert_eq!(profile.parity, Parity::None);
        assert_eq!(profile.stop_bits, StopBits::One);
        assert_eq!(profile.flow_control, FlowControl::None);
        assert!(!profile.dtr);
        assert!(!profile.rts);
    }

    #[test]
    fn applying_transport_does_not_overwrite_model_behavior_or_pacing() {
        let settings = serial_protocol::SerialSettings {
            write_eol: "\n".into(),
            shell_prompt: Some("# ".into()),
            write_chunk_size: 7,
            write_chunk_delay_ms: 3,
            ..serial_protocol::SerialSettings::default()
        };
        let settings = apply_transport_profile(&settings, Some(&safe_transport_profile()));
        assert_eq!(settings.write_eol, "\n");
        assert_eq!(settings.shell_prompt.as_deref(), Some("# "));
        assert_eq!(settings.write_chunk_size, 7);
        assert_eq!(settings.write_chunk_delay_ms, 3);
    }

    #[test]
    fn eol_parser_is_explicit() {
        assert_eq!(parse_eol("cr").unwrap(), "\r");
        assert_eq!(parse_eol("crlf").unwrap(), "\r\n");
        assert!(parse_eol("newline-ish").is_err());
    }

    #[test]
    fn duplicate_merge_requires_replace() {
        let mut profiles = vec![safe_transport_profile()];
        let incoming = vec![safe_transport_profile()];
        assert!(merge_profiles(&mut profiles, incoming.clone(), false).is_err());
        merge_profiles(&mut profiles, incoming, true).unwrap();
        assert_eq!(profiles.len(), 1);
    }

    #[test]
    fn model_profile_files_use_the_protocol_pacing_schema() {
        let profile = ModelProfile {
            name: "slow-dut".into(),
            shell_prompt: Some("# ".into()),
            uboot_prompt: None,
            write_eol: Some("\r".into()),
            echo: Some(EchoMode::On),
            write_chunk_size: Some(8),
            write_chunk_delay_ms: Some(1),
        };
        let encoded = serde_json::to_value(&profile).unwrap();
        assert_eq!(encoded["write_chunk_size"], 8);
        assert_eq!(encoded["write_chunk_delay_ms"], 1);
        assert_eq!(
            serde_json::from_value::<ModelProfile>(encoded).unwrap(),
            profile
        );
    }

    #[test]
    fn transport_update_parser_accepts_partial_scriptable_fields() {
        let parsed = ProfileParser::try_parse_from([
            "profile",
            "transport",
            "update",
            "uart-fast",
            "--baud-rate",
            "921600",
            "--dtr",
            "true",
            "--auto-open",
            "false",
            "--json",
        ])
        .unwrap();
        let ProfileCommand::Transport {
            command: TransportCommand::Update(args),
        } = parsed.command
        else {
            panic!("expected transport update");
        };
        assert_eq!(args.name, "uart-fast");
        assert_eq!(args.baud_rate, Some(921_600));
        assert_eq!(args.dtr, Some(true));
        assert_eq!(args.auto_open, Some(false));
        assert!(args.has_changes());
        assert!(args.json);
    }

    #[test]
    fn model_update_parser_supports_clearing_prompts_and_inheriting_other_fields() {
        let parsed = ProfileParser::try_parse_from([
            "profile",
            "model",
            "update",
            "slow-dut",
            "--clear-shell-prompt",
            "--write-eol",
            "cr",
            "--write-chunk-size",
            "8",
        ])
        .unwrap();
        let ProfileCommand::Model {
            command: ModelCommand::Update(args),
        } = parsed.command
        else {
            panic!("expected model update");
        };
        assert_eq!(args.name, "slow-dut");
        assert!(args.clear_shell_prompt);
        assert_eq!(args.write_eol.as_deref(), Some("\r"));
        assert_eq!(args.write_chunk_size, Some(8));
        assert!(args.has_changes());

        assert!(
            ProfileParser::try_parse_from([
                "profile",
                "model",
                "update",
                "slow-dut",
                "--shell-prompt",
                "# ",
                "--clear-shell-prompt",
            ])
            .is_err()
        );
    }

    #[test]
    fn update_without_fields_is_rejected_before_any_request() {
        let transport = TransportUpdateArgs {
            name: "uart".into(),
            baud_rate: None,
            data_bits: None,
            parity: None,
            stop_bits: None,
            flow_control: None,
            dtr: None,
            rts: None,
            auto_open: None,
            interactive: false,
            json: false,
        };
        assert!(!transport.has_changes());
        assert!(
            ProfileCommand::Transport {
                command: TransportCommand::Update(transport)
            }
            .validate_local()
            .is_err()
        );

        let model = ModelUpdateArgs {
            name: "dut".into(),
            shell_prompt: None,
            clear_shell_prompt: false,
            uboot_prompt: None,
            clear_uboot_prompt: false,
            write_eol: None,
            inherit_write_eol: false,
            echo: None,
            inherit_echo: false,
            write_chunk_size: None,
            inherit_write_chunk_size: false,
            write_chunk_delay_ms: None,
            inherit_write_chunk_delay: false,
            interactive: false,
            json: false,
        };
        assert!(!model.has_changes());
        assert!(
            ProfileCommand::Model {
                command: ModelCommand::Update(model)
            }
            .validate_local()
            .is_err()
        );
    }

    #[test]
    fn partial_updates_preserve_unmentioned_fields_and_can_clear_model_overrides() {
        let mut transport = safe_transport_profile();
        let transport_update = TransportUpdateArgs {
            name: transport.name.clone(),
            baud_rate: Some(921_600),
            data_bits: None,
            parity: None,
            stop_bits: None,
            flow_control: None,
            dtr: Some(true),
            rts: None,
            auto_open: None,
            interactive: false,
            json: false,
        };
        apply_transport_update(&mut transport, &transport_update);
        assert_eq!(transport.baud_rate, 921_600);
        assert!(transport.dtr);
        assert_eq!(transport.data_bits, DataBits::Eight);
        assert_eq!(transport.flow_control, FlowControl::None);
        assert!(transport.auto_open);

        let mut model = ModelProfile {
            name: "dut".into(),
            shell_prompt: Some("# ".into()),
            uboot_prompt: Some("=> ".into()),
            write_eol: Some("\r".into()),
            echo: Some(EchoMode::On),
            write_chunk_size: Some(8),
            write_chunk_delay_ms: Some(1),
        };
        let model_update = ModelUpdateArgs {
            name: model.name.clone(),
            shell_prompt: None,
            clear_shell_prompt: true,
            uboot_prompt: None,
            clear_uboot_prompt: false,
            write_eol: None,
            inherit_write_eol: false,
            echo: Some(EchoArg::Off),
            inherit_echo: false,
            write_chunk_size: None,
            inherit_write_chunk_size: false,
            write_chunk_delay_ms: Some(5),
            inherit_write_chunk_delay: false,
            interactive: false,
            json: false,
        };
        apply_model_update(&mut model, &model_update);
        assert_eq!(model.shell_prompt, None);
        assert_eq!(model.uboot_prompt.as_deref(), Some("=> "));
        assert_eq!(model.write_eol.as_deref(), Some("\r"));
        assert_eq!(model.echo, Some(EchoMode::Off));
        assert_eq!(model.write_chunk_size, Some(8));
        assert_eq!(model.write_chunk_delay_ms, Some(5));
    }
}
