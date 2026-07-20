mod api;
mod capture;
mod config;
mod mcp;
mod render;
mod session;
mod tools;

use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Parser;

#[derive(Parser)]
#[command(name = "serial-mcp", version, about = "MCP adapter for seriald")]
struct Args {
    /// serialctl-compatible TOML config path.
    #[arg(long, env = "SERIALCTL_CONFIG")]
    config: Option<PathBuf>,
    /// seriald HTTP origin, normally the Windows host-only address from a Linux VM.
    #[arg(long, env = "SERIALD_ENDPOINT")]
    endpoint: Option<String>,
    /// File containing the seriald operator token. Never pass the token itself.
    #[arg(long, env = "SERIALD_TOKEN_FILE")]
    token_file: Option<PathBuf>,
    /// Stable audit label for this agent adapter process.
    #[arg(long, env = "SERIAL_MCP_ACTOR_LABEL", default_value = "agent")]
    actor_label: String,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("serial-mcp: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();
    let actor_label = args.actor_label.trim().to_string();
    if actor_label.is_empty()
        || actor_label.len() > 128
        || actor_label.chars().any(char::is_control)
    {
        bail!("actor label must contain 1-128 non-control UTF-8 bytes");
    }
    let resolved = config::resolve(args.config, args.endpoint, args.token_file)?;
    let api = api::ApiClient::new(resolved.endpoint.clone(), resolved.token.clone())?;
    let session =
        session::SessionHandle::spawn(resolved.endpoint, resolved.token, actor_label.clone());
    mcp::serve(tools::AgentTools::new(
        api,
        session,
        actor_label,
        resolved.capture,
    ))
    .await
}
