mod api;
mod capture;
mod config;
mod mcp;
mod render;
mod session;
mod tools;

use std::{net::SocketAddr, path::PathBuf};

use anyhow::{Result, bail};
use clap::Parser;

#[derive(Parser)]
#[command(name = "serial-mcp", version, about = "MCP adapter for seriald")]
struct Args {
    /// Print the complete MCP tool definitions as JSON without connecting.
    #[arg(long)]
    dump_tools: bool,
    /// serialctl-compatible TOML config path.
    #[arg(long, env = "SERIALCTL_CONFIG")]
    config: Option<PathBuf>,
    /// seriald HTTP origin, normally the Windows host-only address from a Linux VM.
    #[arg(long, env = "SERIALD_ENDPOINT")]
    endpoint: Option<String>,
    /// Serve sessionless MCP Streamable HTTP on this loopback address instead
    /// of stdio. The unified `serial` launcher uses 127.0.0.1:3211.
    #[arg(long, env = "SERIAL_MCP_LISTEN")]
    listen: Option<SocketAddr>,
    /// Exit cleanly when the parent launcher closes stdin.
    #[arg(long, hide = true)]
    managed: bool,
    /// Stable audit label for this agent adapter process.
    #[arg(long, env = "SERIAL_MCP_ACTOR_LABEL", default_value = "agent")]
    actor_label: String,
    /// Seconds an unpinned Run may remain idle before abort; 0 is unlimited
    /// and requires every workflow to call run_end explicitly.
    #[arg(long, env = "SERIAL_MCP_ORPHAN_RUN_TIMEOUT_SECONDS")]
    orphan_run_timeout_seconds: Option<u64>,
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
    if args.dump_tools {
        println!(
            "{}",
            serde_json::to_string_pretty(&mcp::tool_definitions())?
        );
        return Ok(());
    }
    let actor_label = args.actor_label.trim().to_string();
    if actor_label.is_empty()
        || actor_label.len() > 128
        || actor_label.chars().any(char::is_control)
    {
        bail!("actor label must contain 1-128 non-control UTF-8 bytes");
    }
    let resolved = config::resolve(args.config, args.endpoint, args.orphan_run_timeout_seconds)?;
    if resolved.orphan_run_timeout.is_none() {
        eprintln!(
            "serial-mcp: warning: orphan Run timeout is unlimited; every workflow must call \
             run_end, or this process will keep renewing its 60-second control lease"
        );
    }
    let api = api::ApiClient::new(resolved.endpoint.clone())?;
    let session = session::SessionHandle::spawn(
        resolved.endpoint,
        actor_label.clone(),
        resolved.orphan_run_timeout,
    );
    let tools = tools::AgentTools::new(api, session, actor_label, resolved.capture);
    match args.listen {
        Some(listen) => mcp::serve_http(tools, listen, args.managed).await,
        None => mcp::serve_stdio(tools).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dump_tools_needs_no_connection_arguments() {
        let args = Args::try_parse_from(["serial-mcp", "--dump-tools"]).unwrap();
        assert!(args.dump_tools);
        assert!(args.config.is_none());
        assert!(args.endpoint.is_none());
        assert!(args.listen.is_none());
        assert!(!args.managed);
        assert!(args.orphan_run_timeout_seconds.is_none());
    }

    #[test]
    fn managed_http_mode_is_available_to_the_unified_launcher() {
        let args = Args::try_parse_from(["serial-mcp", "--listen", "127.0.0.1:3211", "--managed"])
            .unwrap();
        assert!(args.managed);
        assert_eq!(args.listen, Some("127.0.0.1:3211".parse().unwrap()));
    }
}
