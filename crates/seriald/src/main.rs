use anyhow::Context as _;
use clap::{Parser, Subcommand};
use seriald::config::{ConfigPaths, ConfigStore};
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "seriald", version, about = "Shared serial-port control daemon")]
struct Cli {
    /// Use a portable config/data root instead of the OS user directories.
    #[arg(long, global = true, env = "SERIALD_ROOT")]
    root: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the daemon in the foreground (the default command).
    Serve {
        /// Runtime-only bind override. Use the Windows host-only adapter IP for VM access.
        #[arg(long)]
        bind: Option<SocketAddr>,
        /// Exit cleanly when the parent launcher closes stdin.
        #[arg(long, hide = true)]
        managed: bool,
    },
    /// Print the resolved configuration and data paths.
    Paths,
    /// Print the verified endpoint currently owning this data root.
    #[command(hide = true)]
    Discover {
        /// Emit the complete active-endpoint record as JSON.
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("seriald=info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let store = match cli.root {
        Some(root) => ConfigStore::new(ConfigPaths::from_root(&root)),
        None => ConfigStore::platform_default().context("resolve seriald directories")?,
    };

    match cli.command.unwrap_or(Command::Serve {
        bind: None,
        managed: false,
    }) {
        Command::Serve { bind, managed } => {
            let instance = seriald::runtime::ActiveInstance::acquire(store.paths())
                .context("claim the seriald data root")?;
            let loaded = store
                .load_or_create()
                .context("load seriald configuration")?;
            seriald::serve(store, loaded, bind, managed, instance).await
        }
        Command::Paths => {
            println!("config={}", store.paths().config_file.display());
            println!("data={}", store.paths().data_dir.display());
            println!("journal={}", store.paths().journal_dir.display());
            Ok(())
        }
        Command::Discover { json } => {
            if !store.paths().config_file.exists() {
                return Ok(());
            }
            let config = store.load().context("load seriald configuration")?;
            if let Some(active) = seriald::runtime::discover_active(store.paths(), config.server_id)
                .context("discover the active seriald endpoint")?
            {
                if json {
                    println!("{}", serde_json::to_string(&active)?);
                } else {
                    println!("{}", active.endpoint);
                }
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn portable_root_is_a_global_daemon_option() {
        for args in [
            vec!["seriald", "--root", "portable", "serve"],
            vec!["seriald", "serve", "--root", "portable"],
        ] {
            let parsed = Cli::try_parse_from(args).expect("--root should parse around serve");
            assert_eq!(parsed.root, Some(PathBuf::from("portable")));
            assert!(matches!(parsed.command, Some(Command::Serve { .. })));
        }
    }

    #[test]
    fn managed_serve_is_an_internal_parent_lifecycle_mode() {
        let parsed = Cli::try_parse_from(["seriald", "serve", "--managed"]).unwrap();
        assert!(matches!(
            parsed.command,
            Some(Command::Serve { managed: true, .. })
        ));
    }

    #[test]
    fn discovery_is_scoped_by_the_global_portable_root() {
        let parsed =
            Cli::try_parse_from(["seriald", "--root", "portable", "discover", "--json"]).unwrap();
        assert_eq!(parsed.root, Some(PathBuf::from("portable")));
        assert!(matches!(
            parsed.command,
            Some(Command::Discover { json: true })
        ));
    }
}
