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
    },
    /// Display credentials, or enable auth while seriald is stopped.
    Credentials,
    /// Manage authentication while seriald is stopped.
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    /// Print the resolved configuration and data paths.
    Paths,
}

#[derive(Debug, Subcommand)]
enum AuthCommand {
    /// Remove every seriald bearer token (loopback configurations only).
    Disable,
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

    match cli.command.unwrap_or(Command::Serve { bind: None }) {
        Command::Serve { bind } => {
            let loaded = store
                .load_or_create()
                .context("load seriald configuration")?;
            if let Some(credentials) = loaded.initial_credentials.as_ref() {
                println!("seriald created its first authenticated configuration.");
                print_credentials(credentials);
            }
            seriald::serve(store, loaded, bind).await
        }
        Command::Credentials => {
            let mut loaded = store
                .load_or_create()
                .context("load seriald configuration")?;
            if let Some(auth) = loaded.config.auth.as_ref() {
                print_credentials(&auth.credentials_for_explicit_display());
            } else {
                let (auth, credentials) = seriald::auth::AuthConfig::generate();
                loaded.config.auth_required = true;
                loaded.config.auth = Some(auth);
                store
                    .save(&loaded.config)
                    .context("enable authentication in seriald configuration")?;
                println!(
                    "authentication=enabled; seriald must have been stopped before this command; start it again before using these tokens"
                );
                print_credentials(&credentials);
            }
            Ok(())
        }
        Command::Auth {
            command: AuthCommand::Disable,
        } => {
            let loaded = store
                .load_or_create()
                .context("load seriald configuration")?;
            if !loaded.config.auth_required {
                println!("authentication=disabled (loopback only; no tokens exist)");
                return Ok(());
            }
            let staged = loaded
                .config
                .staged_without_authentication()
                .context("disable authentication; change bind to loopback first")?;
            store
                .save(&staged)
                .context("remove credentials from seriald configuration")?;
            println!(
                "authentication=disabled; all seriald tokens were removed; seriald must have been stopped before this command; start it again on loopback"
            );
            Ok(())
        }
        Command::Paths => {
            println!("config={}", store.paths().config_file.display());
            println!("data={}", store.paths().data_dir.display());
            println!("journal={}", store.paths().journal_dir.display());
            Ok(())
        }
    }
}

fn print_credentials(credentials: &seriald::auth::CredentialDisplay) {
    println!("observer_token={}", credentials.observer_token());
    println!("operator_token={}", credentials.operator_token());
    println!("admin_token={}", credentials.admin_token());
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
    fn auth_disable_is_a_daemon_subcommand() {
        let parsed = Cli::try_parse_from(["seriald", "auth", "disable"]).unwrap();
        assert!(matches!(
            parsed.command,
            Some(Command::Auth {
                command: AuthCommand::Disable
            })
        ));
    }
}
