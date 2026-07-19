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
    #[arg(long, env = "SERIALD_ROOT")]
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
    /// Explicitly display the three role credentials.
    Credentials,
    /// Print the resolved configuration and data paths.
    Paths,
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
                println!("seriald created its first configuration.");
                print_credentials(credentials);
                println!(
                    "Save the needed token in serialctl init; it will not be printed again automatically."
                );
            }
            seriald::serve(store, loaded, bind).await
        }
        Command::Credentials => {
            let loaded = store
                .load_or_create()
                .context("load seriald configuration")?;
            let credentials = loaded.config.auth.credentials_for_explicit_display();
            print_credentials(&credentials);
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
