mod api;
mod cli;
mod clipboard;
mod config;
mod display;
mod doctor;
mod history;
mod i18n;
mod model;
mod profile;
mod tui;
mod ws;

pub use cli::{Cli, run};

pub const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:3210";

/// Parse and execute serialctl-compatible arguments from another binary.
///
/// A future unified `serial console ...` entrypoint can prepend an arbitrary
/// program name and reuse the exact same validation without spawning a child
/// process.
pub async fn run_from<I, T>(arguments: I) -> anyhow::Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    use clap::Parser as _;

    run(Cli::try_parse_from(arguments)?).await
}
