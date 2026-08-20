#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

use std::ffi::OsString;

use eframe::egui;
use serial_desktop::app::DesktopApp;

const HELP: &str = "\
Serial Platform Desktop

Usage: serial-desktop [OPTIONS]

Options:
  -h, --help       Print help and exit
  -V, --version    Print version and exit
";

#[derive(Debug, Eq, PartialEq)]
enum StartupAction {
    Gui,
    Help,
    Version,
}

fn startup_action(args: impl IntoIterator<Item = OsString>) -> Result<StartupAction, String> {
    let mut args = args.into_iter();
    let Some(argument) = args.next() else {
        return Ok(StartupAction::Gui);
    };

    let action = match argument.to_str() {
        Some("-h" | "--help") => StartupAction::Help,
        Some("-V" | "--version") => StartupAction::Version,
        Some(value) => return Err(format!("unknown argument: {value}")),
        None => return Err("argument is not valid UTF-8".to_string()),
    };
    if let Some(extra) = args.next() {
        return Err(format!("unexpected argument: {}", extra.to_string_lossy()));
    }
    Ok(action)
}

fn main() -> eframe::Result {
    match startup_action(std::env::args_os().skip(1)) {
        Ok(StartupAction::Gui) => {}
        Ok(StartupAction::Help) => {
            print!("{HELP}");
            return Ok(());
        }
        Ok(StartupAction::Version) => {
            println!("serial-desktop {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Err(error) => {
            eprintln!("error: {error}\n\n{HELP}");
            std::process::exit(2);
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "serial_desktop=info".into()),
        )
        .with_target(false)
        .init();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_app_id("serial-platform-desktop")
            .with_inner_size([1_280.0, 800.0])
            .with_min_inner_size([900.0, 600.0]),
        centered: true,
        ..Default::default()
    };
    eframe::run_native(
        "Serial Platform",
        options,
        Box::new(|creation| Ok(Box::new(DesktopApp::new(creation)))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn no_arguments_launches_the_gui() {
        assert_eq!(startup_action(args(&[])), Ok(StartupAction::Gui));
    }

    #[test]
    fn help_and_version_exit_before_the_gui() {
        assert_eq!(startup_action(args(&["--help"])), Ok(StartupAction::Help));
        assert_eq!(startup_action(args(&["-V"])), Ok(StartupAction::Version));
    }

    #[test]
    fn unsupported_or_extra_arguments_fail_closed() {
        assert_eq!(
            startup_action(args(&["--unknown"])),
            Err("unknown argument: --unknown".to_string())
        );
        assert_eq!(
            startup_action(args(&["--version", "extra"])),
            Err("unexpected argument: extra".to_string())
        );
    }
}
