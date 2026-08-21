export type StartupAction = 'gui' | 'help' | 'version'

export function startupAction(args: string[]): StartupAction {
  if (args.includes('--help') || args.includes('-h')) return 'help'
  if (args.includes('--version') || args.includes('-V')) return 'version'
  return 'gui'
}

export const HELP = `Serial Platform Desktop

Usage: serial-desktop [OPTIONS]

Options:
  -h, --help       Print help and exit
  -V, --version    Print version and exit
`
