# Exercise the packaged window's non-client hit testing, independently of React/CSS inspection.
param([Parameter(Mandatory)][IntPtr] $WindowHandle)

Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class SerialTitleBar {
    [StructLayout(LayoutKind.Sequential)] public struct Point { public int X, Y; }
    [StructLayout(LayoutKind.Sequential)] public struct Rect { public int Left, Top, Right, Bottom; }
    [DllImport("user32.dll")] public static extern bool GetClientRect(IntPtr hwnd, out Rect rect);
    [DllImport("user32.dll")] public static extern bool ClientToScreen(IntPtr hwnd, ref Point point);
    [DllImport("user32.dll")] public static extern uint GetDpiForWindow(IntPtr hwnd);
    [DllImport("user32.dll")] static extern IntPtr SendMessageTimeout(IntPtr hwnd, uint msg, IntPtr wParam,
        IntPtr lParam, uint flags, uint timeout, out UIntPtr result);
    public static int Hit(IntPtr hwnd, int x, int y) {
        var point = new Point { X = x, Y = y };
        if (!ClientToScreen(hwnd, ref point)) return -1;
        int packed = (point.X & 0xffff) | ((point.Y & 0xffff) << 16);
        UIntPtr result;
        return SendMessageTimeout(hwnd, 0x84, IntPtr.Zero, (IntPtr)packed, 2, 1000, out result) == IntPtr.Zero
            ? -1 : (int)result.ToUInt32();
    }
}
'@
$bounds = [SerialTitleBar+Rect]::new()
if (-not [SerialTitleBar]::GetClientRect($WindowHandle, [ref] $bounds)) { throw 'Cannot read the desktop client area.' }
$scale = [SerialTitleBar]::GetDpiForWindow($WindowHandle) / 96.0
$y = [int](28 * $scale)
$deadline = [DateTime]::UtcNow.AddSeconds(20)
do {
    $middle = [SerialTitleBar]::Hit($WindowHandle, [int]($bounds.Right / 2), $y)
    $navigation = [SerialTitleBar]::Hit($WindowHandle, [int](50 * $scale), $y)
    $close = [SerialTitleBar]::Hit($WindowHandle, $bounds.Right - [int](22 * $scale), $y)
    if ($middle -eq 2 -and $navigation -eq 1 -and $close -eq 1) {
        Write-Host 'Windows recognizes the middle of the title bar as draggable and both control areas as clickable.'
        return
    }
    Start-Sleep -Milliseconds 250
} until ([DateTime]::UtcNow -ge $deadline)
throw "Unexpected WM_NCHITTEST results: middle=$middle, navigation=$navigation, close=$close."
