#[cfg(not(windows))]
use std::io::{self, Write as _};

#[cfg(not(windows))]
use anyhow::Context;
use anyhow::Result;

/// Copies text without pulling a desktop clipboard stack into the Ubuntu
/// baseline build. Windows uses the native Unicode clipboard. Other terminals
/// receive OSC 52, which is supported by common local terminal emulators.
pub fn copy_text(text: &str) -> Result<()> {
    copy_text_impl(text)
}

/// Reads text for an in-app right-click paste. Windows exposes this through
/// the native clipboard. On Unix, the terminal remains the clipboard owner,
/// so bracketed paste (normally Ctrl+Shift+V) is the portable path.
pub fn read_text() -> Result<Option<String>> {
    read_text_impl()
}

#[cfg(not(windows))]
fn copy_text_impl(text: &str) -> Result<()> {
    let encoded = encode_base64(text.as_bytes());
    let mut stdout = io::stdout();
    write!(stdout, "\x1b]52;c;{encoded}\x07").context("cannot write OSC 52 clipboard sequence")?;
    stdout
        .flush()
        .context("cannot flush OSC 52 clipboard sequence")
}

#[cfg(not(windows))]
fn read_text_impl() -> Result<Option<String>> {
    Ok(None)
}

#[cfg(windows)]
fn copy_text_impl(text: &str) -> Result<()> {
    windows_clipboard::copy_text(text)
}

#[cfg(windows)]
fn read_text_impl() -> Result<Option<String>> {
    windows_clipboard::read_text().map(Some)
}

#[cfg(not(windows))]
fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        encoded.push(ALPHABET[(first >> 2) as usize] as char);
        encoded.push(ALPHABET[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[(third & 0x3f) as usize] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

#[cfg(windows)]
mod windows_clipboard {
    use std::{ffi::c_void, ptr};

    use anyhow::{Result, bail};

    const CF_UNICODETEXT: u32 = 13;
    const GMEM_MOVEABLE: u32 = 0x0002;

    #[link(name = "user32")]
    unsafe extern "system" {
        fn OpenClipboard(window: *mut c_void) -> i32;
        fn CloseClipboard() -> i32;
        fn EmptyClipboard() -> i32;
        fn GetClipboardData(format: u32) -> *mut c_void;
        fn SetClipboardData(format: u32, memory: *mut c_void) -> *mut c_void;
        fn IsClipboardFormatAvailable(format: u32) -> i32;
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetConsoleWindow() -> *mut c_void;
        fn GlobalAlloc(flags: u32, bytes: usize) -> *mut c_void;
        fn GlobalFree(memory: *mut c_void) -> *mut c_void;
        fn GlobalLock(memory: *mut c_void) -> *mut c_void;
        fn GlobalSize(memory: *mut c_void) -> usize;
        fn GlobalUnlock(memory: *mut c_void) -> i32;
    }

    struct ClipboardGuard;

    impl ClipboardGuard {
        fn open() -> Result<Self> {
            // SAFETY: A console/ConPTY process receives a valid message-window
            // handle from GetConsoleWindow. Supplying it matters for copy:
            // EmptyClipboard followed by SetClipboardData can fail when the
            // clipboard was opened with a null owner.
            let owner = unsafe { GetConsoleWindow() };
            if owner.is_null() {
                bail!("Windows console window is unavailable");
            }
            // SAFETY: `owner` is this console's window handle. The guard
            // closes every successful open.
            if unsafe { OpenClipboard(owner) } == 0 {
                bail!("Windows clipboard is busy");
            }
            Ok(Self)
        }
    }

    impl Drop for ClipboardGuard {
        fn drop(&mut self) {
            // SAFETY: This guard exists only after OpenClipboard succeeded.
            unsafe {
                CloseClipboard();
            }
        }
    }

    pub fn copy_text(text: &str) -> Result<()> {
        let _clipboard = ClipboardGuard::open()?;
        // SAFETY: Clipboard is open on this thread.
        if unsafe { EmptyClipboard() } == 0 {
            bail!("cannot clear the Windows clipboard");
        }

        let encoded = text.encode_utf16().chain([0]).collect::<Vec<_>>();
        let byte_len = encoded.len() * std::mem::size_of::<u16>();
        // SAFETY: GlobalAlloc returns an owned movable block or null.
        let memory = unsafe { GlobalAlloc(GMEM_MOVEABLE, byte_len) };
        if memory.is_null() {
            bail!("cannot allocate Windows clipboard memory");
        }

        // SAFETY: `memory` is a live allocation of `byte_len` bytes and the
        // source has exactly the same initialized length.
        let destination = unsafe { GlobalLock(memory) }.cast::<u16>();
        if destination.is_null() {
            // SAFETY: Ownership has not been transferred to the clipboard.
            unsafe {
                GlobalFree(memory);
            }
            bail!("cannot lock Windows clipboard memory");
        }
        // SAFETY: Both slices are valid and non-overlapping for encoded.len().
        unsafe {
            ptr::copy_nonoverlapping(encoded.as_ptr(), destination, encoded.len());
            GlobalUnlock(memory);
        }

        // SAFETY: Ownership of a successfully published HGLOBAL transfers to
        // the clipboard. On failure it remains ours and must be freed.
        if unsafe { SetClipboardData(CF_UNICODETEXT, memory) }.is_null() {
            unsafe {
                GlobalFree(memory);
            }
            bail!("cannot publish Unicode text to the Windows clipboard");
        }
        Ok(())
    }

    pub fn read_text() -> Result<String> {
        let _clipboard = ClipboardGuard::open()?;
        // SAFETY: Clipboard is open and the format constant is valid.
        if unsafe { IsClipboardFormatAvailable(CF_UNICODETEXT) } == 0 {
            bail!("Windows clipboard does not contain Unicode text");
        }
        // SAFETY: Clipboard owns the returned handle.
        let memory = unsafe { GetClipboardData(CF_UNICODETEXT) };
        if memory.is_null() {
            bail!("cannot read Unicode text from the Windows clipboard");
        }
        // SAFETY: CF_UNICODETEXT is a NUL-terminated UTF-16 allocation.
        let pointer = unsafe { GlobalLock(memory) }.cast::<u16>();
        if pointer.is_null() {
            bail!("cannot lock Windows clipboard text");
        }
        // SAFETY: `memory` is the HGLOBAL returned for CF_UNICODETEXT.
        let capacity = unsafe { GlobalSize(memory) } / std::mem::size_of::<u16>();
        let mut length = 0usize;
        // SAFETY: `length` is bounded by the allocation size reported above.
        while length < capacity && unsafe { *pointer.add(length) } != 0 {
            length += 1;
        }
        if length == capacity {
            // SAFETY: `memory` was successfully locked by this call.
            unsafe {
                GlobalUnlock(memory);
            }
            bail!("Windows clipboard Unicode text is not NUL-terminated");
        }
        // SAFETY: The scan above established `length` initialized u16 values.
        let text = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(pointer, length) });
        // SAFETY: `memory` was successfully locked by this call.
        unsafe {
            GlobalUnlock(memory);
        }
        Ok(text)
    }
}

#[cfg(all(test, not(windows)))]
mod tests {
    use super::encode_base64;

    #[test]
    fn osc52_payload_uses_standard_base64() {
        assert_eq!(encode_base64(b""), "");
        assert_eq!(encode_base64(b"f"), "Zg==");
        assert_eq!(encode_base64(b"fo"), "Zm8=");
        assert_eq!(encode_base64(b"foo"), "Zm9v");
        assert_eq!(encode_base64("串口".as_bytes()), "5Liy5Y+j");
    }
}
