use crate::event_loop::LoopHandle;
use crate::resource::Resource;
use crate::resource::ResourceId;
use crate::resource::Shared;
use anyhow::Result;
use crossterm::terminal;
use std::fs;
use std::io::Write;
use std::mem::ManuallyDrop;
use std::rc::Rc;
use std::sync::mpsc;

pub type OnReadCallback = Box<dyn FnMut(TtyHandle, Result<Vec<u8>>) + 'static>;

#[cfg(unix)]
pub(crate) mod unix_imports {
    pub use rustix::termios::tcgetattr;
    pub use rustix::termios::tcgetwinsize;
    pub use rustix::termios::tcsetattr;
    pub use rustix::termios::OptionalActions;
    pub use rustix::termios::OutputModes;
    pub use std::os::fd::BorrowedFd;
    pub use std::os::fd::FromRawFd;
    pub use std::os::unix::io::AsRawFd;
}

#[cfg(unix)]
pub type FileDescriptor = std::os::fd::RawFd;

#[cfg(unix)]
use unix_imports::*;

#[cfg(windows)]
pub(crate) mod windows_imports {
    pub use anyhow::bail;
    pub use std::io::IsTerminal;
    pub use std::os::windows::io::AsRawHandle;
    pub use std::os::windows::io::BorrowedHandle;
    pub use std::os::windows::io::FromRawHandle;
    pub use windows_sys::Win32::System::Console::GetConsoleScreenBufferInfo;
    pub use windows_sys::Win32::System::Console::SetConsoleCP;
    pub use windows_sys::Win32::System::Console::SetConsoleOutputCP;
    pub use windows_sys::Win32::System::Console::CONSOLE_SCREEN_BUFFER_INFO;
}

#[cfg(windows)]
pub type FileDescriptor = std::os::windows::io::RawHandle;

#[cfg(windows)]
use windows_imports::*;

/// Terminal input mode.
#[derive(Debug)]
pub enum Mode {
    Normal,
    Raw,
}

/// Represents the visible dimensions of a terminal window.
#[derive(Debug)]
pub struct WindowSize {
    pub columns: u16,
    pub rows: u16,
}

/// The internal worker state for a TTY resource.
pub(crate) struct TtyReader {
    pub id: Shared<ResourceId>,
    pub raw_fd: FileDescriptor,
    pub on_read: OnReadCallback,
    pub stop_tx: mpsc::Sender<()>,
}

impl TtyReader {
    /// Returns a handle to the TTY resource.
    pub fn handle(&self, handle: LoopHandle) -> TtyHandle {
        TtyHandle {
            id: Rc::clone(&self.id),
            raw_fd: self.raw_fd,
            handle,
        }
    }

    /// Returns a readable stream over the underlying file descriptor.
    pub fn get_stream(&self) -> FileDescriptorStream {
        FileDescriptorStream::from(self.raw_fd)
    }
}

impl Resource for TtyReader {}

/// A reference like struct to a TTY instance.
#[derive(Debug, Clone)]
pub struct TtyHandle {
    /// The actual raw file-descriptor the TTY wraps.
    pub(crate) raw_fd: FileDescriptor,
    /// A shared pointer to the resource ID of the tty.
    pub(crate) id: Shared<ResourceId>,
    /// A handle to the event-loop.
    pub(crate) handle: LoopHandle,
}

impl TtyHandle {
    /// Writes data to the underlying file descriptor.
    pub fn write<D>(&self, data: D) -> Result<()>
    where
        D: AsRef<[u8]>,
    {
        let mut stream = FileDescriptorStream::from(self.raw_fd);

        stream.write_all(data.as_ref())?;
        stream.flush()?;

        Ok(())
    }

    /// Starts reading from the TTY.
    pub fn start_reading<F>(&self, callback: F)
    where
        F: FnMut(TtyHandle, Result<Vec<u8>>) + 'static,
    {
        // This channel is used to stop the worker thread reading from stdin.
        let (stop_tx, stop_rx) = mpsc::channel();
        let on_read = Box::new(callback);

        let reader = TtyReader {
            id: Rc::clone(&self.id),
            raw_fd: self.raw_fd,
            on_read,
            stop_tx,
        };

        self.handle.tty_read_start(reader, stop_rx);
    }

    /// Stops reading from the TTY.
    pub fn stop_reading(&self) {
        self.handle.tty_close(Rc::clone(&self.id));
    }

    #[cfg(unix)]
    fn enable_opost(&self) -> Result<()> {
        let fd = unsafe { BorrowedFd::borrow_raw(self.raw_fd) };
        let mut termios = tcgetattr(fd)?;

        // Raw mode turns off OPOST, so "\n" no longer moves the cursor to the
        // start of the next line. Turn it back on.
        termios.output_modes |= OutputModes::OPOST | OutputModes::ONLCR;
        tcsetattr(fd, OptionalActions::Now, &termios)?;

        Ok(())
    }

    /// Applies the specified terminal mode to the TTY.
    pub fn set_mode(&self, mode: Mode) -> Result<()> {
        match mode {
            Mode::Raw => {
                terminal::enable_raw_mode()?;
                #[cfg(unix)]
                self.enable_opost()?;
                Ok(())
            }
            Mode::Normal => {
                terminal::disable_raw_mode()?;
                Ok(())
            }
        }
    }

    /// Gets the current window size.
    #[cfg(unix)]
    pub fn window_size(&self) -> Result<WindowSize> {
        let fd = unsafe { BorrowedFd::borrow_raw(self.raw_fd) };
        let size = tcgetwinsize(fd)?;

        Ok(WindowSize {
            columns: size.ws_col,
            rows: size.ws_row,
        })
    }

    /// Gets the current window size.
    #[cfg(windows)]
    pub fn window_size(&self) -> Result<WindowSize> {
        use anyhow::bail;
        use std::io;

        let mut size: CONSOLE_SCREEN_BUFFER_INFO = unsafe { std::mem::zeroed() };
        let result = unsafe { GetConsoleScreenBufferInfo(self.raw_fd, &mut size) };

        if result == 0 {
            bail!(io::Error::last_os_error());
        }

        // Note: srWindow contains inclusive coordinates, not dimensions, so we calculate
        // the visible width and height by subtracting the boundaries and adding 1.
        let columns = (size.srWindow.Right - size.srWindow.Left + 1) as u16;
        let rows = (size.srWindow.Bottom - size.srWindow.Top + 1) as u16;

        Ok(WindowSize { columns, rows })
    }

    /// Returns a handle to the event-loop.
    pub fn loop_handle(&self) -> LoopHandle {
        self.handle.clone()
    }
}

/// A readable/writable stream over a borrowed file descriptor.
pub(crate) struct FileDescriptorStream {
    file: ManuallyDrop<fs::File>,
}

impl From<FileDescriptor> for FileDescriptorStream {
    #[cfg(unix)]
    fn from(value: FileDescriptor) -> Self {
        // Safety: fd is valid for the duration of this call.
        let borrowed = unsafe { BorrowedFd::borrow_raw(value) };
        let file = unsafe { ManuallyDrop::new(fs::File::from_raw_fd(borrowed.as_raw_fd())) };

        FileDescriptorStream { file }
    }

    #[cfg(windows)]
    fn from(value: FileDescriptor) -> Self {
        // Safety: fd is valid for the duration of this call.
        let borrowed = unsafe { BorrowedHandle::borrow_raw(value) };
        let file =
            unsafe { ManuallyDrop::new(fs::File::from_raw_handle(borrowed.as_raw_handle())) };

        FileDescriptorStream { file }
    }
}

impl std::io::Read for FileDescriptorStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl std::io::Write for FileDescriptorStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

#[cfg(unix)]
pub fn is_tty(fd: FileDescriptor) -> bool {
    use crossterm::tty::IsTty;
    fd.is_tty()
}

#[cfg(windows)]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn is_tty(fd: FileDescriptor) -> bool {
    let handle = unsafe { BorrowedHandle::borrow_raw(fd) };
    handle.is_terminal()
}

/// Sets the Windows console input and output code pages to UTF-8.
#[cfg(windows)]
pub fn set_utf8_code_pages() -> Result<()> {
    // Without this, conhost reads our UTF-8 through the ANSI code page
    // (CP1252, CP932, ...) and the bar renders as mojibake.
    if unsafe { SetConsoleOutputCP(65001) } == 0 {
        bail!(std::io::Error::last_os_error());
    }

    // Set the input code page to UTF-8 (best effort).
    unsafe { SetConsoleCP(65001) };

    Ok(())
}
