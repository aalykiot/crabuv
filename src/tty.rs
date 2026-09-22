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
pub type FileDescriptor = std::os::fd::RawFd;

#[cfg(windows)]
pub type FileDescriptor = std::os::windows::io::RawHandle;

/// Terminal input mode.
pub enum Mode {
    /// Line-buffered, processed by terminal driver.
    Normal,
    /// Input delivered directly, bypassing terminal driver.
    Raw,
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

    /// Sets the TTY to raw mode.
    fn enable_raw_mode(&self) -> Result<()> {
        terminal::enable_raw_mode().map_err(Into::into)
    }

    /// Disables raw mode, restoring the terminal to its original settings.
    pub fn disable_raw_mode(&self) -> Result<()> {
        terminal::disable_raw_mode().map_err(Into::into)
    }

    /// Applies the specified terminal mode to the TTY.
    pub fn set_mode(&self, mode: Mode) -> Result<()> {
        match mode {
            Mode::Normal => self.disable_raw_mode(),
            Mode::Raw => self.enable_raw_mode(),
        }
    }

    /// Gets the current window size.
    pub fn window_size(&self) -> Result<terminal::WindowSize> {
        terminal::window_size().map_err(Into::into)
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
        use std::os::fd::BorrowedFd;
        use std::os::fd::FromRawFd;
        use std::os::unix::io::AsRawFd;

        // Safety: fd is valid for the duration of this call.
        let borrowed = unsafe { BorrowedFd::borrow_raw(value) };
        let file = unsafe { ManuallyDrop::new(fs::File::from_raw_fd(borrowed.as_raw_fd())) };

        FileDescriptorStream { file }
    }

    #[cfg(windows)]
    fn from(value: FileDescriptor) -> Self {
        use std::os::windows::io::AsRawHandle;
        use std::os::windows::io::BorrowedHandle;
        use std::os::windows::io::FromRawHandle;

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
