//! A simple TTY echo example.
//!
//! Reads lines from standard input and echoes them back until
//! the user types `/exit`.

use anyhow::Result;
use crabuv::tty::TtyHandle;
use crabuv::EventLoop;
use crabuv::RunMode;

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;

fn main() {
    let mut event_loop = EventLoop::default();
    let handle = event_loop.handle();

    #[cfg(unix)]
    let stdin = std::io::stdin().as_raw_fd();
    #[cfg(windows)]
    let stdin = std::io::stdin().as_raw_handle();

    let tty = handle.tty(stdin);

    tty.start_reading(|tty: TtyHandle, data: Result<Vec<u8>>| match data {
        Ok(bytes) => {
            let line = String::from_utf8_lossy(&bytes);
            let line = line.trim();

            if line == "/exit" {
                tty.stop_reading();
                return;
            }
            println!("You typed: '{line}'");
        }
        Err(e) => {
            eprintln!("Read error: {e}");
            tty.stop_reading();
        }
    });

    println!("Type something (or /exit to quit):");

    event_loop.run(RunMode::Default);
}
