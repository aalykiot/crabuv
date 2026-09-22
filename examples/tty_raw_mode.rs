//! A raw TTY input example.
//!
//! Enables raw mode on the terminal and reads individual keypresses.
//! Each character is echoed back with its ASCII representation and hex value.
//! Press 'q' to quit.

use anyhow::Result;
use crabuv::tty::Mode;
use crabuv::tty::TtyHandle;
use crabuv::EventLoop;
use crabuv::RunMode;
use std::ops::RangeInclusive;
use std::os::fd::AsRawFd;

const PRINTABLE_RANGE: RangeInclusive<u8> = 32..=126;

fn display_char(byte: u8) -> char {
    if PRINTABLE_RANGE.contains(&byte) {
        byte as char
    } else {
        '?'
    }
}

fn handle_input(tty: TtyHandle, data: Result<Vec<u8>>) {
    match data {
        Ok(bytes) => {
            let byte = bytes[0];
            print!("You typed: '{}' (0x{byte:02x})\r\n", display_char(byte));

            if byte == b'q' {
                print!("Exiting...\r\n");
                tty.set_mode(Mode::Normal).unwrap();
                tty.stop_reading();
            }
        }
        Err(e) => {
            eprintln!("Read error: {e}\r\n");
            tty.set_mode(Mode::Normal).unwrap();
            tty.stop_reading();
        }
    }
}

fn main() {
    let mut event_loop = EventLoop::default();
    let handle = event_loop.handle();
    let tty = handle.tty(std::io::stdin().as_raw_fd());

    if let Err(e) = tty.set_mode(Mode::Raw) {
        eprintln!("Failed to enter raw mode: {e}");
        return;
    }

    print!("Raw mode enabled.\r\n");
    print!("Press keys (press 'q' to quit)...\r\n");

    tty.start_reading(handle_input);
    event_loop.run(RunMode::Default);
}
