# crabuv

This library is a multi-platform support library with a focus on asynchronous I/O. It was primarily developed for use by [Dune](https://github.com/aalykiot/dune), but can be also used in any Rust project.

![GitHub](https://img.shields.io/github/license/aalykiot/dune-event-loop?style=flat-square)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/aalykiot/dune-event-loop/ci.yml?branch=main&style=flat-square)

## Features

- Timers
- Asynchronous TCP sockets
- Thread pool
- File system events
- Signals
- ANSI escape code controlled TTY

## Documentation

**Timer handles** are used to schedule callbacks to be called in the future.

```rust
fn main() {
    let mut event_loop = EventLoop::default();
    let handle = event_loop.handle();

    handle.timer(Duration::from_secs(2), TimerKind::Timeout, |_: LoopHandle| {
        println!("Hello, world!");
    });

    event_loop.run(RunMode::Default);
}
```

This library also provides a **thread-pool** which can be used to run user code and get notified in the loop thread. This thread pool is internally used by the Dune runtime to run all file system operations, as well as DNS lookups.

```rust
fn main() {
    let mut event_loop = EventLoop::default();
    let handle = event_loop.handle();

    let read_file = || -> Result<String> { Ok(fs::read_to_string("./examples/async.rs")?) };

    let read_file_cb = |_: LoopHandle, content: Result<String>| match content {
        Ok(text) => println!("{text}"),
        Err(e) => eprintln!("{}", e.to_string()),
    };

    handle.spawn(read_file, Some(read_file_cb));

    event_loop.run(RunMode::Default);
}
```

**TCP handles** are used to create TCP socket streams.

```rust
fn main() {
    let address = "0.0.0.0:3000".parse().unwrap();
    let mut event_loop = EventLoop::default();
    let handle = event_loop.handle();

    let on_close = |_: LoopHandle| println!("Connection closed.");
    let on_write = |_: TcpStreamHandle, result: Result<usize>| {
        if let Err(e) = result {
            eprintln!("{}", e);
        }
    };

    let on_read = move |stream: TcpStreamHandle, data: Result<Vec<u8>>| {
        match data {
            Ok(data) if data.is_empty() => stream.close(on_close),
            Ok(data) => stream.write(data, on_write),
            Err(e) => eprintln!("{}", e),
        };
    };

    let on_connection = move |_: TcpListenerHandle, stream: Result<TcpStreamHandle>| match stream {
        Ok(stream) => stream.start_reading(on_read),
        Err(e) => eprintln!("{}", e),
    };

    match handle.tcp_listen(address, on_connection) {
        Ok(_) => println!("Server is listening on {address}"),
        Err(e) => eprintln!("{}", e),
    };

    event_loop.run(RunMode::Default);
}
```

You can also connect to a remote host using a **TCP handle**.

```rust
handle.tcp_connect("188.184.67.127:80".parse()?, on_connection);
```

**FS handles** are used to watch specified paths for changes.

```rust
fn main() {
    let mut event_loop = EventLoop::default();
    let handle = event_loop.handle();

    let directory = "./examples/";
    let mode = WatchMode::Recursive;

    let on_event = |_: FsWatcherHandle, event: Result<FsEvent>| {
        println!("{event:?}");
    };

    let timeout = Duration::from_secs(10);
    let watcher = handle.fs_watcher(directory, mode, on_event).unwrap();

    let on_timeout = move |_: LoopHandle| {
        watcher.stop();
    };

    handle.timer(timeout, TimerKind::Timeout, on_timeout);

    event_loop.run(RunMode::Default);
}
```

**Signal handles** are wrappers around UNIX signals with some Windows support as well.

> Certain signals, such as `SIGKILL` or `SIGSTOP`, cannot be overridden or subscribed to. Additionally, on the Windows platform, only `SIGINT` is supported.

```rust
fn main() {
    let mut event_loop = EventLoop::default();
    let handle = event_loop.handle();
    let ctrl_c = Cell::new(false);

    // Exit the program on double CTRL+C.
    let cb = move |_: SignalHandle, _: i32| {
        match ctrl_c.get() {
            true => std::process::exit(0),
            false => ctrl_c.set(true),
        };
    };

    handle.signal(Kind::SIGINT, Policy::Oneshot, cb).unwrap();

    loop {
        // We need somehow to keep the program running because signal
        // listeners wont keep the event-loop alive.
        event_loop.run(RunMode::Once);
    }
}
```

**TTY handles** provide access to the terminal (stdin/stdout).

```rust
fn main() {
    let mut event_loop = EventLoop::default();
    let handle = event_loop.handle();
    let tty = handle.tty(std::io::stdin().as_raw_fd());

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
```

> You can run all the above examples located in `/examples` folders using cargo: `cargo run --example [name]`
