use crate::check::Check;
use crate::check::CheckHandle;
use crate::fs::FsEvent;
use crate::fs::FsWatcher;
use crate::fs::FsWatcherHandle;
use crate::fs::WatchMode;
use crate::resource::ResourceId;
use crate::resource::ResourceMap;
use crate::resource::Shared;
use crate::signals::OsSignals;
use crate::signals::Policy;
use crate::signals::SigNum;
use crate::signals::Signal;
use crate::signals::SignalHandle;
use crate::task::OnCompleteCallback;
use crate::task::Output as TaskOutput;
use crate::task::Task;
use crate::task::TaskHandle;
use crate::task::WorkFn;
use crate::tcp_listener::TcpListener;
use crate::tcp_listener::TcpListenerHandle;
use crate::tcp_stream::OnCloseCallback;
use crate::tcp_stream::OnReadCallback;
use crate::tcp_stream::OnWriteCallback;
use crate::tcp_stream::TcpEventKind;
use crate::tcp_stream::TcpStream;
use crate::tcp_stream::TcpStreamHandle;
use crate::tcp_stream::READ_BUFFER_SIZE;
use crate::thread_pool::ThreadPool;
use crate::timers::Timer;
use crate::timers::TimerHandle;
use crate::timers::TimerKind;
use crate::timers::TimersCollection;
use crate::tty::FileDescriptor;
use crate::tty::TtyHandle;
use crate::tty::TtyReader;
use anyhow::anyhow;
use anyhow::Result;
use mio::net::TcpListener as MioListener;
use mio::net::TcpStream as MioSocket;
use mio::Events;
use mio::Interest;
use mio::Poll;
use mio::Registry;
use mio::Token;
use mio::Waker;
use rand::prelude::*;
use slotmap::DefaultKey;
use slotmap::Key;
use slotmap::KeyData;
use std::any::Any;
use std::cell::Cell;
use std::collections::VecDeque;
use std::io;
use std::io::Read;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::Path;
use std::rc::Rc;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

pub(crate) type BasicQueue = Vec<ResourceId>;

enum Request {
    TimerStart(Timer),
    TimerCancel(Shared<ResourceId>),
    TcpInit(Box<TcpStream>),
    TcpWrite(Shared<ResourceId>, Vec<u8>, OnWriteCallback),
    TcpRead(Shared<ResourceId>, OnReadCallback),
    TcpShutdownWrite(Shared<ResourceId>, OnCloseCallback),
    TcpClose(Shared<ResourceId>, OnCloseCallback),
    TcpListen(Box<TcpListener>),
    TcpListenerClose(Shared<ResourceId>, OnCloseCallback),
    TaskSpawn(Task, WorkFn, mpsc::Receiver<()>),
    TaskCancel(Shared<ResourceId>),
    CheckInit(Check),
    CheckRemove(Shared<ResourceId>),
    FsWatcherStart(FsWatcher),
    FsWatcherStop(Shared<ResourceId>),
    SignalStart(SigNum, Signal),
    SignalStop(u64),
    TtyRead(Box<TtyReader>, mpsc::Receiver<()>),
    TtyClose(Shared<ResourceId>),
}

#[allow(dead_code)]
pub(crate) enum Event {
    /// A network operation is available.
    Network(TcpEventKind),
    /// A thread-pool task has been completed.
    ThreadPool(ResourceId, TaskOutput),
    /// A file-system change has been detected.
    FsWatch(ResourceId, Result<FsEvent>),
    /// An interrupt signal detected (Windows platform).
    WinSigInt,
    /// Terminal input has been received.
    TtyInput(ResourceId, Result<Vec<u8>>),
}

#[derive(Debug)]
pub enum RunMode {
    /// Runs the event loop until there are no resources.
    Default,
    /// Polls for I/O events once.
    Once,
    /// Does not block if there are no pending events.
    NoWait,
}

pub struct EventLoop {
    current_time: Instant,
    resources: ResourceMap,
    timers: TimersCollection,
    check_queue: BasicQueue,
    close_queue: BasicQueue,
    signals: OsSignals,
    request_queue: mpsc::Receiver<Request>,
    request_queue_empty: Rc<Cell<bool>>,
    request_sender: Rc<mpsc::Sender<Request>>,
    thread_pool: ThreadPool,
    event_queue: mpsc::Receiver<Event>,
    event_sender: mpsc::Sender<Event>,
    registry: Registry,
    poll: Poll,
    waker: Arc<Waker>,
}

impl EventLoop {
    /// Creates a new event-loop instance.
    pub fn new(num_threads: usize) -> EventLoop {
        // Number of threads should always be a positive non-zero number.
        assert!(num_threads > 0);
        let thread_pool = ThreadPool::new(num_threads);

        let (request_sender, request_queue) = mpsc::channel();
        let (event_sender, event_queue) = mpsc::channel();

        // Initialize the kernel notification multiplexer.
        let poll = Poll::new().unwrap();
        let registry = poll.registry().try_clone().unwrap();

        let waker = Waker::new(poll.registry(), Token(0)).unwrap();
        let waker = Arc::new(waker);

        // Monitor system signals through MIO.
        #[cfg(target_family = "unix")]
        let signals = OsSignals::new(&registry);

        // Monitor system signals through dedicated thread.
        #[cfg(target_family = "windows")]
        let signals = OsSignals::new(event_sender.clone(), Arc::clone(&waker));

        EventLoop {
            current_time: Instant::now(),
            resources: ResourceMap::default(),
            timers: TimersCollection::new(),
            check_queue: Vec::new(),
            close_queue: Vec::new(),
            signals,
            request_queue,
            request_queue_empty: Rc::new(Cell::new(true)),
            request_sender: Rc::new(request_sender),
            thread_pool,
            event_queue,
            event_sender,
            registry,
            poll,
            waker,
        }
    }

    /// Runs the even-loop by providing a run mode.
    pub fn run(&mut self, mode: RunMode) {
        loop {
            self.current_time = Instant::now();
            self.process_requests();
            self.run_timers();
            // Based on the run_mode and what resources the event-loop is currently
            // running, we will calculate the timout of the poll phase.
            let timeout = self.poll_timeout(&mode);

            self.run_poll(timeout);
            self.run_check_callbacks();
            self.run_close();

            // Check if we need to exit, or continue the loop.
            match mode {
                RunMode::Once | RunMode::NoWait => break,
                RunMode::Default if !self.has_pending_events() => break,
                RunMode::Default => {}
            };
        }
    }

    /// Calculates the waiting time the poll phase should block for events.
    fn poll_timeout(&self, mode: &RunMode) -> Option<Duration> {
        // TODO: Describe how the calculation works..
        match mode {
            RunMode::NoWait => Some(Duration::ZERO),
            _ if !self.has_pending_events() => Some(Duration::ZERO),
            _ => {
                let refs = self.check_queue.len() + self.close_queue.len();
                match self.timers.next() {
                    _ if refs > 0 => Some(Duration::ZERO),
                    Some((t, _)) => Some(*t - self.current_time),
                    None => None,
                }
            }
        }
    }

    /// Runs all expired timers.
    fn run_timers(&mut self) {
        // Iterate through all the expired timers.
        for id in self.timers.split_expired_timers(self.current_time) {
            // In case we have a timer in the list but we don't have it as
            // a resource that means the timer was canceled.
            let handle = self.handle();
            let timer = match self.resources.get_mut_as::<Timer>(id) {
                Some(resource) => resource,
                None => continue,
            };

            timer.run_callback(handle);

            // If the timer is repeatable reschedule it, otherwise drop it.
            if let TimerKind::Interval = timer.kind {
                let expires_at = self.current_time + timer.delay;
                self.timers.insert(expires_at, id);
            } else {
                self.resources.remove(id);
            }
        }

        self.process_requests();
    }

    /// Polls for new I/O events (async-tasks, networking, etc).
    fn run_poll(&mut self, timeout: Option<Duration>) {
        // Buffer to hold ready events.
        let mut events = Events::with_capacity(1024);

        // Poll for new network events (this will block the thread).
        if let Err(e) = self.poll.poll(&mut events, timeout) {
            match e.kind() {
                io::ErrorKind::Interrupted => return,
                _ => panic!("{}", e),
            };
        }

        for event in &events {
            // Note: Token(0) is a special token signaling that someone woke us up.
            if event.token() == Token(0) {
                continue;
            }

            // Note: Token(1) is another special token signaling an OS
            // signal is available for processing.
            if event.token() == Token(1) {
                self.signals.run_pending(self.handle());
                continue;
            }

            let token = event.token();
            let readable = event.is_readable() || event.is_read_closed();
            let writable = event.is_writable();

            let event_type = match (readable, writable) {
                (true, _) => TcpEventKind::Read(token),
                (false, true) => TcpEventKind::Write(token),
                _ => continue,
            };

            self.event_sender.send(Event::Network(event_type)).unwrap();
        }

        while let Ok(event) = self.event_queue.try_recv() {
            match event {
                Event::Network(event) => self.process_network_event(event),
                Event::ThreadPool(id, output) => self.process_finished_task(id, output),
                Event::FsWatch(id, event) => self.process_fs_event(id, event),
                Event::WinSigInt => self.signals.run_pending(self.handle()),
                Event::TtyInput(id, data) => self.process_tty_input(id, data),
            }

            // Since each event might schedule additional I/O we need to process
            // the requests queue in every iteration.
            self.process_requests();
        }
    }

    /// Runs all check callbacks for this loop iteration.
    fn run_check_callbacks(&mut self) {
        // Due to Rust's ownership model we need to create a loop handle
        // here and re-use it when necessery.
        let handle = self.handle();

        for id in self.check_queue.iter() {
            // Get the resource from the collection.
            let check = match self.resources.get_mut_as::<Check>(*id) {
                Some(check) => check,
                None => continue,
            };

            check.run_callback(handle.clone());
        }

        // At this point we need to process the request queue in case
        // a check callback scheduled new resources.
        self.process_requests();
    }

    /// Runs any clean-up actions on "dying" resources.
    fn run_close(&mut self) {
        // Due to ownership constraints we need to get a handle
        // to the event-loop before the for-loop.
        let handle = self.handle();

        for id in self.close_queue.drain(..) {
            if let Some(mut resource) = self.resources.remove(id) {
                resource.destroy(handle.clone());
            }
        }

        self.process_requests();
    }

    /// Drains the request_queue to schedule new workload.
    fn process_requests(&mut self) {
        while let Ok(request) = self.request_queue.try_recv() {
            match request {
                Request::TimerStart(timer) => self.timer_start(timer),
                Request::TimerCancel(id) => self.timer_cancel(id),
                Request::TcpInit(stream) => self.tcp_stream_init(stream),
                Request::TcpRead(id, callback) => self.tcp_stream_read_start(id, callback),
                Request::TcpWrite(id, data, cb) => self.tcp_stream_write(id, data, cb),
                Request::TcpShutdownWrite(id, cb) => self.tcp_stream_shutdown_write(id, cb),
                Request::TcpClose(id, callback) => self.tcp_stream_close(id, callback),
                Request::TcpListen(listener) => self.tcp_listener_init(listener),
                Request::TcpListenerClose(id, callback) => self.tcp_listener_close(id, callback),
                Request::TaskSpawn(task, work, cancel_rx) => self.task_spawn(task, work, cancel_rx),
                Request::TaskCancel(id) => self.task_cancel(id),
                Request::CheckInit(check) => self.check_init(check),
                Request::CheckRemove(id) => self.check_remove(id),
                Request::FsWatcherStart(fs_event) => self.fs_watcher_start(fs_event),
                Request::FsWatcherStop(id) => self.fs_watcher_stop(id),
                Request::SignalStart(signum, signal) => self.signal_start(signum, signal),
                Request::SignalStop(id) => self.signal_stop(id),
                Request::TtyRead(stream, stop_rx) => self.tty_read_start(stream, stop_rx),
                Request::TtyClose(id) => self.tty_close(id),
            }
        }
        self.request_queue_empty.set(true);
    }

    /// Processes a finished task from the thread-pool.
    fn process_finished_task(&mut self, id: ResourceId, output: TaskOutput) {
        // If we receive a task ID that doesn't exist as a resource,
        // it means the task has already been canceled.
        let handle = self.handle();

        if let Some(mut resource) = self.resources.remove(id) {
            let task = resource.downcast_mut::<Task>().unwrap();
            task.run_callback(output, handle);
        }
    }

    /// Processes any file-system event that occurred.
    fn process_fs_event(&mut self, id: ResourceId, event: Result<FsEvent>) {
        // Get a reference to the resource and run the callback.
        let handle = self.handle();

        if let Some(watcher) = self.resources.get_mut_as::<FsWatcher>(id) {
            watcher.run_callback(handle, event);
        }
    }

    /// Processes any TTY received data.
    fn process_tty_input(&mut self, id: ResourceId, data: Result<Vec<u8>>) {
        // Get a reference to the resource and run the callback.
        let handle = self.handle();

        if let Some(tty) = self.resources.get_mut_as::<TtyReader>(id) {
            let tty_handle = tty.handle(handle);
            (tty.on_read)(tty_handle, data);
        }
    }

    /// Processes any ready network events from MIO.
    fn process_network_event(&mut self, event: TcpEventKind) {
        // Due to borrowing constraints down the line, we need to acquire
        // a loop handle at this point.
        let handle = self.handle();

        match event {
            TcpEventKind::Read(token) => {
                // MIO doesn't have a way to tell us if the event is for a socket or a listener
                // so we'll try to cast the resource into a tcp stream first. If that fails
                // will try to cast the resource into a tcp listener.
                let id = DefaultKey::from(KeyData::from_ffi(token.0 as u64));

                if let Some(stream) = self.resources.get_mut_as::<TcpStream>(id) {
                    stream.read_from_socket(handle, &mut self.registry, &mut self.close_queue);
                    return;
                }

                let listener = self.resources.get_mut_as::<TcpListener>(id).unwrap();
                let clients = listener.accept(handle);

                for stream in clients {
                    // For every new connection, add the stream to the resources so we can
                    // attach an actual ID and declare interest in the registry.
                    let id_slot = Rc::clone(&stream.id);
                    let id = self.resources.insert(Box::new(stream));

                    id_slot.set(id);

                    // We need to get the stream from the resources to satisfy
                    // rust's ownership and borrowing rules.
                    let stream = self.resources.get_mut_as::<TcpStream>(id).unwrap();
                    let token = stream.token();
                    let socket = &mut stream.socket;

                    self.registry
                        .register(socket, token, Interest::READABLE)
                        .unwrap();
                }
            }
            TcpEventKind::Write(token) => {
                // We need to get the resource ID from the MIO token.
                let id = DefaultKey::from(KeyData::from_ffi(token.0 as u64));
                let stream = self.resources.get_mut_as::<TcpStream>(id).unwrap();
                let registry = &mut self.registry;
                let close_queue = &mut self.close_queue;

                stream.write_to_socket(handle, registry, close_queue);
            }
        }
    }

    /// Schedules a new timer in the event-loop.
    fn timer_start(&mut self, timer: Timer) {
        // First insert the new timer into the resources map, then set its
        // resource ID to the value returned by the insertion operation.
        let expires_at = self.current_time + timer.delay;

        let id_slot = Rc::clone(&timer.id);
        let id = self.resources.insert(Box::new(timer));

        id_slot.set(id);

        self.timers.insert(expires_at, id);
    }

    /// Removes a previously scheduled timer.
    fn timer_cancel(&mut self, id: Shared<ResourceId>) {
        // To achieve O(1) cancellation, we remove the resource but keep the entry
        // in the timer collection. When processing expired timers, canceled
        // ones are simply ignored.
        self.resources.remove(id.get());
    }

    /// Schedules a new task for execution to the event-loop.
    fn task_spawn(&mut self, task: Task, work: WorkFn, cancel_rx: mpsc::Receiver<()>) {
        // The reason we insert the stream to the map and then we get a reference
        // is so we can create a token with the correct resource ID.
        let id_slot = Rc::clone(&task.id);
        let id = self.resources.insert(Box::new(task));

        id_slot.set(id);

        let event_sender = self.event_sender.clone();
        let waker = Arc::clone(&self.waker);

        self.thread_pool.spawn(
            move || {
                let output = work();
                let event = Event::ThreadPool(id, output);
                // Ignore the result to avoid panicking if the receiving end of
                // the channel has already been closed or dropped.
                let _ = event_sender.send(event);
                let _ = waker.wake();
            },
            cancel_rx,
        );
    }

    /// Removes a previously queued task.
    fn task_cancel(&mut self, id: Shared<ResourceId>) {
        // We only need to remove the resource from the map. If a task is already
        // running in the thread pool, its result will be discarded when
        // finished tasks are processed.
        self.resources.remove(id.get());
    }

    /// Initializes a new tcp connection.
    fn tcp_stream_init(&mut self, stream: Box<TcpStream>) {
        // The reason we insert the stream to the map and then we get a reference
        // is so we can create a token with the correct resource ID.
        let id_slot = Rc::clone(&stream.id);
        let id = self.resources.insert(stream);

        id_slot.set(id);

        let stream = self.resources.get_mut_as::<TcpStream>(id).unwrap();

        // When we create a new tcp socket connection we have to make sure
        // it's well connected with the remote host.
        //
        // See https://docs.rs/mio/0.8.4/mio/net/struct.TcpStream.html#notes
        let token = stream.token();
        let socket = &mut stream.socket;

        self.registry
            .register(socket, token, Interest::WRITABLE)
            .unwrap();
    }

    /// Initializes a new tcp listener.
    fn tcp_listener_init(&mut self, listener: Box<TcpListener>) {
        // The reason we insert the stream to the map and then we get a reference
        // is so we can create a token with the correct resource ID.
        let id_slot = Rc::clone(&listener.id);
        let id = self.resources.insert(listener);

        id_slot.set(id);

        let listener = self.resources.get_mut_as::<TcpListener>(id).unwrap();
        let token = listener.token();
        let socket = &mut listener.socket;

        self.registry
            .register(socket, token, Interest::READABLE)
            .unwrap();
    }

    /// Registers interest for writing to a tcp socket.
    fn tcp_stream_write(
        &mut self,
        id: Shared<ResourceId>,
        data: Vec<u8>,
        callback: OnWriteCallback,
    ) {
        // Get a mut reference to a tcp stream resource.
        let stream = self.resources.get_mut_as::<TcpStream>(id.get()).unwrap();
        stream.enqueue(data, callback);

        let token = stream.token();
        let interest = Interest::READABLE.add(Interest::WRITABLE);

        self.registry
            .reregister(&mut stream.socket, token, interest)
            .unwrap();
    }

    /// Registers interest for reading from a tcp socket.
    fn tcp_stream_read_start(&mut self, id: Shared<ResourceId>, callback: OnReadCallback) {
        // Get a mut reference to a tcp stream resource.
        let stream = self.resources.get_mut_as::<TcpStream>(id.get()).unwrap();
        let token = stream.token();

        stream.on_read = Some(callback);

        let interest = match stream.write_queue.len() {
            0 => Interest::READABLE,
            _ => Interest::READABLE.add(Interest::WRITABLE),
        };

        self.registry
            .reregister(&mut stream.socket, token, interest)
            .unwrap();
    }

    /// Schedules a full tcp stream close.
    fn tcp_stream_close(&mut self, id: Shared<ResourceId>, callback: OnCloseCallback) {
        // Get a mut reference to a tcp stream resource.
        let stream = self.resources.get_mut_as::<TcpStream>(id.get()).unwrap();
        stream.on_close = Some(callback);

        self.close_queue.push(id.get());
    }

    /// Stops the listener from accepting new connections.
    fn tcp_listener_close(&mut self, id: Shared<ResourceId>, callback: OnCloseCallback) {
        // Get a mut reference to a tcp stream resource.
        let stream = self.resources.get_mut_as::<TcpListener>(id.get()).unwrap();
        stream.on_close = Some(callback);

        self.close_queue.push(id.get());
    }

    /// Closes the write side of the tcp stream.
    fn tcp_stream_shutdown_write(&mut self, id: Shared<ResourceId>, mut callback: OnCloseCallback) {
        // We need to take the handle here due to borrowing constraints.
        let handle = self.handle();

        if let Some(resource) = self.resources.get_mut(id.get()) {
            resource.destroy(handle.clone());
            callback(handle);
        }
    }

    /// Initializes a check resources to the event-loop.
    fn check_init(&mut self, check: Check) {
        // The reason we insert the stream to the map and then we get a reference
        // is so we can create a token with the correct resource ID.
        let id_slot = Rc::clone(&check.id);
        let id = self.resources.insert(Box::new(check));

        id_slot.set(id);

        self.check_queue.push(id);
    }

    /// Removes a check resource from the event-loop.
    fn check_remove(&mut self, id: Shared<ResourceId>) {
        // TODO: Using retain is not very performant since we're checking every
        // element in the vector. In the future it's best to come up with
        // a better solution.
        let id = id.get();

        self.resources.remove(id);
        self.check_queue.retain(|i| *i != id);
    }

    /// Initializes and starts a new file-system watcher.
    fn fs_watcher_start(&mut self, fs_watcher: FsWatcher) {
        // The reason we insert the stream to the map and then we get a reference
        // is so we can create a token with the correct resource ID.
        let id_slot = Rc::clone(&fs_watcher.id);
        let id = self.resources.insert(Box::new(fs_watcher));

        id_slot.set(id);

        // Note: We obtain a reference to the newly inserted fs_watcher before starting
        // the watcher because the watcher requires a valid resource ID, which is
        // only assigned once the fs_event has been inserted.
        let waker = Arc::clone(&self.waker);
        let event_sender = self.event_sender.clone();

        self.resources
            .get_mut_as::<FsWatcher>(id)
            .unwrap()
            .watch(waker, event_sender);
    }

    /// Stops and removes a file-system watcher from the event-loop.
    fn fs_watcher_stop(&mut self, id: Shared<ResourceId>) {
        self.resources.remove(id.get());
    }

    /// Subscribes a new signal listener to the event-loop.
    fn signal_start(&mut self, signum: SigNum, signal: Signal) {
        // Check if the specific signal is already being tracked.
        let signum: i32 = signum.into();

        if let Some(handlers) = self.signals.handlers.get_mut(&signum) {
            handlers.push(signal);
            return;
        }

        #[cfg(target_family = "unix")]
        {
            self.signals.sources.add_signal(signum).unwrap();
        }

        self.signals.handlers.insert(signum, vec![signal]);
    }

    /// Removes the signal listener from the event-loop.
    fn signal_stop(&mut self, id: u64) {
        self.signals.remove_handler(id)
    }

    /// Registers a resources to read from the TTY stream.
    fn tty_read_start(&mut self, reader: Box<TtyReader>, stop_rx: mpsc::Receiver<()>) {
        // The reason we insert the stream to the map and then we get a reference
        // is so we can create a token with the correct resource ID.
        let mut stream = reader.get_stream();
        let (_, noop_cancellation) = mpsc::channel();

        let id_slot = Rc::clone(&reader.id);
        let id = self.resources.insert(reader);

        id_slot.set(id);

        let read_tty_input = {
            let id = id_slot.get();
            let event_sender = self.event_sender.clone();
            let waker = Arc::clone(&self.waker);
            move || {
                // Buffer to store read bytes.
                let mut buffer = [0u8; 1024];

                // Keep reading from stdin until the TTY handle signals that
                // reading should stop.
                while stop_rx.try_recv().is_err() {
                    let result = stream
                        .read(&mut buffer)
                        .map(|n| buffer[..n].to_vec())
                        .map_err(Into::into);

                    // Notify the event-loop about the stdin input.
                    event_sender.send(Event::TtyInput(id, result)).unwrap();
                    waker.wake().unwrap();
                }
            }
        };

        // Use the thread-pool to schedule the stdin task.
        self.thread_pool.spawn(read_tty_input, noop_cancellation);
    }

    /// Stops reading from a TTY stream.
    fn tty_close(&mut self, id: Shared<ResourceId>) {
        // Send a stop signal to the thread that reads from stdin.
        if let Some(mut resource) = self.resources.remove(id.get()) {
            let tty = resource.downcast_mut::<TtyReader>().unwrap();
            let _ = tty.stop_tx.send(());
        }
    }

    /// Returns true if there is pending work still ongoing.
    pub fn has_pending_events(&self) -> bool {
        !self.resources.is_empty() || !self.request_queue_empty.get()
    }

    /// Returns a new handle to the event-loop.
    pub fn handle(&self) -> LoopHandle {
        LoopHandle {
            request_sender: Rc::clone(&self.request_sender),
            request_queue_empty: Rc::clone(&self.request_queue_empty),
        }
    }

    /// Returns a new interrupt handle to the event-loop (sharable across threads).
    pub fn interrupt_handle(&self) -> LoopInterruptHandle {
        LoopInterruptHandle {
            waker: Arc::clone(&self.waker),
        }
    }
}

impl Default for EventLoop {
    fn default() -> Self {
        let default_pool_size = NonZeroUsize::new(4).unwrap();
        let num_cores = thread::available_parallelism().unwrap_or(default_pool_size);

        Self::new(num_cores.into())
    }
}

#[derive(Debug, Clone)]
pub struct LoopHandle {
    request_sender: Rc<mpsc::Sender<Request>>,
    request_queue_empty: Rc<Cell<bool>>,
}

impl LoopHandle {
    /// Schedules a new timer to the event-loop.
    pub fn timer<F>(&self, delay: Duration, kind: TimerKind, callback: F) -> TimerHandle
    where
        F: FnMut(LoopHandle) + 'static,
    {
        // Since the resource is not yet scheduled in the event-loop, we create a
        // null ID. The event-loop will update this value with a real ID later.
        let id = Rc::new(Cell::new(DefaultKey::null()));
        let callback = Box::new(callback);

        let timer = Timer {
            id,
            delay,
            kind,
            callback,
        };

        // Create a timer handle that we will return to the caller.
        let handle = timer.handle(self.clone());
        let request = Request::TimerStart(timer);

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);

        handle
    }

    /// Removes a timer from the event-loop.
    pub(crate) fn cancel_timer(&self, id: Shared<ResourceId>) {
        // Send a cancel request.
        let request = Request::TimerCancel(id);

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);
    }

    /// Schedules a new task to the event-loop.
    pub fn spawn<T, F, U>(&self, work: F, callback: Option<U>) -> TaskHandle
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
        U: FnOnce(LoopHandle, T) + 'static,
    {
        // Since the resource is not yet scheduled in the event-loop, we create a
        // null ID. The event-loop will update this value with a real ID later.
        let id = Rc::new(Cell::new(DefaultKey::null()));

        // Box the return value for cross-thread transport.
        let work: WorkFn = Box::new(move || {
            let val = work();
            Box::new(val) as Box<dyn Any + Send>
        });

        // Downcast the boxed value back to `T` before calling the user's callback.
        let on_complete: Option<OnCompleteCallback> = callback.map(|cb| {
            Box::new(move |handle: LoopHandle, output: TaskOutput| {
                let val: T = *output.downcast().unwrap();
                cb(handle, val);
            }) as OnCompleteCallback
        });

        let (cancel_tx, cancel_rx) = mpsc::channel();

        let mut task = Task {
            id,
            on_complete,
            cancel_tx,
        };

        let handle = task.handle(self.clone());
        let request = Request::TaskSpawn(task, work, cancel_rx);

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);

        handle
    }

    /// Removes a task from the event-loop.
    pub(crate) fn cancel_task(&self, id: Shared<ResourceId>) {
        // Send a cancel request.
        let request = Request::TaskCancel(id);

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);
    }

    /// Creates a new tcp stream and connects to the specified address.
    pub fn tcp_connect<F>(&self, address: SocketAddr, callback: F) -> Result<()>
    where
        F: FnMut(Result<TcpStreamHandle>) + 'static,
    {
        // Since the resource is not yet scheduled in the event-loop, we create a
        // null ID. The event-loop will update this value with a real ID later.
        let id = Rc::new(Cell::new(DefaultKey::null()));
        let callback = Box::new(callback);

        // Connect to the remote host.
        let stream = Box::new(TcpStream {
            id,
            socket: MioSocket::connect(address)?,
            read_buffer: [0; READ_BUFFER_SIZE],
            on_connection: Some(callback),
            on_read: None,
            on_close: None,
            write_queue: VecDeque::new(),
        });

        self.request_sender.send(Request::TcpInit(stream)).unwrap();
        self.request_queue_empty.set(false);

        Ok(())
    }

    /// Starts listening for incoming connections.
    pub fn tcp_listen<F>(&self, address: SocketAddr, callback: F) -> Result<()>
    where
        F: FnMut(TcpListenerHandle, Result<TcpStreamHandle>) + 'static,
    {
        // Since the resource is not yet scheduled in the event-loop, we create a
        // null ID. The event-loop will update this value with a real ID later.
        let id = Rc::new(Cell::new(DefaultKey::null()));
        let on_connection = Box::new(callback);

        // Bind address to the socket.
        let socket = MioListener::bind(address)?;
        let listener = Box::new(TcpListener {
            id,
            socket,
            on_connection,
            on_close: None,
        });

        self.request_sender
            .send(Request::TcpListen(listener))
            .unwrap();
        self.request_queue_empty.set(false);

        Ok(())
    }

    /// Writes bytes to an open tcp stream.
    pub(crate) fn tcp_write<F, D>(&self, id: Shared<ResourceId>, data: D, callback: F)
    where
        D: AsRef<[u8]>,
        F: FnMut(TcpStreamHandle, Result<usize>) + 'static,
    {
        let request = Request::TcpWrite(id, data.as_ref().to_vec(), Box::new(callback));

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);
    }

    /// Starts reading from an open tcp stream.
    pub(crate) fn tcp_read_start<F>(&self, id: Shared<ResourceId>, callback: F)
    where
        F: Fn(TcpStreamHandle, Result<Vec<u8>>) + 'static,
    {
        let request = Request::TcpRead(id, Box::new(callback));

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);
    }

    /// Closes the write side of the tcp stream.
    pub(crate) fn tcp_shutdown_write<F>(&self, id: Shared<ResourceId>, callback: F)
    where
        F: FnMut(LoopHandle) + 'static,
    {
        let request = Request::TcpShutdownWrite(id, Box::new(callback));

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);
    }

    /// Completely closes the tcp stream.
    pub(crate) fn tcp_close<F>(&self, id: Shared<ResourceId>, callback: F)
    where
        F: FnMut(LoopHandle) + 'static,
    {
        let request = Request::TcpClose(id, Box::new(callback));

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);
    }

    /// Stops a listener from accepting new connections.
    pub(crate) fn tcp_listener_close<F>(&self, id: Shared<ResourceId>, callback: F)
    where
        F: FnMut(LoopHandle) + 'static,
    {
        let request = Request::TcpListenerClose(id, Box::new(callback));

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);
    }

    /// Initializes a new check callback to the event-loop. Check resources will run the
    /// given callback once per loop iteration, right after polling for I/O.
    pub fn check<F>(&self, callback: F) -> CheckHandle
    where
        F: FnMut(CheckHandle) + 'static,
    {
        // Since the resource is not yet scheduled in the event-loop, we create a
        // null ID. The event-loop will update this value with a real ID later.
        let id = Rc::new(Cell::new(DefaultKey::null()));
        let callback = Box::new(callback);

        let check = Check { id, callback };
        let handle = check.handle(self.clone());

        let request = Request::CheckInit(check);

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);

        handle
    }

    /// Removes a check resources from the event-loop.
    pub(crate) fn check_remove(&self, id: Shared<ResourceId>) {
        // Send a remove request.
        let request = Request::CheckRemove(id);

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);
    }

    /// Creates a watcher that monitors the specified path for changes.
    pub fn fs_watcher<P, F>(&self, path: P, mode: WatchMode, callback: F) -> Result<FsWatcherHandle>
    where
        P: AsRef<Path>,
        F: FnMut(FsWatcherHandle, Result<FsEvent>) + 'static,
    {
        // Since the resource is not yet scheduled in the event-loop, we create a
        // null ID. The event-loop will update this value with a real ID later.
        let id = Rc::new(Cell::new(DefaultKey::null()));
        let callback = Box::new(callback);

        // Check if path exists.
        std::fs::metadata(path.as_ref())?;

        let fs_event = FsWatcher {
            id,
            path: path.as_ref().to_path_buf(),
            callback,
            mode,
            watcher: None,
        };

        let handle = fs_event.handle(self.clone());
        let request = Request::FsWatcherStart(fs_event);

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);

        Ok(handle)
    }

    /// Stops the watcher, the callback will no longer be called.
    pub(crate) fn fs_watcher_stop(&self, id: Shared<ResourceId>) {
        // Send a remove request.
        let request = Request::FsWatcherStop(id);

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);
    }

    /// Start the handle with the given callback, watching for the given signal.
    pub fn signal<F>(&self, signum: i32, policy: Policy, callback: F) -> Result<SignalHandle>
    where
        F: FnMut(SignalHandle, i32) + 'static,
    {
        // Parse signal number provided.
        let signum = SigNum::try_from(signum).map_err(|e| anyhow!(e))?;

        // Note: Signals are not stored in the resources map since they don't
        // keep the event-loop alive so, their IDs is a random u64.
        let mut rng = rand::rng();
        let id = rng.random::<u64>();
        let callback = Box::new(callback);

        let signal = Signal {
            id,
            callback,
            policy,
        };

        let handle = signal.handle(self.clone());
        let request = Request::SignalStart(signum, signal);

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);

        Ok(handle)
    }

    /// The callback will no longer be called.
    pub(crate) fn signal_stop(&self, id: u64) {
        // Send a remove request.
        let request = Request::SignalStop(id);

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);
    }

    /// Creates a TTY handle wrapping the given file descriptor.
    pub fn tty(&self, raw_fd: FileDescriptor) -> TtyHandle {
        // TTY resources are added to the resource map only after they start
        // reading terminal input, so for now we'll assign them a null ID.
        let id = Rc::new(Cell::new(DefaultKey::null()));
        let handle = self.clone();

        TtyHandle { id, raw_fd, handle }
    }

    /// Starts reading from the TTY stream.
    pub(crate) fn tty_read_start(&self, reader: TtyReader, stop_rx: mpsc::Receiver<()>) {
        let boxed_reader = Box::new(reader);
        let request = Request::TtyRead(boxed_reader, stop_rx);

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);
    }

    /// Stops reading from a TTY stream.
    pub(crate) fn tty_close(&self, id: Shared<ResourceId>) {
        // Send a remove request.
        let request = Request::TtyClose(id);

        self.request_sender.send(request).unwrap();
        self.request_queue_empty.set(false);
    }
}

#[derive(Clone)]
pub struct LoopInterruptHandle {
    waker: Arc<Waker>,
}

impl LoopInterruptHandle {
    /// Interrupts the poll phase of the event-loop.
    pub fn interrupt(&self) {
        self.waker.wake().unwrap();
    }
}
