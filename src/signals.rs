use crate::event_loop::LoopHandle;
use signal_hook::low_level::emulate_default_handler;
use std::collections::HashMap;

#[cfg(unix)]
mod unix_imports {
    pub(crate) use mio::{Interest, Registry, Token};
    pub(crate) use signal_hook_mio::v1_0::Signals;
}

#[cfg(windows)]
mod win_imports {
    pub(crate) use crate::event_loop::Event;
    pub(crate) use crossterm::event::Event as ConsoleEvent;
    pub(crate) use mio::Waker;
    pub(crate) use std::{sync::mpsc, sync::Arc};
}

#[cfg(unix)]
use unix_imports::*;

#[cfg(windows)]
use win_imports::*;

#[allow(non_snake_case)]
pub mod Kind {
    /// All signals from signal_hook.
    pub use signal_hook::consts::signal::*;

    /// Custom SIGWINCH support on Windows, where signal_hook doesn't
    /// provide it. Windows never raises this signal itself.
    #[cfg(windows)]
    pub const SIGWINCH: i32 = 28;
}

#[derive(Debug, Clone, Copy)]
#[allow(clippy::upper_case_acronyms)]
#[cfg(windows)]
pub(crate) enum WinEvent {
    Sigint,
    Sigwinch,
}

#[cfg(windows)]
impl From<WinEvent> for i32 {
    fn from(value: WinEvent) -> Self {
        match value {
            WinEvent::Sigint => Kind::SIGINT,
            WinEvent::Sigwinch => Kind::SIGWINCH,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Policy {
    /// Invoked at most once.
    Oneshot,
    /// Stays active until explicitly removed.
    Persistent,
}

// Based on linux systems the max allowed signal number is 31.
// https://www-uxsup.csx.cam.ac.uk/courses/moved.Building/signals.pdf
const MAX_SIGNAL_VALUE: i32 = 31;

#[derive(Debug)]
pub(crate) struct SigNum(i32);

impl TryFrom<i32> for SigNum {
    type Error = String;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        // Check if signal is within UNIX range.
        if value > MAX_SIGNAL_VALUE {
            return Err("Signal number out of range".to_string());
        }

        // Forbidden signals are not allowed to be registered.
        if signal_hook::consts::FORBIDDEN.contains(&value) {
            return Err("Forbidden signal provided".to_string());
        }

        Ok(SigNum(value))
    }
}

impl From<SigNum> for i32 {
    fn from(value: SigNum) -> Self {
        value.0
    }
}

type SignalCallback = Box<dyn FnMut(SignalHandle, i32) + 'static>;

pub(crate) struct Signal {
    /// A unique referene.
    pub id: u64,
    /// Callback invoked when the signal is triggered.
    pub callback: SignalCallback,
    /// Whether the callback fires once or persists.
    pub policy: Policy,
}

impl Signal {
    /// Returns a handle to the signal resource.
    pub fn handle(&self, handle: LoopHandle) -> SignalHandle {
        SignalHandle {
            id: self.id,
            handle,
        }
    }
}

/// A reference like struct to an active signal listener.
#[derive(Debug, Clone)]
pub struct SignalHandle {
    /// A unique referene.
    pub(crate) id: u64,
    /// A handle to the event-loop.
    handle: LoopHandle,
}

impl SignalHandle {
    /// The signal callback will no longer be called.
    pub fn stop(&self) {
        self.handle.signal_stop(self.id);
    }

    /// Returns a handle to the event-loop.
    pub fn loop_handle(&self) -> LoopHandle {
        self.handle.clone()
    }
}

/// Abstracts signal handling across platforms.
pub(crate) struct OsSignals {
    #[cfg(target_family = "unix")]
    pub sources: Signals,
    pub handlers: HashMap<i32, Vec<Signal>>,
}

impl OsSignals {
    #[cfg(unix)]
    pub fn new(registry: &Registry) -> Self {
        let mut sources = Signals::new::<[_; 0], i32>([]).unwrap();
        let _ = registry.register(&mut sources, Token(1), Interest::READABLE);

        OsSignals {
            sources,
            handlers: HashMap::new(),
        }
    }

    #[cfg(windows)]
    pub fn new(notifier: mpsc::Sender<Event>, waker: Arc<Waker>) -> Self {
        // Handle Ctrl+C through the Windows console control handler.
        ctrlc::set_handler({
            let (notifier, waker) = (notifier.clone(), Arc::clone(&waker));
            move || Self::notify(&notifier, &waker, WinEvent::Sigint)
        })
        .unwrap();

        // Monitor the console input queue for terminal resize events.
        std::thread::spawn(move || loop {
            if let Ok(ConsoleEvent::Resize(..)) = crossterm::event::read() {
                Self::notify(&notifier, &waker, WinEvent::Sigwinch);
            }
        });

        OsSignals {
            handlers: HashMap::new(),
        }
    }

    #[cfg(windows)]
    fn notify(notifier: &mpsc::Sender<Event>, waker: &Waker, signal: WinEvent) {
        notifier.send(Event::Windows(signal)).unwrap();
        waker.wake().unwrap();
    }

    #[cfg(unix)]
    pub fn run_pending(&mut self, handle: LoopHandle) {
        // Going through the available signals.
        for signal in self.sources.pending() {
            // Get all handlers for the given signal.
            let handlers = match self.handlers.get_mut(&signal) {
                Some(handlers) => handlers,
                None => continue,
            };

            // No listeners for this signal, running default action.
            if handlers.is_empty() {
                emulate_default_handler(signal).unwrap();
                continue;
            }

            handlers.retain_mut(|handler| {
                // Run handler's callback.
                let handle = handler.handle(handle.clone());
                (handler.callback)(handle, signal);

                // Keep the listener if persistent.
                match handler.policy {
                    Policy::Oneshot => false,
                    Policy::Persistent => true,
                }
            });
        }
    }

    #[cfg(windows)]
    pub fn run_pending(&mut self, handle: LoopHandle, event: WinEvent) {
        // Note: on Windows, the event listeners (CTRL+C, console window resize, ...)
        // run on dedicated threads that are always active, so this function can be
        // invoked even if no signal handler was ever registered. In that case, or when
        // the handler list for this signal is empty, we emulate the default behavior
        // for the signal instead of silently swallowing the event.
        let handlers = match self.handlers.get_mut(&event.into()) {
            Some(handlers) if !handlers.is_empty() => handlers,
            _ => {
                emulate_default_handler(event.into()).unwrap();
                return;
            }
        };

        handlers.retain_mut(|handler| {
            // Run handler's callback.
            let handle = handler.handle(handle.clone());
            (handler.callback)(handle, Kind::SIGINT);

            // Keep the listener if persistent.
            match handler.policy {
                Policy::Oneshot => false,
                Policy::Persistent => true,
            }
        });
    }

    pub fn remove_handler(&mut self, id: u64) {
        // Note: Given the structure of this struct we're following the simplest
        // approach to remove the element with O(n^2) complexity. Further
        // improvements can be made in the future if necessary.
        if let Some((_, list)) = self
            .handlers
            .iter_mut()
            .find(|(_, list)| list.iter().any(|handler| handler.id == id))
        {
            list.retain(|handler| handler.id != id);
        }
    }
}
