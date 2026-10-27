#[cfg(all(feature = "io_uring", not(target_os = "linux")))]
compile_error!("pyaio `io_uring` feature wakes via eventfd: Linux only");

use ::tokio::runtime::Handle;
use ::tokio::sync::oneshot;
use ::tokio::task::AbortHandle;
use pyo3::exceptions::{PyRuntimeError, PyStopIteration};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::BoundObject;
use std::cell::{RefCell, UnsafeCell};
use std::future::Future;
use std::mem::MaybeUninit;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
#[cfg(not(feature = "io_uring"))]
use std::thread::Thread;


mod compat;
pub use compat::{err, get_running_loop, into_future_with_locals, tokio, TaskLocals};
