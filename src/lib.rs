use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

const RING_SIZE: usize = 8192;

#[repr(align(64))]
struct Cell {
    seq: AtomicUsize,
    job: UnsafeCell<MaybeUninit<JobKind>>,
}

#[repr(align(64))]
struct Pad(AtomicUsize);

pub enum JobKind {
    Void,
}

pub struct Ring {
    cells: Box<[Cell]>,
    mask: usize,
    enq: Pad,
    deq: Pad,
}
unsafe impl Send for Ring {}
unsafe impl Sync for Ring {}

impl Ring {
    pub fn new() -> Self {
        let cells: Vec<Cell> = (0..RING_SIZE)
            .map(|i| Cell { seq: AtomicUsize::new(i), job: UnsafeCell::new(MaybeUninit::uninit()) })
            .collect();
        Self {
            cells: cells.into(),
            mask: RING_SIZE - 1,
            enq: Pad(AtomicUsize::new(0)),
            deq: Pad(AtomicUsize::new(0)),
        }
    }

    #[inline]
    pub fn push(&self, job: JobKind) -> Result<(), JobKind> {
        let pos = self.enq.0.fetch_add(1, Ordering::Relaxed);
        let cell = &self.cells[pos & self.mask];
        let seq = cell.seq.load(Ordering::Acquire);
        if seq.cast_signed().wrapping_sub(pos.cast_signed()) == 0 {
            unsafe { (*cell.job.get()).write(job); }
            cell.seq.store(pos + 1, Ordering::Release);
            Ok(())
        } else {
            unsafe { (*cell.job.get()).write(JobKind::Void); }
            cell.seq.store(pos + 1, Ordering::Release);
            Err(job)
        }
    }

    #[inline]
    pub fn pop(&self) -> Option<JobKind> {
        let mut pos = self.deq.0.load(Ordering::Relaxed);
        loop {
            let cell = &self.cells[pos & self.mask];
            let seq = cell.seq.load(Ordering::Acquire);
            let dif = seq.cast_signed().wrapping_sub((pos + 1).cast_signed());
            if dif == 0 {
                if self.deq.0.compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                    let job = unsafe { (*cell.job.get()).assume_init_read() };
                    cell.seq.store(pos.wrapping_add(self.mask + 1), Ordering::Release);
                    return Some(job);
                }
                pos = self.deq.0.load(Ordering::Relaxed);
            } else if dif < 0 {
                return None;
            } else {
                pos = self.deq.0.load(Ordering::Relaxed);
            }
        }
    }

    #[inline]
    pub fn in_flight(&self) -> bool {
        self.enq.0.load(Ordering::Acquire) != self.deq.0.load(Ordering::Acquire)
    }
}

use pyo3::prelude::*;
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;

static REGISTRY: Mutex<Vec<Arc<Bridge>>> = Mutex::new(Vec::new());

pub struct Bridge {
    pub ring: Ring,
    pub event_loop: Py<PyAny>,
    pub handle: Handle,
}

impl Bridge {
    pub fn for_loop(py: Python<'_>, lp: &Bound<'_, PyAny>) -> PyResult<Arc<Self>> {
        let mut reg = REGISTRY.lock().unwrap();
        if let Some(b) = reg.iter().find(|b| b.event_loop.as_ptr() == lp.as_ptr()) {
            return Ok(b.clone());
        }
        let bridge = Arc::new(Self {
            ring: Ring::new(),
            event_loop: lp.clone().unbind(),
            handle: Handle::current(),
        });
        reg.push(bridge.clone());
        Ok(bridge)
    }
}

use std::task::{RawWaker, RawWakerVTable, Waker};
use tokio::task::AbortHandle;

fn noop_raw_waker() -> RawWaker {
    fn noop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(|p| RawWaker::new(p, &VTABLE), noop, noop, noop);
    RawWaker::new(std::ptr::null(), &VTABLE)
}

pub fn noop_waker() -> Waker {
    unsafe { Waker::from_raw(noop_raw_waker()) }
}

#[pyclass]
pub struct AbortOnCancel(pub AbortHandle);

#[pymethods]
impl AbortOnCancel {
    fn __call__(&self, fut: Bound<'_, PyAny>) -> PyResult<()> {
        if fut.call_method0("cancelled")?.is_truthy()? {
            self.0.abort();
        }
        Ok(())
    }
}
