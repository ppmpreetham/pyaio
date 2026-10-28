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
mod demo;
pub use compat::{err, get_running_loop, into_future_with_locals, tokio, TaskLocals};

type Job = Box<dyn FnOnce(Python<'_>) + Send + 'static>;

const INLINE_CAP: usize = 48;

#[repr(C, align(8))]
struct InlineBuf([u8; INLINE_CAP]);

type ConvFn = fn(InlineBuf, Python<'_>) -> PyResult<Py<PyAny>>;

enum JobKind {
    FinishInline { target: Py<PyAny>, conv: ConvFn, buf: InlineBuf },
    FinishPy { target: Py<PyAny>, res: PyResult<Py<PyAny>> },
    Cancel { slot: Arc<Slot> },
    Boxed(Job),
    Void,
}

fn finish_job<T>(b: &Arc<Bridge>, target: Py<PyAny>, r: PyResult<T>) -> JobKind
where
    T: Send + 'static + for<'a> IntoPyObject<'a>,
{
    match r {
        Ok(v) if std::mem::size_of::<T>() <= INLINE_CAP && std::mem::align_of::<T>() <= 8 => {
            let conv: ConvFn = |buf, py| to_py(py, unsafe { buf.0.as_ptr().cast::<T>().read() });
            let mut buf = InlineBuf([0u8; INLINE_CAP]);
            unsafe { buf.0.as_mut_ptr().cast::<T>().write(v); }
            JobKind::FinishInline { target, conv, buf }
        }
        Ok(v) => {
            let b = b.clone();
            JobKind::Boxed(Box::new(move |py| finish_future(py, &b, &target, to_py(py, v))))
        }
        Err(e) => JobKind::FinishPy { target, res: Err(e) },
    }
}

const RING_SIZE: usize = 8192;

#[repr(align(64))]
struct Cell {
    seq: AtomicUsize,
    job: UnsafeCell<MaybeUninit<JobKind>>,
}
#[repr(align(64))]
struct Pad(AtomicUsize);
struct Ring {
    cells: Box<[Cell]>,
    mask: usize,
    enq: Pad,
    deq: Pad,
}
unsafe impl Send for Ring {}
unsafe impl Sync for Ring {}

#[allow(clippy::comparison_chain)]
impl Ring {
    fn new() -> Self {
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
    fn push(&self, job: JobKind) -> Result<(), JobKind> {
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
    fn pop(&self) -> Option<JobKind> {
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
    fn in_flight(&self) -> bool {
        self.enq.0.load(Ordering::Acquire) != self.deq.0.load(Ordering::Acquire)
    }
}

thread_local! { static SPARE: RefCell<Vec<JobKind>> = const { RefCell::new(Vec::new()) }; }
static REGISTRY: Mutex<Vec<Arc<Bridge>>> = Mutex::new(Vec::new());

pub fn init(py: Python<'_>, event_loop: &Bound<'_, PyAny>, handle: Handle) -> PyResult<Arc<Bridge>> {
    let ptr = event_loop.as_ptr();
    if let Some(b) = REGISTRY.lock().unwrap().iter().find(|b| b.event_loop.as_ptr() == ptr) {
        return Ok(b.clone());
    }
    let snapshot: Vec<Arc<Bridge>> = REGISTRY.lock().unwrap().clone();
    let closed: Vec<Arc<Bridge>> = snapshot
        .into_iter()
        .filter(|b| b.event_loop.bind(py).call_method0(intern!(py, "is_closed")).and_then(|c| c.is_truthy()).unwrap_or(false))
        .collect();
    let new = Bridge::new(py, event_loop, handle)?;
    {
        let mut reg = REGISTRY.lock().unwrap();
        reg.retain(|b| !closed.iter().any(|c| Arc::ptr_eq(b, c)));
        if let Some(b) = reg.iter().find(|b| b.event_loop.as_ptr() == ptr) {
            return Ok(b.clone());
        }
        reg.push(new.clone());
    }
    drop(closed);
    Ok(new)
}

#[pyclass(frozen)]
struct Drainer {
    #[cfg(feature = "io_uring")]
    read_fd: bool,
    b: OnceLock<Weak<Bridge>>,
}
#[pymethods]
impl Drainer {
    fn __call__(&self, py: Python<'_>) {
        if let Some(b) = self.b.get().and_then(Weak::upgrade) {
            #[cfg(feature = "io_uring")]
            if self.read_fd {
                b.efd_read();
            }
            drain(py, &b);
        }
    }
}

pub struct Bridge {
    ring: Ring,
    overflow: Mutex<Vec<JobKind>>,
    scheduled: AtomicBool,
    #[cfg(feature = "io_uring")]
    efd: i32,
    handle: Handle,
    event_loop: Py<PyAny>,
    call_soon: Py<PyAny>,
    #[cfg(not(feature = "io_uring"))]
    call_soon_threadsafe: Py<PyAny>,
    create_future: Py<PyAny>,
    fut_done: Py<PyAny>,
    fut_set_result: Py<PyAny>,
    fut_set_exception: Py<PyAny>,
    ensure_future: Py<PyAny>,
    drain_cb: Py<PyAny>,
    task_cls: Py<PyAny>,
    eager: bool,
    coroutine_type: Py<PyAny>,
}

impl Bridge {
    fn new(py: Python<'_>, event_loop: &Bound<'_, PyAny>, handle: Handle) -> PyResult<Arc<Self>> {
        #[cfg(feature = "io_uring")]
        let efd = {
            let efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
            if efd < 0 {
                return Err(PyRuntimeError::new_err("pyaio: eventfd failed"));
            }
            efd
        };
        let asyncio = py.import("asyncio")?;
        let (fut_done, fut_set_result, fut_set_exception) = {
            let probe = event_loop.getattr("create_future")?.call0()?;
            let ft = probe.get_type();
            (ft.getattr("done")?.unbind(), ft.getattr("set_result")?.unbind(), ft.getattr("set_exception")?.unbind())
        };
        let drainer = Py::new(py, Drainer {
            #[cfg(feature = "io_uring")]
            read_fd: false,
            b: OnceLock::new(),
        })?;
        #[cfg(feature = "io_uring")]
        let efd_dr = Py::new(py, Drainer { read_fd: true, b: OnceLock::new() })?;
        #[allow(unused_mut)]
        let mut b = Arc::new(Self {
            ring: Ring::new(),
            overflow: Mutex::new(Vec::new()),
            scheduled: AtomicBool::new(false),
            #[cfg(feature = "io_uring")]
            efd,
            handle,
            event_loop: event_loop.clone().unbind(),
            call_soon: event_loop.getattr("call_soon")?.unbind(),
            #[cfg(not(feature = "io_uring"))]
            call_soon_threadsafe: event_loop.getattr("call_soon_threadsafe")?.unbind(),
            create_future: event_loop.getattr("create_future")?.unbind(),
            fut_done,
            fut_set_result,
            fut_set_exception,
            ensure_future: asyncio.getattr("ensure_future")?.unbind(),
            drain_cb: drainer.clone_ref(py).into_any(),
            task_cls: asyncio.getattr("Task")?.unbind(),
            eager: py.version_info() >= (3, 12),
            coroutine_type: py.import("types")?.getattr("CoroutineType")?.unbind(),
        });
        let _ = drainer.get().b.set(Arc::downgrade(&b));
        #[cfg(feature = "io_uring")]
        {
            let _ = efd_dr.get().b.set(Arc::downgrade(&b));
            if let Err(e) = event_loop.call_method1("add_reader", (efd, efd_dr.into_any().bind(py))) {
                Arc::get_mut(&mut b).unwrap().efd = -1;
                unsafe { libc::close(efd); };
                return Err(e);
            }
        }
        Ok(b)
    }

        pub fn for_current_loop(py: Python<'_>) -> PyResult<Arc<Self>> {
        let l = get_running_loop(py)?;
        Self::for_loop(py, &l)
    }

        pub fn for_loop(py: Python<'_>, l: &Bound<'_, PyAny>) -> PyResult<Arc<Self>> {
        let ptr = l.as_ptr();
        if let Some(b) = REGISTRY.lock().unwrap().iter().find(|b| b.event_loop.as_ptr() == ptr) {
            return Ok(b.clone());
        }
        init(py, l, compat::tokio::get_runtime().handle().clone())
    }

    pub fn event_loop<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.event_loop.bind(py).clone()
    }

    #[inline]
    fn push(self: &Arc<Self>, job: JobKind) {
        if let Err(j) = self.ring.push(job) {
            self.overflow.lock().unwrap().push(j);
        }
        if !self.scheduled.swap(true, Ordering::AcqRel) {
            #[cfg(feature = "io_uring")]
            unsafe {
                let one: u64 = 1;
                libc::write(self.efd, std::ptr::from_ref(&one).cast(), 8);
            }
            #[cfg(not(feature = "io_uring"))]
            hub_wake(self);
        }
    }

        #[cfg(feature = "io_uring")]
    fn efd_read(&self) {
        let mut v: u64 = 0;
        unsafe { libc::read(self.efd, std::ptr::from_mut(&mut v).cast(), 8); }
    }

    fn pending(&self) -> bool {
        self.ring.in_flight() || !self.overflow.lock().unwrap().is_empty()
    }

            #[cfg(not(feature = "io_uring"))]
    fn fail_all(&self) {
        self.scheduled.store(false, Ordering::Release);
        while self.ring.pop().is_some() {}
        *self.overflow.lock().unwrap() = Vec::new();
    }

        pub fn report(&self, py: Python<'_>, msg: &str, err: &PyErr) {
        let ctx = PyDict::new(py);
        let _ = ctx.set_item("message", msg);
        let _ = ctx.set_item("exception", err.value(py));
        let _ = self.event_loop.bind(py).call_method1("call_exception_handler", (ctx,));
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        #[cfg(feature = "io_uring")]
        if self.efd >= 0 {
            let efd = self.efd;
            unsafe { libc::close(efd); };
        }
        while self.ring.pop().is_some() {}
        *self.overflow.lock().unwrap() = Vec::new();
    }
}

#[cfg(not(feature = "io_uring"))]
struct Hub {
    pending: Mutex<Vec<Arc<Bridge>>>,
    thread: Thread,
}
#[cfg(not(feature = "io_uring"))]
static HUB: OnceLock<Hub> = OnceLock::new();

#[cfg(not(feature = "io_uring"))]
fn hub() -> &'static Hub {
    HUB.get_or_init(|| {
        let th = std::thread::Builder::new()
            .name("pyaio-waker".into())
            .spawn(|| loop {
                std::thread::park();
                let Some(h) = HUB.get() else { continue };
                let list = std::mem::take(&mut *h.pending.lock().unwrap());
                if list.is_empty() {
                    continue;
                }
                Python::attach(|py| {
                    for b in list {
                        if b.call_soon_threadsafe.bind(py).call1((b.drain_cb.bind(py),)).is_err() {
                            b.fail_all();
                        }
                    }
                });
            })
            .expect("spawn pyaio waker");
        Hub { pending: Mutex::new(Vec::new()), thread: th.thread().clone() }
    })
}

#[cfg(not(feature = "io_uring"))]
fn hub_wake(b: &Arc<Bridge>) {
    let h = hub();
    h.pending.lock().unwrap().push(b.clone());
    h.thread.unpark();
}

const DRAIN_BUDGET: usize = 4;

fn drain(py: Python<'_>, b: &Arc<Bridge>) {
    for _ in 0..DRAIN_BUDGET {
        let mut n = 0usize;
        SPARE.with_borrow_mut(|spare| {
            while let Some(job) = b.ring.pop() {
                if !matches!(job, JobKind::Void) {
                    spare.push(job);
                }
            }
            spare.append(&mut *b.overflow.lock().unwrap());
            n = spare.len();
            for job in spare.drain(..) {
                run_job(py, b, job);
            }
        });
        if n == 0 {
            b.scheduled.store(false, Ordering::Release);
            if b.pending() && !b.scheduled.swap(true, Ordering::AcqRel) {
                let _ = b.call_soon.bind(py).call1((b.drain_cb.bind(py),));
            }
            return;
        }
    }
    let _ = b.call_soon.bind(py).call1((b.drain_cb.bind(py),));
}

#[inline]
fn run_job(py: Python<'_>, b: &Bridge, job: JobKind) {
    let _ = catch_unwind(AssertUnwindSafe(|| match job {
        JobKind::FinishInline { target, conv, buf } => {
            finish_future(py, b, &target, conv(buf, py));
        }
        JobKind::FinishPy { target, res } => finish_future(py, b, &target, res),
        JobKind::Cancel { slot } => {
            if let Some(t) = slot.0.lock().unwrap().take() {
                let _ = t.bind(py).call_method0(intern!(py, "cancel"));
            }
        }
        JobKind::Boxed(f) => f(py),
        JobKind::Void => {}
    }));
}

struct Slot(Mutex<Option<Py<PyAny>>>);

type DoneFn = Box<dyn FnOnce(&Bound<'_, PyAny>) + Send>;

#[pyclass]
struct DoneCb(Mutex<Option<DoneFn>>);
#[pymethods]
impl DoneCb {
    fn __call__(&self, task: &Bound<'_, PyAny>) {
        if let Some(f) = self.0.lock().unwrap().take() {
            f(task);
        }
    }
}

pub struct CallFuture<R> {
    rx: oneshot::Receiver<PyResult<R>>,
    slot: Arc<Slot>,
    bridge: Arc<Bridge>,
    done: bool,
    cancel_on_drop: bool,
}
impl<R> CallFuture<R> {
    #[must_use]
    pub const fn detached(mut self) -> Self {
        self.cancel_on_drop = false;
        self
    }
}
impl<R> Future for CallFuture<R> {
    type Output = PyResult<R>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(r) => {
                self.done = true;
                Poll::Ready(r.unwrap_or_else(|_| Err(PyRuntimeError::new_err("pyaio: event loop closed or call dropped"))))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl<R> Drop for CallFuture<R> {
    fn drop(&mut self) {
        if !self.done && self.cancel_on_drop {
            self.bridge.push(JobKind::Cancel { slot: self.slot.clone() });
        }
    }
}

impl Bridge {
                pub fn call<R, M, C>(self: &Arc<Self>, make: M, conv: C) -> CallFuture<R>
    where
        R: Send + 'static,
        M: for<'py> FnOnce(Python<'py>) -> PyResult<Bound<'py, PyAny>> + Send + 'static,
        C: for<'py> FnOnce(Bound<'py, PyAny>) -> PyResult<R> + Send + 'static,
    {
        self.call_in(None, make, conv)
    }

        pub fn call_in<R, M, C>(self: &Arc<Self>, context: Option<Py<PyAny>>, make: M, conv: C) -> CallFuture<R>
    where
        R: Send + 'static,
        M: for<'py> FnOnce(Python<'py>) -> PyResult<Bound<'py, PyAny>> + Send + 'static,
        C: for<'py> FnOnce(Bound<'py, PyAny>) -> PyResult<R> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let slot = Arc::new(Slot(Mutex::new(None)));
        let (s, b) = (slot.clone(), self.clone());
        self.push(JobKind::Boxed(Box::new(move |py| b.run_call(py, context, make, conv, tx, s))));
        CallFuture { rx, slot, bridge: self.clone(), done: false, cancel_on_drop: true }
    }

    fn start_task<'py>(&self, py: Python<'py>, obj: Bound<'py, PyAny>, ctx: Option<&Py<PyAny>>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let task_cls = self.task_cls.bind(py);
        let lp = self.event_loop.bind(py);
        let is_coro = obj.get_type().is(self.coroutine_type.bind(py));
        if !is_coro && !obj.hasattr(intern!(py, "__await__"))? {
            return Ok(None);
        }
        let kw = PyDict::new(py);
        kw.set_item("loop", lp)?;
        if is_coro {
            if self.eager {
                kw.set_item("eager_start", true)?;
            }
            return match ctx {
                Some(c) => c.bind(py).call_method(intern!(py, "run"), (task_cls, obj), Some(&kw)).map(Some),
                None => task_cls.call((obj,), Some(&kw)).map(Some),
            };
        }
        match ctx {
            Some(c) => c.bind(py).call_method(intern!(py, "run"), (self.ensure_future.bind(py), obj), Some(&kw)).map(Some),
            None => self.ensure_future.bind(py).call((obj,), Some(&kw)).map(Some),
        }
    }

    fn run_call<R, M, C>(self: &Arc<Self>, py: Python<'_>, ctx: Option<Py<PyAny>>, make: M, conv: C, tx: oneshot::Sender<PyResult<R>>, slot: Arc<Slot>)
    where
        R: Send + 'static,
        M: for<'py> FnOnce(Python<'py>) -> PyResult<Bound<'py, PyAny>>,
        C: for<'py> FnOnce(Bound<'py, PyAny>) -> PyResult<R> + Send + 'static,
    {
        let obj = match make(py) {
            Ok(o) => o,
            Err(e) => return drop(tx.send(Err(e))),
        };
        let ctx = ctx.filter(|c| !c.is_none(py));
        let started = match self.start_task(py, obj.clone(), ctx.as_ref()) {
            Ok(s) => s,
            Err(e) => return drop(tx.send(Err(e))),
        };
        let Some(task) = started else { return drop(tx.send(conv(obj))) };
        let finished = task.call_method0(intern!(py, "done")).and_then(|d| d.is_truthy()).unwrap_or(false);
        if finished {
            return drop(tx.send(task.call_method0(intern!(py, "result")).and_then(conv)));
        }
        *slot.0.lock().unwrap() = Some(task.clone().unbind());
        let cb = DoneCb(Mutex::new(Some(Box::new(move |t: &Bound<'_, PyAny>| {
            slot.0.lock().unwrap().take();
            let _ = tx.send(t.call_method0(intern!(t.py(), "result")).and_then(conv));
        }))));
        if let Err(e) = Py::new(py, cb).and_then(|cb| task.call_method1(intern!(py, "add_done_callback"), (cb,))) {
            self.report(py, "pyaio: add_done_callback failed", &e);
        }
    }
}

#[pyclass(frozen)]
struct Ready(Result<Py<PyAny>, PyErr>);
#[pymethods]
impl Ready {
    const fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&self, py: Python<'_>) -> PyResult<()> {
        match &self.0 {
            Ok(v) => Err(PyStopIteration::new_err((v.clone_ref(py),))),
            Err(e) => Err(e.clone_ref(py)),
        }
    }
}

#[pyclass(frozen)]
struct AbortOnCancel(AbortHandle);
#[pymethods]
impl AbortOnCancel {
    fn __call__(&self, fut: &Bound<'_, PyAny>) {
        if fut.call_method0(intern!(fut.py(), "cancelled")).and_then(|c| c.is_truthy()).unwrap_or(false) {
            self.0.abort();
        }
    }
}

fn noop_waker() -> Waker {
    fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VT)
    }
    const fn noop(_: *const ()) {}
    static VT: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VT)) }
}

fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".into()
    }
}

pub(crate) struct Guard<F>(pub Pin<Box<F>>);
impl<F: Future> Future for Guard<F> {
    type Output = Result<F::Output, String>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match catch_unwind(AssertUnwindSafe(|| self.0.as_mut().poll(cx))) {
            Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(p) => Poll::Ready(Err(panic_message(&p))),
        }
    }
}

pub(crate) fn finish_future(py: Python<'_>, b: &Bridge, fut: &Py<PyAny>, r: PyResult<Py<PyAny>>) {
    let f = fut.bind(py);
    if b.fut_done.bind(py).call1((f,)).and_then(|d| d.is_truthy()).unwrap_or(true) {
        return;
    }
    let res = match r {
        Ok(o) => b.fut_set_result.bind(py).call1((f, o)),
        Err(e) => b.fut_set_exception.bind(py).call1((f, e.into_value(py))),
    };
    if let Err(e) = res {
        e.write_unraisable(py, Some(f));
    }
}

pub(crate) fn to_py<T: for<'a> IntoPyObject<'a>>(py: Python<'_>, v: T) -> PyResult<Py<PyAny>> {
    v.into_pyobject(py).map(|b| b.into_bound().into_any().unbind()).map_err(Into::into)
}

fn flatten<T>(r: Result<PyResult<T>, String>) -> PyResult<T> {
    r.unwrap_or_else(|m| Err(err::RustPanic::new_err(format!("rust future panicked: {m}"))))
}

impl Bridge {
                pub fn future_into_py<'py, F, T>(self: &Arc<Self>, py: Python<'py>, fut: F) -> PyResult<Bound<'py, PyAny>>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'a> IntoPyObject<'a> + Send + 'static,
    {
        self.into_py(py, fut, true)
    }

        pub fn spawn_into_py<'py, F, T>(self: &Arc<Self>, py: Python<'py>, fut: F) -> PyResult<Bound<'py, PyAny>>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'a> IntoPyObject<'a> + Send + 'static,
    {
        self.into_py(py, fut, false)
    }

    fn into_py<'py, F, T>(self: &Arc<Self>, py: Python<'py>, fut: F, inline: bool) -> PyResult<Bound<'py, PyAny>>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'a> IntoPyObject<'a> + Send + 'static,
    {
        let mut fut = Guard(Box::pin(fut));
        if inline {
            let _guard = self.handle.enter();
            let w = noop_waker();
            if let Poll::Ready(r) = Pin::new(&mut fut).poll(&mut Context::from_waker(&w)) {
                let r = flatten(r).and_then(|v| to_py(py, v));
                return Ok(Py::new(py, Ready(r))?.into_bound(py).into_any());
            }
        }
        let pyfut = self.create_future.bind(py).call0()?;
        let target = pyfut.clone().unbind();
        let b = self.clone();
        let join = self.handle.spawn(async move {
            let r = flatten(fut.await);
            b.push(finish_job(&b, target, r));
        });
        pyfut.call_method1(intern!(py, "add_done_callback"), (AbortOnCancel(join.abort_handle()),))?;
        Ok(pyfut)
    }

    pub(crate) fn local_into_py<'py, F, T>(self: &Arc<Self>, py: Python<'py>, fut: F) -> PyResult<Bound<'py, PyAny>>
    where
        F: Future<Output = PyResult<T>> + 'static,
        T: for<'a> IntoPyObject<'a> + 'static,
    {
        let pyfut = self.create_future.bind(py).call0()?;
        let target = pyfut.clone().unbind();
        let b = self.clone();
        let fut = Guard(Box::pin(fut));
        let join = ::tokio::task::spawn_local(async move {
            let r = flatten(fut.await);
            let r = Python::attach(|py| r.and_then(|v| to_py(py, v)));
            b.push(JobKind::FinishPy { target, res: r });
        });
        pyfut.call_method1(intern!(py, "add_done_callback"), (AbortOnCancel(join.abort_handle()),))?;
        Ok(pyfut)
    }
}
