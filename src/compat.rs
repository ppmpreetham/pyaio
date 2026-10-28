use crate::{flatten, Bridge};
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use std::future::Future;
use std::sync::Arc;

pub mod err {
    pyo3::create_exception!(pyaio, RustPanic, pyo3::exceptions::PyException);
}

static GET_RUNNING_LOOP: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

pub fn get_running_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    GET_RUNNING_LOOP
        .get_or_try_init(py, || py.import("asyncio")?.getattr("get_running_loop").map(Bound::unbind))?
        .bind(py)
        .call0()
}

#[derive(Debug)]
struct TaskLocalsInner {
    event_loop: Py<PyAny>,
    context: Py<PyAny>,
}

#[derive(Debug)]
pub struct TaskLocals(Arc<TaskLocalsInner>);

impl Clone for TaskLocals {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl TaskLocals {
    #[must_use]
    pub fn new(event_loop: Bound<PyAny>) -> Self {
        Self(Arc::new(TaskLocalsInner { context: event_loop.py().None(), event_loop: event_loop.into() }))
    }
    pub fn with_running_loop(py: Python) -> PyResult<Self> {
        Ok(Self::new(get_running_loop(py)?))
    }
    #[must_use]
    pub fn with_context(self, context: Bound<PyAny>) -> Self {
        Self(Arc::new(TaskLocalsInner { event_loop: self.0.event_loop.clone_ref(context.py()), context: context.into() }))
    }
    pub fn copy_context(self, py: Python) -> PyResult<Self> {
        let ctx = py.import("contextvars")?.call_method0(intern!(py, "copy_context"))?;
        Ok(self.with_context(ctx))
    }
    #[must_use]
    pub fn event_loop<'p>(&self, py: Python<'p>) -> Bound<'p, PyAny> {
        self.0.event_loop.clone_ref(py).into_bound(py)
    }
    #[must_use]
    pub fn context<'p>(&self, py: Python<'p>) -> Bound<'p, PyAny> {
        self.0.context.clone_ref(py).into_bound(py)
    }
    #[deprecated(note = "please use `clone` instead")]
    #[must_use]
    pub fn clone_ref(&self, _py: Python<'_>) -> Self {
        self.clone()
    }
}

pub fn into_future_with_locals(
    locals: &TaskLocals,
    awaitable: Bound<PyAny>,
) -> PyResult<impl Future<Output = PyResult<Py<PyAny>>> + Send> {
    let py = awaitable.py();
    let b = Bridge::for_loop(py, &locals.event_loop(py))?;
    let aw = awaitable.unbind();
    let ctx = {
        let c = locals.context(py);
        if c.is_none() { None } else { Some(c.unbind()) }
    };
    Ok(b.call_in(
        ctx,
        move |py| {
            let o = aw.into_bound(py);
            if o.hasattr(intern!(py, "__await__"))? {
                Ok(o)
            } else {
                Err(PyTypeError::new_err(format!("object {} can't be used in 'await' expression", o.get_type().name()?)))
            }
        },
        |o| Ok(o.unbind()),
    )
    .detached())
}

pub mod tokio {}
