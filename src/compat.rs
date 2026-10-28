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

pub mod tokio {
    use super::{get_running_loop, into_future_with_locals, Bridge, TaskLocals};
    use pyo3::exceptions::PyRuntimeError;
    use pyo3::prelude::{Bound, IntoPyObject, Py, PyAny, PyAnyMethods, Python, PyResult};
    use std::future::Future;
    use ::tokio::runtime::{Builder, Runtime};
    use std::sync::{LazyLock, Mutex, OnceLock};

    static BUILDER: LazyLock<Mutex<Builder>> = LazyLock::new(|| {
        let mut b = Builder::new_multi_thread();
        b.enable_all();
        Mutex::new(b)
    });

    enum Pyo3Runtime {
        Owned(Runtime),
        Borrowed(&'static Runtime),
    }
    static RUNTIME: OnceLock<Pyo3Runtime> = OnceLock::new();

        pub fn init(builder: Builder) {
        *BUILDER.lock().unwrap() = builder;
    }

        #[allow(clippy::result_unit_err)]
    pub fn init_with_runtime(runtime: &'static Runtime) -> Result<(), ()> {
        RUNTIME.set(Pyo3Runtime::Borrowed(runtime)).map_err(|_| ())
    }

    pub fn get_runtime<'a>() -> &'a Runtime {
        match RUNTIME.get_or_init(|| Pyo3Runtime::Owned(BUILDER.lock().unwrap().build().expect("Unable to build Tokio runtime"))) {
            Pyo3Runtime::Owned(r) => r,
            Pyo3Runtime::Borrowed(r) => r,
        }
    }

    ::tokio::task_local! { static TASK_LOCALS: TaskLocals; }

    pub async fn scope<F, R>(locals: TaskLocals, fut: F) -> R
    where
        F: Future<Output = R> + Send + 'static,
    {
        TASK_LOCALS.scope(locals, fut).await
    }

    pub async fn scope_local<F, R>(locals: TaskLocals, fut: F) -> R
    where
        F: Future<Output = R> + 'static,
    {
        TASK_LOCALS.scope(locals, fut).await
    }

    pub fn get_current_locals(py: Python) -> PyResult<TaskLocals> {
        match TASK_LOCALS.try_with(std::clone::Clone::clone) {
            Ok(l) => Ok(l),
            Err(_) => TaskLocals::with_running_loop(py)?.copy_context(py),
        }
    }

    pub fn get_current_loop(py: Python) -> PyResult<Bound<PyAny>> {
        match TASK_LOCALS.try_with(|l| l.event_loop(py)) {
            Ok(l) => Ok(l),
            Err(_) => get_running_loop(py),
        }
    }

    pub fn future_into_py_with_locals<F, T>(py: Python, locals: TaskLocals, fut: F) -> PyResult<Bound<PyAny>>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'py> IntoPyObject<'py> + Send + 'static,
    {
        let b = Bridge::for_loop(py, &locals.event_loop(py))?;
        b.spawn_into_py(py, scope(locals, fut))
    }

    pub fn future_into_py<F, T>(py: Python, fut: F) -> PyResult<Bound<PyAny>>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'py> IntoPyObject<'py> + Send + 'static,
    {
        future_into_py_with_locals(py, get_current_locals(py)?, fut)
    }

    pub fn future_into_py_with_locals_inline<F, T>(py: Python, locals: TaskLocals, fut: F) -> PyResult<Bound<PyAny>>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'py> IntoPyObject<'py> + Send + 'static,
    {
        let b = Bridge::for_loop(py, &locals.event_loop(py))?;
        b.future_into_py(py, scope(locals, fut))
    }

    pub fn future_into_py_inline<F, T>(py: Python, fut: F) -> PyResult<Bound<PyAny>>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'py> IntoPyObject<'py> + Send + 'static,
    {
        future_into_py_with_locals_inline(py, get_current_locals(py)?, fut)
    }

    pub fn local_future_into_py_with_locals<F, T>(py: Python, locals: TaskLocals, fut: F) -> PyResult<Bound<PyAny>>
    where
        F: Future<Output = PyResult<T>> + 'static,
        T: for<'py> IntoPyObject<'py> + 'static,
    {
        let b = Bridge::for_loop(py, &locals.event_loop(py))?;
        b.local_into_py(py, scope_local(locals, fut))
    }

    pub fn local_future_into_py<F, T>(py: Python, fut: F) -> PyResult<Bound<PyAny>>
    where
        F: Future<Output = PyResult<T>> + 'static,
        T: for<'py> IntoPyObject<'py> + 'static,
    {
        local_future_into_py_with_locals(py, get_current_locals(py)?, fut)
    }

    pub fn into_future(awaitable: Bound<PyAny>) -> PyResult<impl Future<Output = PyResult<Py<PyAny>>> + Send> {
        let locals = get_current_locals(awaitable.py())?;
        into_future_with_locals(&locals, awaitable)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn run_until_complete<F, T>(event_loop: Bound<PyAny>, fut: F) -> PyResult<T>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: Send + Sync + 'static,
    {
        let py = event_loop.py();
        let (tx, rx) = std::sync::mpsc::channel::<T>();
        let locals = TaskLocals::new(event_loop.clone()).copy_context(py)?;
        let pyfut = future_into_py_with_locals(py, locals, async move {
            let v = fut.await?;
            let _ = tx.send(v);
            Ok(())
        })?;
        event_loop.call_method1("run_until_complete", (pyfut,))?;
        rx.try_recv().map_err(|_| PyRuntimeError::new_err("future did not complete"))
    }

    pub fn run<F, T>(py: Python, fut: F) -> PyResult<T>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: Send + Sync + 'static,
    {
        let asyncio = py.import("asyncio")?;
        let lp = asyncio.call_method0("new_event_loop")?;
        asyncio.call_method1("set_event_loop", (&lp,))?;
        let res = run_until_complete(lp.clone(), fut);
        if let Ok(sd) = lp.call_method0("shutdown_asyncgens") {
            let _ = lp.call_method1("run_until_complete", (sd,));
        }
        let _ = lp.call_method0("close");
        let _ = asyncio.call_method1("set_event_loop", (py.None(),));
        res
    }
}

#[allow(dead_code)]
fn _keep(r: Result<PyResult<()>, String>) -> PyResult<()> {
    flatten(r)
}
