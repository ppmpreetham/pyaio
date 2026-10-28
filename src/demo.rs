use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use crate::tokio::{
    future_into_py, future_into_py_inline, future_into_py_with_locals, get_current_loop,
    get_current_locals, into_future, local_future_into_py, run, run_until_complete, scope,
};
use crate::{into_future_with_locals, TaskLocals};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static DROPPED: AtomicUsize = AtomicUsize::new(0);
struct DropCount;
impl Drop for DropCount { fn drop(&mut self) { DROPPED.fetch_add(1, Ordering::SeqCst); } }

#[pyfunction]
fn sleep_ret(py: Python<'_>, x: i64, ms: u64) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move { tokio::time::sleep(Duration::from_millis(ms)).await; Ok(x + 1) })
}
#[pyfunction]
fn ready_ret(py: Python<'_>, x: i64) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move { Ok(x + 1) })
}
#[pyfunction]
fn ready_inline(py: Python<'_>, x: i64) -> PyResult<Bound<'_, PyAny>> {
    future_into_py_inline(py, async move { Ok(x + 1) })
}
#[pyfunction]
fn yield_ret(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async { tokio::task::yield_now().await; Ok(0i64) })
}
#[pyfunction]
fn fail(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async { tokio::task::yield_now().await; Err::<i64, _>(PyValueError::new_err("rust error")) })
}
#[pyfunction]
#[allow(unreachable_code)]
fn boom(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async { tokio::task::yield_now().await; panic!("kaboom"); Ok(1i64) })
}
#[pyfunction]
fn hang_then_drop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async { let _g = DropCount; tokio::time::sleep(Duration::from_secs(30)).await; Ok(0i64) })
}
#[pyfunction]
fn dropped() -> usize { DROPPED.load(Ordering::SeqCst) }

#[pyfunction]
fn call_py<'py>(py: Python<'py>, f: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    let locals = get_current_locals(py)?;
    let fut = into_future_with_locals(&locals, f.call0()?)?;
    future_into_py(py, async move { let r = fut.await?; Python::attach(|py| r.extract::<i64>(py)) })
}
#[pyfunction]
fn call_py_in_task(py: Python<'_>, f: Py<PyAny>) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let fut = Python::attach(|py| into_future(f.bind(py).call0()?))?;
        let r = fut.await?;
        Python::attach(|py| r.extract::<i64>(py))
    })
}
#[pyfunction]
fn call_py_err(py: Python<'_>, f: Py<PyAny>) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let fut = Python::attach(|py| into_future(f.bind(py).call0()?))?;
        fut.await?;
        Ok(0i64)
    })
}
#[pyfunction]
fn run_main(py: Python<'_>) -> PyResult<i64> { run(py, async { Ok(7i64) }) }
#[pyfunction]
fn run_until(lp: Bound<'_, PyAny>) -> PyResult<i64> {
    run_until_complete(lp, async { tokio::time::sleep(Duration::from_millis(1)).await; Ok(5i64) })
}
#[pyfunction]
fn current_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> { get_current_loop(py) }
#[pyfunction]
fn locals_probe(py: Python<'_>, lp: &Bound<'_, PyAny>) -> PyResult<(bool, bool)> {
    let l = TaskLocals::new(lp.clone()).copy_context(py)?;
    Ok((l.event_loop(py).is(lp), !l.context(py).is_none()))
}
#[pyfunction]
fn with_locals<'py>(py: Python<'py>, lp: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    let l = TaskLocals::new(lp).copy_context(py)?;
    future_into_py_with_locals(py, l.clone(), scope(l, async { Ok(3i64) }))
}
#[pyfunction]
fn local_compile_check(py: Python<'_>) { let _ = local_future_into_py(py, async { Ok(1i64) }); }

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sleep_ret, m)?)?;
    m.add_function(wrap_pyfunction!(ready_ret, m)?)?;
    m.add_function(wrap_pyfunction!(ready_inline, m)?)?;
    m.add_function(wrap_pyfunction!(yield_ret, m)?)?;
    m.add_function(wrap_pyfunction!(fail, m)?)?;
    m.add_function(wrap_pyfunction!(boom, m)?)?;
    m.add_function(wrap_pyfunction!(hang_then_drop, m)?)?;
    m.add_function(wrap_pyfunction!(dropped, m)?)?;
    m.add_function(wrap_pyfunction!(call_py, m)?)?;
    m.add_function(wrap_pyfunction!(call_py_in_task, m)?)?;
    m.add_function(wrap_pyfunction!(call_py_err, m)?)?;
    m.add_function(wrap_pyfunction!(run_main, m)?)?;
    m.add_function(wrap_pyfunction!(run_until, m)?)?;
    m.add_function(wrap_pyfunction!(current_loop, m)?)?;
    m.add_function(wrap_pyfunction!(locals_probe, m)?)?;
    m.add_function(wrap_pyfunction!(with_locals, m)?)?;
    m.add_function(wrap_pyfunction!(local_compile_check, m)?)?;
    Ok(())
}

#[pymodule]
fn pyaio_faq(m: &Bound<'_, PyModule>) -> PyResult<()> { register(m) }
