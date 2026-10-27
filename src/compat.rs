use pyo3::prelude::*;
use std::sync::Arc;

pub mod err {
    pyo3::create_exception!(pyaio, RustPanic, pyo3::exceptions::PyException);
}

pub fn get_running_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    py.import("asyncio")?.getattr("get_running_loop")?.call0()
}

pub struct TaskLocals;
pub mod tokio {}
pub fn into_future_with_locals() {}
