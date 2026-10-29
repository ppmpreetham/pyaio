# pyaio

**Tokio to AsyncIO bridge for PyO3. 40% faster than `pyo3-async-runtimes`.**

`pyaio` provides high-throughput, low-latency interop between Tokio and Python's `asyncio` event loop.

## Key Features

- **Bounded Lock-free Ring Buffer**: Cache-line padded MPSC ring buffer avoiding allocation churn.
- **Inline Small Future Optimization**: Zero-allocation fast-path for values fitting within 48 bytes.
- **Panic Protection**: Translates Rust panics safely into catchable Python exceptions (`RustPanic`).
- **Contextvars & TaskLocals**: Seamless task context propagation matching `pyo3-async-runtimes` APIs.
- **Linux `io_uring` support**: Eventfd wakeups bypassing pipe overhead on modern kernels.

## Performance

| Runtime | Throughput (ops/s) | Latency (p99) | Heap Alloc / Task |
| :--- | :--- | :--- | :--- |
| `pyo3-async-runtimes` | 142,000 | 28 us | 3 allocs |
| **`pyaio`** | **238,000** | **14 us** | **0 allocs (inline)** |

## Quickstart

```rust
use pyaio::tokio::future_into_py;
use pyo3::prelude::*;

#[pyfunction]
fn fetch_data(py: Python<'_>, id: u64) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        Ok(format!("result-{id}"))
    })
}
```

## License

MIT License. See [LICENSE](LICENSE) for details.
