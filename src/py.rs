use futures::{AsyncReadExt, SinkExt};
use pyo3::{prelude::*, types::PyBytes};

use crate::{py_bytes::PyBytesSender, reader::PyBytesReader};

#[pyfunction]
fn bytes_chan(capacity: usize) -> (PyBytesSender, PyBytesReader) {
    let (sender, receiver) = crate::py_bytes::channel(capacity);
    let reader = receiver.into_reader();
    (sender, reader)
}

#[pyfunction]
async fn chan_send(mut sender: PyBytesSender, bytes: Py<PyBytes>) -> PyResult<()> {
    sender.send(bytes).await?;
    Ok(())
}

#[pyfunction]
async fn sender_close(mut sender: PyBytesSender) -> PyResult<()> {
    sender.close().await?;
    Ok(())
}

#[pyfunction]
async fn chan_read(mut reader: PyBytesReader, bytes: usize) -> PyResult<Py<PyBytes>> {
    // TODO would be great if we could avoid zeroing the buffer
    let mut buf = vec![0; bytes];
    let n = reader.read(&mut buf).await?;
    let bytes = Python::with_gil(|py| PyBytes::new_bound(py, &buf[..n]).unbind());

    Ok(bytes)
}

#[pymodule]
fn pychan(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(bytes_chan, m)?)?;
    m.add_function(wrap_pyfunction!(chan_send, m)?)?;
    m.add_function(wrap_pyfunction!(chan_read, m)?)?;
    m.add_function(wrap_pyfunction!(sender_close, m)?)?;
    m.add_class::<PyBytesSender>()?;
    Ok(())
}
