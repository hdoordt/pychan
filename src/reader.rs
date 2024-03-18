use crossbeam_utils::atomic::AtomicCell;

use self::py_bytes::PyBytesReceiver;

use super::*;

impl PyBytesReceiver {
    pub fn into_reader(self) -> PyBytesReader {
        PyBytesReader::new(self.reader.inner)
    }
}

struct PyBytesReaderScratch {
    cursor: usize,
    bytes: Py<PyBytes>,
}

#[pyclass]
#[derive(Clone)]
pub struct PyBytesReader {
    chan: Arc<PyChanInner<PyBytes>>,
    scratch: Arc<AtomicCell<Option<PyBytesReaderScratch>>>,
}

impl PyBytesReader {
    pub(crate) fn new(chan: Arc<PyChanInner<PyBytes>>) -> Self {
        Self {
            chan,
            scratch: Arc::new(AtomicCell::new(None)),
        }
    }

    fn has_items(&mut self) -> bool {
        !self.chan.buf.is_empty() || {
            let s = self.scratch.take();
            let has_scratch = s.is_some();
            self.scratch.store(s);
            has_scratch
        }
    }
}

impl AsyncRead for PyBytesReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        if !self.has_items() {
            if self.chan.is_closed() {
                return Poll::Ready(Ok(0));
            }
            self.chan.waker.register(cx.waker());
            return Poll::Pending;
        }

        // Write whatever is in scratch to buffer
        let mut n = if let Some(PyBytesReaderScratch { cursor, bytes }) = self.scratch.take() {
            let bytes_slice = Python::with_gil(|py| bytes.as_bytes(py));

            let n = buf.write(&bytes_slice[cursor..]).unwrap();

            let bytes_left = bytes_slice.len() - cursor - n;

            // Buffer was too small to fit all bytes from scratch
            if bytes_left != 0 {
                self.scratch.store(Some(PyBytesReaderScratch {
                    cursor: cursor + n,
                    bytes,
                }));
                return Poll::Ready(Ok(n));
            };
            n
        } else {
            0
        };

        // If there's still space in buf, pop an item from the chan buffer
        // and continue writing
        while let Some(bytes) = self.chan.buf.pop() {
            let bytes_slice = Python::with_gil(|py| bytes.as_bytes(py));
            let m = buf.write(bytes_slice).unwrap();
            n += m;

            if m < bytes_slice.len() {
                self.scratch
                    .store(Some(PyBytesReaderScratch { cursor: m, bytes }));
                break;
            }
        }

        Poll::Ready(Ok(n))
    }
}
