use std::{
    io::Write,
    ops::Not,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use crossbeam_queue::ArrayQueue;
use futures::{sink::Sink, task::AtomicWaker, AsyncRead, Stream};
use pyo3::{
    exceptions::{self},
    prelude::*,
    types::PyBytes,
};

mod py;
pub mod reader;

#[derive(Debug, PartialEq)]
#[pyclass(eq, eq_int)]
pub enum Error {
    QueueFull,
    Closed,
}

impl From<Error> for PyErr {
    fn from(value: Error) -> Self {
        match value {
            this @ Error::QueueFull => PyErr::new::<exceptions::asyncio::QueueFull, _>(this),
            this @ Error::Closed => PyErr::new::<exceptions::PyBrokenPipeError, _>(this),
        }
    }
}

struct PyChanInner<T> {
    buf: ArrayQueue<Py<T>>,
    closed: AtomicBool,
    waker: AtomicWaker,
}

impl<T> PyChanInner<T> {
    fn new(capacity: usize) -> Self {
        Self {
            buf: ArrayQueue::new(capacity),
            closed: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

pub struct PySender<T> {
    inner: Arc<PyChanInner<T>>,
}

impl<T> Clone for PySender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Sink<Py<T>> for PySender<T> {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner
            .buf
            .is_full()
            .not()
            .then_some(Poll::Ready(Ok(())))
            .unwrap_or_else(|| {
                self.inner.waker.register(cx.waker());
                Poll::Pending
            })
    }

    fn start_send(self: Pin<&mut Self>, item: Py<T>) -> Result<(), Self::Error> {
        let res = self.inner.buf.push(item).map_err(|_| Error::QueueFull);
        self.inner.waker.wake();
        res
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.waker.wake();
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.waker.wake();
        Poll::Ready(Ok(()))
    }
}

pub struct PyReceiver<T> {
    inner: Arc<PyChanInner<T>>,
}

impl<T> Clone for PyReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Stream for PyReceiver<T> {
    type Item = Py<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner
            .buf
            .pop()
            .map(|item| Poll::Ready(Some(item)))
            .unwrap_or_else(|| {
                self.inner
                    .is_closed()
                    .not()
                    .then(|| {
                        self.inner.waker.register(cx.waker());
                        Poll::Pending
                    })
                    .unwrap_or(Poll::Ready(None))
            })
    }
}

pub fn channel<T>(capacity: usize) -> (PySender<T>, PyReceiver<T>) {
    let inner = PyChanInner::new(capacity);
    let inner = Arc::new(inner);
    let reader = PyReceiver {
        inner: inner.clone(),
    };
    let writer = PySender { inner };

    (writer, reader)
}

macro_rules! specialized_pychan {
    ($receiver_name:ident, $sender_name:ident, $item:ty, $mod:ident) => {
        pub mod $mod {
            use crate::{PyReceiver, PySender};
            use futures::{sink::Sink, Stream};
            use pyo3::{pyclass, Py};
            use std::{
                pin::Pin,
                task::{Context, Poll},
            };

            #[pyo3::pyclass]
            #[derive(Clone)]
            #[pin_project::pin_project]
            pub struct $sender_name {
                #[pin]
                writer: PySender<$item>,
            }

            impl Sink<Py<$item>> for $sender_name {
                type Error = <PySender<$item> as Sink<Py<$item>>>::Error;

                fn poll_ready(
                    self: Pin<&mut Self>,
                    cx: &mut Context<'_>,
                ) -> Poll<Result<(), Self::Error>> {
                    let this = self.project();
                    this.writer.poll_ready(cx)
                }

                fn start_send(self: Pin<&mut Self>, item: Py<$item>) -> Result<(), Self::Error> {
                    let this = self.project();
                    this.writer.start_send(item)
                }

                fn poll_flush(
                    self: Pin<&mut Self>,
                    cx: &mut Context<'_>,
                ) -> Poll<Result<(), Self::Error>> {
                    let this = self.project();
                    this.writer.poll_flush(cx)
                }

                fn poll_close(
                    self: Pin<&mut Self>,
                    cx: &mut Context<'_>,
                ) -> Poll<Result<(), Self::Error>> {
                    let this = self.project();
                    this.writer.poll_close(cx)
                }
            }

            #[pyclass]
            #[derive(Clone)]
            #[pin_project::pin_project]
            pub struct $receiver_name {
                #[pin]
                pub(crate) reader: PyReceiver<$item>,
            }

            impl Stream for $receiver_name {
                type Item = <PyReceiver<$item> as Stream>::Item;

                fn poll_next(
                    self: Pin<&mut Self>,
                    cx: &mut Context<'_>,
                ) -> Poll<Option<Self::Item>> {
                    let this = self.project();
                    this.reader.poll_next(cx)
                }
            }

            pub fn channel(capacity: usize) -> ($sender_name, $receiver_name) {
                let (writer, reader) = crate::channel(capacity);

                let writer = $sender_name { writer };

                let reader = $receiver_name { reader };

                (writer, reader)
            }
        }
    };
}

specialized_pychan!(
    PyBytesReceiver,
    PyBytesSender,
    pyo3::types::PyBytes,
    py_bytes
);
