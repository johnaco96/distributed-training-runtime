//! `dtruntime._native` — the Rust hot path for DTRS shard I/O.
//!
//! Python's per-record decode loop (`torch.frombuffer` + `torch.stack` per
//! sample) is the measured bottleneck of the data pipeline. This module
//! replaces it with:
//!
//! - [`decode_dtrs`]: one-pass batch decode of a whole DTRS v1 shard into two
//!   contiguous buffers, run with the GIL released. The buffers implement the
//!   Python buffer protocol, so `torch.frombuffer` / `np.frombuffer` wrap
//!   them **zero-copy** — the tensor holds a reference to the Rust memory.
//! - [`ShardStream`]: ordered prefetching iterator over shard URIs — up to
//!   `prefetch` shards are fetched (s3:// via `runtime-storage`'s aws-sdk
//!   client, file:// or plain path), SHA-256-verified, and decoded
//!   concurrently on Rust threads while Python consumes the current one.
//!
//! The DTRS v1 layout (spec: `python/dtruntime/data/shard_reader.py`):
//! 32-byte header (`DTRS` magic, u32 version=1, u32 record_count,
//! u32 input_dim, u32 num_classes, 12 reserved bytes), then fixed-stride
//! records of `input_dim` little-endian f32 features + one i32 label.
//!
//! This crate does not `forbid(unsafe_code)`: exporting the buffer protocol
//! requires filling CPython's `Py_buffer` struct by hand. The unsafe surface
//! is confined to `__getbuffer__` implementations on frozen (immutable)
//! pyclasses whose backing `Vec`s are never touched after construction.

use pyo3::exceptions::{PyIOError, PyStopIteration, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::os::raw::{c_char, c_int, c_void};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;

use runtime_storage::uri::{parse_s3_uri, uri_to_local_path};
use runtime_storage::{S3Storage, S3StorageConfig, StorageBackend, StorageKey};

const HEADER_SIZE: usize = 32;
const MAGIC: &[u8; 4] = b"DTRS";
const VERSION: u32 = 1;

// ── decode ───────────────────────────────────────────────────────────────────

struct Decoded {
    features: Vec<f32>,
    labels: Vec<i32>,
    record_count: usize,
    input_dim: usize,
    num_classes: usize,
}

fn read_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

fn decode(data: &[u8]) -> Result<Decoded, String> {
    if data.len() < HEADER_SIZE {
        return Err(format!(
            "shard too short for DTRS header: {} bytes",
            data.len()
        ));
    }
    if &data[0..4] != MAGIC {
        return Err(format!("bad DTRS magic: {:?}", &data[0..4]));
    }
    let version = read_u32(data, 4);
    if version != VERSION {
        return Err(format!("unsupported DTRS version {version}"));
    }
    let record_count = read_u32(data, 8) as usize;
    let input_dim = read_u32(data, 12) as usize;
    let num_classes = read_u32(data, 16) as usize;
    let record_size = input_dim * 4 + 4;
    let needed = HEADER_SIZE + record_count * record_size;
    if data.len() < needed {
        return Err(format!(
            "truncated DTRS shard: header claims {record_count} records \
             ({needed} bytes), got {}",
            data.len()
        ));
    }

    let mut features = Vec::with_capacity(record_count * input_dim);
    let mut labels = Vec::with_capacity(record_count);
    let body = &data[HEADER_SIZE..needed];
    for record in body.chunks_exact(record_size) {
        let (feat_bytes, label_bytes) = record.split_at(input_dim * 4);
        features.extend(
            feat_bytes
                .chunks_exact(4)
                .map(|b| f32::from_le_bytes(b.try_into().unwrap())),
        );
        labels.push(i32::from_le_bytes(label_bytes.try_into().unwrap()));
    }
    Ok(Decoded {
        features,
        labels,
        record_count,
        input_dim,
        num_classes,
    })
}

// ── buffer-protocol exports ──────────────────────────────────────────────────

/// Fill a `Py_buffer` for a contiguous 1-D array backed by a pyclass field.
///
/// Safety: `owner` must keep `ptr` alive and unmoved for the lifetime of the
/// view — guaranteed here because the exporting pyclasses are `frozen` and
/// never mutate their `Vec`s, and `view.obj` holds a strong reference to the
/// owner until `__releasebuffer__`.
unsafe fn fill_buffer(
    owner: *mut ffi::PyObject,
    ptr: *const c_void,
    len_bytes: usize,
    itemsize: usize,
    format: &'static [u8], // NUL-terminated, e.g. b"f\0"
    view: *mut ffi::Py_buffer,
    flags: c_int,
) -> PyResult<()> {
    if view.is_null() {
        return Err(PyValueError::new_err("null Py_buffer"));
    }
    (*view).obj = owner; // strong ref transferred; released in bf_releasebuffer
    (*view).buf = ptr as *mut c_void;
    (*view).len = len_bytes as ffi::Py_ssize_t;
    // The Vecs are uniquely owned by the frozen pyclass and never read again
    // from Rust, so a writable view is sound; it also keeps torch.frombuffer
    // from warning about read-only buffers.
    (*view).readonly = 0;
    (*view).itemsize = itemsize as ffi::Py_ssize_t;
    (*view).format = if flags & ffi::PyBUF_FORMAT != 0 {
        format.as_ptr() as *mut c_char
    } else {
        std::ptr::null_mut()
    };
    (*view).ndim = 1;
    (*view).shape = std::ptr::null_mut();
    (*view).strides = std::ptr::null_mut();
    (*view).suboffsets = std::ptr::null_mut();
    (*view).internal = std::ptr::null_mut();
    Ok(())
}

// F32Buffer and I32Buffer below are deliberately near-identical: `#[pyclass]`
// cannot be generic, so each element type needs its own concrete class. All the
// unsafe work lives once in `fill_buffer` — what repeats is only the typed
// wrapper (element size and format code). A macro for two instantiations would
// cost more readability than the duplication does.

/// Contiguous little-endian f32 array exported via the buffer protocol.
#[pyclass(frozen, module = "dtruntime._native")]
struct F32Buffer {
    data: Vec<f32>,
}

#[pymethods]
impl F32Buffer {
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let data = &slf.get().data;
        fill_buffer(
            slf.clone().into_ptr(),
            data.as_ptr() as *const c_void,
            std::mem::size_of_val(data.as_slice()),
            4,
            b"f\0",
            view,
            flags,
        )
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {
        // view.obj's strong reference is dropped by CPython after this hook.
    }
}

/// Contiguous little-endian i32 array exported via the buffer protocol.
#[pyclass(frozen, module = "dtruntime._native")]
struct I32Buffer {
    data: Vec<i32>,
}

#[pymethods]
impl I32Buffer {
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let data = &slf.get().data;
        fill_buffer(
            slf.clone().into_ptr(),
            data.as_ptr() as *const c_void,
            std::mem::size_of_val(data.as_slice()),
            4,
            b"i\0",
            view,
            flags,
        )
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}
}

/// One decoded DTRS shard: `features` is a flat f32 buffer of
/// `record_count * input_dim` elements (row-major), `labels` an i32 buffer of
/// `record_count` elements. Wrap them with `torch.frombuffer(...)` /
/// `np.frombuffer(...)` — no copy is made.
#[pyclass(frozen, module = "dtruntime._native")]
struct DtrsBatch {
    #[pyo3(get)]
    record_count: usize,
    #[pyo3(get)]
    input_dim: usize,
    #[pyo3(get)]
    num_classes: usize,
    features: Py<F32Buffer>,
    labels: Py<I32Buffer>,
}

#[pymethods]
impl DtrsBatch {
    #[getter]
    fn features(&self, py: Python<'_>) -> Py<F32Buffer> {
        self.features.clone_ref(py)
    }

    #[getter]
    fn labels(&self, py: Python<'_>) -> Py<I32Buffer> {
        self.labels.clone_ref(py)
    }
}

fn batch_from(py: Python<'_>, d: Decoded) -> PyResult<DtrsBatch> {
    Ok(DtrsBatch {
        record_count: d.record_count,
        input_dim: d.input_dim,
        num_classes: d.num_classes,
        features: Py::new(py, F32Buffer { data: d.features })?,
        labels: Py::new(py, I32Buffer { data: d.labels })?,
    })
}

// ── fetch ────────────────────────────────────────────────────────────────────

/// Per-bucket cache of S3 clients (each owns its own Tokio runtime; the sync
/// `StorageBackend` API is callable from any plain thread). Config comes from
/// the `DTRUNTIME_S3_*` environment via [`S3StorageConfig::from_env`].
fn s3_for_bucket(bucket: &str) -> Result<Arc<S3Storage>, String> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<S3Storage>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = cache.lock().unwrap();
    if let Some(s) = cache.get(bucket) {
        return Ok(Arc::clone(s));
    }
    let storage = S3Storage::new(S3StorageConfig::from_env(bucket)?).map_err(|e| e.to_string())?;
    let storage = Arc::new(storage);
    cache.insert(bucket.to_string(), Arc::clone(&storage));
    Ok(storage)
}

fn fetch_bytes(uri: &str) -> Result<Vec<u8>, String> {
    if uri.starts_with("s3://") {
        let (bucket, key) = parse_s3_uri(uri)?;
        let storage = s3_for_bucket(&bucket)?;
        let storage_key = StorageKey::new(&key).map_err(|e| e.to_string())?;
        storage.get(&storage_key).map_err(|e| e.to_string())
    } else {
        std::fs::read(uri_to_local_path(uri)).map_err(|e| format!("read {uri}: {e}"))
    }
}

fn verify_sha256(uri: &str, data: &[u8], expected: &str) -> Result<(), String> {
    let actual = hex::encode(Sha256::digest(data));
    if !actual.eq_ignore_ascii_case(expected) {
        return Err(format!(
            "checksum mismatch for {uri}: expected {expected}, got {actual}"
        ));
    }
    Ok(())
}

fn fetch_verify_decode(uri: &str, checksum: Option<&str>) -> Result<Decoded, String> {
    let data = fetch_bytes(uri)?;
    if let Some(expected) = checksum {
        verify_sha256(uri, &data, expected)?;
    }
    decode(&data)
}

// ── prefetching stream ───────────────────────────────────────────────────────

/// Ordered prefetching iterator over `(uri, sha256 | None)` shard specs.
///
/// Up to `prefetch` shards are in flight at once, each fetched + verified +
/// decoded on its own Rust thread while Python consumes the head of the
/// queue. Results arrive strictly in spec order.
#[pyclass(module = "dtruntime._native")]
struct ShardStream {
    specs: Vec<(String, Option<String>)>,
    next_spawn: usize,
    prefetch: usize,
    inflight: VecDeque<JoinHandle<Result<Decoded, String>>>,
}

impl ShardStream {
    fn spawn_until_full(&mut self) {
        while self.inflight.len() < self.prefetch && self.next_spawn < self.specs.len() {
            let (uri, checksum) = self.specs[self.next_spawn].clone();
            self.next_spawn += 1;
            self.inflight.push_back(std::thread::spawn(move || {
                fetch_verify_decode(&uri, checksum.as_deref())
            }));
        }
    }
}

#[pymethods]
impl ShardStream {
    #[new]
    #[pyo3(signature = (specs, prefetch = 2))]
    fn new(specs: Vec<(String, Option<String>)>, prefetch: usize) -> PyResult<Self> {
        if prefetch == 0 {
            return Err(PyValueError::new_err("prefetch must be >= 1"));
        }
        let mut stream = ShardStream {
            specs,
            next_spawn: 0,
            prefetch,
            inflight: VecDeque::new(),
        };
        stream.spawn_until_full();
        Ok(stream)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<DtrsBatch> {
        let Some(handle) = slf.inflight.pop_front() else {
            return Err(PyStopIteration::new_err(()));
        };
        slf.spawn_until_full();
        let py = slf.py();
        let result = py.detach(move || {
            handle
                .join()
                .unwrap_or_else(|_| Err("shard fetch thread panicked".into()))
        });
        batch_from(py, result.map_err(PyIOError::new_err)?)
    }

    fn __len__(&self) -> usize {
        self.specs.len()
    }
}

// ── module ───────────────────────────────────────────────────────────────────

/// Decode a full DTRS v1 shard from an in-memory bytes-like object.
#[pyfunction]
fn decode_dtrs(py: Python<'_>, data: &[u8]) -> PyResult<DtrsBatch> {
    let decoded = py.detach(|| decode(data)).map_err(PyValueError::new_err)?;
    batch_from(py, decoded)
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DtrsBatch>()?;
    m.add_class::<F32Buffer>()?;
    m.add_class::<I32Buffer>()?;
    m.add_class::<ShardStream>()?;
    m.add_function(wrap_pyfunction!(decode_dtrs, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
