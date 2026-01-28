// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{sync::Arc, time::Duration};

use pyo3::{
    exceptions::PyValueError,
    pyclass, pymethods,
    types::{PyByteArray, PyByteArrayMethods, PyBytes, PyDictMethods},
    Bound, PyResult, Python,
};

use lance::dataset::BlobFile as InnerBlobFile;

use crate::{error::PythonErrorExt, rt};

#[pyclass]
pub struct LanceBlobFile {
    inner: Arc<InnerBlobFile>,
}

#[pymethods]
impl LanceBlobFile {
    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let inner = self.inner.clone();
        rt().block_on(Some(py), inner.close())?.infer_error()
    }

    pub fn is_closed(&self, py: Python<'_>) -> PyResult<bool> {
        let inner = self.inner.clone();
        rt().block_on(Some(py), inner.is_closed())
    }

    pub fn seek(&self, py: Python<'_>, position: u64) -> PyResult<()> {
        let inner = self.inner.clone();
        rt().block_on(Some(py), inner.seek(position))?.infer_error()
    }

    pub fn tell(&self, py: Python<'_>) -> PyResult<u64> {
        let inner = self.inner.clone();
        rt().block_on(Some(py), inner.tell())?.infer_error()
    }

    pub fn size(&self) -> u64 {
        self.inner.size()
    }

    #[pyo3(signature = (expires_in_seconds=None))]
    pub fn location<'py>(
        &self,
        py: Python<'py>,
        expires_in_seconds: Option<u64>,
    ) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let inner = self.inner.clone();
        let expires = expires_in_seconds.map(Duration::from_secs);
        let (location_uri, url_opt, (offset, length)) = rt()
            .block_on(Some(py), inner.location(expires))?
            .infer_error()?;

        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("location_uri", location_uri)?;

        if let Some(url) = url_opt {
            dict.set_item("url", url)?;
        }

        let headers = pyo3::types::PyDict::new(py);
        headers.set_item("Range", format!("bytes={}-{}", offset, offset + length - 1))?;
        dict.set_item("headers", headers)?;

        Ok(dict)
    }

    pub fn readall<'a>(&'a self, py: Python<'a>) -> PyResult<Bound<'a, PyBytes>> {
        let inner = self.inner.clone();
        let data = rt().block_on(Some(py), inner.read())?.infer_error()?;
        Ok(PyBytes::new(py, &data))
    }

    pub fn read_into(&self, dst: Bound<'_, PyByteArray>) -> PyResult<usize> {
        let inner = self.inner.clone();

        let data = rt()
            .block_on(Some(dst.py()), inner.read_up_to(dst.len()))?
            .infer_error()?;

        // Need to re-check length because the buffer could have been resized
        // by Python code while we were reading.
        if dst.len() < data.len() {
            Err(PyValueError::new_err("Buffer too small"))
        } else {
            // Safety: We've checked the buffer size above.  We've held the
            // GIL since then and so no other Python code could have modified
            // the buffer.
            unsafe {
                dst.as_bytes_mut()[0..data.len()].copy_from_slice(&data);
            }
            Ok(data.len())
        }
    }
}

impl From<InnerBlobFile> for LanceBlobFile {
    fn from(inner: InnerBlobFile) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}
