//! `minifs._regex`: translates Rust-style regexes (as `rg` accepts them) into PostgreSQL
//! ARE. The emitter is in [`translate`]; this module is the Python binding.

use pyo3::prelude::*;

#[cfg(feature = "oracle")]
pub mod oracle;
pub mod translate;

/// Version of the Rust crate, so Python can check it imported the extension it expects.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Whether the `oracle` feature (the Rust `regex` crate) was compiled in.
pub const HAS_ORACLE: bool = cfg!(feature = "oracle");

pyo3::create_exception!(
    _regex,
    RegexError,
    pyo3::exceptions::PyValueError,
    "A pattern the translator rejects. `message` is the text to show (ripgrep's format for \
     parse errors), `span` the character offsets of the offending part when known, and \
     `syntax` whether regex-syntax (and so ripgrep) rejected it."
);

fn regex_error(py: Python<'_>, err: translate::Error) -> PyErr {
    let pyerr = RegexError::new_err(err.message.clone());
    let value = pyerr.value(py);
    let set = || -> PyResult<()> {
        value.setattr("message", &err.message)?;
        value.setattr("span", err.span)?;
        value.setattr("syntax", err.syntax)
    };
    match set() {
        Ok(()) => pyerr,
        Err(e) => e,
    }
}

/// Translates `pattern` into ARE, returning the line form. The file form is
/// `FILE_FORM_PREFIX` plus the line form.
#[pyfunction(name = "translate")]
#[pyo3(signature = (pattern, ignore_case = false))]
fn py_translate(py: Python<'_>, pattern: &str, ignore_case: bool) -> PyResult<String> {
    translate::translate(pattern, ignore_case)
        .map(|t| t.line_form)
        .map_err(|e| regex_error(py, e))
}

/// Escapes regex metacharacters, so the result matches `text` literally.
#[pyfunction(name = "escape")]
fn py_escape(text: &str) -> String {
    translate::escape(text)
}

#[cfg(feature = "oracle")]
#[pyfunction(name = "oracle_is_match")]
#[pyo3(signature = (pattern, haystack, ignore_case = false))]
fn py_oracle_is_match(pattern: &str, haystack: &str, ignore_case: bool) -> PyResult<bool> {
    oracle::oracle_is_match(pattern, haystack, ignore_case)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

#[cfg(feature = "oracle")]
#[pyfunction(name = "oracle_random_pattern")]
fn py_oracle_random_pattern(seed: u64) -> String {
    oracle::random_pattern(seed)
}

#[pymodule]
fn _regex(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add("HAS_ORACLE", HAS_ORACLE)?;
    m.add("FILE_FORM_PREFIX", translate::FILE_FORM_PREFIX)?;
    m.add("RegexError", m.py().get_type::<RegexError>())?;
    m.add_function(wrap_pyfunction!(py_translate, m)?)?;
    m.add_function(wrap_pyfunction!(py_escape, m)?)?;
    #[cfg(feature = "oracle")]
    {
        m.add("ORACLE_CHARS", oracle::CHARS)?;
        m.add_function(wrap_pyfunction!(py_oracle_is_match, m)?)?;
        m.add_function(wrap_pyfunction!(py_oracle_random_pattern, m)?)?;
    }
    Ok(())
}
