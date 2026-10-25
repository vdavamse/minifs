//! `minifs._regex`: translates Rust-style regexes and globs (as `rg` accepts them) into
//! PostgreSQL ARE. The emitter is in [`translate`], globs in [`glob`]; this module is the
//! Python binding.

use pyo3::prelude::*;

pub mod glob;
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
     parse errors), `span` the character offsets of the offending part when known, \
     `syntax` whether regex-syntax (and so ripgrep) rejected it, and `construct` a label of what \
     was rejected, for metrics."
);

fn regex_error(py: Python<'_>, err: translate::Error) -> PyErr {
    let pyerr = RegexError::new_err(err.message.clone());
    let value = pyerr.value(py);
    let set = || -> PyResult<()> {
        value.setattr("message", &err.message)?;
        value.setattr("span", err.span)?;
        value.setattr("syntax", err.syntax)?;
        value.setattr("construct", err.construct)
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

/// `(regex, prefix, max_depth, name_like, only_dir)`: a [`glob::GlobPlan`] for Python.
type PyGlobPlan = (String, Vec<String>, Option<u32>, Option<String>, bool);

/// The regex and pruning hints for a glob; with `exclude`, for what follows rg's `-g '!'`.
#[pyfunction(name = "compile_glob")]
#[pyo3(signature = (pattern, exclude = false))]
fn py_compile_glob(py: Python<'_>, pattern: &str, exclude: bool) -> PyResult<PyGlobPlan> {
    let plan = glob::compile(pattern, exclude).map_err(|e| regex_error(py, e))?;
    Ok((
        plan.regex,
        plan.prefix,
        plan.max_depth,
        plan.name_like,
        plan.only_dir,
    ))
}

/// Translates a glob, as `rg -g` takes it, into an anchored ARE over relative file paths. A
/// pattern ending in `/` matches only directories, so it is rejected.
#[pyfunction(name = "glob_to_are")]
fn py_glob_to_are(py: Python<'_>, pattern: &str) -> PyResult<String> {
    let plan = glob::compile(pattern, false).map_err(|e| regex_error(py, e))?;
    if plan.only_dir {
        return Err(regex_error(
            py,
            translate::Error {
                message: glob::ONLY_DIR.to_string(),
                span: None,
                syntax: false,
                construct: "glob",
            },
        ));
    }
    Ok(plan.regex)
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
#[pyfunction(name = "oracle_glob_is_match")]
#[pyo3(signature = (pattern, path, is_dir = false, exclude = false))]
fn py_oracle_glob_is_match(
    pattern: &str,
    path: &str,
    is_dir: bool,
    exclude: bool,
) -> PyResult<bool> {
    oracle::oracle_glob_is_match(pattern, path, is_dir, exclude)
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
    m.add_function(wrap_pyfunction!(py_compile_glob, m)?)?;
    m.add_function(wrap_pyfunction!(py_glob_to_are, m)?)?;
    #[cfg(feature = "oracle")]
    {
        m.add("ORACLE_CHARS", oracle::CHARS)?;
        m.add_function(wrap_pyfunction!(py_oracle_is_match, m)?)?;
        m.add_function(wrap_pyfunction!(py_oracle_random_pattern, m)?)?;
        m.add_function(wrap_pyfunction!(py_oracle_glob_is_match, m)?)?;
    }
    Ok(())
}
