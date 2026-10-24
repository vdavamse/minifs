//! `minifs._regex`: translates Rust-style regexes (as `rg` accepts them) into PostgreSQL
//! ARE. The translator itself lands with #9; this module is the build scaffolding.

use pyo3::prelude::*;

/// Version of the Rust crate, so Python can check it imported the extension it expects.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Whether the `oracle` feature (the Rust `regex` crate) was compiled in.
pub const HAS_ORACLE: bool = cfg!(feature = "oracle");

/// Reports whether `pattern` matches anywhere in `haystack`, using the Rust `regex` crate.
/// Differential tests compare the translated ARE against this.
#[cfg(feature = "oracle")]
pub fn oracle_is_match(pattern: &str, haystack: &str) -> Result<bool, regex::Error> {
    Ok(regex::Regex::new(pattern)?.is_match(haystack))
}

#[cfg(feature = "oracle")]
#[pyfunction(name = "oracle_is_match")]
fn py_oracle_is_match(pattern: &str, haystack: &str) -> PyResult<bool> {
    oracle_is_match(pattern, haystack)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

#[pymodule]
fn _regex(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add("HAS_ORACLE", HAS_ORACLE)?;
    #[cfg(feature = "oracle")]
    m.add_function(wrap_pyfunction!(py_oracle_is_match, m)?)?;
    Ok(())
}

#[cfg(all(test, feature = "oracle"))]
mod tests {
    use super::*;

    #[test]
    fn oracle_matches_and_rejects() {
        assert!(oracle_is_match(r"fn \w+\(", "pub fn main() {").unwrap());
        assert!(!oracle_is_match(r"^\d+$", "12a").unwrap());
        assert!(oracle_is_match(r"(", "").is_err());
    }
}
