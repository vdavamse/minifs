"""Type stubs for the Rust extension built from crates/minifs-regex."""

__version__: str
HAS_ORACLE: bool

# Only present when the crate was built with the `oracle` feature (development builds).
def oracle_is_match(pattern: str, haystack: str) -> bool: ...
