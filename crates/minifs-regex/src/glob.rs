//! Glob patterns, as ripgrep's `-g` takes them, translated into PostgreSQL ARE over paths
//! relative to the search root, plus the hints that let `vfs_glob` walk less of the tree.
//!
//! A pattern goes through ripgrep's own pipeline: the gitignore rules of the `ignore` crate
//! (a pattern without `/` matches at any depth, a leading `/` anchors, a trailing `/` means
//! directories only), then `globset`, which compiles it into a Rust regex. globset's regexes
//! match bytes (`(?-u)`); Postgres matches characters, so the regex is rewritten to Unicode
//! semantics (`?` and classes match one character, not one byte) and then emitted as ARE by
//! [`crate::translate`]. Classes never match `/` either, so they stay within one segment.

use globset::GlobBuilder;
use regex_syntax::ParserBuilder;
use regex_syntax::hir::literal::{ExtractKind, Extractor};
use regex_syntax::hir::{Hir, HirKind};

use crate::translate::{self, Error, Subject};

/// Deepest brace nesting accepted. globset turns braces into a regex recursively, so an
/// unbounded depth could overflow the stack of a thread with a small one (512 KB on macOS).
pub const MAX_BRACE_DEPTH: usize = 64;

/// Why a pattern ending in `/` can't list files.
pub const ONLY_DIR: &str =
    "a pattern ending in '/' matches only directories, and only files are listed";

/// Largest glob, in characters, classes and anchors of its regex. `vfs_glob` matches the regex
/// against every path it walks, and a real glob has a few dozen; this still admits a literal
/// path of a few thousand characters.
pub const MAX_ATOMS: u64 = 4096;

/// What `vfs_glob` needs for one pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobPlan {
    /// Anchored ARE over the path relative to the search root.
    pub regex: String,
    /// Literal leading directories: only that subtree can match.
    pub prefix: Vec<String>,
    /// The deepest level below the prefix that can match; `None` when unbounded (`**`).
    pub max_depth: Option<u32>,
    /// A LIKE pattern every matching file name satisfies; `None` when it would filter nothing.
    pub name_like: Option<String>,
    /// The pattern ended in `/`: it matches directories only.
    pub only_dir: bool,
}

fn error(message: impl Into<String>, syntax: bool) -> Error {
    Error {
        message: message.into(),
        span: None,
        syntax,
    }
}

/// Compiles `pattern` as `rg -g` does. Errors carry globset's message (as rg prints it after
/// `error parsing glob '<pattern>': `), with `syntax` set, or a reason of our own. An include
/// with a leading `!` or `#` is refused, since rg would read a negation or a comment. An
/// `exclude` is the part after rg's `-g '!'`, where both are ordinary characters.
pub fn compile(pattern: &str, exclude: bool) -> Result<GlobPlan, Error> {
    if pattern.contains('\0') {
        return Err(error("NUL character", false));
    }
    if pattern.len() > translate::MAX_PATTERN_LEN {
        return Err(error(
            format!(
                "glob is too long ({} KB, the limit is {} KB)",
                pattern.len().div_ceil(1024),
                translate::MAX_PATTERN_LEN / 1024
            ),
            false,
        ));
    }
    let (actual, only_dir) = gitignore_glob(pattern, exclude)?;
    if brace_depth(&actual) > MAX_BRACE_DEPTH {
        return Err(error("braces nested too deeply", false));
    }
    let glob = GlobBuilder::new(&actual)
        .literal_separator(true)
        .backslash_escape(true)
        .allow_unclosed_class(false)
        .build()
        .map_err(|e| error(e.kind().to_string(), true))?;
    let hir = ParserBuilder::new()
        .dot_matches_new_line(true)
        .build()
        .parse(&unicode_regex(glob.regex()))
        // globset writes valid regexes; never echo one, it can be as long as the pattern.
        .map_err(|_| error("internal error: globset's regex doesn't parse", false))?;
    let atoms = translate::atoms(&hir, false);
    if atoms > MAX_ATOMS {
        return Err(error(
            format!(
                "glob is too large ({atoms} characters and classes once translated, the limit \
                 is {MAX_ATOMS}); use a shorter pattern"
            ),
            false,
        ));
    }
    let regex = translate::translate_hir(&hir, Subject::Path)?;
    let prefix = literal_prefix(&hir);
    let max_depth = max_slashes(&hir)
        .and_then(|n| u32::try_from(n.checked_add(1)?).ok())
        .map(|levels| levels.saturating_sub(prefix.len() as u32));
    Ok(GlobPlan {
        regex,
        prefix,
        max_depth,
        name_like: name_like(&hir),
        only_dir,
    })
}

/// The deepest brace nesting of a glob, scanned as globset's parser reads it: a backslash
/// escapes the next character and braces inside a class are literal. An unclosed class ends
/// the scan; globset rejects it.
fn brace_depth(glob: &str) -> usize {
    let (mut depth, mut max) = (0usize, 0usize);
    let mut chars = glob.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                chars.next();
            }
            '[' => {
                // `[!` / `[^` negate, and a `]` right after the opening is literal.
                if matches!(chars.peek(), Some('!' | '^')) {
                    chars.next();
                }
                if chars.peek() == Some(&']') {
                    chars.next();
                }
                if !chars.by_ref().any(|c| c == ']') {
                    break;
                }
            }
            '{' => {
                depth += 1;
                max = max.max(depth);
            }
            '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    max
}

/// The glob globset gets, and whether it is for directories only: what
/// `ignore::gitignore::GitignoreBuilder::add_line` does to each `-g` pattern (for an exclude,
/// to what follows its `!`).
fn gitignore_glob(pattern: &str, exclude: bool) -> Result<(String, bool), Error> {
    // In an include, rg would take these as a comment (ignoring the glob) and as a negation.
    if !exclude && pattern.starts_with('#') {
        return Err(error(
            "a leading '#' makes the glob a comment, which rg ignores; escape it as '\\#'",
            false,
        ));
    }
    if !exclude && pattern.starts_with('!') {
        return Err(error(
            "a leading '!' negates the glob in rg; pass it as an exclude, or escape it as '\\!'",
            false,
        ));
    }
    // Trailing whitespace is dropped unless escaped.
    let mut line = if pattern.ends_with("\\ ") {
        pattern
    } else {
        pattern.trim_end()
    };
    let mut absolute = false;
    if !exclude && (line.starts_with("\\!") || line.starts_with("\\#")) {
        line = &line[1..];
    } else if let Some(rest) = line.strip_prefix('/') {
        // A leading '/' only anchors.
        line = rest;
        absolute = true;
    }
    let only_dir = line.ends_with('/');
    if only_dir {
        line = &line[..line.len() - 1];
        // An escaped trailing '/' is one too (ripgrep#2236).
        if line.ends_with('\\') {
            line = &line[..line.len() - 1];
        }
    }
    if line.is_empty() {
        return Err(error("empty pattern", false));
    }
    let mut actual = line.to_string();
    // Without a '/', the pattern matches the name at any depth.
    if !absolute && !line.contains('/') && !(actual.starts_with("**/") || actual == "**") {
        actual = format!("**/{actual}");
    }
    // `dir/**` matches what is inside dir, not dir itself.
    if actual.ends_with("/**") {
        actual.push_str("/*");
    }
    Ok((actual, only_dir))
}

/// globset's byte regex with Unicode semantics. globset writes every non-ASCII character as
/// its UTF-8 bytes (`é` is `\xc3\xa9`), in literals and in class ranges alike, so they are
/// decoded back into characters: `[é-ö]` is then a range of characters, not of bytes, and
/// `?` (`[^/]`) matches one character. Every class also loses `/`, which globset leaves in
/// negated classes and ranges such as `[+-0]`.
fn unicode_regex(regex: &str) -> String {
    let regex = regex.strip_prefix("(?-u)").unwrap_or(regex);
    let mut out = String::with_capacity(regex.len());
    let mut chars = regex.chars().peekable();
    let mut bytes = Vec::new();
    // Some(negated) inside a class. globset escapes every '[', ']' and '^' that is a literal,
    // so a bare one is syntax.
    let mut class: Option<bool> = None;
    while let Some(c) = chars.next() {
        if c == '\\' {
            let escaped = chars.next().expect("globset never ends a regex with '\\'");
            if escaped == 'x' {
                let hex: String = chars.by_ref().take(2).collect();
                bytes.push(u8::from_str_radix(&hex, 16).expect("globset writes \\xHH"));
                if chars.peek() == Some(&'\\') {
                    let mut ahead = chars.clone();
                    ahead.next();
                    if ahead.peek() == Some(&'x') {
                        continue;
                    }
                }
                // The bytes of whole characters, so always valid UTF-8.
                out.push_str(&String::from_utf8_lossy(&bytes));
                bytes.clear();
            } else {
                out.push('\\');
                out.push(escaped);
            }
            continue;
        }
        match (c, class) {
            ('[', None) => {
                out.push('[');
                let negated = chars.peek() == Some(&'^');
                if negated {
                    out.push(chars.next().expect("peeked"));
                }
                class = Some(negated);
            }
            (']', Some(negated)) => {
                out.push_str(if negated { "/]" } else { "&&[^/]]" });
                class = None;
            }
            _ => out.push(c),
        }
    }
    out
}

/// The literal leading directories every match starts with.
fn literal_prefix(hir: &Hir) -> Vec<String> {
    let seq = Extractor::new().kind(ExtractKind::Prefix).extract(hir);
    let Some(lcp) = seq.longest_common_prefix() else {
        return Vec::new();
    };
    // Up to the last '/', which is ASCII, so a character boundary.
    let Some(end) = lcp.iter().rposition(|&b| b == b'/') else {
        return Vec::new();
    };
    let dirs: Vec<String> = String::from_utf8_lossy(&lcp[..end])
        .split('/')
        .map(str::to_string)
        .collect();
    if dirs.iter().any(|d| d.is_empty() || d == "." || d == "..") {
        return Vec::new(); // `a//b`, `./a` and `../a` match nothing anyway
    }
    dirs
}

/// The most `/` a match of `hir` can contain; `None` when unbounded.
fn max_slashes(hir: &Hir) -> Option<u64> {
    match hir.kind() {
        HirKind::Empty | HirKind::Look(_) => Some(0),
        HirKind::Literal(lit) => Some(lit.0.iter().filter(|&&b| b == b'/').count() as u64),
        HirKind::Class(class) => Some(u64::from(class_has_slash(class))),
        HirKind::Repetition(rep) => match (max_slashes(&rep.sub)?, rep.max) {
            (0, _) => Some(0),
            (_, None) => None,
            (n, Some(max)) => Some(n.saturating_mul(u64::from(max))),
        },
        HirKind::Capture(cap) => max_slashes(&cap.sub),
        HirKind::Concat(subs) => subs
            .iter()
            .try_fold(0u64, |sum, sub| Some(sum.saturating_add(max_slashes(sub)?))),
        HirKind::Alternation(subs) => subs
            .iter()
            .try_fold(0u64, |max, sub| Some(max.max(max_slashes(sub)?))),
    }
}

fn class_has_slash(class: &regex_syntax::hir::Class) -> bool {
    match class {
        regex_syntax::hir::Class::Unicode(c) => c
            .ranges()
            .iter()
            .any(|r| r.start() <= '/' && '/' <= r.end()),
        regex_syntax::hir::Class::Bytes(c) => c
            .ranges()
            .iter()
            .any(|r| r.start() <= b'/' && b'/' <= r.end()),
    }
}

/// A LIKE pattern for the file name: the part of the regex after the last place a `/` can
/// match, with a class as `_` and anything repeated or alternative as `%`.
fn name_like(hir: &Hir) -> Option<String> {
    let items: &[Hir] = match hir.kind() {
        HirKind::Concat(subs) => subs,
        _ => std::slice::from_ref(hir),
    };
    let mut like = Like::default();
    // The items with no '/' at the end are all in the name.
    let split = items
        .iter()
        .rposition(|item| max_slashes(item) != Some(0))
        .map_or(0, |i| i + 1);
    match items[..split].last().map(Hir::kind) {
        // The name starts right after a literal's last '/'.
        Some(HirKind::Literal(lit)) => {
            let after = lit.0.iter().rposition(|&b| b == b'/').map_or(0, |i| i + 1);
            like.literal(&String::from_utf8_lossy(&lit.0[after..]));
        }
        // It starts after a '/' or at the start of the path, unless what comes before can
        // end inside the name.
        _ if !empty_or_ends_in_slash(&items[..split]) => like.any(),
        _ => {}
    }
    for item in &items[split..] {
        like.push(item);
    }
    (like.0 != "%").then_some(like.0)
}

/// Whether every match of the concatenation of `items` is empty or ends in `/`.
fn empty_or_ends_in_slash(items: &[Hir]) -> bool {
    for item in items.iter().rev() {
        let ok = match item.kind() {
            HirKind::Empty | HirKind::Look(_) => true,
            HirKind::Literal(lit) => lit.0.last() == Some(&b'/'),
            HirKind::Class(_) => false,
            HirKind::Repetition(rep) => empty_or_ends_in_slash(std::slice::from_ref(&rep.sub)),
            HirKind::Capture(cap) => empty_or_ends_in_slash(std::slice::from_ref(&cap.sub)),
            HirKind::Alternation(subs) => subs
                .iter()
                .all(|sub| empty_or_ends_in_slash(std::slice::from_ref(sub))),
            HirKind::Concat(subs) => empty_or_ends_in_slash(subs),
        };
        if !ok {
            return false;
        }
        if item.properties().minimum_len() != Some(0) {
            return true;
        }
    }
    true
}

#[derive(Default)]
struct Like(String);

impl Like {
    fn any(&mut self) {
        // `\%` is a literal percent sign, not a wildcard to merge with.
        let wildcard = self.0.ends_with('%') && !self.0.ends_with("\\%");
        if !wildcard {
            self.0.push('%');
        }
    }

    fn literal(&mut self, text: &str) {
        for c in text.chars() {
            if matches!(c, '\\' | '%' | '_') {
                self.0.push('\\');
            }
            self.0.push(c);
        }
    }

    fn push(&mut self, hir: &Hir) {
        match hir.kind() {
            HirKind::Empty | HirKind::Look(_) => {}
            HirKind::Literal(lit) => self.literal(&String::from_utf8_lossy(&lit.0)),
            HirKind::Class(_) => self.0.push('_'),
            HirKind::Repetition(rep) if rep.max == Some(0) => {}
            HirKind::Concat(subs) => subs.iter().for_each(|sub| self.push(sub)),
            HirKind::Repetition(_) | HirKind::Capture(_) | HirKind::Alternation(_) => self.any(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn regex(pattern: &str) -> String {
        compile(pattern, false).unwrap().regex
    }

    fn rejected(pattern: &str) -> Error {
        compile(pattern, false).unwrap_err()
    }

    fn plan(
        regex: &str,
        prefix: &[&str],
        max_depth: Option<u32>,
        name_like: Option<&str>,
    ) -> GlobPlan {
        GlobPlan {
            regex: regex.to_string(),
            prefix: prefix.iter().map(|s| s.to_string()).collect(),
            max_depth,
            name_like: name_like.map(str::to_string),
            only_dir: false,
        }
    }

    #[test]
    fn byte_regexes_become_unicode() {
        assert_eq!(unicode_regex(r"(?-u)^\xc3\xa9$"), "^é$");
        assert_eq!(
            unicode_regex(r"(?-u)^[\xc3\xa9-\xc3\xb6]\\x$"),
            r"^[é-ö&&[^/]]\\x$"
        );
        assert_eq!(unicode_regex(r"(?-u)^[^a][^/]*$"), "^[^a/][^//]*$");
        assert_eq!(
            unicode_regex(r"(?-u)^[\[\]\^]\xf0\x9f\x98\x80$"),
            r"^[\[\]\^&&[^/]]😀$"
        );
    }

    #[test]
    fn translation() {
        let any = "[\\u0001-\\u002E0-\u{10FFFF}]";
        assert_eq!(
            regex("*.md"),
            format!("^(?:/?|[\\u0001-\u{10FFFF}]*/){any}*\\.md$")
        );
        assert_eq!(regex("src/*.py"), format!("^src/{any}*\\.py$"));
        assert_eq!(regex("/src/*.py"), format!("^src/{any}*\\.py$"));
        assert_eq!(regex("/a"), "^a$");
        assert_eq!(
            regex("é?"),
            format!("^(?:/?|[\\u0001-\u{10FFFF}]*/)é{any}$")
        );
        // Classes never match '/', and ranges are characters.
        assert_eq!(regex("/[!a]"), "^[\\u0001-\\u002E0-\\u0060b-\u{10FFFF}]$");
        assert_eq!(regex("/[.-0]"), "^[\\u002E0]$");
        assert_eq!(regex("/[é-ö]"), "^[é-ö]$");
        assert_eq!(regex("/[/]"), "^(?:(?!))$");
        // Names may contain newlines.
        assert_eq!(regex("/a\nb"), "^a\\u000Ab$");
        assert_eq!(regex("/{a,b}c"), "^[ab]c$");
        assert_eq!(regex("/{a,b}{c,dd}"), "^[ab](?:c|dd)$");
        assert_eq!(regex("/a{b,{c,d}e}"), "^a(?:b|[cd]e)$");
        // Empty alternatives are dropped, as globset does by default.
        assert_eq!(regex("/a{b,}"), "^ab$");
        assert_eq!(regex("**"), "^[\\u0001-\u{10FFFF}]*$");
    }

    #[test]
    fn gitignore_rules() {
        assert_eq!(
            gitignore_glob("*.md", false).unwrap(),
            ("**/*.md".into(), false)
        );
        assert_eq!(
            gitignore_glob("**/x", false).unwrap(),
            ("**/x".into(), false)
        );
        assert_eq!(gitignore_glob("**", false).unwrap(), ("**".into(), false));
        assert_eq!(gitignore_glob("/x", false).unwrap(), ("x".into(), false));
        assert_eq!(gitignore_glob("a/b", false).unwrap(), ("a/b".into(), false));
        assert_eq!(
            gitignore_glob("src/**", false).unwrap(),
            ("src/**/*".into(), false)
        );
        assert_eq!(
            gitignore_glob("build/", false).unwrap(),
            ("**/build".into(), true)
        );
        assert_eq!(
            gitignore_glob("build\\/", false).unwrap(),
            ("**/build".into(), true)
        );
        assert_eq!(gitignore_glob("a/b/", false).unwrap(), ("a/b".into(), true));
        assert_eq!(
            gitignore_glob("x.md  ", false).unwrap(),
            ("**/x.md".into(), false)
        );
        assert_eq!(
            gitignore_glob("x\\ ", false).unwrap(),
            ("**/x\\ ".into(), false)
        );
        assert_eq!(
            gitignore_glob("\\!x", false).unwrap(),
            ("**/!x".into(), false)
        );
        assert_eq!(
            gitignore_glob("\\#x", false).unwrap(),
            ("**/#x".into(), false)
        );
    }

    #[test]
    fn errors_are_globsets() {
        let err = rejected("[ab");
        assert!(err.syntax);
        assert_eq!(err.message, "unclosed character class; missing ']'");
        assert_eq!(
            rejected("{a,b").message,
            "unclosed alternate group; missing '}' (maybe escape '{' with '[{]'?)"
        );
        assert_eq!(
            rejected("a}").message,
            "unopened alternate group; missing '{' (maybe escape '}' with '[}]'?)"
        );
        assert_eq!(rejected("[z-a]").message, "invalid range; 'z' > 'a'");
        assert_eq!(rejected("a\\").message, "dangling '\\'");
        for (pattern, message) in [
            ("", "empty pattern"),
            ("   ", "empty pattern"),
            ("/", "empty pattern"),
            ("a\0", "NUL character"),
            ("!a", "negates"),
            ("#a", "comment"),
        ] {
            let err = rejected(pattern);
            assert!(
                !err.syntax && err.message.contains(message),
                "{pattern:?}: {err}"
            );
        }
        assert!(compile("src/", false).unwrap().only_dir);
        // Refused before globset recurses: this used to overflow a 512 KB stack.
        let deep = format!("{}a{}", "{".repeat(5000), "}".repeat(5000));
        let message = std::thread::Builder::new()
            .stack_size(512 * 1024)
            .spawn(move || compile(&deep, false).unwrap_err().message)
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(message, "braces nested too deeply");
        let ok = format!(
            "{}a{}",
            "{".repeat(MAX_BRACE_DEPTH),
            "}".repeat(MAX_BRACE_DEPTH)
        );
        assert!(compile(&ok, false).is_ok());
        // Braces in classes and escaped ones don't count.
        assert_eq!(brace_depth(r"{a,[{{]\{{b}}"), 2);
        assert_eq!(brace_depth("[]{]{"), 1);
        assert!(
            rejected(&"?".repeat(5000))
                .message
                .starts_with("glob is too large (")
        );
        // In an exclude (what follows rg's `-g '!'`), a leading '!' or '#' is a character.
        assert_eq!(gitignore_glob("#x", true).unwrap(), ("**/#x".into(), false));
        assert_eq!(gitignore_glob("!x", true).unwrap(), ("**/!x".into(), false));
        assert_eq!(
            gitignore_glob("\\!x", true).unwrap(),
            ("**/\\!x".into(), false)
        );
    }

    #[test]
    fn hints() {
        let p = |pattern: &str| {
            let mut plan = compile(pattern, false).unwrap();
            plan.regex.clear();
            plan
        };
        assert_eq!(p("*.md"), plan("", &[], None, Some("%.md")));
        assert_eq!(p("README.md"), plan("", &[], None, Some("README.md")));
        assert_eq!(
            p("docs/guides/**/*.md"),
            plan("", &["docs", "guides"], None, Some("%.md"))
        );
        assert_eq!(p("src/*/*.py"), plan("", &["src"], Some(2), Some("%.py")));
        assert_eq!(p("src/a.py"), plan("", &["src"], Some(1), Some("a.py")));
        assert_eq!(p("src/**"), plan("", &["src"], None, None));
        assert_eq!(p("src/*.[ch]"), plan("", &["src"], Some(1), Some("%._")));
        assert_eq!(p("{a,b}"), plan("", &[], None, Some("_")));
        assert_eq!(p("{ab,c}"), plan("", &[], None, None));
        assert_eq!(
            p("{src,lib}/x_1%.py"),
            plan("", &[], Some(2), Some(r"x\_1\%.py"))
        );
        assert_eq!(
            p("{src/a,src/b/c}/*.py"),
            plan("", &["src"], Some(3), Some("%.py"))
        );
        assert_eq!(p("{src/a,src/b}/c"), plan("", &["src"], Some(2), Some("c")));
        assert_eq!(
            p("src/*.{md,txt}"),
            plan("", &["src"], Some(1), Some("%.%"))
        );
        assert_eq!(p("a?b**c"), plan("", &[], None, Some("a_b%c")));
        assert_eq!(p("**"), plan("", &[], None, None));
        assert_eq!(p("a/b{c,/d}"), plan("", &["a"], Some(2), None));
        assert_eq!(p("a/{b,c/}d"), plan("", &["a"], Some(2), Some("%d")));
        // `**` at the start of an alternative is a `*`.
        assert_eq!(p("x{**,y}/z"), plan("", &[], Some(2), Some("z")));
        assert_eq!(p("a/é?"), plan("", &["a"], Some(1), Some("é_")));
        assert_eq!(p("a/%\\*"), plan("", &["a"], Some(1), Some("\\%*")));
        assert_eq!(p("a/%*"), plan("", &["a"], Some(1), Some("\\%%")));
    }
}
