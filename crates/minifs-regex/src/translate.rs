//! Translation of Rust regexes (ripgrep's dialect) into PostgreSQL ARE.
//!
//! The pattern is parsed with regex-syntax, configured as ripgrep configures it, and ARE is
//! emitted from the HIR, one function per node kind. The emitted pattern never uses ARE's
//! character classes, word boundaries or case folding, which follow the database's ctype:
//! every class is spelled out as code-point ranges, so the result means the same in every
//! locale.

use std::fmt::Write as _;
use std::sync::OnceLock;

use regex_syntax::ParserBuilder;
use regex_syntax::hir::{Class, ClassUnicode, ClassUnicodeRange, Hir, HirKind, Look, Repetition};

/// Largest repetition count ARE accepts (`DUPMAX`).
pub const MAX_REPEAT: u32 = 255;

/// Largest translated pattern, in bytes. Keeps compilation in Postgres bounded: one Unicode
/// `\w` is about 5.6 KB, and a `\b` holds four of them.
pub const MAX_TRANSLATED_LEN: usize = 256 * 1024;

/// Largest number of atoms that expanding counted repetitions may add. ARE copies the operand
/// of `{n,m}` up to `m` times, so nested counts multiply: `(?:a{255}){255}` adds 65,024 and is
/// "too complex" for Postgres, while `(?:[a-z]{0,255}){32}` (8,159) compiles in about 0.15 s.
/// Plain length isn't counted here; [`MAX_TRANSLATED_LEN`] bounds it.
pub const MAX_EXTRA_ATOMS: u64 = 8192;

/// Largest number of atoms before expansion. Bounds wide patterns: a 12,000-way alternation of
/// short words (about 70,000 atoms) compiles in 0.08 s, but 16,000 ways makes Postgres fail an
/// allocation.
pub const MAX_ATOMS: u64 = 50_000;

/// ARE's newline-sensitive mode: `^` and `$` also match at line breaks.
pub const FILE_FORM_PREFIX: &str = "(?n)";

/// Never matches: a negative lookahead for the empty string. Used for empty classes.
const NEVER: &str = "(?:(?!))";

/// The ARE translation of one pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Translation {
    /// For testing one line at a time (`line ~ line_form`).
    pub line_form: String,
}

impl Translation {
    /// [`FILE_FORM_PREFIX`] plus the line form, for testing a whole file at once. Matches
    /// every file in which some line matches the line form.
    pub fn file_form(&self) -> String {
        format!("{FILE_FORM_PREFIX}{}", self.line_form)
    }
}

/// Why a pattern was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    /// For a parse error, regex-syntax's message, formatted as ripgrep prints it. Otherwise a
    /// one-line reason with a hint.
    pub message: String,
    /// Character (not byte) offsets `[start, end)` of the offending part of the pattern, when
    /// known.
    pub span: Option<(usize, usize)>,
    /// Whether regex-syntax itself rejected the pattern, so ripgrep rejects it too.
    pub syntax: bool,
}

impl Error {
    fn unsupported(message: impl Into<String>) -> Error {
        Error {
            message: message.into(),
            span: None,
            syntax: false,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

/// Parses `pattern` as ripgrep does and translates it into ARE.
pub fn translate(pattern: &str, ignore_case: bool) -> Result<Translation, Error> {
    let hir = parse(pattern, ignore_case)?;
    let linear = atoms(&hir, false);
    if linear > MAX_ATOMS {
        return Err(Error::unsupported(format!(
            "regex is too large for Postgres to compile ({linear} characters, classes and \
             anchors; the limit is {MAX_ATOMS}); use a shorter pattern"
        )));
    }
    let extra = atoms(&hir, true).saturating_sub(linear);
    if extra > MAX_EXTRA_ATOMS {
        return Err(Error::unsupported(format!(
            "counted repetitions expand to {extra} extra copies, more than Postgres can \
             compile (the limit is {MAX_EXTRA_ATOMS}); use smaller counts or + / *"
        )));
    }
    let mut line_form = String::new();
    emit(&hir, &mut line_form)?;
    Ok(Translation { line_form })
}

/// The parser configuration of ripgrep without `-U`, `-P` or `--crlf`: Unicode on, `^`/`$`
/// match at line ends, and patterns that can match invalid UTF-8 accepted (they're rejected
/// later, when emitted, if they really need non-ASCII bytes).
fn parser(ignore_case: bool) -> regex_syntax::Parser {
    ParserBuilder::new()
        .unicode(true)
        .utf8(false)
        .multi_line(true)
        .line_terminator(b'\n')
        .octal(false)
        .case_insensitive(ignore_case)
        .build()
}

/// What ripgrep wraps each pattern in before parsing it, so `a)(b` parses and a `(?x)` comment
/// can't swallow the end of the pattern.
const RG_WRAP: (&str, &str) = ("(?:", ")");

fn parse(pattern: &str, ignore_case: bool) -> Result<Hir, Error> {
    let wrapped = format!("{}{pattern}{}", RG_WRAP.0, RG_WRAP.1);
    let pattern_chars = pattern.chars().count();
    // Spans count characters of the pattern as given, not of the wrapper.
    let unwrap = |byte_offset| {
        char_offset(&wrapped, byte_offset)
            .saturating_sub(RG_WRAP.0.len())
            .min(pattern_chars)
    };
    parser(ignore_case).parse(&wrapped).map_err(|err| {
        let span = match &err {
            regex_syntax::Error::Parse(e) => Some(e.span()),
            regex_syntax::Error::Translate(e) => Some(e.span()),
            _ => None,
        }
        .map(|s| (unwrap(s.start.offset), unwrap(s.end.offset)))
        // An error at the wrapper's own parens has no position in the pattern as given.
        .filter(|(start, end)| start < end);
        Error {
            message: err.to_string(),
            span,
            syntax: true,
        }
    })
}

fn char_offset(text: &str, byte_offset: usize) -> usize {
    text.get(..byte_offset)
        .map_or(0, |prefix| prefix.chars().count())
}

/// Atoms in the NFA Postgres builds: characters, classes and anchors, with a word boundary
/// counting as its four lookarounds. With `expand`, each counted repetition's operand counts as
/// many times as its largest count, as ARE copies it.
fn atoms(hir: &Hir, expand: bool) -> u64 {
    match hir.kind() {
        HirKind::Empty => 0,
        HirKind::Literal(lit) => String::from_utf8_lossy(&lit.0).chars().count() as u64,
        HirKind::Class(_) => 1,
        HirKind::Look(Look::Start | Look::End | Look::StartLF | Look::EndLF) => 1,
        HirKind::Look(_) => 4,
        HirKind::Repetition(rep) => {
            // Counts over MAX_REPEAT are rejected with their own message when emitted.
            let copies = if expand {
                rep.max.unwrap_or(rep.min).max(rep.min).clamp(1, MAX_REPEAT)
            } else {
                1
            };
            atoms(&rep.sub, expand).saturating_mul(u64::from(copies))
        }
        HirKind::Capture(cap) => atoms(&cap.sub, expand),
        HirKind::Concat(subs) | HirKind::Alternation(subs) => subs
            .iter()
            .map(|sub| atoms(sub, expand))
            .fold(0, u64::saturating_add),
    }
}

/// Emits `hir`, stopping as soon as the output passes [`MAX_TRANSLATED_LEN`], so a short
/// pattern of many `\b`s never allocates more than the limit plus one node.
fn emit(hir: &Hir, out: &mut String) -> Result<(), Error> {
    emit_node(hir, out)?;
    if out.len() > MAX_TRANSLATED_LEN {
        return Err(Error::unsupported(format!(
            "regex is too large once translated (over {} KB); \
             avoid repeating Unicode classes such as \\w or \\b many times",
            MAX_TRANSLATED_LEN / 1024
        )));
    }
    Ok(())
}

fn emit_node(hir: &Hir, out: &mut String) -> Result<(), Error> {
    match hir.kind() {
        HirKind::Empty => out.push_str("(?:)"),
        HirKind::Literal(lit) => emit_literal(&lit.0, out)?,
        HirKind::Class(class) => emit_class(&unicode_class(class)?, out),
        HirKind::Look(look) => emit_look(*look, out)?,
        HirKind::Repetition(rep) => emit_repetition(rep, out)?,
        HirKind::Capture(cap) => {
            out.push_str("(?:");
            emit(&cap.sub, out)?;
            out.push(')');
        }
        HirKind::Concat(subs) => {
            for sub in subs {
                emit(sub, out)?;
            }
        }
        HirKind::Alternation(subs) => {
            out.push_str("(?:");
            for (i, sub) in subs.iter().enumerate() {
                if i > 0 {
                    out.push('|');
                }
                emit(sub, out)?;
            }
            out.push(')');
        }
    }
    Ok(())
}

fn emit_literal(bytes: &[u8], out: &mut String) -> Result<(), Error> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        Error::unsupported(
            "regex matches bytes that aren't valid UTF-8, which never occur in text; \
             remove the non-ASCII escapes from the (?-u) part",
        )
    })?;
    for c in text.chars() {
        if c == '\n' {
            return Err(newline_error());
        }
        if is_are_meta(c) {
            out.push('\\');
            out.push(c);
        } else if c.is_control() {
            push_escaped(c, out);
        } else {
            out.push(c);
        }
    }
    Ok(())
}

/// Characters that are special somewhere in an ARE outside a bracket expression.
fn is_are_meta(c: char) -> bool {
    matches!(
        c,
        '\\' | '^' | '$' | '.' | '[' | ']' | '|' | '(' | ')' | '*' | '+' | '?' | '{' | '}'
    )
}

fn newline_error() -> Error {
    Error::unsupported(
        "the literal \"\\n\" is not allowed in a regex: search matches one line at a time, \
         so no line contains a newline",
    )
}

/// The class as Unicode ranges. A byte class from `(?-u)` is accepted only when it's ASCII:
/// lines are text, not bytes.
fn unicode_class(class: &Class) -> Result<ClassUnicode, Error> {
    match class {
        Class::Unicode(class) => Ok(class.clone()),
        Class::Bytes(class) => {
            if !class.is_ascii() {
                return Err(Error::unsupported(
                    "a byte class from (?-u) can match non-ASCII bytes, but search matches \
                     text; use a Unicode class instead (drop the (?-u))",
                ));
            }
            Ok(ClassUnicode::new(class.iter().map(|r| {
                ClassUnicodeRange::new(char::from(r.start()), char::from(r.end()))
            })))
        }
    }
}

/// A bracket expression listing the class's ranges. `\n` is removed as ripgrep removes it (a
/// line never contains one), and so is NUL, which Postgres text can't hold.
fn emit_class(class: &ClassUnicode, out: &mut String) {
    let mut class = class.clone();
    class.difference(&ClassUnicode::new([
        ClassUnicodeRange::new('\0', '\0'),
        ClassUnicodeRange::new('\n', '\n'),
    ]));
    if class.ranges().is_empty() {
        out.push_str(NEVER);
        return;
    }
    out.push('[');
    for range in class.ranges() {
        push_class_char(range.start(), out);
        if range.end() != range.start() {
            if u32::from(range.end()) > u32::from(range.start()) + 1 {
                out.push('-');
            }
            push_class_char(range.end(), out);
        }
    }
    out.push(']');
}

/// Inside a bracket expression, ASCII letters and digits and printable non-ASCII characters
/// stand for themselves; everything else is a code-point escape, so nothing can start a
/// collating element or end the bracket. Non-ASCII characters stay literal to keep big classes
/// such as `\w` small.
fn push_class_char(c: char, out: &mut String) {
    if c.is_ascii_alphanumeric() || (!c.is_ascii() && !c.is_control()) {
        out.push(c);
    } else {
        push_escaped(c, out);
    }
}

fn push_escaped(c: char, out: &mut String) {
    let code = u32::from(c);
    if code <= 0xFFFF {
        let _ = write!(out, "\\u{code:04X}");
    } else {
        let _ = write!(out, "\\U{code:08X}");
    }
}

fn emit_look(look: Look, out: &mut String) -> Result<(), Error> {
    // A line is the whole text, so start and end of text and of line coincide; in the file
    // form, `(?n)` makes `^` and `$` match at every line break.
    let (word, kind) = match look {
        Look::Start | Look::StartLF => {
            out.push('^');
            return Ok(());
        }
        Look::End | Look::EndLF => {
            out.push('$');
            return Ok(());
        }
        Look::StartCRLF | Look::EndCRLF => {
            return Err(Error::unsupported(
                "CRLF-aware anchors from the R flag are not supported; use ^ and $ without (?R)",
            ));
        }
        Look::WordAscii => (WordClass::Ascii, Boundary::Word),
        Look::WordAsciiNegate => {
            // ripgrep matches bytes, so this is also true between the bytes of one non-ASCII
            // character, a position ARE can't express.
            return Err(Error::unsupported(
                "the ASCII non-word-boundary (?-u:\\B) is not supported; use \\B instead",
            ));
        }
        Look::WordUnicode => (WordClass::Unicode, Boundary::Word),
        Look::WordUnicodeNegate => (WordClass::Unicode, Boundary::NotWord),
        Look::WordStartAscii => (WordClass::Ascii, Boundary::Start),
        Look::WordEndAscii => (WordClass::Ascii, Boundary::End),
        Look::WordStartUnicode => (WordClass::Unicode, Boundary::Start),
        Look::WordEndUnicode => (WordClass::Unicode, Boundary::End),
        Look::WordStartHalfAscii => (WordClass::Ascii, Boundary::StartHalf),
        Look::WordEndHalfAscii => (WordClass::Ascii, Boundary::EndHalf),
        Look::WordStartHalfUnicode => (WordClass::Unicode, Boundary::StartHalf),
        Look::WordEndHalfUnicode => (WordClass::Unicode, Boundary::EndHalf),
    };
    let w = word.bracket();
    let _ = match kind {
        Boundary::Word => write!(out, "(?:(?<={w})(?!{w})|(?<!{w})(?={w}))"),
        Boundary::NotWord => write!(out, "(?:(?<={w})(?={w})|(?<!{w})(?!{w}))"),
        Boundary::Start => write!(out, "(?<!{w})(?={w})"),
        Boundary::End => write!(out, "(?<={w})(?!{w})"),
        Boundary::StartHalf => write!(out, "(?<!{w})"),
        Boundary::EndHalf => write!(out, "(?!{w})"),
    };
    Ok(())
}

#[derive(Clone, Copy)]
enum WordClass {
    Ascii,
    Unicode,
}

#[derive(Clone, Copy)]
enum Boundary {
    Word,
    NotWord,
    Start,
    End,
    StartHalf,
    EndHalf,
}

impl WordClass {
    /// The bracket expression for `\w` (or `(?-u:\w)`), emitted once and reused.
    fn bracket(self) -> &'static str {
        static ASCII: OnceLock<String> = OnceLock::new();
        static UNICODE: OnceLock<String> = OnceLock::new();
        let (cell, pattern) = match self {
            WordClass::Ascii => (&ASCII, r"(?-u:\w)"),
            WordClass::Unicode => (&UNICODE, r"\w"),
        };
        cell.get_or_init(|| {
            let hir = parser(false).parse(pattern).expect("\\w parses");
            let HirKind::Class(class) = hir.kind() else {
                unreachable!("\\w is a class")
            };
            let mut out = String::new();
            emit_class(
                &unicode_class(class).expect("\\w is ASCII or Unicode"),
                &mut out,
            );
            out
        })
    }
}

fn emit_repetition(rep: &Repetition, out: &mut String) -> Result<(), Error> {
    for count in [Some(rep.min), rep.max].into_iter().flatten() {
        if count > MAX_REPEAT {
            return Err(Error::unsupported(format!(
                "repetition count {count} is too large: Postgres allows at most {MAX_REPEAT}; \
                 use a smaller count or + / *"
            )));
        }
    }
    // ARE quantifies only an atom: a single character, a bracket expression or a group.
    // Anything else (a longer literal, a lookaround, another repetition) is grouped first.
    let atomic = match rep.sub.kind() {
        HirKind::Literal(lit) => std::str::from_utf8(&lit.0).is_ok_and(|s| s.chars().count() == 1),
        HirKind::Class(_) | HirKind::Capture(_) | HirKind::Alternation(_) | HirKind::Empty => true,
        _ => false,
    };
    if atomic {
        emit(&rep.sub, out)?;
    } else {
        out.push_str("(?:");
        emit(&rep.sub, out)?;
        out.push(')');
    }
    match (rep.min, rep.max) {
        (0, None) => out.push('*'),
        (1, None) => out.push('+'),
        (0, Some(1)) => out.push('?'),
        (n, None) => {
            let _ = write!(out, "{{{n},}}");
        }
        (n, Some(m)) if n == m => {
            let _ = write!(out, "{{{n}}}");
        }
        (n, Some(m)) => {
            let _ = write!(out, "{{{n},{m}}}");
        }
    }
    if !rep.greedy {
        out.push('?');
    }
    Ok(())
}

/// Escapes every regex metacharacter in `text`, so the result matches `text` literally. For
/// fixed-string search with `ignore_case`.
pub fn escape(text: &str) -> String {
    regex_syntax::escape(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(pattern: &str) -> String {
        translate(pattern, false).unwrap().line_form
    }

    fn rejected(pattern: &str) -> Error {
        translate(pattern, false).unwrap_err()
    }

    #[test]
    fn literals_escape_are_metacharacters() {
        assert_eq!(line("abc"), "abc");
        assert_eq!(line(r"a\.b\(c\)\{2\}"), r"a\.b\(c\)\{2\}");
        assert_eq!(line(r"\$x\^\|\[\]\*\+\?\\"), r"\$x\^\|\[\]\*\+\?\\");
        assert_eq!(line(r"a\tb"), r"a\u0009b");
        assert_eq!(line(r"\x{1F600}é"), "😀é");
        assert_eq!(line(r"\x00"), r"\u0000");
    }

    #[test]
    fn classes_are_explicit_ranges() {
        assert_eq!(line("[a-c]"), "[a-c]");
        assert_eq!(line("[ab]"), "[ab]");
        assert_eq!(line("[abd]"), "[abd]");
        assert_eq!(line(r"[\]\-^]"), r"[\u002D\u005D\u005E]");
        // \n is removed from negated classes and from \s, as rg removes it.
        assert_eq!(line("[^a]"), "[\\u0001-\\u0009\\u000B-\\u0060b-\u{10FFFF}]");
        assert_eq!(line(r"(?-u:\s)"), r"[\u0009\u000B-\u000D\u0020]");
        assert!(line(r"\d").contains("٠-٩"));
        // Non-ASCII control characters are escaped even inside brackets.
        assert_eq!(line("[\u{85}é]"), r"[\u0085é]");
        assert_eq!(line(r"[a&&b]"), NEVER);
    }

    #[test]
    fn case_insensitivity_folds_into_classes() {
        assert_eq!(line("(?i)e"), "[Ee]");
        assert_eq!(translate("ab", true).unwrap().line_form, "[Aa][Bb]");
        // Rust's simple case folding includes U+017F LATIN SMALL LETTER LONG S.
        assert_eq!(line("(?i)s"), "[Ssſ]");
    }

    #[test]
    fn anchors() {
        assert_eq!(line(r"^a$"), "^a$");
        assert_eq!(line(r"\Aa\z"), "^a$");
        assert_eq!(line(r"(?m)^a$"), "^a$");
        assert!(rejected(r"(?R)^a").message.contains("CRLF"));
    }

    #[test]
    fn word_boundaries_are_lookarounds() {
        let w = WordClass::Ascii.bracket();
        assert_eq!(w, r"[0-9A-Z\u005Fa-z]");
        assert_eq!(
            line(r"(?-u:\b)x"),
            format!("(?:(?<={w})(?!{w})|(?<!{w})(?={w}))x")
        );
        assert!(rejected(r"(?-u:\B)x").message.contains("use \\B"));
        assert!(!rejected(r"(?-u:\B)x").syntax);
        assert_eq!(line(r"(?-u:\<)x"), format!("(?<!{w})(?={w})x"));
        assert_eq!(line(r"x(?-u:\>)"), format!("x(?<={w})(?!{w})"));
        assert_eq!(line(r"(?-u:\b{start-half})x"), format!("(?<!{w})x"));
        assert_eq!(line(r"x(?-u:\b{end-half})"), format!("x(?!{w})"));
        let u = WordClass::Unicode.bracket();
        assert!(u.len() > 3000 && u.contains("Ø-ö"));
        assert_eq!(
            line(r"\bx"),
            format!("(?:(?<={u})(?!{u})|(?<!{u})(?={u}))x")
        );
    }

    #[test]
    fn repetitions() {
        assert_eq!(line("a*b+c?"), "a*b+c?");
        assert_eq!(line("a{2}b{2,}c{2,5}"), "a{2}b{2,}c{2,5}");
        assert_eq!(line("a*?b+?c??d{2,3}?"), "a*?b+?c??d{2,3}?");
        assert_eq!(line("(ab)+"), "(?:ab)+");
        assert_eq!(line("(?:ab)+"), "(?:ab)+");
        // A lookaround or another repetition is grouped before it's quantified.
        let star = |sub: Hir| {
            let mut out = String::new();
            emit_repetition(
                &Repetition {
                    min: 0,
                    max: None,
                    greedy: true,
                    sub: Box::new(sub),
                },
                &mut out,
            )
            .unwrap();
            out
        };
        assert_eq!(star(Hir::look(Look::Start)), "(?:^)*");
        assert_eq!(star(Hir::literal("ab".as_bytes())), "(?:ab)*");
        assert_eq!(star(Hir::literal("é".as_bytes())), "é*");
        let inner = Hir::repetition(Repetition {
            min: 1,
            max: None,
            greedy: true,
            sub: Box::new(Hir::literal("a".as_bytes())),
        });
        assert_eq!(star(inner), "(?:a+)*");
        assert_eq!(line("a{255}"), "a{255}");
        assert!(rejected("a{256}").message.contains("at most 255"));
        assert!(rejected("a{1,300}").message.contains("300"));
    }

    #[test]
    fn groups_and_alternation() {
        assert_eq!(line("TODO|FIXME"), "(?:TODO|FIXME)");
        assert_eq!(line("(?P<n>a)b"), "(?:a)b");
        assert_eq!(line(""), "(?:)");
        assert_eq!(line("a|"), "(?:a|(?:))");
    }

    #[test]
    fn file_form_is_newline_sensitive_line_form() {
        let t = translate(r"^\d+$", false).unwrap();
        assert_eq!(t.file_form(), format!("(?n){}", t.line_form));
    }

    #[test]
    fn patterns_are_wrapped_as_ripgrep_wraps_them() {
        // rg parses `(?:PATTERN)`: a stray `)` closes the wrapper, and a `(?x)` comment eats it.
        assert_eq!(line("a)(b"), "a(?:b)");
        assert_eq!(line("a)|(b"), "(?:a|(?:b))");
        let err = rejected("(?x)a#c");
        assert!(
            err.syntax && err.message.contains("unclosed group"),
            "{err}"
        );
        // The error is at the wrapper's paren, which isn't part of the pattern.
        assert_eq!(err.span, None);
        assert_eq!(rejected("a)").span, None);
        assert_eq!(rejected("a))").span, Some((2, 3)));
    }

    #[test]
    fn syntax_errors_carry_rg_message_and_span() {
        let err = rejected("foo(?=bar)");
        assert!(err.syntax);
        assert!(
            err.message.starts_with("regex parse error:"),
            "{}",
            err.message
        );
        assert!(err.message.contains("look-around"));
        assert_eq!(err.span, Some((3, 6)));
        // Spans count characters, not bytes.
        assert_eq!(rejected(r"é\1").span, Some((1, 3)));
        for pattern in [r"(a)\1", r"\Z", r"\Qa\E", r"\h", r"\p{Nope}", "(", "a{2,1}"] {
            assert!(rejected(pattern).syntax, "{pattern}");
        }
    }

    #[test]
    fn unsupported_constructs_are_rejected() {
        for pattern in ["a\\nb", "a\nb", "[\\n]"] {
            let err = rejected(pattern);
            assert!(
                !err.syntax && err.message.contains("\\n"),
                "{pattern}: {err}"
            );
        }
        assert!(rejected(r"(?-u:\xFF)").message.contains("UTF-8"));
        assert!(
            rejected(r"(?-u:[\x00-\xFF])")
                .message
                .contains("byte class")
        );
        assert_eq!(line(r"(?-u:[a-c])"), "[a-c]");
        assert_eq!(
            line(r"\w{100}"),
            format!("{}{{100}}", WordClass::Unicode.bracket())
        );
        assert!(rejected(&r"\w".repeat(80)).message.contains("too large"));
        // Emission stops at the limit instead of building the whole string first.
        assert!(rejected(&r"\b".repeat(1_000)).message.contains("too large"));
    }

    #[test]
    fn nested_counts_are_limited_by_their_expansion() {
        assert_eq!(line("(?:a{0,255}){32}"), "(?:a{0,255}){32}");
        // Long literals and single counts aren't expansions to worry about.
        assert_eq!(line(&"a".repeat(5000)), "a".repeat(5000));
        assert_eq!(line(&"語".repeat(2000)), "語".repeat(2000));
        assert!(translate("(?:abcdefghijklmnopq){0,255}", false).is_ok());
        // Wide patterns are bounded before expansion.
        let words = |n: usize| {
            (0..n)
                .map(|i| format!("w{i}"))
                .collect::<Vec<_>>()
                .join("|")
        };
        assert!(translate(&words(5000), false).is_ok());
        assert!(rejected(&words(16_000)).message.contains("too large"));
        assert_eq!(
            line(r"\w{255}").len(),
            WordClass::Unicode.bracket().len() + 5
        );
        for pattern in ["(?:a{255}){255}", "(?:a{0,255}){33}", "(?:(?:ab){20}){255}"] {
            let err = rejected(pattern);
            assert!(err.message.contains("expand to"), "{pattern}: {err}");
        }
        // A single count over 255 gets the clearer message.
        assert!(rejected("a{1000}").message.contains("at most 255"));
    }

    #[test]
    fn escape_round_trips() {
        assert_eq!(line(&escape("a.b(c)")), r"a\.b\(c\)");
    }
}
