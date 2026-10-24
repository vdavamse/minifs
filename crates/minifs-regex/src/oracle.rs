//! Test-only helpers behind the `oracle` feature: the Rust `regex` crate as the reference
//! matcher, and random patterns for property tests.

use regex_syntax::hir::{Capture, Class, ClassUnicode, ClassUnicodeRange, Hir, Look, Repetition};

/// Reports whether `pattern` matches anywhere in `haystack`, using the Rust `regex` crate.
/// Differential tests compare the translated ARE against this.
pub fn oracle_is_match(
    pattern: &str,
    haystack: &str,
    ignore_case: bool,
) -> Result<bool, regex::Error> {
    // Wrapped as ripgrep, and so the translator, wrap it.
    Ok(regex::RegexBuilder::new(&format!("(?:{pattern})"))
        .case_insensitive(ignore_case)
        .build()?
        .is_match(haystack))
}

/// A random pattern, printed from a random HIR with regex-syntax's printer. The same seed
/// always gives the same pattern. Literals and classes draw from [`CHARS`], so patterns and
/// random lines over the same characters match often enough to be interesting.
pub fn random_pattern(seed: u64) -> String {
    let mut generator = Generator {
        rng: SplitMix64(seed),
        captures: 0,
    };
    generator.hir(4).to_string()
}

/// Characters for random literals and class ranges: ASCII, regex metacharacters, and
/// non-ASCII letters whose classification or case folding differs between Rust and an ASCII
/// view (`é`, `ß`, `ſ` folds to `s`, the Kelvin sign folds to `k`, `ǅ` is titlecase, `٣` is a
/// digit, `😀` is outside the BMP), plus control, separator and combining characters that the
/// emitter writes as escapes.
pub const CHARS: &str = "aAbBeEsSkKxz09_ -.()[]{}*+?|^$\\/#&~\t\r\u{1}\u{7f}\u{85}\u{a0}\u{2028}\u{301}éÉßſ\u{212a}ǅ٣😀";

const PERL_CLASSES: &[&str] = &[
    r"\w",
    r"\W",
    r"\d",
    r"\D",
    r"\s",
    r"\S",
    r"\pL",
    r"\p{Greek}",
    r"\p{Lu}",
    ".",
    r"(?-u:\w)",
];

const LOOKS: &[Look] = &[
    Look::Start,
    Look::End,
    Look::StartLF,
    Look::EndLF,
    Look::WordAscii,
    Look::WordUnicode,
    Look::WordUnicodeNegate,
    Look::WordStartAscii,
    Look::WordEndAscii,
    Look::WordStartUnicode,
    Look::WordEndUnicode,
    Look::WordStartHalfAscii,
    Look::WordEndHalfAscii,
    Look::WordStartHalfUnicode,
    Look::WordEndHalfUnicode,
];

/// A small, dependency-free PRNG (splitmix64): enough for reproducible test inputs.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
}

struct Generator {
    rng: SplitMix64,
    captures: u32,
}

impl Generator {
    fn hir(&mut self, depth: u32) -> Hir {
        let kinds = if depth == 0 { 3 } else { 8 };
        match self.rng.below(kinds) {
            0 => self.literal(),
            1 => self.class(),
            2 => Hir::look(LOOKS[self.rng.below(LOOKS.len())]),
            3 => self.repetition(depth - 1),
            4 => {
                self.captures += 1;
                let sub = Box::new(self.hir(depth - 1));
                Hir::capture(Capture {
                    index: self.captures,
                    name: None,
                    sub,
                })
            }
            5 | 6 => {
                let n = 2 + self.rng.below(3);
                Hir::concat((0..n).map(|_| self.hir(depth - 1)).collect())
            }
            _ => {
                let n = 2 + self.rng.below(2);
                let mut subs: Vec<Hir> = (0..n).map(|_| self.hir(depth - 1)).collect();
                if self.rng.chance(10) {
                    subs.push(Hir::empty());
                }
                Hir::alternation(subs)
            }
        }
    }

    fn char(&mut self) -> char {
        let chars: Vec<char> = CHARS.chars().collect();
        chars[self.rng.below(chars.len())]
    }

    fn literal(&mut self) -> Hir {
        let n = 1 + self.rng.below(3);
        let text: String = (0..n).map(|_| self.char()).collect();
        Hir::literal(text.as_bytes())
    }

    fn class(&mut self) -> Hir {
        let mut class = if self.rng.chance(40) {
            let pattern = PERL_CLASSES[self.rng.below(PERL_CLASSES.len())];
            match regex_syntax::parse(pattern)
                .expect("perl class parses")
                .into_kind()
            {
                regex_syntax::hir::HirKind::Class(Class::Unicode(class)) => class,
                regex_syntax::hir::HirKind::Class(Class::Bytes(class)) => {
                    ClassUnicode::new(class.iter().map(|r| {
                        ClassUnicodeRange::new(char::from(r.start()), char::from(r.end()))
                    }))
                }
                kind => unreachable!("{pattern} is a class, not {kind:?}"),
            }
        } else {
            let n = 1 + self.rng.below(3);
            ClassUnicode::new((0..n).map(|_| {
                let (a, b) = (self.char(), self.char());
                ClassUnicodeRange::new(a.min(b), a.max(b))
            }))
        };
        if self.rng.chance(20) {
            class.case_fold_simple();
        }
        if self.rng.chance(25) {
            class.negate();
        }
        // A class holding only \n would print as the literal \n, which ripgrep rejects.
        class.difference(&ClassUnicode::new([ClassUnicodeRange::new('\n', '\n')]));
        Hir::class(Class::Unicode(class))
    }

    fn repetition(&mut self, depth: u32) -> Hir {
        let min = self.rng.below(3) as u32;
        let max = match self.rng.below(3) {
            0 => None,
            _ => Some(min + self.rng.below(3) as u32),
        };
        let greedy = self.rng.chance(70);
        let sub = Box::new(self.hir(depth));
        Hir::repetition(Repetition {
            min,
            max,
            greedy,
            sub,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::translate;

    #[test]
    fn oracle_matches_and_rejects() {
        assert!(oracle_is_match(r"fn \w+\(", "pub fn main() {", false).unwrap());
        assert!(!oracle_is_match(r"^\d+$", "12a", false).unwrap());
        assert!(oracle_is_match("ERROR", "an error", true).unwrap());
        assert!(oracle_is_match(r"(", "", false).is_err());
        assert!(oracle_is_match("a)(b", "ab", false).unwrap());
        assert!(oracle_is_match("(?x)a#c", "a", false).is_err());
    }

    #[test]
    fn random_patterns_are_reproducible_and_translate() {
        assert_eq!(random_pattern(7), random_pattern(7));
        for seed in 0..2000 {
            let pattern = random_pattern(seed);
            // The printed pattern must parse, and translate unless it's over the size cap.
            oracle_is_match(&pattern, "", false).unwrap_or_else(|e| panic!("{pattern}: {e}"));
            if let Err(e) = translate(&pattern, false) {
                assert!(e.message.contains("too large"), "{pattern}: {e}");
            }
        }
    }
}
