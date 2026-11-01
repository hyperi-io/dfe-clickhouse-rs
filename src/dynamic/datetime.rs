// Project:   dfe-clickhouse
// File:      src/dynamic/datetime.rs
// Purpose:   Date-time text read once, written as ticks or as JSONEachRow text
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Date-time text read once and written per insert path.
//!
//! [`parse`] reads `YYYY-MM-DD[T| ]HH:MM:SS[.fraction][Z|+-HH[:MM]]`. Text with
//! no zone is UTC, and text that names one is moved to UTC. The RowBinary
//! encoder writes the instant as ticks.

/// An instant read from date-time text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UtcInstant {
    /// Whole seconds since the Unix epoch, floored.
    pub(crate) secs: i64,
    /// Nanoseconds past `secs`.
    pub(crate) nanos: u32,
    /// Whether the text named its zone.
    pub(crate) zoned: bool,
}

/// The most fraction digits a `DateTime64` holds.
const MAX_PRECISION: u8 = 9;

/// Byte length of `YYYY-MM-DD HH:MM:SS`.
const WHOLE_SECONDS_LEN: usize = 19;

/// Read date-time text in the form the module doc names. Fraction digits past
/// nine are dropped, as ClickHouse drops them.
///
/// # Errors
///
/// What is wrong with text that is not that form, names a date or time that
/// does not exist, or carries an offset past 23:59.
pub(crate) fn parse(text: &str) -> Result<UtcInstant, &'static str> {
    const INVALID: &str = "invalid DateTime string";
    let b = text.as_bytes();
    if b.len() < WHOLE_SECONDS_LEN
        || b[4] != b'-'
        || b[7] != b'-'
        || !matches!(b[10], b'T' | b't' | b' ')
        || b[13] != b':'
        || b[16] != b':'
    {
        return Err(INVALID);
    }
    let (Some(year), Some(month), Some(day), Some(hour), Some(min), Some(sec)) = (
        digits(b, 0, 4),
        digits(b, 5, 2),
        digits(b, 8, 2),
        digits(b, 11, 2),
        digits(b, 14, 2),
        digits(b, 17, 2),
    ) else {
        return Err(INVALID);
    };
    if !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || min > 59
        || sec > 59
    {
        return Err(INVALID);
    }

    let mut pos = WHOLE_SECONDS_LEN;
    let mut nanos = 0_u32;
    if b.get(pos) == Some(&b'.') {
        let start = pos + 1;
        let len = b[start..].iter().take_while(|c| c.is_ascii_digit()).count();
        if len == 0 {
            return Err("invalid fractional seconds");
        }
        for (place, &c) in (0_u32..).zip(&b[start..start + len.min(9)]) {
            nanos += u32::from(c - b'0') * 10_u32.pow(8 - place);
        }
        pos = start + len;
    }

    let offset = match b.get(pos) {
        None => None,
        Some(b'Z' | b'z') => {
            pos += 1;
            Some(0)
        }
        Some(&sign @ (b'+' | b'-')) => {
            let (hh, mm, len) = match b.len() - pos - 1 {
                5 if b[pos + 3] == b':' => (digits(b, pos + 1, 2), digits(b, pos + 4, 2), 6),
                4 => (digits(b, pos + 1, 2), digits(b, pos + 3, 2), 5),
                2 => (digits(b, pos + 1, 2), Some(0), 3),
                _ => return Err("invalid timezone offset"),
            };
            let (Some(hh), Some(mm)) = (hh, mm) else {
                return Err("invalid timezone offset");
            };
            if hh > 23 || mm > 59 {
                return Err("invalid timezone offset");
            }
            pos += len;
            let magnitude = i64::from(hh * 3600 + mm * 60);
            Some(if sign == b'-' { -magnitude } else { magnitude })
        }
        Some(_) => return Err(INVALID),
    };
    if pos != b.len() {
        return Err(INVALID);
    }

    // Four digits, so the year always fits.
    let year = i32::try_from(year).map_err(|_| INVALID)?;
    let local =
        civil_days_from_epoch(year, month, day) * 86_400 + i64::from(hour * 3600 + min * 60 + sec);
    Ok(UtcInstant {
        secs: local - offset.unwrap_or(0),
        nanos,
        zoned: offset.is_some(),
    })
}

impl UtcInstant {
    /// The instant in units of `10^-precision` seconds, which is what a
    /// `DateTime64(precision)` stores. `None` past the range of `i64`.
    pub(crate) fn ticks(self, precision: u8) -> Option<i64> {
        let precision = u32::from(precision.min(MAX_PRECISION));
        self.secs
            .checked_mul(10_i64.pow(precision))?
            .checked_add(i64::from(self.nanos) / 10_i64.pow(9 - precision))
    }
}

/// The ASCII digits at `b[at..at + n]` as a number.
fn digits(b: &[u8], at: usize, n: usize) -> Option<u32> {
    b.get(at..at + n)?.iter().try_fold(0_u32, |acc, &c| {
        c.is_ascii_digit().then(|| acc * 10 + u32::from(c - b'0'))
    })
}

/// Days in `month` of `year`, proleptic Gregorian. 0 for a month past 12.
pub(crate) fn days_in_month(year: u32, month: u32) -> u32 {
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    }
}

/// Days since 1970-01-01 for a civil date (Howard Hinnant's algorithm).
pub(crate) fn civil_days_from_epoch(year: i32, month: u32, day: u32) -> i64 {
    let y = i64::from(if month <= 2 { year - 1 } else { year });
    let m = i64::from(if month <= 2 { month + 9 } else { month - 3 });
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * m + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-04-07 07:03:00 UTC.
    const SECS: i64 = 1_775_545_380;

    fn instant(text: &str) -> UtcInstant {
        parse(text).unwrap_or_else(|e| panic!("{text}: {e}"))
    }

    #[test]
    fn every_zone_form_reads_as_the_same_instant() {
        for text in [
            "2026-04-07T17:03:00.123456789+10:00",
            "2026-04-07T17:03:00.123456789+1000",
            "2026-04-07T17:03:00.123456789+10",
            "2026-04-07T01:33:00.123456789-05:30",
            "2026-04-07 07:03:00.123456789Z",
            "2026-04-07t07:03:00.123456789z",
            "2026-04-07T07:03:00.123456789+00:00",
            "2026-04-07T07:03:00.123456789-00:00",
        ] {
            assert_eq!(
                instant(text),
                UtcInstant {
                    secs: SECS,
                    nanos: 123_456_789,
                    zoned: true
                },
                "{text}"
            );
        }
    }

    #[test]
    fn zone_less_text_reads_as_utc_and_says_so() {
        for text in ["2026-04-07 07:03:00", "2026-04-07T07:03:00"] {
            assert_eq!(
                instant(text),
                UtcInstant {
                    secs: SECS,
                    nanos: 0,
                    zoned: false
                },
                "{text}"
            );
        }
    }

    #[test]
    fn an_offset_that_crosses_midnight_or_a_year_moves_the_date() {
        // 2026-04-06 16:00:00 UTC.
        assert_eq!(instant("2026-04-07 02:00:00+10:00").secs, 1_775_491_200);
        // 2026-01-01 00:30:00 UTC.
        assert_eq!(instant("2025-12-31T23:30:00-01:00").secs, 1_767_227_400);
    }

    #[test]
    fn fraction_digits_past_nine_are_dropped() {
        assert_eq!(
            instant("2026-04-07 07:03:00.1234567891234Z").nanos,
            123_456_789
        );
        assert_eq!(instant("2026-04-07 07:03:00.1Z").nanos, 100_000_000);
    }

    #[test]
    fn malformed_text_is_refused() {
        for text in [
            "",
            "2026-04-07",
            "2026-04-07X07:03:00",
            "2026-13-07 07:03:00",
            "2026-02-29 07:03:00",
            "2026-04-31 07:03:00",
            "2026-04-07 24:00:00",
            "2026-04-07 07:03:60",
            "2026-04-07 07:03:00.",
            "2026-04-07 07:03:00+24:00",
            "2026-04-07 07:03:00+10:60",
            "2026-04-07 07:03:00+1",
            "2026-04-07 07:03:00+10:00 ",
            "2026-04-07 07:03:00 UTC",
            "2026-1\u{e9}-7 07:03:00",
            "\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}",
        ] {
            assert!(parse(text).is_err(), "{text:?} must be refused");
        }
        assert!(parse("2024-02-29 07:03:00").is_ok(), "2024 is a leap year");
    }

    #[test]
    fn ticks_hold_the_column_precision() {
        let i = instant("2026-04-07T07:03:00.123456789Z");
        assert_eq!(i.ticks(0), Some(SECS));
        assert_eq!(i.ticks(3), Some(SECS * 1_000 + 123));
        assert_eq!(i.ticks(6), Some(SECS * 1_000_000 + 123_456));
        assert_eq!(i.ticks(9), Some(SECS * 1_000_000_000 + 123_456_789));
        assert_eq!(instant("1969-12-31T23:59:58.750Z").ticks(3), Some(-1_250));
    }
}
