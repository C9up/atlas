//! Dates and times across the JavaScript boundary: the strings a bound
//! parameter may arrive as, and the strings a decoded cell leaves as.
//!
//! The parsers below all follow one rule: NEVER reject a string Postgres
//! itself would accept. Postgres coerces `'2026-03-10T00:00:00Z'` to a `date`
//! without complaint, so refusing it client-side would make atlas stricter
//! than the database it drives — a regression, not a guard. A date-typed
//! parameter therefore accepts a full instant and keeps its date part, and a
//! time-typed one keeps the time part.
//!
//! What they accept is exactly what atlas accepted when it was built on
//! chrono, down to chrono's leniencies: one or two digits for a month, a day
//! or a clock field, whitespace before any number, any run of whitespace
//! where the shape has a space, a year with an explicit sign and any number of
//! digits, and a leap second (`:60`). The tests at the bottom pin both
//! directions against the values chrono produced.

use std::ops::RangeInclusive;

use time::{Duration, Month, PrimitiveDateTime};

/// The Rust type a `date` cell decodes to.
pub(crate) use time::Date;
/// The Rust type a `timestamptz` cell decodes to.
pub(crate) use time::OffsetDateTime as TimestampTz;
/// The Rust type a `timestamp` / `datetime` cell decodes to.
pub(crate) use time::PrimitiveDateTime as Timestamp;
/// The Rust type a `time` cell decodes to.
pub(crate) use time::Time as TimeOfDay;

/// Parse the `TIMESTAMP` shapes the JS side emits: RFC 3339 (the decode side
/// writes a `Z` suffix even for a tz-less column), a `T`- or space-separated
/// local timestamp, and a bare date (midnight).
pub(crate) fn parse_naive_datetime(s: &str) -> Option<Timestamp> {
    parse_instant(s)?.resolve()
}

/// Parse a `TIMESTAMPTZ`: an offset-bearing instant, or any naive form read as
/// UTC (atlas writes UTC, and the naive-string guard upstream already refuses
/// an ambiguous value before it reaches here).
pub(crate) fn parse_utc_datetime(s: &str) -> Option<TimestampTz> {
    parse_naive_datetime(s).map(PrimitiveDateTime::assume_utc)
}

/// Parse a `DATE`. A full instant keeps its date part: a chronos `DateTime`
/// serialises to `2026-03-10T00:00:00Z` even for a date-only column, and
/// Postgres would have taken it.
pub(crate) fn parse_date(s: &str) -> Option<Date> {
    let s = s.trim();
    scan(s, DATE)
        .and_then(|fields| fields.date())
        .or_else(|| parse_instant(s).map(|instant| instant.at.date()))
}

/// Parse a `TIME`. A full instant keeps its time part, for symmetry with
/// `parse_date`.
pub(crate) fn parse_time(s: &str) -> Option<TimeParam> {
    let s = s.trim();
    TIME_FORMATS
        .iter()
        .find_map(|items| scan(s, items)?.time())
        .or_else(|| parse_instant(s).map(|instant| (instant.at.time(), instant.leap)))
        .map(|(time, leap)| TimeParam::new(time, leap))
}

/// A `time` parameter as Postgres receives it: microseconds since midnight.
///
/// Not a `TimeOfDay`, because a leap second on the last minute of the day
/// (`23:59:60`) is `24:00:00`, which Postgres accepts and a time of day
/// cannot hold.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TimeParam(i64);

impl TimeParam {
    fn new(time: TimeOfDay, leap: bool) -> Self {
        let (hour, minute, second, nanosecond) = time.as_hms_nano();
        Self(
            i64::from(hour) * 3_600_000_000
                + i64::from(minute) * 60_000_000
                + (i64::from(second) + i64::from(leap)) * 1_000_000
                + i64::from(nanosecond / 1_000),
        )
    }
}

impl sqlx::Type<sqlx::Postgres> for TimeParam {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <TimeOfDay as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl sqlx::Encode<'_, sqlx::Postgres> for TimeParam {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <i64 as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }

    fn size_hint(&self) -> usize {
        std::mem::size_of::<i64>()
    }
}

/// A timestamp without a time zone as an ISO 8601 string with a `Z` suffix
/// (the write path stores UTC), so `new Date(...)` reads it back unambiguously.
/// Always six fractional digits.
pub(crate) fn timestamp_to_json(v: Timestamp) -> serde_json::Value {
    serde_json::Value::String(format!(
        "{}T{}.{:06}Z",
        date_text(v.date()),
        clock_text(v.time()),
        v.nanosecond() / 1_000
    ))
}

/// A timestamp with a time zone as RFC 3339: no fraction when there is none,
/// otherwise three, six or nine digits, whichever is the shortest exact one,
/// then the offset as `±HH:MM` (`+00:00` for the UTC values sqlx decodes).
pub(crate) fn timestamptz_to_json(v: TimestampTz) -> serde_json::Value {
    let offset = v.offset();
    let (hours, minutes, _) = offset.as_hms();
    serde_json::Value::String(format!(
        "{}T{}{}{}{:02}:{:02}",
        date_text(v.date()),
        clock_text(v.time()),
        fraction_text(v.nanosecond()),
        if offset.is_negative() { '-' } else { '+' },
        hours.unsigned_abs(),
        minutes.unsigned_abs()
    ))
}

/// A date as `YYYY-MM-DD`.
pub(crate) fn date_to_json(v: Date) -> serde_json::Value {
    serde_json::Value::String(date_text(v))
}

/// A time of day as `HH:MM:SS`, with as many fractional digits as it needs.
pub(crate) fn time_to_json(v: TimeOfDay) -> serde_json::Value {
    serde_json::Value::String(format!(
        "{}{}",
        clock_text(v),
        fraction_text(v.nanosecond())
    ))
}

/// `YYYY-MM-DD`. A year outside 0..=9999 carries its sign and at least four
/// digits (`+10000`, `-0001`), as ISO 8601 requires.
fn date_text(date: Date) -> String {
    let year = date.year();
    let month = u8::from(date.month());
    let day = date.day();
    if (0..=9999).contains(&year) {
        format!("{year:04}-{month:02}-{day:02}")
    } else {
        format!("{year:+05}-{month:02}-{day:02}")
    }
}

fn clock_text(time: TimeOfDay) -> String {
    format!(
        "{:02}:{:02}:{:02}",
        time.hour(),
        time.minute(),
        time.second()
    )
}

/// Nothing for a whole second, otherwise the shortest of three, six or nine
/// digits that holds the fraction exactly.
fn fraction_text(nanosecond: u32) -> String {
    if nanosecond == 0 {
        String::new()
    } else if nanosecond.is_multiple_of(1_000_000) {
        format!(".{:03}", nanosecond / 1_000_000)
    } else if nanosecond.is_multiple_of(1_000) {
        format!(".{:06}", nanosecond / 1_000)
    } else {
        format!(".{nanosecond:09}")
    }
}

/// A parsed instant, already moved to UTC when it carried an offset.
///
/// A leap second (`:60`) is held as `:59` plus `leap`: the instant itself is
/// one second later, but its date and time-of-day parts are those of the
/// `:59` second — `2026-03-09T23:59:60Z` is a `date` of 2026-03-09 and a
/// `time` of `24:00:00`.
struct Instant {
    at: Timestamp,
    leap: bool,
}

impl Instant {
    fn resolve(self) -> Option<Timestamp> {
        if self.leap {
            self.at.checked_add(Duration::SECOND)
        } else {
            Some(self.at)
        }
    }
}

/// RFC 3339 first, then the local shapes, then a bare date at midnight.
fn parse_instant(s: &str) -> Option<Instant> {
    let s = s.trim();
    rfc3339(s)
        .or_else(|| {
            DATETIME_FORMATS
                .iter()
                .find_map(|items| scan(s, items)?.instant())
        })
        .or_else(|| {
            scan(s, DATE)?.date().map(|date| Instant {
                at: date.midnight(),
                leap: false,
            })
        })
}

/// RFC 3339: exactly `YYYY-MM-DD`, then `T`, `t` or a space, then
/// `HH:MM:SS`, an optional fraction of at least one digit (digits past the
/// ninth are dropped), and `Z`, `z` or a `±HH:MM` offset under a day — the
/// minus may also be U+2212.
fn rfc3339(s: &str) -> Option<Instant> {
    let head = s.as_bytes().get(..19)?;
    let digit = |i: usize| {
        head.get(i)
            .filter(|byte| byte.is_ascii_digit())
            .map(|byte| byte - b'0')
    };
    let pair = |i: usize| Some(digit(i)? * 10 + digit(i + 1)?);
    let byte_is = |i: usize, expected: &[u8]| head.get(i).is_some_and(|b| expected.contains(b));

    let year = i32::from(pair(0)?) * 100 + i32::from(pair(2)?);
    let month = Month::try_from(pair(5)?).ok()?;
    let day = pair(8)?;
    let (hour, minute, second) = (pair(11)?, pair(14)?, pair(17)?);
    let separators = byte_is(4, b"-")
        && byte_is(7, b"-")
        && byte_is(10, b"tT ")
        && byte_is(13, b":")
        && byte_is(16, b":");
    if !separators {
        return None;
    }
    let date = Date::from_calendar_date(year, month, day).ok()?;
    let (second, leap) = if second == 60 {
        (59, true)
    } else {
        (second, false)
    };

    // The first 19 bytes are ASCII, so 19 is a character boundary.
    let rest = s.get(19..)?;
    let (rest, nanosecond) = match rest.strip_prefix('.') {
        Some(fraction) => nanosecond(fraction)?,
        None => (rest, 0),
    };
    let time = TimeOfDay::from_hms_nano(hour, minute, second, nanosecond).ok()?;
    let (rest, offset_seconds) = utc_offset(rest)?;
    if !rest.is_empty() || offset_seconds.abs() >= 86_400 {
        return None;
    }
    let at = PrimitiveDateTime::new(date, time)
        .checked_sub(Duration::seconds(i64::from(offset_seconds)))?;
    Some(Instant { at, leap })
}

/// `Z`, `z`, or a sign (`+`, `-`, U+2212), two hour digits, a colon and two
/// minute digits; the offset in seconds east of UTC.
fn utc_offset(s: &str) -> Option<(&str, i32)> {
    if let Some(rest) = s.strip_prefix(['Z', 'z']) {
        return Some((rest, 0));
    }
    let (negative, rest) = match s.strip_prefix('+') {
        Some(rest) => (false, rest),
        None => (true, s.strip_prefix(['-', '\u{2212}'])?),
    };
    let two_digits = |text: &str, first: RangeInclusive<u8>| -> Option<i32> {
        match text.as_bytes() {
            [tens, units, ..] if first.contains(tens) && units.is_ascii_digit() => {
                Some(i32::from(tens - b'0') * 10 + i32::from(units - b'0'))
            }
            _ => None,
        }
    };
    let hours = two_digits(rest, b'0'..=b'9')?;
    let rest = rest.get(2..)?.strip_prefix(':')?;
    let minutes = two_digits(rest, b'0'..=b'5')?;
    let rest = rest.get(2..)?;
    let seconds = hours * 3600 + minutes * 60;
    Some((rest, if negative { -seconds } else { seconds }))
}

/// One piece of a local date or time shape, read the way chrono's
/// `parse_from_str` reads the matching `%` specifier.
#[derive(Clone, Copy)]
enum Item {
    /// `%Y`: an optional sign; with one, any number of digits, without, one
    /// to four.
    Year,
    /// `%m`, `%d`, `%H`, `%M`, `%S`: one or two digits, in range.
    Number(Field),
    /// `%.f`: nothing, or a dot and at least one digit.
    Fraction,
    /// Literal text, matched exactly.
    Literal(&'static str),
    /// A space in the shape: any run of whitespace, including none.
    Space,
}

#[derive(Clone, Copy)]
enum Field {
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl Field {
    fn range(self) -> RangeInclusive<i64> {
        match self {
            Self::Month => 1..=12,
            Self::Day => 1..=31,
            Self::Hour => 0..=23,
            Self::Minute => 0..=59,
            // 60 is a leap second.
            Self::Second => 0..=60,
        }
    }
}

const DATE: &[Item] = &[
    Item::Year,
    Item::Literal("-"),
    Item::Number(Field::Month),
    Item::Literal("-"),
    Item::Number(Field::Day),
];

const DATETIME_FORMATS: [&[Item]; 6] = [
    // %Y-%m-%dT%H:%M:%S%.f
    &[
        Item::Year,
        Item::Literal("-"),
        Item::Number(Field::Month),
        Item::Literal("-"),
        Item::Number(Field::Day),
        Item::Literal("T"),
        Item::Number(Field::Hour),
        Item::Literal(":"),
        Item::Number(Field::Minute),
        Item::Literal(":"),
        Item::Number(Field::Second),
        Item::Fraction,
    ],
    // %Y-%m-%d %H:%M:%S%.f
    &[
        Item::Year,
        Item::Literal("-"),
        Item::Number(Field::Month),
        Item::Literal("-"),
        Item::Number(Field::Day),
        Item::Space,
        Item::Number(Field::Hour),
        Item::Literal(":"),
        Item::Number(Field::Minute),
        Item::Literal(":"),
        Item::Number(Field::Second),
        Item::Fraction,
    ],
    // %Y-%m-%dT%H:%M:%S
    &[
        Item::Year,
        Item::Literal("-"),
        Item::Number(Field::Month),
        Item::Literal("-"),
        Item::Number(Field::Day),
        Item::Literal("T"),
        Item::Number(Field::Hour),
        Item::Literal(":"),
        Item::Number(Field::Minute),
        Item::Literal(":"),
        Item::Number(Field::Second),
    ],
    // %Y-%m-%d %H:%M:%S
    &[
        Item::Year,
        Item::Literal("-"),
        Item::Number(Field::Month),
        Item::Literal("-"),
        Item::Number(Field::Day),
        Item::Space,
        Item::Number(Field::Hour),
        Item::Literal(":"),
        Item::Number(Field::Minute),
        Item::Literal(":"),
        Item::Number(Field::Second),
    ],
    // %Y-%m-%dT%H:%M
    &[
        Item::Year,
        Item::Literal("-"),
        Item::Number(Field::Month),
        Item::Literal("-"),
        Item::Number(Field::Day),
        Item::Literal("T"),
        Item::Number(Field::Hour),
        Item::Literal(":"),
        Item::Number(Field::Minute),
    ],
    // %Y-%m-%d %H:%M
    &[
        Item::Year,
        Item::Literal("-"),
        Item::Number(Field::Month),
        Item::Literal("-"),
        Item::Number(Field::Day),
        Item::Space,
        Item::Number(Field::Hour),
        Item::Literal(":"),
        Item::Number(Field::Minute),
    ],
];

const TIME_FORMATS: [&[Item]; 3] = [
    // %H:%M:%S%.f
    &[
        Item::Number(Field::Hour),
        Item::Literal(":"),
        Item::Number(Field::Minute),
        Item::Literal(":"),
        Item::Number(Field::Second),
        Item::Fraction,
    ],
    // %H:%M:%S
    &[
        Item::Number(Field::Hour),
        Item::Literal(":"),
        Item::Number(Field::Minute),
        Item::Literal(":"),
        Item::Number(Field::Second),
    ],
    // %H:%M
    &[
        Item::Number(Field::Hour),
        Item::Literal(":"),
        Item::Number(Field::Minute),
    ],
];

/// The fields a shape read, before they are checked against each other.
#[derive(Default)]
struct Fields {
    year: Option<i32>,
    month: Option<u8>,
    day: Option<u8>,
    hour: Option<u8>,
    minute: Option<u8>,
    second: Option<u8>,
    nanosecond: Option<u32>,
}

impl Fields {
    fn date(&self) -> Option<Date> {
        let month = Month::try_from(self.month?).ok()?;
        Date::from_calendar_date(self.year?, month, self.day?).ok()
    }

    /// The time of day, and whether its second was a leap second.
    fn time(&self) -> Option<(TimeOfDay, bool)> {
        let (second, leap) = match self.second.unwrap_or(0) {
            60 => (59, true),
            second => (second, false),
        };
        let nanosecond = match self.nanosecond {
            Some(_) if self.second.is_none() => return None,
            Some(nanosecond) => nanosecond,
            None => 0,
        };
        let time = TimeOfDay::from_hms_nano(self.hour?, self.minute?, second, nanosecond).ok()?;
        Some((time, leap))
    }

    fn instant(&self) -> Option<Instant> {
        let date = self.date()?;
        let (time, leap) = self.time()?;
        Some(Instant {
            at: PrimitiveDateTime::new(date, time),
            leap,
        })
    }
}

/// Read `s` against `items`; the whole string must be consumed.
fn scan(mut s: &str, items: &[Item]) -> Option<Fields> {
    let mut fields = Fields::default();
    for item in items {
        match *item {
            Item::Literal(text) => s = s.strip_prefix(text)?,
            Item::Space => s = s.trim_start(),
            Item::Fraction => {
                if let Some(digits) = s.strip_prefix('.') {
                    let (rest, value) = nanosecond(digits)?;
                    fields.nanosecond = Some(value);
                    s = rest;
                }
            }
            Item::Year => {
                s = s.trim_start();
                let (rest, value) = if let Some(digits) = s.strip_prefix('-') {
                    let (rest, value) = number(digits, 1, usize::MAX)?;
                    (rest, value.checked_neg()?)
                } else if let Some(digits) = s.strip_prefix('+') {
                    number(digits, 1, usize::MAX)?
                } else {
                    number(s, 1, 4)?
                };
                fields.year = Some(i32::try_from(value).ok()?);
                s = rest;
            }
            Item::Number(field) => {
                s = s.trim_start();
                let (rest, value) = number(s, 1, 2)?;
                if !field.range().contains(&value) {
                    return None;
                }
                let value = u8::try_from(value).ok()?;
                let slot = match field {
                    Field::Month => &mut fields.month,
                    Field::Day => &mut fields.day,
                    Field::Hour => &mut fields.hour,
                    Field::Minute => &mut fields.minute,
                    Field::Second => &mut fields.second,
                };
                *slot = Some(value);
                s = rest;
            }
        }
    }
    s.is_empty().then_some(fields)
}

/// Between `min` and `max` leading ASCII digits as a number, and the rest.
fn number(s: &str, min: usize, max: usize) -> Option<(&str, i64)> {
    let digits = s.bytes().take(max).take_while(u8::is_ascii_digit).count();
    if digits < min {
        return None;
    }
    let value = s.bytes().take(digits).try_fold(0i64, |value, byte| {
        value.checked_mul(10)?.checked_add(i64::from(byte - b'0'))
    })?;
    // Every byte before `digits` is an ASCII digit: a character boundary.
    Some((s.get(digits..)?, value))
}

/// A fraction of a second: at least one digit, the first nine counted as
/// nanoseconds, any further ones dropped.
fn nanosecond(s: &str) -> Option<(&str, u32)> {
    const SCALE: [u32; 10] = [
        0,
        100_000_000,
        10_000_000,
        1_000_000,
        100_000,
        10_000,
        1_000,
        100,
        10,
        1,
    ];
    let (rest, value) = number(s, 1, 9)?;
    let scale = SCALE.get(s.len() - rest.len())?;
    let value = u32::try_from(value).ok()?.checked_mul(*scale)?;
    Some((rest.trim_start_matches(|c: char| c.is_ascii_digit()), value))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bytes sqlx writes to the Postgres wire for a bound value.
    fn wire<T: for<'q> sqlx::Encode<'q, sqlx::Postgres>>(value: T) -> Vec<u8> {
        let mut buf = sqlx::postgres::PgArgumentBuffer::default();
        let _ = value.encode_by_ref(&mut buf).expect("encodes");
        buf.to_vec()
    }

    fn wire_i64(bytes: &[u8]) -> i64 {
        i64::from_be_bytes(bytes.try_into().expect("8 bytes"))
    }

    fn wire_i32(bytes: &[u8]) -> i32 {
        i32::from_be_bytes(bytes.try_into().expect("4 bytes"))
    }

    /// The proleptic Gregorian date `days` after 2000-01-01 (Howard Hinnant's
    /// `civil_from_days`), so the expectations below read as dates rather
    /// than as raw wire integers, without going through any date library.
    fn civil(days: i64) -> String {
        let z = days + 10_957 + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = yoe + era * 400 + i64::from(month <= 2);
        format!("{year:04}-{month:02}-{day:02}")
    }

    fn clock(micros: i64) -> String {
        let (secs, frac) = (micros.div_euclid(1_000_000), micros.rem_euclid(1_000_000));
        format!(
            "{:02}:{:02}:{:02}.{frac:06}",
            secs / 3600,
            secs / 60 % 60,
            secs % 60
        )
    }

    /// What Postgres receives for a `timestamp` parameter, as text.
    fn bound_timestamp(s: &str) -> Option<String> {
        parse_naive_datetime(s).map(|v| {
            let us = wire_i64(&wire(v));
            let day = us.div_euclid(86_400_000_000);
            format!("{} {}", civil(day), clock(us.rem_euclid(86_400_000_000)))
        })
    }

    fn bound_timestamptz(s: &str) -> Option<String> {
        parse_utc_datetime(s).map(|v| {
            let us = wire_i64(&wire(v));
            let day = us.div_euclid(86_400_000_000);
            format!("{} {}", civil(day), clock(us.rem_euclid(86_400_000_000)))
        })
    }

    fn bound_date(s: &str) -> Option<String> {
        parse_date(s).map(|v| civil(i64::from(wire_i32(&wire(v)))))
    }

    fn bound_time(s: &str) -> Option<String> {
        parse_time(s).map(|v| clock(wire_i64(&wire(v))))
    }

    /// An input, then what reaches Postgres for each parameter type.
    type Bound = (
        &'static str,
        Option<&'static str>,
        Option<&'static str>,
        Option<&'static str>,
    );

    /// Each input, then what Postgres receives for it as a `timestamp` (and as a
    /// `timestamptz`, which reads every input the same way), a `date` and a
    /// `time` parameter; `None` is a parameter atlas refuses. Recorded from the
    /// chrono-based parsers these replaced, so a change here is a change in what
    /// reaches the database.
    #[rustfmt::skip]
    const BOUND: &[Bound] = &[
        ("2026-03-10T12:34:56Z", Some("2026-03-10 12:34:56.000000"), Some("2026-03-10"), Some("12:34:56.000000")),
        ("2026-03-10T12:34:56.123Z", Some("2026-03-10 12:34:56.123000"), Some("2026-03-10"), Some("12:34:56.123000")),
        ("2026-03-10T12:34:56.123456Z", Some("2026-03-10 12:34:56.123456"), Some("2026-03-10"), Some("12:34:56.123456")),
        ("2026-03-10T12:34:56.123456789Z", Some("2026-03-10 12:34:56.123456"), Some("2026-03-10"), Some("12:34:56.123456")),
        ("2026-03-10T12:34:56.1234567891234Z", Some("2026-03-10 12:34:56.123456"), Some("2026-03-10"), Some("12:34:56.123456")),
        ("2026-03-10T12:34:56.0000005Z", Some("2026-03-10 12:34:56.000000"), Some("2026-03-10"), Some("12:34:56.000000")),
        ("2026-03-10t12:34:56z", Some("2026-03-10 12:34:56.000000"), Some("2026-03-10"), Some("12:34:56.000000")),
        ("2026-03-10 12:34:56+02:00", Some("2026-03-10 10:34:56.000000"), Some("2026-03-10"), Some("10:34:56.000000")),
        ("2026-03-10T12:34:56-05:30", Some("2026-03-10 18:04:56.000000"), Some("2026-03-10"), Some("18:04:56.000000")),
        ("2026-03-10T12:34:56+23:59", Some("2026-03-09 12:35:56.000000"), Some("2026-03-09"), Some("12:35:56.000000")),
        ("2026-03-10T12:34:56-00:00", Some("2026-03-10 12:34:56.000000"), Some("2026-03-10"), Some("12:34:56.000000")),
        ("2026-03-10T12:34:56+24:00", None, None, None),
        ("2026-03-10T12:34:56+05:60", None, None, None),
        ("2026-03-10T12:34:56\u{2212}01:00", Some("2026-03-10 13:34:56.000000"), Some("2026-03-10"), Some("13:34:56.000000")),
        ("2026-03-10T12:34:56+0100", None, None, None),
        ("2026-03-10T12:34:56+01", None, None, None),
        ("2026-03-10T12:34:56+01:", None, None, None),
        ("2026-03-10T12:34:56.Z", None, None, None),
        ("2026-03-10T12:34:56,5Z", None, None, None),
        ("2026-03-10T23:59:60Z", Some("2026-03-11 00:00:00.000000"), Some("2026-03-10"), Some("24:00:00.000000")),
        ("2026-03-10T23:59:60.5+01:00", Some("2026-03-10 23:00:00.500000"), Some("2026-03-10"), Some("23:00:00.500000")),
        ("2026-03-10T00:59:60+01:00", Some("2026-03-10 00:00:00.000000"), Some("2026-03-09"), Some("24:00:00.000000")),
        ("2026-03-09T23:59:60.999999Z", Some("2026-03-10 00:00:00.999999"), Some("2026-03-09"), Some("24:00:00.999999")),
        ("2026-02-30T00:00:00Z", None, None, None),
        ("2026-13-01T00:00:00Z", None, None, None),
        ("2026-03-10T24:00:00Z", None, None, None),
        ("2026-03-10T12:60:00Z", None, None, None),
        ("2026-03-10T12:34:61Z", None, None, None),
        ("0000-01-01T00:00:00Z", Some("0000-01-01 00:00:00.000000"), Some("0000-01-01"), Some("00:00:00.000000")),
        ("0000-01-01T00:30:00+01:00", Some("-001-12-31 23:30:00.000000"), Some("-001-12-31"), Some("23:30:00.000000")),
        ("9999-12-31T23:59:59.999999Z", Some("9999-12-31 23:59:59.999999"), Some("9999-12-31"), Some("23:59:59.999999")),
        ("9999-12-31T23:59:59-01:00", Some("10000-01-01 00:59:59.000000"), Some("10000-01-01"), Some("00:59:59.000000")),
        ("1999-12-31T23:59:59.9999995Z", Some("2000-01-01 00:00:00.000000"), Some("1999-12-31"), Some("23:59:59.999999")),
        ("1969-07-20T20:17:40Z", Some("1969-07-20 20:17:40.000000"), Some("1969-07-20"), Some("20:17:40.000000")),
        ("2026-03-10T12:34:56", Some("2026-03-10 12:34:56.000000"), Some("2026-03-10"), Some("12:34:56.000000")),
        ("2026-03-10 12:34:56", Some("2026-03-10 12:34:56.000000"), Some("2026-03-10"), Some("12:34:56.000000")),
        ("2026-03-10T12:34:56.5", Some("2026-03-10 12:34:56.500000"), Some("2026-03-10"), Some("12:34:56.500000")),
        ("2026-03-10 12:34:56.000001", Some("2026-03-10 12:34:56.000001"), Some("2026-03-10"), Some("12:34:56.000001")),
        ("2026-03-10T12:34:56.123456789123", Some("2026-03-10 12:34:56.123456"), Some("2026-03-10"), Some("12:34:56.123456")),
        ("2026-03-10T12:34:56.", None, None, None),
        ("2026-03-10T12:34", Some("2026-03-10 12:34:00.000000"), Some("2026-03-10"), Some("12:34:00.000000")),
        ("2026-03-10 12:34", Some("2026-03-10 12:34:00.000000"), Some("2026-03-10"), Some("12:34:00.000000")),
        ("2026-03-10T12", None, None, None),
        ("2026-03-10", Some("2026-03-10 00:00:00.000000"), Some("2026-03-10"), Some("00:00:00.000000")),
        ("2026-3-1", Some("2026-03-01 00:00:00.000000"), Some("2026-03-01"), Some("00:00:00.000000")),
        ("2026-3-1 1:2:3", Some("2026-03-01 01:02:03.000000"), Some("2026-03-01"), Some("01:02:03.000000")),
        ("26-03-10", Some("0026-03-10 00:00:00.000000"), Some("0026-03-10"), Some("00:00:00.000000")),
        ("2026-03-10  12:34:56", Some("2026-03-10 12:34:56.000000"), Some("2026-03-10"), Some("12:34:56.000000")),
        ("2026-03-10\t12:34:56", Some("2026-03-10 12:34:56.000000"), Some("2026-03-10"), Some("12:34:56.000000")),
        ("2026-03-10\u{3000}12:34:56", Some("2026-03-10 12:34:56.000000"), Some("2026-03-10"), Some("12:34:56.000000")),
        ("2026-03-1012:34:56", Some("2026-03-10 12:34:56.000000"), Some("2026-03-10"), Some("12:34:56.000000")),
        ("2026-03-10T 12:34:56", Some("2026-03-10 12:34:56.000000"), Some("2026-03-10"), Some("12:34:56.000000")),
        ("2026-03-10T12: 34: 56", Some("2026-03-10 12:34:56.000000"), Some("2026-03-10"), Some("12:34:56.000000")),
        ("2026-03-10t12:34:56", None, None, None),
        ("2026-03-10T12:34:60", Some("2026-03-10 12:35:00.000000"), Some("2026-03-10"), Some("12:35:00.000000")),
        ("2026-03-10 23:59:60.25", Some("2026-03-11 00:00:00.250000"), Some("2026-03-10"), Some("24:00:00.250000")),
        ("2026-03-10T12:34:56Zjunk", None, None, None),
        ("+10000-01-01T00:00:00", Some("10000-01-01 00:00:00.000000"), Some("10000-01-01"), Some("00:00:00.000000")),
        ("+002026-03-10", Some("2026-03-10 00:00:00.000000"), Some("2026-03-10"), Some("00:00:00.000000")),
        ("-0001-12-31 23:59:59", Some("-001-12-31 23:59:59.000000"), Some("-001-12-31"), Some("23:59:59.000000")),
        ("-4713-11-24", Some("-4713-11-24 00:00:00.000000"), Some("-4713-11-24"), Some("00:00:00.000000")),
        ("+262142-12-31", Some("262142-12-31 00:00:00.000000"), Some("262142-12-31"), Some("00:00:00.000000")),
        ("20261-03-10", None, None, None),
        ("2026- 03-10", Some("2026-03-10 00:00:00.000000"), Some("2026-03-10"), Some("00:00:00.000000")),
        ("2026 -03-10", None, None, None),
        ("   2026-03-10T12:34:56Z   ", Some("2026-03-10 12:34:56.000000"), Some("2026-03-10"), Some("12:34:56.000000")),
        ("", None, None, None),
        ("abc", None, None, None),
        ("2026", None, None, None),
        ("2026-03", None, None, None),
        ("0000-02-29", Some("0000-02-29 00:00:00.000000"), Some("0000-02-29"), Some("00:00:00.000000")),
        ("1900-02-29", None, None, None),
        ("2000-02-29", Some("2000-02-29 00:00:00.000000"), Some("2000-02-29"), Some("00:00:00.000000")),
        ("12:34", None, None, Some("12:34:00.000000")),
        ("12:34:56", None, None, Some("12:34:56.000000")),
        ("12:34:56.789", None, None, Some("12:34:56.789000")),
        ("12:34:56.000001", None, None, Some("12:34:56.000001")),
        ("12:34:56.123456789", None, None, Some("12:34:56.123456")),
        ("1:2:3", None, None, Some("01:02:03.000000")),
        ("00:00:00", None, None, Some("00:00:00.000000")),
        ("23:59:59.999999", None, None, Some("23:59:59.999999")),
        ("23:59:60", None, None, Some("24:00:00.000000")),
        ("12:30:60.25", None, None, Some("12:31:00.250000")),
        ("24:00:00", None, None, None),
        ("12:60", None, None, None),
        ("12:34:56Z", None, None, None),
        ("12: 34", None, None, Some("12:34:00.000000")),
        ("12 :34", None, None, None),
        ("12:34:56.", None, None, None),
        ("noon", None, None, None),
    ];

    /// Each value, then the string a decoded `timestamp`, `timestamptz`, `date` and
    /// `time` cell holding it reaches JavaScript as. Recorded from the chrono-based
    /// formatters these replaced.
    #[rustfmt::skip]
    const DECODED: &[(&str, &str, &str, &str, &str)] = &[
        ("2026-03-10T12:34:56", "2026-03-10T12:34:56.000000Z", "2026-03-10T12:34:56+00:00", "2026-03-10", "12:34:56"),
        ("2026-03-10T12:34:56.5", "2026-03-10T12:34:56.500000Z", "2026-03-10T12:34:56.500+00:00", "2026-03-10", "12:34:56.500"),
        ("2026-03-10T12:34:56.25", "2026-03-10T12:34:56.250000Z", "2026-03-10T12:34:56.250+00:00", "2026-03-10", "12:34:56.250"),
        ("2026-03-10T12:34:56.123", "2026-03-10T12:34:56.123000Z", "2026-03-10T12:34:56.123+00:00", "2026-03-10", "12:34:56.123"),
        ("2026-03-10T12:34:56.000001", "2026-03-10T12:34:56.000001Z", "2026-03-10T12:34:56.000001+00:00", "2026-03-10", "12:34:56.000001"),
        ("2026-03-10T12:34:56.123456", "2026-03-10T12:34:56.123456Z", "2026-03-10T12:34:56.123456+00:00", "2026-03-10", "12:34:56.123456"),
        ("2026-03-10T12:34:56.1234567", "2026-03-10T12:34:56.123456Z", "2026-03-10T12:34:56.123456700+00:00", "2026-03-10", "12:34:56.123456700"),
        ("2026-03-10T12:34:56.123456789", "2026-03-10T12:34:56.123456Z", "2026-03-10T12:34:56.123456789+00:00", "2026-03-10", "12:34:56.123456789"),
        ("2026-03-10T12:34:56.000000001", "2026-03-10T12:34:56.000000Z", "2026-03-10T12:34:56.000000001+00:00", "2026-03-10", "12:34:56.000000001"),
        ("0000-01-01T00:00:00", "0000-01-01T00:00:00.000000Z", "0000-01-01T00:00:00+00:00", "0000-01-01", "00:00:00"),
        ("0999-05-06T07:08:09", "0999-05-06T07:08:09.000000Z", "0999-05-06T07:08:09+00:00", "0999-05-06", "07:08:09"),
        ("0099-01-01", "0099-01-01T00:00:00.000000Z", "0099-01-01T00:00:00+00:00", "0099-01-01", "00:00:00"),
        ("-0001-12-31T23:59:59.5", "-0001-12-31T23:59:59.500000Z", "-0001-12-31T23:59:59.500+00:00", "-0001-12-31", "23:59:59.500"),
        ("-12345-06-07T00:00:00", "-12345-06-07T00:00:00.000000Z", "-12345-06-07T00:00:00+00:00", "-12345-06-07", "00:00:00"),
        ("+10000-01-01T00:00:00", "+10000-01-01T00:00:00.000000Z", "+10000-01-01T00:00:00+00:00", "+10000-01-01", "00:00:00"),
        ("9999-12-31T23:59:59.999999", "9999-12-31T23:59:59.999999Z", "9999-12-31T23:59:59.999999+00:00", "9999-12-31", "23:59:59.999999"),
        ("+262142-12-31T23:59:59", "+262142-12-31T23:59:59.000000Z", "+262142-12-31T23:59:59+00:00", "+262142-12-31", "23:59:59"),
    ];

    #[test]
    fn bound_parameters_reach_postgres_unchanged() {
        for &(input, timestamp, date, time) in BOUND {
            assert_eq!(
                bound_timestamp(input).as_deref(),
                timestamp,
                "timestamp {input:?}"
            );
            assert_eq!(
                bound_timestamptz(input).as_deref(),
                timestamp,
                "timestamptz {input:?}"
            );
            assert_eq!(bound_date(input).as_deref(), date, "date {input:?}");
            assert_eq!(bound_time(input).as_deref(), time, "time {input:?}");
        }
    }

    /// The one place the port widens what atlas takes: chrono stopped at the
    /// years -262 143..=262 142 and refused the rest client-side, while
    /// Postgres keeps dates up to 5 874 897. A year past chrono's range now
    /// reaches the database, which decides.
    #[test]
    fn years_past_chronos_range_reach_postgres() {
        assert_eq!(bound_date("+262143-01-01").as_deref(), Some("262143-01-01"));
        assert_eq!(bound_date("+999999-12-31").as_deref(), Some("999999-12-31"));
        assert_eq!(
            bound_timestamp("+290000-06-01 12:00").as_deref(),
            Some("290000-06-01 12:00:00.000000")
        );
        assert_eq!(
            text(date_to_json(parse_date("+262143-01-01").expect("a date"))).as_deref(),
            Some("+262143-01-01")
        );
        // Past the widest year the type holds, still refused.
        assert_eq!(bound_date("+1000000-01-01"), None);
    }

    fn text(v: serde_json::Value) -> Option<String> {
        v.as_str().map(str::to_owned)
    }

    #[test]
    fn decoded_cells_reach_javascript_unchanged() {
        for &(input, timestamp, timestamptz, date, time) in DECODED {
            let naive = parse_naive_datetime(input).expect("a timestamp");
            assert_eq!(
                text(timestamp_to_json(naive)).as_deref(),
                Some(timestamp),
                "{input:?}"
            );
            let utc = parse_utc_datetime(input).expect("a timestamptz");
            assert_eq!(
                text(timestamptz_to_json(utc)).as_deref(),
                Some(timestamptz),
                "{input:?}"
            );
            let day = parse_date(input).expect("a date");
            assert_eq!(text(date_to_json(day)).as_deref(), Some(date), "{input:?}");
            assert_eq!(
                text(time_to_json(naive.time())).as_deref(),
                Some(time),
                "{input:?}"
            );
        }
    }
}
