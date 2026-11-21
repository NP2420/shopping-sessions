//! Timestamps per rules.md: F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 with `+HH:MM`/`-HH:MM`, F3 digits = epoch ms.

pub const DAY_MS: i64 = 86_400_000;

#[inline(always)]
fn d2(f: &[u8], i: usize) -> Option<i64> {
    let a = f[i].wrapping_sub(b'0');
    let b = f[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 {
        Some((a as i64) * 10 + b as i64)
    } else {
        None
    }
}

#[inline(always)]
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline(always)]
fn month_days(y: i64, m: i64) -> i64 {
    const MD: [i64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if m == 2 && (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) {
        29
    } else {
        MD[m as usize]
    }
}

/// First 19 bytes `YYYY-MM-DDTHH:MM:SS` to epoch ms, validating the calendar.
#[inline(always)]
fn civil19(f: &[u8]) -> Option<i64> {
    if f[4] != b'-' || f[7] != b'-' || f[10] != b'T' || f[13] != b':' || f[16] != b':' {
        return None;
    }
    let y = d2(f, 0)? * 100 + d2(f, 2)?;
    let mo = d2(f, 5)?;
    let d = d2(f, 8)?;
    let h = d2(f, 11)?;
    let mi = d2(f, 14)?;
    let s = d2(f, 17)?;
    if mo < 1 || mo > 12 || d < 1 || d > month_days(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * 1000)
}

/// Epoch ms, or None for a bad timestamp. A huge F3 returns i64::MAX (outside every window).
#[inline]
pub fn parse(f: &[u8]) -> Option<i64> {
    let n = f.len();
    if n == 20 && f[19] == b'Z' {
        return civil19(f);
    }
    if n == 25 && (f[19] == b'+' || f[19] == b'-') && f[22] == b':' {
        let base = civil19(f)?;
        let oh = d2(f, 20)?;
        let om = d2(f, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        return Some(if f[19] == b'+' { base - off } else { base + off });
    }
    if n == 0 {
        return None;
    }
    let mut v: i64 = 0;
    let mut sig = 0u32;
    for &b in f {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if sig == 0 && d == 0 {
            continue;
        }
        sig += 1;
        if sig <= 18 {
            v = v * 10 + d as i64;
        }
    }
    if sig > 18 {
        return Some(i64::MAX);
    }
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(f1, 1791071890000);
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"1791071890000"), Some(f1));
        assert_eq!(parse(b"0001791071890000"), Some(f1));
        assert_eq!(parse(b"2026-10-04T01:30:00+02:00"), parse(b"2026-10-03T23:30:00Z"));
        for bad in [&b""[..], b"2026-10-03 23:58:10Z", b"2026-10-03T23:58:10z", b"2026-02-29T00:00:00Z", b"2026-10-03T24:00:00Z",
            b"2026-10-03T23:58:60Z", b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60", b"2026-10-03T23:58:10+0200",
            b"1791071890000.0", b"1.79e12", b"-5", b"2026-13-03T23:58:10Z", b"2026-10-03T23:58:10.000Z", b"2026-10-03T23:58:10"] {
            assert!(parse(bad).is_none(), "{}", String::from_utf8_lossy(bad));
        }
        assert!(parse(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse(b"2026-10-03T23:58:10-00:00"), Some(f1));
        assert_eq!(parse(b"123456789012345678901"), Some(i64::MAX));
        assert_eq!(parse(b"86400000"), Some(86_400_000));
    }
}
