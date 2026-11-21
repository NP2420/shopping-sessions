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

/// Reference implementation (kept for tests and rare forms).
#[inline(never)]
pub fn parse_slow(f: &[u8]) -> Option<i64> {
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


const Y0: i64 = 1900;
const NY: usize = 400;

/// days_from_civil(y, m, 1) for y in Y0..Y0+NY, m in 1..=12, plus month lengths.
struct Cal {
    start: [[i32; 13]; NY],
    len: [[u8; 13]; NY],
}

static CAL: std::sync::OnceLock<Box<Cal>> = std::sync::OnceLock::new();

fn cal() -> &'static Cal {
    CAL.get_or_init(|| {
        let mut c = Box::new(Cal { start: [[0; 13]; NY], len: [[0; 13]; NY] });
        for yi in 0..NY {
            let y = Y0 + yi as i64;
            for m in 1..=12 {
                c.start[yi][m] = days_from_civil(y, m as i64, 1) as i32;
                c.len[yi][m] = month_days(y, m as i64) as u8;
            }
        }
        c
    })
}

pub fn init() {
    cal();
}

#[inline(always)]
fn swar_all_digits(x: u64, mask: u64) -> bool {
    // each byte in `mask` must be an ASCII digit
    let hi = x & (0xF0F0_F0F0_F0F0_F0F0 & mask);
    let lo = (x & (0x0F0F_0F0F_0F0F_0F0F & mask)).wrapping_add(0x0606_0606_0606_0606 & mask);
    hi == (0x3030_3030_3030_3030 & mask) && (lo & (0xF0F0_F0F0_F0F0_F0F0 & mask)) == 0
}

/// 8 ASCII digits (little-endian load, first char lowest byte) to their value.
#[inline(always)]
fn swar8(x: u64) -> u64 {
    let mut v = x.wrapping_sub(0x3030_3030_3030_3030);
    v = v.wrapping_mul(10).wrapping_add(v >> 8);
    (((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000u64 << 32)))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000u64 << 32))))
        >> 32
}

#[inline(always)]
fn ld8(f: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(f[i..i + 8].try_into().unwrap())
}

/// `YYYY-MM-DDTHH:MM:SS` (first 19 bytes) to epoch ms via the calendar table.
#[inline(always)]
fn civil_fast(f: &[u8]) -> Option<i64> {
    // bytes 0..8 "YYYY-MM-", 8..16 "DDTHH:MM", 11..19 "HH:MM:SS"
    let a = ld8(f, 0);
    let b = ld8(f, 8);
    let c = ld8(f, 11);
    // separators: '-' at 4,7 ; 'T' at 10 (byte 2 of b) ; ':' at 13 (byte 5 of b / 2 of c) and 16 (5 of c)
    const SA: u64 = 0xFF00_00FF_0000_0000; // bytes 4 and 7
    const SB: u64 = 0x0000_0000_00FF_0000; // byte 2 of b (pos 10)
    const SC: u64 = 0x0000_FF00_00FF_0000; // bytes 2 and 5 of c (pos 13, 16)
    if a & SA != 0x2D00_002D_0000_0000 || b & SB != 0x0000_0000_0054_0000 || c & SC != 0x0000_3A00_003A_0000 {
        return None;
    }
    if !swar_all_digits(a, !SA) || !swar_all_digits(b, 0x0000_0000_0000_FFFF) || !swar_all_digits(c, !SC) {
        return None;
    }
    let d = |x: u64, i: u32| -> i64 { ((x >> (8 * i)) & 0xF) as i64 };
    let y = d(a, 0) * 1000 + d(a, 1) * 100 + d(a, 2) * 10 + d(a, 3);
    let mo = d(a, 5) * 10 + d(a, 6);
    let dd = d(b, 0) * 10 + d(b, 1);
    let h = d(c, 0) * 10 + d(c, 1);
    let mi = d(c, 3) * 10 + d(c, 4);
    let s = d(c, 6) * 10 + d(c, 7);
    let yi = y - Y0;
    if yi < 0 || yi >= NY as i64 {
        return civil19(f);
    }
    if mo < 1 || mo > 12 || h > 23 || mi > 59 || s > 59 || dd < 1 {
        return None;
    }
    let cal = cal();
    let yi = yi as usize;
    if dd > cal.len[yi][mo as usize] as i64 {
        return None;
    }
    let days = cal.start[yi][mo as usize] as i64 + dd - 1;
    Some(days * DAY_MS + (h * 3600 + mi * 60 + s) * 1000)
}

/// Epoch ms, or None for a bad timestamp. A huge F3 returns i64::MAX (outside every window).
#[inline(always)]
pub fn parse(f: &[u8]) -> Option<i64> {
    let n = f.len();
    if n == 20 {
        if f[19] == b'Z' {
            return civil_fast(f);
        }
        return parse_slow(f);
    }
    if n == 13 {
        let a = ld8(f, 0);
        let b = ld8(f, 5);
        if swar_all_digits(a, !0) && swar_all_digits(b, !0) {
            // digits 0..8 and 8..13 (b holds 5..13; take its top 5 digits by shifting in '0's)
            let hi = swar8(a);
            let lo = swar8((b >> 24) << 24 | 0x0030_3030);
            return Some((hi * 100_000 + lo) as i64);
        }
        return None;
    }
    if n == 25 {
        if (f[19] == b'+' || f[19] == b'-') && f[22] == b':' {
            let base = civil_fast(f)?;
            let oh = d2(f, 20)?;
            let om = d2(f, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 60 + om) * 60_000;
            return Some(if f[19] == b'+' { base - off } else { base + off });
        }
        return parse_slow(f);
    }
    parse_slow(f)
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

    #[test]
    fn fast_matches_slow() {
        let mut x: u64 = 0x1234_5678_9abc_def1;
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alphabet = b"0123456789-T:Z+ zt.";
        let templates: [&[u8]; 6] = [b"2026-10-03T23:58:10Z", b"2028-02-29T00:00:00Z", b"1791071890000", b"2026-10-03T19:58:10-04:00", b"0000000000000", b"9999-12-31T23:59:59+23:59"];
        for _ in 0..2_000_000 {
            let t = templates[(rnd() % 6) as usize];
            let mut v = t.to_vec();
            let k = rnd() % 4;
            for _ in 0..k {
                let i = (rnd() as usize) % v.len();
                let r = rnd();
                v[i] = if r % 3 == 0 { alphabet[(r as usize / 3) % alphabet.len()] } else { b'0' + ((r >> 8) % 10) as u8 };
            }
            assert_eq!(parse(&v), parse_slow(&v), "{}", String::from_utf8_lossy(&v));
        }
    }
}
