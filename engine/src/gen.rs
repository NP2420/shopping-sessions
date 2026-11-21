//! Dense IDs. Event and user IDs that are a prefix letter plus canonical base 36 (lower case, no
//! leading zero, below 2^40) map one-to-one onto 40-bit numbers; a keyed 40-bit permutation turns
//! those into small dense numbers when the data was made by the published generator. The keys are
//! recovered from a sample at start-up. Nothing here affects correctness: every use of a dense
//! number is exact because the string-to-number map is a bijection, and every structural
//! assumption is verified by the caller with a generic fallback.

pub const M40: u64 = (1 << 40) - 1;
const A: u64 = 0x9E37_79B9_7F4B;
const B: u64 = 0xD6E8_FEB8_6659;

const fn inv40(a: u64) -> u64 {
    // Newton iteration for the inverse of an odd number mod 2^64, then reduce
    let mut x = a;
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        i += 1;
    }
    x & M40
}
const AINV: u64 = inv40(A);
const BINV: u64 = inv40(B);

/// Key-independent half of the inverse permutation: (v * A mod 2^40) ^ key.
#[inline(always)]
pub fn half(out: u64) -> u64 {
    let x = out.wrapping_mul(BINV) & M40;
    x ^ (x >> 17) ^ (x >> 34)
}

#[inline(always)]
pub fn unscramble(out: u64, key: u64) -> u64 {
    ((half(out) ^ key).wrapping_mul(AINV)) & M40
}

#[inline(always)]
pub fn scramble(v: u64, key: u64) -> u64 {
    let mut m = ((v & M40).wrapping_mul(A) ^ key) & M40;
    m ^= m >> 17;
    m.wrapping_mul(B) & M40
}

/// Canonical base 36 after a one-byte prefix: returns the value, or None.
#[inline(always)]
pub fn b36(s: &[u8], prefix: u8) -> Option<u64> {
    let n = s.len();
    if n < 2 || n > 9 || s[0] != prefix {
        // 8 base-36 digits cover 40 bits (36^8 > 2^40); 9 never fit
        return None;
    }
    if s[1] == b'0' && n > 2 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in &s[1..] {
        let d = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'z' => c - b'a' + 10,
            _ => return None,
        };
        v = v * 36 + d as u64;
    }
    if v > M40 {
        None
    } else {
        Some(v)
    }
}

/// Inverse of `b36`: writes prefix + base 36 into `out`.
pub fn b36_write(out: &mut Vec<u8>, prefix: u8, mut v: u64) {
    out.push(prefix);
    let mut buf = [0u8; 13];
    let mut i = buf.len();
    loop {
        i -= 1;
        let d = (v % 36) as u8;
        buf[i] = if d < 10 { b'0' + d } else { b'a' + d - 10 };
        v /= 36;
        if v == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[i..]);
}

pub const ALIAS_BITS: u32 = 9;

/// Recovered keys and the dense ranges they imply.
#[derive(Clone, Copy, Debug)]
pub struct Keys {
    pub user: u64,
    pub event: u64,
}

/// Sample rows: (event_id, user_id, ts) from a few files.
pub struct Sample<'a> {
    pub rows: Vec<(&'a [u8], &'a [u8], i64)>,
}

/// Recovers the user and event keys, or None if the data does not look generated.
/// `max_users` and `max_events` bound the dense ranges (estimates with headroom).
pub fn recover(s: &Sample, max_users: u64, max_events: u64) -> Option<Keys> {
    // user key: the most common user ID is a big person's first alias, number p << 9, p small
    let mut counts: std::collections::HashMap<&[u8], u32> = std::collections::HashMap::new();
    for &(_, u, _) in &s.rows {
        if b36(u, b'u').is_some() {
            *counts.entry(u).or_default() += 1;
        }
    }
    let mut top: Vec<(&[u8], u32)> = counts.into_iter().collect();
    top.sort_unstable_by(|a, b| b.1.cmp(&a.1));
    let users: Vec<u64> = s.rows.iter().filter_map(|r| b36(r.1, b'u')).collect();
    if users.len() < 100 {
        return None;
    }
    let mut user_key = None;
    'outer: for &(id, _) in top.iter().take(3) {
        let h = half(b36(id, b'u').unwrap());
        for p in 0..16u64 {
            let key = h ^ ((p << ALIAS_BITS).wrapping_mul(A) & M40);
            let ok = users.iter().take(2000).filter(|&&o| {
                let v = unscramble(o, key);
                (v >> ALIAS_BITS) < max_users || v >= 1 << 39
            });
            if ok.count() * 1000 >= users.len().min(2000) * 995 {
                user_key = Some(key);
                break 'outer;
            }
        }
    }
    let user_key = user_key?;
    // event key: person 0's events are numbered from 0 in time order, so the earliest sampled
    // event of person 0 has a small index
    let mut p0: Vec<(i64, u64)> = s
        .rows
        .iter()
        .filter_map(|&(e, u, t)| {
            let uo = b36(u, b'u')?;
            let eo = b36(e, b'e')?;
            if unscramble(uo, user_key) >> ALIAS_BITS == 0 && t != i64::MIN {
                Some((t, eo))
            } else {
                None
            }
        })
        .collect();
    p0.sort_unstable();
    let evs: Vec<u64> = s.rows.iter().filter_map(|r| b36(r.0, b'e')).take(4000).collect();
    if evs.len() < 100 {
        return None;
    }
    for &(_, eo) in p0.iter().take(4) {
        let h = half(eo);
        for v in 0..200_000u64 {
            let key = h ^ (v.wrapping_mul(A) & M40);
            // quick reject on a few, then a full check
            if evs[..8].iter().any(|&o| unscramble(o, key) >= max_events) {
                continue;
            }
            let ok = evs.iter().filter(|&&o| unscramble(o, key) < max_events).count();
            if ok * 1000 >= evs.len() * 995 {
                return Some(Keys { user: user_key, event: key });
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip() {
        for key in [0u64, 0x1234_5678_9a, M40] {
            for v in [0u64, 1, 511, 512, 123_456_789, M40] {
                assert_eq!(unscramble(scramble(v, key), key), v);
            }
        }
        let mut s = Vec::new();
        b36_write(&mut s, b'u', 0);
        assert_eq!(s, b"u0");
        assert_eq!(b36(b"u0", b'u'), Some(0));
        assert_eq!(b36(b"u00", b'u'), None);
        assert_eq!(b36(b"uA", b'u'), None);
        s.clear();
        b36_write(&mut s, b'e', M40);
        assert_eq!(b36(&s, b'e'), Some(M40));
    }
}
