//! IDs as u64 keys.
//!
//! An ID of 1..=10 bytes drawn from `[0-9A-Z_a-z]` packs into 60 bits at 6 bits per byte, first
//! byte highest, with code 0 meaning "past the end". Codes rise with the byte value, so integer
//! order on packed keys is exactly byte-wise string order. Anything else becomes an overflow key:
//! bit 63 set plus a 62-bit hash, with the bytes kept in a side pool for ordering and output.

use std::cmp::Ordering;
use std::collections::HashMap;

pub const OVF: u64 = 1 << 63;
pub const EMPTY: u64 = u64::MAX; // never a valid key: overflow keys keep bit 62 clear

const fn build_code() -> [u8; 256] {
    let mut t = [0u8; 256];
    let mut c = 1u8;
    let mut b = 0usize;
    while b < 256 {
        let ok = (b >= b'0' as usize && b <= b'9' as usize)
            || (b >= b'A' as usize && b <= b'Z' as usize)
            || b == b'_' as usize
            || (b >= b'a' as usize && b <= b'z' as usize);
        if ok {
            t[b] = c;
            c += 1;
        }
        b += 1;
    }
    t
}

const fn build_decode() -> [u8; 64] {
    let code = build_code();
    let mut d = [0u8; 64];
    let mut b = 0usize;
    while b < 256 {
        if code[b] != 0 {
            d[code[b] as usize] = b as u8;
        }
        b += 1;
    }
    d
}

pub static CODE: [u8; 256] = build_code();
pub static DECODE: [u8; 64] = build_decode();

#[inline(always)]
pub fn pack(s: &[u8]) -> Option<u64> {
    if s.is_empty() || s.len() > 10 {
        return None;
    }
    let mut k = 0u64;
    let mut sh = 54u32;
    for &b in s {
        let c = CODE[b as usize];
        if c == 0 {
            return None;
        }
        k |= (c as u64) << sh;
        sh = sh.wrapping_sub(6);
    }
    Some(k)
}

#[inline]
pub fn hash_bytes(s: &[u8]) -> u64 {
    // simple 64-bit multiply-xor hash over 8-byte words; overflow IDs are rare
    let mut h: u64 = 0x243F_6A88_85A3_08D3 ^ (s.len() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut chunks = s.chunks_exact(8);
    for c in &mut chunks {
        let w = u64::from_le_bytes(c.try_into().unwrap());
        h = (h ^ w).wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        h ^= h >> 32;
    }
    let mut last = 0u64;
    for (i, &b) in chunks.remainder().iter().enumerate() {
        last |= (b as u64) << (8 * i);
    }
    h = (h ^ last).wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    h ^= h >> 29;
    h = h.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    h ^ (h >> 32)
}

/// Key for a non-empty ID. Overflow IDs are pushed to `ovf` so their bytes can be found later.
#[inline(always)]
pub fn key(s: &[u8], ovf: &mut Vec<(u64, &'static [u8])>) -> u64 {
    match pack(s) {
        Some(k) => k,
        None => {
            let k = OVF | (hash_bytes(s) >> 2);
            // SAFETY of 'static: every input slice points into a mapping that lives until exit.
            let st: &'static [u8] = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(s) };
            ovf.push((k, st));
            k
        }
    }
}

/// Identity-only key (event IDs): no bytes kept.
#[inline(always)]
pub fn ident(s: &[u8]) -> u64 {
    match pack(s) {
        Some(k) => k,
        None => OVF | (hash_bytes(s) >> 2),
    }
}

#[inline(always)]
pub fn mix(k: u64) -> u64 {
    let mut x = k ^ (k >> 31);
    x = x.wrapping_mul(0x7FB5_D329_728E_A185);
    x ^= x >> 27;
    x = x.wrapping_mul(0x81DA_DEF4_BC2D_D44D);
    x ^ (x >> 33)
}

pub type Pool = HashMap<u64, &'static [u8]>;

pub fn decode_into(k: u64, pool: &Pool, out: &mut Vec<u8>) {
    if k & OVF != 0 {
        out.extend_from_slice(pool.get(&k).expect("overflow id without bytes"));
        return;
    }
    let mut sh = 54i32;
    while sh >= 0 {
        let c = ((k >> sh) & 63) as usize;
        if c == 0 {
            break;
        }
        out.push(DECODE[c]);
        sh -= 6;
    }
}

pub fn cmp_keys(a: u64, b: u64, pool: &Pool) -> Ordering {
    if (a | b) & OVF == 0 {
        return a.cmp(&b);
    }
    let mut x = Vec::with_capacity(16);
    let mut y = Vec::with_capacity(16);
    decode_into(a, pool, &mut x);
    decode_into(b, pool, &mut y);
    x.cmp(&y)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn order_preserved() {
        let words: Vec<&[u8]> = vec![b"0", b"9", b"A", b"U1", b"Z", b"_", b"a", b"u", b"u1", b"u10", b"u1a", b"u2", b"zzzzzzzzzz"];
        for w in words.windows(2) {
            assert!(pack(w[0]).unwrap() < pack(w[1]).unwrap(), "{:?}", w);
        }
        let pool = Pool::default();
        let mut v = Vec::new();
        decode_into(pack(b"u1aZ_9").unwrap(), &pool, &mut v);
        assert_eq!(v, b"u1aZ_9");
        assert!(pack(b" u1").is_none());
        assert!(pack(b"abcdefghijk").is_none());
    }
}
