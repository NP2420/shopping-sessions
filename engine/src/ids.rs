//! IDs as u64 keys.
//!
//! An ID of 1..=10 bytes drawn from `[0-9A-Z_a-z]` packs into 60 bits at 6 bits per byte, first
//! byte highest, with code 0 meaning "past the end". Codes rise with the byte value, so integer
//! order on packed keys is exactly byte-wise string order. Anything else becomes an overflow key:
//! bit 63 set plus a 62-bit hash, with the bytes kept in a side pool for ordering and output.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AO};
use std::sync::Mutex;

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
pub fn pack_slow(s: &[u8]) -> Option<u64> {
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


/// Same as `pack_slow`, 16 bytes at a time. The 16-byte load never crosses a page boundary
/// (it falls back otherwise), so it cannot fault even at the end of a mapping.
#[inline(always)]
pub fn pack(s: &[u8]) -> Option<u64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let n = s.len();
        let p = s.as_ptr();
        if n == 0 || n > 10 {
            return None;
        }
        if (p as usize & 4095) > 4096 - 16 {
            return pack_slow(s);
        }
        let v = _mm_loadu_si128(p as *const __m128i);
        let idx = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        let inb = _mm_cmpgt_epi8(_mm_set1_epi8(n as i8), idx);
        let in_range = |lo: u8, len: u8| -> (__m128i, __m128i) {
            let x = _mm_sub_epi8(v, _mm_set1_epi8(lo as i8));
            let ok = _mm_cmpeq_epi8(_mm_min_epu8(x, _mm_set1_epi8((len - 1) as i8)), x);
            (x, ok)
        };
        let (dx, dok) = in_range(b'0', 10);
        let (ux, uok) = in_range(b'A', 26);
        let (lx, lok) = in_range(b'a', 26);
        let sok = _mm_cmpeq_epi8(v, _mm_set1_epi8(b'_' as i8));
        let ok = _mm_or_si128(_mm_or_si128(dok, uok), _mm_or_si128(lok, sok));
        if _mm_movemask_epi8(_mm_or_si128(ok, _mm_xor_si128(inb, _mm_set1_epi8(-1)))) != 0xFFFF {
            return None;
        }
        let code = _mm_or_si128(
            _mm_or_si128(
                _mm_and_si128(dok, _mm_add_epi8(dx, _mm_set1_epi8(1))),
                _mm_and_si128(uok, _mm_add_epi8(ux, _mm_set1_epi8(11))),
            ),
            _mm_or_si128(
                _mm_and_si128(lok, _mm_add_epi8(lx, _mm_set1_epi8(38))),
                _mm_and_si128(sok, _mm_set1_epi8(37)),
            ),
        );
        let code = _mm_and_si128(code, inb);
        let pairs = _mm_maddubs_epi16(code, _mm_set1_epi16(0x0140)); // c0*64 + c1 per 16-bit lane
        let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_1000)); // (c0c1)*4096 + c2c3
        let l0 = _mm_cvtsi128_si32(quads) as u32 as u64;
        let l1 = _mm_extract_epi32(quads, 1) as u32 as u64;
        let l2 = _mm_extract_epi32(quads, 2) as u32 as u64;
        return Some((l0 << 36) | (l1 << 12) | (l2 >> 12));
    }
    #[allow(unreachable_code)]
    pack_slow(s)
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

/// Key for a non-empty user ID. Overflow IDs go into `pool` (a concurrent table) so their
/// bytes can be found later.
#[inline(always)]
pub fn key(s: &[u8], pool: &Pool) -> u64 {
    match pack(s) {
        Some(k) => k,
        None => {
            let k = OVF | (hash_bytes(s) >> 2);
            // SAFETY of 'static: every input slice points into a mapping that lives until exit.
            pool.insert(k, unsafe { std::mem::transmute::<&[u8], &'static [u8]>(s) });
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

/// Order-only key (click IDs: copies of a click are identical, so only their order matters).
/// Short IDs pack as usual; longer ones keep a pointer to their bytes and length: bit 63, then
/// 15 bits of length (all ones: the pointer is to a leaked slice), then the 48-bit address.
#[inline(always)]
pub fn okey(s: &[u8]) -> u64 {
    match pack(s) {
        Some(k) => k,
        None => {
            let st: &'static [u8] = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(s) };
            if s.len() < 0x7FFF {
                OVF | ((s.len() as u64) << 48) | (st.as_ptr() as u64)
            } else {
                let b: &'static &'static [u8] = Box::leak(Box::new(st));
                OVF | (0x7FFFu64 << 48) | (b as *const &'static [u8] as u64)
            }
        }
    }
}

#[inline]
fn okey_bytes(k: u64) -> &'static [u8] {
    let n = ((k >> 48) & 0x7FFF) as usize;
    let p = (k & ((1u64 << 48) - 1)) as usize;
    unsafe {
        if n == 0x7FFF {
            *(p as *const &'static [u8])
        } else {
            std::slice::from_raw_parts(p as *const u8, n)
        }
    }
}

#[inline]
fn short_bytes(k: u64, buf: &mut [u8; 10]) -> usize {
    let mut n = 0;
    let mut sh = 54i32;
    while sh >= 0 {
        let c = ((k >> sh) & 63) as usize;
        if c == 0 {
            break;
        }
        buf[n] = DECODE[c];
        n += 1;
        sh -= 6;
    }
    n
}

/// Byte order of two order-only keys.
#[inline]
pub fn ocmp(a: u64, b: u64) -> Ordering {
    if (a | b) & OVF == 0 {
        return a.cmp(&b);
    }
    let (mut x, mut y) = ([0u8; 10], [0u8; 10]);
    let xa: &[u8] = if a & OVF != 0 { okey_bytes(a) } else { let n = short_bytes(a, &mut x); &x[..n] };
    let yb: &[u8] = if b & OVF != 0 { okey_bytes(b) } else { let n = short_bytes(b, &mut y); &y[..n] };
    xa.cmp(yb)
}

#[inline(always)]
pub fn mix(k: u64) -> u64 {
    let mut x = k ^ (k >> 31);
    x = x.wrapping_mul(0x7FB5_D329_728E_A185);
    x ^= x >> 27;
    x = x.wrapping_mul(0x81DA_DEF4_BC2D_D44D);
    x ^ (x >> 33)
}

/// Bytes of overflow user IDs by key: an open-addressing table filled concurrently while the
/// inputs are parsed and read only after. Slots come in cache lines of four keys then their four
/// values; a full line spills into the next. Key 0 marks a free slot (overflow keys have bit 63).
/// The value packs the address with the length in the top 16 bits (all ones: the address is
/// of a leaked slice). A key that finds no slot within `PROBES` lines goes to a locked side map.
pub struct Pool {
    slots: Vec<u64>, // lines of [4 keys, 4 values], zeroed on allocation
    mask: usize,     // line mask
    spill: Mutex<HashMap<u64, &'static [u8]>>,
}

const PROBES: usize = 16;

impl Pool {
    /// A table of about `want` slots (rounded up to a power of two).
    pub fn new(want: usize) -> Pool {
        let lines = (want / 4).next_power_of_two().max(1 << 10);
        let mut slots: Vec<u64> = Vec::with_capacity(8 * lines + 8);
        slots.resize(8 * lines + 8, 0);
        // line-align the start so each line is one cache line
        Pool { slots, mask: lines - 1, spill: Mutex::new(HashMap::new()) }
    }
    #[inline(always)]
    fn line(&self, i: usize) -> *const AtomicU64 {
        let base = self.slots.as_ptr() as usize;
        let al = (base + 63) & !63;
        (al as *const u64).wrapping_add(8 * i) as *const AtomicU64
    }
    #[inline]
    pub fn insert(&self, k: u64, s: &'static [u8]) {
        let mut i = mix(k) as usize & self.mask;
        for _ in 0..PROBES {
            let l = self.line(i);
            for j in 0..4 {
                let ks = unsafe { &*l.add(j) };
                let cur = ks.load(AO::Relaxed);
                if cur == k {
                    return;
                }
                if cur == 0 {
                    match ks.compare_exchange(0, k, AO::Relaxed, AO::Relaxed) {
                        Ok(_) => {
                            let v = if s.len() < 0xFFFF {
                                ((s.len() as u64) << 48) | s.as_ptr() as u64
                            } else {
                                let b: &'static &'static [u8] = Box::leak(Box::new(s));
                                (0xFFFFu64 << 48) | (b as *const &'static [u8] as u64)
                            };
                            unsafe { &*l.add(4 + j) }.store(v, AO::Relaxed);
                            return;
                        }
                        Err(x) if x == k => return,
                        Err(_) => {}
                    }
                }
            }
            i = (i + 1) & self.mask;
        }
        self.spill.lock().unwrap().insert(k, s);
    }
    pub fn get(&self, k: u64) -> Option<&'static [u8]> {
        let mut i = mix(k) as usize & self.mask;
        for _ in 0..PROBES {
            let l = self.line(i);
            for j in 0..4 {
                let cur = unsafe { &*l.add(j) }.load(AO::Relaxed);
                if cur == k {
                    let v = unsafe { &*l.add(4 + j) }.load(AO::Relaxed);
                    let n = (v >> 48) as usize;
                    let p = (v & ((1u64 << 48) - 1)) as usize;
                    return Some(unsafe {
                        if n == 0xFFFF {
                            *(p as *const &'static [u8])
                        } else {
                            std::slice::from_raw_parts(p as *const u8, n)
                        }
                    });
                }
                if cur == 0 {
                    return None;
                }
            }
            i = (i + 1) & self.mask;
        }
        self.spill.lock().unwrap().get(&k).copied()
    }
}

pub fn decode_into(k: u64, pool: &Pool, out: &mut Vec<u8>) {
    if k & OVF != 0 {
        out.extend_from_slice(pool.get(k).expect("overflow id without bytes"));
        return;
    }
    let mut b = [0u8; 10];
    let n = short_bytes(k, &mut b);
    out.extend_from_slice(&b[..n]);
}

pub fn cmp_keys(a: u64, b: u64, pool: &Pool) -> Ordering {
    if (a | b) & OVF == 0 {
        return a.cmp(&b);
    }
    let (mut x, mut y) = ([0u8; 10], [0u8; 10]);
    let xa: &[u8] = if a & OVF != 0 { pool.get(a).expect("overflow id without bytes") } else { let n = short_bytes(a, &mut x); &x[..n] };
    let yb: &[u8] = if b & OVF != 0 { pool.get(b).expect("overflow id without bytes") } else { let n = short_bytes(b, &mut y); &y[..n] };
    xa.cmp(yb)
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
        let pool = Pool::new(16);
        let mut v = Vec::new();
        decode_into(pack(b"u1aZ_9").unwrap(), &pool, &mut v);
        assert_eq!(v, b"u1aZ_9");
        assert!(pack(b" u1").is_none());
        assert!(pack(b"abcdefghijk").is_none());
    }

    #[test]
    fn simd_matches_slow() {
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alpha = b"0123456789AZaz_ u\x22-~@[`{/:\x00\xff";
        let mut buf = vec![0u8; 8192];
        for _ in 0..3_000_000 {
            let n = (rnd() % 13) as usize;
            let off = (rnd() % 8100) as usize;
            for i in 0..n {
                let r = rnd();
                buf[off + i] = if r % 4 == 0 { alpha[(r as usize >> 3) % alpha.len()] } else { b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_"[(r as usize >> 8) % 63] };
            }
            let sl = &buf[off..off + n];
            assert_eq!(pack(sl), pack_slow(sl), "{:?}", sl);
        }
    }
}
