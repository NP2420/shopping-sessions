//! Structural CSV scanner: finds newlines, commas and quotes 64 bytes at a time (AVX2), and hands
//! each data line that starts inside a chunk to a callback as N field slices, unquoted.
//!
//! Field count is exact without looking at quotes, because no value holds a comma. Quote validity:
//! a line is fine iff every quote in it is the first or last byte of a field that both starts and
//! ends with a quote, so counting the quotes in the line and the quotes stripped is enough.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[inline(always)]
fn masks(p: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        let a = _mm256_loadu_si256(p.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(p.as_ptr().add(32) as *const __m256i);
        let nl = _mm256_set1_epi8(b'\n' as i8);
        let cm = _mm256_set1_epi8(b',' as i8);
        let qt = _mm256_set1_epi8(b'"' as i8);
        let m = |x: __m256i, y: __m256i| -> u64 {
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, y)) as u32 as u64
        };
        let n = m(a, nl) | (m(b, nl) << 32);
        let c = m(a, cm) | (m(b, cm) << 32);
        let q = m(a, qt) | (m(b, qt) << 32);
        (n, c, q)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let (mut n, mut c, mut q) = (0u64, 0u64, 0u64);
        for (i, &b) in p.iter().enumerate() {
            n |= ((b == b'\n') as u64) << i;
            c |= ((b == b',') as u64) << i;
            q |= ((b == b'"') as u64) << i;
        }
        (n, c, q)
    }
}

#[inline(always)]
fn bits_from(i: u32) -> u64 {
    if i >= 64 {
        0
    } else {
        !0u64 << i
    }
}

/// Unquote fields in place, without branching per field; `q` is the number of quotes in the line.
#[inline(always)]
fn unquote<const N: usize>(f: &mut [&[u8]; N], q: u32) -> bool {
    let mut stripped = 0u32;
    for x in f.iter_mut() {
        let l = x.len();
        let p = x.as_ptr();
        // first and last byte; for an empty field both reads are of byte 0 of a valid line or
        // the separator after it, and the length test below rejects it anyway
        let (a, z) = if l == 0 { (0u8, 0u8) } else { unsafe { (*p, *p.add(l - 1)) } };
        let w = ((a == b'"') & (z == b'"') & (l >= 2)) as usize;
        *x = unsafe { std::slice::from_raw_parts(p.add(w), l - 2 * w) };
        stripped += 2 * w as u32;
    }
    stripped == q
}

/// Calls `f` for every well-formed data line starting in [start, end) of `d`.
/// The first line of the file (start == 0) is the header and is skipped.
#[inline(always)]
pub fn lines<const N: usize>(d: &'static [u8], start: usize, end: usize, mut f: impl FnMut(&mut [&'static [u8]; N])) {
    let n = d.len();
    let mut ls = if start == 0 {
        memchr::memchr(b'\n', d).map_or(n, |i| i + 1)
    } else {
        memchr::memchr(b'\n', &d[start - 1..]).map_or(n, |i| start + i)
    };
    if ls >= end {
        return;
    }
    let mut seps = [0usize; N];
    let mut nf = 0usize;
    let mut qcount = 0u32;
    let mut base = ls;
    let mut pad;
    loop {
        let (nlm, cm, qm) = if base + 64 <= n {
            masks(unsafe { &*(d.as_ptr().add(base) as *const [u8; 64]) })
        } else {
            let rem = n - base;
            pad = [0u8; 64];
            pad[..rem].copy_from_slice(&d[base..]);
            let (a, b, c) = masks(&pad);
            // a virtual newline at end of file ends a last line that has none
            let v = if rem < 64 { 1u64 << rem } else { 0 };
            (a | v, b, c)
        };
        let mut lsb = ls.saturating_sub(base) as u32; // line start bit in this block (0 if earlier)
        let mut bits = nlm | cm;
        while bits != 0 {
            let i = bits.trailing_zeros();
            bits &= bits - 1;
            let p = base + i as usize;
            if (cm >> i) & 1 != 0 {
                if nf < N - 1 {
                    seps[nf] = p;
                }
                nf += 1;
                continue;
            }
            // newline at p: the line is [ls, p)
            let q = qcount + (qm & bits_from(lsb) & !bits_from(i)).count_ones();
            if nf == N - 1 {
                let mut fl: [&'static [u8]; N] = [&[]; N];
                let mut a = ls;
                for k in 0..N - 1 {
                    fl[k] = unsafe { d.get_unchecked(a..seps[k]) };
                    a = seps[k] + 1;
                }
                fl[N - 1] = unsafe { d.get_unchecked(a..p.min(n)) };
                if q == 0 || unquote(&mut fl, q) {
                    f(&mut fl);
                }
            }
            ls = p + 1;
            nf = 0;
            qcount = 0;
            lsb = i + 1;
            if ls >= end {
                return;
            }
        }
        qcount += (qm & bits_from(lsb)).count_ones();
        base += 64;
        if base >= n + 1 {
            return;
        }
    }
}
