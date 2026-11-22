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

/// `unquote` for a line that does not end at end of file: every field is followed by a separator
/// and preceded by one (or the previous line's newline), so the first and last byte of an empty
/// field can be read without a length test; they are separators, never quotes.
#[inline(always)]
fn unquote_inner<const N: usize>(f: &mut [&[u8]; N], q: u32) -> bool {
    let mut stripped = 0u32;
    for x in f.iter_mut() {
        let l = x.len();
        let p = x.as_ptr();
        let (a, z) = unsafe { (*p, *p.wrapping_add(l).wrapping_sub(1)) };
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
                let ok = q == 0 || if p < n { unquote_inner(&mut fl, q) } else { unquote(&mut fl, q) };
                if ok {
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

/// Variant of `lines`: a rolling window of three 64-byte blocks of masks; each line is cut with
/// shifts and bit counts relative to its start, with no loop over separators. Lines longer than
/// 128 bytes (rare) take a scalar path.
#[inline(always)]
pub fn lines3<const N: usize>(d: &'static [u8], start: usize, end: usize, mut f: impl FnMut(&mut [&'static [u8]; N])) {
    let n = d.len();
    let mut ls = if start == 0 {
        memchr::memchr(b'\n', d).map_or(n, |i| i + 1)
    } else {
        memchr::memchr(b'\n', &d[start - 1..]).map_or(n, |i| start + i)
    };
    if ls >= end {
        return;
    }
    // masks of the block at `at`; past the end of the data, a newline at n ends the last line
    let blk = |at: usize| -> (u64, u64, u64) {
        if at + 64 <= n {
            masks(unsafe { &*(d.as_ptr().add(at) as *const [u8; 64]) })
        } else if at <= n {
            let rem = n - at;
            let mut pad = [0u8; 64];
            pad[..rem].copy_from_slice(&d[at..]);
            let (a, b, c) = masks(&pad);
            let v = if rem < 64 { 1u64 << rem } else { 0 };
            (a | v, b, c)
        } else {
            (0, 0, 0)
        }
    };
    let mut base = ls;
    let (mut n0, mut c0, mut q0) = blk(base);
    let (mut n1, mut c1, mut q1) = blk(base + 64);
    let (mut n2, mut c2, mut q2) = blk(base + 128);
    #[inline(always)]
    fn win(a: u64, b: u64, c: u64, off: u32) -> u128 {
        let lo = if off == 0 { a } else { (a >> off) | (b << (64 - off)) };
        let hi = if off == 0 { b } else { (b >> off) | (c << (64 - off)) };
        lo as u128 | ((hi as u128) << 64)
    }
    loop {
        while ls - base >= 64 {
            base += 64;
            (n0, c0, q0) = (n1, c1, q1);
            (n1, c1, q1) = (n2, c2, q2);
            (n2, c2, q2) = blk(base + 128);
        }
        let off = (ls - base) as u32;
        let nl = win(n0, n1, n2, off);
        if nl == 0 {
            // a line of 128 bytes or more: scalar
            let le = memchr::memchr(b'\n', &d[ls..]).map_or(n, |i| ls + i);
            let line = &d[ls..le];
            let mut fl: [&'static [u8]; N] = [&[]; N];
            let mut k = 0;
            let mut a = ls;
            let mut bad = false;
            for (i, &ch) in line.iter().enumerate() {
                if ch == b',' {
                    if k == N - 1 {
                        bad = true;
                        break;
                    }
                    fl[k] = unsafe { d.get_unchecked(a..ls + i) };
                    k += 1;
                    a = ls + i + 1;
                }
            }
            if !bad && k == N - 1 {
                fl[N - 1] = unsafe { d.get_unchecked(a..le) };
                let q = line.iter().filter(|&&ch| ch == b'"').count() as u32;
                if q == 0 || unquote(&mut fl, q) {
                    f(&mut fl);
                }
            }
            ls = le + 1;
            if ls >= end {
                return;
            }
            continue;
        }
        let le = nl.trailing_zeros();
        let below = (1u128 << le) - 1;
        let mut cm = win(c0, c1, c2, off) & below;
        if cm.count_ones() as usize == N - 1 {
            let mut fl: [&'static [u8]; N] = [&[]; N];
            let mut a = ls;
            for k in 0..N - 1 {
                let i = cm.trailing_zeros() as usize;
                cm &= cm - 1;
                fl[k] = unsafe { d.get_unchecked(a..ls + i) };
                a = ls + i + 1;
            }
            let lend = ls + le as usize;
            fl[N - 1] = unsafe { d.get_unchecked(a..lend.min(n)) };
            let q = (win(q0, q1, q2, off) & below).count_ones();
            let ok = q == 0 || if lend < n { unquote_inner(&mut fl, q) } else { unquote(&mut fl, q) };
            if ok {
                f(&mut fl);
            }
        }
        ls += le as usize + 1;
        if ls >= end {
            return;
        }
    }
}
