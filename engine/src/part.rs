//! Partitioned output with software write-combining: each partition has a staging buffer of whole
//! cache lines, flushed to the partition's region with non-temporal stores (no read-for-ownership).
//! Partitions that outgrow their region continue in an overflow Vec. Any record size works: the
//! staging buffer is the smallest whole number of lines (at least two) that holds whole records.

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Line([u8; 64]);

pub struct Parts<T: Copy> {
    k: usize,          // records per staging buffer
    nl: usize,         // lines per staging buffer
    cap: usize,        // records per partition region (multiple of k)
    region: Vec<Line>, // nparts * cap * size / 64 lines
    count: Vec<u32>,   // records flushed into each region
    wc: Vec<Line>,     // staging lines, nl per partition
    wcn: Vec<u8>,      // records staged per partition
    pub overflow: Vec<Vec<T>>,
    _t: std::marker::PhantomData<T>,
}

const fn gcd(a: usize, b: usize) -> usize {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

impl<T: Copy> Parts<T> {
    pub fn new(nparts: usize, cap_hint: usize) -> Self {
        let sz = std::mem::size_of::<T>();
        assert!(sz > 0 && sz <= 64 && std::mem::align_of::<T>() <= 64);
        let l0 = sz / gcd(sz, 64);
        let nl = l0 * ((2 + l0 - 1) / l0);
        let k = nl * 64 / sz;
        assert!(k <= 255);
        let cap = ((cap_hint + k - 1) / k * k).max(k);
        let lines = nparts * cap * sz / 64;
        let mut region = Vec::with_capacity(lines);
        unsafe { region.set_len(lines) }; // filled only by flushes; never read past `count`
        Parts {
            k,
            nl,
            cap,
            region,
            count: vec![0; nparts],
            wc: vec![Line([0; 64]); nparts * nl],
            wcn: vec![0; nparts],
            overflow: (0..nparts).map(|_| Vec::new()).collect(),
            _t: std::marker::PhantomData,
        }
    }

    #[inline(always)]
    pub fn push(&mut self, p: usize, r: T) {
        unsafe {
            let n = *self.wcn.get_unchecked(p) as usize;
            let st = self.wc.as_mut_ptr().add(p * self.nl) as *mut T;
            st.add(n).write(r);
            if n + 1 < self.k {
                *self.wcn.get_unchecked_mut(p) = (n + 1) as u8;
                return;
            }
            *self.wcn.get_unchecked_mut(p) = 0;
            self.flush(p);
        }
    }

    #[inline(always)]
    unsafe fn flush(&mut self, p: usize) {
        let c = *self.count.get_unchecked(p) as usize;
        let src = self.wc.as_ptr().add(p * self.nl) as *const u8;
        if c + self.k <= self.cap {
            let sz = std::mem::size_of::<T>();
            let dst = (self.region.as_mut_ptr() as *mut u8).add((p * self.cap + c) * sz);
            for l in 0..self.nl {
                stream64(dst.add(64 * l), src.add(64 * l));
            }
            *self.count.get_unchecked_mut(p) = (c + self.k) as u32;
        } else {
            let st = src as *const T;
            let o = self.overflow.get_unchecked_mut(p);
            for i in 0..self.k {
                o.push(st.add(i).read());
            }
        }
    }

    /// Moves staged records to the overflow lists and fences the streaming stores.
    pub fn finish(&mut self) {
        for p in 0..self.wcn.len() {
            let n = self.wcn[p] as usize;
            let st = unsafe { self.wc.as_ptr().add(p * self.nl) as *const T };
            for i in 0..n {
                let r = unsafe { st.add(i).read() };
                self.overflow[p].push(r);
            }
            self.wcn[p] = 0;
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
    }

    /// Records of partition p: the region part and the overflow part.
    #[inline(always)]
    pub fn get(&self, p: usize) -> (&[T], &[T]) {
        let c = self.count[p] as usize;
        let a = unsafe { std::slice::from_raw_parts((self.region.as_ptr() as *const T).add(p * self.cap), c) };
        (a, &self.overflow[p])
    }

    #[inline(always)]
    pub fn len(&self, p: usize) -> usize {
        self.count[p] as usize + self.overflow[p].len()
    }
}

#[inline(always)]
unsafe fn stream64(dst: *mut u8, src: *const u8) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        let a = _mm256_load_si256(src as *const __m256i);
        let b = _mm256_load_si256(src.add(32) as *const __m256i);
        _mm256_stream_si256(dst as *mut __m256i, a);
        _mm256_stream_si256(dst.add(32) as *mut __m256i, b);
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    std::ptr::copy_nonoverlapping(src, dst, 64);
}

unsafe impl<T: Copy + Send> Send for Parts<T> {}
unsafe impl<T: Copy + Sync> Sync for Parts<T> {}
