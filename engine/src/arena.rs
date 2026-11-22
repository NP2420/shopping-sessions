//! Global allocator: large allocations come from a per-thread bump pointer inside one big
//! anonymous region advised for transparent huge pages; small ones go to the system allocator.
//!
//! The process is short-lived and exits with `_exit`, so large blocks are never reused: `dealloc`
//! is a no-op for them, and `realloc` grows in place when the block is the thread's latest.
//! Fresh region memory is already zero, so `alloc_zeroed` costs nothing extra.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

const BIG: usize = 64 << 10;
const REGION: usize = 1 << 37; // 128 GiB of address space, reserved lazily
const CHUNK: usize = 64 << 20;

static BASE: AtomicUsize = AtomicUsize::new(0);
static NEXT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static CUR: Cell<(usize, usize)> = const { Cell::new((0, 0)) }; // (next free, end)
}

pub struct Arena;

#[inline(always)]
fn base() -> usize {
    let b = BASE.load(Ordering::Acquire);
    if b != 0 {
        return b;
    }
    init()
}

#[cold]
fn init() -> usize {
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            REGION,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        if p == libc::MAP_FAILED {
            return 0;
        }
        // align the usable start to 2 MiB so huge pages line up
        let start = (p as usize + (2 << 20) - 1) & !((2 << 20) - 1);
        let _ = libc::MADV_HUGEPAGE; // experiment: no huge-page advice
        if false { libc::madvise(start as *mut libc::c_void, REGION - (start - p as usize), libc::MADV_HUGEPAGE); }
        match BASE.compare_exchange(0, start, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => start,
            Err(other) => {
                libc::munmap(p, REGION);
                other
            }
        }
    }
}

#[inline(always)]
fn in_region(p: usize) -> bool {
    let b = BASE.load(Ordering::Relaxed);
    b != 0 && p >= b && p < b + REGION - (2 << 20)
}

#[inline(always)]
unsafe fn bump(size: usize, align: usize) -> *mut u8 {
    let b = base();
    if b == 0 {
        return std::ptr::null_mut();
    }
    CUR.with(|c| {
        let (mut next, mut end) = c.get();
        let mut at = (next + align - 1) & !(align - 1);
        if next == 0 || at + size > end {
            let want = (size + align + CHUNK - 1) & !(CHUNK - 1);
            let off = NEXT.fetch_add(want, Ordering::Relaxed);
            if off + want > REGION - (2 << 20) {
                return std::ptr::null_mut();
            }
            next = b + off;
            end = next + want;
            at = (next + align - 1) & !(align - 1);
        }
        c.set((at + size, end));
        at as *mut u8
    })
}

unsafe impl GlobalAlloc for Arena {
    #[inline]
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if l.size() < BIG {
            return System.alloc(l);
        }
        let p = bump(l.size(), l.align().max(64));
        if p.is_null() {
            System.alloc(l)
        } else {
            p
        }
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if l.size() < BIG {
            return System.alloc_zeroed(l);
        }
        let p = bump(l.size(), l.align().max(64));
        if p.is_null() {
            System.alloc_zeroed(l)
        } else {
            p // never-used region memory is zero
        }
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if !in_region(p as usize) {
            System.dealloc(p, l);
        }
    }
    #[inline]
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let pu = p as usize;
        if in_region(pu) {
            // grow in place if this is the thread's most recent block and the chunk has room
            let grown = CUR.with(|c| {
                let (next, end) = c.get();
                if pu + l.size() == next && pu + new <= end {
                    c.set((pu + new, end));
                    true
                } else {
                    false
                }
            });
            if grown || new <= l.size() {
                return p;
            }
            let q = self.alloc(Layout::from_size_align_unchecked(new, l.align()));
            if !q.is_null() {
                std::ptr::copy_nonoverlapping(p, q, l.size());
            }
            return q;
        }
        if new < BIG {
            return System.realloc(p, l, new);
        }
        let q = self.alloc(Layout::from_size_align_unchecked(new, l.align()));
        if !q.is_null() {
            std::ptr::copy_nonoverlapping(p, q, l.size().min(new));
            System.dealloc(p, l);
        }
        q
    }
}
