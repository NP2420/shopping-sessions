//! Dense-ID fast path.
//!
//! When the generator's keys are recovered, an event ID maps to a dense index and a user ID to
//! (person, alias). Events are partitioned by index range; within a range, dedup is a direct slot
//! per index and the winners come out in index order. For the published generator that order is
//! each person's events in time order, so a "simple" person's events are one sorted run and need
//! no hashing, scattering or sorting. Everything is verified, and anything that is not exactly a
//! simple person goes to the generic path:
//!
//! * the person's linked IDs must form one component holding only that person's IDs (no shared
//!   device, no foreign or upper-case ID), and every ID it uses in events and clicks must be one
//!   of them (or, with no links, alias 0 only);
//! * its winners must form exactly one run in index order, non-decreasing in time;
//! * no event of the person may take the generic route (a non-canonical event ID, for instance);
//! * the bucket that owns its run must also own its clicks.

use crate::gen::{self, b36, unscramble, Keys, ALIAS_BITS, M40};
use crate::ids::{self, Pool, EMPTY, OVF};
use crate::part::Parts;
use crate::*;
use std::sync::atomic::{AtomicU8, Ordering};

pub const UFLAG: u64 = 1 << 62; // user field holds a decoded user number, not a packed key

#[derive(Clone, Copy)]
#[repr(C)]
pub struct FRec {
    pub idx: u32, // dense event index; u32::MAX marks an empty slot
    pub ing: u32,
    pub user: u64,
    pub ts: u32, // u32::MAX = invalid copy
    pub pc: u32,
    pub _p: u64,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct FClk {
    pub p: u32,
    pub a: u32,
    pub ts: u32,
    pub camp: u32,
    pub ck: u64,
    pub _p: u64,
}

#[derive(Clone, Copy)]
pub struct Fast {
    pub keys: Keys,
    pub cap: u64,    // dense event indexes below this go to the fast path
    pub ishift: u32, // index bucket = idx >> ishift
    pub nib: usize,
    pub np: usize, // persons below this have dense tables
    pub cshift: u32,
    pub ncb: usize,
}

impl Fast {
    pub fn new(keys: Keys, ev_bytes: usize) -> Fast {
        let cap = (ev_bytes / 60 + 100_000) as u64;
        let ishift = 15;
        let nib = ((cap >> ishift) + 1) as usize;
        let np = ev_bytes / 1500 + 10_000;
        let cshift = 10;
        let ncb = (np >> cshift) + 1;
        Fast { keys, cap, ishift, nib, np, cshift, ncb }
    }

    /// Event index for a canonical event ID, if dense.
    #[inline(always)]
    pub fn event_index(&self, e: &[u8]) -> Option<u32> {
        let o = b36(e, b'e')?;
        let i = unscramble(o, self.keys.event);
        if i < self.cap {
            Some(i as u32)
        } else {
            None
        }
    }

    /// UFLAG | user number for a canonical user ID of a person below `np`.
    #[inline(always)]
    pub fn user_number(&self, u: &[u8]) -> Option<u64> {
        let o = b36(u, b'u')?;
        let n = unscramble(o, self.keys.user);
        if (n >> ALIAS_BITS) < self.np as u64 {
            Some(n)
        } else {
            None
        }
    }

    /// Packed key of the user ID for a decoded number (the original string, re-encoded).
    pub fn user_key(&self, num: u64) -> u64 {
        let mut s = Vec::with_capacity(12);
        gen::b36_write(&mut s, b'u', gen::scramble(num, self.keys.user));
        ids::pack(&s).expect("canonical user ids pack")
    }

    pub fn user_string(&self, num: u64, out: &mut Vec<u8>) {
        gen::b36_write(out, b'u', gen::scramble(num, self.keys.user));
    }
}

pub const COMPLEX: u8 = 1;

/// Per-person facts from the links.
pub struct PTab {
    pub lmask: Vec<u64>, // linked aliases (alias < 64)
    pub lname: Vec<u64>, // the component's name, if linked
    pub flags: Vec<AtomicU8>,
}

impl PTab {
    #[inline(always)]
    pub fn complex(&self, p: usize) -> bool {
        self.flags[p].load(Ordering::Relaxed) & COMPLEX != 0
    }
    #[inline(always)]
    pub fn set_complex(&self, p: usize) {
        if self.flags[p].load(Ordering::Relaxed) & COMPLEX == 0 {
            self.flags[p].fetch_or(COMPLEX, Ordering::Relaxed);
        }
    }
    /// Whether events and clicks by alias `a` stay inside the person's own component.
    #[inline(always)]
    pub fn alias_ok(&self, p: usize, a: u32) -> bool {
        let m = self.lmask[p];
        if m != 0 {
            a < 64 && (m >> a) & 1 != 0
        } else {
            a == 0
        }
    }
}

/// Builds per-person facts from the union-find table: which aliases are linked, the component
/// name, and whether the component holds anything besides that person's own IDs.
pub fn build_ptab(links: &LinkTab, pool: &Pool, fast: &Fast) -> PTab {
    let np = fast.np;
    let mut lmask = vec![0u64; np];
    let mut lname = vec![EMPTY; np];
    let flags: Vec<AtomicU8> = (0..np).map(|_| AtomicU8::new(0)).collect();
    // owner of each component (by name): a person index, or MIXED
    const MIXED: u32 = u32::MAX;
    let cap = links.slots.len().max(2);
    let mask = cap - 1;
    let mut own: Vec<(u64, u32)> = vec![(EMPTY, 0); cap];
    let mut buf = Vec::with_capacity(16);
    let owner_slot = |own: &mut Vec<(u64, u32)>, name: u64| -> usize {
        let mut i = ids::mix(name) as usize & mask;
        loop {
            if own[i].0 == name || own[i].0 == EMPTY {
                return i;
            }
            i = (i + 1) & mask;
        }
    };
    for s in links.slots.iter() {
        let (k, name) = (s[0], s[1]);
        if k == EMPTY {
            continue;
        }
        buf.clear();
        ids::decode_into(k, pool, &mut buf);
        let who = match fast.user_number(&buf) {
            Some(n) => {
                let p = (n >> ALIAS_BITS) as usize;
                let a = (n & ((1 << ALIAS_BITS) - 1)) as u32;
                if a < 64 {
                    if lmask[p] == 0 {
                        lname[p] = name;
                    } else if lname[p] != name {
                        flags[p].store(COMPLEX, Ordering::Relaxed);
                    }
                    lmask[p] |= 1 << a;
                } else {
                    flags[p].store(COMPLEX, Ordering::Relaxed);
                }
                p as u32
            }
            None => MIXED,
        };
        let i = owner_slot(&mut own, name);
        if own[i].0 == EMPTY {
            own[i] = (name, who);
        } else if own[i].1 != who {
            own[i].1 = MIXED;
        }
    }
    for p in 0..np {
        if lmask[p] != 0 {
            let i = owner_slot(&mut own, lname[p]);
            if own[i].1 != p as u32 {
                flags[p].store(COMPLEX, Ordering::Relaxed);
            }
        }
    }
    let _ = M40;
    PTab { lmask, lname, flags }
}

/// One bucket's winners in index order, as runs of one person each.
#[derive(Default)]
pub struct BucketA {
    pub w: Vec<(u32, u32, u32)>, // (ts, pc, alias)
    pub runs: Vec<Run>,
    pub maxp: u32,
}

#[derive(Clone, Copy)]
pub struct Run {
    pub p: u32,
    pub start: u32,
    pub len: u32,
    pub ok: bool,
}

/// Dedup an index bucket by direct slot and cut its winners into runs.
pub fn pass_a(
    b: usize,
    fp: &[Parts<FRec>],
    fast: &Fast,
    pt: &PTab,
    links: &LinkTab,
    shift: u32,
    slots: &mut Vec<FRec>,
    gen_out: &mut Parts<PRec>,
) -> BucketA {
    let size = 1usize << fast.ishift;
    let base = (b << fast.ishift) as u32;
    let empty = FRec { idx: u32::MAX, ing: 0, user: 0, ts: 0, pc: 0, _p: 0 };
    slots.clear();
    slots.resize(size, empty);
    let mut any = false;
    for t in fp {
        let (x1, x2) = t.get(b);
        for r in x1.iter().chain(x2) {
            any = true;
            let s = unsafe { slots.get_unchecked_mut((r.idx - base) as usize) };
            if s.idx == u32::MAX || r.ing < s.ing {
                *s = *r;
            }
        }
    }
    let mut out = BucketA::default();
    if !any {
        return out;
    }
    let amask = (1u64 << ALIAS_BITS) - 1;
    let mut prev_ts = 0u32;
    for s in slots.iter() {
        if s.idx == u32::MAX || s.ts == u32::MAX {
            continue;
        }
        if s.user & UFLAG == 0 {
            let pk = links.get(s.user);
            gen_out.push(bucket(pk, shift), PRec { pk, ts: s.ts, pc: s.pc });
            continue;
        }
        let num = s.user & !UFLAG;
        let p = (num >> ALIAS_BITS) as u32;
        let a = (num & amask) as u32;
        let pos = out.w.len() as u32;
        out.w.push((s.ts, s.pc, a));
        let aok = pt.alias_ok(p as usize, a);
        match out.runs.last_mut() {
            Some(r) if r.p == p && r.start + r.len == pos => {
                r.len += 1;
                r.ok &= aok & (s.ts >= prev_ts);
            }
            _ => out.runs.push(Run { p, start: pos, len: 1, ok: aok }),
        }
        prev_ts = s.ts;
        if p > out.maxp {
            out.maxp = p;
        }
    }
    out
}

/// After every bucket's pass A: each person's home bucket, or COMPLEX; and the click ranges.
pub struct Plan {
    pub home: Vec<u32>,
    pub lo: Vec<u32>, // click person range per bucket, inclusive
    pub hi: Vec<u32>,
}

pub fn classify(a: &[std::sync::OnceLock<BucketA>], fast: &Fast, pt: &PTab) -> Plan {
    let mut home = vec![u32::MAX; fast.np];
    let nib = a.len();
    let mut env = vec![0u32; nib];
    let mut m = 0u32;
    for b in 0..nib {
        if let Some(ba) = a[b].get() {
            for r in &ba.runs {
                let p = r.p as usize;
                if !r.ok || home[p] != u32::MAX {
                    pt.set_complex(p);
                }
                home[p] = b as u32;
            }
            m = m.max(ba.maxp);
        }
        env[b] = m;
    }
    let mut lo = vec![0u32; nib];
    let mut hi = vec![0u32; nib];
    for b in 0..nib {
        lo[b] = if b == 0 { 0 } else { env[b - 1] + 1 };
        hi[b] = if b + 1 == nib { fast.np as u32 - 1 } else { env[b] };
        if b > 0 && env[b - 1] == env[b] && b + 1 != nib {
            // empty range: lo > hi
        }
    }
    // a person's clicks must be handled by its home bucket
    for b in 0..nib {
        if let Some(ba) = a[b].get() {
            for r in &ba.runs {
                if r.p < lo[b] || r.p > hi[b] {
                    pt.set_complex(r.p as usize);
                }
            }
        }
    }
    Plan { home, lo, hi }
}

pub struct BWork {
    pub cl: Vec<(u32, CItem)>,
    pub ccnt: Vec<u32>,
    pub cs: Vec<CItem>,
    pub evs: Vec<u64>,
    pub name: Vec<u8>,
    pub win: Win,
}

/// Pass B for one index bucket: simple persons are finished here; everything else goes generic.
#[allow(clippy::too_many_arguments)]
pub fn pass_b(
    b: usize,
    ba: &BucketA,
    fc: &[Parts<FClk>],
    fast: &Fast,
    pt: &PTab,
    plan: &Plan,
    links: &LinkTab,
    pool: &Pool,
    ctx: &Ctx,
    shift: u32,
    w: &mut BWork,
    acc: &mut Acc,
    uo: &mut UserOut,
    gen_ev: &mut Parts<PRec>,
    gen_cl: &mut Parts<CRec>,
) {
    let (lo, hi) = (plan.lo[b], plan.hi[b]);
    // clicks of persons in [lo, hi]
    w.cl.clear();
    if lo <= hi {
        let simple = |p: u32| !pt.complex(p as usize) && plan.home[p as usize] == b as u32;
        for cb in (lo >> fast.cshift) as usize..=((hi >> fast.cshift) as usize).min(fast.ncb - 1) {
            for t in fc {
                let (x1, x2) = t.get(cb);
                for c in x1.iter().chain(x2) {
                    if c.p < lo || c.p > hi {
                        continue;
                    }
                    if simple(c.p) {
                        w.cl.push((c.p, CItem { ts: c.ts, camp: c.camp, ck: c.ck }));
                    } else {
                        let num = ((c.p as u64) << ALIAS_BITS) | c.a as u64;
                        let pk = links.get(fast.user_key(num));
                        gen_cl.push(bucket(pk, shift), CRec { pk, ck: c.ck, ts: c.ts, camp: c.camp, _p: 0 });
                    }
                }
            }
        }
    }
    // group local clicks by person (counting sort over the range)
    let span = if lo <= hi { (hi - lo + 1) as usize } else { 0 };
    w.ccnt.clear();
    w.ccnt.resize(span + 1, 0);
    for &(p, _) in &w.cl {
        w.ccnt[(p - lo) as usize + 1] += 1;
    }
    for i in 1..w.ccnt.len() {
        w.ccnt[i] += w.ccnt[i - 1];
    }
    w.cs.clear();
    w.cs.resize(w.cl.len(), CItem { ts: 0, camp: 0, ck: 0 });
    {
        let mut pos = w.ccnt.clone();
        for &(p, c) in &w.cl {
            let k = (p - lo) as usize;
            w.cs[pos[k] as usize] = c;
            pos[k] += 1;
        }
    }
    for r in &ba.runs {
        let p = r.p as usize;
        let run = &ba.w[r.start as usize..(r.start + r.len) as usize];
        if pt.complex(p) || plan.home[p] != b as u32 || r.p < lo || r.p > hi {
            for &(ts, pc, a) in run {
                let num = ((p as u64) << ALIAS_BITS) | a as u64;
                let pk = links.get(fast.user_key(num));
                gen_ev.push(bucket(pk, shift), PRec { pk, ts, pc });
            }
            continue;
        }
        w.evs.clear();
        w.evs.extend(run.iter().map(|&(ts, pc, _)| ((ts as u64) << 32) | pc as u64));
        let k = (r.p - lo) as usize;
        let cls = &mut w.cs[w.ccnt[k] as usize..w.ccnt[k + 1] as usize];
        if cls.len() > 1 {
            cls.sort_unstable_by(|x, y| {
                x.ts.cmp(&y.ts).then_with(|| {
                    if (x.ck | y.ck) & OVF == 0 {
                        y.ck.cmp(&x.ck)
                    } else {
                        ids::cmp_keys(y.ck, x.ck, pool)
                    }
                })
            });
        }
        w.name.clear();
        if pt.lmask[p] != 0 {
            ids::decode_into(pt.lname[p], pool, &mut w.name);
        } else {
            fast.user_string((p as u64) << ALIAS_BITS, &mut w.name);
        }
        let cls: &[CItem] = cls;
        sessions(&w.name, &w.evs, cls, ctx, &mut w.win, acc, uo);
    }
}
