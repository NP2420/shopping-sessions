//! Shopping Sessions: native pipeline. See rules.md for the spec.
//!
//! Phases (all parallel over NCPU threads):
//!   L  parse links -> edge lists
//!   E  parse events -> records scattered by hash(event_id); union-find runs alongside
//!   D  dedup each event-id bucket and scatter winners by person; parse clicks by person
//!   P  per person bucket: group, sort, sessionize, attribute, aggregate, write Parquet

mod arena;
mod fast;
mod gen;
mod ids;
mod part;
mod pq;
mod scan;
mod ts;

use ids::{Pool, EMPTY, OVF};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

#[global_allocator]
static ALLOC: arena::Arena = arena::Arena;

const DAY: i64 = ts::DAY_MS;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;

// ---------------------------------------------------------------- input files

fn map_file(path: &Path) -> &'static [u8] {
    use std::os::unix::io::AsRawFd;
    let f = std::fs::File::open(path).unwrap_or_else(|e| die(&format!("open {}: {e}", path.display())));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0) as usize;
    if len == 0 {
        return &[];
    }
    unsafe {
        let p = libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0);
        if p == libc::MAP_FAILED {
            die("mmap failed");
        }
        std::slice::from_raw_parts(p as *const u8, len)
    }
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn die(msg: &str) -> ! {
    eprintln!("fatal: {msg}");
    unsafe { libc::_exit(2) }
}

#[derive(Clone, Copy)]
struct Chunk {
    data: &'static [u8],
    start: usize,
    end: usize,
}

fn chunks_of(files: &[&'static [u8]], size: usize) -> Vec<Chunk> {
    let mut v = Vec::new();
    for &d in files {
        let mut s = 0;
        while s < d.len() {
            let e = (s + size).min(d.len());
            v.push(Chunk { data: d, start: s, end: e });
            s = e;
        }
    }
    v
}

// ---------------------------------------------------------------- CSV dialect

/// Calls `f` on every data line that starts inside [start, end). The first line of a file is the header.
#[inline(always)]
fn for_lines(c: Chunk, mut f: impl FnMut(&[u8])) {
    let d = c.data;
    let n = d.len();
    let mut pos = if c.start == 0 {
        memchr::memchr(b'\n', d).map_or(n, |i| i + 1)
    } else {
        memchr::memchr(b'\n', &d[c.start - 1..]).map_or(n, |i| c.start + i)
    };
    while pos < c.end {
        let le = memchr::memchr(b'\n', &d[pos..]).map_or(n, |i| pos + i);
        f(&d[pos..le]);
        pos = le + 1;
    }
}

/// Splits a line into exactly N fields, unquoting. False for a malformed line.
#[inline(always)]
fn split<'a, const N: usize>(line: &'a [u8], f: &mut [&'a [u8]; N]) -> bool {
    let mut n = 0;
    let mut st = 0;
    let mut quoted = false;
    for (i, &c) in line.iter().enumerate() {
        if c == b',' {
            if n == N - 1 {
                return false;
            }
            f[n] = &line[st..i];
            n += 1;
            st = i + 1;
        } else if c == b'"' {
            quoted = true;
        }
    }
    if n != N - 1 {
        return false;
    }
    f[N - 1] = &line[st..];
    if quoted {
        for x in f.iter_mut() {
            if memchr::memchr(b'"', x).is_some() {
                let l = x.len();
                if l >= 2 && x[0] == b'"' && x[l - 1] == b'"' && memchr::memchr(b'"', &x[1..l - 1]).is_none() {
                    *x = &x[1..l - 1];
                } else {
                    return false;
                }
            }
        }
    }
    true
}

fn read_small(path: &Path, nf: usize) -> Vec<Vec<Vec<u8>>> {
    let d = map_file(path);
    let mut rows = Vec::new();
    for_lines(Chunk { data: d, start: 0, end: d.len() }, |line| {
        if nf == 2 {
            let mut f = [&[][..]; 2];
            if split::<2>(line, &mut f) {
                rows.push(f.iter().map(|x| x.to_vec()).collect());
            }
        }
    });
    rows
}

// ---------------------------------------------------------------- lookups

/// `prefix` followed by 1..=9 ASCII digits with no leading zero.
#[inline(always)]
fn canon_num(s: &[u8], prefix: &[u8]) -> Option<usize> {
    let p = prefix.len();
    if s.len() <= p || s.len() > p + 9 || &s[..p] != prefix || s[p] == b'0' {
        return None;
    }
    let mut v = 0usize;
    for &b in &s[p..] {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as usize;
    }
    Some(v)
}

struct Ctx {
    ws: i64,
    we: i64,
    day0: i64,
    ndays: usize,
    cats: Vec<Vec<u8>>,
    unknown: u8,
    prod_tab: Vec<u8>, // 255 = not in file
    prod_map: HashMap<Vec<u8>, u8>,
    camp_tab: Vec<u32>, // u32::MAX = not in file
    camp_map: HashMap<Vec<u8>, u32>,
    camp_names: Vec<Vec<u8>>,
    camp_chan: Vec<Vec<u8>>,
}

impl Ctx {
    #[inline(always)]
    fn cat_of(&self, p: &[u8]) -> u8 {
        if let Some(n) = canon_num(p, b"p") {
            if n < self.prod_tab.len() {
                let c = self.prod_tab[n];
                return if c == 255 { self.unknown } else { c };
            }
            return self.unknown;
        }
        *self.prod_map.get(p).unwrap_or(&self.unknown)
    }
    #[inline(always)]
    fn camp_of(&self, c: &[u8]) -> Option<u32> {
        if let Some(n) = canon_num(c, b"c_") {
            if n < self.camp_tab.len() && self.camp_tab[n] != u32::MAX {
                return Some(self.camp_tab[n]);
            }
            return None;
        }
        self.camp_map.get(c).copied()
    }
}

fn build_ctx(data: &Path) -> Ctx {
    let man = std::fs::read_to_string(data.join("manifest.json")).unwrap_or_else(|_| die("manifest"));
    let get = |k: &str| -> i64 {
        let i = man.find(&format!("\"{k}\"")).unwrap_or_else(|| die("manifest key"));
        let rest = &man[i + k.len() + 2..];
        let a = rest.find('"').unwrap() + 1;
        let b = a + rest[a..].find('"').unwrap();
        ts::parse(rest[a..b].as_bytes()).unwrap_or_else(|| die("manifest ts"))
    };
    let ws = get("window_start");
    let we = get("window_end");
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;

    // products: one pass collecting (product, raw category index), then sort category names
    let pd = map_file(&data.join("products.csv"));
    let mut raw_cats: Vec<&'static [u8]> = Vec::new();
    let mut rows: Vec<(&'static [u8], u32)> = Vec::with_capacity(pd.len() / 14 + 1);
    let mut last: (&[u8], u32) = (&[], u32::MAX);
    let mut cat_map: HashMap<&'static [u8], u32> = HashMap::new();
    scan::lines::<2>(pd, 0, pd.len(), |f| {
        let c = f[1];
        let ci = if last.1 != u32::MAX && last.0 == c {
            last.1
        } else {
            let ci = if raw_cats.len() < 64 {
                match raw_cats.iter().position(|&x| x == c) {
                    Some(i) => i as u32,
                    None => {
                        raw_cats.push(c);
                        cat_map.insert(c, (raw_cats.len() - 1) as u32);
                        (raw_cats.len() - 1) as u32
                    }
                }
            } else {
                *cat_map.entry(c).or_insert_with(|| {
                    raw_cats.push(c);
                    (raw_cats.len() - 1) as u32
                })
            };
            last = (c, ci);
            ci
        };
        rows.push((f[0], ci));
    });
    let mut cats: Vec<Vec<u8>> = raw_cats.iter().map(|c| c.to_vec()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    if cats.len() > 255 {
        die("more than 254 categories");
    }
    let remap: Vec<u8> = raw_cats.iter().map(|c| cats.binary_search_by(|x| x.as_slice().cmp(c)).unwrap() as u8).collect();
    let unknown = cats.binary_search(&b"unknown".to_vec()).unwrap() as u8;
    let mut prod_tab = Vec::new();
    let mut prod_map = HashMap::new();
    for &(pid, ci) in &rows {
        if pid.is_empty() {
            continue;
        }
        let c = remap[ci as usize];
        if let Some(n) = canon_num(pid, b"p") {
            if n < 50_000_000 {
                if prod_tab.len() <= n {
                    prod_tab.resize((n + 1).max(prod_tab.len() * 2).min(50_000_000), 255u8);
                }
                prod_tab[n] = c;
                continue;
            }
        }
        prod_map.insert(pid.to_vec(), c);
    }

    let crows = read_small(&data.join("campaigns.csv"), 2);
    let mut camp_tab = Vec::new();
    let mut camp_map = HashMap::new();
    let mut camp_names = Vec::new();
    let mut camp_chan = Vec::new();
    for r in &crows {
        if r[0].is_empty() {
            continue;
        }
        let id = camp_names.len() as u32;
        camp_names.push(r[0].clone());
        camp_chan.push(r[1].clone());
        if let Some(n) = canon_num(&r[0], b"c_") {
            if n < 50_000_000 {
                if camp_tab.len() <= n {
                    camp_tab.resize(n + 1, u32::MAX);
                }
                camp_tab[n] = id;
                continue;
            }
        }
        camp_map.insert(r[0].clone(), id);
    }
    Ctx { ws, we, day0, ndays, cats, unknown, prod_tab, prod_map, camp_tab, camp_map, camp_names, camp_chan }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
#[repr(C)]
struct EvRec {
    eid: u64,
    user: u64,
    ing: u32, // ingest_ts in epoch seconds
    ts: u32,  // ms since window_start; u32::MAX = invalid copy
    pc: u32,  // price << 8 | category (price 0 for view/cart)
    _p: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct PRec {
    pk: u64,
    ts: u32,
    pc: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct CRec {
    pk: u64,
    ck: u64,
    ts: u32, // ms since window_start - 7 days
    camp: u32,
    _p: u64,
}

#[inline(always)]
fn bucket(k: u64, shift: u32) -> usize {
    (ids::mix(k) >> shift) as usize
}

type Ovf = Vec<(u64, &'static [u8])>;

#[inline(always)]
fn parse_price(p: &[u8]) -> Option<u32> {
    if p.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in p {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            v = 10_000_001; // keep scanning for non-digits; stays too large
        }
    }
    if v >= 1 && v <= 10_000_000 {
        Some(v as u32)
    } else {
        None
    }
}

fn parse_events(c: Chunk, ctx: &Ctx, shift: u32, out: &mut part::Parts<EvRec>, ovf: &mut Ovf, fa: Option<&fast::Fast>, fout: &mut part::Parts<fast::FRec>) {
    parse_events_g::<true>(c, ctx, shift, out, ovf, fa, fout)
}

#[inline(always)]
fn parse_events_g<const PUSH: bool>(
    c: Chunk,
    ctx: &Ctx,
    shift: u32,
    out: &mut part::Parts<EvRec>,
    ovf: &mut Ovf,
    fa: Option<&fast::Fast>,
    fout: &mut part::Parts<fast::FRec>,
) {
    let ws = ctx.ws;
    let we = ctx.we;
    let mut dates = ts::Dates::new();
    scan::lines::<7>(c.data, c.start, c.end, |f| {
        if f[0].is_empty() {
            return;
        }
        let ing_ms = dates.parse(f[3]).unwrap_or(i64::MAX);
        let ing = if ing_ms >= 0 && ing_ms / 1000 < u32::MAX as i64 { (ing_ms / 1000) as u32 } else { u32::MAX };
        let (tok, t) = dates.window(f[2]);
        let ty = event_type(f[4]); // 0 bad, 1 view/cart, 2 purchase
        let mut price = 0u32;
        let mut pok = ty == 1;
        if ty == 2 {
            if let Some(p) = parse_price(f[6]) {
                price = p;
                pok = true;
            }
        }
        let late = (ing_ms != i64::MAX) & (ing_ms.wrapping_sub(t) > DAY);
        let valid = !f[1].is_empty() & !f[5].is_empty() & tok & (t >= ws) & (t < we) & pok & !late;
        let (mut user, mut tsr, mut pc) = (0u64, u32::MAX, 0u32);
        if let Some(fa) = fa {
            if let Some(idx) = fa.event_index(f[0]) {
                if valid {
                    user = match fa.user_number(f[1]) {
                        Some(n) => fast::UFLAG | n,
                        None => ids::key(f[1], ovf),
                    };
                    tsr = (t - ws) as u32;
                    pc = (price << 8) | ctx.cat_of(f[5]) as u32;
                }
                let r = fast::FRec { idx, ing, user, ts: tsr, pc, _p: 0 };
                if PUSH {
                    fout.push((idx >> fa.ishift) as usize, r);
                } else {
                    std::hint::black_box(r);
                }
                return;
            }
        }
        let eid = ids::ident(f[0]);
        if valid {
            user = ids::key(f[1], ovf);
            tsr = (t - ws) as u32;
            pc = (price << 8) | ctx.cat_of(f[5]) as u32;
        }
        let r = EvRec { eid, user, ing, ts: tsr, pc, _p: 0 };
        if PUSH {
            out.push(bucket(eid, shift), r);
        } else {
            std::hint::black_box(r);
        }
    });
}

/// 1 for `view` or `cart`, 2 for `purchase`, 0 for anything else.
#[inline(always)]
fn event_type(t: &[u8]) -> u32 {
    let p = t.as_ptr();
    if t.len() > 8 || (p as usize & 4095) > 4096 - 8 {
        return match t {
            b"view" | b"cart" => 1,
            b"purchase" => 2,
            _ => 0,
        };
    }
    let w = unsafe { std::ptr::read_unaligned(p as *const u64) };
    let n = t.len();
    let m = if n == 8 { !0u64 } else { (1u64 << (8 * n)) - 1 };
    let w = w & m;
    let vc = (n == 4) & ((w == u32::from_le_bytes(*b"view") as u64) | (w == u32::from_le_bytes(*b"cart") as u64));
    let pu = (n == 8) & (w == u64::from_le_bytes(*b"purchase"));
    vc as u32 | ((pu as u32) << 1)
}

fn parse_links(c: Chunk, edges: &mut Vec<(u64, u64)>, ovf: &mut Ovf) {
    scan::lines::<2>(c.data, c.start, c.end, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let a = ids::key(f[0], ovf);
        let b = ids::key(f[1], ovf);
        edges.push((a, b));
    });
}

/// Clicks: dense users go straight to the fast path's person partitions; the rest keep their
/// user key in `tmp` until the links are known (see `resolve_clicks`).
fn parse_clicks(c: Chunk, ctx: &Ctx, ovf: &mut Ovf, tmp: &mut Vec<CRec>, fa: Option<&fast::Fast>, fout: &mut part::Parts<fast::FClk>) {
    let lo = ctx.ws - WEEK;
    let hi = ctx.we;
    let mut dates = ts::Dates::new();
    scan::lines::<4>(c.data, c.start, c.end, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let camp = match ctx.camp_of(f[2]) {
            Some(x) => x,
            None => return,
        };
        let (tok, t) = dates.window(f[3]);
        if !(tok & (t >= lo) & (t < hi)) {
            return;
        }
        let ck = ids::key(f[0], ovf);
        if let Some(fa) = fa {
            if let Some(n) = fa.user_number(f[1]) {
                let p = (n >> gen::ALIAS_BITS) as u32;
                let a = (n & ((1 << gen::ALIAS_BITS) - 1)) as u32;
                fout.push((p >> fa.cshift) as usize, fast::FClk { p, a, ts: (t - lo) as u32, camp, ck, _p: 0 });
                return;
            }
        }
        let uk = ids::key(f[1], ovf);
        tmp.push(CRec { pk: uk, ck, ts: (t - lo) as u32, camp, _p: 0 });
    });
}

/// Maps the user keys of generic clicks to persons and partitions them.
fn resolve_clicks(tmp: &[CRec], links: &LinkTab, shift: u32, out: &mut part::Parts<CRec>) {
    const D: usize = 16;
    for i in 0..tmp.len() {
        if i + D < tmp.len() {
            links.prefetch(tmp[i + D].pk);
        }
        let mut r = tmp[i];
        r.pk = links.get(r.pk);
        out.push(bucket(r.pk, shift), r);
    }
}

// ---------------------------------------------------------------- linked IDs

struct LinkTab {
    pub slots: Vec<[u64; 2]>,
    mask: usize,
}

impl LinkTab {
    #[inline(always)]
    fn get(&self, k: u64) -> u64 {
        let mut i = ids::mix(k) as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s[0] == k {
                return s[1];
            }
            if s[0] == EMPTY {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        let i = ids::mix(k) as usize & self.mask;
        prefetch(unsafe { self.slots.as_ptr().add(i) } as *const u8);
    }
}

#[inline(always)]
fn prefetch(p: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
}

/// Union-find directly in an open-addressing table: a node is its slot. The root of a set is
/// always its smallest ID, so at the end every slot's value becomes its person's name, and the
/// same table serves `get` (an ID not in the table is a person on its own).
fn build_links(edges: &[Vec<(u64, u64)>], pool: &Pool, fa: Option<&fast::Fast>) -> (LinkTab, Option<fast::PTab>) {
    let ne: usize = edges.iter().map(|e| e.len()).sum();
    let mut cap = (ne * 2).next_power_of_two().max(16);
    loop {
        if let Some(t) = try_links(edges, pool, cap, fa) {
            return t;
        }
        cap *= 2;
    }
}

fn try_links(edges: &[Vec<(u64, u64)>], pool: &Pool, cap: usize, fa: Option<&fast::Fast>) -> Option<(LinkTab, Option<fast::PTab>)> {
    let mask = cap - 1;
    let limit = cap / 10 * 9;
    // slot = [key, parent slot]; parent slot index stored in the value word during the build
    let mut sl: Vec<[u64; 2]> = vec![[EMPTY, 0]; cap];
    let mut used = 0usize;
    #[inline(always)]
    fn insert(sl: &mut [[u64; 2]], mask: usize, k: u64, used: &mut usize) -> usize {
        let mut i = ids::mix(k) as usize & mask;
        loop {
            let s = unsafe { sl.get_unchecked_mut(i) };
            if s[0] == k {
                return i;
            }
            if s[0] == EMPTY {
                *s = [k, i as u64];
                *used += 1;
                return i;
            }
            i = (i + 1) & mask;
        }
    }
    #[inline(always)]
    fn find(sl: &mut [[u64; 2]], mut x: usize) -> usize {
        loop {
            let p = sl[x][1] as usize;
            if p == x {
                return x;
            }
            let g = sl[p][1] as usize;
            sl[x][1] = g as u64;
            x = g;
        }
    }
    const D: usize = 8;
    for es in edges {
        for (j, &(a, b)) in es.iter().enumerate() {
            if j + D < es.len() {
                let (pa, pb) = es[j + D];
                prefetch(unsafe { sl.as_ptr().add(ids::mix(pa) as usize & mask) } as *const u8);
                prefetch(unsafe { sl.as_ptr().add(ids::mix(pb) as usize & mask) } as *const u8);
            }
            let ia = insert(&mut sl, mask, a, &mut used);
            let ib = insert(&mut sl, mask, b, &mut used);
            if used > limit {
                return None;
            }
            let ra = find(&mut sl, ia);
            let rb = find(&mut sl, ib);
            if ra != rb {
                let (ka, kb) = (sl[ra][0], sl[rb][0]);
                let a_smaller = if (ka | kb) & OVF == 0 { ka < kb } else { ids::cmp_keys(ka, kb, pool).is_lt() };
                if a_smaller {
                    sl[rb][1] = ra as u64;
                } else {
                    sl[ra][1] = rb as u64;
                }
            }
        }
    }
    // resolve every slot to its root, then replace parent indices with names
    for i in 0..cap {
        if sl[i][0] != EMPTY {
            let r = find(&mut sl, i);
            sl[i][1] = r as u64;
        }
    }
    // here every slot's value is its root's slot index: the fast path's person table needs that
    let pt = fa.map(|f| fast::ptab_from_roots(&sl, pool, f));
    for i in 0..cap {
        if sl[i][0] != EMPTY {
            let r = sl[i][1] as usize;
            sl[i][1] = sl[r][0];
        }
    }
    Some((LinkTab { slots: sl, mask }, pt))
}

// ---------------------------------------------------------------- dedup

fn dedup_bucket(
    b: usize,
    ev: &[part::Parts<EvRec>],
    links: &LinkTab,
    shift: u32,
    table: &mut Vec<(u64, *const EvRec)>,
    win: &mut Vec<PRec>,
    out: &mut part::Parts<PRec>,
    fp: Option<(&fast::Fast, &fast::PTab)>,
    pool: &Pool,
    extras: &mut Vec<(u32, u32, u32, u32)>,
) {
    let n: usize = ev.iter().map(|t| t.len(b)).sum();
    if n == 0 {
        return;
    }
    let cap = (n + n / 2).next_power_of_two();
    let mask = cap - 1;
    table.clear();
    table.resize(cap, (EMPTY, std::ptr::null()));
    for t in ev {
      let (v1, v2) = t.get(b);
      for v in [v1, v2] {
        for (j, r) in v.iter().enumerate() {
            if j + 8 < v.len() {
                prefetch(unsafe { table.as_ptr().add(ids::mix(v.get_unchecked(j + 8).eid) as usize & mask) } as *const u8);
            }
            let mut i = (ids::mix(r.eid) as usize) & mask;
            loop {
                let s = unsafe { table.get_unchecked_mut(i) };
                if s.0 == EMPTY {
                    *s = (r.eid, r as *const EvRec);
                    break;
                }
                if s.0 == r.eid {
                    if r.ing < unsafe { (*s.1).ing } {
                        s.1 = r as *const EvRec;
                    }
                    break;
                }
                i = (i + 1) & mask;
            }
        }
      }
    }
    win.clear();
    for s in table.iter() {
        if s.0 == EMPTY {
            continue;
        }
        let r = unsafe { &*s.1 };
        if r.ts == u32::MAX {
            continue;
        }
        if let Some((fa, pt)) = fp {
            // an event of a dense person on this route joins that person's run later
            let mut sb = Vec::new();
            ids::decode_into(r.user, pool, &mut sb);
            if let Some(n) = fa.user_number(&sb) {
                let p = (n >> gen::ALIAS_BITS) as u32;
                let a = (n & ((1 << gen::ALIAS_BITS) - 1)) as u32;
                if !pt.alias_ok(p as usize, a) {
                    pt.set_complex(p as usize);
                }
                extras.push((p, a, r.ts, r.pc));
                continue;
            }
        }
        win.push(PRec { pk: r.user, ts: r.ts, pc: r.pc });
    }
    const D: usize = 16;
    for i in 0..win.len() {
        if i + D < win.len() {
            links.prefetch(win[i + D].pk);
        }
        let mut r = win[i];
        r.pk = links.get(r.pk);
        out.push(bucket(r.pk, shift), r);
    }
}

// ---------------------------------------------------------------- per person

#[derive(Clone, Copy)]
struct CItem {
    ts: u32,
    camp: u32,
    ck: u64,
}

struct Acc {
    // campaign_daily accumulators, (ncamp + 1) x ndays: sessions, purchasing, revenue, assisted
    camp: Vec<[i64; 4]>,
    ndays: usize,
}

#[derive(Default)]
struct UserOut {
    rows: usize,
    uid: pq::Bytes,
    day: pq::Bytes,
    ses: pq::Bytes,
    evn: pq::Bytes,
    rev: pq::Bytes,
    topd: Vec<u8>,
    topv: pq::Bytes,
}

struct Work {
    tab: Vec<(u64, u32)>,
    lids: Vec<u32>,
    cnt: Vec<u32>,
    off: Vec<u32>,
    pks: Vec<u64>,
    sorted: Vec<u64>,
    clid: Vec<u32>,
    ccnt: Vec<u32>,
    coff: Vec<u32>,
    csorted: Vec<CItem>,
    win: Win,
    name: Vec<u8>,
}

/// Click-window and category scratch for `sessions`.
struct Win {
    wcnt: Vec<u32>,
    wpos: Vec<u32>,
    active: Vec<u32>,
    catsum: Vec<i64>,
}

impl Win {
    fn new(ncamp: usize, ncat: usize) -> Self {
        Win { wcnt: vec![0; ncamp], wpos: vec![0; ncamp], active: Vec::new(), catsum: vec![0; ncat] }
    }
}

impl Work {
    fn new(ncamp: usize, ncat: usize) -> Self {
        Work {
            tab: Vec::new(),
            lids: Vec::new(),
            cnt: Vec::new(),
            off: Vec::new(),
            pks: Vec::new(),
            sorted: Vec::new(),
            clid: Vec::new(),
            ccnt: Vec::new(),
            coff: Vec::new(),
            csorted: Vec::new(),
            win: Win::new(ncamp, ncat),
            name: Vec::new(),
        }
    }
}

fn person_bucket(
    b: usize,
    pev: &[part::Parts<PRec>],
    pcl: &[part::Parts<CRec>],
    ctx: &Ctx,
    pool: &Pool,
    w: &mut Work,
    acc: &mut Acc,
    uo: &mut UserOut,
) {
    let nev: usize = pev.iter().map(|t| t.len(b)).sum();
    if nev == 0 {
        return;
    }
    // people per bucket are far fewer than events: start small and grow at half load
    let mut cap = (nev / 8).next_power_of_two().max(64);
    let mut mask = cap - 1;
    w.tab.clear();
    w.tab.resize(cap, (EMPTY, 0));
    w.lids.clear();
    w.cnt.clear();
    w.pks.clear();
    for t in pev {
        let (x1, x2) = t.get(b);
        for r in x1.iter().chain(x2) {
            let mut i = ids::mix(r.pk) as usize & mask;
            let lid = loop {
                let s = unsafe { w.tab.get_unchecked_mut(i) };
                if s.0 == r.pk {
                    break s.1;
                }
                if s.0 == EMPTY {
                    let l = w.pks.len() as u32;
                    *s = (r.pk, l);
                    w.pks.push(r.pk);
                    w.cnt.push(0);
                    if w.pks.len() * 2 > cap {
                        cap *= 2;
                        mask = cap - 1;
                        w.tab.clear();
                        w.tab.resize(cap, (EMPTY, 0));
                        for (l2, &k) in w.pks.iter().enumerate() {
                            let mut j = ids::mix(k) as usize & mask;
                            while w.tab[j].0 != EMPTY {
                                j = (j + 1) & mask;
                            }
                            w.tab[j] = (k, l2 as u32);
                        }
                    }
                    break l;
                }
                i = (i + 1) & mask;
            };
            w.lids.push(lid);
            w.cnt[lid as usize] += 1;
        }
    }
    let np = w.pks.len();
    w.off.clear();
    w.off.reserve(np + 1);
    let mut s = 0u32;
    for &c in &w.cnt {
        w.off.push(s);
        s += c;
    }
    w.off.push(s);
    w.sorted.clear();
    w.sorted.resize(nev, 0);
    {
        let mut pos: Vec<u32> = w.off[..np].to_vec();
        let mut j = 0;
        for t in pev {
            let (x1, x2) = t.get(b);
            for r in x1.iter().chain(x2) {
                let l = w.lids[j] as usize;
                j += 1;
                w.sorted[pos[l] as usize] = ((r.ts as u64) << 32) | r.pc as u64;
                pos[l] += 1;
            }
        }
    }
    // clicks of the same people
    w.clid.clear();
    w.ccnt.clear();
    w.ccnt.resize(np, 0);
    for t in pcl {
        let (x1, x2) = t.get(b);
        for c in x1.iter().chain(x2) {
            let mut i = ids::mix(c.pk) as usize & mask;
            let lid = loop {
                let s = unsafe { w.tab.get_unchecked(i) };
                if s.0 == c.pk {
                    break s.1;
                }
                if s.0 == EMPTY {
                    break u32::MAX;
                }
                i = (i + 1) & mask;
            };
            w.clid.push(lid);
            if lid != u32::MAX {
                w.ccnt[lid as usize] += 1;
            }
        }
    }
    w.coff.clear();
    let mut s = 0u32;
    for &c in &w.ccnt {
        w.coff.push(s);
        s += c;
    }
    w.coff.push(s);
    w.csorted.clear();
    w.csorted.resize(s as usize, CItem { ts: 0, camp: 0, ck: 0 });
    {
        let mut pos: Vec<u32> = w.coff[..np].to_vec();
        let mut j = 0;
        for t in pcl {
            let (x1, x2) = t.get(b);
            for c in x1.iter().chain(x2) {
                let l = w.clid[j];
                j += 1;
                if l == u32::MAX {
                    continue;
                }
                w.csorted[pos[l as usize] as usize] = CItem { ts: c.ts, camp: c.camp, ck: c.ck };
                pos[l as usize] += 1;
            }
        }
    }
    for p in 0..np {
        let (a, z) = (w.off[p] as usize, w.off[p + 1] as usize);
        let evs = &mut w.sorted[a..z];
        evs.sort_unstable();
        let (ca, cz) = (w.coff[p] as usize, w.coff[p + 1] as usize);
        let cls = &mut w.csorted[ca..cz];
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
        ids::decode_into(w.pks[p], pool, &mut w.name);
        sessions(&w.name, evs, cls, ctx, &mut w.win, acc, uo);
    }
}

/// Sessions, attribution and aggregation for one person: `evs` holds (ts << 32 | price << 8 | cat)
/// sorted by time, `cls` the person's clicks sorted by (ts, click_id descending).
#[inline]
fn sessions(name: &[u8], evs: &[u64], cls: &[CItem], ctx: &Ctx, win: &mut Win, acc: &mut Acc, uo: &mut UserOut) {
    let organic = ctx.camp_names.len();
    let ndays = acc.ndays;
    // sessions
    let mut lo = 0usize;
    let mut hi = 0usize;
    win.active.clear();
    let mut cur_day = i64::MIN;
    let (mut d_ses, mut d_ev, mut d_rev) = (0i64, 0i64, 0i64);
    let mut d_any = false;
    let mut i = 0;
    let n = evs.len();
    while i <= n {
        // flush a finished day, or the last one
        let start = if i < n { (evs[i] >> 32) as i64 } else { 0 };
        let day = if i < n { (ctx.ws + start).div_euclid(DAY) } else { i64::MAX };
        if day != cur_day && cur_day != i64::MIN {
            uo.rows += 1;
            uo.uid.str(name);
            uo.day.i32(cur_day as i32);
            uo.ses.i32(d_ses as i32);
            uo.evn.i32(d_ev as i32);
            uo.rev.i64(d_rev);
            if d_any {
                let mut best = 0usize;
                let mut bv = -1i64;
                for (ci, v) in win.catsum.iter_mut().enumerate() {
                    if *v > bv && *v > 0 {
                        bv = *v;
                        best = ci;
                    }
                    *v = 0;
                }
                uo.topd.push(1);
                uo.topv.str(&ctx.cats[best]);
            } else {
                uo.topd.push(0);
            }
            d_ses = 0;
            d_ev = 0;
            d_rev = 0;
            d_any = false;
        }
        if i == n {
            break;
        }
        cur_day = day;
        // one session
        let mut j = i + 1;
        let mut prev = start;
        let mut rev = 0i64;
        let mut bought = false;
        {
            let pc = evs[i] as u32;
            let price = (pc >> 8) as i64;
            if price > 0 {
                rev += price;
                bought = true;
                win.catsum[(pc & 255) as usize] += price;
            }
        }
        while j < n {
            let t = (evs[j] >> 32) as i64;
            if t - prev > GAP {
                break;
            }
            prev = t;
            let pc = evs[j] as u32;
            let price = (pc >> 8) as i64;
            if price > 0 {
                rev += price;
                bought = true;
                win.catsum[(pc & 255) as usize] += price;
            }
            j += 1;
        }
        d_ses += 1;
        d_ev += (j - i) as i64;
        d_rev += rev;
        d_any |= bought;
        // attribution: click times are relative to window_start - 7 days
        let sc = start + WEEK;
        while hi < cls.len() && (cls[hi].ts as i64) <= sc {
            let c = cls[hi].camp as usize;
            if win.wcnt[c] == 0 {
                win.wpos[c] = win.active.len() as u32;
                win.active.push(c as u32);
            }
            win.wcnt[c] += 1;
            hi += 1;
        }
        while lo < hi && (cls[lo].ts as i64) < sc - WEEK {
            let c = cls[lo].camp as usize;
            win.wcnt[c] -= 1;
            if win.wcnt[c] == 0 {
                let p = win.wpos[c] as usize;
                let last = win.active.pop().unwrap();
                if (last as usize) != c {
                    win.active[p] = last;
                    win.wpos[last as usize] = p as u32;
                }
            }
            lo += 1;
        }
        let di = (day - ctx.day0) as usize;
        let credited = if hi > lo { cls[hi - 1].camp as usize } else { organic };
        let a = &mut acc.camp[credited * ndays + di];
        a[0] += 1;
        a[1] += bought as i64;
        a[2] += rev;
        for &c in &win.active {
            acc.camp[c as usize * ndays + di][3] += 1;
        }
        i = j;
    }
    // reset the click window for the next person
    for &c in &win.active {
        win.wcnt[c as usize] = 0;
    }
    win.active.clear();
}

fn user_cols(uo: &UserOut) -> Vec<u8> {
    use pq::{Col, Logical::{Date, Plain, Str}, T_BYTE_ARRAY, T_INT32, T_INT64};
    let cols = [
        Col { name: "user_id", ptype: T_BYTE_ARRAY, logical: Str, optional: false, defs: None, values: &uo.uid.0 },
        Col { name: "day", ptype: T_INT32, logical: Date, optional: false, defs: None, values: &uo.day.0 },
        Col { name: "sessions", ptype: T_INT32, logical: Plain, optional: false, defs: None, values: &uo.ses.0 },
        Col { name: "events", ptype: T_INT32, logical: Plain, optional: false, defs: None, values: &uo.evn.0 },
        Col { name: "revenue_cents", ptype: T_INT64, logical: Plain, optional: false, defs: None, values: &uo.rev.0 },
        Col { name: "top_category", ptype: T_BYTE_ARRAY, logical: Str, optional: true, defs: Some(&uo.topd), values: &uo.topv.0 },
    ];
    pq::encode(uo.rows, &cols)
}

fn write_file(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap_or_else(|e| die(&format!("write {}: {e}", path.display())));
}

// ---------------------------------------------------------------- main

#[inline(never)]
fn bench(mode: &str, files: &[&'static [u8]], ctx: &Ctx) -> ! {
    // single-threaded micro benchmark over all event files (pages pre-touched)
    let mut sum = 0u64;
    for d in files {
        for i in (0..d.len()).step_by(4096) {
            sum = sum.wrapping_add(d[i] as u64);
        }
    }
    let reps: usize = std::env::var("SS_REPS").ok().and_then(|v| v.parse().ok()).unwrap_or(5);
    let mut best = f64::MAX;
    let mut lines = 0u64;
    for _ in 0..reps {
    let t = std::time::Instant::now();
    lines = 0;
    let mut o = Ovf::new();
    let mut b = part::Parts::<EvRec>::new(512, 50_000);
    let mut fdummy = part::Parts::<fast::FRec>::new(1, 4);
    let fk = recover_keys(files).map(|k| fast::Fast::new(k, files.iter().map(|d| d.len()).sum()));
    let mut fb = part::Parts::<fast::FRec>::new(fk.map_or(1, |f| f.nib), 40_000);
    for &d in files {
        let c = Chunk { data: d, start: 0, end: d.len() };
        match mode {
            "scan" => scan::lines::<7>(d, 0, d.len(), |f| {
                lines += 1;
                sum = sum.wrapping_add(f[2].len() as u64);
            }),
            "keys" => {
                if lines == 0 {
                    let t0 = std::time::Instant::now();
                    let k = recover_keys(files);
                    eprintln!("keys {:?} in {:.2} ms", k, t0.elapsed().as_secs_f64() * 1e3);
                }
                lines += 1;
            }
            "ts" => scan::lines::<7>(d, 0, d.len(), |f| {
                lines += 1;
                sum = sum.wrapping_add(ts::parse(f[2]).unwrap_or(0) as u64 ^ ts::parse(f[3]).unwrap_or(0) as u64);
            }),
            "ids" => scan::lines::<7>(d, 0, d.len(), |f| {
                lines += 1;
                if !f[0].is_empty() && !f[1].is_empty() {
                    sum = sum.wrapping_add(ids::ident(f[0]) ^ ids::key(f[1], &mut o));
                }
            }),
            "prod" => scan::lines::<7>(d, 0, d.len(), |f| {
                lines += 1;
                sum = sum.wrapping_add(ctx.cat_of(f[5]) as u64 + parse_price(f[6]).unwrap_or(0) as u64 + (f[4] == b"view") as u64);
            }),
            "nopush" => {
                parse_events_g::<false>(c, ctx, 64 - 9, &mut b, &mut o, None, &mut fdummy);
                lines += 1;
            }
            "fparse" => {
                parse_events(c, ctx, 64 - 9, &mut b, &mut o, fk.as_ref(), &mut fb);
            }
            "parse" => {
                parse_events(c, ctx, 64 - 9, &mut b, &mut o, None, &mut fdummy);
            }
            _ => {}
        }
    }
    if mode == "fparse" {
        fb.finish();
        lines = (0..fk.unwrap().nib).map(|p| fb.len(p) as u64).sum();
    }
    if mode == "parse" {
        b.finish();
        lines = (0..512).map(|p| b.len(p) as u64).sum();
    }
    best = best.min(t.elapsed().as_secs_f64());
    }
    let el = best;
    eprintln!("{mode}: {lines} lines, {:.1} ns/line, {:.2} GB/s ({sum})", el * 1e9 / lines as f64, files.iter().map(|d| d.len()).sum::<usize>() as f64 / el / 1e9);
    unsafe { libc::_exit(0) }
}

/// Samples the start of the largest event files and recovers the generator's keys.
fn recover_keys(files: &[&'static [u8]]) -> Option<gen::Keys> {
    let total: usize = files.iter().map(|d| d.len()).sum();
    let mut big: Vec<&'static [u8]> = files.to_vec();
    big.sort_unstable_by_key(|d| std::cmp::Reverse(d.len()));
    let mut rows = Vec::new();
    for d in big.iter().take(4) {
        let end = d.len().min(1 << 20);
        scan::lines::<7>(d, 0, end, |f| {
            rows.push((f[0], f[1], ts::parse(f[2]).unwrap_or(i64::MIN)));
        });
    }
    let est = (total / 50) as u64 + 10_000;
    gen::recover(&gen::Sample { rows }, est / 30 * 4 + 10_000, est)
}

fn env_or_arg(name: &str, idx: usize, default: &str) -> PathBuf {
    if let Ok(v) = std::env::var(name) {
        return PathBuf::from(v);
    }
    std::env::args().nth(idx).map(PathBuf::from).unwrap_or_else(|| PathBuf::from(default))
}

fn main() {
    let t0 = std::time::Instant::now();
    let verbose = std::env::var_os("SS_VERBOSE").is_some();
    let lap = |what: &str| {
        if verbose {
            eprintln!("{:>8.1} ms  {what}", t0.elapsed().as_secs_f64() * 1e3);
        }
    };
    let data = env_or_arg("DATA_DIR", 1, "/data");
    let out = env_or_arg("OUTPUT_DIR", 2, "/output");
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()));

    ts::init();
    let ctx = build_ctx(&data);
    let ev_files: Vec<&'static [u8]> = csv_files(&data.join("events")).iter().map(|p| map_file(p)).collect();
    let cl_files: Vec<&'static [u8]> = csv_files(&data.join("clicks")).iter().map(|p| map_file(p)).collect();
    let ln_files: Vec<&'static [u8]> = csv_files(&data.join("links")).iter().map(|p| map_file(p)).collect();
    let ev_bytes: usize = ev_files.iter().map(|d| d.len()).sum();
    if let Ok(mode) = std::env::var("SS_BENCH") {
        bench(&mode, &ev_files, &ctx);
    }
    let all_bytes = ev_bytes + cl_files.iter().map(|d| d.len()).sum::<usize>();
    let csize = (all_bytes / (nt * 16)).clamp(256 << 10, 4 << 20);
    let ev_chunks = chunks_of(&ev_files, csize);
    let cl_chunks = chunks_of(&cl_files, csize);
    let ln_chunks = chunks_of(&ln_files, csize);
    let est_events = ev_bytes / 80 + 1;
    let nb = (est_events / 40_000).next_power_of_two().clamp(64, 2048);
    let shift = 64 - nb.trailing_zeros();
    let per = est_events / nt / nb * 5 / 4 + 32;
    lap("setup");

    // L: links
    let next = AtomicUsize::new(0);
    let link_parts: Vec<(Vec<(u64, u64)>, Ovf)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut e = Vec::new();
                    let mut o = Ovf::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= ln_chunks.len() {
                            break;
                        }
                        parse_links(ln_chunks[i], &mut e, &mut o);
                    }
                    (e, o)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut pool = Pool::default();
    for (_, o) in &link_parts {
        pool.extend(o.iter().copied());
    }
    let edges: Vec<Vec<(u64, u64)>> = link_parts.into_iter().map(|(e, _)| e).collect();
    lap("links parsed");

    // keys of the generator, for the dense-ID fast path (SS_GENERIC=1 turns it off)
    let fa: Option<fast::Fast> = if std::env::var_os("SS_GENERIC").is_some() {
        None
    } else {
        recover_keys(&ev_files).map(|k| fast::Fast::new(k, ev_bytes))
    };
    lap(if fa.is_some() { "keys recovered" } else { "no keys: generic only" });
    let fa = fa.as_ref();
    let nib = fa.map_or(0, |f| f.nib);
    let ishift = fa.map_or(0, |f| f.ishift);
    let fper = fa.map_or(4, |f| ((1usize << f.ishift) / nt) * 5 / 4 + 64);

    // E: events, with union-find (and the fast path's person table) on its own thread
    let next = AtomicUsize::new(0);
    let gper = if fa.is_some() { per / 8 + 32 } else { per };
    let ncb = fa.map_or(1, |f| f.ncb);
    let cl_bytes: usize = cl_files.iter().map(|d| d.len()).sum();
    let fcper = fa.map_or(4, |f| cl_bytes / 48 / nt / f.ncb * 3 / 2 + 64);
    let ne_tasks = ev_chunks.len() + cl_chunks.len();
    #[allow(clippy::type_complexity)]
    let (ev_parts, (links, ptab)): (Vec<(part::Parts<EvRec>, part::Parts<fast::FRec>, part::Parts<fast::FClk>, Vec<CRec>, Ovf)>, (LinkTab, Option<fast::PTab>)) = std::thread::scope(|s| {
        let uf = s.spawn(|| {
            let t = std::time::Instant::now();
            let r = build_links(&edges, &pool, fa);
            if verbose {
                eprintln!("uf + ptab {:.1} ms", t.elapsed().as_secs_f64() * 1e3);
            }
            r
        });
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut b = part::Parts::<EvRec>::new(nb, gper);
                    let mut fb = part::Parts::<fast::FRec>::new(nib.max(1), fper);
                    let mut fc = part::Parts::<fast::FClk>::new(ncb, fcper);
                    let mut ct = Vec::new();
                    let mut o = Ovf::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= ne_tasks {
                            break;
                        }
                        if i < ev_chunks.len() {
                            parse_events(ev_chunks[i], &ctx, shift, &mut b, &mut o, fa, &mut fb);
                        } else {
                            parse_clicks(cl_chunks[i - ev_chunks.len()], &ctx, &mut o, &mut ct, fa, &mut fc);
                        }
                    }
                    b.finish();
                    fb.finish();
                    fc.finish();
                    (b, fb, fc, ct, o)
                })
            })
            .collect();
        let parts = hs.into_iter().map(|h| h.join().unwrap()).collect();
        (parts, uf.join().unwrap())
    });
    lap("events parsed + union-find");
    for (_, _, _, _, o) in &ev_parts {
        pool.extend(o.iter().copied());
    }
    let mut ev: Vec<part::Parts<EvRec>> = Vec::with_capacity(nt);
    let mut fev: Vec<part::Parts<fast::FRec>> = Vec::with_capacity(nt);
    let mut fcl: Vec<part::Parts<fast::FClk>> = Vec::with_capacity(nt);
    let mut ctmp: Vec<Vec<CRec>> = Vec::with_capacity(nt);
    for (a, b, c, d, _) in ev_parts {
        ev.push(a);
        fev.push(b);
        fcl.push(c);
        ctmp.push(d);
    }
    let fp: Option<(&fast::Fast, &fast::PTab)> = fa.zip(ptab.as_ref());

    // D: clicks, generic dedup buckets, fast pass A
    let next = AtomicUsize::new(0);
    let ncl = ctmp.len();
    let ntasks = ncl + nb + nib;
    let pper = est_events / nt / nb + 32;
    let pper = if fa.is_some() { pper / 4 + 32 } else { pper * 3 / 2 };
    let cper = cl_bytes / 48 / nt / nb * 3 / 2 + 32;
    let cper = if fa.is_some() { cper / 4 + 32 } else { cper };
    let bucket_a: Vec<std::sync::OnceLock<fast::BucketA>> = (0..nib).map(|_| std::sync::OnceLock::new()).collect();
    let d_parts: Vec<(part::Parts<PRec>, part::Parts<CRec>, Vec<(u32, u32, u32, u32)>)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut pe = part::Parts::<PRec>::new(nb, pper);
                    let mut pc = part::Parts::<CRec>::new(nb, cper);
                    let mut table = Vec::new();
                    let mut win = Vec::new();
                    let mut slots = Vec::new();
                    let mut ex = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= ntasks {
                            break;
                        }
                        if i < ncl {
                            resolve_clicks(&ctmp[i], &links, shift, &mut pc);
                        } else if i < ncl + nb {
                            dedup_bucket(i - ncl, &ev, &links, shift, &mut table, &mut win, &mut pe, fp, &pool, &mut ex);
                        } else {
                            let b = i - ncl - nb;
                            let (f, pt) = fp.unwrap();
                            let r = fast::pass_a(b, &fev, f, pt, &links, shift, &mut slots, &mut pe);
                            let _ = bucket_a[b].set(r);
                        }
                    }
                    pe.finish();
                    pc.finish();
                    (pe, pc, ex)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("dedup + clicks + pass A");
    let mut pev = Vec::with_capacity(2 * nt + 1);
    let mut pcl = Vec::with_capacity(2 * nt);
    let mut extras: Vec<(u32, u32, u32, u32)> = Vec::new();
    for (a, b, x) in d_parts {
        pev.push(a);
        pcl.push(b);
        extras.extend(x);
    }
    extras.sort_unstable();
    let _ = ishift;

    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap_or_else(|_| die("mkdir user_daily"));
    std::fs::create_dir_all(&cd).unwrap_or_else(|_| die("mkdir campaign_daily"));
    let ncamp = ctx.camp_names.len();

    // B: simple persons straight from index order
    let mut outs: Vec<(UserOut, Acc)> = Vec::with_capacity(nt);
    if let Some((f, pt)) = fp {
        let plan = fast::classify(&bucket_a, f, pt);
        // extras of persons not finished on the fast path go generic
        let mut gx = part::Parts::<PRec>::new(nb, 16);
        extras.retain(|&(p, a, ts, pc)| {
            if pt.complex(p as usize) || plan.home[p as usize] == u32::MAX {
                let pk = links.get(f.user_key(((p as u64) << gen::ALIAS_BITS) | a as u64));
                gx.push(bucket(pk, shift), PRec { pk, ts, pc });
                false
            } else {
                true
            }
        });
        gx.finish();
        pev.push(gx);
        let extras = &extras;
        lap("classified");
        let next = AtomicUsize::new(0);
        let b_parts: Vec<(UserOut, Acc, part::Parts<PRec>, part::Parts<CRec>)> = std::thread::scope(|s| {
            let hs: Vec<_> = (0..nt)
                .map(|_| {
                    let (bucket_a, fcl, plan, links, pool, ctx, next) = (&bucket_a, &fcl, &plan, &links, &pool, &ctx, &next);
                    s.spawn(move || {
                        let mut acc = Acc { camp: vec![[0; 4]; (ncamp + 1) * ctx.ndays], ndays: ctx.ndays };
                        let mut uo = UserOut::default();
                        let mut ge = part::Parts::<PRec>::new(nb, 64);
                        let mut gc = part::Parts::<CRec>::new(nb, 32);
                        let mut w = fast::BWork { cl: Vec::new(), ca: Vec::new(), ccnt: Vec::new(), cs: Vec::new(), evs: Vec::new(), name: Vec::new(), win: Win::new(ncamp, ctx.cats.len()) };
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            if i >= nib {
                                break;
                            }
                            fast::pass_b(i, bucket_a, fcl, extras, f, pt, plan, links, pool, ctx, shift, &mut w, &mut acc, &mut uo, &mut ge, &mut gc);
                        }
                        ge.finish();
                        gc.finish();
                        (uo, acc, ge, gc)
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        for (uo, acc, ge, gc) in b_parts {
            outs.push((uo, acc));
            pev.push(ge);
            pcl.push(gc);
        }
        lap("fast persons done");
    } else {
        for _ in 0..nt {
            outs.push((UserOut::default(), Acc { camp: vec![[0; 4]; (ncamp + 1) * ctx.ndays], ndays: ctx.ndays }));
        }
    }

    // P: generic person buckets, largest first
    let mut order: Vec<(usize, usize)> = (0..nb).map(|b| (pev.iter().map(|t| t.len(b)).sum::<usize>(), b)).collect();
    if verbose {
        let ge: usize = order.iter().map(|x| x.0).sum();
        let gc: usize = (0..nb).map(|b| pcl.iter().map(|t| t.len(b)).sum::<usize>()).sum();
        eprintln!("generic events {ge}, generic clicks {gc}");
    }
    order.sort_unstable_by(|a, b| b.cmp(a));
    let next = AtomicUsize::new(0);
    let accs: Vec<Acc> = std::thread::scope(|s| {
        let hs: Vec<_> = outs
            .into_iter()
            .enumerate()
            .map(|(t, (mut uo, mut acc))| {
                let (order, pev, pcl, ctx, pool, next, ud) = (&order, &pev, &pcl, &ctx, &pool, &next, &ud);
                s.spawn(move || {
                    let mut w = Work::new(ncamp, ctx.cats.len());
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= order.len() {
                            break;
                        }
                        person_bucket(order[i].1, pev, pcl, ctx, pool, &mut w, &mut acc, &mut uo);
                    }
                    if uo.rows > 0 || t == 0 {
                        write_file(&ud.join(format!("part-{t:03}.parquet")), &user_cols(&uo));
                    }
                    acc
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("sessions + user_daily written");

    // campaign_daily
    let nd = ctx.ndays;
    let mut tot = vec![[0i64; 4]; (ncamp + 1) * nd];
    for a in &accs {
        for (x, y) in tot.iter_mut().zip(&a.camp) {
            for k in 0..4 {
                x[k] += y[k];
            }
        }
    }
    let mut cid = pq::Bytes::default();
    let mut ch = pq::Bytes::default();
    let mut day = pq::Bytes::default();
    let mut ses = pq::Bytes::default();
    let mut pur = pq::Bytes::default();
    let mut rev = pq::Bytes::default();
    let mut ass = pq::Bytes::default();
    let mut rows = 0;
    for c in 0..=ncamp {
        for d in 0..nd {
            let a = tot[c * nd + d];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            rows += 1;
            if c == ncamp {
                cid.str(b"organic");
                ch.str(b"organic");
            } else {
                cid.str(&ctx.camp_names[c]);
                ch.str(&ctx.camp_chan[c]);
            }
            day.i32((ctx.day0 + d as i64) as i32);
            ses.i32(a[0] as i32);
            pur.i32(a[1] as i32);
            rev.i64(a[2]);
            ass.i32(a[3] as i32);
        }
    }
    {
        use pq::{Col, Logical::{Date, Plain, Str}, T_BYTE_ARRAY, T_INT32, T_INT64};
        let cols = [
            Col { name: "campaign_id", ptype: T_BYTE_ARRAY, logical: Str, optional: false, defs: None, values: &cid.0 },
            Col { name: "channel", ptype: T_BYTE_ARRAY, logical: Str, optional: false, defs: None, values: &ch.0 },
            Col { name: "day", ptype: T_INT32, logical: Date, optional: false, defs: None, values: &day.0 },
            Col { name: "sessions", ptype: T_INT32, logical: Plain, optional: false, defs: None, values: &ses.0 },
            Col { name: "purchasing_sessions", ptype: T_INT32, logical: Plain, optional: false, defs: None, values: &pur.0 },
            Col { name: "revenue_cents", ptype: T_INT64, logical: Plain, optional: false, defs: None, values: &rev.0 },
            Col { name: "assisted_sessions", ptype: T_INT32, logical: Plain, optional: false, defs: None, values: &ass.0 },
        ];
        write_file(&cd.join("part-000.parquet"), &pq::encode(rows, &cols));
    }
    lap("campaign_daily written");
    unsafe { libc::_exit(0) }
}
