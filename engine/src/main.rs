//! Shopping Sessions: native pipeline. See rules.md for the spec.
//!
//! Phases (all parallel over NCPU threads):
//!   L  parse links -> edge lists
//!   E  parse events -> records scattered by hash(event_id); union-find runs alongside
//!   D  dedup each event-id bucket and scatter winners by person; parse clicks by person
//!   P  per person bucket: group, sort, sessionize, attribute, aggregate, write Parquet

mod ids;
mod pq;
mod ts;

use ids::{Pool, EMPTY, OVF};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

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

    let prows = read_small(&data.join("products.csv"), 2);
    let mut cats: Vec<Vec<u8>> = prows.iter().map(|r| r[1].clone()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    if cats.len() > 255 {
        die("more than 254 categories");
    }
    let cidx: HashMap<Vec<u8>, u8> = cats.iter().enumerate().map(|(i, c)| (c.clone(), i as u8)).collect();
    let unknown = cidx[&b"unknown".to_vec()];
    let mut prod_tab = Vec::new();
    let mut prod_map = HashMap::new();
    for r in &prows {
        if r[0].is_empty() {
            continue;
        }
        let c = cidx[&r[1]];
        if let Some(n) = canon_num(&r[0], b"p") {
            if n < 50_000_000 {
                if prod_tab.len() <= n {
                    prod_tab.resize(n + 1, 255u8);
                }
                prod_tab[n] = c;
                continue;
            }
        }
        prod_map.insert(r[0].clone(), c);
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

fn parse_events(c: Chunk, ctx: &Ctx, shift: u32, out: &mut [Vec<EvRec>], ovf: &mut Ovf) {
    let ws = ctx.ws;
    let we = ctx.we;
    for_lines(c, |line| {
        let mut f = [&[][..]; 7];
        if !split::<7>(line, &mut f) || f[0].is_empty() {
            return;
        }
        let eid = ids::ident(f[0]);
        let ing_ms = ts::parse(f[3]).unwrap_or(i64::MAX);
        let ing = if ing_ms >= 0 && ing_ms / 1000 < u32::MAX as i64 { (ing_ms / 1000) as u32 } else { u32::MAX };
        let mut r = EvRec { eid, user: 0, ing, ts: u32::MAX, pc: 0, _p: 0 };
        'valid: {
            if f[1].is_empty() || f[5].is_empty() {
                break 'valid;
            }
            let t = match ts::parse(f[2]) {
                Some(t) if t >= ws && t < we => t,
                _ => break 'valid,
            };
            let price = match f[4] {
                b"view" | b"cart" => 0,
                b"purchase" => match parse_price(f[6]) {
                    Some(p) => p,
                    None => break 'valid,
                },
                _ => break 'valid,
            };
            if ing_ms != i64::MAX && ing_ms - t > DAY {
                break 'valid;
            }
            r.user = ids::key(f[1], ovf);
            r.ts = (t - ws) as u32;
            r.pc = (price << 8) | ctx.cat_of(f[5]) as u32;
        }
        out[bucket(eid, shift)].push(r);
    });
}

fn parse_links(c: Chunk, edges: &mut Vec<(u64, u64)>, ovf: &mut Ovf) {
    for_lines(c, |line| {
        let mut f = [&[][..]; 2];
        if !split::<2>(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let a = ids::key(f[0], ovf);
        let b = ids::key(f[1], ovf);
        edges.push((a, b));
    });
}

fn parse_clicks(c: Chunk, ctx: &Ctx, links: &LinkTab, shift: u32, out: &mut [Vec<CRec>], ovf: &mut Ovf) {
    let lo = ctx.ws - WEEK;
    let hi = ctx.we;
    for_lines(c, |line| {
        let mut f = [&[][..]; 4];
        if !split::<4>(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let camp = match ctx.camp_of(f[2]) {
            Some(x) => x,
            None => return,
        };
        let t = match ts::parse(f[3]) {
            Some(t) if t >= lo && t < hi => t,
            _ => return,
        };
        let ck = ids::key(f[0], ovf);
        let uk = ids::key(f[1], ovf);
        let pk = links.get(uk);
        out[bucket(pk, shift)].push(CRec { pk, ck, ts: (t - lo) as u32, camp });
    });
}

// ---------------------------------------------------------------- linked IDs

struct LinkTab {
    keys: Vec<u64>,
    vals: Vec<u64>,
    mask: usize,
}

impl LinkTab {
    #[inline(always)]
    fn get(&self, k: u64) -> u64 {
        let mut i = ids::mix(k) as usize & self.mask;
        loop {
            let kk = unsafe { *self.keys.get_unchecked(i) };
            if kk == k {
                return unsafe { *self.vals.get_unchecked(i) };
            }
            if kk == EMPTY {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}

#[derive(Default, Clone, Copy)]
struct FxBuild;
#[derive(Default)]
struct Fx(u64);
impl std::hash::Hasher for Fx {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.rotate_left(5) ^ b as u64).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
        }
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = ids::mix(v);
    }
}
impl std::hash::BuildHasher for FxBuild {
    type Hasher = Fx;
    fn build_hasher(&self) -> Fx {
        Fx(0)
    }
}

fn build_links(edges: &[Vec<(u64, u64)>], pool: &Pool) -> LinkTab {
    let ne: usize = edges.iter().map(|e| e.len()).sum();
    let mut idx: HashMap<u64, u32, FxBuild> = HashMap::with_capacity_and_hasher(ne * 2 + 16, FxBuild);
    let mut keys: Vec<u64> = Vec::with_capacity(ne * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(ne * 2);
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let g = p[p[x as usize] as usize];
            p[x as usize] = g;
            x = g;
        }
        x
    }
    let mut intern = |k: u64, keys: &mut Vec<u64>, parent: &mut Vec<u32>| -> u32 {
        *idx.entry(k).or_insert_with(|| {
            keys.push(k);
            parent.push(parent.len() as u32);
            (keys.len() - 1) as u32
        })
    };
    for es in edges {
        for &(a, b) in es {
            let ia = intern(a, &mut keys, &mut parent);
            let ib = intern(b, &mut keys, &mut parent);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                // root = the smaller ID, so the root's key is the person's name
                if ids::cmp_keys(keys[rb as usize], keys[ra as usize], pool).is_lt() {
                    parent[ra as usize] = rb;
                } else {
                    parent[rb as usize] = ra;
                }
            }
        }
    }
    let n = keys.len();
    let cap = (n * 2).next_power_of_two().max(2);
    let mut t = LinkTab { keys: vec![EMPTY; cap], vals: vec![0; cap], mask: cap - 1 };
    for i in 0..n {
        let r = find(&mut parent, i as u32);
        let name = keys[r as usize];
        let k = keys[i];
        if name == k {
            continue;
        }
        let mut j = ids::mix(k) as usize & t.mask;
        while t.keys[j] != EMPTY {
            j = (j + 1) & t.mask;
        }
        t.keys[j] = k;
        t.vals[j] = name;
    }
    t
}

// ---------------------------------------------------------------- dedup

fn dedup_bucket(
    b: usize,
    ev: &[Vec<Vec<EvRec>>],
    links: &LinkTab,
    shift: u32,
    table: &mut Vec<(u64, *const EvRec)>,
    out: &mut [Vec<PRec>],
) {
    let n: usize = ev.iter().map(|t| t[b].len()).sum();
    if n == 0 {
        return;
    }
    let cap = (n * 2).next_power_of_two();
    let mask = cap - 1;
    table.clear();
    table.resize(cap, (EMPTY, std::ptr::null()));
    for t in ev {
        for r in &t[b] {
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
    for s in table.iter() {
        if s.0 == EMPTY {
            continue;
        }
        let r = unsafe { &*s.1 };
        if r.ts == u32::MAX {
            continue;
        }
        let pk = links.get(r.user);
        out[bucket(pk, shift)].push(PRec { pk, ts: r.ts, pc: r.pc });
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
    wcnt: Vec<u32>,
    wpos: Vec<u32>,
    active: Vec<u32>,
    catsum: Vec<i64>,
    name: Vec<u8>,
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
            wcnt: vec![0; ncamp],
            wpos: vec![0; ncamp],
            active: Vec::new(),
            catsum: vec![0; ncat],
            name: Vec::new(),
        }
    }
}

fn person_bucket(
    b: usize,
    pev: &[Vec<Vec<PRec>>],
    pcl: &[Vec<Vec<CRec>>],
    ctx: &Ctx,
    pool: &Pool,
    w: &mut Work,
    acc: &mut Acc,
    uo: &mut UserOut,
) {
    let nev: usize = pev.iter().map(|t| t[b].len()).sum();
    if nev == 0 {
        return;
    }
    let cap = (nev * 2).next_power_of_two();
    let mask = cap - 1;
    w.tab.clear();
    w.tab.resize(cap, (EMPTY, 0));
    w.lids.clear();
    w.cnt.clear();
    w.pks.clear();
    for t in pev {
        for r in &t[b] {
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
            for r in &t[b] {
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
        for c in &t[b] {
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
            for c in &t[b] {
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
    let organic = ctx.camp_names.len();
    let ndays = acc.ndays;
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
        // sessions
        let mut lo = 0usize;
        let mut hi = 0usize;
        w.active.clear();
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
                uo.uid.str(&w.name);
                uo.day.i32(cur_day as i32);
                uo.ses.i32(d_ses as i32);
                uo.evn.i32(d_ev as i32);
                uo.rev.i64(d_rev);
                if d_any {
                    let mut best = 0usize;
                    let mut bv = -1i64;
                    for (ci, v) in w.catsum.iter_mut().enumerate() {
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
                    w.catsum[(pc & 255) as usize] += price;
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
                    w.catsum[(pc & 255) as usize] += price;
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
                if w.wcnt[c] == 0 {
                    w.wpos[c] = w.active.len() as u32;
                    w.active.push(c as u32);
                }
                w.wcnt[c] += 1;
                hi += 1;
            }
            while lo < hi && (cls[lo].ts as i64) < sc - WEEK {
                let c = cls[lo].camp as usize;
                w.wcnt[c] -= 1;
                if w.wcnt[c] == 0 {
                    let p = w.wpos[c] as usize;
                    let last = w.active.pop().unwrap();
                    if (last as usize) != c {
                        w.active[p] = last;
                        w.wpos[last as usize] = p as u32;
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
            for &c in &w.active {
                acc.camp[c as usize * ndays + di][3] += 1;
            }
            i = j;
        }
        // reset the click window for the next person
        for &c in &w.active {
            w.wcnt[c as usize] = 0;
        }
        w.active.clear();
    }
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

    let ctx = build_ctx(&data);
    let ev_files: Vec<&'static [u8]> = csv_files(&data.join("events")).iter().map(|p| map_file(p)).collect();
    let cl_files: Vec<&'static [u8]> = csv_files(&data.join("clicks")).iter().map(|p| map_file(p)).collect();
    let ln_files: Vec<&'static [u8]> = csv_files(&data.join("links")).iter().map(|p| map_file(p)).collect();
    let ev_bytes: usize = ev_files.iter().map(|d| d.len()).sum();
    let all_bytes = ev_bytes + cl_files.iter().map(|d| d.len()).sum::<usize>();
    let csize = (all_bytes / (nt * 16)).clamp(256 << 10, 4 << 20);
    let ev_chunks = chunks_of(&ev_files, csize);
    let cl_chunks = chunks_of(&cl_files, csize);
    let ln_chunks = chunks_of(&ln_files, csize);
    let est_events = ev_bytes / 80 + 1;
    let nb = (est_events / 12_000).next_power_of_two().clamp(64, 4096);
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

    // E: events, with union-find on its own thread
    let next = AtomicUsize::new(0);
    let (ev_parts, links): (Vec<(Vec<Vec<EvRec>>, Ovf)>, LinkTab) = std::thread::scope(|s| {
        let uf = s.spawn(|| build_links(&edges, &pool));
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut b: Vec<Vec<EvRec>> = (0..nb).map(|_| Vec::with_capacity(per)).collect();
                    let mut o = Ovf::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= ev_chunks.len() {
                            break;
                        }
                        parse_events(ev_chunks[i], &ctx, shift, &mut b, &mut o);
                    }
                    (b, o)
                })
            })
            .collect();
        let parts = hs.into_iter().map(|h| h.join().unwrap()).collect();
        (parts, uf.join().unwrap())
    });
    lap("events parsed + union-find");
    for (_, o) in &ev_parts {
        pool.extend(o.iter().copied());
    }
    let ev: Vec<Vec<Vec<EvRec>>> = ev_parts.into_iter().map(|(b, _)| b).collect();

    // D: clicks to person buckets, dedup event buckets to person buckets
    let next = AtomicUsize::new(0);
    let ntasks = cl_chunks.len() + nb;
    let pper = est_events / nt / nb + 32;
    let d_parts: Vec<(Vec<Vec<PRec>>, Vec<Vec<CRec>>, Ovf)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut pe: Vec<Vec<PRec>> = (0..nb).map(|_| Vec::with_capacity(pper)).collect();
                    let mut pc: Vec<Vec<CRec>> = (0..nb).map(|_| Vec::new()).collect();
                    let mut o = Ovf::new();
                    let mut table = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= ntasks {
                            break;
                        }
                        if i < cl_chunks.len() {
                            parse_clicks(cl_chunks[i], &ctx, &links, shift, &mut pc, &mut o);
                        } else {
                            dedup_bucket(i - cl_chunks.len(), &ev, &links, shift, &mut table, &mut pe);
                        }
                    }
                    (pe, pc, o)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("dedup + clicks");
    for (_, _, o) in &d_parts {
        pool.extend(o.iter().copied());
    }
    let mut pev = Vec::with_capacity(nt);
    let mut pcl = Vec::with_capacity(nt);
    for (a, b, _) in d_parts {
        pev.push(a);
        pcl.push(b);
    }

    // P: person buckets, largest first
    let mut order: Vec<(usize, usize)> = (0..nb).map(|b| (pev.iter().map(|t| t[b].len()).sum::<usize>(), b)).collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap_or_else(|_| die("mkdir user_daily"));
    std::fs::create_dir_all(&cd).unwrap_or_else(|_| die("mkdir campaign_daily"));
    let next = AtomicUsize::new(0);
    let ncamp = ctx.camp_names.len();
    let accs: Vec<Acc> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|t| {
                let (order, pev, pcl, ctx, pool, next, ud) = (&order, &pev, &pcl, &ctx, &pool, &next, &ud);
                s.spawn(move || {
                    let mut w = Work::new(ncamp, ctx.cats.len());
                    let mut acc = Acc { camp: vec![[0; 4]; (ncamp + 1) * ctx.ndays], ndays: ctx.ndays };
                    let mut uo = UserOut::default();
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
