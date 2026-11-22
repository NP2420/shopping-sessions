//! Minimal Parquet writer: one row group, one uncompressed PLAIN data page (v1) per column.

pub const T_INT32: i32 = 1;
pub const T_INT64: i32 = 2;
pub const T_BYTE_ARRAY: i32 = 6;

#[derive(Clone, Copy, PartialEq)]
pub enum Logical {
    Plain,
    Str,
    Date,
}

pub struct Col<'a> {
    pub name: &'a str,
    pub ptype: i32,
    pub logical: Logical,
    pub optional: bool,
    /// For optional columns: one byte per row, 1 = present.
    pub defs: Option<&'a [u8]>,
    /// PLAIN-encoded non-null values.
    pub values: &'a [u8],
}

struct Thrift {
    b: Vec<u8>,
    stack: Vec<i16>,
    last: i16,
}

impl Thrift {
    fn new() -> Self {
        Thrift { b: Vec::with_capacity(1024), stack: Vec::new(), last: 0 }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let d = id - self.last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        self.last = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.zz(v as i64);
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zz(v);
    }
    fn binf(&mut self, id: i16, s: &[u8]) {
        self.field(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn list(&mut self, ety: u8, n: usize) {
        if n < 15 {
            self.b.push(((n as u8) << 4) | ety);
        } else {
            self.b.push(0xF0 | ety);
            self.varint(n as u64);
        }
    }
    fn begin(&mut self) {
        self.stack.push(self.last);
        self.last = 0;
    }
    fn structf(&mut self, id: i16) {
        self.field(id, 12);
        self.begin();
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last = self.stack.pop().unwrap();
    }
}

fn def_levels(defs: &[u8], out: &mut Vec<u8>) {
    // RLE/bit-packed hybrid, bit width 1, one bit-packed run, with the v1 4-byte length prefix.
    let groups = (defs.len() + 7) / 8;
    let mut body = Vec::with_capacity(groups + 10);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        body.push((h as u8) | 0x80);
        h >>= 7;
    }
    body.push(h as u8);
    for g in 0..groups {
        let mut byte = 0u8;
        for i in 0..8 {
            let j = g * 8 + i;
            if j < defs.len() && defs[j] != 0 {
                byte |= 1 << i;
            }
        }
        body.push(byte);
    }
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
}

/// Writes the file with one vectored write of small headers and the column buffers themselves.
pub fn write(path: &std::path::Path, num_rows: usize, cols: &[Col]) -> std::io::Result<()> {
    use std::io::Write;
    let mut heads: Vec<Vec<u8>> = Vec::with_capacity(cols.len() + 2);
    let mut offs = Vec::with_capacity(cols.len());
    let mut off = 4usize;
    if num_rows > 0 {
        for c in cols {
            let mut h = Vec::new();
            let mut levels = Vec::new();
            if c.optional {
                def_levels(c.defs.expect("optional column needs defs"), &mut levels);
            }
            let page_len = levels.len() + c.values.len();
            let mut t = Thrift::new();
            t.begin();
            t.i32f(1, 0);
            t.i32f(2, page_len as i32);
            t.i32f(3, page_len as i32);
            t.structf(5);
            t.i32f(1, num_rows as i32);
            t.i32f(2, 0);
            t.i32f(3, 3);
            t.i32f(4, 3);
            t.end();
            t.end();
            h.extend_from_slice(&t.b);
            h.extend_from_slice(&levels);
            offs.push((off, h.len() + c.values.len()));
            off += h.len() + c.values.len();
            heads.push(h);
        }
    }
    let meta = footer(num_rows, cols, &offs);
    let mut f = std::fs::File::create(path)?;
    let mut parts: Vec<&[u8]> = Vec::with_capacity(2 * cols.len() + 2);
    parts.push(b"PAR1");
    if num_rows > 0 {
        for (h, c) in heads.iter().zip(cols) {
            parts.push(h);
            parts.push(c.values);
        }
    }
    parts.push(&meta);
    let mut i = 0;
    let mut skip = 0usize;
    while i < parts.len() {
        let mut v: Vec<std::io::IoSlice> = Vec::with_capacity(parts.len() - i);
        v.push(std::io::IoSlice::new(&parts[i][skip..]));
        for p in &parts[i + 1..] {
            v.push(std::io::IoSlice::new(p));
        }
        let mut n = f.write_vectored(&v)?;
        if n == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "write"));
        }
        // advance past what was written
        while i < parts.len() && n >= parts[i].len() - skip {
            n -= parts[i].len() - skip;
            i += 1;
            skip = 0;
        }
        if i < parts.len() {
            skip += n;
        }
    }
    Ok(())
}

/// Footer: metadata, its length, magic. `offs` holds each column chunk's (offset, size).
fn footer(num_rows: usize, cols: &[Col], offs: &[(usize, usize)]) -> Vec<u8> {
    let mut m = Thrift::new();
    m.begin();
    m.i32f(1, 1);
    m.field(2, 9);
    m.list(12, cols.len() + 1);
    m.begin();
    m.binf(4, b"schema");
    m.i32f(5, cols.len() as i32);
    m.end();
    for c in cols {
        m.begin();
        m.i32f(1, c.ptype);
        m.i32f(3, if c.optional { 1 } else { 0 });
        m.binf(4, c.name.as_bytes());
        match c.logical {
            Logical::Str => {
                m.i32f(6, 0);
                m.structf(10);
                m.structf(1);
                m.end();
                m.end();
            }
            Logical::Date => {
                m.i32f(6, 6);
                m.structf(10);
                m.structf(6);
                m.end();
                m.end();
            }
            Logical::Plain => {}
        }
        m.end();
    }
    m.i64f(3, num_rows as i64);
    m.field(4, 9);
    if num_rows > 0 {
        m.list(12, 1);
        m.begin();
        m.field(1, 9);
        m.list(12, cols.len());
        let mut total = 0i64;
        for (c, &(off, len)) in cols.iter().zip(offs) {
            total += len as i64;
            m.begin();
            m.i64f(2, off as i64);
            m.structf(3);
            m.i32f(1, c.ptype);
            m.field(2, 9);
            m.list(5, 2);
            m.zz(0);
            m.zz(3);
            m.field(3, 9);
            m.list(8, 1);
            m.varint(c.name.len() as u64);
            m.b.extend_from_slice(c.name.as_bytes());
            m.i32f(4, 0);
            m.i64f(5, num_rows as i64);
            m.i64f(6, len as i64);
            m.i64f(7, len as i64);
            m.i64f(9, off as i64);
            m.end();
            m.end();
        }
        m.i64f(2, total);
        m.i64f(3, num_rows as i64);
        m.end();
    } else {
        m.list(12, 0);
    }
    m.end();
    let mut out = m.b;
    let n = out.len() as u32;
    out.extend_from_slice(&n.to_le_bytes());
    out.extend_from_slice(b"PAR1");
    out
}

#[allow(dead_code)]
pub fn encode(num_rows: usize, cols: &[Col]) -> Vec<u8> {
    let mut est = 64;
    for c in cols {
        est += c.values.len() + c.defs.map_or(0, |d| d.len() / 8 + 16) + 64;
    }
    let mut out = Vec::with_capacity(est + 1024);
    out.extend_from_slice(b"PAR1");
    let mut chunks = Vec::new(); // (offset, total size)
    if num_rows > 0 {
        for c in cols {
            let mut levels = Vec::new();
            if c.optional {
                def_levels(c.defs.expect("optional column needs defs"), &mut levels);
            }
            let page_len = levels.len() + c.values.len();
            let mut h = Thrift::new();
            h.begin();
            h.i32f(1, 0); // DATA_PAGE
            h.i32f(2, page_len as i32);
            h.i32f(3, page_len as i32);
            h.structf(5);
            h.i32f(1, num_rows as i32);
            h.i32f(2, 0); // PLAIN
            h.i32f(3, 3); // RLE
            h.i32f(4, 3); // RLE
            h.end();
            h.end();
            let off = out.len();
            out.extend_from_slice(&h.b);
            out.extend_from_slice(&levels);
            out.extend_from_slice(c.values);
            chunks.push((off, out.len() - off));
        }
    }
    let mut m = Thrift::new();
    m.begin();
    m.i32f(1, 1);
    m.field(2, 9);
    m.list(12, cols.len() + 1);
    m.begin();
    m.binf(4, b"schema");
    m.i32f(5, cols.len() as i32);
    m.end();
    for c in cols {
        m.begin();
        m.i32f(1, c.ptype);
        m.i32f(3, if c.optional { 1 } else { 0 });
        m.binf(4, c.name.as_bytes());
        match c.logical {
            Logical::Str => {
                m.i32f(6, 0); // UTF8
                m.structf(10);
                m.structf(1);
                m.end();
                m.end();
            }
            Logical::Date => {
                m.i32f(6, 6); // DATE
                m.structf(10);
                m.structf(6);
                m.end();
                m.end();
            }
            Logical::Plain => {}
        }
        m.end();
    }
    m.i64f(3, num_rows as i64);
    m.field(4, 9);
    if num_rows > 0 {
        m.list(12, 1);
        m.begin();
        m.field(1, 9);
        m.list(12, cols.len());
        let mut total = 0i64;
        for (c, &(off, len)) in cols.iter().zip(&chunks) {
            total += len as i64;
            m.begin();
            m.i64f(2, off as i64);
            m.structf(3);
            m.i32f(1, c.ptype);
            m.field(2, 9);
            m.list(5, 2);
            m.zz(0);
            m.zz(3);
            m.field(3, 9);
            m.list(8, 1);
            m.varint(c.name.len() as u64);
            m.b.extend_from_slice(c.name.as_bytes());
            m.i32f(4, 0);
            m.i64f(5, num_rows as i64);
            m.i64f(6, len as i64);
            m.i64f(7, len as i64);
            m.i64f(9, off as i64);
            m.end();
            m.end();
        }
        m.i64f(2, total);
        m.i64f(3, num_rows as i64);
        m.end();
    } else {
        m.list(12, 0);
    }
    m.end();
    out.extend_from_slice(&m.b);
    out.extend_from_slice(&(m.b.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    out
}

/// Column value buffers built row by row.
#[derive(Default)]
pub struct Bytes(pub Vec<u8>);
impl Bytes {
    #[inline(always)]
    pub fn i32(&mut self, v: i32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    #[inline(always)]
    pub fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    #[inline(always)]
    pub fn str(&mut self, s: &[u8]) {
        self.0.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.0.extend_from_slice(s);
    }
}

// ---------------------------------------------------------------- dictionary-encoded columns

/// A column for `write_dict`: PLAIN values, or a dictionary (PLAIN-encoded entries) and one key
/// per non-null value.
pub enum Enc {
    Plain(Vec<u8>),
    Dict { dict: Vec<u8>, ndict: usize, keys: Vec<u32> },
}

pub struct DCol<'a> {
    pub name: &'a str,
    pub ptype: i32,
    pub logical: Logical,
    pub optional: bool,
    pub defs: Option<&'a [u8]>,
    pub enc: Enc,
}

fn bit_width(max: u32) -> u32 {
    (32 - max.leading_zeros()).max(1)
}

/// Bit width byte, then one bit-packed run of `keys` (padded to a multiple of 8).
fn hybrid_keys(keys: &[u32], w: u32, out: &mut Vec<u8>) {
    out.push(w as u8);
    let groups = (keys.len() + 7) / 8;
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        out.push((h as u8) | 0x80);
        h >>= 7;
    }
    out.push(h as u8);
    let start = out.len();
    out.resize(start + groups * w as usize, 0);
    let buf = &mut out[start..];
    let mut acc = 0u64;
    let mut bits = 0u32;
    let mut o = 0usize;
    for g in 0..groups * 8 {
        let k = if g < keys.len() { keys[g] as u64 } else { 0 };
        acc |= k << bits;
        bits += w;
        while bits >= 8 {
            buf[o] = acc as u8;
            o += 1;
            acc >>= 8;
            bits -= 8;
        }
    }
    if bits > 0 {
        buf[o] = acc as u8;
    }
}

fn page_header(t: &mut Thrift, ptype: i32, len: usize) {
    t.begin();
    t.i32f(1, ptype);
    t.i32f(2, len as i32);
    t.i32f(3, len as i32);
}

/// Like `write`, but columns may be dictionary encoded (a PLAIN dictionary page, then a data
/// page of RLE_DICTIONARY keys). One row group; uncompressed.
pub fn write_dict(path: &std::path::Path, num_rows: usize, cols: &[DCol]) -> std::io::Result<()> {
    use std::io::Write;
    let mut file: Vec<u8> = Vec::new();
    file.extend_from_slice(b"PAR1");
    // per column: (chunk start, chunk size, data page offset, dictionary page offset or 0)
    let mut meta: Vec<(usize, usize, usize, usize)> = Vec::with_capacity(cols.len());
    if num_rows > 0 {
        for c in cols {
            let start = file.len();
            let mut levels = Vec::new();
            if c.optional {
                def_levels(c.defs.expect("optional column needs defs"), &mut levels);
            }
            let mut dict_off = 0usize;
            let (body, enc_id): (Vec<u8>, i32) = match &c.enc {
                Enc::Plain(v) => {
                    let mut b = levels;
                    b.extend_from_slice(v);
                    (b, 0)
                }
                Enc::Dict { ndict: 0, .. } => (levels, 0), // no values: an empty PLAIN page
                Enc::Dict { dict, ndict, keys } => {
                    dict_off = file.len();
                    let mut t = Thrift::new();
                    page_header(&mut t, 2, dict.len());
                    t.structf(7);
                    t.i32f(1, *ndict as i32);
                    t.i32f(2, 0);
                    t.end();
                    t.end();
                    file.extend_from_slice(&t.b);
                    file.extend_from_slice(dict);
                    let mut b = levels;
                    hybrid_keys(keys, bit_width(ndict.saturating_sub(1) as u32), &mut b);
                    (b, 8)
                }
            };
            let data_off = file.len();
            let mut t = Thrift::new();
            page_header(&mut t, 0, body.len());
            t.structf(5);
            t.i32f(1, num_rows as i32);
            t.i32f(2, enc_id);
            t.i32f(3, 3);
            t.i32f(4, 3);
            t.end();
            t.end();
            file.extend_from_slice(&t.b);
            file.extend_from_slice(&body);
            meta.push((start, file.len() - start, data_off, dict_off));
        }
    }
    // footer
    let mut m = Thrift::new();
    m.begin();
    m.i32f(1, 1);
    m.field(2, 9);
    m.list(12, cols.len() + 1);
    m.begin();
    m.binf(4, b"schema");
    m.i32f(5, cols.len() as i32);
    m.end();
    for c in cols {
        m.begin();
        m.i32f(1, c.ptype);
        m.i32f(3, if c.optional { 1 } else { 0 });
        m.binf(4, c.name.as_bytes());
        match c.logical {
            Logical::Str => {
                m.i32f(6, 0);
                m.structf(10);
                m.structf(1);
                m.end();
                m.end();
            }
            Logical::Date => {
                m.i32f(6, 6);
                m.structf(10);
                m.structf(6);
                m.end();
                m.end();
            }
            Logical::Plain => {}
        }
        m.end();
    }
    m.i64f(3, num_rows as i64);
    m.field(4, 9);
    if num_rows > 0 {
        m.list(12, 1);
        m.begin();
        m.field(1, 9);
        m.list(12, cols.len());
        let mut total = 0i64;
        for (c, &(start, len, data_off, dict_off)) in cols.iter().zip(&meta) {
            total += len as i64;
            let is_dict = dict_off != 0;
            m.begin();
            m.i64f(2, start as i64);
            m.structf(3);
            m.i32f(1, c.ptype);
            m.field(2, 9);
            if is_dict {
                m.list(5, 3);
                m.zz(0);
                m.zz(3);
                m.zz(8);
            } else {
                m.list(5, 2);
                m.zz(0);
                m.zz(3);
            }
            m.field(3, 9);
            m.list(8, 1);
            m.varint(c.name.len() as u64);
            m.b.extend_from_slice(c.name.as_bytes());
            m.i32f(4, 0);
            m.i64f(5, num_rows as i64);
            m.i64f(6, len as i64);
            m.i64f(7, len as i64);
            m.i64f(9, data_off as i64);
            if is_dict {
                m.i64f(11, dict_off as i64);
            }
            m.end();
            m.end();
        }
        m.i64f(2, total);
        m.i64f(3, num_rows as i64);
        m.end();
    } else {
        m.list(12, 0);
    }
    m.end();
    let n = m.b.len() as u32;
    file.extend_from_slice(&m.b);
    file.extend_from_slice(&n.to_le_bytes());
    file.extend_from_slice(b"PAR1");
    std::fs::File::create(path)?.write_all(&file)
}

/// Dictionary of fixed-width PLAIN values (4 or 8 bytes); None when there are too many distinct.
pub fn dict_fixed(values: &[u8], w: usize, limit: usize) -> Option<Enc> {
    let n = values.len() / w;
    let get = |i: usize| -> u64 {
        if w == 4 {
            u32::from_le_bytes(values[4 * i..4 * i + 4].try_into().unwrap()) as u64
        } else {
            u64::from_le_bytes(values[8 * i..8 * i + 8].try_into().unwrap())
        }
    };
    let cap = (2 * limit).next_power_of_two().max(16);
    let mask = cap - 1;
    let mut slots: Vec<u32> = vec![u32::MAX; cap];
    let mut vals: Vec<u64> = Vec::new();
    let mut keys: Vec<u32> = Vec::with_capacity(n);
    for i in 0..n {
        let x = get(i);
        let mut h = (x.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) as usize & mask;
        loop {
            let s = slots[h];
            if s == u32::MAX {
                if vals.len() >= limit {
                    return None;
                }
                slots[h] = vals.len() as u32;
                keys.push(vals.len() as u32);
                vals.push(x);
                break;
            }
            if vals[s as usize] == x {
                keys.push(s);
                break;
            }
            h = (h + 1) & mask;
        }
    }
    let mut dict = Vec::with_capacity(vals.len() * w);
    for &v in &vals {
        dict.extend_from_slice(&v.to_le_bytes()[..w]);
    }
    Some(Enc::Dict { dict, ndict: vals.len(), keys })
}

/// Dictionary of PLAIN byte arrays (u32 length + bytes) where equal values are adjacent or
/// repeat from a small set: a key per value, a new entry whenever the value differs from every
/// entry seen (hash on bytes).
pub fn dict_bytes(values: &[u8], limit: usize) -> Option<Enc> {
    let mut map: std::collections::HashMap<&[u8], u32> = std::collections::HashMap::new();
    let mut dict = Vec::new();
    let mut keys = Vec::new();
    let mut ndict = 0usize;
    let mut i = 0usize;
    let mut last: (&[u8], u32) = (&[], u32::MAX);
    while i < values.len() {
        let l = u32::from_le_bytes(values[i..i + 4].try_into().unwrap()) as usize;
        let s = &values[i + 4..i + 4 + l];
        i += 4 + l;
        if last.1 != u32::MAX && last.0 == s {
            keys.push(last.1);
            continue;
        }
        let k = match map.get(s) {
            Some(&k) => k,
            None => {
                if ndict >= limit {
                    return None;
                }
                let k = ndict as u32;
                map.insert(s, k);
                dict.extend_from_slice(&(l as u32).to_le_bytes());
                dict.extend_from_slice(s);
                ndict += 1;
                k
            }
        };
        keys.push(k);
        last = (s, k);
    }
    Some(Enc::Dict { dict, ndict, keys })
}

/// Rows of one person are adjacent and each person appears once: a new entry on every change.
pub fn dict_runs(values: &[u8]) -> Enc {
    let mut dict = Vec::new();
    let mut keys = Vec::new();
    let mut ndict = 0usize;
    let mut i = 0usize;
    let mut last: &[u8] = &[];
    while i < values.len() {
        let l = u32::from_le_bytes(values[i..i + 4].try_into().unwrap()) as usize;
        let s = &values[i + 4..i + 4 + l];
        i += 4 + l;
        if ndict == 0 || s != last {
            dict.extend_from_slice(&(l as u32).to_le_bytes());
            dict.extend_from_slice(s);
            ndict += 1;
            last = s;
        }
        keys.push(ndict as u32 - 1);
    }
    Enc::Dict { dict, ndict, keys }
}
