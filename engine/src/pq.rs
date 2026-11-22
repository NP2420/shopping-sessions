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
