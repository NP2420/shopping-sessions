#!/usr/bin/env python3
"""Plain, slow reference for rules.md. It is the oracle for differential tests, not the entry.

Usage: reference.py [DATA_DIR OUTPUT_DIR] [--csv]
Writes OUTPUT_DIR/user_daily/part-0.parquet and OUTPUT_DIR/campaign_daily/part-0.parquet,
or OUTPUT_DIR/user_daily.csv and OUTPUT_DIR/campaign_daily.csv with --csv.
"""
import datetime as dt
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 7 * DAY_MS
EPOCH = dt.date(1970, 1, 1)

F1 = re.compile(rb"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z")
F2 = re.compile(rb"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})([+-])([0-9]{2}):([0-9]{2})")
F3 = re.compile(rb"[0-9]+")
DIGITS = F3


def days_from_civil(y, m, d):
    y -= m <= 2
    era = y // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def month_days(y, m):
    if m == 2:
        leap = (y % 4 == 0 and y % 100 != 0) or y % 400 == 0
        return 29 if leap else 28
    return 30 if m in (4, 6, 9, 11) else 31


def civil_ms(g):
    y, mo, d, h, mi, s = (int(x) for x in g[:6])
    if not (1 <= mo <= 12 and 1 <= d <= month_days(y, mo) and h <= 23 and mi <= 59 and s <= 59):
        return None
    return ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * 1000


def parse_ts(v):
    """Milliseconds since the epoch, or None for a bad timestamp (window not checked)."""
    if v is None:
        return None
    m = F1.fullmatch(v)
    if m:
        return civil_ms(m.groups())
    m = F2.fullmatch(v)
    if m:
        base = civil_ms(m.groups())
        oh, om = int(m.group(8)), int(m.group(9))
        if base is None or oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000
        return base - off if m.group(7) == b"+" else base + off
    if F3.fullmatch(v):
        return int(v)
    return None


def parse_field(f):
    if b'"' in f:
        if len(f) >= 2 and f[0:1] == b'"' and f[-1:] == b'"' and b'"' not in f[1:-1]:
            f = f[1:-1]
        else:
            return False, None
    return True, (f if f else None)


def read_rows(path, nfields):
    data = Path(path).read_bytes()
    if not data:
        return
    lines = data.split(b"\n")
    if data.endswith(b"\n"):
        lines.pop()
    for line in lines[1:]:
        if not line:
            continue
        parts = line.split(b",")
        if len(parts) != nfields:
            continue
        row = []
        for p in parts:
            ok, v = parse_field(p)
            if not ok:
                break
            row.append(v)
        else:
            yield row


def csv_files(folder):
    folder = Path(folder)
    if not folder.is_dir():
        return []
    return sorted(p for p in folder.iterdir() if p.name.endswith(".csv") and p.is_file())


class DSU:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        p = self.parent
        if x not in p:
            return x
        root = x
        while p[root] != root:
            root = p[root]
        while p[x] != root:
            p[x], x = root, p[x]
        return root

    def union(self, a, b):
        for x in (a, b):
            self.parent.setdefault(x, x)
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        # keep the byte-wise smallest ID as the root, so find() returns the person's name
        if rb < ra:
            ra, rb = rb, ra
        self.parent[rb] = ra


def run(data_dir):
    data_dir = Path(data_dir)
    manifest = json.loads((data_dir / "manifest.json").read_text())
    ws = parse_ts(manifest["window_start"].encode())
    we = parse_ts(manifest["window_end"].encode())

    categories = {r[0]: r[1] for r in read_rows(data_dir / "products.csv", 2) if r[0] is not None}
    channels = {r[0]: r[1] for r in read_rows(data_dir / "campaigns.csv", 2) if r[0] is not None}

    dsu = DSU()
    for f in csv_files(data_dir / "links"):
        for a, b in read_rows(f, 2):
            if a is None or b is None:
                continue
            dsu.union(a, b)

    # events: dedup on event_id by smallest ingest_ts, then validate
    kept = {}
    for f in csv_files(data_dir / "events"):
        for row in read_rows(f, 7):
            eid = row[0]
            if eid is None:
                continue
            ing = parse_ts(row[3])
            if ing is None:
                ing = float("inf")  # never happens per rules; keeps the code total
            cur = kept.get(eid)
            if cur is None or ing < cur[0]:
                kept[eid] = (ing, row)

    per_person = defaultdict(list)
    for ing, (eid, user, ts_raw, _ing_raw, etype, product, price_raw) in kept.values():
        if user is None or product is None:
            continue
        ts = parse_ts(ts_raw)
        if ts is None or not (ws <= ts < we):
            continue
        if etype not in (b"view", b"cart", b"purchase"):
            continue
        price = 0
        if etype == b"purchase":
            if price_raw is None or not DIGITS.fullmatch(price_raw):
                continue
            price = int(price_raw)
            if not (1 <= price <= 10_000_000):
                continue
        if ing - ts > DAY_MS:
            continue
        cat = categories.get(product, b"unknown")
        per_person[dsu.find(user)].append((ts, etype == b"purchase", price, cat))

    # clicks
    seen_click = set()
    clicks = defaultdict(list)
    for f in csv_files(data_dir / "clicks"):
        for cid, user, camp, ts_raw in read_rows(f, 4):
            if cid is None or cid in seen_click:
                continue
            seen_click.add(cid)
            if user is None or camp is None or camp not in channels:
                continue
            ts = parse_ts(ts_raw)
            if ts is None or not (ws - WEEK_MS <= ts < we):
                continue
            clicks[dsu.find(user)].append((ts, cid, camp))

    user_daily = {}
    camp_daily = {}

    def camp_row(camp, day):
        key = (camp, day)
        r = camp_daily.get(key)
        if r is None:
            r = camp_daily[key] = [0, 0, 0, 0]
        return r

    for person, evs in per_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        prev = None
        for e in evs:
            if prev is None or e[0] - prev > GAP_MS:
                sessions.append([])
            sessions[-1].append(e)
            prev = e[0]
        pclicks = clicks.get(person, [])
        for s in sessions:
            start = s[0][0]
            day = start // DAY_MS
            n = len(s)
            rev = sum(e[2] for e in s if e[1])
            purchased = any(e[1] for e in s)
            catsum = defaultdict(int)
            for e in s:
                if e[1]:
                    catsum[e[3]] += e[2]
            u = user_daily.get((person, day))
            if u is None:
                u = user_daily[(person, day)] = [0, 0, 0, defaultdict(int)]
            u[0] += 1
            u[1] += n
            u[2] += rev
            for c, v in catsum.items():
                u[3][c] += v

            best = None
            assists = set()
            for cts, cid, camp in pclicks:
                if cts <= start and start - cts <= WEEK_MS:
                    assists.add(camp)
                    if best is None or cts > best[0] or (cts == best[0] and cid < best[1]):
                        best = (cts, cid, camp)
            credited = best[2] if best else b"organic"
            r = camp_row(credited, day)
            r[0] += 1
            r[1] += 1 if purchased else 0
            r[2] += rev
            for camp in assists:
                camp_row(camp, day)[3] += 1

    users = []
    for (person, day), (ns, ne, rev, cats) in user_daily.items():
        top = None
        if cats:
            top = min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0]
        users.append((person, day, ns, ne, rev, top))
    camps = []
    for (camp, day), (ns, nps, rev, nas) in camp_daily.items():
        ch = b"organic" if camp == b"organic" else channels[camp]
        camps.append((camp, ch, day, ns, nps, rev, nas))
    return users, camps


def dec(b):
    return None if b is None else b.decode("utf-8", "surrogateescape")


def write(users, camps, out_dir, as_csv):
    out = Path(out_dir)
    ucols = ["user_id", "day", "sessions", "events", "revenue_cents", "top_category"]
    ccols = ["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"]
    if as_csv:
        out.mkdir(parents=True, exist_ok=True)
        import csv
        for name, cols, rows, dayidx in (("user_daily", ucols, users, 1), ("campaign_daily", ccols, camps, 2)):
            with open(out / f"{name}.csv", "w", newline="", encoding="utf-8", errors="surrogateescape") as fh:
                w = csv.writer(fh, lineterminator="\n")
                w.writerow(cols)
                for r in sorted(rows, key=lambda r: tuple(str(x) for x in r)):
                    r = list(r)
                    r[dayidx] = (EPOCH + dt.timedelta(days=r[dayidx])).isoformat()
                    w.writerow(["" if x is None else (dec(x) if isinstance(x, bytes) else x) for x in r])
        return
    import pyarrow as pa
    import pyarrow.parquet as pq
    for name, cols, rows, dayidx in (("user_daily", ucols, users, 1), ("campaign_daily", ccols, camps, 2)):
        d = out / name
        d.mkdir(parents=True, exist_ok=True)
        arrays = []
        for i, c in enumerate(cols):
            vals = [r[i] for r in rows]
            if i == dayidx:
                arrays.append(pa.array([EPOCH + dt.timedelta(days=v) for v in vals], type=pa.date32()))
            elif vals and isinstance(next((v for v in vals if v is not None), 0), bytes) or c in ("user_id", "top_category", "campaign_id", "channel"):
                arrays.append(pa.array([dec(v) for v in vals], type=pa.string()))
            else:
                arrays.append(pa.array(vals, type=pa.int64()))
        pq.write_table(pa.table(arrays, names=cols), d / "part-0.parquet")


def main():
    args = [a for a in sys.argv[1:] if a != "--csv"]
    as_csv = "--csv" in sys.argv
    data_dir = args[0] if len(args) >= 1 else os.environ.get("DATA_DIR", "/data")
    out_dir = args[1] if len(args) >= 2 else os.environ.get("OUTPUT_DIR", "/output")
    users, camps = run(data_dir)
    write(users, camps, out_dir, as_csv)


if __name__ == "__main__":
    main()
