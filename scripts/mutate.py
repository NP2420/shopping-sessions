#!/usr/bin/env python3
"""Copies a dataset and injects rule-legal oddities the generator never makes, to stress the
fast path's checks: winning duplicates under another user, shuffled timestamps, extra links
between people, upper-case IDs, clicks by foreign aliases.

usage: mutate.py SRC DST SEED [NLINKS [SCALE]]   (DST must not exist)"""
import random
import shutil
import sys
from pathlib import Path

src, dst, seed = Path(sys.argv[1]), Path(sys.argv[2]), int(sys.argv[3])
rnd = random.Random(seed)
shutil.copytree(src, dst)


def rows(path):
    lines = path.read_bytes().split(b"\n")
    return [l for l in lines[1:] if l]


users, evrows = [], []
for f in sorted((dst / "events").glob("*.csv")):
    for r in rows(f):
        p = r.split(b",")
        if len(p) == 7:
            evrows.append(p)
            if p[1] and b'"' not in p[1]:
                users.append(p[1])
users = users or [b"u1"]

# earliest ingest per event ID (F1 strings compare like times), so an injected winner is strictly
# earlier: rules.md says copies with equal ingest_ts are identical
first = {}
for p in evrows:
    k = p[0].strip(b'"')
    v = p[3].strip(b'"')
    if k not in first or v < first[k]:
        first[k] = v

SCALE = float(sys.argv[5]) if len(sys.argv) > 5 else 1.0  # shrink every rate
extra_ev = []
for p in evrows:
    x = rnd.random() / SCALE
    if x < 0.01:  # an earlier-ingested copy under another user: it wins dedup
        q = list(p)
        q[1] = rnd.choice(users)
        ing = first[q[0].strip(b'"')]
        if len(ing) == 20 and ing[17:19].isdigit() and int(ing[17:19]) > 0:
            q[3] = ing[:17] + b"%02d" % (int(ing[17:19]) - 1) + ing[19:]
            extra_ev.append(q)
    elif x < 0.015:  # a new event with another event's timestamp: runs out of order
        q = list(p)
        q[2] = rnd.choice(evrows)[2]
        q[0] = b"ex%d" % len(extra_ev)
        extra_ev.append(q)
    elif x < 0.017:  # upper-case event ID: a different event on the generic route
        q = list(p)
        q[0] = b"E%d" % len(extra_ev)
        extra_ev.append(q)
    elif x < 0.019:  # upper-case user ID
        q = list(p)
        q[0] = b"ey%d" % len(extra_ev)
        q[1] = q[1].upper()
        extra_ev.append(q)
with open(dst / "events" / "part-mut.csv", "wb") as f:
    f.write(b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n")
    for q in extra_ev:
        f.write(b",".join(q) + b"\n")

links = []
NLINK = int(sys.argv[4]) if len(sys.argv) > 4 else max(10, len(users) // 500)
for _ in range(NLINK):  # join random people
    links.append(rnd.choice(users) + b"," + rnd.choice(users))
for _ in range(max(5, len(users) // 2000)):  # link an upper-case twin
    a = rnd.choice(users)
    links.append(a + b"," + a.upper())
with open(dst / "links" / "part-mut.csv", "wb") as f:
    f.write(b"user_id_a,user_id_b\n" + b"\n".join(links) + b"\n")

cl = []
for f in sorted((dst / "clicks").glob("*.csv")):
    for r in rows(f):
        p = r.split(b",")
        if len(p) == 4 and rnd.random() < 0.01 * SCALE:  # a click by someone else
            p = list(p)
            p[0] = b"km%d" % len(cl)  # unique: copies of one click must stay identical
            p[1] = rnd.choice(users)
            cl.append(p)
with open(dst / "clicks" / "part-mut.csv", "wb") as f:
    f.write(b"click_id,user_id,campaign_id,ts\n" + b"".join(b",".join(p) + b"\n" for p in cl))
print(f"mutated: +{len(extra_ev)} events, +{len(links)} links, +{len(cl)} clicks")
