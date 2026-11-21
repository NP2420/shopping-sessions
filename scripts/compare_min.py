#!/usr/bin/env python3
"""Exact, order-insensitive comparison of an output folder (Parquet) with expected CSVs.
usage: compare_min.py OUT_DIR EXPECTED_DIR"""
import sys
from pathlib import Path
import polars as pl

KEYS = {"user_daily": ["user_id", "day"], "campaign_daily": ["campaign_id", "day"]}

def load_parquet(d):
    files = [f for f in sorted(Path(d).glob("*.parquet")) if not f.name.startswith((".", "_"))]
    df = pl.concat([pl.read_parquet(f) for f in files], how="vertical_relaxed")
    return df.with_columns([pl.col(c).cast(pl.Int64) for c, t in df.schema.items() if t.is_integer()])

def main():
    out, exp = map(Path, sys.argv[1:3])
    ok = True
    for t, keys in KEYS.items():
        a = load_parquet(out / t)
        b = pl.read_csv(exp / f"{t}.csv", infer_schema_length=0)
        b = b.with_columns([pl.col("day").str.to_date()] + [pl.col(c).cast(pl.Int64) for c in b.columns if c not in ("user_id", "campaign_id", "channel", "day", "top_category")])
        b = b.with_columns(pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).alias(c) for c in ("top_category",) if c in b.columns)
        a = a.select(b.columns)
        diff = a.join(b, on=b.columns, how="anti", nulls_equal=True).height + b.join(a, on=b.columns, how="anti", nulls_equal=True).height
        print(f"{t}: rows {a.height} vs {b.height}, differing {diff}")
        ok &= diff == 0 and a.height == b.height
    print("MATCH" if ok else "DIFFER")
    sys.exit(0 if ok else 1)

main()
