# Shopping Sessions

Builders Buildathon, Databricks track. A static Rust binary in an empty image reads the click-stream
CSVs in `/data` and writes `user_daily` and `campaign_daily` Parquet tables to `/output`.

## Layout

- `engine/`: the pipeline (Rust, std + `libc` + `memchr`)
  - `scan.rs`: SIMD CSV scanner (newline/comma/quote bitmasks per 64 bytes)
  - `ts.rs`: timestamp forms F1/F2/F3 with SWAR fast paths
  - `ids.rs`: IDs packed into order-preserving u64 keys (6 bits per byte), with an overflow path
  - `pq.rs`: minimal Parquet writer (PLAIN, uncompressed, one page per column)
  - `main.rs`: phases: links and union-find, event parse with dedup buckets, clicks, per-person sessions and attribution
- `reference/reference.py`: slow, plain reference used as the test oracle
- `scripts/`: differential tests against the reference
- `SUBMISSIONS.md`: every scored submission

## Test

```sh
cd engine && cargo test --release && cargo build --release
scripts/check.sh engine/target/release/ss   # fixtures, dev set, generated seeds vs the reference
```

`SS_VERBOSE=1` prints phase timings to stderr.
