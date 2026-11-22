# Submissions

| # | Commit | Time (practice) | Result | Change / notes |
|---|--------|-----------------|--------|----------------|
| 1 | e291ce8 | 1.3555 s | correct | First generic engine: SIMD scan, SWAR ts, packed IDs, eid-bucket dedup, union-find table. Local 2-core: ~3.0 s wall / ~5.7 CPU-s on 20M. |
| 2 | 7030c9d | 1.0717 s | correct | + products parse w/o allocs, THP bump allocator, write-combined NT scatter (events/persons/clicks), small growable person tables, prefetch in dedup. Local 2-core ~2.6 s. |
| 3 | c9ec46b | 1.1622 s | correct | + branch-free unquote/F1-F3 ts/event type, 2-line WC staging. Local single-thread parse -10%; scorer slower (noise up to 20%?) - inconclusive |
| 4 | 2fcfc5f | 0.9765 s | correct | + dense-ID fast path (key recovery, direct-slot dedup, run-based persons, verified fallback), date cache, person table built in union-find. ~11% of events still generic. |
| 5 | 1fa4834 | 0.9364 s | correct | + runs across buckets, wide aliases, extras (generic events 11% -> 1.3%), clicks parsed in event phase, stale-slot pass A, small assist array |
| 6 | 763eb19 | 0.8993 s | correct | + SWAR product ids, key search overlapped with links (smaller sample), lean ingest parse, pre-reserved buffers |
| 7 | f801706 | 0.8762 s | correct | + 24-byte fast records (no padding; write-combining for any record size) |
| 8 | b4cae74 | 0.8409 s | correct | + 8-byte pass-A winners with alias exceptions (runs read in place), forward extras walk, finished-person bitmap, unquote w/o empty test, SWAR time of day |

Scorer v5 (Nov 21 ~9 PM): board reset; 1 in 8 IDs now carry a 32-hex suffix and the ID bijection is keyed per seed, so the key-recovery fast path never fires. Scores below are v5.

| # | Commit | Time (practice) | Result | Change / notes |
|---|--------|-----------------|--------|----------------|
| 9 | 5378036 | 1.3459 s | correct | v5 baseline: generic path only (keys not recoverable). Long user IDs hit a serial 4M-entry HashMap merge (~0.4 s local). |
| 10 | 7627552 | 1.1372 s | correct | + concurrent open-addressing table for long user IDs (no serial merge), click IDs as pointer keys (order only), key search off by default |
| 11 | 697705f | 1.0995 s | correct | + link table as one-line buckets of four (SIMD compare, branch-free hit select), dedup scan without emptiness branches |
| 12 | 3b0e3bf | 1.1216 s | correct | + dedup table in four-slot lines, pool in four-slot lines sized for long user IDs (noise vs #11?) |
| 13 | 6695c19 | 1.3105 s | correct | + clicks placed in person partitions during parsing, person table lines, radix for big persons. 0.2 s worse than #11: regression or a slow machine? |
| 14 | 6695c19 | 1.1104 s | correct | same commit as #13: #13's 1.31 s was a slow machine (about 18% spread between two runs of one commit) |
| 15 | 6063438 (branch exp-nothp) | 1.3184 s | correct | #14 without huge-page advice: slower, keep THP |
| 16 | 1119b33 | 1.1057 s | correct | same engine as #14 (log-only commits on top): third sample, 1.10-1.11 s |
