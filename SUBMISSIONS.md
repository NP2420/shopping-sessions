# Submissions

| # | Commit | Time (practice) | Result | Change / notes |
|---|--------|-----------------|--------|----------------|
| 1 | e291ce8 | 1.3555 s | correct | First generic engine: SIMD scan, SWAR ts, packed IDs, eid-bucket dedup, union-find table. Local 2-core: ~3.0 s wall / ~5.7 CPU-s on 20M. |
| 2 | 7030c9d | 1.0717 s | correct | + products parse w/o allocs, THP bump allocator, write-combined NT scatter (events/persons/clicks), small growable person tables, prefetch in dedup. Local 2-core ~2.6 s. |
