#!/bin/bash
# Full local correctness check: fixtures, dev set, differential seeds. usage: check.sh BIN
B=$1; K=/root/kit; S=$K/starter-duckdb/starter-duckdb
cd $S && python3 scripts/run_fixtures.py fixtures -- $B 2>&1 | tail -1
rm -rf $K/out-ss; NCPU=2 $B $K/dev $K/out-ss && python3 scripts/compare.py $K/out-ss $K/dev-answer/dev-answer | tail -1
cd scripts && $(dirname $0)/difftest.sh $B $K/generator/generator/target/release/generate $PWD/compare.py ${SEEDS:-2:200000:50000 3:300000:80000 4:150000:40000 5:400000:100000 6:100000:30000} | grep -c "^ok" | sed 's/^/difftest ok: /'
