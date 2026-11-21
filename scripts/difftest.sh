#!/bin/bash
# Differential test: engine vs reference on generated datasets.
# usage: difftest.sh ENGINE_BIN GENERATE_BIN COMPARE_PY "seed:events:clicks ..."
set -u
ENG=$1; GEN=$2; CMP=$3; shift 3
W=${WORK:-/tmp/difftest}
mkdir -p "$W"
fail=0
for spec in "$@"; do
  IFS=: read -r seed ev cl <<<"$spec"
  d="$W/s$seed-$ev"
  [ -d "$d" ] || "$GEN" --seed "$seed" --events "$ev" --clicks "$cl" --output "$d" >/dev/null
  rm -rf "$d.ref" "$d.eng"
  [ -f "$d.refcsv/user_daily.csv" ] || python3 "$(dirname "$0")/../reference/reference.py" "$d" "$d.refcsv" --csv
  NCPU=${NCPU:-2} "$ENG" "$d" "$d.eng" || { echo "FAIL $spec: engine exit $?"; fail=1; continue; }
  if python3 "$CMP" "$d.eng" "$d.refcsv" > "$d.cmp" 2>&1; then echo "ok   $spec"; else echo "FAIL $spec"; cat "$d.cmp"; fail=1; fi
done
exit $fail
