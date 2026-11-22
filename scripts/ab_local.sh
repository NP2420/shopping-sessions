#!/bin/bash
# Interleaved A/B of commits on the local Docker scorer (v5 data, 8 CPUs).
#
#   scripts/ab_local.sh ROUNDS COMMIT...
#
# Builds one image per commit from `git archive` (needs the commits fetched), makes the v5 data
# volumes if missing, then times every commit once per round, in turn, and prints medians.
set -euo pipefail
export MSYS_NO_PATHCONV=1
ROUNDS=$1; shift
CPUS=${CPUS:-8}; MEM=${MEM:-24g}
SEED=7; EVENTS=20000000; CLICKS=5000000; P=ss5; GDIR=kit5/generator
cd "$(dirname "$0")/.."
REPO=$(pwd -W 2>/dev/null || pwd)
git fetch -q origin '+refs/heads/*:refs/remotes/origin/*'

for c in "$@"; do
  echo "== build $c"
  git archive "$c" | docker build -q -t "ss-ab:$c" - >/dev/null
done

if ! docker image inspect $P-gen:local >/dev/null 2>&1; then
  echo "== build generator image (once)"
  docker build -q -t $P-gen:local -f - "$REPO/$GDIR" >/dev/null <<'EOF'
FROM rust:1-slim-bookworm
WORKDIR /g
COPY . .
RUN cargo build --release && cp target/release/generate /usr/local/bin/
EOF
fi
VOL=$P-p$SEED-$EVENTS
if ! docker run --rm -v $VOL:/d alpine test -f /d/manifest.json 2>/dev/null; then
  echo "== generating $VOL"
  docker run --rm -v $VOL:/d $P-gen:local sh -c "rm -rf /d/* && generate --seed $SEED --events $EVENTS --clicks $CLICKS --output /d >/dev/null"
fi

run() { # image -> seconds from container start to exit
  docker volume rm -f ss-out >/dev/null; docker volume create ss-out >/dev/null
  docker run --rm -v ss-out:/o alpine chmod 777 /o >/dev/null
  local cid
  cid=$(docker create --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
    --cpus "$CPUS" --cpuset-cpus "${CPUSET:-0-7}" --memory "$MEM" --memory-swap "$MEM" --user 1000:1000 \
    -v $VOL:/data:ro -v ss-out:/output -v ss-scratch:/scratch \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
    -e NCPU="$CPUS" -e MEM_GB="${MEM%g}" "$1")
  docker start -a "$cid" >/dev/null
  local s f; s=$(docker inspect -f '{{.State.StartedAt}}' "$cid"); f=$(docker inspect -f '{{.State.FinishedAt}}' "$cid")
  docker rm "$cid" >/dev/null
  awk -v a="$(date -d "$s" +%s.%N)" -v b="$(date -d "$f" +%s.%N)" 'BEGIN { printf "%.3f\n", b - a }'
}

echo "== warm-up"
for c in "$@"; do run "ss-ab:$c" >/dev/null; done
declare -A T
for r in $(seq "$ROUNDS"); do
  line="round $r:"
  for c in "$@"; do
    t=$(run "ss-ab:$c"); T[$c]="${T[$c]:-} $t"; line="$line  $c $t"
  done
  echo "$line"
done
echo "== medians"
for c in "$@"; do
  echo "$c $(echo ${T[$c]} | tr ' ' '\n' | sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}')"
done
