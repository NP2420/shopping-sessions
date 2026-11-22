#!/bin/bash
# Local scorer: build the image, check correctness on the dev set, time runs on a ~20M-event set.
# Everything runs in Docker, data lives in named volumes (fast; no Windows bind mounts).
#
#   scripts/local_bench.sh [RUNS] [SEED] [EVENTS] [CLICKS]
#
# Needs: docker. Works from Git Bash (MSYS_NO_PATHCONV is set below).
set -euo pipefail
export MSYS_NO_PATHCONV=1
RUNS=${1:-5}; SEED=${2:-7}; EVENTS=${3:-20000000}; CLICKS=${4:-5000000}
CPUS=${CPUS:-8}; MEM=${MEM:-24g}
# V=5 uses the scorer-v5 generator unzipped in kit5/ (its own image and data volumes)
V=${V:-5}
if [ "$V" = 5 ]; then GDIR=kit5/generator; P=ss5; else GDIR=kit/generator/generator; P=ss; fi
cd "$(dirname "$0")/.."
REPO=$(pwd -W 2>/dev/null || pwd)
IMG=ss-entry:local

echo "== build entry image"
docker build -q -t $IMG . >/dev/null

echo "== build generator image (once)"
if ! docker image inspect $P-gen:local >/dev/null 2>&1; then
  docker build -q -t $P-gen:local -f - "$REPO/$GDIR" >/dev/null <<'EOF'
FROM rust:1-slim-bookworm
WORKDIR /g
COPY . .
RUN cargo build --release && cp target/release/generate /usr/local/bin/
EOF
fi

gen() { # volume seed events clicks
  if ! docker run --rm -v "$1":/d alpine test -f /d/manifest.json 2>/dev/null; then
    echo "   generating $1"
    docker run --rm -v "$1":/d $P-gen:local sh -c "rm -rf /d/* && generate --seed $2 --events $3 --clicks $4 --output /d >/dev/null"
  fi
}
gen $P-dev 1 1000000 250000
gen $P-p$SEED-$EVENTS $SEED $EVENTS $CLICKS

run() { # data-volume -> prints seconds (container StartedAt..FinishedAt) and leaves output in volume ss-out
  docker volume rm -f ss-out >/dev/null; docker volume create ss-out >/dev/null
  docker run --rm -v ss-out:/o alpine chmod 777 /o >/dev/null
  local cid
  cid=$(docker create --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
    --cpus "$CPUS" --cpuset-cpus "${CPUSET:-0-7}" --memory "$MEM" --memory-swap "$MEM" --user 1000:1000 \
    -v "$1":/data:ro -v ss-out:/output -v ss-scratch:/scratch \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
    -e NCPU="$CPUS" -e MEM_GB="${MEM%g}" ${VERBOSE:+-e SS_VERBOSE=1} $IMG)
  docker start -a "$cid" >/dev/null
  local code; code=$(docker inspect -f '{{.State.ExitCode}}' "$cid")
  local s f; s=$(docker inspect -f '{{.State.StartedAt}}' "$cid"); f=$(docker inspect -f '{{.State.FinishedAt}}' "$cid")
  docker rm "$cid" >/dev/null
  awk -v a="$(date -d "$s" +%s.%N)" -v b="$(date -d "$f" +%s.%N)" -v c="$code" 'BEGIN { printf "%.3f s  exit=%s\n", b - a, c }'
}

echo "== dev set correctness"
run $P-dev
ANS=$([ "$V" = 5 ] && echo kit5/dev-answer || echo kit/dev-answer/dev-answer)
[ -d "$REPO/$ANS" ] && docker run --rm -v ss-out:/out:ro -v "$REPO/$ANS":/ans:ro -v "$REPO/scripts":/s:ro python:3.12-slim \
  sh -c "pip install -q polars >/dev/null 2>&1 && python3 /s/compare_min.py /out /ans" || true

echo "== timing on seed $SEED, $EVENTS events ($RUNS runs)"
for i in $(seq "$RUNS"); do run $P-p$SEED-$EVENTS; done
echo "== phase timings (one verbose run)"
VERBOSE=1 run $P-p$SEED-$EVENTS
