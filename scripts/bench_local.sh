#!/bin/bash
# Min-of-N phase timings on a dataset: bench_local.sh BIN DATA [N]
B=$1; D=$2; N=${3:-5}; O=/tmp/bench_out
for i in $(seq $N); do rm -rf $O; s=$(date +%s.%N); SS_VERBOSE=1 NCPU=${NCPU:-2} $B $D $O 2>&1 | awk '/ms/{printf "%s=%s ", $3, $1}'; e=$(date +%s.%N); echo "wall=$(echo "$e - $s" | bc)"; done | awk '{for(i=1;i<=NF;i++){split($i,a,"="); if(!(a[1] in m) || a[2]<m[a[1]]) m[a[1]]=a[2]; if(!(a[1] in o)){o[a[1]]=++n; k[n]=a[1]}}} END {for(i=1;i<=n;i++) printf "%s=%s ", k[i], m[k[i]]; print ""}'
