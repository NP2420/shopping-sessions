#!/bin/bash
# Interleaved single-thread runs of two prof builds; min per section. usage: abprof.sh BIN_A BIN_B [N] [DATA]
A=$1; B=$2; N=${3:-5}; D=${4:-/root/b5/p20}
for i in $(seq $N); do
  for x in A B; do
    bin=${!x}; rm -rf /tmp/abp.out
    NCPU=${NCPU:-1} SS_VERBOSE=1 $bin $D /tmp/abp.out 2>&1 | awk -v x=$x '/prof/ {n=$0; sub(/^ *prof +/,"",n); sub(/ +[0-9.]+ ms.*/,"",n); for(i=1;i<=NF;i++) if($(i+1)=="ms") v=$i; print x"|"n"|"v} /background writes done/ {print x"|total|"$1}'
  done
done | awk -F'|' '{k=$2; if(!(k in a)) {a[k]=1e18; b[k]=1e18; o[++m]=k} if($1=="A" && $3<a[k]) a[k]=$3; if($1=="B" && $3<b[k]) b[k]=$3} END {for(i=1;i<=m;i++) printf "%-14s A %8.1f  B %8.1f  %+6.1f%%\n", o[i], a[o[i]], b[o[i]], (b[o[i]]-a[o[i]])/a[o[i]]*100}'
