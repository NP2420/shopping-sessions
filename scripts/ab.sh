#!/bin/bash
# Interleaved A/B of the single-thread bench: ab.sh BIN_A BIN_B MODE DATA [ROUNDS]
A=$1; B=$2; M=$3; D=$4; R=${5:-6}
for i in $(seq $R); do
  for v in A B; do bin=${!v}; x=$(SS_REPS=2 SS_BENCH=$M $bin $D /tmp/x 2>&1 | grep -o "[0-9.]* ns/line" | cut -d' ' -f1); echo "$v $x"; done
done | sort -k1,1 -k2,2n | awk '{v[$1]=v[$1]" "$2; c[$1]++; a[$1,c[$1]]=$2} END {for (k in v) printf "%s median %.1f ns/line (%s)\n", k, a[k,int((c[k]+1)/2)], v[k]}'
