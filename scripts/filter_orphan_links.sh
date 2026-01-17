#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <input.gfa>" >&2
  exit 1
fi

input="$1"

awk -F'\t' '
  NR==FNR {
    if ($1=="S") ids[$2]=1
    next
  }
  $1=="L" {
    if (ids[$2] && ids[$4]) print
    next
  }
  { print }
' "$input" "$input"
