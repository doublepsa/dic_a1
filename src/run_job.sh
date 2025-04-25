#!/usr/bin/env bash

# Usage:
#   ./run_job.sh [--dev] [--debug]
#     --dev    run locally on reviews_devset.json → output_dev.txt
#     --debug  write elapsed seconds to debug*.txt (mode-dependent)

set -euo pipefail

DEV=false
DEBUG=false

for arg in "$@"; do
  case "$arg" in
    --dev)   DEV=true   ;;
    --debug) DEBUG=true ;;
    *) echo "Unknown option: $arg" >&2; exit 1 ;;
  esac
done

# Build command and filenames
CMD=(python job.py --files stopwords.txt)
if $DEV; then
  CMD+=(-r local reviews_devset.json)
  OUT_FILE="output_dev.txt"
  DBG_FILE="debug_dev.txt"
else
  CMD+=(--hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar -r hadoop \
        hdfs:///user/dic25_shared/amazon-reviews/full/reviewscombined.json)
  OUT_FILE="output.txt"
  DBG_FILE="debug.txt"
fi

# remove old files
rm -f "$OUT_FILE"
if $DEBUG; then rm -f "$DBG_FILE"; fi

# Run and optionally time
if $DEBUG; then
  SECONDS=0
  "${CMD[@]}" > "$OUT_FILE"
  echo "$SECONDS" > "$DBG_FILE"
else
  "${CMD[@]}" > "$OUT_FILE"
fi
