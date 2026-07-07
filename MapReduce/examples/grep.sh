#!/usr/bin/env bash
# Distributed Grep — Map phase (Hadoop streaming example)
#
# Filters stdin for lines matching a pattern, emits filename\tline pairs.
#
# Usage:
#   GREP_PATTERN="error" cat input.txt | bash grep.sh map
#   cat intermediate.txt | bash grep.sh reduce

PATTERN="${GREP_PATTERN:-error}"

if [[ "$1" == "map" ]]; then
    grep -i "$PATTERN" || true
elif [[ "$1" == "reduce" ]]; then
    # Identity reducer: pass through all matching lines
    cat
else
    echo "Usage: $0 <map|reduce>" >&2
    exit 1
fi
