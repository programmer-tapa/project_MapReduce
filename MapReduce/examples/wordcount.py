#!/usr/bin/env python3
"""
Word Count — Map phase (Hadoop streaming example)

Reads lines from stdin, emits tab-separated word\t1 pairs to stdout.

Usage:
    cat input.txt | python3 wordcount.py map
    cat intermediate.txt | python3 wordcount.py reduce
"""

import sys


def map_phase():
    """Emit (word, 1) for each word in the input."""
    for line in sys.stdin:
        for word in line.strip().split():
            # Clean: remove non-alpha characters, lowercase
            cleaned = ''.join(c for c in word if c.isalpha())
            if cleaned:
                print(f"{cleaned}\t1")


def reduce_phase():
    """Sum counts for each word. Input is sorted by key."""
    current_key = None
    current_count = 0

    for line in sys.stdin:
        parts = line.strip().split('\t', 1)
        if len(parts) != 2:
            continue
        key, value = parts

        if key == current_key:
            current_count += int(value)
        else:
            if current_key is not None:
                print(f"{current_key}\t{current_count}")
            current_key = key
            current_count = int(value)

    # Emit the last key
    if current_key is not None:
        print(f"{current_key}\t{current_count}")


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("map", "reduce"):
        print(f"Usage: {sys.argv[0]} <map|reduce>", file=sys.stderr)
        sys.exit(1)

    if sys.argv[1] == "map":
        map_phase()
    else:
        reduce_phase()
