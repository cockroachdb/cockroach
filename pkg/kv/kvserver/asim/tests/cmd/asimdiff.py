# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#!/usr/bin/env python3
import re
import sys
import argparse

def extract_value(line, metric):
    """Extract the value for a specific metric (sum, mean, or stddev) from a line"""
    if metric == 'sum':
        pattern = r'sum=(\d+)%?'
    elif metric == 'mean':
        pattern = r'mean=([\d.]+)%?'
    elif metric == 'stddev':
        pattern = r'stddev=([\d.]+)%?'
    else:
        return None

    match = re.search(pattern, line)
    if match:
        value = match.group(1)
        # Return as float for mean/stddev, int for sum
        return float(value) if metric in ['mean', 'stddev'] else int(value)
    return None

def process_diff(diff_content, keyword, metric):
    """Process git diff and extract changes for lines containing keyword"""
    lines = diff_content.split('\n')
    results = []
    total_before = 0
    total_after = 0
    current_file = None

    for line in lines:
        # Track current file being processed
        if line.startswith('diff --git') or line.startswith('+++'):
            if line.startswith('+++'):
                # Extract filename from +++ b/path/to/file
                match = re.search(r'\+\+\+ b/(.+)', line)
                if match:
                    current_file = match.group(1)

        if keyword not in line:
            continue

        if line.startswith('-') and not line.startswith('---'):
            # Removed line (before)
            value = extract_value(line, metric)
            if value is not None:
                total_before += value
                results.append({
                    'type': 'before',
                    'line': line[1:].strip(),
                    'value': value,
                    'file': current_file
                })
        elif line.startswith('+') and not line.startswith('+++'):
            # Added line (after)
            value = extract_value(line, metric)
            if value is not None:
                total_after += value
                results.append({
                    'type': 'after',
                    'line': line[1:].strip(),
                    'value': value,
                    'file': current_file
                })

    return results, total_before, total_after

def print_results(results, total_before, total_after, metric):
    """Print formatted results"""
    # Group before/after pairs
    before_lines = [r for r in results if r['type'] == 'before']
    after_lines = [r for r in results if r['type'] == 'after']

    # Collect individual diffs for statistics and sorting
    entries = []

    # Pair up changes and calculate diffs
    max_len = max(len(before_lines), len(after_lines))
    for i in range(max_len):
        before_val = before_lines[i]['value'] if i < len(before_lines) else 0
        after_val = after_lines[i]['value'] if i < len(after_lines) else 0
        diff = after_val - before_val

        full_path = after_lines[i]['file'] if i < len(after_lines) else (before_lines[i]['file'] if i < len(before_lines) else 'unknown')
        filename = full_path.split('/')[-1] if full_path else 'unknown'
        line_content = after_lines[i]['line'] if i < len(after_lines) else ''

        entries.append({
            'filename': filename,
            'before': before_val,
            'after': after_val,
            'diff': diff,
            'line': line_content
        })

    # Sort by absolute value of diff (largest to smallest)
    entries.sort(key=lambda x: abs(x['diff']), reverse=True)

    # Determine format string based on metric type
    if metric == 'sum':
        fmt = lambda x: f"{x:.0f}"
    else:  # mean or stddev
        fmt = lambda x: f"{x:.2f}"

    # Print sorted entries
    diffs = []
    for entry in entries:
        diffs.append(entry['diff'])
        pct_diff = (entry['diff'] / entry['before'] * 100) if entry['before'] != 0 else 0
        print(f"[{entry['filename']}] before = {fmt(entry['before'])},  after = {fmt(entry['after'])},  diff = {entry['diff']:+.2f} ({pct_diff:+.1f}%)")
        print(f"  {entry['line']}")

    # Calculate statistics
    total_diff = total_after - total_before
    avg_diff = sum(diffs) / len(diffs) if diffs else 0
    max_diff = max(diffs) if diffs else 0
    min_diff = min(diffs) if diffs else 0

    # Calculate percent change
    pct_change = (total_diff / total_before * 100) if total_before > 0 else 0

    # Count regressions and improvements
    regressions = sum(1 for d in diffs if d > 0)
    improvements = sum(1 for d in diffs if d < 0)
    unchanged = sum(1 for d in diffs if d == 0)

    print(f"total diff = {total_diff:+.2f}  (total before = {fmt(total_before)}, total after = {fmt(total_after)})")
    print(f"percent change = {pct_change:+.1f}%")
    print(f"avg diff = {avg_diff:+.2f},  max diff = {max_diff:+.2f},  min diff = {min_diff:+.2f}")
    print(f"changes: {regressions} regressions, {improvements} improvements, {unchanged} unchanged")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Analyze git diff for lines containing a keyword with metric values',
        epilog='Example: git diff | python %(prog)s thrash_pct --metric sum'
    )
    parser.add_argument('keyword', help='Keyword to search for in diff lines (e.g., thrash_pct)')
    parser.add_argument('--metric', '-m', choices=['sum', 'mean', 'stddev'], default='sum',
                        help='Metric to extract and compare (default: sum)')
    parser.add_argument('--file', '-f', help='Diff file to read (if not provided, reads from stdin)')

    args = parser.parse_args()

    # Read from stdin or file
    if args.file:
        with open(args.file, 'r') as f:
            diff_content = f.read()
    else:
        diff_content = sys.stdin.read()

    results, total_before, total_after = process_diff(diff_content, args.keyword, args.metric)

    if not results:
        print(f"No lines containing '{args.keyword}' with {args.metric} values found in the diff!")
        sys.exit(1)

    print_results(results, total_before, total_after, args.metric)
