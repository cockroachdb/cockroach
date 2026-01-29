#!/usr/bin/env python3

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

"""
Size Analysis Tool for CockroachDB debug.zip files.

Shows breakdown of zip contents by file type. With --logs, also analyzes
log files to show top source locations (file:line) by volume.

Handles truncated zip files (missing EOCD) by scanning Central Directory
entries from the end of the file. Supports ZIP64.

Usage:
    python3 scripts/zipsize.py debug.zip              # Just show file type breakdown
    python3 scripts/zipsize.py debug.zip --logs       # Also analyze log locations
    python3 scripts/zipsize.py debug.zip --logs --limit=100  # Sample 100 log files
"""

import argparse
import os
import random
import re
import struct
import sys
import zipfile
import zlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

# Pre-compiled regex patterns for performance
# Log line boundary: newline followed by severity letter and 6-digit date
LOG_LINE_BOUNDARY = re.compile(r'\n(?=[IWEF]\d{6})')

# Location pattern: optional goroutine prefix, then file.go:line
# Examples: "3@pebble/event.go:1043" or "kv/kvserver/store.go:892"
LOCATION_PATTERN = re.compile(r'(\d+@)?([a-zA-Z0-9_/.-]+\.go:\d+)')

# ZIP signatures
CD_SIGNATURE = b'PK\x01\x02'  # Central Directory entry
LOCAL_SIGNATURE = b'PK\x03\x04'  # Local file header


@dataclass
class CDEntry:
    """Central Directory entry parsed from a truncated zip."""
    filename: str
    compress_method: int
    compressed_size: int
    uncompressed_size: int
    local_header_offset: int


def parse_zip64_extra(extra_data: bytes, needs_uncomp: bool, needs_comp: bool, needs_offset: bool) -> Tuple[int, int, int]:
    """
    Parse ZIP64 extended information extra field.

    The ZIP64 extra field (tag 0x0001) contains 64-bit values for fields
    that were set to 0xFFFFFFFF in the central directory.

    Returns: (uncompressed_size, compressed_size, local_header_offset)
    """
    uncomp_size = 0
    comp_size = 0
    local_offset = 0

    pos = 0
    while pos + 4 <= len(extra_data):
        tag, size = struct.unpack('<HH', extra_data[pos:pos + 4])
        pos += 4

        if tag == 0x0001:  # ZIP64 extended information
            field_pos = pos
            # Fields are present in order: uncompressed, compressed, offset, disk
            # but only if the corresponding CD field was 0xFFFFFFFF
            if needs_uncomp and field_pos + 8 <= pos + size:
                uncomp_size = struct.unpack('<Q', extra_data[field_pos:field_pos + 8])[0]
                field_pos += 8
            if needs_comp and field_pos + 8 <= pos + size:
                comp_size = struct.unpack('<Q', extra_data[field_pos:field_pos + 8])[0]
                field_pos += 8
            if needs_offset and field_pos + 8 <= pos + size:
                local_offset = struct.unpack('<Q', extra_data[field_pos:field_pos + 8])[0]
                field_pos += 8
            break

        pos += size

    return uncomp_size, comp_size, local_offset


def scan_central_directory(filepath: str, scan_bytes: int = 200 * 1024 * 1024) -> List[CDEntry]:
    """
    Scan the end of a file for Central Directory entries.

    This handles truncated zip files that are missing the EOCD record.
    We scan backwards from the end looking for CD entry signatures.
    Supports ZIP64 format for large archives.

    Args:
        filepath: Path to the zip file
        scan_bytes: How many bytes from the end to scan (default 200MB)

    Returns:
        List of CDEntry objects found
    """
    entries = []
    file_size = os.path.getsize(filepath)
    scan_start = max(0, file_size - scan_bytes)

    with open(filepath, 'rb') as f:
        f.seek(scan_start)
        data = f.read()

    # Find all CD signatures
    pos = 0
    while True:
        idx = data.find(CD_SIGNATURE, pos)
        if idx == -1:
            break

        # CD entry structure (46 bytes minimum + variable filename/extra/comment):
        # 4 bytes: signature (PK\x01\x02)
        # 2 bytes: version made by
        # 2 bytes: version needed
        # 2 bytes: flags
        # 2 bytes: compression method
        # 2 bytes: mod time
        # 2 bytes: mod date
        # 4 bytes: crc32
        # 4 bytes: compressed size
        # 4 bytes: uncompressed size
        # 2 bytes: filename length
        # 2 bytes: extra field length
        # 2 bytes: comment length
        # 2 bytes: disk number start
        # 2 bytes: internal attributes
        # 4 bytes: external attributes
        # 4 bytes: local header offset
        # N bytes: filename
        # M bytes: extra field
        # K bytes: comment

        if idx + 46 > len(data):
            pos = idx + 1
            continue

        try:
            (sig, ver_made, ver_needed, flags, method, mod_time, mod_date,
             crc, comp_size, uncomp_size, name_len, extra_len, comment_len,
             disk_start, int_attr, ext_attr, local_offset) = struct.unpack(
                '<4sHHHHHHIIIHHHHHII', data[idx:idx + 46]
            )

            if sig != CD_SIGNATURE:
                pos = idx + 1
                continue

            # Sanity checks
            if name_len > 1024 or extra_len > 65535 or comment_len > 65535:
                pos = idx + 1
                continue

            if idx + 46 + name_len + extra_len > len(data):
                pos = idx + 1
                continue

            filename = data[idx + 46:idx + 46 + name_len].decode('utf-8', errors='replace')

            # Handle ZIP64: if any size/offset field is 0xFFFFFFFF, read from extra field
            if uncomp_size == 0xFFFFFFFF or comp_size == 0xFFFFFFFF or local_offset == 0xFFFFFFFF:
                extra_data = data[idx + 46 + name_len:idx + 46 + name_len + extra_len]
                z64_uncomp, z64_comp, z64_offset = parse_zip64_extra(
                    extra_data,
                    needs_uncomp=(uncomp_size == 0xFFFFFFFF),
                    needs_comp=(comp_size == 0xFFFFFFFF),
                    needs_offset=(local_offset == 0xFFFFFFFF)
                )
                if uncomp_size == 0xFFFFFFFF:
                    uncomp_size = z64_uncomp
                if comp_size == 0xFFFFFFFF:
                    comp_size = z64_comp
                if local_offset == 0xFFFFFFFF:
                    local_offset = z64_offset

            entries.append(CDEntry(
                filename=filename,
                compress_method=method,
                compressed_size=comp_size,
                uncompressed_size=uncomp_size,
                local_header_offset=local_offset
            ))

            # Move past this entry
            pos = idx + 46 + name_len + extra_len + comment_len

        except struct.error:
            pos = idx + 1
            continue

    return entries


def read_local_file(filepath: str, entry: CDEntry) -> Optional[bytes]:
    """
    Read file data from a local file header in the zip.

    Args:
        filepath: Path to the zip file
        entry: CDEntry with the local header offset

    Returns:
        Decompressed file contents, or None on error
    """
    try:
        with open(filepath, 'rb') as f:
            f.seek(entry.local_header_offset)
            header = f.read(30)

            if len(header) < 30 or header[:4] != LOCAL_SIGNATURE:
                return None

            # Local file header structure:
            # 4 bytes: signature
            # 2 bytes: version needed
            # 2 bytes: flags
            # 2 bytes: compression method
            # 2 bytes: mod time
            # 2 bytes: mod date
            # 4 bytes: crc32
            # 4 bytes: compressed size
            # 4 bytes: uncompressed size
            # 2 bytes: filename length
            # 2 bytes: extra field length
            (sig, ver, flags, method, mod_time, mod_date, crc,
             comp_size, uncomp_size, name_len, extra_len) = struct.unpack(
                '<4sHHHHHIIIHH', header
            )

            # Skip filename and extra field
            f.seek(entry.local_header_offset + 30 + name_len + extra_len)

            # Use sizes from CD entry (more reliable for truncated files)
            compressed_data = f.read(entry.compressed_size)

            if len(compressed_data) < entry.compressed_size:
                # File data is truncated, try to decompress what we have
                pass

            if entry.compress_method == 0:  # Stored
                return compressed_data
            elif entry.compress_method == 8:  # Deflated
                try:
                    # -15 for raw deflate (no zlib header)
                    return zlib.decompress(compressed_data, -15)
                except zlib.error:
                    # Try with partial data
                    try:
                        decompressor = zlib.decompressobj(-15)
                        return decompressor.decompress(compressed_data)
                    except zlib.error:
                        return None
            else:
                return None

    except (OSError, struct.error) as e:
        return None


@dataclass
class LocationStats:
    """Statistics for a single log location."""
    count: int
    total_bytes: int
    example: str


def parse_log_content(content: str) -> Dict[str, LocationStats]:
    """
    Parse log content and aggregate by location.

    Returns a dict mapping location -> LocationStats.
    """
    results: Dict[str, LocationStats] = {}

    # Split on log line boundaries
    lines = LOG_LINE_BOUNDARY.split(content)

    for line in lines:
        if not line:
            continue

        # Extract location from the line
        # Location appears after the timestamp and goroutine ID
        # Format: I251114 11:24:49.388377 100 3@pebble/event.go:1043 ...
        match = LOCATION_PATTERN.search(line)
        if not match:
            continue

        location = match.group(2)  # Get the file:line part without goroutine prefix
        line_bytes = len(line.encode('utf-8', errors='replace'))

        if location in results:
            stats = results[location]
            stats.count += 1
            stats.total_bytes += line_bytes
        else:
            # Extract example: show content after the first ], then strip digits
            if ']' in line:
                example = line.split(']', 1)[1].strip()
                # Strip leading digits and whitespace (e.g., "12345 content" -> "content")
                example = re.sub(r'^\d+\s*', '', example)
            else:
                example = line.strip()
            # Store up to 1000 chars (display will truncate further based on --example)
            example = example[:1000]
            # Clean up example (remove newlines)
            example = example.replace('\n', ' ').strip()
            results[location] = LocationStats(
                count=1,
                total_bytes=line_bytes,
                example=example
            )

    return results


def process_file_standard(args: Tuple[str, str]) -> Tuple[str, Dict[str, Tuple[int, int, str]], int]:
    """
    Process a single log file from a standard (non-truncated) zip archive.

    Args:
        args: Tuple of (zip_path, file_name)

    Returns:
        Tuple of (file_name, location_stats_dict, line_count)
        where location_stats_dict maps location -> (count, bytes, example)
    """
    zip_path, file_name = args

    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            with zf.open(file_name) as f:
                content = f.read().decode('utf-8', errors='replace')
    except Exception as e:
        print(f"Warning: Failed to read {file_name}: {e}", file=sys.stderr)
        return (file_name, {}, 0)

    stats = parse_log_content(content)

    # Convert to serializable format for IPC
    result = {
        loc: (s.count, s.total_bytes, s.example)
        for loc, s in stats.items()
    }

    total_lines = sum(s.count for s in stats.values())

    return (file_name, result, total_lines)


def process_file_truncated(args: Tuple[str, str, int, int, int]) -> Tuple[str, Dict[str, Tuple[int, int, str]], int]:
    """
    Process a single log file from a truncated zip archive using CD entry info.

    Args:
        args: Tuple of (zip_path, file_name, compress_method, compressed_size, local_offset)

    Returns:
        Tuple of (file_name, location_stats_dict, line_count)
        where location_stats_dict maps location -> (count, bytes, example)
    """
    zip_path, file_name, compress_method, compressed_size, local_offset = args

    entry = CDEntry(
        filename=file_name,
        compress_method=compress_method,
        compressed_size=compressed_size,
        uncompressed_size=0,  # Not needed for reading
        local_header_offset=local_offset
    )

    data = read_local_file(zip_path, entry)
    if data is None:
        return (file_name, {}, 0)

    try:
        content = data.decode('utf-8', errors='replace')
    except Exception as e:
        print(f"Warning: Failed to decode {file_name}: {e}", file=sys.stderr)
        return (file_name, {}, 0)

    stats = parse_log_content(content)

    # Convert to serializable format for IPC
    result = {
        loc: (s.count, s.total_bytes, s.example)
        for loc, s in stats.items()
    }

    total_lines = sum(s.count for s in stats.values())

    return (file_name, result, total_lines)


def merge_results(
    all_results: List[Dict[str, Tuple[int, int, str]]]
) -> Dict[str, Tuple[int, int, str]]:
    """
    Merge results from multiple workers.

    Aggregates counts and bytes, keeps first example seen.
    """
    merged: Dict[str, Tuple[int, int, str]] = {}

    for result in all_results:
        for location, (count, bytes_, example) in result.items():
            if location in merged:
                old_count, old_bytes, old_example = merged[location]
                merged[location] = (old_count + count, old_bytes + bytes_, old_example)
            else:
                merged[location] = (count, bytes_, example)

    return merged


def format_bytes(num_bytes: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(num_bytes) < 1024:
            return f"{num_bytes:.1f}{unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f}PB"


def format_count(n: int) -> str:
    """Format count as human-readable string (e.g., 200k, 2.4M)."""
    if n < 1000:
        return str(n)
    elif n < 1000000:
        return f"{n/1000:.1f}k"
    elif n < 1000000000:
        return f"{n/1000000:.1f}M"
    else:
        return f"{n/1000000000:.1f}B"


def is_log_file(filename: str) -> bool:
    """Check if a filename is a log file we want to analyze."""
    # Match patterns like nodes/*/logs/*.log or nodes/*/logs/*.parquet
    return '/logs/' in filename and (filename.endswith('.log') or filename.endswith('.parquet'))


def categorize_file(filename: str) -> str:
    """
    Categorize a file from the debug zip.

    Categories:
    - cluster: files not in nodes/* (cluster-level info)
    - node_logs: nodes/*/logs/*
    - node_tables: nodes/*/crdb_internal.*.txt
    - range_info: nodes/*/ranges/*.json or nodes/*/ranges.json
    - node_stacks: nodes/*/stacks*
    - heapprof_current: nodes/*/heap.pprof
    - heapprof_history: nodes/*/heapprof/*
    - cpuprof: nodes/*/cpu.pprof
    - gossip: nodes/*/gossip.json
    - goroutine_history: nodes/*/goroutines/*
    - lsm: nodes/*/lsm.txt
    - uncategorized: anything else
    """
    # Normalize path separators
    path = filename.replace('\\', '/')

    # Check if it's under nodes/
    if '/nodes/' not in path and not path.startswith('nodes/'):
        # Handle debug/nodes/ prefix
        if '/debug/' in path:
            after_debug = path.split('/debug/', 1)[1] if '/debug/' in path else path
            if not after_debug.startswith('nodes/'):
                return 'cluster'
        else:
            return 'cluster'

    # It's a node file - categorize further
    if '/logs/' in path and (path.endswith('.log') or path.endswith('.parquet')):
        return 'node_logs'
    elif 'crdb_internal.' in path and path.endswith('.txt'):
        return 'node_tables'
    elif '/ranges/' in path or path.endswith('ranges.json'):
        return 'range_info'
    elif '/stacks' in path or 'stacks.' in path:
        return 'node_stacks'
    elif path.endswith('/heap.pprof'):
        return 'heapprof_current'
    elif '/heapprof/' in path:
        return 'heapprof_history'
    elif path.endswith('/cpu.pprof'):
        return 'cpuprof'
    elif path.endswith('/gossip.json'):
        return 'gossip'
    elif '/goroutines/' in path:
        return 'goroutine_history'
    elif path.endswith('/lsm.txt'):
        return 'lsm'
    else:
        return 'uncategorized'


@dataclass
class CategoryStats:
    """Statistics for a file category."""
    count: int = 0
    compressed_bytes: int = 0
    uncompressed_bytes: int = 0


def compute_zip_summary(entries: List[CDEntry]) -> Dict[str, CategoryStats]:
    """Compute summary statistics by file category."""
    stats: Dict[str, CategoryStats] = {
        'cluster': CategoryStats(),
        'node_logs': CategoryStats(),
        'node_tables': CategoryStats(),
        'range_info': CategoryStats(),
        'node_stacks': CategoryStats(),
        'heapprof_current': CategoryStats(),
        'heapprof_history': CategoryStats(),
        'cpuprof': CategoryStats(),
        'gossip': CategoryStats(),
        'goroutine_history': CategoryStats(),
        'lsm': CategoryStats(),
        'uncategorized': CategoryStats(),
    }

    for entry in entries:
        cat = categorize_file(entry.filename)
        stats[cat].count += 1
        stats[cat].compressed_bytes += entry.compressed_size
        stats[cat].uncompressed_bytes += entry.uncompressed_size

    return stats


def print_zip_summary(entries: List[CDEntry], show_all: bool = False) -> None:
    """Print a summary of the zip contents by category.

    Args:
        entries: List of CDEntry objects from the zip
        show_all: If True, show all categories. If False, roll up categories <1% into "Other".
    """
    stats = compute_zip_summary(entries)

    total_count = sum(s.count for s in stats.values())
    total_compressed = sum(s.compressed_bytes for s in stats.values())
    total_uncompressed = sum(s.uncompressed_bytes for s in stats.values())

    print()
    print()
    print("* Zip Contents Summary")
    print("=" * 30)
    print()
    header = f"{'Category':<20} {'Size':>12} {'%':>6} {'Compressed':>12} {'Count':>10}"
    print(header)
    print("-" * len(header))

    labels = {
        'cluster': 'Cluster files',
        'node_logs': 'Node logs',
        'node_tables': 'Node tables',
        'range_info': 'Range info',
        'node_stacks': 'Node stacks',
        'heapprof_current': 'Heap profiles',
        'heapprof_history': 'Hist. heap profiles',
        'cpuprof': 'CPU profiles',
        'gossip': 'Gossip',
        'goroutine_history': 'Goroutine history',
        'lsm': 'LSM info',
        'uncategorized': 'Uncategorized',
    }

    # Sort by uncompressed size descending
    sorted_cats = sorted(stats.items(), key=lambda x: x[1].uncompressed_bytes, reverse=True)

    # Track "Other" rollup for small categories
    other_count = 0
    other_compressed = 0
    other_uncompressed = 0

    for cat, s in sorted_cats:
        if s.count == 0:
            continue
        pct = (s.uncompressed_bytes / total_uncompressed * 100) if total_uncompressed > 0 else 0

        # Roll up small categories into "Other" unless show_all is set
        if not show_all and pct < 1.0:
            other_count += s.count
            other_compressed += s.compressed_bytes
            other_uncompressed += s.uncompressed_bytes
            continue

        print(f"{labels[cat]:<20} {format_bytes(s.uncompressed_bytes):>12} {pct:>5.1f}% {format_bytes(s.compressed_bytes):>12} {format_count(s.count):>10}")

    # Print "Other" rollup if there are any
    if other_count > 0:
        other_pct = (other_uncompressed / total_uncompressed * 100) if total_uncompressed > 0 else 0
        print(f"{'Other':<20} {format_bytes(other_uncompressed):>12} {other_pct:>5.1f}% {format_bytes(other_compressed):>12} {format_count(other_count):>10}")

    print("-" * len(header))
    print(f"{'Total':<20} {format_bytes(total_uncompressed):>12} 100.0% {format_bytes(total_compressed):>12} {format_count(total_count):>10}")
    print()


def find_log_files_standard(zip_path: str) -> Tuple[List[CDEntry], List[str]]:
    """
    Find all log files in a standard zip archive.

    Returns:
        Tuple of (all_entries, log_file_names)
    """
    all_entries = []
    log_files = []

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for info in zf.infolist():
            entry = CDEntry(
                filename=info.filename,
                compress_method=info.compress_type,
                compressed_size=info.compress_size,
                uncompressed_size=info.file_size,
                local_header_offset=info.header_offset
            )
            all_entries.append(entry)
            if is_log_file(info.filename):
                log_files.append(info.filename)

    return all_entries, log_files


def find_log_files_truncated(zip_path: str) -> Tuple[List[CDEntry], List[CDEntry]]:
    """
    Find all log files in a truncated zip archive by scanning CD entries.

    Returns:
        Tuple of (all_entries, log_entries)
    """
    print("Zip appears truncated, scanning for CD entries...", end='', flush=True)
    entries = scan_central_directory(zip_path)
    print(f" found {len(entries):,}.")

    log_entries = [e for e in entries if is_log_file(e.filename)]
    return entries, log_entries


def check_zip_valid(zip_path: str) -> bool:
    """Check if a zip file has a valid EOCD (is not truncated)."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Try to read the file list - this will fail if EOCD is missing
            _ = zf.namelist()
        return True
    except zipfile.BadZipFile:
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Analyze size breakdown of debug.zip archive. Use --logs to analyze log line locations.'
    )
    parser.add_argument('zipfile', help='Path to debug.zip file')
    parser.add_argument(
        '--logs', action='store_true',
        help='Analyze log files by source location (file:line)'
    )
    parser.add_argument(
        '--top', type=int, default=10,
        help='With --logs: top locations by bytes (default: 10, 0 to skip)'
    )
    parser.add_argument(
        '--top-count', type=int, default=0,
        help='With --logs: top locations by count (default: 0, 0 to skip)'
    )
    parser.add_argument(
        '--limit', type=int, default=None,
        help='With --logs: limit number of log files to process'
    )
    parser.add_argument(
        '--example', type=int, default=65,
        help='With --logs: example line length (0 to hide, default: 65)'
    )
    parser.add_argument(
        '--other', action='store_true',
        help='Show all categories (by default, categories <1%% are rolled into "Other")'
    )
    parser.add_argument(
        '--print-uncategorized', type=int, default=0, metavar='N',
        help=argparse.SUPPRESS
    )

    args = parser.parse_args()

    if not os.path.exists(args.zipfile):
        print(f"Error: File not found: {args.zipfile}", file=sys.stderr)
        sys.exit(1)

    # Check if zip is valid or truncated
    is_truncated = not check_zip_valid(args.zipfile)

    # Scan zip entries
    if is_truncated:
        all_entries, log_entries = find_log_files_truncated(args.zipfile)
    else:
        print("Reading zip entries...", end='', flush=True)
        all_entries, log_files = find_log_files_standard(args.zipfile)
        print(f" found {len(all_entries):,}.")

    # If --logs, do log analysis
    analyzed = False
    if args.logs:
        if is_truncated:
            if not log_entries:
                print("No log files found in archive.", file=sys.stderr)
            else:
                total_log_files = len(log_entries)
                if args.limit and args.limit < len(log_entries):
                    random.shuffle(log_entries)
                    log_entries = log_entries[:args.limit]
                    print(f"Analyzing {len(log_entries):,} log files (sampled from {total_log_files:,})...", end='', flush=True)
                elif total_log_files > 1000:
                    print(f"Analyzing {total_log_files:,} log files (tip: --limit can sample a subset)...", end='', flush=True)
                else:
                    print(f"Analyzing {total_log_files:,} log files...", end='', flush=True)
                num_files = len(log_entries)
                work_items = [
                    (args.zipfile, e.filename, e.compress_method, e.compressed_size, e.local_header_offset)
                    for e in log_entries
                ]
                process_func = process_file_truncated
                analyzed = True
        else:
            if not log_files:
                print("No log files found in archive.", file=sys.stderr)
            else:
                total_log_files = len(log_files)
                if args.limit and args.limit < len(log_files):
                    random.shuffle(log_files)
                    log_files = log_files[:args.limit]
                    print(f"Analyzing {len(log_files):,} log files (sampled from {total_log_files:,})...", end='', flush=True)
                elif total_log_files > 1000:
                    print(f"Analyzing {total_log_files:,} log files (tip: --limit can sample a subset)...", end='', flush=True)
                else:
                    print(f"Analyzing {total_log_files:,} log files...", end='', flush=True)
                num_files = len(log_files)
                work_items = [(args.zipfile, f) for f in log_files]
                process_func = process_file_standard
                analyzed = True

    # Process files in parallel (if analyzing)
    merged = None
    if analyzed:
        all_results = []
        total_lines = 0
        files_processed = 0

        workers = os.cpu_count() or 4

        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process_func, item): item for item in work_items}

            files_with_data = 0
            for future in as_completed(futures):
                file_name, result, lines = future.result()
                all_results.append(result)
                total_lines += lines
                files_processed += 1
                if lines > 0:
                    files_with_data += 1

        # Complete the "Analyzing..." line
        if files_with_data < num_files:
            print(f" done ({files_with_data:,} had data, {num_files - files_with_data:,} empty/truncated).")
        else:
            print(" done.")

        # Merge results
        merged = merge_results(all_results)
        total_bytes = sum(b for _, b, _ in merged.values())

    # Print zip summary (always)
    if all_entries:
        print_zip_summary(all_entries, show_all=args.other)

        # Print 'uncategorized' files if requested
        if args.print_uncategorized > 0:
            other_files = [e for e in all_entries if categorize_file(e.filename) == 'uncategorized']
            # Sort by uncompressed size descending
            other_files.sort(key=lambda e: e.uncompressed_size, reverse=True)
            if other_files:
                print()
                print(f"* Uncategorized files (top {min(args.print_uncategorized, len(other_files))} of {len(other_files)})")
                print("=" * 30)
                print()
                for entry in other_files[:args.print_uncategorized]:
                    print(f"  {format_bytes(entry.uncompressed_size):>10}  {entry.filename}")
                print()

    # Print log analysis summary (if analyzed)
    if analyzed and merged:
        print()
        print()
        print("* Log Analysis Summary")
        print("=" * 30)
        print()
        is_sampled = num_files < total_log_files
        if is_sampled:
            print(f"Sampled files:  {num_files:,} (of {total_log_files:,})")
            print(f"Sampled lines:  {total_lines:,}")
            print(f"Sampled bytes:  {format_bytes(total_bytes)}")
        else:
            print(f"Log files:  {num_files:,}")
            print(f"Total lines:  {total_lines:,}")
            print(f"Total bytes:  {format_bytes(total_bytes)}")
        print(f"Unique locations: {len(merged):,}")

        def print_top_table(title: str, sorted_locs: list, limit: int, example_len: int):
            """Print a table of top locations."""
            print()
            print()
            print(f"* {title}")
            print("=" * 30)
            print()
            show_example = example_len > 0
            example_on_own_line = example_len > 70

            if is_sampled:
                header = f"{'Location':<45} {'Count%':>8} {'Bytes%':>8} {'Avg':>8}"
                if show_example and not example_on_own_line:
                    header += "  Example"
                print(header)
                print("-" * len(header))
            else:
                header = f"{'Location':<45} {'Count':>10} {'%':>6} {'Bytes':>10} {'%':>6} {'Avg':>8}"
                if show_example and not example_on_own_line:
                    header += "  Example"
                print(header)
                print("-" * len(header))

            for location, (count, bytes_, example) in sorted_locs[:limit]:
                count_pct = (count / total_lines * 100) if total_lines > 0 else 0
                bytes_pct = (bytes_ / total_bytes * 100) if total_bytes > 0 else 0
                avg_bytes = bytes_ // count if count > 0 else 0

                # Truncate location if too long
                loc_display = location[:43] + '..' if len(location) > 45 else location

                if is_sampled:
                    line = (
                        f"{loc_display:<45} "
                        f"{count_pct:>7.1f}% "
                        f"{bytes_pct:>7.1f}% "
                        f"{format_bytes(avg_bytes):>8}"
                    )
                else:
                    line = (
                        f"{loc_display:<45} "
                        f"{format_count(count):>10} "
                        f"{count_pct:>5.1f}% "
                        f"{format_bytes(bytes_):>10} "
                        f"{bytes_pct:>5.1f}% "
                        f"{format_bytes(avg_bytes):>8}"
                    )

                if show_example:
                    example_display = example[:example_len] + '...' if len(example) > example_len else example
                    if example_on_own_line:
                        print(line)
                        print(f'\t\tEx: "{example_display}"')
                    else:
                        print(f"{line}  {example_display}")
                else:
                    print(line)


        # Top locations by bytes
        if args.top > 0:
            by_bytes = sorted(merged.items(), key=lambda x: x[1][1], reverse=True)
            print_top_table(f"Top {args.top} Locations by Bytes", by_bytes, args.top, args.example)

        # Top locations by count
        if args.top_count > 0:
            by_count = sorted(merged.items(), key=lambda x: x[1][0], reverse=True)
            print_top_table(f"Top {args.top_count} Locations by Count", by_count, args.top_count, args.example)


if __name__ == '__main__':
    main()
