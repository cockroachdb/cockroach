#!/usr/bin/env python3
#
# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.
"""
Compare transitive dependencies for all go_test targets between two git commits.

For each test target in //pkg/..., computes the set of transitive //pkg/...
dependencies at each commit and reports additions and removals. This is useful
for detecting lost init-hook registrations when moving packages.

Bazel query results are cached per commit SHA under ~/.cache/dep-diff/ so that
re-runs with different flags or a changed commit avoid redundant work.

Usage:
    ./scripts/dep-diff.py <commit-a> <commit-b>

Example:
    ./scripts/dep-diff.py HEAD~1 HEAD
    ./scripts/dep-diff.py HEAD~1 HEAD --only-removed --no-tests
    ./scripts/dep-diff.py HEAD~1 HEAD --no-cache
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from collections import defaultdict

CACHE_DIR = os.path.join(os.path.expanduser("~"), ".cache", "dep-diff")

_VAR_TMP = "/private/var/tmp"
_TMP_ROOT = _VAR_TMP if os.path.isdir(_VAR_TMP) else tempfile.gettempdir()
OUTPUT_BASE = os.path.join(_TMP_ROOT, "dep-diff-output-base")


def resolve_commit(ref, repo_root):
    """Resolve a git ref to a full SHA."""
    r = subprocess.run(
        ["git", "rev-parse", ref],
        capture_output=True, text=True, cwd=repo_root, check=True,
    )
    return r.stdout.strip()


def cache_path(sha):
    return os.path.join(CACHE_DIR, f"{sha}.json")


def load_cache(sha):
    """Load cached query results for a commit, or None if not cached."""
    p = cache_path(sha)
    if not os.path.exists(p):
        return None
    try:
        with open(p) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def save_cache(sha, targets_raw, dot_raw):
    """Save raw bazel query output for a commit."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(cache_path(sha), "w") as f:
        json.dump({"targets": targets_raw, "dot": dot_raw}, f)


def create_worktree(repo_root, commit, path):
    subprocess.run(
        [
            "git",
            "-c", "core.hooksPath=/dev/null",
            "-c", "submodule.recurse=false",
            "-c", "fetch.recurseSubmodules=false",
            "worktree", "add", "--detach", path, commit,
        ],
        cwd=repo_root, check=True,
        capture_output=True,
    )


def remove_worktree(repo_root, path):
    subprocess.run(
        ["git", "worktree", "remove", "--force", path],
        cwd=repo_root,
        capture_output=True,
    )


def get_repository_cache(repo_root):
    """Discover the main workspace's bazel repository cache."""
    r = subprocess.run(
        ["bazel", "info", "repository_cache"],
        capture_output=True, text=True, cwd=repo_root,
    )
    if r.returncode == 0 and r.stdout.strip():
        return r.stdout.strip()
    return None


def bazel_query(expr, output_format, worktree, output_base, repository_cache=None):
    """Run a bazel query in the given worktree with a dedicated output base."""
    cmd = [
        "bazel",
        f"--output_base={output_base}",
        "query",
        f"--output={output_format}",
    ]
    if repository_cache:
        cmd.append(f"--repository_cache={repository_cache}")
        cmd.append("--experimental_repository_cache_hardlinks")
    if output_format == "graph":
        cmd.append("--nograph:factored")
    cmd.append(expr)
    r = subprocess.run(
        cmd,
        capture_output=True, text=True, cwd=worktree,
    )
    if r.returncode != 0:
        print(f"bazel query failed:\n{r.stderr}", file=sys.stderr)
        sys.exit(1)
    return r.stdout


def query_commit(sha, worktree, output_base, repository_cache=None):
    """Run bazel queries for a commit and return raw output strings."""
    targets_raw = bazel_query(
        'kind("go_test", //pkg/...)',
        "label",
        worktree,
        output_base,
        repository_cache,
    )
    dot_raw = bazel_query(
        'deps(kind("go_test", //pkg/...)) intersect //pkg/...',
        "graph",
        worktree,
        output_base,
        repository_cache,
    )
    return targets_raw, dot_raw


def parse_targets(targets_raw):
    return {t for t in targets_raw.strip().split("\n") if t}


def parse_dot(dot_content):
    graph = defaultdict(set)
    edge_re = re.compile(r'"([^"]+)"\s*->\s*"([^"]+)"')
    for m in edge_re.finditer(dot_content):
        graph[m.group(1)].add(m.group(2))
    # Ensure all nodes exist as keys even if they have no outgoing edges.
    node_re = re.compile(r'"([^"]+)"\s*\[')
    for m in node_re.finditer(dot_content):
        graph.setdefault(m.group(1), set())
    return graph


def transitive_deps(graph, start):
    """BFS to collect transitive deps."""
    visited = set()
    stack = [start]
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        for dep in graph.get(node, set()):
            if dep not in visited:
                stack.append(dep)
    visited.discard(start)
    return visited


def get_commit_data(sha, repo_root, use_cache, output_base, repository_cache=None):
    """Get targets and graph for a commit, using cache when available.

    output_base is the shared bazel output base directory used for queries.
    repository_cache, if set, points at the main workspace's repo cache so
    external repos aren't re-downloaded.
    """
    if use_cache:
        cached = load_cache(sha)
        if cached is not None:
            print(f"  Using cached data for {sha[:12]}", file=sys.stderr)
            targets = parse_targets(cached["targets"])
            graph = parse_dot(cached["dot"])
            return targets, graph

    # Need to query bazel. Create a worktree.
    wt = tempfile.mkdtemp(prefix=f"dep-diff-wt-{sha[:12]}-")

    try:
        print(f"  Creating worktree for {sha[:12]}...", file=sys.stderr)
        create_worktree(repo_root, sha, wt)

        print(f"  Querying bazel for {sha[:12]}...", file=sys.stderr)
        targets_raw, dot_raw = query_commit(sha, wt, output_base, repository_cache)

        if use_cache:
            save_cache(sha, targets_raw, dot_raw)
            print(f"  Cached results for {sha[:12]}", file=sys.stderr)

        targets = parse_targets(targets_raw)
        graph = parse_dot(dot_raw)
        return targets, graph
    finally:
        remove_worktree(repo_root, wt)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("commit_a", help="Base commit (before)")
    parser.add_argument("commit_b", help="Head commit (after)")
    parser.add_argument("--only-removed", action="store_true",
                        help="Only show targets that lost dependencies")
    parser.add_argument("--no-tests", action="store_true",
                        help="Skip targets ending in _test")
    parser.add_argument("--no-cache", action="store_true",
                        help="Ignore and don't write cache")
    parser.add_argument("--lost-dep", metavar="LABEL",
                        help="Only show targets that lost this specific dep "
                             "(e.g. //pkg/cloud/impl:cloudimpl)")
    args = parser.parse_args()

    repo_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()

    sha_a = resolve_commit(args.commit_a, repo_root)
    sha_b = resolve_commit(args.commit_b, repo_root)
    use_cache = not args.no_cache
    print(f"Comparing deps: {sha_a[:12]} -> {sha_b[:12]}", file=sys.stderr)
    print(f"Output base: {OUTPUT_BASE}", file=sys.stderr)

    # Use a persistent output base so external repos are downloaded once and
    # reused across runs. Queries run sequentially so there's no conflict.
    os.makedirs(OUTPUT_BASE, exist_ok=True)

    repo_cache = get_repository_cache(repo_root)
    if repo_cache:
        print(f"Using repository cache: {repo_cache}", file=sys.stderr)

    targets_a, graph_a = get_commit_data(
        sha_a, repo_root, use_cache, OUTPUT_BASE, repo_cache)
    print(f"  {sha_a[:12]}: {len(targets_a)} targets, {len(graph_a)} nodes",
          file=sys.stderr)

    targets_b, graph_b = get_commit_data(
        sha_b, repo_root, use_cache, OUTPUT_BASE, repo_cache)
    print(f"  {sha_b[:12]}: {len(targets_b)} targets, {len(graph_b)} nodes",
          file=sys.stderr)

    print("Computing diffs...", file=sys.stderr)
    all_targets = sorted(targets_a | targets_b)
    found_diff = False

    for target in all_targets:
        if args.no_tests and target.endswith("_test"):
            continue
        if target in targets_a and target not in targets_b:
            if not args.only_removed:
                print(f"\n=== {target} ===")
                print("  (target removed)")
                found_diff = True
            continue
        if target not in targets_a and target in targets_b:
            if not args.only_removed:
                print(f"\n=== {target} ===")
                print("  (target added)")
                found_diff = True
            continue

        deps_a = transitive_deps(graph_a, target)
        deps_b = transitive_deps(graph_b, target)
        removed = deps_a - deps_b
        added = deps_b - deps_a

        if args.lost_dep:
            if args.lost_dep not in removed:
                continue
        elif args.only_removed and not removed:
            continue

        if added or removed:
            found_diff = True
            print(f"\n=== {target} ===")
            for d in sorted(removed):
                print(f"  - {d}")
            if not args.only_removed:
                for d in sorted(added):
                    print(f"  + {d}")

    if not found_diff:
        print("\nNo dependency changes found.", file=sys.stderr)


if __name__ == "__main__":
    main()
