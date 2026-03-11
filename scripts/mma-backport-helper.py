#!/usr/bin/env python3
"""
mma-backport-helper.py

Identifies MMA-related PRs on upstream/master since the release-26.2 branch
point that have NOT yet been backported to upstream/release-26.2.

The script works in three passes:
  1. Path-based: finds first-parent (merge) commits on master that touch
     known MMA-related directories.
  2. Title-based: finds first-parent commits whose commit message matches
     MMA-related keywords (catches PRs that only touch files outside the
     canonical directories, e.g. server plumbing, metrics, etc.).
  3. Author-based: finds PRs authored by MMA team members (catches PRs
     that don't touch MMA directories or use MMA keywords, e.g. supporting
     infrastructure changes).

It then extracts PR numbers from each commit's subject line and subtracts
the set of PRs already backported to release-26.2.

Output is in topological merge order (oldest first), which is the order
you'd want to backport in to minimize conflicts.

Usage:
  python3 scripts/mma-backport-helper.py [--fetch] [--upstream NAME]
"""

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Core MMA packages and their test data.
MMA_DIRS = [
    "pkg/kv/kvserver/allocator/mmaprototype",
    "pkg/kv/kvserver/allocator/allocatorimpl",
    "pkg/kv/kvserver/mmaintegration",
    "pkg/kv/kvserver/mma_store_rebalancer.go",
    "pkg/kv/kvserver/asim",
    # Supporting packages frequently modified alongside MMA.
    "pkg/kv/kvserver/allocator/storepool",
]

# Perl-compatible regex for git --grep (matches against full commit message).
# \b is needed because "mma" appears as a substring in words like "summary".
TITLE_GREP_PATTERN = (
    r"\bmma\b|mmaprototype|mmaintegration|storeliveness|\basim\b"
    r"|allocator.*(mma|rebalanc)"
    r"|storepool.*(mma|suspect|liveness)"
)

# MMA team members: GitHub username -> list of git author emails.
# Used by the author-based pass (Pass 3) to find PRs by team members that
# might not touch MMA directories or use MMA keywords in the title.
MMA_TEAM: dict[str, list[str]] = {
    "sumeerbhola": [
        "sumeer@cockroachlabs.com",
        "54990988+sumeerbhola@users.noreply.github.com",
    ],
    "tbg": [
        "tobias.b.grieger@gmail.com",
    ],
    "wenyihu6": [
        "wenyi@cockroachlabs.com",
        "wenyi.hu@cockroachlabs.com",
    ],
    "angeladietz": [
        "angela.dietz@cockroachlabs.com",
    ],
    "5hubh4m": [
        "shubham.chaudhary@cockroachlabs.com",
    ],
}

# Derived sets for quick lookups.
MMA_TEAM_GITHUB_USERS: set[str] = set(MMA_TEAM.keys())
MMA_TEAM_EMAILS: set[str] = {
    email for emails in MMA_TEAM.values() for email in emails
}

# Python regex for filtering individual PR descriptions within multi-PR merges.
MMA_PR_DESC_RE = re.compile(
    r"\bmma\b|mmaprototype|mmaintegration|allocator|storepool"
    r"|storeliveness|store.liveness|asim",
    re.IGNORECASE,
)

# PRs to ignore (false positives or intentionally skipped).
# Map from PR number to a short reason string.
IGNORED_PRS: dict[int, str] = {
    # Author-based matches not specific to MMA.
    165450: "author-based: kvnemesis: add MVCC GC operation to KVNemesis",
    166328: "author-based: replicastats: removed an unnecessary indirection in `replicaStatsRecord`",
    166419: "author-based: roachtest: enable Go execution traces in kv/restart/nodes=12",
    166421: "author-based: roachtest: enable Go execution traces in kv/splits",
    166532: "author-based: investigate: analyze heap profiles on OOM failures",
    166423: "author-based: roachtest: add Datadog Logs link to GitHub issues",
    166560: "author-based: tsdump2duck: add CLI tool to convert tsdump.gob to DuckDB SQL",
    167100: "author-based: engflow-artifacts: rewrite protobuf parsing with proper schema",
    167211: "author-based: CLAUDE.md: improve PR description guidance",
    166156: "author-based: intentresolver: chunk intent resolution to limit in-flight requests",
    166698: "author-based: kvserver: introduce stageDestroyReplica and pendingReplicaDestruction",
    166924: "author-based: server: use liveness cache for high-frequency scan callers",
    167297: "author-based: intentresolver: expose intent budget constants as env vars",
    166760: "author-based: kvcoord: fix a potential persistent failure loop in `DistSender`",
    167289: "author-based: kvserver: stop treating split/merge trigger errors as replica corruption"
}

# Regexes to extract PR numbers from merge commit subjects.
# Bors merges look like "Merge #NNNNN #NNNNN ..." — all numbers are PRs.
BORS_PR_NUM_RE = re.compile(r"#(\d+)")
# Squash/trunk-io merges look like "title (#NNNNN)" — only the trailing
# parenthesized number is the PR; other #refs in the title are issue numbers.
SQUASH_PR_NUM_RE = re.compile(r"\(#(\d+)\)\s*$")

# Format used to separate subject and body in a single git log call.
# Using a delimiter that won't appear in commit messages.
RECORD_SEP = "---<RECORD>---"
FIELD_SEP = "---<FIELD>---"


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class PRInfo:
    number: int
    title: str
    merge_sha: str
    source: str  # "path", "title", "author", or "both"
    merge_pos: int = 0  # topological position (0 = oldest)


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def git(*args: str, check: bool = True) -> str:
    """Run a git command and return stripped stdout."""
    result = subprocess.run(
        ["git"] + list(args),
        capture_output=True,
        text=True,
        check=check,
    )
    return result.stdout.strip()


def git_lines(*args: str) -> list[str]:
    """Run a git command and return non-empty output lines."""
    out = git(*args)
    return [line for line in out.splitlines() if line]


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def find_split_point(master: str, release: str) -> str:
    return git("merge-base", master, release)


def parse_merge_commit(raw: str) -> tuple[str, str, str]:
    """Parse a record from our custom git log format into (sha, subject, body)."""
    parts = raw.split(FIELD_SEP, 2)
    sha = parts[0].strip()
    subject = parts[1].strip() if len(parts) > 1 else ""
    body = parts[2].strip() if len(parts) > 2 else ""
    return sha, subject, body


def extract_pr_numbers(subject: str) -> list[int]:
    """Extract PR numbers from a merge commit subject.

    Bors subjects ("Merge #NNN #NNN") list only PR numbers, so we extract all.
    Squash/trunk-io subjects ("title (#NNN)") may contain issue refs in the
    title text; only the trailing "(#NNN)" is the actual PR number.
    """
    if subject.startswith("Merge #"):
        return [int(m) for m in BORS_PR_NUM_RE.findall(subject)]
    m = SQUASH_PR_NUM_RE.search(subject)
    return [int(m.group(1))] if m else []


def extract_pr_title(body: str, pr_num: int, subject: str) -> str:
    """Get a human-readable title for a PR from the merge commit body.

    Bors merge bodies contain lines like:
        159099: mma: plumbing changes for existing MMA metrics r=user a=user
    We extract the title and strip reviewer/author annotations.
    """
    for line in body.splitlines():
        if line.startswith(f"{pr_num}:"):
            # Strip "r=... a=..." annotations.
            title = re.sub(r"\s+r=.*", "", line)
            # Strip the leading "NNNNN: " prefix.
            title = re.sub(rf"^{pr_num}:\s*", "", title)
            return title
    # Fallback: for squash-merge style "title (#NNNNN)", strip the PR ref.
    title = re.sub(r"\s*\(#\d+\)\s*$", "", subject)
    return title


def get_first_parent_commits(
    master: str, split: str, extra_args: Optional[list[str]] = None
) -> list[tuple[str, str, str]]:
    """Get first-parent commits as (sha, subject, body) tuples.

    Returns in git's default order (newest first).
    """
    fmt = f"{FIELD_SEP}".join(["%H", "%s", "%b"])
    args = [
        "log", "--first-parent",
        f"--format={RECORD_SEP}{fmt}",
        master, "--not", split,
    ]
    if extra_args:
        args.extend(extra_args)
    raw = git(*args)
    if not raw:
        return []
    records = raw.split(RECORD_SEP)
    results = []
    for rec in records:
        rec = rec.strip()
        if not rec:
            continue
        results.append(parse_merge_commit(rec))
    return results


def find_path_based_prs(
    master: str, split: str, dirs: list[str]
) -> dict[int, PRInfo]:
    """Pass 1: find PRs in merge commits that touch MMA directories."""
    commits = get_first_parent_commits(master, split, ["--"] + dirs)
    prs: dict[int, PRInfo] = {}
    for sha, subject, body in commits:
        pr_nums = extract_pr_numbers(subject)
        is_multi = len(pr_nums) > 1
        for num in pr_nums:
            title = extract_pr_title(body, num, subject)
            if is_multi and not MMA_PR_DESC_RE.search(title):
                # Multi-PR merge: skip co-merged PRs that aren't MMA-related.
                continue
            prs[num] = PRInfo(
                number=num,
                title=title,
                merge_sha=sha,
                source="path",
            )
    return prs


def find_title_based_prs(
    master: str, split: str
) -> dict[int, PRInfo]:
    """Pass 2: find PRs whose commit message matches MMA keywords."""
    commits = get_first_parent_commits(
        master, split,
        [f"--grep={TITLE_GREP_PATTERN}", "-i", "--perl-regexp"],
    )
    prs: dict[int, PRInfo] = {}
    for sha, subject, body in commits:
        pr_nums = extract_pr_numbers(subject)
        is_multi = len(pr_nums) > 1
        for num in pr_nums:
            title = extract_pr_title(body, num, subject)
            if is_multi and not MMA_PR_DESC_RE.search(title):
                continue
            prs[num] = PRInfo(
                number=num,
                title=title,
                merge_sha=sha,
                source="title",
            )
    return prs


def find_author_based_prs(
    master: str, split: str
) -> dict[int, PRInfo]:
    """Pass 3: find PRs authored by MMA team members.

    Uses two sub-strategies:
      A) Bors-style merges: grep for 'a=<github_username>' in commit body.
      B) Non-bors merges (trunk-io, etc.): batch-check the author email of
         each merged branch tip.
    """
    prs: dict[int, PRInfo] = {}
    usernames = sorted(MMA_TEAM_GITHUB_USERS)

    # --- Sub-pass A: bors merges with a=<username> in body ---
    pattern = "|".join(rf"a={re.escape(u)}\b" for u in usernames)
    commits = get_first_parent_commits(
        master, split,
        [f"--grep={pattern}", "--perl-regexp"],
    )
    bors_merge_shas: set[str] = set()
    for sha, subject, body in commits:
        bors_merge_shas.add(sha)
        pr_nums = extract_pr_numbers(subject)
        is_multi = len(pr_nums) > 1
        for num in pr_nums:
            title = extract_pr_title(body, num, subject)
            if is_multi:
                # Only include PRs whose line has a=<team_member>.
                matched = False
                for line in body.splitlines():
                    if line.startswith(f"{num}:"):
                        if any(f"a={u}" in line for u in usernames):
                            matched = True
                        break
                if not matched:
                    continue
            prs[num] = PRInfo(
                number=num, title=title, merge_sha=sha, source="author",
            )

    # --- Sub-pass B: non-bors merges – batch-check branch tip authors ---
    # Get all first-parent merges with their parent SHAs.
    lines = git_lines(
        "log", "--first-parent", "--format=%H %P",
        master, "--not", split,
    )
    # Build mapping: branch_tip_sha -> merge_sha (skip merges already handled).
    tip_to_merge: dict[str, str] = {}
    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue  # not a merge commit
        merge_sha = parts[0]
        if merge_sha in bors_merge_shas:
            continue
        for tip_sha in parts[2:]:
            tip_to_merge[tip_sha] = merge_sha

    if tip_to_merge:
        # Batch-check author emails of branch tips in a single git call.
        tip_shas = list(tip_to_merge.keys())
        raw = git("log", "--no-walk", "--format=%H %ae", *tip_shas)
        matched_merge_shas: set[str] = set()
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split(None, 1)
            if len(parts) < 2:
                continue
            tip_sha, email = parts
            if email in MMA_TEAM_EMAILS and tip_sha in tip_to_merge:
                matched_merge_shas.add(tip_to_merge[tip_sha])

        # Fetch details for the matched merge commits.
        fmt = FIELD_SEP.join(["%H", "%s", "%b"])
        for merge_sha in matched_merge_shas:
            raw_detail = git("log", "-1", f"--format={fmt}", merge_sha)
            sha, subject, body = parse_merge_commit(raw_detail)
            for num in extract_pr_numbers(subject):
                if num not in prs:
                    title = extract_pr_title(body, num, subject)
                    prs[num] = PRInfo(
                        number=num, title=title, merge_sha=sha, source="author",
                    )

    return prs


def get_backported_prs(release: str, split: str) -> set[int]:
    """Find PR numbers already referenced on the release branch.

    Backport commits reference original PR numbers in their branch name
    or commit body (e.g. "backport-release-26.2-159099").
    """
    raw = git(
        "log", "--first-parent", "--format=%s%n%b",
        release, "--not", split,
    )
    # Match 5-6 digit numbers which are likely PR numbers.
    return {int(m) for m in re.findall(r"\d{5,6}", raw)}


def assign_merge_positions(
    prs: dict[int, PRInfo], master: str, split: str
) -> None:
    """Assign topological merge positions to PRs (oldest = 0).

    This determines the backport order: apply in ascending position order.
    We only iterate over first-parent subjects (cheap string scan) and only
    for the PR numbers we care about, so this is fast.
    """
    # Get all first-parent subjects in oldest-first order.
    lines = git_lines(
        "log", "--first-parent", "--format=%H %s",
        "--reverse", master, "--not", split,
    )
    remaining = set(prs.keys())
    for pos, line in enumerate(lines):
        if not remaining:
            break
        # Quick check: only parse if any of our PR numbers appear in the line.
        matched = set()
        for num in remaining:
            if f"#{num}" in line:
                prs[num].merge_pos = pos
                matched.add(num)
        remaining -= matched


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

GITHUB_PR_URL = "https://github.com/cockroachdb/cockroach/pull/{}"


def print_report(
    missing: list[PRInfo],
    ignored: list[tuple[PRInfo, str]],  # (pr, reason)
    present: list[PRInfo],
    total: int,
) -> None:
    print("=" * 64)
    print("MMA Backport Candidates: master -> release-26.2")
    print("=" * 64)
    print()
    print(f"Total MMA-related PRs on master since branch split: {total}")
    print(f"Already backported: {len(present)}")
    print(f"Ignored (skipped):  {len(ignored)}")
    print(f"NOT YET backported: {len(missing)}")
    print()

    if missing:
        print("-" * 64)
        print("PRs NOT YET backported (in cherry-pick order, oldest first)")
        print("-" * 64)
        print()
        print("  Legend: [P]=path-based  [T]=title-based  [A]=author-based  [B]=both(P+T)")
        print()
        for i, pr in enumerate(missing, 1):
            tag = {"path": "P", "title": "T", "author": "A", "both": "B"}.get(pr.source, "?")
            short_sha = pr.merge_sha[:11]
            url = GITHUB_PR_URL.format(pr.number)
            print(f"  {i:>2}. [{tag}] #{pr.number:<6}  {short_sha}  {pr.title}")
            print(f"            {url}")
        print()

    if ignored:
        print("-" * 64)
        print("PRs ignored (add/remove in IGNORED_PRS dict)")
        print("-" * 64)
        for pr, reason in ignored:
            short_sha = pr.merge_sha[:11]
            print(f"  #{pr.number:<6}  {short_sha}  {pr.title}")
            print(f"       reason: {reason}")
        print()

    if present:
        print("-" * 64)
        print("PRs already backported (for reference)")
        print("-" * 64)
        for pr in present:
            short_sha = pr.merge_sha[:11]
            print(f"  #{pr.number:<6}  {short_sha}  {pr.title}")
        print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Identify MMA-related PRs that need backporting to release-26.2.",
    )
    parser.add_argument(
        "--fetch", action="store_true",
        help="Run 'git fetch' before analysis.",
    )
    parser.add_argument(
        "--upstream", default="origin",
        help="Name of the upstream remote (default: origin).",
    )
    args = parser.parse_args()

    master = f"{args.upstream}/master"
    release = f"{args.upstream}/release-26.2"

    if args.fetch:
        print("Fetching upstream...", file=sys.stderr)
        git("fetch", args.upstream, "master", "release-26.2")

    # Find the branch split point.
    split = find_split_point(master, release)
    split_desc = git("log", "--oneline", "-1", split)
    print(f"Branch split point: {split_desc}", file=sys.stderr)
    print(file=sys.stderr)

    # Pass 1: path-based discovery.
    print("Pass 1: path-based (directories touched)...", file=sys.stderr)
    path_prs = find_path_based_prs(master, split, MMA_DIRS)

    # Pass 2: title-based discovery.
    print("Pass 2: title-based (commit message keywords)...", file=sys.stderr)
    title_prs = find_title_based_prs(master, split)

    # Pass 3: author-based discovery.
    print("Pass 3: author-based (MMA team members)...", file=sys.stderr)
    author_prs = find_author_based_prs(master, split)

    # Merge results: path takes precedence; mark "both" if found in multiple passes.
    all_prs: dict[int, PRInfo] = {}
    for num, info in path_prs.items():
        all_prs[num] = info
    for num, info in title_prs.items():
        if num in all_prs:
            all_prs[num].source = "both"
        else:
            all_prs[num] = info
    for num, info in author_prs.items():
        if num not in all_prs:
            all_prs[num] = info

    # Assign topological merge positions for ordering.
    print("Building merge order index...", file=sys.stderr)
    assign_merge_positions(all_prs, master, split)

    # Determine which PRs are already backported.
    backported = get_backported_prs(release, split)

    # Split into missing, ignored, and present, sorted by merge position.
    by_pos = sorted(all_prs.values(), key=lambda p: p.merge_pos)
    missing = []
    ignored = []
    present = []
    for p in by_pos:
        if p.number in backported:
            present.append(p)
        elif p.number in IGNORED_PRS:
            ignored.append((p, IGNORED_PRS[p.number]))
        else:
            missing.append(p)

    print_report(missing, ignored, present, len(all_prs))


if __name__ == "__main__":
    main()
