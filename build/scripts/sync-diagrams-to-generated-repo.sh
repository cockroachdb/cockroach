#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# sync-diagrams-to-generated-repo.sh
#
# This script syncs generated SQL diagrams from the cockroach repo to the
# generated-diagrams repository. It is designed to be called by a bot service
# after the diagram generation CI workflow completes successfully.
#
# Usage:
#   ./sync-diagrams-to-generated-repo.sh \
#     --source-commit <sha> \
#     --source-pr <number> \
#     --artifacts-dir <path> \
#     [--dry-run]
#
# Environment variables:
#   GITHUB_TOKEN - Required for creating PRs and pushing to generated-diagrams
#   DIAGRAMS_BOT_TOKEN - Alternative token for bot operations

set -euo pipefail

# Configuration
GENERATED_DIAGRAMS_REPO="cockroachdb/generated-diagrams"
DOCS_INFRA_TEAM="docs-infra-prs"
BOT_NAME="sql-diagram-bot"
BOT_EMAIL="sql-diagram-bot@cockroachlabs.com"

# Parse arguments
SOURCE_COMMIT=""
SOURCE_PR=""
ARTIFACTS_DIR=""
DRY_RUN=false
VALIDATION_SUMMARY=""
SKIP_DOC_WARNINGS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --source-commit)
            SOURCE_COMMIT="$2"
            shift 2
            ;;
        --source-pr)
            SOURCE_PR="$2"
            shift 2
            ;;
        --artifacts-dir)
            ARTIFACTS_DIR="$2"
            shift 2
            ;;
        --validation-summary)
            VALIDATION_SUMMARY="$2"
            shift 2
            ;;
        --skip-doc-warnings)
            SKIP_DOC_WARNINGS="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$SOURCE_COMMIT" ]]; then
    echo "Error: --source-commit is required"
    exit 1
fi

if [[ -z "$ARTIFACTS_DIR" ]]; then
    echo "Error: --artifacts-dir is required"
    exit 1
fi

# Use DIAGRAMS_BOT_TOKEN if available, otherwise fall back to GITHUB_TOKEN
TOKEN="${DIAGRAMS_BOT_TOKEN:-${GITHUB_TOKEN:-}}"
if [[ -z "$TOKEN" && "$DRY_RUN" != "true" ]]; then
    echo "Error: GITHUB_TOKEN or DIAGRAMS_BOT_TOKEN is required"
    exit 1
fi

echo "=== SQL Diagram Sync Script ==="
echo "Source commit: $SOURCE_COMMIT"
echo "Source PR: ${SOURCE_PR:-N/A}"
echo "Artifacts directory: $ARTIFACTS_DIR"
echo "Dry run: $DRY_RUN"
echo ""

# Create temporary working directory
WORK_DIR=$(mktemp -d)
trap "rm -rf $WORK_DIR" EXIT

echo "Working directory: $WORK_DIR"

# Clone generated-diagrams repository
echo ""
echo "=== Cloning $GENERATED_DIAGRAMS_REPO ==="
if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Would clone $GENERATED_DIAGRAMS_REPO"
    mkdir -p "$WORK_DIR/generated-diagrams"
else
    cd "$WORK_DIR"
    git clone "https://x-access-token:${TOKEN}@github.com/${GENERATED_DIAGRAMS_REPO}.git" generated-diagrams
fi

cd "$WORK_DIR/generated-diagrams"

# Configure git
git config user.name "$BOT_NAME"
git config user.email "$BOT_EMAIL"

# Create a new branch for this update
BRANCH_NAME="diagram-sync/${SOURCE_COMMIT:0:8}"
if [[ -n "$SOURCE_PR" ]]; then
    BRANCH_NAME="diagram-sync/pr-${SOURCE_PR}"
fi

echo ""
echo "=== Creating branch: $BRANCH_NAME ==="
if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Would create branch $BRANCH_NAME"
else
    git checkout -b "$BRANCH_NAME"
fi

# Update BNF files
echo ""
echo "=== Updating BNF files ==="
mkdir -p bnf
if [[ -d "$ARTIFACTS_DIR" ]]; then
    BNF_COUNT=$(find "$ARTIFACTS_DIR" -name "*.bnf" 2>/dev/null | wc -l)
    echo "Found $BNF_COUNT BNF files to sync"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY RUN] Would copy BNF files from $ARTIFACTS_DIR to bnf/"
    else
        cp "$ARTIFACTS_DIR"/*.bnf bnf/ 2>/dev/null || true
    fi
else
    echo "Warning: Artifacts directory not found: $ARTIFACTS_DIR"
fi

# Update HTML/SVG files
echo ""
echo "=== Updating grammar SVG files ==="
mkdir -p grammar_svg
if [[ -d "$ARTIFACTS_DIR" ]]; then
    HTML_COUNT=$(find "$ARTIFACTS_DIR" -name "*.html" 2>/dev/null | wc -l)
    echo "Found $HTML_COUNT HTML files to sync"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[DRY RUN] Would copy HTML files from $ARTIFACTS_DIR to grammar_svg/"
    else
        cp "$ARTIFACTS_DIR"/*.html grammar_svg/ 2>/dev/null || true
    fi
fi

# Update manifest
echo ""
echo "=== Updating manifest ==="
MANIFEST_FILE="manifest.json"
TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Would update manifest with:"
    echo "  generated_at: $TIMESTAMP"
    echo "  source_commit: $SOURCE_COMMIT"
    echo "  source_pr: ${SOURCE_PR:-null}"
else
    cat > "$MANIFEST_FILE" << EOF
{
  "generated_at": "$TIMESTAMP",
  "source_commit": "$SOURCE_COMMIT",
  "source_pr": ${SOURCE_PR:-null},
  "bnf_count": $(find bnf -name "*.bnf" 2>/dev/null | wc -l),
  "svg_count": $(find grammar_svg -name "*.html" 2>/dev/null | wc -l)
}
EOF
fi

# Check if there are any changes
echo ""
echo "=== Checking for changes ==="
if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Would check git status"
else
    git add -A
    if git diff --cached --quiet; then
        echo "No changes detected. Exiting."
        exit 0
    fi

    CHANGED_FILES=$(git diff --cached --name-only | wc -l)
    echo "Detected $CHANGED_FILES changed files"
fi

# Create commit
echo ""
echo "=== Creating commit ==="
COMMIT_MSG="chore: sync SQL diagrams from cockroach

Source commit: $SOURCE_COMMIT"

if [[ -n "$SOURCE_PR" ]]; then
    COMMIT_MSG="$COMMIT_MSG
Source PR: cockroachdb/cockroach#$SOURCE_PR"
fi

COMMIT_MSG="$COMMIT_MSG

This commit was automatically generated by the SQL diagram sync bot.
"

if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Would create commit with message:"
    echo "$COMMIT_MSG"
else
    git commit -m "$COMMIT_MSG"
fi

# Push branch
echo ""
echo "=== Pushing branch ==="
if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Would push branch $BRANCH_NAME"
else
    git push -u origin "$BRANCH_NAME"
fi

# Create pull request
echo ""
echo "=== Creating pull request ==="
PR_TITLE="chore: sync SQL diagrams from cockroach"
if [[ -n "$SOURCE_PR" ]]; then
    PR_TITLE="chore: sync SQL diagrams from cockroach#$SOURCE_PR"
fi

PR_BODY="## Automated SQL Diagram Sync

This PR was automatically created by the SQL diagram sync bot.

### Source
- **Commit**: cockroachdb/cockroach@${SOURCE_COMMIT}
- **PR**: ${SOURCE_PR:+cockroachdb/cockroach#$SOURCE_PR}

### Changes
This PR updates the BNF and SVG diagram files generated from \`sql.y\`.
"

if [[ -n "$VALIDATION_SUMMARY" ]]; then
    PR_BODY="$PR_BODY

### Validation Summary
\`\`\`
$VALIDATION_SUMMARY
\`\`\`
"
fi

if [[ -n "$SKIP_DOC_WARNINGS" ]]; then
    PR_BODY="$PR_BODY

### SKIP DOC Warnings
The following grammar rules are intentionally suppressed from documentation:
\`\`\`
$SKIP_DOC_WARNINGS
\`\`\`
"
fi

PR_BODY="$PR_BODY

---
**Note**: This PR requires manual review and merge. Auto-merge is disabled for safety.
"

if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Would create PR with title: $PR_TITLE"
    echo "[DRY RUN] Would assign to: $DOCS_INFRA_TEAM"
else
    # Create PR using gh CLI
    PR_URL=$(gh pr create \
        --repo "$GENERATED_DIAGRAMS_REPO" \
        --title "$PR_TITLE" \
        --body "$PR_BODY" \
        --base main \
        --head "$BRANCH_NAME" \
        2>&1)

    echo "Created PR: $PR_URL"

    # Extract PR number and assign reviewers
    PR_NUMBER=$(echo "$PR_URL" | grep -oE '[0-9]+$')
    if [[ -n "$PR_NUMBER" ]]; then
        echo "Assigning PR #$PR_NUMBER to $DOCS_INFRA_TEAM"
        gh pr edit "$PR_NUMBER" \
            --repo "$GENERATED_DIAGRAMS_REPO" \
            --add-assignee "$DOCS_INFRA_TEAM" 2>/dev/null || true

        # Add labels
        gh pr edit "$PR_NUMBER" \
            --repo "$GENERATED_DIAGRAMS_REPO" \
            --add-label "automated,diagram-sync" 2>/dev/null || true
    fi
fi

echo ""
echo "=== Sync complete ==="
