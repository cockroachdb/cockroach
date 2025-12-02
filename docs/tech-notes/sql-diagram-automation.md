# SQL Diagram Automation - Technical Documentation

This document provides technical details about the SQL diagram automation pipeline for the Docs Infrastructure team.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         cockroachdb/cockroach                            │
│                                                                          │
│  pkg/cmd/docgen/diagrams.go  ──────────►  CI Workflow                   │
│         │                                     │                          │
│         │                                     ▼                          │
│         │                          ┌──────────────────┐                 │
│         │                          │  Generate BNF    │                 │
│         │                          │  Build SVG       │                 │
│         │                          │  Run Validation  │                 │
│         │                          └────────┬─────────┘                 │
│         │                                   │                            │
│         ▼                                   ▼                            │
│  pkg/sql/parser/sql.y              Package Artifacts                    │
│                                             │                            │
└─────────────────────────────────────────────┼────────────────────────────┘
                                              │
                                              ▼
                              ┌───────────────────────────────┐
                              │     Bot Service (Webhook)     │
                              └───────────────┬───────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     cockroachdb/generated-diagrams                       │
│                                                                          │
│  bnf/              ◄─────── Sync BNF files                              │
│  grammar_svg/      ◄─────── Sync HTML/SVG files                         │
│  manifest.json     ◄─────── Update metadata                             │
│                                                                          │
│                              │                                           │
│                              ▼                                           │
│                     Create PR (no auto-merge)                           │
│                     Assign to docs-infra-prs                            │
└─────────────────────────────────────────────────────────────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          cockroachdb/docs                                │
│                                                                          │
│  Uses remote_include to fetch diagrams from generated-diagrams          │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## CI Workflow Details

### Trigger Conditions

The workflow runs when:
1. PRs modify `pkg/cmd/docgen/diagrams.go`
2. PRs modify `pkg/sql/parser/sql.y`
3. PRs modify `docs/generated/sql/bnf/BUILD.bazel`
4. PRs are labeled with `docgen-diagram`
5. Manual workflow dispatch

### Workflow Jobs

#### 1. `generate-diagrams`

**Runner**: `self-hosted, ubuntu_big_2004`
**Timeout**: 60 minutes

Steps:
1. Checkout code
2. Get EngFlow keys (for Bazel remote execution)
3. Generate BNF files: `./dev generate bnf`
4. Build SVG diagrams: `./dev build docs/generated/sql/bnf:svg`
5. Run validation tests
6. Package artifacts
7. Upload artifacts to GitHub Actions

#### 2. `trigger-bot-sync`

**Condition**: Runs only if diagrams changed and validation passed

Steps:
1. Download diagram artifacts
2. POST to bot service endpoint with:
   - Source commit SHA
   - Source PR number
   - Validation status
   - SKIP DOC warnings

#### 3. `comment-on-pr`

**Condition**: Always runs for PRs

Steps:
1. Create a summary comment on the PR with:
   - Validation status
   - Whether diagrams changed
   - SKIP DOC warnings

## Validation Tests

Located in `pkg/cmd/docgen/tests/validation_test.go`:

| Test | Purpose |
|------|---------|
| `TestMissingBNFFiles` | Ensures all specs in diagrams.go have BNF files |
| `TestMissingHTMLDiagrams` | Ensures all BNF files have HTML output |
| `TestStmtNamingConflicts` | Detects `_stmt` suffix conflicts |
| `TestReplaceUnlinkMismatch` | Warns about potential broken links |
| `TestBrokenGrammarLinks` | Finds links to non-existent grammar rules |
| `TestUnusedDiagrams` | Warns about diagrams not used in docs |
| `TestSKIPDOCSuppressions` | Reports SKIP DOC annotations |

## Bot Service

### Endpoint

```
POST /api/sync-diagrams
Authorization: Bearer <DIAGRAMS_BOT_TOKEN>
Content-Type: application/json
```

### Request Payload

```json
{
  "source_repo": "cockroachdb/cockroach",
  "source_commit": "<sha>",
  "source_pr": <number>,
  "source_branch": "<branch-name>",
  "validation_status": "passed|failed",
  "skip_doc_warnings": "<warnings>",
  "action": "sync_diagrams"
}
```

### Bot Actions

1. Clone `cockroachdb/generated-diagrams`
2. Create branch `diagram-sync/pr-<number>` or `diagram-sync/<sha>`
3. Copy BNF files to `bnf/`
4. Copy HTML files to `grammar_svg/`
5. Update `manifest.json`
6. Commit and push
7. Create PR:
   - Assign to `docs-infra-prs`
   - Add labels: `automated`, `diagram-sync`
   - Include validation summary
   - Include SKIP DOC warnings
8. **No auto-merge** - requires manual review

### Sync Script

The bot uses `build/scripts/sync-diagrams-to-generated-repo.sh`:

```bash
./build/scripts/sync-diagrams-to-generated-repo.sh \
  --source-commit <sha> \
  --source-pr <number> \
  --artifacts-dir <path> \
  [--dry-run]
```

## Secret Configuration

| Secret | Purpose |
|--------|---------|
| `DIAGRAMS_BOT_TOKEN` | Token for bot to create PRs in generated-diagrams |
| `GITHUB_TOKEN` | Standard GitHub Actions token for PR comments |

## SKIP DOC Handling

The `SKIP DOC` annotation in `sql.y` suppresses grammar rules from documentation:

```yacc
some_rule:
  KEYWORD expr
  {
    // SKIP DOC
    $$.val = ...
  }
```

These suppressions are:
- Detected during validation
- Reported in PR comments
- Included in generated-diagrams PR descriptions

## Troubleshooting

### CI fails with "Missing BNF files"

1. Check that the spec name is in `docs/generated/sql/bnf/BUILD.bazel` FILES list
2. Verify the `stmt` production exists in `sql.y`
3. Check for typos in the spec name

### Bot doesn't create PR

1. Verify `DIAGRAMS_BOT_TOKEN` is configured
2. Check bot service logs
3. Ensure validation passed
4. Confirm diagrams actually changed

### Diagrams look wrong

1. Check `inline` list - may need to expand more productions
2. Check `replace`/`unlink` - may need to remove confusing elements
3. Check `match`/`exclude` - may need to filter branches

### SKIP DOC not working

The `SKIP DOC` annotation must be in the action block:
```yacc
production:
  items { /* SKIP DOC */ $$.val = ... }
```

Not in comments before the production.

## Rollback Procedure

If a bad diagram is merged:

1. Revert the PR in `cockroachdb/cockroach`
2. The CI will trigger a new sync
3. Merge the new sync PR in `generated-diagrams`
4. Docs will automatically use the reverted diagrams

## Monitoring

### Key Metrics

- CI workflow success rate
- Time from diagram change to docs update
- Number of SKIP DOC suppressions
- Unused diagram count

### Alerts

Set up alerts for:
- CI workflow failures on master
- Bot service unavailability
- generated-diagrams sync failures
