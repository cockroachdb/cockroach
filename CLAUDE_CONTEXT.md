# AVRO Import Roachtests — Session Context

**DELETE THIS FILE** before merging the PR. It exists only for Claude Code
to regain context across sessions.

## PR

- **PR**: https://github.com/cockroachdb/cockroach/pull/164976 (draft, no reviews yet)
- **Issue**: #164461
- **Epic**: CRDB-48845
- **Branch**: `worktree-avro-import-roachtests` (pushed to fork as `mw5h:avro-import-roachtests`)
- **Worktree**: `.claude/worktrees/avro-import-roachtests`

## What this PR does

Adds AVRO import roachtest coverage. A critical AVRO OCF bug (#161317)
existed for ~5-6 years undetected because there were no roachtests for
AVRO imports. The existing import roachtests only exercise CSV via TPC-H.

## Commits (3)

1. **`generate-import-fixtures: add extensible fixture generation tool`**
   - `pkg/cmd/generate-import-fixtures/` — 4 files:
     - `format.go` — `OutputFormat` interface (pluggable format architecture)
     - `tpch.go` — TPC-H table defs (column names, types, CSV parsers)
     - `format_avro.go` — AVRO impl (OCF w/ snappy + binary records)
     - `main.go` — CLI: reads pipe-delimited `.tbl` files, outputs AVRO
   - User requested NO GCS upload functionality — just local file I/O

2. **`roachtest/import: make import format a property of each dataset`**
   - Pure refactor, no behavior change
   - Added `formatImportStmt` to `dataset` interface
   - `tpchDataset` explicitly declares CSV format
   - Deleted standalone `formatImportStmt` function
   - Updated `runSyncImportJob`, `runAsyncImportJob`, `runRawAsyncImportJob`,
     and `importCancellationRunner` to use the interface method

3. **`roachtest/import: add AVRO OCF and binary records datasets`**
   - Extracted `initTPCHSchemaAndFingerprint` shared helper from `tpchDataset.init()`
   - Added `avroOCFDataset` (OCF, self-describing) and `avroBinDataset`
     (binary records, needs schema URI)
   - 12 new entries in `datasets` map (6 TPC-H tables × 2 AVRO formats)
   - Added `anyAvroOCFDataset` and `anyAvroBinDataset` helper functions
   - Added dedicated `avro-ocf` and `avro-bin` test specs in `tests` slice
   - Reuses CSV fingerprints — AVRO imports produce identical table contents

## User edits after my commits

The user made a small edit to `import.go` after my commits:
- Extracted the `10 * time.Hour` timeout into a `const importTestTimeout`
- Used it in `registerImport` and `importPauseRunner`
- This is already committed (the commits were rebased/amended)

## Key design decisions

- `formatImportStmt` takes explicit `tableName` and `urls` params (not
  just accessing `ds.getDataURLs()`) because `importCancellationRunner`
  imports subsets of files
- AVRO datasets share the same GCS bucket `cockroach-fixtures-us-east1`
  but under `tpch-avro/` prefix (vs `tpch-csv/` for CSV)
- Schema files at `tpch-avro/schema/{table}.avsc`
- Data files at `tpch-avro/{sf-1,sf-100}/{table}.{1-8}.{ocf,bin}`

## Pre-merge TODOs

- [ ] Generate sf-1 AVRO fixtures using the tool and upload to GCS
- [ ] Validate fixtures via local IMPORT INTO
- [ ] Run `import/avro-ocf` and `import/avro-bin` tests locally
- [ ] Address any review feedback
- [ ] Delete this CLAUDE_CONTEXT.md file

## File locations

- Fixture tool: `pkg/cmd/generate-import-fixtures/`
- Import test: `pkg/cmd/roachtest/tests/import.go`
- BUILD.bazel for tool: `pkg/cmd/generate-import-fixtures/BUILD.bazel` (auto-generated)
