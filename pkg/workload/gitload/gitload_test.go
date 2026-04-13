// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gitload

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os/exec"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestGitloadIngestAndVerify exercises the full gitload cycle: generate a
// synthetic repo, create tables, ingest commits, and run oracle verification.
// It then clears tables and re-ingests with a different seed to verify the
// cycle path.
func TestGitloadIngestAndVerify(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if _, err := exec.LookPath("git"); err != nil {
		skip.IgnoreLint(t, "git not in PATH")
	}

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "defaultdb",
	})
	defer srv.Stopper().Stop(ctx)

	repoPath := t.TempDir()
	numCommits := 60
	seed := int64(42)

	// Phase 1: Generate the synthetic git repo.
	require.NoError(t, generateRepo(ctx, repoPath, numCommits, seed))

	// Phase 2: Create all tables.
	g := &gitload{}
	for _, tbl := range g.Tables() {
		_, err := db.ExecContext(ctx,
			fmt.Sprintf("CREATE TABLE %s %s", tbl.Name, tbl.Schema))
		require.NoError(t, err)
	}

	// Phase 3: Add FK constraints and initialize repo_stats (same as PostLoad).
	setupFKsAndStats(t, ctx, db)

	// Phase 4: Ingest commits.
	require.NoError(t, ingest(ctx, db, repoPath, seed,
		numCommits, 0 /* maxBlobSize */, false /* skipContent */, nil /* hists */))

	// Phase 5: Verify everything within a plain read-only transaction.
	// We avoid runAllChecks because it uses AS OF SYSTEM TIME
	// follower_read_timestamp() which may not have the just-written data.
	verifyAll(t, ctx, db, repoPath)

	// Phase 6: Test a second cycle with a different seed.
	require.NoError(t, clearAllTables(ctx, db))
	newSeed := int64(99)
	require.NoError(t, ingest(ctx, db, repoPath, newSeed,
		numCommits, 0 /* maxBlobSize */, false /* skipContent */, nil /* hists */))
	verifyAll(t, ctx, db, repoPath)
}

// setupFKsAndStats adds FK constraints and initializes the repo_stats
// singleton row, mirroring the PostLoad hook.
func setupFKsAndStats(t *testing.T, ctx context.Context, db *gosql.DB) {
	t.Helper()
	for _, fk := range fkConstraints {
		_, err := db.ExecContext(ctx, fk)
		require.NoError(t, err)
	}
	for _, idx := range secondaryIndexes {
		_, err := db.ExecContext(ctx, idx)
		require.NoError(t, err)
	}
	_, err := db.ExecContext(ctx,
		`INSERT INTO repo_stats (id, total_commits, total_files_changed, total_blobs_bytes)
		 VALUES (1, 0, 0, 0)
		 ON CONFLICT (id) DO NOTHING`)
	require.NoError(t, err)
}

// TestGitloadInlineVerification exercises inline verification against partial
// data (mid-ingestion) and empty data (post-truncation). This validates that
// runInlineChecks does not false-positive when the database contains a
// transactionally-consistent subset of the full commit history.
func TestGitloadInlineVerification(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if _, err := exec.LookPath("git"); err != nil {
		skip.IgnoreLint(t, "git not in PATH")
	}

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "defaultdb",
	})
	defer srv.Stopper().Stop(ctx)

	repoPath := t.TempDir()
	numCommits := 60
	seed := int64(42)

	require.NoError(t, generateRepo(ctx, repoPath, numCommits, seed))

	g := &gitload{}
	for _, tbl := range g.Tables() {
		_, err := db.ExecContext(ctx,
			fmt.Sprintf("CREATE TABLE %s %s", tbl.Name, tbl.Schema))
		require.NoError(t, err)
	}
	setupFKsAndStats(t, ctx, db)

	// Test 1: Inline verification on empty data should be a no-op.
	t.Run("empty", func(t *testing.T) {
		verifyPartial(t, ctx, db, repoPath)
	})

	// Test 2: Ingest only the first 20 commits (partial ingestion),
	// then run inline verification on the partial data.
	t.Run("partial", func(t *testing.T) {
		require.NoError(t, ingest(ctx, db, repoPath, seed,
			20 /* maxCommits */, 0 /* maxBlobSize */, false /* skipContent */, nil /* hists */))
		verifyPartial(t, ctx, db, repoPath)
	})

	// Test 3: Ingest all remaining commits, then run full verification.
	t.Run("full", func(t *testing.T) {
		// Clear and re-ingest all commits.
		require.NoError(t, clearAllTables(ctx, db))
		require.NoError(t, ingest(ctx, db, repoPath, seed,
			numCommits, 0 /* maxBlobSize */, false /* skipContent */, nil /* hists */))

		// Both partial and full verification should pass.
		verifyPartial(t, ctx, db, repoPath)
		verifyAll(t, ctx, db, repoPath)
	})
}

// verifyPartial runs all oracle verification checks with partial=true for the
// commit graph, simulating what runInlineChecks does but within a plain
// read-only transaction (without AS OF SYSTEM TIME, which may not see
// just-written data in tests).
func verifyPartial(t *testing.T, ctx context.Context, db *gosql.DB, repoPath string) {
	t.Helper()
	tx, err := db.BeginTx(ctx, &gosql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	var commitCount int64
	require.NoError(t, tx.QueryRowContext(ctx,
		"SELECT count(*) FROM commits").Scan(&commitCount))
	if commitCount == 0 {
		require.NoError(t, tx.Commit())
		return
	}

	require.NoError(t, verifyRepoStats(ctx, tx))
	require.NoError(t, verifyCommitGraph(ctx, tx, repoPath, true /* partial */, 0))
	require.NoError(t, verifyCommitMetadata(ctx, tx, repoPath))
	require.NoError(t, verifyFileLatest(ctx, tx, repoPath))
	require.NoError(t, replay(ctx, tx, repoPath, 1 /* verifyEveryN */))
	require.NoError(t, verifyBlobContent(ctx, tx))
	require.NoError(t, verifyReferentialIntegrity(ctx, tx))
	require.NoError(t, verifySecondaryIndexes(ctx, tx))
	require.NoError(t, tx.Commit())
}

// verifyAll runs all oracle verification levels within a plain read-only
// transaction with partial=false (full bidirectional comparison).
func verifyAll(t *testing.T, ctx context.Context, db *gosql.DB, repoPath string) {
	t.Helper()
	tx, err := db.BeginTx(ctx, &gosql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	require.NoError(t, verifyRepoStats(ctx, tx))
	require.NoError(t, verifyCommitGraph(ctx, tx, repoPath, false /* partial */, 0))
	require.NoError(t, verifyCommitMetadata(ctx, tx, repoPath))
	require.NoError(t, verifyFileLatest(ctx, tx, repoPath))
	require.NoError(t, replay(ctx, tx, repoPath, 1 /* verifyEveryN */))
	require.NoError(t, verifyBlobContent(ctx, tx))
	require.NoError(t, verifyReferentialIntegrity(ctx, tx))
	require.NoError(t, verifySecondaryIndexes(ctx, tx))
	require.NoError(t, tx.Commit())
}
