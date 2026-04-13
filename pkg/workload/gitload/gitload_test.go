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

const testAgentID = "agent-0"

// TestGitload is the top-level test that starts a single shared test server
// and runs all gitload subtests against it. Sharing the server avoids
// redundant startup and schema-change overhead (FK constraints, secondary
// indexes) that previously caused the suite to exceed its 15-minute timeout.
func TestGitload(t *testing.T) {
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

	disableAutoStats(t, ctx, db)
	setupSchema(t, ctx, db)

	t.Run("IngestAndVerify", func(t *testing.T) {
		testIngestAndVerify(t, ctx, db)
	})
	t.Run("InlineVerification", func(t *testing.T) {
		testInlineVerification(t, ctx, db)
	})
	t.Run("TruncatedBlobs", func(t *testing.T) {
		testTruncatedBlobs(t, ctx, db)
	})
	t.Run("ClearAgentData", func(t *testing.T) {
		testClearAgentData(t, ctx, db)
	})
}

// setupSchema creates all gitload tables, FK constraints, secondary indexes,
// and initializes the repo_stats row. Called once per server.
func setupSchema(t *testing.T, ctx context.Context, db *gosql.DB) {
	t.Helper()
	g := &gitload{}
	for _, tbl := range g.Tables() {
		_, err := db.ExecContext(ctx,
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s %s", tbl.Name, tbl.Schema))
		require.NoError(t, err)
	}
	for _, fk := range fkConstraints {
		_, err := db.ExecContext(ctx, fk)
		require.NoError(t, err)
	}
	for _, idx := range secondaryIndexes {
		_, err := db.ExecContext(ctx, idx)
		require.NoError(t, err)
	}
}

// resetAllTables truncates all data from gitload tables and re-initializes
// the repo_stats row for testAgentID.
func resetAllTables(t *testing.T, ctx context.Context, db *gosql.DB) {
	t.Helper()
	_, err := db.ExecContext(ctx,
		`TRUNCATE commit_diffs, commit_parents, file_latest, commits, blobs, repo_stats CASCADE`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO repo_stats (agent_id, total_commits, total_files_changed, total_blobs_bytes)
		 VALUES ($1, 0, 0, 0)
		 ON CONFLICT (agent_id) DO NOTHING`, testAgentID)
	require.NoError(t, err)
}

// testIngestAndVerify exercises the full gitload cycle: generate a synthetic
// repo, ingest commits, run oracle verification, then clear and re-ingest
// with a different seed to verify the cycle path.
func testIngestAndVerify(t *testing.T, ctx context.Context, db *gosql.DB) {
	resetAllTables(t, ctx, db)

	repoPath := t.TempDir()
	numCommits := 20
	seed := int64(42)

	require.NoError(t, generateRepo(ctx, repoPath, numCommits, seed))

	require.NoError(t, ingest(ctx, db, repoPath, seed,
		numCommits, 0 /* maxBlobSize */, false /* skipContent */, nil, /* hists */
		testAgentID, defaultBatchSize))

	verifyAll(t, ctx, db, repoPath, testAgentID)

	// Test a second cycle with a different seed using clearAgentData.
	require.NoError(t, clearAgentData(ctx, db, testAgentID))
	newSeed := int64(99)
	require.NoError(t, ingest(ctx, db, repoPath, newSeed,
		numCommits, 0 /* maxBlobSize */, false /* skipContent */, nil, /* hists */
		testAgentID, defaultBatchSize))
	verifyAll(t, ctx, db, repoPath, testAgentID)
}

// testInlineVerification exercises inline verification against partial data
// (mid-ingestion) and empty data (post-truncation).
func testInlineVerification(t *testing.T, ctx context.Context, db *gosql.DB) {
	resetAllTables(t, ctx, db)

	repoPath := t.TempDir()
	numCommits := 30
	seed := int64(42)

	require.NoError(t, generateRepo(ctx, repoPath, numCommits, seed))

	t.Run("empty", func(t *testing.T) {
		verifyPartial(t, ctx, db, repoPath, testAgentID)
	})

	t.Run("partial", func(t *testing.T) {
		require.NoError(t, ingest(ctx, db, repoPath, seed,
			numCommits/3 /* maxCommits */, 0 /* maxBlobSize */, false /* skipContent */, nil, /* hists */
			testAgentID, defaultBatchSize))
		verifyPartial(t, ctx, db, repoPath, testAgentID)
	})

	t.Run("full", func(t *testing.T) {
		require.NoError(t, clearAgentData(ctx, db, testAgentID))
		deleteOrphanedBlobs(t, ctx, db)
		require.NoError(t, ingest(ctx, db, repoPath, seed,
			numCommits, 0 /* maxBlobSize */, false /* skipContent */, nil, /* hists */
			testAgentID, defaultBatchSize))

		verifyPartial(t, ctx, db, repoPath, testAgentID)
		verifyAll(t, ctx, db, repoPath, testAgentID)
	})
}

// testTruncatedBlobs verifies that blob content verification handles
// truncated blobs correctly.
func testTruncatedBlobs(t *testing.T, ctx context.Context, db *gosql.DB) {
	resetAllTables(t, ctx, db)

	repoPath := t.TempDir()
	numCommits := 15
	seed := int64(42)

	require.NoError(t, generateRepo(ctx, repoPath, numCommits, seed))

	require.NoError(t, ingest(ctx, db, repoPath, seed,
		numCommits, 100 /* maxBlobSize */, false /* skipContent */, nil, /* hists */
		testAgentID, defaultBatchSize))

	tx, err := db.BeginTx(ctx, &gosql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	require.NoError(t, verifyBlobContent(ctx, tx))
	require.NoError(t, verifyRepoStats(ctx, tx, testAgentID))
	require.NoError(t, verifyReferentialIntegrity(ctx, tx))
	require.NoError(t, tx.Commit())
}

// testClearAgentData verifies that clearAgentData properly removes all
// agent-scoped data while preserving shared blobs.
func testClearAgentData(t *testing.T, ctx context.Context, db *gosql.DB) {
	resetAllTables(t, ctx, db)

	repoPath := t.TempDir()
	numCommits := 15
	seed := int64(42)

	require.NoError(t, generateRepo(ctx, repoPath, numCommits, seed))

	require.NoError(t, ingest(ctx, db, repoPath, seed,
		numCommits, 0 /* maxBlobSize */, false /* skipContent */, nil, /* hists */
		testAgentID, defaultBatchSize))

	// Verify data exists before clear.
	var commitCount int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM commits WHERE agent_id = $1", testAgentID,
	).Scan(&commitCount))
	require.Greater(t, commitCount, 0)

	var blobCount int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM blobs",
	).Scan(&blobCount))
	require.Greater(t, blobCount, 0)

	// Clear agent data.
	require.NoError(t, clearAgentData(ctx, db, testAgentID))

	// Agent-scoped data should be gone.
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM commits WHERE agent_id = $1", testAgentID,
	).Scan(&commitCount))
	require.Equal(t, 0, commitCount)

	// Blobs are shared and should still exist.
	var blobCountAfter int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM blobs",
	).Scan(&blobCountAfter))
	require.Equal(t, blobCount, blobCountAfter)

	// repo_stats should be zeroed out.
	var totalCommits int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT total_commits FROM repo_stats WHERE agent_id = $1", testAgentID,
	).Scan(&totalCommits))
	require.Equal(t, 0, totalCommits)
}

// disableAutoStats turns off automatic statistics collection to prevent
// background stats jobs from causing lock contention during ingestion.
func disableAutoStats(t *testing.T, ctx context.Context, db *gosql.DB) {
	t.Helper()
	_, err := db.ExecContext(ctx,
		`SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	require.NoError(t, err)
}

// deleteOrphanedBlobs removes blobs not referenced by any commit_diffs or
// file_latest row.
func deleteOrphanedBlobs(t *testing.T, ctx context.Context, db *gosql.DB) {
	t.Helper()
	_, err := db.ExecContext(ctx,
		`DELETE FROM blobs WHERE sha256 NOT IN (
			SELECT blob_sha256 FROM commit_diffs WHERE blob_sha256 IS NOT NULL
			UNION
			SELECT blob_sha256 FROM file_latest WHERE blob_sha256 IS NOT NULL
		)`)
	require.NoError(t, err)
}

// verifyPartial runs all oracle verification checks with partial=true for the
// commit graph, simulating what runInlineChecks does.
func verifyPartial(
	t *testing.T, ctx context.Context, db *gosql.DB, repoPath string, agentID string,
) {
	t.Helper()
	tx, err := db.BeginTx(ctx, &gosql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	var commitCount int64
	require.NoError(t, tx.QueryRowContext(ctx,
		"SELECT count(*) FROM commits WHERE agent_id = $1", agentID,
	).Scan(&commitCount))
	if commitCount == 0 {
		require.NoError(t, tx.Commit())
		return
	}

	require.NoError(t, verifyRepoStats(ctx, tx, agentID))
	require.NoError(t, verifyCommitGraph(ctx, tx, repoPath, true /* partial */, 0, agentID))
	require.NoError(t, verifyCommitMetadata(ctx, tx, repoPath, agentID))
	require.NoError(t, verifyFileLatest(ctx, tx, repoPath, agentID))
	require.NoError(t, replay(ctx, tx, repoPath, 1 /* verifyEveryN */, agentID))
	require.NoError(t, verifyBlobContent(ctx, tx))
	require.NoError(t, verifyReferentialIntegrity(ctx, tx))
	require.NoError(t, verifySecondaryIndexes(ctx, tx))
	require.NoError(t, tx.Commit())
}

// verifyAll runs all oracle verification levels within a plain read-only
// transaction with partial=false (full bidirectional comparison).
func verifyAll(t *testing.T, ctx context.Context, db *gosql.DB, repoPath string, agentID string) {
	t.Helper()
	tx, err := db.BeginTx(ctx, &gosql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	require.NoError(t, verifyRepoStats(ctx, tx, agentID))
	require.NoError(t, verifyCommitGraph(ctx, tx, repoPath, false /* partial */, 0, agentID))
	require.NoError(t, verifyCommitMetadata(ctx, tx, repoPath, agentID))
	require.NoError(t, verifyFileLatest(ctx, tx, repoPath, agentID))
	require.NoError(t, replay(ctx, tx, repoPath, 1 /* verifyEveryN */, agentID))
	require.NoError(t, verifyBlobContent(ctx, tx))
	require.NoError(t, verifyReferentialIntegrity(ctx, tx))
	require.NoError(t, verifySecondaryIndexes(ctx, tx))
	require.NoError(t, tx.Commit())
}
