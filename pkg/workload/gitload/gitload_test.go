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
	disableAutoStats(t, ctx, db)

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

	setupFKsAndStats(t, ctx, db, testAgentID)

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

// setupFKsAndStats adds FK constraints, secondary indexes, and initializes
// repo_stats rows for the given agents, mirroring the PostLoad hook.
func setupFKsAndStats(t *testing.T, ctx context.Context, db *gosql.DB, agentIDs ...string) {
	t.Helper()
	for _, fk := range fkConstraints {
		_, err := db.ExecContext(ctx, fk)
		require.NoError(t, err)
	}
	for _, idx := range secondaryIndexes {
		_, err := db.ExecContext(ctx, idx)
		require.NoError(t, err)
	}
	for _, aid := range agentIDs {
		_, err := db.ExecContext(ctx,
			`INSERT INTO repo_stats (agent_id, total_commits, total_files_changed, total_blobs_bytes)
			 VALUES ($1, 0, 0, 0)
			 ON CONFLICT (agent_id) DO NOTHING`, aid)
		require.NoError(t, err)
	}
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
	disableAutoStats(t, ctx, db)

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
	setupFKsAndStats(t, ctx, db, testAgentID)

	t.Run("empty", func(t *testing.T) {
		verifyPartial(t, ctx, db, repoPath, testAgentID)
	})

	t.Run("partial", func(t *testing.T) {
		require.NoError(t, ingest(ctx, db, repoPath, seed,
			20 /* maxCommits */, 0 /* maxBlobSize */, false /* skipContent */, nil, /* hists */
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

// verifyPartial runs all oracle verification checks with partial=true for the
// commit graph, simulating what runInlineChecks does but within a plain
// read-only transaction (without AS OF SYSTEM TIME, which may not see
// just-written data in tests).
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

// TestGitloadTruncatedBlobs verifies that blob content verification handles
// truncated blobs correctly. When --max-blob-size truncates stored content,
// the SHA-256 key (computed on the full content) won't match sha256(stored),
// so verifyBlobContent must skip truncated blobs instead of reporting a
// false positive.
func TestGitloadTruncatedBlobs(t *testing.T) {
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

	repoPath := t.TempDir()
	numCommits := 30
	seed := int64(42)

	require.NoError(t, generateRepo(ctx, repoPath, numCommits, seed))

	g := &gitload{}
	for _, tbl := range g.Tables() {
		_, err := db.ExecContext(ctx,
			fmt.Sprintf("CREATE TABLE %s %s", tbl.Name, tbl.Schema))
		require.NoError(t, err)
	}
	setupFKsAndStats(t, ctx, db, testAgentID)

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

// TestGitloadClearAgentData verifies that clearAgentData properly removes
// all agent-scoped data while preserving shared blobs and other agents' data.
func TestGitloadClearAgentData(t *testing.T) {
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

	repoPath := t.TempDir()
	numCommits := 30
	seed := int64(42)

	require.NoError(t, generateRepo(ctx, repoPath, numCommits, seed))

	g := &gitload{}
	for _, tbl := range g.Tables() {
		_, err := db.ExecContext(ctx,
			fmt.Sprintf("CREATE TABLE %s %s", tbl.Name, tbl.Schema))
		require.NoError(t, err)
	}
	setupFKsAndStats(t, ctx, db, testAgentID)

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
// file_latest row. Used in tests to clean up shared blobs between cycles.
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
