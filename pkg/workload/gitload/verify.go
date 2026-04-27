// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gitload

import (
	"context"
	"crypto/sha256"
	gosql "database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

// runAllChecks runs all oracle verification checks against the database and
// the git repo within a single historical read transaction to ensure a
// consistent snapshot. Using AS OF SYSTEM TIME avoids uncertainty interval
// errors and makes it safe to run concurrently with the workload.
func runAllChecks(ctx context.Context, db *gosql.DB, repoPath string, verifyEveryN int) error {
	tx, err := db.BeginTx(ctx, &gosql.TxOptions{ReadOnly: true})
	if err != nil {
		return errors.Wrap(err, "starting read transaction")
	}
	defer func() { _ = tx.Rollback() }()

	// Use a follower read timestamp to avoid uncertainty interval errors
	// when reading across nodes during concurrent writes.
	if _, err := tx.ExecContext(ctx,
		"SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()"); err != nil {
		return errors.Wrap(err, "setting AS OF SYSTEM TIME")
	}

	if err := verifyRepoStats(ctx, tx); err != nil {
		return errors.Wrap(err, "repo stats verification failed")
	}

	// Read the expected commit count from repo_stats. verifyRepoStats
	// already confirmed this matches COUNT(*) FROM commits, so this is
	// the authoritative expected count. We pass it to verifyCommitGraph
	// so it can switch to partial mode when the DB has fewer commits
	// than the git repo (e.g. when --commits limited ingestion).
	var dbExpectedCommits int
	if err := tx.QueryRowContext(ctx,
		"SELECT total_commits FROM repo_stats WHERE id = 1",
	).Scan(&dbExpectedCommits); err != nil {
		return errors.Wrap(err, "reading repo_stats for commit count")
	}

	if err := verifyCommitGraph(ctx, tx, repoPath, false /* partial */, dbExpectedCommits); err != nil {
		return errors.Wrap(err, "commit graph verification failed")
	}
	if err := verifyCommitMetadata(ctx, tx, repoPath); err != nil {
		return errors.Wrap(err, "commit metadata verification failed")
	}
	if err := verifyFileLatest(ctx, tx, repoPath); err != nil {
		return errors.Wrap(err, "file_latest verification failed")
	}
	if err := replay(ctx, tx, repoPath, verifyEveryN); err != nil {
		return errors.Wrap(err, "replay verification failed")
	}
	if err := verifyBlobContent(ctx, tx); err != nil {
		return errors.Wrap(err, "blob content verification failed")
	}
	if err := verifyReferentialIntegrity(ctx, tx); err != nil {
		return errors.Wrap(err, "referential integrity verification failed")
	}
	if err := verifySecondaryIndexes(ctx, tx); err != nil {
		return errors.Wrap(err, "secondary index verification failed")
	}
	return tx.Commit()
}

// runInlineChecks runs verification checks that are safe to execute while
// ingestion is in progress. It reads a consistent snapshot via AS OF SYSTEM
// TIME follower_read_timestamp() and validates the partial data that has been
// ingested so far. If the snapshot contains 0 commits (e.g. tables are being
// cleared between ingestion cycles), verification is skipped.
func runInlineChecks(ctx context.Context, db *gosql.DB, repoPath string, verifyEveryN int) error {
	tx, err := db.BeginTx(ctx, &gosql.TxOptions{ReadOnly: true})
	if err != nil {
		return errors.Wrap(err, "starting inline verification transaction")
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		"SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()"); err != nil {
		return errors.Wrap(err, "setting AS OF SYSTEM TIME for inline verification")
	}

	// Check if there's any data to verify. During table truncation between
	// ingestion cycles, the snapshot may be empty.
	var commitCount int64
	if err := tx.QueryRowContext(ctx,
		"SELECT count(*) FROM commits",
	).Scan(&commitCount); err != nil {
		return errors.Wrap(err, "counting commits for inline verification")
	}
	if commitCount == 0 {
		_ = tx.Commit()
		return nil
	}

	fmt.Printf("inline verification: checking %d commits\n", commitCount)

	if err := verifyRepoStats(ctx, tx); err != nil {
		return errors.Wrap(err, "inline: repo stats verification failed")
	}
	if err := verifyCommitGraph(ctx, tx, repoPath, true /* partial */, 0); err != nil {
		return errors.Wrap(err, "inline: commit graph verification failed")
	}
	if err := verifyCommitMetadata(ctx, tx, repoPath); err != nil {
		return errors.Wrap(err, "inline: commit metadata verification failed")
	}
	if err := verifyFileLatest(ctx, tx, repoPath); err != nil {
		return errors.Wrap(err, "inline: file_latest verification failed")
	}
	if err := replay(ctx, tx, repoPath, verifyEveryN); err != nil {
		return errors.Wrap(err, "inline: replay verification failed")
	}
	if err := verifyBlobContent(ctx, tx); err != nil {
		return errors.Wrap(err, "inline: blob content verification failed")
	}
	if err := verifyReferentialIntegrity(ctx, tx); err != nil {
		return errors.Wrap(err, "inline: referential integrity verification failed")
	}
	if err := verifySecondaryIndexes(ctx, tx); err != nil {
		return errors.Wrap(err, "inline: secondary index verification failed")
	}

	fmt.Printf("inline verification passed: %d commits verified\n", commitCount)
	return tx.Commit()
}

// verifyRepoStats checks that the repo_stats counters match actual row counts
// and blob size totals.
func verifyRepoStats(ctx context.Context, tx *gosql.Tx) error {
	var totalCommits, totalFilesChanged, totalBlobsBytes int64
	if err := tx.QueryRowContext(ctx,
		`SELECT total_commits, total_files_changed, total_blobs_bytes
		 FROM repo_stats WHERE id = 1`,
	).Scan(&totalCommits, &totalFilesChanged, &totalBlobsBytes); err != nil {
		return errors.Wrap(err, "reading repo_stats")
	}

	var actualCommits int64
	if err := tx.QueryRowContext(ctx,
		"SELECT count(*) FROM commits",
	).Scan(&actualCommits); err != nil {
		return errors.Wrap(err, "counting commits")
	}

	var actualDiffs int64
	if err := tx.QueryRowContext(ctx,
		"SELECT count(*) FROM commit_diffs",
	).Scan(&actualDiffs); err != nil {
		return errors.Wrap(err, "counting commit_diffs")
	}

	var actualBlobsBytes int64
	if err := tx.QueryRowContext(ctx,
		"SELECT coalesce(sum(size), 0) FROM blobs",
	).Scan(&actualBlobsBytes); err != nil {
		return errors.Wrap(err, "summing blob sizes")
	}

	if totalCommits != actualCommits {
		return errors.Newf(
			"repo_stats.total_commits=%d but commits table has %d rows",
			totalCommits, actualCommits,
		)
	}
	if totalFilesChanged != actualDiffs {
		return errors.Newf(
			"repo_stats.total_files_changed=%d but commit_diffs table has %d rows",
			totalFilesChanged, actualDiffs,
		)
	}
	if totalBlobsBytes != actualBlobsBytes {
		return errors.Newf(
			"repo_stats.total_blobs_bytes=%d but sum(blobs.size)=%d",
			totalBlobsBytes, actualBlobsBytes,
		)
	}

	fmt.Printf("repo_stats verified: %d commits, %d file changes, %d blob bytes\n",
		totalCommits, totalFilesChanged, totalBlobsBytes)
	return nil
}

// verifyCommitGraph compares the commit_parents table against the git DAG
// loaded via git rev-list --parents. When partial is false, it verifies a
// full bidirectional match (commit counts must match, every git edge must
// exist in the DB). When partial is true, only commits present in the DB
// are checked — this is safe during mid-ingestion when not all commits
// have been inserted yet.
//
// maxCommits controls the expected number of ingested commits. When
// maxCommits is positive and smaller than the git DAG, the check
// automatically switches to partial mode because the DB intentionally
// contains only a subset of the full history.
func verifyCommitGraph(
	ctx context.Context, tx *gosql.Tx, repoPath string, partial bool, maxCommits int,
) error {
	gitDAG, err := loadCommitDAG(ctx, repoPath)
	if err != nil {
		return errors.Wrap(err, "loading git DAG")
	}

	var dbCommitCount int64
	if err := tx.QueryRowContext(ctx,
		"SELECT count(*) FROM commits",
	).Scan(&dbCommitCount); err != nil {
		return errors.Wrap(err, "counting commits")
	}

	// When --commits limits ingestion below the full git DAG, the DB
	// intentionally contains a subset. Switch to partial edge comparison
	// and adjust the expected count.
	expectedCommits := int64(len(gitDAG))
	if maxCommits > 0 && int64(maxCommits) < expectedCommits {
		expectedCommits = int64(maxCommits)
		partial = true
	}

	// In full mode, verify commit counts match exactly.
	if !partial {
		if expectedCommits != dbCommitCount {
			return errors.Newf(
				"commit count mismatch: git has %d commits, DB has %d",
				len(gitDAG), dbCommitCount,
			)
		}
	}

	// Load all parent edges from the database, ordered by parent_index.
	rows, err := tx.QueryContext(ctx,
		`SELECT commit_hash, parent_hash, parent_index
		 FROM commit_parents
		 ORDER BY commit_hash, parent_index`)
	if err != nil {
		return errors.Wrap(err, "querying commit_parents")
	}
	defer rows.Close()

	dbDAG := make(map[string][]string)
	for rows.Next() {
		var commitHash, parentHash string
		var parentIndex int
		if err := rows.Scan(&commitHash, &parentHash, &parentIndex); err != nil {
			return errors.Wrap(err, "scanning commit_parents row")
		}
		_ = parentIndex // ordering guaranteed by ORDER BY
		dbDAG[commitHash] = append(dbDAG[commitHash], parentHash)
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "iterating commit_parents rows")
	}

	var mismatches []string

	if partial {
		// Partial mode: only validate DB-side edges against git.
		// For each commit with parents in the DB, verify its parent
		// list matches git. Don't flag git commits that are missing
		// from the DB (they haven't been ingested yet).
		for hash, dbParents := range dbDAG {
			gitParents, ok := gitDAG[hash]
			if !ok {
				mismatches = append(mismatches, fmt.Sprintf(
					"commit %s: in DB commit_parents but not in git", hash[:12],
				))
				continue
			}
			if len(gitParents) != len(dbParents) {
				mismatches = append(mismatches, fmt.Sprintf(
					"commit %s: git has %d parents, DB has %d",
					hash[:12], len(gitParents), len(dbParents),
				))
				continue
			}
			for i, gp := range gitParents {
				if gp != dbParents[i] {
					mismatches = append(mismatches, fmt.Sprintf(
						"commit %s parent[%d]: git=%s DB=%s",
						hash[:12], i, gp[:12], dbParents[i][:12],
					))
				}
			}
		}
	} else {
		// Full mode: bidirectional comparison.
		for hash, gitParents := range gitDAG {
			dbParents := dbDAG[hash]
			if len(gitParents) != len(dbParents) {
				mismatches = append(mismatches, fmt.Sprintf(
					"commit %s: git has %d parents, DB has %d",
					hash[:12], len(gitParents), len(dbParents),
				))
				continue
			}
			for i, gp := range gitParents {
				if gp != dbParents[i] {
					mismatches = append(mismatches, fmt.Sprintf(
						"commit %s parent[%d]: git=%s DB=%s",
						hash[:12], i, gp[:12], dbParents[i][:12],
					))
				}
			}
		}
		for hash := range dbDAG {
			if _, ok := gitDAG[hash]; !ok {
				mismatches = append(mismatches, fmt.Sprintf(
					"commit %s: in DB commit_parents but not in git", hash[:12],
				))
			}
		}
	}

	if len(mismatches) > 0 {
		sort.Strings(mismatches)
		detail := strings.Join(mismatches, "\n  ")
		return errors.Newf(
			"commit graph mismatch (%d issues):\n  %s",
			len(mismatches), detail,
		)
	}

	if partial {
		fmt.Printf("commit graph verified (partial): %d/%d commits, DB edges match git\n",
			dbCommitCount, len(gitDAG))
	} else {
		fmt.Printf("commit graph verified: %d commits, all parent edges match git\n",
			len(gitDAG))
	}
	return nil
}

// verifyCommitMetadata compares every field in the commits table against git.
func verifyCommitMetadata(ctx context.Context, tx *gosql.Tx, repoPath string) error {
	rows, err := tx.QueryContext(ctx,
		`SELECT hash, tree_hash, author_name, author_email, author_date,
		        committer_name, committer_email, committer_date, message
		 FROM commits`)
	if err != nil {
		return errors.Wrap(err, "querying commits")
	}
	defer rows.Close()

	var mismatches []string
	var count int
	for rows.Next() {
		var db commitMeta
		if err := rows.Scan(
			&db.hash, &db.treeHash, &db.authorName, &db.authorEmail,
			&db.authorDate, &db.committerName, &db.committerEmail,
			&db.committerDate, &db.message,
		); err != nil {
			return errors.Wrap(err, "scanning commit row")
		}
		count++

		git, err := getCommitMeta(ctx, repoPath, db.hash)
		if err != nil {
			return errors.Wrapf(err, "getting git metadata for %s", db.hash[:12])
		}

		for _, f := range []struct {
			field  string
			dbVal  string
			gitVal string
		}{
			{"tree_hash", db.treeHash, git.treeHash},
			{"author_name", db.authorName, git.authorName},
			{"author_email", db.authorEmail, git.authorEmail},
			{"author_date", db.authorDate, git.authorDate},
			{"committer_name", db.committerName, git.committerName},
			{"committer_email", db.committerEmail, git.committerEmail},
			{"committer_date", db.committerDate, git.committerDate},
			{"message", db.message, git.message},
		} {
			if f.dbVal != f.gitVal {
				mismatches = append(mismatches, fmt.Sprintf(
					"commit %s %s: db=%q git=%q",
					db.hash[:12], f.field, f.dbVal, f.gitVal,
				))
			}
		}
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "iterating commit rows")
	}

	if len(mismatches) > 0 {
		sort.Strings(mismatches)
		detail := strings.Join(mismatches, "\n  ")
		return errors.Newf(
			"commit metadata mismatch (%d issues):\n  %s",
			len(mismatches), detail,
		)
	}

	fmt.Printf("commit metadata verified: %d commits, all fields match git\n", count)
	return nil
}

// verifyFileLatest compares the file_latest table against git ls-tree at the
// latest commit, verifying both path sets and blob SHA-256 content hashes.
func verifyFileLatest(ctx context.Context, tx *gosql.Tx, repoPath string) error {
	// Get the latest commit hash.
	var latestHash string
	err := tx.QueryRowContext(ctx,
		"SELECT hash FROM commits ORDER BY topo_order DESC LIMIT 1",
	).Scan(&latestHash)
	if err != nil {
		if errors.Is(err, gosql.ErrNoRows) {
			fmt.Println("no commits in database, skipping file_latest verification")
			return nil
		}
		return errors.Wrap(err, "getting latest commit")
	}

	// Get ground truth from git.
	gitFiles, err := gitLsTree(ctx, repoPath, latestHash)
	if err != nil {
		return errors.Wrap(err, "git ls-tree")
	}

	// Get database file set with blob hashes.
	rows, err := tx.QueryContext(ctx,
		"SELECT file_path, blob_sha256 FROM file_latest")
	if err != nil {
		return errors.Wrap(err, "querying file_latest")
	}
	defer rows.Close()

	dbFiles := make(map[string]string) // path -> blob_sha256
	for rows.Next() {
		var path, blobSHA string
		if err := rows.Scan(&path, &blobSHA); err != nil {
			return errors.Wrap(err, "scanning file_latest row")
		}
		dbFiles[path] = blobSHA
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "iterating file_latest rows")
	}

	// Compare path sets bidirectionally.
	var mismatches []string
	for path := range gitFiles {
		if _, ok := dbFiles[path]; !ok {
			mismatches = append(mismatches, fmt.Sprintf("in git but not in DB: %s", path))
		}
	}
	for path := range dbFiles {
		if _, ok := gitFiles[path]; !ok {
			mismatches = append(mismatches, fmt.Sprintf("in DB but not in git: %s", path))
		}
	}

	if len(mismatches) > 0 {
		sort.Strings(mismatches)
		detail := strings.Join(mismatches, "\n  ")
		return errors.Newf(
			"file_latest path mismatch at commit %s (%d mismatches):\n  %s",
			latestHash, len(mismatches), detail,
		)
	}

	// Verify blob SHA-256 content hashes for every file.
	for path, dbSHA := range dbFiles {
		content, err := readBlob(ctx, repoPath, latestHash, path)
		if err != nil {
			return errors.Wrapf(err, "reading blob for %s at %s", path, latestHash)
		}
		gitSHA := sha256.Sum256(content)
		gitSHAHex := hex.EncodeToString(gitSHA[:])
		if gitSHAHex != dbSHA {
			mismatches = append(mismatches, fmt.Sprintf(
				"blob SHA-256 mismatch for %s: db=%s git=%s", path, dbSHA, gitSHAHex))
		}
	}

	if len(mismatches) > 0 {
		sort.Strings(mismatches)
		detail := strings.Join(mismatches, "\n  ")
		return errors.Newf(
			"file_latest blob hash mismatch at commit %s (%d mismatches):\n  %s",
			latestHash, len(mismatches), detail,
		)
	}

	fmt.Printf("file_latest verified: %d files match git at %s\n", len(gitFiles), latestHash[:12])
	return nil
}

// replay rebuilds the file tree from database diffs and verifies against git at
// sampled commits. This is the strongest oracle: it validates the entire diff
// chain stored in the database faithfully reproduces the git history.
//
// Each commit's tree is built by cloning its first parent's tree and applying
// the commit's diffs. This correctly handles branching histories where a single
// cumulative tree would bleed files across sibling branches.
func replay(ctx context.Context, tx *gosql.Tx, repoPath string, verifyEveryN int) error {
	// Load first-parent relationships so we can build per-commit trees.
	firstParent, err := loadFirstParents(ctx, tx)
	if err != nil {
		return err
	}

	rows, err := tx.QueryContext(ctx,
		`SELECT c.hash, c.topo_order, d.file_path, d.op, d.old_path, d.blob_sha256
		 FROM commits c
		 JOIN commit_diffs d ON c.hash = d.commit_hash
		 ORDER BY c.topo_order, d.file_path`,
	)
	if err != nil {
		return errors.Wrap(err, "querying commits with diffs for replay")
	}
	defer rows.Close()

	// Per-commit file trees: commit hash -> (path -> blob SHA-256).
	trees := make(map[string]map[string]string)
	var currentTree map[string]string
	var lastHash string
	var lastTopoOrder int
	var totalVerified int

	for rows.Next() {
		var hash, path, op string
		var topoOrder int
		var oldPath, blobSHA gosql.NullString
		if err := rows.Scan(&hash, &topoOrder, &path, &op, &oldPath, &blobSHA); err != nil {
			return errors.Wrap(err, "scanning replay row")
		}

		// If we've moved to a new commit, finalize the previous one.
		if lastHash != "" && hash != lastHash {
			trees[lastHash] = currentTree
			if shouldVerify(lastTopoOrder, verifyEveryN) {
				if err := verifyTree(ctx, repoPath, lastHash, currentTree); err != nil {
					return errors.Wrapf(err, "replay verification at topo_order=%d", lastTopoOrder)
				}
				totalVerified++
			}
		}

		// Starting a new commit: clone the first parent's tree.
		if hash != lastHash {
			currentTree = cloneParentTree(trees, firstParent, hash)
		}

		// Apply the diff to the current commit's tree.
		switch op {
		case "A", "M":
			if blobSHA.Valid {
				currentTree[path] = blobSHA.String
			}
		case "D":
			delete(currentTree, path)
		case "R":
			if oldPath.Valid {
				delete(currentTree, oldPath.String)
			}
			if blobSHA.Valid {
				currentTree[path] = blobSHA.String
			}
		}

		lastHash = hash
		lastTopoOrder = topoOrder
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "iterating replay rows")
	}

	// Always verify the final commit.
	if lastHash != "" {
		trees[lastHash] = currentTree
		if err := verifyTree(ctx, repoPath, lastHash, currentTree); err != nil {
			return errors.Wrapf(err, "final replay verification at topo_order=%d", lastTopoOrder)
		}
		totalVerified++
	}

	fmt.Printf("replay verified %d commit snapshots\n", totalVerified)
	return nil
}

// loadFirstParents queries the commit_parents table and returns a map from
// each commit hash to its first parent hash (parent_index = 0). Root commits
// have no entry in the returned map.
func loadFirstParents(ctx context.Context, tx *gosql.Tx) (map[string]string, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT commit_hash, parent_hash
		 FROM commit_parents WHERE parent_index = 0`)
	if err != nil {
		return nil, errors.Wrap(err, "querying first parents")
	}
	defer rows.Close()

	firstParent := make(map[string]string)
	for rows.Next() {
		var commitHash, parentHash string
		if err := rows.Scan(&commitHash, &parentHash); err != nil {
			return nil, errors.Wrap(err, "scanning first parent row")
		}
		firstParent[commitHash] = parentHash
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterating first parent rows")
	}
	return firstParent, nil
}

// cloneParentTree returns a copy of the first parent's tree for the given
// commit. If the commit is a root (no first parent), an empty tree is returned.
func cloneParentTree(
	trees map[string]map[string]string, firstParent map[string]string, commitHash string,
) map[string]string {
	parent, hasParent := firstParent[commitHash]
	if !hasParent {
		return make(map[string]string)
	}
	parentTree := trees[parent]
	clone := make(map[string]string, len(parentTree))
	for k, v := range parentTree {
		clone[k] = v
	}
	return clone
}

// shouldVerify returns true if the commit at the given topo order should be
// verified during replay. If verifyEveryN is 0, only the final commit is
// verified (handled by the caller).
func shouldVerify(topoOrder int, verifyEveryN int) bool {
	if verifyEveryN <= 0 {
		return false
	}
	return topoOrder%verifyEveryN == 0
}

// verifyTree compares the in-memory tree against git ls-tree at the given
// commit, verifying both path sets and blob SHA-256 content hashes.
func verifyTree(
	ctx context.Context, repoPath string, commitHash string, tree map[string]string,
) error {
	gitFiles, err := gitLsTree(ctx, repoPath, commitHash)
	if err != nil {
		return errors.Wrap(err, "git ls-tree for verification")
	}

	var mismatches []string
	for path := range gitFiles {
		if _, ok := tree[path]; !ok {
			mismatches = append(mismatches, fmt.Sprintf("in git but not in replay tree: %s", path))
		}
	}
	for path := range tree {
		if _, ok := gitFiles[path]; !ok {
			mismatches = append(mismatches, fmt.Sprintf("in replay tree but not in git: %s", path))
		}
	}

	if len(mismatches) > 0 {
		sort.Strings(mismatches)
		detail := strings.Join(mismatches, "\n  ")
		return errors.Newf(
			"replay tree path mismatch at commit %s (%d mismatches):\n  %s",
			commitHash, len(mismatches), detail,
		)
	}

	// Verify blob SHA-256 content hashes for every file in the tree.
	for path, dbSHA := range tree {
		content, err := readBlob(ctx, repoPath, commitHash, path)
		if err != nil {
			return errors.Wrapf(err, "reading blob for %s at %s", path, commitHash)
		}
		gitSHA := sha256.Sum256(content)
		gitSHAHex := hex.EncodeToString(gitSHA[:])
		if gitSHAHex != dbSHA {
			mismatches = append(mismatches, fmt.Sprintf(
				"blob SHA-256 mismatch for %s: db=%s git=%s", path, dbSHA, gitSHAHex))
		}
	}

	if len(mismatches) > 0 {
		sort.Strings(mismatches)
		detail := strings.Join(mismatches, "\n  ")
		return errors.Newf(
			"replay tree blob hash mismatch at commit %s (%d mismatches):\n  %s",
			commitHash, len(mismatches), detail,
		)
	}
	return nil
}

// verifyBlobContent checks that every blob's stored content matches its
// SHA-256 primary key. This catches storage-layer corruption where the hash
// column survives but the content column is silently corrupted. Blobs with
// NULL content (skip-content mode) are skipped.
func verifyBlobContent(ctx context.Context, tx *gosql.Tx) error {
	rows, err := tx.QueryContext(ctx,
		`SELECT sha256, content FROM blobs WHERE content IS NOT NULL`)
	if err != nil {
		return errors.Wrap(err, "querying blobs for content verification")
	}
	defer rows.Close()

	var mismatches []string
	var count int
	for rows.Next() {
		var storedSHA string
		var content []byte
		if err := rows.Scan(&storedSHA, &content); err != nil {
			return errors.Wrap(err, "scanning blob row")
		}
		count++

		computed := sha256.Sum256(content)
		computedHex := hex.EncodeToString(computed[:])
		if computedHex != storedSHA {
			mismatches = append(mismatches, fmt.Sprintf(
				"blob %s: stored SHA-256 does not match content (computed %s)",
				storedSHA, computedHex,
			))
		}
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "iterating blob rows")
	}

	if len(mismatches) > 0 {
		sort.Strings(mismatches)
		detail := strings.Join(mismatches, "\n  ")
		return errors.Newf(
			"blob content integrity failure (%d issues):\n  %s",
			len(mismatches), detail,
		)
	}

	fmt.Printf("blob content verified: %d blobs, all content matches SHA-256 key\n", count)
	return nil
}

// verifyReferentialIntegrity checks that all foreign key relationships hold
// at the data level. This catches corruption even when FK constraints exist,
// since constraints can be violated by bugs in the storage or transaction
// layer.
func verifyReferentialIntegrity(ctx context.Context, tx *gosql.Tx) error {
	type refCheck struct {
		name  string
		query string
	}
	checks := []refCheck{
		{
			"commit_parents.commit_hash → commits",
			`SELECT count(*) FROM commit_parents cp
			 LEFT JOIN commits c ON cp.commit_hash = c.hash
			 WHERE c.hash IS NULL`,
		},
		{
			"commit_parents.parent_hash → commits",
			`SELECT count(*) FROM commit_parents cp
			 LEFT JOIN commits c ON cp.parent_hash = c.hash
			 WHERE c.hash IS NULL`,
		},
		{
			"commit_diffs.commit_hash → commits",
			`SELECT count(*) FROM commit_diffs cd
			 LEFT JOIN commits c ON cd.commit_hash = c.hash
			 WHERE c.hash IS NULL`,
		},
		{
			"file_latest.last_commit_hash → commits",
			`SELECT count(*) FROM file_latest fl
			 LEFT JOIN commits c ON fl.last_commit_hash = c.hash
			 WHERE c.hash IS NULL`,
		},
		{
			"commit_diffs.blob_sha256 → blobs",
			`SELECT count(*) FROM commit_diffs cd
			 LEFT JOIN blobs b ON cd.blob_sha256 = b.sha256
			 WHERE cd.blob_sha256 IS NOT NULL AND b.sha256 IS NULL`,
		},
		{
			"file_latest.blob_sha256 → blobs",
			`SELECT count(*) FROM file_latest fl
			 LEFT JOIN blobs b ON fl.blob_sha256 = b.sha256
			 WHERE fl.blob_sha256 IS NOT NULL AND b.sha256 IS NULL`,
		},
	}

	var mismatches []string
	for _, check := range checks {
		var orphaned int64
		if err := tx.QueryRowContext(ctx, check.query).Scan(&orphaned); err != nil {
			return errors.Wrapf(err, "checking %s", check.name)
		}
		if orphaned > 0 {
			mismatches = append(mismatches, fmt.Sprintf(
				"%s: %d orphaned rows", check.name, orphaned,
			))
		}
	}

	if len(mismatches) > 0 {
		detail := strings.Join(mismatches, "\n  ")
		return errors.Newf(
			"referential integrity violation (%d issues):\n  %s",
			len(mismatches), detail,
		)
	}

	fmt.Printf("referential integrity verified for %d relationships\n", len(checks))
	return nil
}

// indexCheck defines a secondary index consistency check.
type indexCheck struct {
	name      string // human-readable name
	table     string // table name
	indexName string // secondary index name
	columns   string // columns to SELECT for comparison
}

// verifySecondaryIndexes compares primary key scans against forced secondary
// index scans using EXCEPT ALL. Any difference indicates index corruption.
func verifySecondaryIndexes(ctx context.Context, tx *gosql.Tx) error {
	checks := []indexCheck{
		{
			name:      "commit_diffs_file_path",
			table:     "commit_diffs",
			indexName: "idx_commit_diffs_file_path",
			columns:   "commit_hash, file_path, op, old_path, file_mode, blob_sha256",
		},
		{
			name:      "commits_topo_order",
			table:     "commits",
			indexName: "idx_commits_topo_order",
			columns:   "hash, topo_order",
		},
		{
			name:      "file_latest_last_commit",
			table:     "file_latest",
			indexName: "idx_file_latest_last_commit",
			columns:   "file_path, last_commit_hash, last_topo_order, blob_sha256",
		},
	}

	for _, check := range checks {
		if err := verifyOneIndex(ctx, tx, check); err != nil {
			return errors.Wrapf(err, "index check %s", check.name)
		}
	}

	fmt.Printf("secondary index consistency verified for %d indexes\n",
		len(checks))
	return nil
}

// verifyOneIndex compares the result of a primary key scan against a forced
// secondary index scan for a single index using EXCEPT ALL.
func verifyOneIndex(ctx context.Context, tx *gosql.Tx, check indexCheck) error {
	// Count from primary scan.
	var primaryCount int64
	if err := tx.QueryRowContext(ctx,
		fmt.Sprintf("SELECT count(*) FROM %s", check.table),
	).Scan(&primaryCount); err != nil {
		return errors.Wrap(err, "counting primary rows")
	}

	// Count from index scan.
	var indexCount int64
	if err := tx.QueryRowContext(ctx,
		fmt.Sprintf("SELECT count(*) FROM %s@{FORCE_INDEX=%s}",
			check.table, check.indexName),
	).Scan(&indexCount); err != nil {
		return errors.Wrap(err, "counting index rows")
	}

	if primaryCount != indexCount {
		return errors.Newf(
			"row count mismatch: primary=%d, index %s=%d",
			primaryCount, check.indexName, indexCount,
		)
	}

	// Rows in primary but missing from index.
	var missingFromIndex int64
	if err := tx.QueryRowContext(ctx,
		fmt.Sprintf(
			`SELECT count(*) FROM (
				SELECT %[1]s FROM %[2]s
				EXCEPT ALL
				SELECT %[1]s FROM %[2]s@{FORCE_INDEX=%[3]s}
			)`,
			check.columns, check.table, check.indexName,
		),
	).Scan(&missingFromIndex); err != nil {
		return errors.Wrap(err, "checking primary EXCEPT index")
	}

	// Rows in index but not in primary.
	var extraInIndex int64
	if err := tx.QueryRowContext(ctx,
		fmt.Sprintf(
			`SELECT count(*) FROM (
				SELECT %[1]s FROM %[2]s@{FORCE_INDEX=%[3]s}
				EXCEPT ALL
				SELECT %[1]s FROM %[2]s
			)`,
			check.columns, check.table, check.indexName,
		),
	).Scan(&extraInIndex); err != nil {
		return errors.Wrap(err, "checking index EXCEPT primary")
	}

	if missingFromIndex > 0 || extraInIndex > 0 {
		return errors.Newf(
			"index %s data mismatch: %d rows in primary but not in index, "+
				"%d rows in index but not in primary",
			check.indexName, missingFromIndex, extraInIndex,
		)
	}

	return nil
}
