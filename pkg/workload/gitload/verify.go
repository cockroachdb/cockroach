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

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/errors"
)

// discoverAgentsTx returns the list of agent IDs from repo_stats within
// an existing transaction.
func discoverAgentsTx(ctx context.Context, tx *gosql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx,
		"SELECT agent_id FROM repo_stats ORDER BY agent_id")
	if err != nil {
		return nil, errors.Wrap(err, "discovering agents")
	}
	defer rows.Close()

	var agents []string
	for rows.Next() {
		var aid string
		if err := rows.Scan(&aid); err != nil {
			return nil, errors.Wrap(err, "scanning agent_id")
		}
		agents = append(agents, aid)
	}
	return agents, rows.Err()
}

// runAllChecks runs all oracle verification checks within a single
// retry-safe read-only transaction. It auto-discovers agents from
// repo_stats and runs per-agent checks for each, then runs cross-agent
// shared checks. Agents with 0 commits (mid-cycle) are skipped; the
// cross-agent checks still run regardless.
func runAllChecks(ctx context.Context, db *gosql.DB, baseRepoPath string, verifyEveryN int) error {
	txOpts := &gosql.TxOptions{ReadOnly: true}
	return crdb.ExecuteTx(ctx, db, txOpts, func(tx *gosql.Tx) error {
		agents, err := discoverAgentsTx(ctx, tx)
		if err != nil {
			return err
		}
		if len(agents) == 0 {
			fmt.Println("no agents found in repo_stats, nothing to verify")
			return nil
		}

		for _, aid := range agents {
			repoPath := agentRepoPath(baseRepoPath, aid)

			var actualCommits int64
			if err := tx.QueryRowContext(ctx,
				"SELECT count(*) FROM commits WHERE agent_id = $1", aid,
			).Scan(&actualCommits); err != nil {
				return errors.Wrapf(err, "counting commits for %s", aid)
			}
			if actualCommits == 0 {
				fmt.Printf("%s: 0 commits (mid-cycle or freshly cleared), skipping\n", aid)
				continue
			}

			if err := verifyRepoStats(ctx, tx, aid); err != nil {
				return errors.Wrapf(err, "%s: repo stats verification failed", aid)
			}
			if err := verifyCommitGraph(
				ctx, tx, repoPath, true /* partial */, 0, aid,
			); err != nil {
				return errors.Wrapf(err, "%s: commit graph verification failed", aid)
			}
			if err := verifyCommitMetadata(ctx, tx, repoPath, aid); err != nil {
				return errors.Wrapf(err, "%s: commit metadata verification failed", aid)
			}
			if err := verifyFileLatest(ctx, tx, repoPath, aid); err != nil {
				return errors.Wrapf(err, "%s: file_latest verification failed", aid)
			}
			if err := replay(ctx, tx, repoPath, verifyEveryN, aid); err != nil {
				return errors.Wrapf(err, "%s: replay verification failed", aid)
			}
		}

		if err := verifyTotalBlobBytes(ctx, tx); err != nil {
			return errors.Wrap(err, "total blob bytes verification failed")
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
		return nil
	})
}

// runInlineChecks runs verification checks that are safe to execute while
// ingestion is in progress. It reads a consistent snapshot via AS OF SYSTEM
// TIME follower_read_timestamp() and validates the partial data that has been
// ingested so far.
func runInlineChecks(
	ctx context.Context, db *gosql.DB, baseRepoPath string, verifyEveryN int,
) error {
	tx, err := db.BeginTx(ctx, &gosql.TxOptions{ReadOnly: true})
	if err != nil {
		return errors.Wrap(err, "starting inline verification transaction")
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		"SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()"); err != nil {
		return errors.Wrap(err, "setting AS OF SYSTEM TIME")
	}

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

	agents, err := discoverAgentsTx(ctx, tx)
	if err != nil {
		return err
	}

	fmt.Printf("inline verification: checking %d commits across %d agents\n",
		commitCount, len(agents))

	for _, aid := range agents {
		repoPath := agentRepoPath(baseRepoPath, aid)

		var agentCommits int64
		if err := tx.QueryRowContext(ctx,
			"SELECT count(*) FROM commits WHERE agent_id = $1", aid,
		).Scan(&agentCommits); err != nil {
			return errors.Wrapf(err, "counting commits for %s", aid)
		}
		if agentCommits == 0 {
			continue
		}

		if err := verifyRepoStats(ctx, tx, aid); err != nil {
			return errors.Wrapf(err, "inline: %s: repo stats failed", aid)
		}
		if err := verifyCommitGraph(
			ctx, tx, repoPath, true /* partial */, 0, aid,
		); err != nil {
			return errors.Wrapf(err, "inline: %s: commit graph failed", aid)
		}
		if err := verifyCommitMetadata(ctx, tx, repoPath, aid); err != nil {
			return errors.Wrapf(err, "inline: %s: commit metadata failed", aid)
		}
		// verifyFileLatest is skipped during inline verification: during
		// partial ingestion of a branching DAG, file_latest accumulates
		// diffs from multiple branches, but git ls-tree at the latest
		// commit only reflects one branch's tree.
		if err := replay(ctx, tx, repoPath, verifyEveryN, aid); err != nil {
			return errors.Wrapf(err, "inline: %s: replay failed", aid)
		}
	}

	if err := verifyTotalBlobBytes(ctx, tx); err != nil {
		return errors.Wrap(err, "inline: total blob bytes failed")
	}
	if err := verifyBlobContent(ctx, tx); err != nil {
		return errors.Wrap(err, "inline: blob content failed")
	}
	if err := verifyReferentialIntegrity(ctx, tx); err != nil {
		return errors.Wrap(err, "inline: referential integrity failed")
	}
	if err := verifySecondaryIndexes(ctx, tx); err != nil {
		return errors.Wrap(err, "inline: secondary index failed")
	}

	fmt.Printf("inline verification passed: %d commits verified\n", commitCount)
	return tx.Commit()
}

// verifyRepoStats checks that the repo_stats counters match actual row counts
// for a single agent.
func verifyRepoStats(ctx context.Context, tx *gosql.Tx, agentID string) error {
	var totalCommits, totalFilesChanged int64
	if err := tx.QueryRowContext(ctx,
		`SELECT total_commits, total_files_changed
		 FROM repo_stats WHERE agent_id = $1`, agentID,
	).Scan(&totalCommits, &totalFilesChanged); err != nil {
		return errors.Wrap(err, "reading repo_stats")
	}

	var actualCommits int64
	if err := tx.QueryRowContext(ctx,
		"SELECT count(*) FROM commits WHERE agent_id = $1", agentID,
	).Scan(&actualCommits); err != nil {
		return errors.Wrap(err, "counting commits")
	}

	var actualDiffs int64
	if err := tx.QueryRowContext(ctx,
		"SELECT count(*) FROM commit_diffs WHERE agent_id = $1", agentID,
	).Scan(&actualDiffs); err != nil {
		return errors.Wrap(err, "counting commit_diffs")
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

	fmt.Printf("%s: repo_stats verified: %d commits, %d file changes\n",
		agentID, totalCommits, totalFilesChanged)
	return nil
}

// verifyTotalBlobBytes checks that the sum of all agents'
// total_blobs_bytes does not exceed the actual sum of blob sizes.
// Blobs are shared across agents via ON CONFLICT DO NOTHING and are
// never deleted, so after clear+re-ingest cycles the stats may
// undercount (re-inserted blobs are already present). The invariant
// is: stats <= actual. Overcounting would indicate a bug.
func verifyTotalBlobBytes(ctx context.Context, tx *gosql.Tx) error {
	var statsBlobBytes int64
	if err := tx.QueryRowContext(ctx,
		"SELECT coalesce(sum(total_blobs_bytes), 0) FROM repo_stats",
	).Scan(&statsBlobBytes); err != nil {
		return errors.Wrap(err, "summing repo_stats blob bytes")
	}

	var actualBlobBytes int64
	if err := tx.QueryRowContext(ctx,
		"SELECT coalesce(sum(size), 0) FROM blobs",
	).Scan(&actualBlobBytes); err != nil {
		return errors.Wrap(err, "summing blob sizes")
	}

	if statsBlobBytes > actualBlobBytes {
		return errors.Newf(
			"sum(repo_stats.total_blobs_bytes)=%d exceeds sum(blobs.size)=%d",
			statsBlobBytes, actualBlobBytes,
		)
	}

	fmt.Printf("total blob bytes verified: stats=%d, actual=%d bytes\n",
		statsBlobBytes, actualBlobBytes)
	return nil
}

// verifyCommitGraph compares the commit_parents table against the git DAG
// loaded via git rev-list --parents. When partial is false, it verifies a
// full bidirectional match. When partial is true, only commits present in
// the DB are checked.
func verifyCommitGraph(
	ctx context.Context, tx *gosql.Tx, repoPath string, partial bool, maxCommits int, agentID string,
) error {
	gitDAG, err := loadCommitDAG(ctx, repoPath)
	if err != nil {
		return errors.Wrap(err, "loading git DAG")
	}

	var dbCommitCount int64
	if err := tx.QueryRowContext(ctx,
		"SELECT count(*) FROM commits WHERE agent_id = $1", agentID,
	).Scan(&dbCommitCount); err != nil {
		return errors.Wrap(err, "counting commits")
	}

	expectedCommits := int64(len(gitDAG))
	if maxCommits > 0 && int64(maxCommits) < expectedCommits {
		expectedCommits = int64(maxCommits)
		partial = true
	}

	if !partial {
		if expectedCommits != dbCommitCount {
			return errors.Newf(
				"commit count mismatch: git has %d commits, DB has %d",
				len(gitDAG), dbCommitCount,
			)
		}
	}

	rows, err := tx.QueryContext(ctx,
		`SELECT commit_hash, parent_hash, parent_index
		 FROM commit_parents WHERE agent_id = $1
		 ORDER BY commit_hash, parent_index`, agentID)
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
		_ = parentIndex
		dbDAG[commitHash] = append(dbDAG[commitHash], parentHash)
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "iterating commit_parents rows")
	}

	var mismatches []string

	if partial {
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
		fmt.Printf("%s: commit graph verified (partial): %d/%d commits\n",
			agentID, dbCommitCount, len(gitDAG))
	} else {
		fmt.Printf("%s: commit graph verified: %d commits\n",
			agentID, len(gitDAG))
	}
	return nil
}

// verifyCommitMetadata compares every field in the commits table against git.
func verifyCommitMetadata(
	ctx context.Context, tx *gosql.Tx, repoPath string, agentID string,
) error {
	rows, err := tx.QueryContext(ctx,
		`SELECT hash, tree_hash, author_name, author_email, author_date,
		        committer_name, committer_email, committer_date, message
		 FROM commits WHERE agent_id = $1`, agentID)
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

	fmt.Printf("%s: commit metadata verified: %d commits\n", agentID, count)
	return nil
}

// verifyFileLatest compares the file_latest table against git ls-tree at the
// latest commit for a specific agent.
func verifyFileLatest(ctx context.Context, tx *gosql.Tx, repoPath string, agentID string) error {
	var latestHash string
	err := tx.QueryRowContext(ctx,
		"SELECT hash FROM commits WHERE agent_id = $1 ORDER BY topo_order DESC LIMIT 1",
		agentID,
	).Scan(&latestHash)
	if err != nil {
		if errors.Is(err, gosql.ErrNoRows) {
			fmt.Printf("%s: no commits, skipping file_latest verification\n", agentID)
			return nil
		}
		return errors.Wrap(err, "getting latest commit")
	}

	gitFiles, err := gitLsTree(ctx, repoPath, latestHash)
	if err != nil {
		return errors.Wrap(err, "git ls-tree")
	}

	rows, err := tx.QueryContext(ctx,
		"SELECT file_path, blob_sha256 FROM file_latest WHERE agent_id = $1",
		agentID)
	if err != nil {
		return errors.Wrap(err, "querying file_latest")
	}
	defer rows.Close()

	dbFiles := make(map[string]string)
	for rows.Next() {
		var path string
		var blobSHA gosql.NullString
		if err := rows.Scan(&path, &blobSHA); err != nil {
			return errors.Wrap(err, "scanning file_latest row")
		}
		if blobSHA.Valid {
			dbFiles[path] = blobSHA.String
		} else {
			dbFiles[path] = ""
		}
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "iterating file_latest rows")
	}

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

	fmt.Printf("%s: file_latest verified: %d files match git at %s\n",
		agentID, len(gitFiles), latestHash[:12])
	return nil
}

// replay rebuilds the file tree from database diffs and verifies against git at
// sampled commits for a specific agent.
func replay(
	ctx context.Context, tx *gosql.Tx, repoPath string, verifyEveryN int, agentID string,
) error {
	firstParent, err := loadFirstParents(ctx, tx, agentID)
	if err != nil {
		return err
	}

	rows, err := tx.QueryContext(ctx,
		`SELECT c.hash, c.topo_order, d.file_path, d.op, d.old_path, d.blob_sha256
		 FROM commits c
		 JOIN commit_diffs d ON c.agent_id = d.agent_id AND c.hash = d.commit_hash
		 WHERE c.agent_id = $1
		 ORDER BY c.topo_order, d.file_path`, agentID,
	)
	if err != nil {
		return errors.Wrap(err, "querying commits with diffs for replay")
	}
	defer rows.Close()

	trees := make(map[string]map[string]string)

	refCount := make(map[string]int)
	for _, parent := range firstParent {
		refCount[parent]++
	}

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

		if lastHash != "" && hash != lastHash {
			trees[lastHash] = currentTree
			if shouldVerify(lastTopoOrder, verifyEveryN) {
				if err := verifyTree(ctx, repoPath, lastHash, currentTree); err != nil {
					return errors.Wrapf(err, "replay verification at topo_order=%d", lastTopoOrder)
				}
				totalVerified++
			}
			if parent, ok := firstParent[lastHash]; ok {
				refCount[parent]--
				if refCount[parent] <= 0 {
					delete(trees, parent)
				}
			}
		}

		if hash != lastHash {
			currentTree = cloneParentTree(trees, firstParent, hash)
		}

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

	if lastHash != "" {
		trees[lastHash] = currentTree
		if err := verifyTree(ctx, repoPath, lastHash, currentTree); err != nil {
			return errors.Wrapf(err, "final replay verification at topo_order=%d", lastTopoOrder)
		}
		totalVerified++
		if parent, ok := firstParent[lastHash]; ok {
			refCount[parent]--
			if refCount[parent] <= 0 {
				delete(trees, parent)
			}
		}
	}

	fmt.Printf("%s: replay verified %d commit snapshots\n", agentID, totalVerified)
	return nil
}

// loadFirstParents queries the commit_parents table and returns a map from
// each commit hash to its first parent hash (parent_index = 0).
func loadFirstParents(
	ctx context.Context, tx *gosql.Tx, agentID string,
) (map[string]string, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT commit_hash, parent_hash
		 FROM commit_parents WHERE agent_id = $1 AND parent_index = 0`,
		agentID)
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
// verified during replay.
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
// SHA-256 primary key. Truncated blobs (size > len(content)) are skipped.
func verifyBlobContent(ctx context.Context, tx *gosql.Tx) error {
	rows, err := tx.QueryContext(ctx,
		`SELECT sha256, size, content FROM blobs WHERE content IS NOT NULL`)
	if err != nil {
		return errors.Wrap(err, "querying blobs for content verification")
	}
	defer rows.Close()

	var mismatches []string
	var count, truncatedCount int
	for rows.Next() {
		var storedSHA string
		var size int
		var content []byte
		if err := rows.Scan(&storedSHA, &size, &content); err != nil {
			return errors.Wrap(err, "scanning blob row")
		}
		count++

		if size > len(content) {
			truncatedCount++
			continue
		}

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

	fmt.Printf(
		"blob content verified: %d blobs (%d truncated, skipped)\n",
		count, truncatedCount,
	)
	return nil
}

// verifyReferentialIntegrity checks that all foreign key relationships hold
// at the data level.
func verifyReferentialIntegrity(ctx context.Context, tx *gosql.Tx) error {
	type refCheck struct {
		name  string
		query string
	}
	checks := []refCheck{
		{
			"commit_parents.commit_hash → commits",
			`SELECT count(*) FROM commit_parents cp
			 LEFT JOIN commits c ON cp.agent_id = c.agent_id AND cp.commit_hash = c.hash
			 WHERE c.hash IS NULL`,
		},
		{
			"commit_parents.parent_hash → commits",
			`SELECT count(*) FROM commit_parents cp
			 LEFT JOIN commits c ON cp.agent_id = c.agent_id AND cp.parent_hash = c.hash
			 WHERE c.hash IS NULL`,
		},
		{
			"commit_diffs.commit_hash → commits",
			`SELECT count(*) FROM commit_diffs cd
			 LEFT JOIN commits c ON cd.agent_id = c.agent_id AND cd.commit_hash = c.hash
			 WHERE c.hash IS NULL`,
		},
		{
			"file_latest.last_commit_hash → commits",
			`SELECT count(*) FROM file_latest fl
			 LEFT JOIN commits c ON fl.agent_id = c.agent_id AND fl.last_commit_hash = c.hash
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
	name      string
	table     string
	indexName string
	columns   string
}

// verifySecondaryIndexes compares primary key scans against forced secondary
// index scans using EXCEPT ALL.
func verifySecondaryIndexes(ctx context.Context, tx *gosql.Tx) error {
	checks := []indexCheck{
		{
			name:      "commit_diffs_file_path",
			table:     "commit_diffs",
			indexName: "idx_commit_diffs_file_path",
			columns:   "agent_id, commit_hash, file_path, op, old_path, file_mode, blob_sha256",
		},
		{
			name:      "commits_topo_order",
			table:     "commits",
			indexName: "idx_commits_topo_order",
			columns:   "agent_id, hash, topo_order",
		},
		{
			name:      "file_latest_last_commit",
			table:     "file_latest",
			indexName: "idx_file_latest_last_commit",
			columns:   "agent_id, file_path, last_commit_hash, last_topo_order, blob_sha256",
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
	var primaryCount int64
	if err := tx.QueryRowContext(ctx,
		fmt.Sprintf("SELECT count(*) FROM %s", check.table),
	).Scan(&primaryCount); err != nil {
		return errors.Wrap(err, "counting primary rows")
	}

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
