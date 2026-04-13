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
	"slices"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
)

type blobData struct {
	sha256Hex string
	content   []byte
	size      int
}

type commitData struct {
	hash      string
	topoOrder int
	parents   []string
	meta      commitMeta
	diffs     []diffEntry
	blobs     map[string]blobData
}

// readBlobsForDiffs reads blob content for all non-delete diffs in a commit,
// computing SHA-256 hashes and optionally truncating content.
func readBlobsForDiffs(
	ctx context.Context,
	repoPath string,
	hash string,
	diffs []diffEntry,
	maxBlobSize int,
	skipContent bool,
) (map[string]blobData, error) {
	blobs := make(map[string]blobData)
	for _, d := range diffs {
		if d.op == 'D' {
			continue
		}
		if _, ok := blobs[d.path]; ok {
			continue
		}
		content, err := readBlob(ctx, repoPath, hash, d.path)
		if err != nil {
			return nil, errors.Wrapf(err, "reading blob %s:%s", hash, d.path)
		}
		h := sha256.Sum256(content)
		bd := blobData{
			sha256Hex: hex.EncodeToString(h[:]),
			size:      len(content),
		}
		if !skipContent {
			if maxBlobSize > 0 && len(content) > maxBlobSize {
				content = content[:maxBlobSize]
			}
			bd.content = content
		}
		blobs[d.path] = bd
	}
	return blobs, nil
}

// ingest loads the commit history from the git repo at repoPath into the
// database. It produces a topological ordering of the DAG using the given seed
// for tie-breaking, then inserts commits in batches within retry-safe
// transactions.
func ingest(
	ctx context.Context,
	db *gosql.DB,
	repoPath string,
	seed int64,
	maxCommits int,
	maxBlobSize int,
	skipContent bool,
	hists *histogram.Histograms,
	agentID string,
	batchSize int,
) error {
	var alreadyIngested int
	err := db.QueryRowContext(ctx,
		"SELECT total_commits FROM repo_stats WHERE agent_id = $1", agentID,
	).Scan(&alreadyIngested)
	if err != nil {
		return errors.Wrap(err, "reading repo_stats for resume check")
	}
	if alreadyIngested > 0 {
		fmt.Printf("%s: repo_stats shows %d commits already ingested, resuming\n",
			agentID, alreadyIngested)
	}

	fmt.Printf("%s: loading commit DAG from git repo...\n", agentID)
	dag, err := loadCommitDAG(ctx, repoPath)
	if err != nil {
		return errors.Wrap(err, "loading commit DAG")
	}
	fmt.Printf("%s: loaded DAG with %d commits, computing topological sort...\n",
		agentID, len(dag))

	order, err := randomTopoSort(dag, seed)
	if err != nil {
		return errors.Wrap(err, "topological sort")
	}

	if maxCommits > 0 && len(order) > maxCommits {
		order = order[:maxCommits]
	}

	if alreadyIngested > 0 && alreadyIngested < len(order) {
		var lastHash string
		err := db.QueryRowContext(ctx,
			"SELECT hash FROM commits WHERE agent_id = $1 ORDER BY topo_order DESC LIMIT 1",
			agentID,
		).Scan(&lastHash)
		if err != nil {
			return errors.Wrap(err, "validating resume state")
		}
		if lastHash != order[alreadyIngested-1] {
			return errors.Newf(
				"resume state mismatch: last ingested commit %s does not match "+
					"expected %s (seed may have changed); clear tables and restart",
				lastHash[:12], order[alreadyIngested-1][:12],
			)
		}
		order = order[alreadyIngested:]
	} else if alreadyIngested >= len(order) {
		fmt.Printf("%s: all %d commits already ingested, nothing to do\n",
			agentID, alreadyIngested)
		return nil
	}

	total := alreadyIngested + len(order)
	for batchStart := 0; batchStart < len(order); batchStart += batchSize {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		batchEnd := min(batchStart+batchSize, len(order))
		batch := order[batchStart:batchEnd]

		commits := make([]commitData, 0, len(batch))
		for i, hash := range batch {
			topoOrder := alreadyIngested + batchStart + i + 1
			meta, err := getCommitMeta(ctx, repoPath, hash)
			if err != nil {
				return errors.Wrapf(err, "getting commit metadata for %s", hash)
			}
			diffs, err := getDiffTree(ctx, repoPath, hash, dag[hash])
			if err != nil {
				return errors.Wrapf(err, "getting diff tree for %s", hash)
			}
			blobs, err := readBlobsForDiffs(
				ctx, repoPath, hash, diffs, maxBlobSize, skipContent,
			)
			if err != nil {
				return err
			}
			commits = append(commits, commitData{
				hash:      hash,
				topoOrder: topoOrder,
				parents:   dag[hash],
				meta:      meta,
				diffs:     diffs,
				blobs:     blobs,
			})
		}

		start := timeutil.Now()
		if err := insertCommitBatch(
			ctx, db, commits, skipContent, agentID,
		); err != nil {
			return errors.Wrapf(err, "inserting batch at topo_order %d-%d",
				commits[0].topoOrder, commits[len(commits)-1].topoOrder)
		}
		if hists != nil {
			hists.Get("ingest-commit-batch").Record(timeutil.Since(start))
		}

		lastTopo := commits[len(commits)-1].topoOrder
		fmt.Printf("%s: ingested %d/%d commits\n", agentID, lastTopo, total)
	}

	fmt.Printf("%s: ingestion complete: %d commits\n", agentID, total)
	return nil
}

// insertCommitBatch writes multiple commits in a single transaction, reducing
// per-transaction overhead (consensus round-trips, intent resolution).
func insertCommitBatch(
	ctx context.Context, db *gosql.DB, commits []commitData, skipContent bool, agentID string,
) error {
	return crdb.ExecuteTx(ctx, db, nil /* txopts */, func(tx *gosql.Tx) error {
		var totalNewBlobBytes int64
		totalFilesChanged := 0

		for _, c := range commits {
			for _, bd := range c.blobs {
				var res gosql.Result
				var err error
				if skipContent {
					res, err = tx.ExecContext(ctx,
						`INSERT INTO blobs (sha256, size, content)
						 VALUES ($1, $2, NULL)
						 ON CONFLICT (sha256) DO NOTHING`,
						bd.sha256Hex, bd.size,
					)
				} else {
					res, err = tx.ExecContext(ctx,
						`INSERT INTO blobs (sha256, size, content)
						 VALUES ($1, $2, $3)
						 ON CONFLICT (sha256) DO NOTHING`,
						bd.sha256Hex, bd.size, bd.content,
					)
				}
				if err != nil {
					return errors.Wrap(err, "inserting blob")
				}
				affected, err := res.RowsAffected()
				if err != nil {
					return errors.Wrap(err, "checking blob rows affected")
				}
				if affected > 0 {
					totalNewBlobBytes += int64(bd.size)
				}
			}

			if _, err := tx.ExecContext(ctx,
				`INSERT INTO commits (agent_id, hash, tree_hash, author_name,
				 author_email, author_date, committer_name, committer_email,
				 committer_date, message, topo_order)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
				agentID, c.meta.hash, c.meta.treeHash, c.meta.authorName,
				c.meta.authorEmail, c.meta.authorDate, c.meta.committerName,
				c.meta.committerEmail, c.meta.committerDate, c.meta.message,
				c.topoOrder,
			); err != nil {
				return errors.Wrap(err, "inserting commit")
			}

			for i, parent := range c.parents {
				if _, err := tx.ExecContext(ctx,
					`INSERT INTO commit_parents
					 (agent_id, commit_hash, parent_hash, parent_index)
					 VALUES ($1, $2, $3, $4)`,
					agentID, c.hash, parent, i,
				); err != nil {
					return errors.Wrap(err, "inserting commit parent")
				}
			}

			slices.SortStableFunc(c.diffs, func(a, b diffEntry) int {
				return diffOpPriority(a.op) - diffOpPriority(b.op)
			})

			for _, d := range c.diffs {
				var blobSHA *string
				if d.op != 'D' {
					if bd, ok := c.blobs[d.path]; ok {
						blobSHA = &bd.sha256Hex
					}
				}

				if _, err := tx.ExecContext(ctx,
					`INSERT INTO commit_diffs
					 (agent_id, commit_hash, file_path, op, old_path,
					  file_mode, blob_sha256)
					 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
					agentID, c.hash, d.path, string(d.op),
					nilIfEmpty(d.oldPath), d.mode, blobSHA,
				); err != nil {
					return errors.Wrap(err, "inserting commit diff")
				}

				switch d.op {
				case 'A', 'M':
					if _, err := tx.ExecContext(ctx,
						`UPSERT INTO file_latest
						 (agent_id, file_path, last_commit_hash,
						  last_topo_order, blob_sha256)
						 VALUES ($1, $2, $3, $4, $5)`,
						agentID, d.path, c.hash, c.topoOrder, blobSHA,
					); err != nil {
						return errors.Wrap(err, "upserting file_latest")
					}
				case 'D':
					if _, err := tx.ExecContext(ctx,
						`DELETE FROM file_latest
						 WHERE agent_id = $1 AND file_path = $2`,
						agentID, d.path,
					); err != nil {
						return errors.Wrap(err, "deleting file_latest")
					}
				case 'R':
					if _, err := tx.ExecContext(ctx,
						`DELETE FROM file_latest
						 WHERE agent_id = $1 AND file_path = $2`,
						agentID, d.oldPath,
					); err != nil {
						return errors.Wrap(err, "deleting old file_latest for rename")
					}
					if _, err := tx.ExecContext(ctx,
						`UPSERT INTO file_latest
						 (agent_id, file_path, last_commit_hash,
						  last_topo_order, blob_sha256)
						 VALUES ($1, $2, $3, $4, $5)`,
						agentID, d.path, c.hash, c.topoOrder, blobSHA,
					); err != nil {
						return errors.Wrap(err, "upserting new file_latest for rename")
					}
				}
			}

			totalFilesChanged += len(c.diffs)
		}

		_, err := tx.ExecContext(ctx,
			`UPDATE repo_stats SET
			 total_commits = total_commits + $1,
			 total_files_changed = total_files_changed + $2,
			 total_blobs_bytes = total_blobs_bytes + $3
			 WHERE agent_id = $4`,
			len(commits), totalFilesChanged, totalNewBlobBytes, agentID)
		return err
	})
}

// nilIfEmpty returns nil if s is empty, otherwise a pointer to s.
func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// diffOpPriority returns a sort key for diff operations. Deletions are
// processed first so that a rename's old_path removal doesn't clobber a
// path upserted by another diff in the same commit.
func diffOpPriority(op byte) int {
	switch op {
	case 'D':
		return 0
	case 'R':
		return 1
	default:
		return 2
	}
}

// clearAgentData deletes all rows belonging to a single agent using DELETE
// (not TRUNCATE) so that other agents' data is preserved during concurrent
// clear/ingest cycles. Blobs are shared and not deleted.
func clearAgentData(ctx context.Context, db *gosql.DB, agentID string) error {
	for _, tbl := range []string{
		"commit_diffs", "commit_parents", "file_latest", "commits",
	} {
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %s WHERE agent_id = $1", tbl), agentID,
		); err != nil {
			return errors.Wrapf(err, "deleting %s for %s", tbl, agentID)
		}
	}
	if _, err := db.ExecContext(ctx,
		`UPDATE repo_stats SET
		 total_commits = 0, total_files_changed = 0, total_blobs_bytes = 0
		 WHERE agent_id = $1`, agentID,
	); err != nil {
		return errors.Wrapf(err, "resetting repo_stats for %s", agentID)
	}
	return nil
}
