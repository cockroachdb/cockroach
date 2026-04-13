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

// ingest loads the commit history from the git repo at repoPath into the
// database. It produces a topological ordering of the DAG using the given seed
// for tie-breaking, then inserts each commit within a retry-safe transaction.
func ingest(
	ctx context.Context,
	db *gosql.DB,
	repoPath string,
	seed int64,
	maxCommits int,
	maxBlobSize int,
	skipContent bool,
	hists *histogram.Histograms,
) error {
	// Check for resume: read current total_commits.
	var alreadyIngested int
	err := db.QueryRowContext(ctx,
		"SELECT total_commits FROM repo_stats WHERE id = 1",
	).Scan(&alreadyIngested)
	if err != nil {
		return errors.Wrap(err, "reading repo_stats for resume check")
	}
	if alreadyIngested > 0 {
		fmt.Printf("repo_stats shows %d commits already ingested, resuming\n", alreadyIngested)
	}

	fmt.Println("loading commit DAG from git repo...")
	dag, err := loadCommitDAG(ctx, repoPath)
	if err != nil {
		return errors.Wrap(err, "loading commit DAG")
	}
	fmt.Printf("loaded DAG with %d commits, computing topological sort...\n", len(dag))

	order, err := randomTopoSort(dag, seed)
	if err != nil {
		return errors.Wrap(err, "topological sort")
	}
	fmt.Printf("topological sort complete, %d commits to process\n", len(order))

	if maxCommits > 0 && len(order) > maxCommits {
		order = order[:maxCommits]
	}

	// Skip already-ingested commits.
	if alreadyIngested > 0 && alreadyIngested < len(order) {
		order = order[alreadyIngested:]
	} else if alreadyIngested >= len(order) {
		fmt.Printf("all %d commits already ingested, nothing to do\n", alreadyIngested)
		return nil
	}

	for i, hash := range order {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		topoOrder := alreadyIngested + i + 1
		start := timeutil.Now()
		if err := insertOneCommit(
			ctx, db, repoPath, hash, topoOrder, dag[hash], maxBlobSize, skipContent,
		); err != nil {
			return errors.Wrapf(err, "inserting commit %s (topo_order=%d)", hash, topoOrder)
		}
		if hists != nil {
			hists.Get("ingest-commit").Record(timeutil.Since(start))
		}
		if topoOrder == 1 || topoOrder%10 == 0 {
			fmt.Printf("ingested %d/%d commits\n", topoOrder, alreadyIngested+len(order))
		}
	}

	fmt.Printf("ingestion complete: %d commits\n", alreadyIngested+len(order))
	return nil
}

// insertOneCommit fetches metadata and diffs from git, then inserts everything
// into the database within a single retry-safe transaction. All git I/O happens
// before the transaction to minimize its duration.
func insertOneCommit(
	ctx context.Context,
	db *gosql.DB,
	repoPath string,
	hash string,
	topoOrder int,
	parents []string,
	maxBlobSize int,
	skipContent bool,
) error {
	// Fetch metadata outside the transaction.
	meta, err := getCommitMeta(ctx, repoPath, hash)
	if err != nil {
		return errors.Wrap(err, "getting commit metadata")
	}

	// Fetch diffs outside the transaction.
	diffs, err := getDiffTree(ctx, repoPath, hash, parents)
	if err != nil {
		return errors.Wrap(err, "getting diff tree")
	}

	// Pre-read all blob content outside the transaction.
	type blobData struct {
		sha256Hex string
		content   []byte
		size      int
	}
	blobsByPath := make(map[string]blobData)
	for _, d := range diffs {
		if d.op == 'D' {
			continue
		}
		path := d.path
		if _, ok := blobsByPath[path]; ok {
			continue
		}
		content, err := readBlob(ctx, repoPath, hash, path)
		if err != nil {
			return errors.Wrapf(err, "reading blob %s:%s", hash, path)
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
		blobsByPath[path] = bd
	}

	return crdb.ExecuteTx(ctx, db, nil /* txopts */, func(tx *gosql.Tx) error {
		// Insert blobs.
		var newBlobBytes int64
		for _, bd := range blobsByPath {
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
				newBlobBytes += int64(bd.size)
			}
		}

		// Insert commit metadata.
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO commits (hash, tree_hash, author_name, author_email,
			 author_date, committer_name, committer_email, committer_date,
			 message, topo_order)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			meta.hash, meta.treeHash, meta.authorName, meta.authorEmail,
			meta.authorDate, meta.committerName, meta.committerEmail,
			meta.committerDate, meta.message, topoOrder,
		); err != nil {
			return errors.Wrap(err, "inserting commit")
		}

		// Insert commit_parents edges.
		for i, parent := range parents {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO commit_parents (commit_hash, parent_hash, parent_index)
				 VALUES ($1, $2, $3)`,
				hash, parent, i,
			); err != nil {
				return errors.Wrap(err, "inserting commit parent")
			}
		}

		// Sort diffs so deletions are processed before renames and
		// additions. This prevents a rename's old_path deletion from
		// removing a path that was just upserted by another diff in
		// the same commit.
		slices.SortStableFunc(diffs, func(a, b diffEntry) int {
			return diffOpPriority(a.op) - diffOpPriority(b.op)
		})

		// Insert commit_diffs and update file_latest.
		for _, d := range diffs {
			var blobSHA *string
			if d.op != 'D' {
				if bd, ok := blobsByPath[d.path]; ok {
					blobSHA = &bd.sha256Hex
				}
			}

			if _, err := tx.ExecContext(ctx,
				`INSERT INTO commit_diffs (commit_hash, file_path, op, old_path,
				 file_mode, blob_sha256)
				 VALUES ($1, $2, $3, $4, $5, $6)`,
				hash, d.path, string(d.op), nilIfEmpty(d.oldPath),
				d.mode, blobSHA,
			); err != nil {
				return errors.Wrap(err, "inserting commit diff")
			}

			// Update file_latest based on operation type.
			switch d.op {
			case 'A', 'M':
				if _, err := tx.ExecContext(ctx,
					`UPSERT INTO file_latest (file_path, last_commit_hash,
					 last_topo_order, blob_sha256)
					 VALUES ($1, $2, $3, $4)`,
					d.path, hash, topoOrder, blobSHA,
				); err != nil {
					return errors.Wrap(err, "upserting file_latest")
				}
			case 'D':
				if _, err := tx.ExecContext(ctx,
					`DELETE FROM file_latest WHERE file_path = $1`,
					d.path,
				); err != nil {
					return errors.Wrap(err, "deleting file_latest")
				}
			case 'R':
				// Rename: delete old path, upsert new path.
				if _, err := tx.ExecContext(ctx,
					`DELETE FROM file_latest WHERE file_path = $1`,
					d.oldPath,
				); err != nil {
					return errors.Wrap(err, "deleting old file_latest for rename")
				}
				if _, err := tx.ExecContext(ctx,
					`UPSERT INTO file_latest (file_path, last_commit_hash,
					 last_topo_order, blob_sha256)
					 VALUES ($1, $2, $3, $4)`,
					d.path, hash, topoOrder, blobSHA,
				); err != nil {
					return errors.Wrap(err, "upserting new file_latest for rename")
				}
			}
		}

		// Update repo_stats.
		if _, err := tx.ExecContext(ctx,
			`UPDATE repo_stats SET
			 total_commits = total_commits + 1,
			 total_files_changed = total_files_changed + $1,
			 total_blobs_bytes = total_blobs_bytes + $2
			 WHERE id = 1`,
			len(diffs), newBlobBytes,
		); err != nil {
			return errors.Wrap(err, "updating repo_stats")
		}

		return nil
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
	default: // A, M
		return 2
	}
}

// clearAllTables truncates all gitload tables and re-initializes
// repo_stats. TRUNCATE CASCADE is used for efficiency; by the time the
// Ops worker runs, PostLoad has already created FK constraints.
func clearAllTables(ctx context.Context, db *gosql.DB) error {
	if _, err := db.ExecContext(ctx,
		`TRUNCATE TABLE commit_diffs, commit_parents, file_latest,
		 commits, blobs, repo_stats CASCADE`,
	); err != nil {
		return errors.Wrap(err, "truncating tables")
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO repo_stats (id, total_commits, total_files_changed, total_blobs_bytes)
		 VALUES (1, 0, 0, 0)`,
	); err != nil {
		return errors.Wrap(err, "initializing repo_stats")
	}
	return nil
}
