// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package gitload implements a workload that ingests a git repository's commit
// history into CockroachDB and verifies data integrity using git as an oracle.
// Any discrepancy between database state and git ground truth indicates a
// CockroachDB bug.
//
// The workload continuously cycles through: clear all tables, ingest the
// repo with a new random topological ordering, and repeat. Different seeds
// produce different lock orderings, FK reference patterns, and contention
// profiles.
//
// Verification is available via `workload check gitload` which runs three
// oracle levels: repo_stats consistency, file_latest path set comparison
// against git, and full diff chain replay.
package gitload

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

const (
	blobsSchema = `(
		sha256 STRING PRIMARY KEY,
		size INT NOT NULL,
		content BYTES
	)`

	commitsSchema = `(
		hash STRING PRIMARY KEY,
		tree_hash STRING NOT NULL,
		author_name STRING NOT NULL,
		author_email STRING NOT NULL,
		author_date STRING NOT NULL,
		committer_name STRING NOT NULL,
		committer_email STRING NOT NULL,
		committer_date STRING NOT NULL,
		message STRING NOT NULL,
		topo_order INT NOT NULL
	)`

	// commit_parents has no inline FK; constraints are added in PostLoad.
	commitParentsSchema = `(
		commit_hash STRING NOT NULL,
		parent_hash STRING NOT NULL,
		parent_index INT NOT NULL,
		PRIMARY KEY (commit_hash, parent_index)
	)`

	// commit_diffs has no inline FK; constraints are added in PostLoad.
	commitDiffsSchema = `(
		commit_hash STRING NOT NULL,
		file_path STRING NOT NULL,
		op STRING NOT NULL,
		old_path STRING,
		file_mode STRING,
		blob_sha256 STRING,
		PRIMARY KEY (commit_hash, file_path)
	)`

	fileLatestSchema = `(
		file_path STRING PRIMARY KEY,
		last_commit_hash STRING NOT NULL,
		last_topo_order INT NOT NULL,
		blob_sha256 STRING
	)`

	repoStatsSchema = `(
		id INT PRIMARY KEY,
		total_commits INT NOT NULL DEFAULT 0,
		total_files_changed INT NOT NULL DEFAULT 0,
		total_blobs_bytes INT NOT NULL DEFAULT 0
	)`

	defaultCommits         = 1000
	defaultSeed            = 42
	defaultMaxBlobSize     = 65536
	defaultVerifyEvery     = 100
	defaultInlineVerifySec = 0
	defaultRepoPath        = "/tmp/gitload-repo"
)

// fkConstraints defines the FK constraints added during PostLoad. These are
// deferred from the schema to avoid validation overhead during bulk ingestion
// (same pattern as TPCC). Uses IF NOT EXISTS so re-running init is safe.
var fkConstraints = []string{
	`ALTER TABLE commit_parents ADD CONSTRAINT IF NOT EXISTS fk_commit_parents_commit
	 FOREIGN KEY (commit_hash) REFERENCES commits (hash)`,
	`ALTER TABLE commit_diffs ADD CONSTRAINT IF NOT EXISTS fk_commit_diffs_commit
	 FOREIGN KEY (commit_hash) REFERENCES commits (hash)`,
	`ALTER TABLE commit_diffs ADD CONSTRAINT IF NOT EXISTS fk_commit_diffs_blob
	 FOREIGN KEY (blob_sha256) REFERENCES blobs (sha256)`,
	`ALTER TABLE file_latest ADD CONSTRAINT IF NOT EXISTS fk_file_latest_commit
	 FOREIGN KEY (last_commit_hash) REFERENCES commits (hash)`,
	`ALTER TABLE file_latest ADD CONSTRAINT IF NOT EXISTS fk_file_latest_blob
	 FOREIGN KEY (blob_sha256) REFERENCES blobs (sha256)`,
}

// secondaryIndexes defines the secondary indexes added during PostLoad for
// index consistency verification.
var secondaryIndexes = []string{
	`CREATE INDEX IF NOT EXISTS idx_commit_diffs_file_path
	 ON commit_diffs(file_path)`,
	`CREATE INDEX IF NOT EXISTS idx_commits_topo_order
	 ON commits(topo_order)`,
	`CREATE INDEX IF NOT EXISTS idx_file_latest_last_commit
	 ON file_latest(last_commit_hash)`,
}

type gitload struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	repoPath        string
	seed            int64
	numCommits      int
	maxBlobSize     int
	skipContent     bool
	verifyEveryN    int
	inlineVerifySec int
}

var gitloadMeta = workload.Meta{
	Name:        "gitload",
	Description: "Ingests git commit history and verifies data integrity using git as an oracle",
	Version:     "1.0.0",
	New: func() workload.Generator {
		g := &gitload{}
		g.flags.FlagSet = pflag.NewFlagSet("gitload", pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			"repo":          {RuntimeOnly: true},
			"seed":          {RuntimeOnly: true},
			"commits":       {RuntimeOnly: true},
			"max-blob-size": {RuntimeOnly: true},
			"skip-content":  {RuntimeOnly: true},
			"verify-every":  {RuntimeOnly: true},
			"inline-verify": {RuntimeOnly: true},
		}
		g.flags.StringVar(&g.repoPath, "repo", defaultRepoPath,
			"Path to the git repository (created during init)")
		g.flags.Int64Var(&g.seed, "seed", defaultSeed,
			"Base seed for deterministic repo generation and topo sort")
		g.flags.IntVar(&g.numCommits, "commits", defaultCommits,
			"Number of commits in the synthetic git repo")
		g.flags.IntVar(&g.maxBlobSize, "max-blob-size", defaultMaxBlobSize,
			"Maximum blob content size in bytes (0 = unlimited)")
		g.flags.BoolVar(&g.skipContent, "skip-content", false,
			"Store only blob hashes, not content")
		g.flags.IntVar(&g.verifyEveryN, "verify-every", defaultVerifyEvery,
			"Verify tree at every Nth commit during replay (0 = final only)")
		g.flags.IntVar(&g.inlineVerifySec, "inline-verify", defaultInlineVerifySec,
			"Run verification concurrently with ingestion every N seconds (0 = disabled)")
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

func init() {
	workload.Register(gitloadMeta)
}

// Meta implements the Generator interface.
func (*gitload) Meta() workload.Meta {
	return gitloadMeta
}

// Flags implements the Flagser interface.
func (g *gitload) Flags() workload.Flags {
	return g.flags
}

// ConnFlags implements the ConnFlagser interface.
func (g *gitload) ConnFlags() *workload.ConnFlags {
	return g.connFlags
}

// Tables implements the Generator interface. Returns schema-only tables with
// empty InitialRows. FK constraints are added in PostLoad.
func (g *gitload) Tables() []workload.Table {
	return []workload.Table{
		{Name: "blobs", Schema: blobsSchema},
		{Name: "commits", Schema: commitsSchema},
		{Name: "commit_parents", Schema: commitParentsSchema},
		{Name: "commit_diffs", Schema: commitDiffsSchema},
		{Name: "file_latest", Schema: fileLatestSchema},
		{Name: "repo_stats", Schema: repoStatsSchema},
	}
}

// Hooks implements the Hookser interface.
func (g *gitload) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if _, err := exec.LookPath("git"); err != nil {
				return errors.New("git is not in PATH")
			}
			if g.numCommits < 1 {
				return errors.New("--commits must be >= 1")
			}
			if g.maxBlobSize < 0 {
				return errors.New("--max-blob-size must be >= 0")
			}
			return nil
		},
		PostLoad: func(ctx context.Context, db *gosql.DB) error {
			// Generate the synthetic git repo.
			if err := generateRepo(ctx, g.repoPath, g.numCommits, g.seed); err != nil {
				return errors.Wrap(err, "generating synthetic repo")
			}

			for _, fk := range fkConstraints {
				if _, err := db.ExecContext(ctx, fk); err != nil {
					return errors.Wrap(err, "adding FK constraint")
				}
			}

			for _, idx := range secondaryIndexes {
				if _, err := db.ExecContext(ctx, idx); err != nil {
					return errors.Wrap(err, "adding secondary index")
				}
			}

			// Initialize the repo_stats singleton row (idempotent).
			if _, err := db.ExecContext(ctx,
				`INSERT INTO repo_stats (id, total_commits, total_files_changed, total_blobs_bytes)
				 VALUES (1, 0, 0, 0)
				 ON CONFLICT (id) DO NOTHING`,
			); err != nil {
				return errors.Wrap(err, "initializing repo_stats")
			}

			return nil
		},
		CheckConsistency: func(ctx context.Context, db *gosql.DB) error {
			return runAllChecks(ctx, db, g.repoPath, g.verifyEveryN)
		},
	}
}

// Ops implements the Opser interface. It returns a QueryLoad with a single
// ingestion worker that continuously cycles through: clear -> ingest with new
// seed -> repeat. Each cycle uses a different seed, producing different
// topological orderings that exercise different lock orderings and contention
// profiles. When --inline-verify is set, a second worker runs verification
// concurrently with ingestion, reading consistent snapshots via AS OF SYSTEM
// TIME follower_read_timestamp().
func (g *gitload) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	db, err := gosql.Open("cockroach", strings.Join(urls, " "))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	maxConns := 2
	if g.inlineVerifySec > 0 {
		maxConns = 4 // extra headroom for the verification worker
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)

	var cycle atomic.Int64

	ql := workload.QueryLoad{
		Close: func(_ context.Context) error {
			return db.Close()
		},
	}

	hists := reg.GetHandle()
	workerFn := func(ctx context.Context) error {
		c := cycle.Add(1)
		currentSeed := g.seed + c

		fmt.Printf("starting ingestion cycle %d with seed %d\n", c, currentSeed)

		// Clear all tables.
		if err := clearAllTables(ctx, db); err != nil {
			return errors.Wrapf(err, "clearing tables for cycle %d", c)
		}

		// Ingest with the current seed. Per-commit latencies are recorded
		// by ingest() via the histogram handle.
		start := timeutil.Now()
		if err := ingest(
			ctx, db, g.repoPath, currentSeed,
			g.numCommits, g.maxBlobSize, g.skipContent, hists,
		); err != nil {
			return errors.Wrapf(err, "ingestion cycle %d", c)
		}

		fmt.Printf("completed ingestion cycle %d in %s\n", c, timeutil.Since(start))
		return nil
	}
	ql.WorkerFns = append(ql.WorkerFns, workerFn)

	// Add a background verification worker that periodically checks
	// consistency while ingestion is in progress.
	if g.inlineVerifySec > 0 {
		interval := time.Duration(g.inlineVerifySec) * time.Second
		repoPath := g.repoPath
		verifyEveryN := g.verifyEveryN
		verifierFn := func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(interval):
			}

			start := timeutil.Now()
			if err := runInlineChecks(ctx, db, repoPath, verifyEveryN); err != nil {
				return errors.Wrap(err, "inline verification")
			}
			elapsed := timeutil.Since(start)
			hists.Get("inline-verify").Record(elapsed)
			return nil
		}
		ql.WorkerFns = append(ql.WorkerFns, verifierFn)
	}

	return ql, nil
}
