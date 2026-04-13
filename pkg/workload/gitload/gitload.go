// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package gitload implements a workload that ingests git commit history into
// CockroachDB and verifies data integrity using git as an oracle. Any
// discrepancy between database state and git ground truth indicates a
// CockroachDB bug.
//
// Multiple concurrent agents (controlled by --concurrency) each ingest their
// own synthetic git repo into shared tables, creating contention on blob dedup,
// FK validation, and secondary index maintenance. Each agent continuously
// cycles through: clear its data, ingest with a new topological ordering,
// and repeat.
package gitload

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

const (
	blobsSchema = `(
		sha256 STRING PRIMARY KEY,
		size INT NOT NULL,
		content BYTES
	)`

	commitsSchema = `(
		agent_id STRING NOT NULL,
		hash STRING NOT NULL,
		tree_hash STRING NOT NULL,
		author_name STRING NOT NULL,
		author_email STRING NOT NULL,
		author_date STRING NOT NULL,
		committer_name STRING NOT NULL,
		committer_email STRING NOT NULL,
		committer_date STRING NOT NULL,
		message STRING NOT NULL,
		topo_order INT NOT NULL,
		PRIMARY KEY (agent_id, hash)
	)`

	commitParentsSchema = `(
		agent_id STRING NOT NULL,
		commit_hash STRING NOT NULL,
		parent_hash STRING NOT NULL,
		parent_index INT NOT NULL,
		PRIMARY KEY (agent_id, commit_hash, parent_index)
	)`

	commitDiffsSchema = `(
		agent_id STRING NOT NULL,
		commit_hash STRING NOT NULL,
		file_path STRING NOT NULL,
		op STRING NOT NULL,
		old_path STRING,
		file_mode STRING,
		blob_sha256 STRING,
		PRIMARY KEY (agent_id, commit_hash, file_path)
	)`

	fileLatestSchema = `(
		agent_id STRING NOT NULL,
		file_path STRING NOT NULL,
		last_commit_hash STRING NOT NULL,
		last_topo_order INT NOT NULL,
		blob_sha256 STRING,
		PRIMARY KEY (agent_id, file_path)
	)`

	repoStatsSchema = `(
		agent_id STRING PRIMARY KEY,
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
	defaultConcurrency     = 4
	defaultBatchSize       = 50
)

func makeAgentID(i int) string {
	return fmt.Sprintf("agent-%d", i)
}

func agentRepoPath(basePath, agentID string) string {
	return filepath.Join(basePath, agentID)
}

// fkConstraints defines the FK constraints added during PostLoad. These are
// deferred from the schema to avoid validation overhead during bulk ingestion
// (same pattern as TPCC). Uses IF NOT EXISTS so re-running init is safe.
var fkConstraints = []string{
	`ALTER TABLE commit_parents ADD CONSTRAINT IF NOT EXISTS fk_commit_parents_commit
	 FOREIGN KEY (agent_id, commit_hash) REFERENCES commits (agent_id, hash)`,
	`ALTER TABLE commit_diffs ADD CONSTRAINT IF NOT EXISTS fk_commit_diffs_commit
	 FOREIGN KEY (agent_id, commit_hash) REFERENCES commits (agent_id, hash)`,
	`ALTER TABLE commit_diffs ADD CONSTRAINT IF NOT EXISTS fk_commit_diffs_blob
	 FOREIGN KEY (blob_sha256) REFERENCES blobs (sha256)`,
	`ALTER TABLE file_latest ADD CONSTRAINT IF NOT EXISTS fk_file_latest_commit
	 FOREIGN KEY (agent_id, last_commit_hash) REFERENCES commits (agent_id, hash)`,
	`ALTER TABLE file_latest ADD CONSTRAINT IF NOT EXISTS fk_file_latest_blob
	 FOREIGN KEY (blob_sha256) REFERENCES blobs (sha256)`,
}

var secondaryIndexes = []string{
	`CREATE INDEX IF NOT EXISTS idx_commit_diffs_file_path
	 ON commit_diffs(agent_id, file_path)`,
	`CREATE INDEX IF NOT EXISTS idx_commits_topo_order
	 ON commits(agent_id, topo_order)`,
	`CREATE INDEX IF NOT EXISTS idx_file_latest_last_commit
	 ON file_latest(agent_id, last_commit_hash)`,
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
	batchSize       int
	clean           bool
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
			"batch-size":    {RuntimeOnly: true},
			"clean":         {RuntimeOnly: true},
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
		g.flags.IntVar(&g.batchSize, "batch-size", defaultBatchSize,
			"Number of commits per transaction during ingestion")
		g.flags.BoolVar(&g.clean, "clean", false,
			"Remove existing git repos before generating new ones (use with --drop for a full reset)")
		g.connFlags = workload.NewConnFlags(&g.flags)
		if f := g.connFlags.Lookup("concurrency"); f != nil {
			f.DefValue = fmt.Sprintf("%d", defaultConcurrency)
			g.connFlags.Concurrency = defaultConcurrency
		}
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
			numAgents := g.connFlags.Concurrency
			db.SetMaxOpenConns(numAgents + 2)
			db.SetMaxIdleConns(numAgents + 2)

			if g.clean {
				fmt.Printf("--clean: removing existing git repos at %s\n", g.repoPath)
				if err := os.RemoveAll(g.repoPath); err != nil {
					return errors.Wrap(err, "cleaning repo path")
				}
			}

			for i := 0; i < numAgents; i++ {
				aid := makeAgentID(i)
				if _, err := db.ExecContext(ctx,
					`INSERT INTO repo_stats (agent_id, total_commits, total_files_changed, total_blobs_bytes)
					 VALUES ($1, 0, 0, 0)
					 ON CONFLICT (agent_id) DO NOTHING`, aid,
				); err != nil {
					return errors.Wrapf(err, "initializing repo_stats for %s", aid)
				}
			}

			eg, egCtx := errgroup.WithContext(ctx)
			for i := 0; i < numAgents; i++ {
				aid := makeAgentID(i)
				repoPath := agentRepoPath(g.repoPath, aid)
				agentSeed := g.seed + int64(i)

				eg.Go(func() error {
					if err := generateRepo(egCtx, repoPath, g.numCommits, agentSeed); err != nil {
						return errors.Wrapf(err, "generating repo for %s", aid)
					}
					return ingest(
						egCtx, db, repoPath, agentSeed,
						g.numCommits, g.maxBlobSize, g.skipContent, nil, aid,
						g.batchSize,
					)
				})
			}
			if err := eg.Wait(); err != nil {
				return err
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

			return nil
		},
		CheckConsistency: func(ctx context.Context, db *gosql.DB) error {
			return runAllChecks(ctx, db, g.repoPath, g.verifyEveryN)
		},
	}
}

// Ops implements the Opser interface. It returns a QueryLoad with one
// ingestion worker per agent (controlled by --concurrency). Each agent
// continuously cycles through: clear its data, ingest with a new seed,
// and repeat. When --inline-verify is set, an additional worker runs
// verification concurrently.
func (g *gitload) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	db, err := gosql.Open("cockroach", strings.Join(urls, " "))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	numAgents := g.connFlags.Concurrency
	maxConns := numAgents + 2
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)

	for i := 0; i < numAgents; i++ {
		aid := makeAgentID(i)
		repoPath := agentRepoPath(g.repoPath, aid)
		agentSeed := g.seed + int64(i)

		if err := generateRepo(ctx, repoPath, g.numCommits, agentSeed); err != nil {
			db.Close()
			return workload.QueryLoad{}, errors.Wrapf(err, "generating repo for %s", aid)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO repo_stats (agent_id, total_commits, total_files_changed, total_blobs_bytes)
			 VALUES ($1, 0, 0, 0)
			 ON CONFLICT (agent_id) DO NOTHING`, aid,
		); err != nil {
			db.Close()
			return workload.QueryLoad{}, errors.Wrapf(err, "initializing repo_stats for %s", aid)
		}
	}

	ql := workload.QueryLoad{
		Close: func(_ context.Context) error {
			return db.Close()
		},
	}

	for i := 0; i < numAgents; i++ {
		aid := makeAgentID(i)
		repoPath := agentRepoPath(g.repoPath, aid)
		agentSeed := g.seed + int64(i)
		hists := reg.GetHandle()
		var cycle atomic.Int64

		workerFn := func(ctx context.Context) error {
			c := cycle.Add(1)
			currentSeed := agentSeed + c*int64(numAgents)

			start := timeutil.Now()
			if err := clearAgentData(ctx, db, aid); err != nil {
				return errors.Wrapf(err, "%s: clearing data", aid)
			}
			if err := ingest(
				ctx, db, repoPath, currentSeed,
				g.numCommits, g.maxBlobSize, g.skipContent, hists, aid,
				g.batchSize,
			); err != nil {
				return errors.Wrapf(err, "%s: ingestion cycle %d", aid, c)
			}
			fmt.Printf("%s: cycle %d complete in %s\n",
				aid, c, timeutil.Since(start))
			return nil
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}

	if g.inlineVerifySec > 0 {
		interval := time.Duration(g.inlineVerifySec) * time.Second
		repoPath := g.repoPath
		verifyEveryN := g.verifyEveryN
		hists := reg.GetHandle()
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
			hists.Get("inline-verify").Record(timeutil.Since(start))
			return nil
		}
		ql.WorkerFns = append(ql.WorkerFns, verifierFn)
	}

	return ql, nil
}
