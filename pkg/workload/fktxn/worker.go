// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"context"
	gosql "database/sql"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/errors"
)

// WorkerConfig is the static configuration a worker receives from the
// orchestrator at startup. The same config is shared by every worker on the
// same shard / sub-DAG group; per-worker variation comes from the worker's
// RNG, which decides PK assignments and op selection.
type WorkerConfig struct {
	// DB is the connection the worker uses for all transactions. Each worker
	// owns its own *sql.DB (or a pool with at least one dedicated conn) so
	// per-worker transaction state is independent.
	DB *gosql.DB

	// Sorted, Sub, Dropped describe the sub-DAG the worker drives. All
	// workers receive the same sub-DAG. Cross-worker collision behavior
	// depends on Pools: nil means each chain samples fresh from each column's
	// full type domain (collisions are incidental); non-nil means sampling
	// from shared per-constraint pools (collisions are deliberate).
	Sorted  []*Table
	Sub     *FKGraph
	Dropped []FKEdge

	// Pools, when non-nil, holds per-constraint value pools shared by every
	// worker for the lifetime of the run. AssignPKs picks a pkIdx into
	// Pools.PKs[T]; FK and UC values come from Pools at row-build time. See
	// pks.go for how pkIdx interacts with FK propagation and chain identity.
	Pools *Pools

	// Mix controls the relative frequency of Upsert/Update/Delete events
	// when the chain is in the Exists state. Gone always advances via Upsert.
	Mix OpMix

	// MinChainLen and MaxChainLen bound the number of events stacked into
	// each chain transaction. A chain runs MinChainLen +
	// rng.Intn(MaxChainLen-MinChainLen+1) events against a single PK
	// assignment, all inside one BEGIN/COMMIT, so the source's lock manager
	// sees many ops on the same row and exercises lock synthesis.
	MinChainLen, MaxChainLen int

	// TolerateSrcErrors controls whether source-side errors (FK violations,
	// serialization errors) are logged and skipped or returned as fatal.
	// True for the workload's normal operation; false for unit tests that
	// want to assert no errors occur.
	TolerateSrcErrors bool
}

// Worker drives one stream of chained transactions against a sub-DAG. It is
// not safe to share across goroutines; create one Worker per goroutine.
type Worker struct {
	cfg WorkerConfig
	rng *rand.Rand
}

// NewWorker constructs a worker with its own RNG. Pass independent RNGs
// across workers so they make independent op-mix and PK-sampling choices.
func NewWorker(cfg WorkerConfig, rng *rand.Rand) *Worker {
	return &Worker{cfg: cfg, rng: rng}
}

// ChainResult summarizes one Run: how many events the chain attempted,
// how many committed (i.e. survived to the chain's COMMIT), and any
// source error that fired during the chain. The committed/attempted ratio
// is the per-event success rate under contention; the FailedEvent +
// FailErr fields let the caller categorize the first failure observed.
type ChainResult struct {
	// Attempted counts events the chain tried (one per FSM step).
	Attempted int
	// Committed counts events that survived to the chain's COMMIT. Zero when
	// the chain rolled back, otherwise equal to the number of events that
	// ran without error before the chain committed.
	Committed int
	// FailedEvent is the FSM event whose action returned the source error
	// that ended the chain. Nil if the chain ran to completion. Useful for
	// breaking down contention failures by op type.
	FailedEvent fsm.Event
	// FailErr is the underlying error returned by the failed action. Nil
	// when the chain ran to completion. Used for classifying the contention
	// type (FK violation vs. serialization vs. unique violation, etc.).
	FailErr error
}

// FailureClass returns a coarse categorization of FailErr suitable for
// reporting and aggregation. Returns "" if the chain succeeded.
func (r ChainResult) FailureClass() string {
	if r.FailErr == nil {
		return ""
	}
	return classifyError(r.FailErr)
}

// Run executes one chain: pick a fresh PK assignment, open one transaction,
// drive the FSM through MinChainLen..MaxChainLen events against that single
// transaction, then commit. Stacking many events in one txn is the design
// goal — same-txn lock synthesis only fires when the source's lock manager
// sees multiple ops on the same row inside one BEGIN/COMMIT.
//
// The chain ends at the first event that returns an error. Without
// savepoints, the txn enters a failed state on any error and cannot accept
// further statements; the chain is rolled back and any earlier events are
// discarded with it (CRDB is all-or-nothing per txn). TolerateSrcErrors
// controls whether the worker function returns the error to the framework
// (failing the run) or swallows it (the next chain proceeds).
func (w *Worker) Run(ctx context.Context) (ChainResult, error) {
	pkIdx := AssignPKs(w.rng, w.cfg.Pools)

	tx, err := w.cfg.DB.BeginTx(ctx, nil)
	if err != nil {
		return ChainResult{}, errors.Wrap(err, "begin chain")
	}
	// chainExtended holds the per-event scratch state (current tx, fresh-FK
	// flag, last-event row count). The FSM keeps a stable pointer to it for
	// the lifetime of the chain.
	ext := &chainExtended{
		tx:      tx,
		rng:     w.rng,
		sorted:  w.cfg.Sorted,
		sub:     w.cfg.Sub,
		dropped: w.cfg.Dropped,
		pools:   w.cfg.Pools,
		pkIdx:   pkIdx,
	}
	machine := fsm.MakeMachine(chainTransitions, chainStateUnknown{}, ext)

	var res ChainResult
	var succeeded int
	chainLen := w.pickChainLen()
	for i := 0; i < chainLen; i++ {
		state := machine.CurState()
		event := w.cfg.Mix.pickEvent(w.rng, state)
		res.Attempted++
		// Roll fresh parent indexes for non-PK FK columns only when we're
		// re-writing an existing chain (Exists → Exists). On the first
		// UPSERT (Unknown/None → Exists), the chain's parent rows are being
		// written in the same walk and only exist at pkIdx, so a fresh
		// parentIdx would dangle.
		_, fromExists := state.(chainStateExists)
		ext.repointFKs = fromExists
		if err := machine.Apply(ctx, event); err != nil {
			// Any error poisons the chain's tx — without savepoints there
			// is no way to recover and continue. Roll back and end the
			// chain. If the error is a tolerated source error, the worker
			// function returns nil so the workload framework loops to the
			// next chain.
			_ = tx.Rollback() //nolint:returnerrcheck
			res.FailedEvent = event
			res.FailErr = err
			if w.cfg.TolerateSrcErrors && isSourceError(err) {
				return res, nil
			}
			return res, err
		}
		succeeded++
	}

	if succeeded == 0 {
		// Nothing to commit. Rollback avoids leaving a no-op txn for LDR to
		// replicate.
		_ = tx.Rollback() //nolint:returnerrcheck
		return res, nil
	}
	if err := tx.Commit(); err != nil {
		// Commit failure can surface contention that wasn't visible inside
		// the chain (e.g. serialization rollback at commit time). Treat it
		// the same as an action error.
		if w.cfg.TolerateSrcErrors && isSourceError(err) {
			if res.FailErr == nil {
				res.FailErr = err
			}
			return res, nil
		}
		return res, err
	}
	res.Committed = succeeded
	return res, nil
}

func (w *Worker) pickChainLen() int {
	if w.cfg.MaxChainLen <= w.cfg.MinChainLen {
		return w.cfg.MinChainLen
	}
	return w.cfg.MinChainLen + w.rng.Intn(w.cfg.MaxChainLen-w.cfg.MinChainLen+1)
}

// sourceErrorClass maps a coarse classification name to substring patterns
// that identify the error in the wire message. We match strings (rather than
// pgcode) so the workload runs uniformly against any Postgres-compatible
// target. Order matters only for reporting consistency — the classifier
// returns the first matching class.
var sourceErrorClass = []struct {
	class    string
	patterns []string
}{
	{"fk_violation", []string{
		"foreign key violation",
		"violates foreign key constraint",
	}},
	{"serialization", []string{
		"restart transaction",
		"RETRY_SERIALIZABLE",
		"RETRY_WRITE_TOO_OLD",
	}},
	{"unique_violation", []string{
		"duplicate key value",
		"violates unique constraint",
	}},
	// SQLSTATE 22003 covers any value the source can't represent: integer
	// overflow (including expression-index sums of two near-max ints),
	// out-of-range time/timestamp/interval, decimal precision/scale. The
	// workload generates datums at the extremes of each type's domain via
	// randgen.RandDatum, so these are tolerable source-side rejections.
	{"out_of_range", []string{
		"out of range",
	}},
}

// isSourceError reports whether err is a tolerable source-side failure
// produced by concurrent contention. Connection-level and assertion errors
// fall through and are returned to the caller.
func isSourceError(err error) bool {
	return classifyError(err) != ""
}

// classifyError returns the source-error class name (e.g. "fk_violation")
// or "" if err is not a known contention error. Used by ChainResult to
// surface what kind of contention each failed chain hit.
func classifyError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	for _, c := range sourceErrorClass {
		for _, p := range c.patterns {
			if strings.Contains(msg, p) {
				return c.class
			}
		}
	}
	return ""
}
