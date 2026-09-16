// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	mathrandv2 "math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
)

// orchestratorConfig is the static run-time configuration assembled by Ops()
// and handed to the orchestrator. It owns the connection details, worker
// shape, and op mix.
type orchestratorConfig struct {
	URLs              []string
	Workers           int
	MinChainLen       int
	MaxChainLen       int
	OpsPerRotation    int
	Mix               OpMix
	TolerateSrcErrors bool
	Seed              int64
	// RowPoolSize, when > 0, makes the workload sample PK, UC, and FK column
	// values from per-constraint pools of RowPoolSize pre-generated tuples
	// instead of from each column's full type domain. Smaller values raise
	// cross-worker collision rates (PK, UC, and FK) and the source-side
	// serialization that comes with them. 0 disables pooling.
	RowPoolSize int
}

// orchestrator owns the shared sub-DAG and PK pool that all workers operate
// on. It also owns the rotation state: workers consult the orchestrator
// before each chain to learn the current sub-DAG generation, and a swap
// happens out-of-band when the chain count crosses the rotation threshold.
//
// Lifecycle:
//   - newOrchestrator opens one *sql.DB per worker (each round-robined onto
//     a different URL), discovers the schema, builds the initial sub-DAG/
//     pool, and returns one WorkerFn per worker.
//   - Each WorkerFn runs one chain per call (the workload framework loops).
//   - On chain rotation, the next worker to enter the rotation block swaps
//     the shared state under a write lock; concurrent workers wait via the
//     rwmutex so they pick up the new state on the very next chain.
//   - Close() shuts down every per-worker *sql.DB.
type orchestrator struct {
	cfg    orchestratorConfig
	dbName string
	hists  *histogram.Histograms

	// graphs is the full set of FK connected components discovered from the
	// live schema. Sub-DAG rotation re-picks one of these.
	graphs []*FKGraph

	// pools, when non-nil, are the run-stable per-constraint value pools
	// every worker shares for the lifetime of the run. Built once at startup
	// over the whole schema (not per rotation); rotation only re-picks the
	// sub-DAG, not the keyspace. Nil when RowPoolSize=0.
	pools *Pools

	// chainsCompleted counts every chain across all workers; the rotation
	// threshold compares against this counter.
	chainsCompleted atomic.Uint64

	// stateMu guards both state and orchRNG. State is read under a read lock
	// (workers do this once per chain via snapshotState); rotation upgrades
	// to the write lock to swap state and to advance orchRNG.
	stateMu sync.RWMutex
	state   *sharedState
	// orchRNG drives sub-DAG re-rolls. It is only touched under stateMu's
	// write lock — never by workers directly.
	orchRNG *mathrandv2.Rand
}

// sharedState is the per-rotation snapshot every worker drives against. The
// orchestrator builds one in newOrchestrator and replaces it on rotation.
// Pools live on the orchestrator (run-stable), not on sharedState.
type sharedState struct {
	// generation distinguishes one rotation from the next. Workers don't use
	// it directly today, but recording it makes contention diagnostics
	// cheaper if we add per-rotation metrics later.
	generation uint64
	sorted     []*Table
	sub        *FKGraph
	dropped    []FKEdge
}

// newOrchestrator opens one *sql.DB per worker (each pinned to a different
// URL via round-robin), discovers the schema, builds the initial sub-DAG
// and pool, and produces a QueryLoad with one worker function per
// --workers. Each worker function runs exactly one chain per invocation;
// the workload framework drives the loop.
func newOrchestrator(
	ctx context.Context, cfg orchestratorConfig, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	if len(cfg.URLs) == 0 {
		return workload.QueryLoad{}, errors.New("no URLs provided")
	}

	closeAll := func(dbs []*gosql.DB) {
		for _, db := range dbs {
			_ = db.Close()
		}
	}

	// One *sql.DB per worker, round-robined across URLs. Spreading workers
	// across nodes matters: a single shared *sql.DB pins every worker to
	// whichever node it first connected to, which concentrates load on one
	// node and defeats the point of accepting multiple URLs.
	workerDBs := make([]*gosql.DB, cfg.Workers)
	for i := range workerDBs {
		url := cfg.URLs[i%len(cfg.URLs)]
		db, err := gosql.Open("cockroach", url)
		if err != nil {
			closeAll(workerDBs[:i])
			return workload.QueryLoad{}, errors.Wrapf(err, "opening worker %d connection", i)
		}
		workerDBs[i] = db
	}

	dbName, err := currentDatabase(ctx, workerDBs[0])
	if err != nil {
		closeAll(workerDBs)
		return workload.QueryLoad{}, errors.Wrap(err, "resolving current database")
	}
	schema, err := DiscoverSchema(workerDBs[0], dbName)
	if err != nil {
		closeAll(workerDBs)
		return workload.QueryLoad{}, errors.Wrap(err, "discovering schema")
	}
	graphs := BuildFKGraphs(schema)
	if len(graphs) == 0 {
		closeAll(workerDBs)
		return workload.QueryLoad{}, errors.Newf(
			"no FK graphs discovered in database %q; fktxn needs at least one FK constraint",
			dbName,
		)
	}

	o := &orchestrator{
		cfg:    cfg,
		dbName: dbName,
		hists:  reg.GetHandle(),
		graphs: graphs,
		// Seed the orchestrator's RNG separately from worker RNGs so re-
		// rolling the sub-DAG produces independent shapes from worker op
		// selection.
		orchRNG: mathrandv2.New(mathrandv2.NewPCG(uint64(cfg.Seed), 0)),
	}

	if cfg.RowPoolSize > 0 {
		// BuildPools wants a math/rand v1 RNG (matching randgen's API). Seed
		// it from orchRNG so pool generation is deterministic and independent
		// of worker RNGs. Pools are run-stable: built once here, never
		// re-rolled on rotation.
		poolRNG := rand.New(rand.NewSource(int64(o.orchRNG.Uint64())))
		pools, err := BuildPools(poolRNG, schema, cfg.RowPoolSize)
		if err != nil {
			closeAll(workerDBs)
			return workload.QueryLoad{}, errors.Wrap(err, "building per-constraint pools")
		}
		o.pools = pools
	}

	state, err := o.buildState(1)
	if err != nil {
		closeAll(workerDBs)
		return workload.QueryLoad{}, errors.Wrap(err, "building initial sub-DAG")
	}
	o.state = state
	logSubDAG(ctx, state)

	workerFns := make([]func(context.Context) error, cfg.Workers)
	for i := range workerFns {
		// Per-worker RNG: stable across the run (so the same seed reproduces
		// the same op sequence per worker), independent across workers.
		rng := rand.New(rand.NewSource(cfg.Seed + int64(i) + 1))
		workerFns[i] = o.makeWorkerFn(workerDBs[i], rng)
	}

	return workload.QueryLoad{
		WorkerFns: workerFns,
		Close: func(_ context.Context) error {
			closeAll(workerDBs)
			return nil
		},
	}, nil
}

// makeWorkerFn returns a function the workload framework calls once per
// chain. Each call snapshots the shared state, runs one chain against it,
// records latency and outcome, and (if the rotation threshold is crossed)
// swaps the shared state for the next chain.
func (o *orchestrator) makeWorkerFn(db *gosql.DB, rng *rand.Rand) func(context.Context) error {
	return func(ctx context.Context) error {
		state := o.snapshotState()
		w := NewWorker(WorkerConfig{
			DB:                db,
			Sorted:            state.sorted,
			Sub:               state.sub,
			Dropped:           state.dropped,
			Pools:             o.pools,
			Mix:               o.cfg.Mix,
			MinChainLen:       o.cfg.MinChainLen,
			MaxChainLen:       o.cfg.MaxChainLen,
			TolerateSrcErrors: o.cfg.TolerateSrcErrors,
		}, rng)

		start := timeutil.Now()
		res, err := w.Run(ctx)
		elapsed := timeutil.Since(start)
		// txn_attempt counts every chain the worker tried (one source txn
		// per chain). committed_txn counts the subset that reached COMMIT;
		// the failure-class histograms cover the rest.
		o.hists.Get(`txn_attempt`).Record(elapsed)
		if res.Committed > 0 {
			o.hists.Get(`committed_txn`).Record(elapsed)
		}
		if class := res.FailureClass(); class != "" {
			o.hists.Get(class).Record(elapsed)
		}
		if err != nil {
			return err
		}

		count := o.chainsCompleted.Add(1)
		o.maybeRotate(ctx, count)
		return nil
	}
}

// snapshotState returns a stable pointer to the current sharedState.
// Workers hold no reference to o.state directly so a swap during rotation
// doesn't tear in-flight chains.
func (o *orchestrator) snapshotState() *sharedState {
	o.stateMu.RLock()
	defer o.stateMu.RUnlock()
	return o.state
}

// maybeRotate replaces the sub-DAG and pool when the chain count crosses the
// rotation threshold. The first worker to observe a threshold crossing
// performs the swap; concurrent observers re-check under the write lock and
// no-op if the swap already happened.
func (o *orchestrator) maybeRotate(ctx context.Context, count uint64) {
	threshold := uint64(o.cfg.OpsPerRotation)
	if threshold == 0 {
		return
	}
	wantGen := count/threshold + 1
	o.stateMu.Lock()
	defer o.stateMu.Unlock()
	// Re-check under the write lock: another worker may have already rotated
	// past this generation while we waited for the lock.
	if o.state.generation >= wantGen {
		return
	}
	state, err := o.buildState(wantGen)
	if err != nil {
		log.Dev.Warningf(ctx, "fktxn: sub-DAG rotation failed: %v", err)
		return
	}
	o.state = state
	logSubDAG(ctx, state)
}

// logSubDAG emits one info line summarizing a sub-DAG selection. Used to
// diagnose post-rotation behavior (e.g. when a rotation collapses the sub-DAG
// down to a single table and the workload starts hitting unique violations
// that look like an FK ordering bug).
func logSubDAG(ctx context.Context, state *sharedState) {
	tables := make([]string, 0, len(state.sub.Tables))
	for name := range state.sub.Tables {
		tables = append(tables, name)
	}
	sort.Strings(tables)
	edges := make([]string, 0, len(state.sub.Edges))
	for _, e := range state.sub.Edges {
		edges = append(edges, fmt.Sprintf("%s->%s", e.ReferencingTable, e.ReferencedTable))
	}
	sort.Strings(edges)
	dropped := make([]string, 0, len(state.dropped))
	for _, e := range state.dropped {
		dropped = append(dropped, fmt.Sprintf("%s->%s", e.ReferencingTable, e.ReferencedTable))
	}
	sort.Strings(dropped)
	log.Dev.Infof(ctx,
		"fktxn: sub-DAG generation=%d tables=[%s] edges=[%s] dropped=[%s]",
		state.generation,
		strings.Join(tables, ","),
		strings.Join(edges, ","),
		strings.Join(dropped, ","),
	)
}

// buildState picks a random FK graph and derives a random sub-DAG. Caller
// must hold stateMu's write lock — buildState reads from o.orchRNG. Pools
// live on the orchestrator and are built once at startup; rotation only
// affects the sub-DAG.
func (o *orchestrator) buildState(generation uint64) (*sharedState, error) {
	graph := o.graphs[o.orchRNG.IntN(len(o.graphs))]
	sorted, sub, dropped, err := RandomSubDAG(o.orchRNG, graph)
	if err != nil {
		return nil, errors.Wrap(err, "selecting sub-DAG")
	}
	return &sharedState{
		generation: generation,
		sorted:     sorted,
		sub:        sub,
		dropped:    dropped,
	}, nil
}

// currentDatabase returns the connection's current database name.
func currentDatabase(ctx context.Context, db *gosql.DB) (string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var name string
	if err := db.QueryRowContext(queryCtx, "SELECT current_database()").Scan(&name); err != nil {
		return "", err
	}
	if name == "" {
		return "", errors.New("current_database() returned empty string")
	}
	return name, nil
}
