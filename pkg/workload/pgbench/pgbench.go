// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package pgbench implements a CockroachDB workload that mirrors the schema
// and transactions of PostgreSQL's pgbench tool. It is intended as a
// bug-hunting and cross-database comparability harness, not as a published
// benchmark.
//
// The default ("tpcb") transaction is the deprecated TPC-B variant that
// pgbench has shipped since 1996:
//
//	BEGIN;
//	UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
//	SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
//	UPDATE pgbench_tellers  SET tbalance = tbalance + :delta WHERE tid = :tid;
//	UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
//	INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
//	  VALUES (:tid, :bid, :aid, :delta, current_timestamp);
//	END;
//
// At scale s the table cardinalities are:
//
//	pgbench_branches: s            (extreme heat — all clients update these s rows)
//	pgbench_tellers:  10*s
//	pgbench_accounts: 100000*s     (bulk of the working set; cold)
//	pgbench_history:  starts empty (append-only)
//
// The combination of multi-statement transactions, severe heat skew across
// tables of very different sizes, a read-after-write within the txn, and an
// append-only history table exercises code paths that single-table workloads
// like `kv` do not.
package pgbench

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

const (
	// pgbench multipliers from the upstream tool. Do not change without a
	// corresponding update to the consistency check.
	naccountsPerScale = 100000
	ntellersPerScale  = 10
	nbranchesPerScale = 1

	// Filler widths chosen to approximate pgbench's row sizes (~100 bytes
	// for accounts/tellers/branches, ~50 bytes for history). Matching the
	// upstream filler widths matters for any comparison that touches block
	// cache footprint or page-fault behavior.
	accountsFillerWidth = 84
	tellersFillerWidth  = 84
	branchesFillerWidth = 88
	historyFillerWidth  = 22

	// pgbench draws delta uniformly from [-5000, 5000].
	deltaRange = 5000

	defaultScale     = 1
	defaultBatchSize = 1000
	defaultMode      = modeTPCB
	modeTPCB         = "tpcb"
	modeSimpleUpdate = "simple-update"
	modeSelectOnly   = "select-only"
)

var RandomSeed = workload.NewUint64RandomSeed()

// pgbench is the workload generator. The schema and transaction shapes are
// chosen to mirror upstream pgbench so that performance pathologies observed
// against PostgreSQL can be reproduced (or ruled out) against CockroachDB.
type pgbench struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	// scale controls table cardinalities (see naccountsPerScale et al.).
	scale int
	// batchSize controls how many rows are emitted per FillBatch call during
	// initial data load. It does not affect the running workload.
	batchSize int
	// mode selects the transaction template; see the mode* constants.
	mode string
}

func init() {
	workload.Register(pgbenchMeta)
}

var pgbenchMeta = workload.Meta{
	Name: `pgbench`,
	Description: `pgbench mirrors PostgreSQL's pgbench tool (TPC-B-like) for ` +
		`bug-hunting and cross-database comparability.`,
	Version:    `1.0.0`,
	RandomSeed: RandomSeed,
	New: func() workload.Generator {
		g := &pgbench{}
		g.flags.FlagSet = pflag.NewFlagSet(`pgbench`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`batch-size`: {RuntimeOnly: true},
		}
		g.flags.IntVar(&g.scale, `scale`, defaultScale,
			`Scale factor; sets accounts=100000*scale, tellers=10*scale, branches=scale.`)
		g.flags.IntVar(&g.batchSize, `batch-size`, defaultBatchSize,
			`Rows per batch during initial data load.`)
		g.flags.StringVar(&g.mode, `mode`, defaultMode,
			`Transaction mode: tpcb (default 5-statement), simple-update (-N), select-only (-S).`)
		RandomSeed.AddFlag(&g.flags)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*pgbench) Meta() workload.Meta { return pgbenchMeta }

// Flags implements the Flagser interface.
func (g *pgbench) Flags() workload.Flags { return g.flags }

// ConnFlags implements the ConnFlagser interface.
func (g *pgbench) ConnFlags() *workload.ConnFlags { return g.connFlags }

// Hooks implements the Hookser interface.
func (g *pgbench) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if g.scale < 1 {
				return errors.Errorf(`scale must be >= 1; was %d`, g.scale)
			}
			if g.batchSize <= 0 {
				return errors.Errorf(`batch-size must be > 0; was %d`, g.batchSize)
			}
			switch g.mode {
			case modeTPCB, modeSimpleUpdate, modeSelectOnly:
			default:
				return errors.Errorf(
					`invalid mode %q (want one of tpcb, simple-update, select-only)`, g.mode)
			}
			return nil
		},
		CheckConsistency: g.checkConsistency,
	}
}

// checkConsistency verifies the invariants maintained by the workload.
// Which sums must match depends on the mode the workload was run in, since
// simple-update and select-only deliberately do not touch every table.
//
// Always-true invariants (across all modes, assuming a single mode was run
// since init):
//
//  1. Row counts in accounts/tellers/branches match the scale factor — no
//     mode inserts or deletes from these tables.
//  2. sum(abalance) == sum(history.delta) — both are written together by
//     tpcb and simple-update; both stay zero in select-only.
//  3. sum(bbalance) == sum(tbalance) — both are written only by tpcb.
//
// Mode-specific invariant: under tpcb, the four sums are all equal because
// every transaction applies the same delta to abalance, bbalance, tbalance,
// and history.delta.
//
// A violation of (1) catches accidental schema or row drops. A violation of
// (2) or (3) indicates a lost or duplicated write somewhere in the
// transactional path.
func (g *pgbench) checkConsistency(ctx context.Context, db *gosql.DB) error {
	expected := []struct {
		table string
		count int64
	}{
		{`pgbench_accounts`, int64(naccountsPerScale * g.scale)},
		{`pgbench_tellers`, int64(ntellersPerScale * g.scale)},
		{`pgbench_branches`, int64(nbranchesPerScale * g.scale)},
	}
	for _, e := range expected {
		var got int64
		if err := db.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT count(*) FROM %s`, e.table)).Scan(&got); err != nil {
			return errors.Wrapf(err, "row count for %s", e.table)
		}
		if got != e.count {
			return errors.Errorf("row count for %s: expected %d, got %d", e.table, e.count, got)
		}
	}

	var aSum, bSum, tSum, hSum int64
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(sum(abalance), 0) FROM pgbench_accounts`).Scan(&aSum); err != nil {
		return errors.Wrap(err, "sum(abalance)")
	}
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(sum(bbalance), 0) FROM pgbench_branches`).Scan(&bSum); err != nil {
		return errors.Wrap(err, "sum(bbalance)")
	}
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(sum(tbalance), 0) FROM pgbench_tellers`).Scan(&tSum); err != nil {
		return errors.Wrap(err, "sum(tbalance)")
	}
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(sum(delta), 0) FROM pgbench_history`).Scan(&hSum); err != nil {
		return errors.Wrap(err, "sum(history.delta)")
	}
	if aSum != hSum {
		return errors.Errorf(
			"balance invariant violated: sum(abalance)=%d != sum(history.delta)=%d", aSum, hSum)
	}
	if bSum != tSum {
		return errors.Errorf(
			"balance invariant violated: sum(bbalance)=%d != sum(tbalance)=%d", bSum, tSum)
	}
	if g.mode == modeTPCB && aSum != bSum {
		return errors.Errorf(
			"balance invariant violated under tpcb mode: sum(abalance)=%d != sum(bbalance)=%d",
			aSum, bSum)
	}
	fmt.Printf("pgbench: row counts ok; sum(abalance)=%d sum(bbalance)=%d sum(tbalance)=%d sum(history.delta)=%d\n",
		aSum, bSum, tSum, hSum)
	return nil
}

// Schema definitions. We deliberately match upstream pgbench, including:
//   - 1-based primary keys (so values from a PG benchmark can be cross-loaded);
//   - CHAR(N) filler columns of the same widths as upstream;
//   - no primary key on pgbench_history (it is an append-only log).
//
// CockroachDB will assign pgbench_history a hidden unique_rowid PK, which
// spreads inserts across ranges — a notable behavioral divergence from PG
// (where the heap appends sequentially). This is a feature for our purposes:
// we get the same logical workload without an artificial single-range
// hotspot, and any consumer that explicitly wants the hotspot can add a
// sequential PK after the fact.
const (
	accountsSchema = `(
		aid      INT NOT NULL PRIMARY KEY,
		bid      INT,
		abalance INT,
		filler   CHAR(84),
		FAMILY (aid, bid, abalance, filler)
	)`
	tellersSchema = `(
		tid      INT NOT NULL PRIMARY KEY,
		bid      INT,
		tbalance INT,
		filler   CHAR(84),
		FAMILY (tid, bid, tbalance, filler)
	)`
	branchesSchema = `(
		bid      INT NOT NULL PRIMARY KEY,
		bbalance INT,
		filler   CHAR(88),
		FAMILY (bid, bbalance, filler)
	)`
	historySchema = `(
		tid    INT,
		bid    INT,
		aid    INT,
		delta  INT,
		mtime  TIMESTAMP,
		filler CHAR(22)
	)`
)

var accountsTypes = []*types.T{types.Int, types.Int, types.Int, types.Bytes}

// Tables implements the Generator interface.
func (g *pgbench) Tables() []workload.Table {
	naccounts := naccountsPerScale * g.scale
	ntellers := ntellersPerScale * g.scale
	nbranches := nbranchesPerScale * g.scale

	// Accounts is the only table large enough to justify columnar batched
	// init; the others are tiny and use the simpler Tuples helper.
	accountsBatches := (naccounts + g.batchSize - 1) / g.batchSize
	accounts := workload.Table{
		Name:   `pgbench_accounts`,
		Schema: accountsSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: accountsBatches,
			FillBatch: func(batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
				rowBegin := batchIdx * g.batchSize
				rowEnd := rowBegin + g.batchSize
				if rowEnd > naccounts {
					rowEnd = naccounts
				}
				cb.Reset(accountsTypes, rowEnd-rowBegin, coldata.StandardColumnFactory)
				aidCol := cb.ColVec(0).Int64()
				bidCol := cb.ColVec(1).Int64()
				abalCol := cb.ColVec(2).Int64()
				fillerCol := cb.ColVec(3).Bytes()
				fillerCol.Reset()
				for rowIdx := rowBegin; rowIdx < rowEnd; rowIdx++ {
					// pgbench is 1-indexed; aid in [1, naccounts].
					aid := int64(rowIdx + 1)
					off := rowIdx - rowBegin
					aidCol[off] = aid
					// bid = ceil(aid / naccountsPerScale), 1-indexed.
					bidCol[off] = (aid-1)/naccountsPerScale + 1
					abalCol[off] = 0
					var buf []byte
					*a, buf = a.Alloc(accountsFillerWidth)
					for i := range buf {
						buf[i] = ' '
					}
					fillerCol.Set(off, buf)
				}
			},
		},
	}

	branches := workload.Table{
		Name:   `pgbench_branches`,
		Schema: branchesSchema,
		InitialRows: workload.Tuples(nbranches, func(i int) []interface{} {
			return []interface{}{i + 1, 0, blankFiller(branchesFillerWidth)}
		}),
	}

	tellers := workload.Table{
		Name:   `pgbench_tellers`,
		Schema: tellersSchema,
		InitialRows: workload.Tuples(ntellers, func(i int) []interface{} {
			tid := i + 1
			bid := (tid-1)/ntellersPerScale + 1
			return []interface{}{tid, bid, 0, blankFiller(tellersFillerWidth)}
		}),
	}

	history := workload.Table{
		Name:   `pgbench_history`,
		Schema: historySchema,
		// Starts empty.
	}

	return []workload.Table{accounts, branches, tellers, history}
}

// blankFiller returns a string of n spaces. pgbench uses CHAR(N) defaulted
// to spaces; we generate the equivalent client-side so the row size on disk
// matches what PG produces.
func blankFiller(n int) string {
	return strings.Repeat(" ", n)
}

// Ops implements the Opser interface.
func (g *pgbench) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db.SetMaxOpenConns(g.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(g.connFlags.Concurrency + 1)

	stmts, err := prepareStatements(ctx, db)
	if err != nil {
		return workload.QueryLoad{}, errors.CombineErrors(err, db.Close())
	}

	naccounts := naccountsPerScale * g.scale
	ntellers := ntellersPerScale * g.scale
	nbranches := nbranchesPerScale * g.scale

	ql := workload.QueryLoad{
		Close: func(_ context.Context) error { return db.Close() },
	}
	for i := 0; i < g.connFlags.Concurrency; i++ {
		// Seed each worker independently so a given (seed, worker) pair is
		// reproducible.
		rng := rand.New(rand.NewPCG(RandomSeed.Seed(), uint64(i)))
		hists := reg.GetHandle()
		mode := g.mode

		workerFn := func(ctx context.Context) error {
			aid := rng.IntN(naccounts) + 1
			bid := rng.IntN(nbranches) + 1
			tid := rng.IntN(ntellers) + 1
			delta := rng.IntN(2*deltaRange+1) - deltaRange

			start := timeutil.Now()
			err := runTxn(ctx, db, stmts, mode, aid, bid, tid, delta)
			hists.Get(mode).Record(timeutil.Since(start))
			return err
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}
	return ql, nil
}

// pgbenchStmts holds the prepared statements used by all worker goroutines.
// Statements are prepared once on the shared *sql.DB; database/sql handles
// per-connection re-preparation transparently.
type pgbenchStmts struct {
	updateAccount *gosql.Stmt
	selectAccount *gosql.Stmt
	updateTeller  *gosql.Stmt
	updateBranch  *gosql.Stmt
	insertHistory *gosql.Stmt
}

func prepareStatements(ctx context.Context, db *gosql.DB) (*pgbenchStmts, error) {
	prep := func(q string) (*gosql.Stmt, error) {
		s, err := db.PrepareContext(ctx, q)
		if err != nil {
			return nil, errors.Wrapf(err, "preparing %q", q)
		}
		return s, nil
	}
	s := &pgbenchStmts{}
	var err error
	if s.updateAccount, err = prep(
		`UPDATE pgbench_accounts SET abalance = abalance + $1 WHERE aid = $2`); err != nil {
		return nil, err
	}
	if s.selectAccount, err = prep(
		`SELECT abalance FROM pgbench_accounts WHERE aid = $1`); err != nil {
		return nil, err
	}
	if s.updateTeller, err = prep(
		`UPDATE pgbench_tellers SET tbalance = tbalance + $1 WHERE tid = $2`); err != nil {
		return nil, err
	}
	if s.updateBranch, err = prep(
		`UPDATE pgbench_branches SET bbalance = bbalance + $1 WHERE bid = $2`); err != nil {
		return nil, err
	}
	if s.insertHistory, err = prep(
		`INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) ` +
			`VALUES ($1, $2, $3, $4, current_timestamp)`); err != nil {
		return nil, err
	}
	return s, nil
}

// runTxn executes one pgbench transaction in the requested mode. It uses
// crdb.ExecuteTx so serializable conflicts are retried transparently — the
// same retry loop ledger uses. Bug-hunters who want to observe raw retry
// behavior should disable retries at the workload-runner layer rather than
// here, to keep the per-mode SQL identical to upstream pgbench.
func runTxn(
	ctx context.Context, db *gosql.DB, stmts *pgbenchStmts, mode string, aid, bid, tid, delta int,
) error {
	return crdb.ExecuteTx(ctx, db, nil /* txopts */, func(tx *gosql.Tx) error {
		switch mode {
		case modeSelectOnly:
			var abalance int
			return tx.StmtContext(ctx, stmts.selectAccount).
				QueryRowContext(ctx, aid).Scan(&abalance)

		case modeSimpleUpdate:
			if _, err := tx.StmtContext(ctx, stmts.updateAccount).
				ExecContext(ctx, delta, aid); err != nil {
				return err
			}
			var abalance int
			if err := tx.StmtContext(ctx, stmts.selectAccount).
				QueryRowContext(ctx, aid).Scan(&abalance); err != nil {
				return err
			}
			_, err := tx.StmtContext(ctx, stmts.insertHistory).
				ExecContext(ctx, tid, bid, aid, delta)
			return err

		case modeTPCB:
			if _, err := tx.StmtContext(ctx, stmts.updateAccount).
				ExecContext(ctx, delta, aid); err != nil {
				return err
			}
			var abalance int
			if err := tx.StmtContext(ctx, stmts.selectAccount).
				QueryRowContext(ctx, aid).Scan(&abalance); err != nil {
				return err
			}
			if _, err := tx.StmtContext(ctx, stmts.updateTeller).
				ExecContext(ctx, delta, tid); err != nil {
				return err
			}
			if _, err := tx.StmtContext(ctx, stmts.updateBranch).
				ExecContext(ctx, delta, bid); err != nil {
				return err
			}
			_, err := tx.StmtContext(ctx, stmts.insertHistory).
				ExecContext(ctx, tid, bid, aid, delta)
			return err

		default:
			return errors.AssertionFailedf("unknown mode %q", mode)
		}
	})
}
