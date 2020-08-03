// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ledger

import (
	"context"
	gosql "database/sql"
	"hash/fnv"
	"math/rand"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

type ledger struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed              int64
	customers         int
	interleaved       bool
	inlineArgs        bool
	splits            int
	fks               bool
	historicalBalance bool
	mix               string

	txs  []tx
	deck []int // contains indexes into the txs slice

	reg      *histogram.Registry
	rngPool  *sync.Pool
	hashPool *sync.Pool
}

func init() {
	workload.Register(ledgerMeta)
}

var ledgerMeta = workload.Meta{
	Name:        `ledger`,
	Description: `Ledger simulates an accounting system using double-entry bookkeeping`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &ledger{}
		g.flags.FlagSet = pflag.NewFlagSet(`ledger`, pflag.ContinueOnError)
		g.connFlags = workload.NewConnFlags(&g.flags)
		g.flags.Int64Var(&g.seed, `seed`, 1, `Random number generator seed`)
		g.flags.IntVar(&g.customers, `customers`, 1000, `Number of customers`)
		g.flags.BoolVar(&g.interleaved, `interleaved`, false, `Use interleaved tables`)
		g.flags.BoolVar(&g.inlineArgs, `inline-args`, false, `Use inline query arguments`)
		g.flags.IntVar(&g.splits, `splits`, 0, `Number of splits to perform before starting normal operations`)
		g.flags.BoolVar(&g.fks, `fks`, true, `Add the foreign keys`)
		g.flags.BoolVar(&g.historicalBalance, `historical-balance`, false, `Perform balance txns using historical reads`)
		g.flags.StringVar(&g.mix, `mix`,
			`balance=50,withdrawal=37,deposit=12,reversal=0`,
			`Weights for the transaction mix.`)
		return g
	},
}

// FromFlags returns a new ledger Generator configured with the given flags.
func FromFlags(flags ...string) workload.Generator {
	return workload.FromFlags(ledgerMeta, flags...)
}

// Meta implements the Generator interface.
func (*ledger) Meta() workload.Meta { return ledgerMeta }

// Flags implements the Flagser interface.
func (w *ledger) Flags() workload.Flags { return w.flags }

// Hooks implements the Hookser interface.
func (w *ledger) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if w.interleaved {
				return errors.Errorf("interleaved tables are not yet supported")
			}
			return initializeMix(w)
		},
		PostLoad: func(sqlDB *gosql.DB) error {
			if w.fks {
				fkStmts := []string{
					`create index entry_auto_index_fk_customer on entry (customer_id ASC)`,
					`create index entry_auto_index_fk_transaction on entry (transaction_id ASC)`,
					`alter table entry add foreign key (customer_id) references customer (id)`,
					`alter table entry add foreign key (transaction_id) references transaction (external_id)`,
				}
				for _, fkStmt := range fkStmts {
					if _, err := sqlDB.Exec(fkStmt); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (w *ledger) Tables() []workload.Table {
	if w.rngPool == nil {
		w.rngPool = &sync.Pool{
			New: func() interface{} { return rand.New(rand.NewSource(timeutil.Now().UnixNano())) },
		}
	}
	if w.hashPool == nil {
		w.hashPool = &sync.Pool{
			New: func() interface{} { return fnv.New64() },
		}
	}

	customer := workload.Table{
		Name:   `customer`,
		Schema: ledgerCustomerSchema,
		InitialRows: workload.TypedTuples(
			w.customers,
			ledgerCustomerTypes,
			w.ledgerCustomerInitialRow,
		),
		Splits: workload.Tuples(
			numTxnsPerCustomer*w.splits,
			w.ledgerCustomerSplitRow,
		),
	}
	transaction := workload.Table{
		Name:   `transaction`,
		Schema: ledgerTransactionSchema,
		InitialRows: workload.TypedTuples(
			numTxnsPerCustomer*w.customers,
			ledgerTransactionColTypes,
			w.ledgerTransactionInitialRow,
		),
		Splits: workload.Tuples(
			w.splits,
			w.ledgerTransactionSplitRow,
		),
	}
	entry := workload.Table{
		Name:   `entry`,
		Schema: ledgerEntrySchema,
		InitialRows: workload.Tuples(
			numEntriesPerCustomer*w.customers,
			w.ledgerEntryInitialRow,
		),
		Splits: workload.Tuples(
			numEntriesPerCustomer*w.splits,
			w.ledgerEntrySplitRow,
		),
	}
	session := workload.Table{
		Name:   `session`,
		Schema: ledgerSessionSchema,
		InitialRows: workload.Tuples(
			w.customers,
			w.ledgerSessionInitialRow,
		),
		Splits: workload.Tuples(
			w.splits,
			w.ledgerSessionSplitRow,
		),
	}
	return []workload.Table{
		customer, transaction, entry, session,
	}
}

// Ops implements the Opser interface.
func (w *ledger) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(w, w.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(w.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(w.connFlags.Concurrency + 1)

	w.reg = reg
	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	now := timeutil.Now().UnixNano()
	for i := 0; i < w.connFlags.Concurrency; i++ {
		worker := &worker{
			config:   w,
			hists:    reg.GetHandle(),
			db:       db,
			rng:      rand.New(rand.NewSource(now + int64(i))),
			deckPerm: append([]int(nil), w.deck...),
			permIdx:  len(w.deck),
		}
		ql.WorkerFns = append(ql.WorkerFns, worker.run)
	}
	return ql, nil
}
