// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package tpcc

import (
	"context"
	gosql "database/sql"
	"math/rand"
	"net/url"
	"sync"

	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

type tpcc struct {
	flags workload.Flags

	seed        int64
	warehouses  int
	interleaved bool
	nowString   string

	mix        string
	doWaits    bool
	workers    int
	fks        bool
	dbOverride string

	txs []tx
	// deck contains indexes into the txs slice.
	deck []int

	auditor *auditor

	reg *workload.HistogramRegistry

	split   bool
	scatter bool

	partitions int

	usePostgres  bool
	serializable bool
	txOpts       *gosql.TxOptions

	expensiveChecks bool

	randomCIDsCache struct {
		syncutil.Mutex
		values [][]int
	}
	rngPool *sync.Pool
}

func init() {
	workload.Register(tpccMeta)
}

// FromWarehouses returns a tpcc generator pre-configured with the specified
// number of warehouses.
func FromWarehouses(warehouses int) workload.Generator {
	gen := tpccMeta.New().(*tpcc)
	gen.warehouses = warehouses
	return gen
}

var tpccMeta = workload.Meta{
	Name: `tpcc`,
	Description: `TPC-C simulates a transaction processing workload` +
		` using a rich schema of multiple tables`,
	Version: `2.0.1`,
	New: func() workload.Generator {
		g := &tpcc{}
		g.flags.FlagSet = pflag.NewFlagSet(`tpcc`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`db`:               {RuntimeOnly: true},
			`fks`:              {RuntimeOnly: true},
			`mix`:              {RuntimeOnly: true},
			`partitions`:       {RuntimeOnly: true},
			`scatter`:          {RuntimeOnly: true},
			`serializable`:     {RuntimeOnly: true},
			`split`:            {RuntimeOnly: true},
			`wait`:             {RuntimeOnly: true},
			`workers`:          {RuntimeOnly: true},
			`expensive-checks`: {RuntimeOnly: true, CheckConsistencyOnly: true},
		}

		g.flags.Int64Var(&g.seed, `seed`, 1, `Random number generator seed`)
		g.flags.IntVar(&g.warehouses, `warehouses`, 1, `Number of warehouses for loading`)
		g.flags.BoolVar(&g.interleaved, `interleaved`, false, `Use interleaved tables`)
		// Hardcode this since it doesn't seem like anyone will want to change
		// it and it's really noisy in the generated fixture paths.
		g.nowString = `2006-01-02 15:04:05`

		g.flags.StringVar(&g.mix, `mix`,
			`newOrder=10,payment=10,orderStatus=1,delivery=1,stockLevel=1`,
			`Weights for the transaction mix. The default matches the TPCC spec.`)
		g.flags.BoolVar(&g.doWaits, `wait`, true, `Run in wait mode (include think/keying sleeps)`)
		g.flags.StringVar(&g.dbOverride, `db`, ``,
			`Override for the SQL database to use. If empty, defaults to the generator name`)
		g.flags.IntVar(&g.workers, `workers`, 0,
			`Number of concurrent workers. Defaults to --warehouses * 10`)
		g.flags.BoolVar(&g.fks, `fks`, true, `Add the foreign keys`)
		g.flags.IntVar(&g.partitions, `partitions`, 0, `Partition tables (requires split)`)
		g.flags.BoolVar(&g.scatter, `scatter`, false, `Scatter ranges`)
		g.flags.BoolVar(&g.serializable, `serializable`, false, `Force serializable mode`)
		g.flags.BoolVar(&g.split, `split`, false, `Split tables`)

		g.flags.BoolVar(&g.expensiveChecks, `expensive-checks`, false, `Run expensive checks`)
		return g
	},
}

// Meta implements the Generator interface.
func (*tpcc) Meta() workload.Meta { return tpccMeta }

// Flags implements the Flagser interface.
func (w *tpcc) Flags() workload.Flags { return w.flags }

// Hooks implements the Hookser interface.
func (w *tpcc) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if w.workers == 0 {
				w.workers = w.warehouses * numWorkersPerWarehouse
			}
			if w.doWaits && w.workers != w.warehouses*numWorkersPerWarehouse {
				return errors.Errorf(`--waits=true and --warehouses=%d requires --workers=%d`,
					w.warehouses, w.warehouses*numWorkersPerWarehouse)
			}

			if w.partitions != 0 && !w.split {
				return errors.Errorf(`--partitions requires --split`)
			}

			if w.serializable {
				w.txOpts = &gosql.TxOptions{Isolation: gosql.LevelSerializable}
			}

			w.auditor = newAuditor(w.warehouses)

			return initializeMix(w)
		},
		PostLoad: func(sqlDB *gosql.DB) error {
			fkStmts := []string{
				`alter table district add foreign key (d_w_id) references warehouse (w_id)`,
				`alter table customer add foreign key (c_w_id, c_d_id) references district (d_w_id, d_id)`,
				`alter table history add foreign key (h_c_w_id, h_c_d_id, h_c_id) references customer (c_w_id, c_d_id, c_id)`,
				`alter table history add foreign key (h_w_id, h_d_id) references district (d_w_id, d_id)`,
				`alter table "order" add foreign key (o_w_id, o_d_id, o_c_id) references customer (c_w_id, c_d_id, c_id)`,
				`alter table stock add foreign key (s_w_id) references warehouse (w_id)`,
				`alter table stock add foreign key (s_i_id) references item (i_id)`,
				`alter table order_line add foreign key (ol_w_id, ol_d_id, ol_o_id) references "order" (o_w_id, o_d_id, o_id)`,
				`alter table order_line add foreign key (ol_supply_w_id, ol_d_id) references stock (s_w_id, s_i_id)`,
			}
			for _, fkStmt := range fkStmts {
				if _, err := sqlDB.Exec(fkStmt); err != nil {
					return err
				}
			}
			return nil
		},
		PostRun: func(start time.Time) error {
			w.auditor.runChecks()
			const totalHeader = "\n_elapsed_______tpmC____efc__avg(ms)__p50(ms)__p90(ms)__p95(ms)__p99(ms)_pMax(ms)"
			fmt.Println(totalHeader)
			startElapsed := timeutil.Since(start)

			const newOrderName = `newOrder`
			w.reg.Tick(func(t workload.HistogramTick) {
				if newOrderName == t.Name {
					tpmC := float64(t.Ops) / startElapsed.Seconds() * 60
					fmt.Printf("%7.1fs %10.1f %5.1f%% %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
						startElapsed.Seconds(),
						tpmC,
						100*tpmC/(12.86*float64(w.warehouses)),
						time.Duration(t.Cumulative.Mean()).Seconds()*1000,
						time.Duration(t.Cumulative.ValueAtQuantile(50)).Seconds()*1000,
						time.Duration(t.Cumulative.ValueAtQuantile(90)).Seconds()*1000,
						time.Duration(t.Cumulative.ValueAtQuantile(95)).Seconds()*1000,
						time.Duration(t.Cumulative.ValueAtQuantile(99)).Seconds()*1000,
						time.Duration(t.Cumulative.ValueAtQuantile(100)).Seconds()*1000,
					)
				}
			})
			return nil
		},
		CheckConsistency: func(ctx context.Context, db *gosql.DB) error {
			// TODO(arjun): We should run each test in a single transaction as
			// currently we have to shut down load before running the checks.
			for _, check := range allChecks() {
				if !w.expensiveChecks && check.expensive {
					continue
				}
				start := timeutil.Now()
				err := check.f(db)
				log.Infof(ctx, `check %s took %s`, check.name, timeutil.Since(start))
				if err != nil {
					return errors.Wrapf(err, `check failed: %s`, check.name)
				}
			}
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (w *tpcc) Tables() []workload.Table {
	if w.rngPool == nil {
		w.rngPool = &sync.Pool{
			New: func() interface{} { return rand.New(rand.NewSource(timeutil.Now().UnixNano())) },
		}
	}

	warehouse := workload.Table{
		Name:   `warehouse`,
		Schema: tpccWarehouseSchema,
		InitialRows: workload.Tuples(
			w.warehouses,
			w.tpccWarehouseInitialRow,
		),
	}
	district := workload.Table{
		Name:   `district`,
		Schema: tpccDistrictSchema,
		InitialRows: workload.Tuples(
			numDistrictsPerWarehouse*w.warehouses,
			w.tpccDistrictInitialRow,
		),
	}
	customer := workload.Table{
		Name:   `customer`,
		Schema: tpccCustomerSchema,
		InitialRows: workload.Tuples(
			numCustomersPerWarehouse*w.warehouses,
			w.tpccCustomerInitialRow,
		),
	}
	history := workload.Table{
		Name:   `history`,
		Schema: tpccHistorySchema,
		InitialRows: workload.Tuples(
			numCustomersPerWarehouse*w.warehouses,
			w.tpccHistoryInitialRow,
		),
	}
	order := workload.Table{
		Name:   `order`,
		Schema: tpccOrderSchema,
		InitialRows: workload.Tuples(
			numOrdersPerWarehouse*w.warehouses,
			w.tpccOrderInitialRow,
		),
	}
	newOrder := workload.Table{
		Name:   `new_order`,
		Schema: tpccNewOrderSchema,
		InitialRows: workload.Tuples(
			numNewOrdersPerWarehouse*w.warehouses,
			w.tpccNewOrderInitialRow,
		),
	}
	item := workload.Table{
		Name:   `item`,
		Schema: tpccItemSchema,
		InitialRows: workload.Tuples(
			numItems,
			w.tpccItemInitialRow,
		),
	}
	stock := workload.Table{
		Name:   `stock`,
		Schema: tpccStockSchema,
		InitialRows: workload.Tuples(
			numStockPerWarehouse*w.warehouses,
			w.tpccStockInitialRow,
		),
	}
	orderLine := workload.Table{
		Name:   `order_line`,
		Schema: tpccOrderLineSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: numOrdersPerWarehouse * w.warehouses,
			Batch:      w.tpccOrderLineInitialRowBatch,
		},
	}
	if w.interleaved {
		district.Schema += tpccDistrictSchemaInterleave
		customer.Schema += tpccCustomerSchemaInterleave
		order.Schema += tpccOrderSchemaInterleave
		stock.Schema += tpccStockSchemaInterleave
		orderLine.Schema += tpccOrderLineSchemaInterleave
		// This natural-seeming interleave makes performance worse, because this
		// table has a ton of churn and produces a lot of MVCC tombstones, which
		// then will gum up the works of scans over the parent table.
		_ = tpccNewOrderSchemaInterleave
	}
	return []workload.Table{
		warehouse, district, customer, history, order, newOrder, item, stock, orderLine,
	}
}

// Ops implements the Opser interface.
func (w *tpcc) Ops(urls []string, reg *workload.HistogramRegistry) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(w, w.dbOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	parsedURL, err := url.Parse(urls[0])
	if err != nil {
		return workload.QueryLoad{}, err
	}

	w.usePostgres = parsedURL.Port() == "5432"

	nConns := w.warehouses / len(urls)
	dbs := make([]*gosql.DB, len(urls))
	for i, url := range urls {
		dbs[i], err = gosql.Open(`postgres`, url)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		// Allow a maximum of concurrency+1 connections to the database.
		dbs[i].SetMaxOpenConns(nConns)
		dbs[i].SetMaxIdleConns(nConns)
	}

	if w.split {
		splitTables(dbs[0], w.warehouses)

		if w.partitions > 0 {
			partitionTables(dbs[0], w.warehouses, w.partitions)
		}
	}

	if w.scatter {
		scatterRanges(dbs[0])
	}

	// If no partitions were specified, pretend there is a single partition
	// containing all warehouses.
	if w.partitions == 0 {
		w.partitions = 1
	}
	// Assign each DB connection pool to a partition. This assumes that dbs[i] is
	// a machine that holds partition "i % *partitions".
	partitionDBs := make([][]*gosql.DB, w.partitions)
	for i, db := range dbs {
		p := i % w.partitions
		partitionDBs[p] = append(partitionDBs[p], db)
	}
	for i := range partitionDBs {
		if partitionDBs[i] == nil {
			partitionDBs[i] = dbs
		}
	}

	w.reg = reg

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for workerIdx := 0; workerIdx < w.workers; workerIdx++ {
		warehouse := workerIdx % w.warehouses
		// NB: Each partition contains "warehouses / partitions" warehouses. See
		// partitionTables().
		p := (warehouse * w.partitions) / w.warehouses
		dbs := partitionDBs[p]
		db := dbs[warehouse%len(dbs)]
		worker := &worker{
			config:    w,
			hists:     reg.GetHandle(),
			idx:       workerIdx,
			db:        db,
			warehouse: warehouse,
			deckPerm:  make([]int, len(w.deck)),
			permIdx:   len(w.deck),
		}
		copy(worker.deckPerm, w.deck)

		ql.WorkerFns = append(ql.WorkerFns, worker.run)
	}
	// Preregister all of the histograms so they always print.
	for _, tx := range allTxs {
		reg.GetHandle().Get(tx.name)
	}
	return ql, nil
}
