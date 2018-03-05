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
	gosql "database/sql"

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
	dbOverride string

	txs         []tx
	totalWeight int
}

func init() {
	workload.Register(tpccMeta)
}

var tpccMeta = workload.Meta{
	Name: `tpcc`,
	Description: `TPC-C simulates a transaction processing workload` +
		` using a rich schema of multiple tables`,
	Version: `1.0.0`,
	New: func() workload.Generator {
		g := &tpcc{}
		g.flags.FlagSet = pflag.NewFlagSet(`tpcc`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`db`:      {RuntimeOnly: true},
			`mix`:     {RuntimeOnly: true},
			`wait`:    {RuntimeOnly: true},
			`workers`: {RuntimeOnly: true},
		}

		g.flags.Int64Var(&g.seed, `seed`, 1, `Random number generator seed`)
		g.flags.IntVar(&g.warehouses, `warehouses`, 1, `Number of warehouses for loading`)
		g.flags.BoolVar(&g.interleaved, `interleaved`, false, `Use interleaved tables`)
		// Hardcode this since it doesn't seem like anyone will want to change
		// it and it's really noisy in the generated fixture paths.
		g.nowString = `2006-01-02 15:04:05`

		g.flags.StringVar(&g.mix, `mix`,
			`newOrder=45,payment=43,orderStatus=4,delivery=4,stockLevel=4`,
			`Weights for the transaction mix. The default matches the TPCC spec.`)
		g.flags.BoolVar(&g.doWaits, `wait`, true, `Run in wait mode (include think/keying sleeps)`)
		g.flags.StringVar(&g.dbOverride, `db`, ``,
			`Override for the SQL database to use. If empty, defaults to the generator name`)
		g.flags.IntVar(&g.workers, `workers`, 0,
			`Number of concurrent workers. Defaults to --warehouses * 10`)

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

			return initializeMix(w)
		},
	}
}

// Tables implements the Generator interface.
func (w *tpcc) Tables() []workload.Table {
	warehouse := workload.Table{
		Name:            `warehouse`,
		Schema:          tpccWarehouseSchema,
		InitialRowCount: w.warehouses,
		InitialRowFn:    w.tpccWarehouseInitialRow,
	}
	district := workload.Table{
		Name:            `district`,
		Schema:          tpccDistrictSchema,
		InitialRowCount: numDistrictsPerWarehouse * w.warehouses,
		InitialRowFn:    w.tpccDistrictInitialRow,
	}
	customer := workload.Table{
		Name:            `customer`,
		Schema:          tpccCustomerSchema,
		InitialRowCount: numCustomersPerWarehouse * w.warehouses,
		InitialRowFn:    w.tpccCustomerInitialRow,
	}
	history := workload.Table{
		Name:            `history`,
		Schema:          tpccHistorySchema,
		InitialRowCount: numCustomersPerWarehouse * w.warehouses,
		InitialRowFn:    w.tpccHistoryInitialRow,
	}
	order := workload.Table{
		Name:            `order`,
		Schema:          tpccOrderSchema,
		InitialRowCount: numOrdersPerWarehouse * w.warehouses,
		InitialRowFn:    w.tpccOrderInitialRow,
	}
	newOrder := workload.Table{
		Name:            `new_order`,
		Schema:          tpccNewOrderSchema,
		InitialRowCount: numNewOrdersPerWarehouse * w.warehouses,
		InitialRowFn:    w.tpccNewOrderInitialRow,
	}
	item := workload.Table{
		Name:            `item`,
		Schema:          tpccItemSchema,
		InitialRowCount: numItems,
		InitialRowFn:    w.tpccItemInitialRow,
	}
	stock := workload.Table{
		Name:            `stock`,
		Schema:          tpccStockSchema,
		InitialRowCount: numStockPerWarehouse * w.warehouses,
		InitialRowFn:    w.tpccStockInitialRow,
	}
	orderLine := workload.Table{
		Name:            `order_line`,
		Schema:          tpccOrderLineSchema,
		InitialRowCount: hackOrderLinesPerWarehouse * w.warehouses,
		InitialRowFn:    w.tpccOrderLineInitialRow,
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

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for workerIdx := 0; workerIdx < w.workers; workerIdx++ {
		warehouse := workerIdx / numWorkersPerWarehouse
		db := dbs[warehouse%len(dbs)]
		worker := &worker{
			config:    w,
			hists:     reg.GetHandle(),
			idx:       workerIdx,
			db:        db,
			warehouse: warehouse,
		}
		ql.WorkerFns = append(ql.WorkerFns, worker.run)
	}
	if w.doWaits {
		// TODO(dan): doWaits is currently our catch-all for "run this to spec".
		// It should probably be renamed to match.
		ql.ResultHist = `newOrder`
	}
	return ql, nil
}
