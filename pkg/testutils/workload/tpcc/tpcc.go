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
	"github.com/cockroachdb/cockroach/pkg/testutils/workload"
	"github.com/spf13/pflag"
)

type tpcc struct {
	flags *pflag.FlagSet

	seed        int64
	warehouses  int
	interleaved bool

	nowString string
}

func init() {
	workload.Register(tpccMeta)
}

var tpccMeta = workload.Meta{
	Name: `tpcc`,
	Description: `TPC-C simulates a transaction processing workload` +
		` using a rich schema of multiple tables`,
	New: func() workload.Generator {
		g := &tpcc{flags: pflag.NewFlagSet(`tpcc`, pflag.ContinueOnError)}
		g.flags.Int64Var(&g.seed, `seed`, 1, `Random number generator seed`)
		g.flags.IntVar(&g.warehouses, `warehouses`, 1, `Number of warehouses for loading`)
		g.flags.BoolVar(&g.interleaved, `interleaved`, false, `Use interleaved tables`)
		g.flags.StringVar(&g.nowString, `now`, `2006-01-02 15:04:05`,
			`Timestamp to use in data generation`)
		return g
	},
}

// Meta implements the Generator interface.
func (*tpcc) Meta() workload.Meta { return tpccMeta }

// Flags implements the Generator interface.
func (w *tpcc) Flags() *pflag.FlagSet {
	return w.flags
}

// Hooks implements the Generator interface.
func (*tpcc) Hooks() workload.Hooks {
	return workload.Hooks{}
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

// Ops implements the Generator interface.
func (w *tpcc) Ops() []workload.Operation {
	// TODO(dan): Implement these.
	return nil
}
