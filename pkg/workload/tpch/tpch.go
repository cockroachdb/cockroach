// Copyright 2018 The Cockroach Authors.
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

package tpch

import (
	"context"
	gosql "database/sql"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	numNation        = 25
	numRegion        = 5
	numPartPerSF     = 200000
	numSupplierPerSF = 10000
	numPartSuppPerSF = 800000
	numCustomerPerSF = 150000
	numOrderPerSF    = 1500000
	numLineItemPerSF = 6001215
)

type tpch struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed        int64
	scaleFactor int

	distsql bool

	queriesRaw      string
	selectedQueries []string
}

func init() {
	workload.Register(tpchMeta)
}

var tpchMeta = workload.Meta{
	Name:        `tpch`,
	Description: `TPC-H is a read-only workload of "analytics" queries on large datasets.`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &tpch{}
		g.flags.FlagSet = pflag.NewFlagSet(`tpcc`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`queries`:  {RuntimeOnly: true},
			`dist-sql`: {RuntimeOnly: true},
		}
		g.flags.Int64Var(&g.seed, `seed`, 1, `Random number generator seed`)
		g.flags.IntVar(&g.scaleFactor, `scale-factor`, 1,
			`Linear scale of how much data to use (each SF is ~1GB)`)
		g.flags.StringVar(&g.queriesRaw, `queries`, `1,3,7,8,9,19`,
			`Queries to run. Use a comma separated list of query numbers`)
		g.flags.BoolVar(&g.distsql, `dist-sql`, true, `Use DistSQL for query execution`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*tpch) Meta() workload.Meta { return tpchMeta }

// Flags implements the Flagser interface.
func (w *tpch) Flags() workload.Flags { return w.flags }

// Hooks implements the Hookser interface.
func (w *tpch) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			for _, queryName := range strings.Split(w.queriesRaw, `,`) {
				queryName = strings.TrimSpace(queryName)
				switch queryName {
				case `2`, `4`, `13`, `16`, `17`, `18`, `20`, `21`, `22`:
					return errors.Errorf(`query is unsupported: %s`, queryName)
				case `5`, `6`, `10`, `12`, `14`:
					return errors.Errorf(`query causes Cockroach panic (see #13692): %s`, queryName)
				case `11`:
					return errors.Errorf(`group with having not supported yet: %s`, queryName)
				}
				if _, ok := queriesByName[queryName]; !ok {
					return errors.Errorf(`unknown query: %s`, queryName)
				}
				w.selectedQueries = append(w.selectedQueries, queryName)
			}
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (w *tpch) Tables() []workload.Table {
	// TODO(dan): Implement the InitialRowFns for these.
	nation := workload.Table{
		Name:        `nation`,
		Schema:      tpchNationSchema,
		InitialRows: workload.Tuples(numNation, nil),
	}
	region := workload.Table{
		Name:        `region`,
		Schema:      tpchRegionSchema,
		InitialRows: workload.Tuples(numRegion, nil),
	}
	part := workload.Table{
		Name:        `part`,
		Schema:      tpchPartSchema,
		InitialRows: workload.Tuples(numPartPerSF*w.scaleFactor, nil),
	}
	supplier := workload.Table{
		Name:        `supplier`,
		Schema:      tpchSupplierSchema,
		InitialRows: workload.Tuples(numSupplierPerSF*w.scaleFactor, nil),
	}
	partsupp := workload.Table{
		Name:        `partsupp`,
		Schema:      tpchPartSuppSchema,
		InitialRows: workload.Tuples(numPartSuppPerSF*w.scaleFactor, nil),
	}
	customer := workload.Table{
		Name:        `customer`,
		Schema:      tpchCustomerSchema,
		InitialRows: workload.Tuples(numCustomerPerSF*w.scaleFactor, nil),
	}
	orders := workload.Table{
		Name:        `orders`,
		Schema:      tpchOrdersSchema,
		InitialRows: workload.Tuples(numOrderPerSF*w.scaleFactor, nil),
	}
	lineitem := workload.Table{
		Name:        `lineitem`,
		Schema:      tpchLineItemSchema,
		InitialRows: workload.Tuples(numLineItemPerSF*w.scaleFactor, nil),
	}

	return []workload.Table{
		nation, region, part, supplier, partsupp, customer, orders, lineitem,
	}
}

// Ops implements the Opser interface.
func (w *tpch) Ops(urls []string, reg *workload.HistogramRegistry) (workload.QueryLoad, error) {
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

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < w.connFlags.Concurrency; i++ {
		worker := &worker{
			config: w,
			hists:  reg.GetHandle(),
			db:     db,
		}
		ql.WorkerFns = append(ql.WorkerFns, worker.run)
	}
	return ql, nil
}

type worker struct {
	config *tpch
	hists  *workload.Histograms
	db     *gosql.DB
	ops    int
}

func (w *worker) run(ctx context.Context) error {
	queryName := w.config.selectedQueries[w.ops%len(w.config.selectedQueries)]
	w.ops++

	var query string
	if w.config.distsql {
		query = `SET DISTSQL = 'always'; ` + queriesByName[queryName]
	} else {
		query = `SET DISTSQL = 'off'; ` + queriesByName[queryName]
	}

	start := timeutil.Now()
	rows, err := w.db.Query(query)
	if err != nil {
		return err
	}
	var numRows int
	for rows.Next() {
		numRows++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	elapsed := timeutil.Since(start)
	w.hists.Get(queryName).Record(elapsed)
	log.Infof(ctx, "[%s] return %d rows after %4.2f seconds:\n  %s",
		queryName, numRows, elapsed.Seconds(), query)
	return nil
}

const (
	tpchNationSchema = `(
		n_nationkey       INTEGER NOT NULL PRIMARY KEY,
		n_name            CHAR(25) NOT NULL,
		n_regionkey       INTEGER NOT NULL,
		n_comment         VARCHAR(152),
		INDEX n_rk (n_regionkey ASC)
	)`
	tpchRegionSchema = `(
		r_regionkey       INTEGER NOT NULL PRIMARY KEY,
		r_name            CHAR(25) NOT NULL,
		r_comment         VARCHAR(152)
	)`
	tpchPartSchema = `(
		p_partkey         INTEGER NOT NULL PRIMARY KEY,
		p_name            VARCHAR(55) NOT NULL,
		p_mfgr            CHAR(25) NOT NULL,
		p_brand           CHAR(10) NOT NULL,
		p_type            VARCHAR(25) NOT NULL,
		p_size            INTEGER NOT NULL,
		p_container       CHAR(10) NOT NULL,
		p_retailprice     DECIMAL(15,2) NOT NULL,
		p_comment         VARCHAR(23) NOT NULL
	)`
	tpchSupplierSchema = `(
		s_suppkey         INTEGER NOT NULL PRIMARY KEY,
		s_name            CHAR(25) NOT NULL,
		s_address         VARCHAR(40) NOT NULL,
		s_nationkey       INTEGER NOT NULL,
		s_phone           CHAR(15) NOT NULL,
		s_acctbal         DECIMAL(15,2) NOT NULL,
		s_comment         VARCHAR(101) NOT NULL,
		INDEX s_nk (s_nationkey ASC)
	)`
	tpchPartSuppSchema = `(
		ps_partkey            INTEGER NOT NULL,
		ps_suppkey            INTEGER NOT NULL,
		ps_availqty           INTEGER NOT NULL,
		ps_supplycost         DECIMAL(15,2) NOT NULL,
		ps_comment            VARCHAR(199) NOT NULL,
		INDEX ps_sk (ps_suppkey ASC),
		PRIMARY KEY (ps_partkey ASC, ps_suppkey ASC),
		UNIQUE INDEX ps_sk_pk (ps_suppkey ASC, ps_partkey ASC)
	)`
	tpchCustomerSchema = `(
		c_custkey         INTEGER NOT NULL PRIMARY KEY,
		c_name            VARCHAR(25) NOT NULL,
		c_address         VARCHAR(40) NOT NULL,
		c_nationkey       INTEGER NOT NULL,
		c_phone           CHAR(15) NOT NULL,
		c_acctbal         DECIMAL(15,2)   NOT NULL,
		c_mktsegment      CHAR(10) NOT NULL,
		c_comment         VARCHAR(117) NOT NULL,
		INDEX c_nk (c_nationkey ASC)
	)`
	tpchOrdersSchema = `(
		o_orderkey           INTEGER NOT NULL PRIMARY KEY,
		o_custkey            INTEGER NOT NULL,
		o_orderstatus        CHAR(1) NOT NULL,
		o_totalprice         DECIMAL(15,2) NOT NULL,
		o_orderdate          DATE NOT NULL,
		o_orderpriority      CHAR(15) NOT NULL,
		o_clerk              CHAR(15) NOT NULL,
		o_shippriority       INTEGER NOT NULL,
		o_comment            VARCHAR(79) NOT NULL,
		INDEX o_ck (o_custkey ASC),
		INDEX o_od (o_orderdate ASC)
	)`
	tpchLineItemSchema = `(
		l_orderkey      INTEGER NOT NULL,
		l_partkey       INTEGER NOT NULL,
		l_suppkey       INTEGER NOT NULL,
		l_linenumber    INTEGER NOT NULL,
		l_quantity      DECIMAL(15,2) NOT NULL,
		l_extendedprice DECIMAL(15,2) NOT NULL,
		l_discount      DECIMAL(15,2) NOT NULL,
		l_tax           DECIMAL(15,2) NOT NULL,
		l_returnflag    CHAR(1) NOT NULL,
		l_linestatus    CHAR(1) NOT NULL,
		l_shipdate      DATE NOT NULL,
		l_commitdate    DATE NOT NULL,
		l_receiptdate   DATE NOT NULL,
		l_shipinstruct  CHAR(25) NOT NULL,
		l_shipmode      CHAR(10) NOT NULL,
		l_comment       VARCHAR(44) NOT NULL,
		PRIMARY KEY (l_orderkey, l_linenumber),
		INDEX l_ok (l_orderkey ASC),
		INDEX l_pk (l_partkey ASC),
		INDEX l_sk (l_suppkey ASC),
		INDEX l_sd (l_shipdate ASC),
		INDEX l_cd (l_commitdate ASC),
		INDEX l_rd (l_receiptdate ASC),
		INDEX l_pk_sk (l_partkey ASC, l_suppkey ASC),
		INDEX l_sk_pk (l_suppkey ASC, l_partkey ASC)
	)`
)
