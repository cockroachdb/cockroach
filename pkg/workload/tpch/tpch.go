// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpch

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
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

	distsql       bool
	disableChecks bool

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
		g.flags.FlagSet = pflag.NewFlagSet(`tpch`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`queries`:        {RuntimeOnly: true},
			`dist-sql`:       {RuntimeOnly: true},
			`disable-checks`: {RuntimeOnly: true},
		}
		g.flags.Int64Var(&g.seed, `seed`, 1, `Random number generator seed`)
		g.flags.IntVar(&g.scaleFactor, `scale-factor`, 1,
			`Linear scale of how much data to use (each SF is ~1GB)`)
		g.flags.StringVar(&g.queriesRaw, `queries`, `1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22`,
			`Queries to run. Use a comma separated list of query numbers`)
		g.flags.BoolVar(&g.distsql, `dist-sql`, true, `Use DistSQL for query execution`)
		g.flags.BoolVar(&g.disableChecks, `disable-checks`, false,
			"Disable checking the output against the expected rows (default false). "+
				"Note that the checks are only supported for scale factor 1")
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
			if w.scaleFactor != 1 {
				fmt.Printf("check for expected rows is only supported with " +
					"scale factor 1, so it was disabled\n")
				w.disableChecks = true
			}
			for _, queryName := range strings.Split(w.queriesRaw, `,`) {
				if _, ok := queriesByName[queryName]; !ok {
					return errors.Errorf(`unknown query: %s`, queryName)
				}
				if !queriesToSkip[queryName] {
					w.selectedQueries = append(w.selectedQueries, queryName)
				}
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
func (w *tpch) Ops(urls []string, reg *histogram.Registry) (workload.QueryLoad, error) {
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
	hists  *histogram.Histograms
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

	vals := make([]interface{}, maxCols)
	for i := range vals {
		vals[i] = new(interface{})
	}

	start := timeutil.Now()
	rows, err := w.db.Query(query)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return err
	}
	var numRows int
	for rows.Next() {
		if !w.config.disableChecks {
			if !queriesToCheckOnlyNumRows[queryName] && !queriesToSkip[queryName] {
				if err = rows.Scan(vals[:numColsByQueryName[queryName]]...); err != nil {
					return err
				}

				expectedRow := expectedRowsByQueryName[queryName][numRows]
				for i, expectedValue := range expectedRow {
					if val := *vals[i].(*interface{}); val != nil {
						if strings.Compare(expectedValue, fmt.Sprint(val)) != 0 {
							return errors.Errorf("wrong result on query %s in row %d in column %d: got %q, expected %q",
								queryName, numRows, i, fmt.Sprint(val), expectedValue)
						}
					}
				}
			}
		}
		numRows++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !w.config.disableChecks {
		if !queriesToSkip[queryName] {
			if numRows != numExpectedRowsByQueryName[queryName] {
				return errors.Errorf("query %s returned wrong number of rows: got %d, expected %d", queryName, numRows, numExpectedRowsByQueryName[queryName])
			}
		}
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
		p_retailprice     FLOAT NOT NULL,
		p_comment         VARCHAR(23) NOT NULL
	)`
	tpchSupplierSchema = `(
		s_suppkey         INTEGER NOT NULL PRIMARY KEY,
		s_name            CHAR(25) NOT NULL,
		s_address         VARCHAR(40) NOT NULL,
		s_nationkey       INTEGER NOT NULL,
		s_phone           CHAR(15) NOT NULL,
		s_acctbal         FLOAT NOT NULL,
		s_comment         VARCHAR(101) NOT NULL,
		INDEX s_nk (s_nationkey ASC)
	)`
	tpchPartSuppSchema = `(
		ps_partkey            INTEGER NOT NULL,
		ps_suppkey            INTEGER NOT NULL,
		ps_availqty           INTEGER NOT NULL,
		ps_supplycost         FLOAT NOT NULL,
		ps_comment            VARCHAR(199) NOT NULL,
		PRIMARY KEY (ps_partkey ASC, ps_suppkey ASC),
		INDEX ps_sk (ps_suppkey ASC)
	)`
	tpchCustomerSchema = `(
		c_custkey         INTEGER NOT NULL PRIMARY KEY,
		c_name            VARCHAR(25) NOT NULL,
		c_address         VARCHAR(40) NOT NULL,
		c_nationkey       INTEGER NOT NULL,
		c_phone           CHAR(15) NOT NULL,
		c_acctbal         FLOAT NOT NULL,
		c_mktsegment      CHAR(10) NOT NULL,
		c_comment         VARCHAR(117) NOT NULL,
		INDEX c_nk (c_nationkey ASC)
	)`
	tpchOrdersSchema = `(
		o_orderkey           INTEGER NOT NULL PRIMARY KEY,
		o_custkey            INTEGER NOT NULL,
		o_orderstatus        CHAR(1) NOT NULL,
		o_totalprice         FLOAT NOT NULL,
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
		l_quantity      FLOAT NOT NULL,
		l_extendedprice FLOAT NOT NULL,
		l_discount      FLOAT NOT NULL,
		l_tax           FLOAT NOT NULL,
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
