// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opbench_test

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opbench"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/opttester"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var pgurl = flag.String("url", "postgresql://localhost:26257/tpch?sslmode=disable&user=root", "the url to connect to")

const rewriteEstimatedFlag = "opbench-rewrite-estimated"
const rewriteActualFlag = "opbench-rewrite-actual"

var rewriteEstimated = flag.Bool(rewriteEstimatedFlag, false, "re-calculate the estimated costs for each Plan")
var rewriteActual = flag.Bool(rewriteActualFlag, false, "re-measure the runtime for each Plan")

// TODO(justin): we need a system to get consistent re-measurements of all the
// "actual" results. We will probably want some ability to kick off a roachprod
// cluster which will crunch through them all. It's still valuable to be
// able to do individual tests locally for the purposes of iteration.
// TODO(justin): there should be a metadata file that records the setup that
// each measurement was recorded on (what computer, cockroach version, date,
// etc).
// TODO(justin): we should be able to give each test tags, in case you want
// to plot say, every scan spec, or every hash join spec.
// TODO(justin): at some point we will likely want to record data along more
// dimensions (various sizes of node clusters, number CPUs, etc).
// TODO(justin): this should run as a roachtest which keeps track of the
// correlation of estimated and actual over time.
// TODO(justin): these tests currently measure the latency of a query, this
// is not entirely correct. We should measure throughput (somehow), but
// latency is easier for now.

// TestOpBench is a hybrid benchmark/test of the cost model. It supports
// running parameterized pre-specified plans and verifying their estimated
// costs against a CSV.
//
// Similar to the data-driven tests, it supports automatically updating
// test files when the cost model changes when run with the
// -opbench-rewrite-estimated flag.
//
// It also supports actually running the plans against a cluster to measure how
// long they take to run when run with the -opbench-rewrite-actual flag. This
// will run the queries against the url specified by the -url flag (by default
// localhost:26257).
//
// The end result of this is that the resulting CSV files can be imported
// into a plotting program to inspect the correlation, or the correlation
// can be manually verified.
func TestOpBench(t *testing.T) {
	rm := runMode{
		rewriteEstimated: *rewriteEstimated,
		rewriteActual:    *rewriteActual,
	}
	for _, spec := range Benches {
		t.Run(spec.Name, func(t *testing.T) {
			runBench(t, spec, fmt.Sprintf("testdata/%s.csv", spec.Name), rm)
		})
	}
}

// measureQuery runs a query against a running Cockroach cluster and records how
// long it takes to run.
func measureQuery(planText string) (int64, error) {
	db, err := gosql.Open("postgres", *pgurl)
	if err != nil {
		return 0, errors.Wrap(err, "can only recompute actual results when pointed at a running Cockroach cluster")
	}

	ctx := context.Background()

	c, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}

	if _, err := c.ExecContext(ctx, "SET allow_prepare_as_opt_plan = 'on'"); err != nil {
		return 0, err
	}

	// TODO(justin): make this more resilient: good error (or auto-import)
	// if TPCH isn't available.
	if _, err := c.ExecContext(ctx, "USE tpch"); err != nil {
		return 0, err
	}

	// Use a 1 minute timeout.
	// These benchmarks shouldn't go that long generally anyway.
	if _, err := c.ExecContext(ctx, "SET statement_timeout = 60000"); err != nil {
		return 0, err
	}

	text := fmt.Sprintf(`PREPARE p AS OPT PLAN '%s'`, planText)

	_, err = c.ExecContext(ctx, text)
	if err != nil {
		return 0, err
	}

	// TODO(justin): optionally take several measurements and record the
	// mean+stdev.
	start := timeutil.Now().UnixNano()
	if _, err := c.ExecContext(ctx, "EXECUTE p DISCARD ROWS"); err != nil {
		return 0, err
	}
	end := timeutil.Now().UnixNano()
	if _, err := c.ExecContext(ctx, "DEALLOCATE p"); err != nil {
		return 0, err
	}

	return end - start, nil
}

type runMode struct {
	rewriteEstimated bool
	rewriteActual    bool
}

// param is used to keep track of which parameter exists at which
// index in the CSV file.
type param struct {
	idx  int
	name string
}

// getBlankCSV returns an io.Reader to a CSV containing all combinations
// of possible InputNames for the Spec so that the framework can fill in the
// blanks.
func getBlankCSV(spec *opbench.Spec) io.Reader {
	var out bytes.Buffer
	w := csv.NewWriter(&out)

	inputs := spec.InputNames()

	header := append(append([]string(nil), inputs...), "estimated", "actual")
	if err := w.Write(header); err != nil {
		panic(err)
	}

	it := opbench.NewConfigIterator(spec)
	c, ok := it.Next()
	for ok {
		var rec []string
		for _, t := range inputs {
			rec = append(rec, fmt.Sprintf("%d", int(c[t])))
		}
		// Add placeholder values for the estimated and actual.
		rec = append(rec, "0", "0")
		if err := w.Write(rec); err != nil {
			panic(err)
		}
		c, ok = it.Next()
	}
	w.Flush()

	return &out
}

// runBench iterates through a configuration CSV (possibly creating one if it
// doesn't exist) and verifies that the estimated costs for each query did not
// change. It can optionally run in a mode which rewrites the estimated cost,
// the actual runtime, or both.
func runBench(t *testing.T, spec *opbench.Spec, path string, mode runMode) {
	f, err := os.Open(path)
	var r *csv.Reader
	if err != nil {
		if !mode.rewriteEstimated || !mode.rewriteActual {
			t.Fatalf(
				"file %q does not exist, to create it, run with -%s and -%s",
				path,
				rewriteEstimatedFlag,
				rewriteActualFlag,
			)
		}
		r = csv.NewReader(getBlankCSV(spec))
	} else {
		r = csv.NewReader(f)
	}
	defer f.Close()

	headers, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}

	var result bytes.Buffer
	w := csv.NewWriter(&result)
	if err := w.Write(headers); err != nil {
		t.Fatal(err)
	}

	params := make([]param, 0)
	estimatedIdx := -1
	actualIdx := -1
	for i := range headers {
		switch headers[i] {
		case "estimated":
			estimatedIdx = i
		case "actual":
			actualIdx = i
		default:
			params = append(params, param{i, headers[i]})
		}
	}

	// TODO(justin): we should support multiple catalogs, and each query
	// should specify which catalog it pertains to.
	catalog := opbench.MakeTPCHCatalog()

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		conf := opbench.Configuration{}
		newRecord := make([]string, len(record))
		copy(newRecord, record)
		for i := range params {
			val, err := strconv.ParseFloat(record[params[i].idx], 64)
			if err != nil {
				t.Fatal(err)
			}
			conf[params[i].name] = val
		}

		planText := spec.FillInParams(conf)

		// Re-compute the actual cost. We just pass through the old value
		// if not asked to regenerate this.
		if mode.rewriteActual {
			actual, err := measureQuery(planText)
			if err != nil {
				t.Fatal(err)
			}

			seconds := float64(actual) / 1000000000

			newRecord[actualIdx] = fmt.Sprintf("%f", seconds)
		}

		// Compute the estimated cost.

		tester := opttester.New(catalog, planText)
		e, err := tester.Expr()
		if err != nil {
			t.Fatal(err)
		}

		cost := fmt.Sprintf("%f", e.(memo.RelExpr).Cost())

		if mode.rewriteEstimated {
			newRecord[estimatedIdx] = cost
		} else if newRecord[estimatedIdx] != cost {
			t.Errorf(
				"%s/%s:\n  expected: %s\n    actual: %s",
				path,
				conf,
				newRecord[estimatedIdx],
				cost,
			)
		}
		if err := w.Write(newRecord); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()

	if mode.rewriteEstimated || mode.rewriteActual {
		if err := ioutil.WriteFile(path, result.Bytes(), 0644); err != nil {
			t.Fatal(err)
		}
	}
}

// Benches is the set of benchmarks we run.
var Benches = []*opbench.Spec{
	HashJoinSpec,
	MergeJoinSpec,
	LookupJoinSpec,
	SortLineitemSpec,
	ScanOrdersSpec,
	ScanLineitemSpec,
	StreamingGroupByLineitemSpec,
	HashGroupByLineitemSpec,
	StreamingSetOpSpec,
	HashSetOpSpec,
}

// HashJoinSpec does a hash join between supplier and lineitem.
var HashJoinSpec = &opbench.Spec{
	Name: "tpch-hash-join",
	Plan: `
(Root
	(InnerJoin
		(Scan
			[
				(Table "supplier")
				(Cols "s_suppkey")
				(Index "supplier@s_nk")
				(HardLimit $supplier_rows)
			]
		)
		(Scan
			[
				(Table "lineitem")
				(Cols "l_suppkey")
				(Index "lineitem@l_sk")
				(HardLimit $lineitem_rows)
			]
		)
		[
			(Eq (Var "l_suppkey") (Var "s_suppkey"))
		]
		[ ]
	)
	(Presentation "l_suppkey")
	(NoOrdering)
)`,

	Inputs: []opbench.Options{
		{Field: "lineitem_rows", Values: []float64{10, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000, 11000000, 12000000}},
		{Field: "supplier_rows", Values: []float64{10, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000, 16000, 17000, 18000, 19000, 20000}},
	},

	GetParam: func(paramName string, config opbench.Configuration) string {
		switch paramName {
		case "lineitem_rows":
			return fmt.Sprintf("%d", int(config["lineitem_rows"]))
		case "supplier_rows":
			return fmt.Sprintf("%d", int(config["supplier_rows"]))
		}
		panic(errors.AssertionFailedf("can't handle %q", paramName))
	},
}

// MergeJoinSpec does a merge join between supplier and lineitem.
var MergeJoinSpec = &opbench.Spec{
	Name: "tpch-merge-join",
	Plan: `
(Root
	(MergeJoin
		(Scan
			[
				(Table "lineitem")
				(Cols "l_suppkey")
				(Index "lineitem@l_sk")
				(HardLimit $lineitem_rows)
			]
		)
		(Scan
			[
				(Table "supplier")
				(Cols "s_suppkey")
				(HardLimit $supplier_rows)
			]
		)
		[ ]
		[
			(JoinType "inner-join")
			(LeftEq "+l_suppkey")
			(RightEq "+s_suppkey")
			(LeftOrdering "+l_suppkey")
			(RightOrdering "+s_suppkey")
		]
	)
	(Presentation "l_suppkey")
	(NoOrdering)
)`,

	Inputs: []opbench.Options{
		{Field: "lineitem_rows", Values: []float64{10, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000, 11000000, 12000000}},
		{Field: "supplier_rows", Values: []float64{10, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000, 16000, 17000, 18000, 19000, 20000}},
	},

	GetParam: func(paramName string, config opbench.Configuration) string {
		switch paramName {
		case "lineitem_rows":
			return fmt.Sprintf("%d", int(config["lineitem_rows"]))
		case "supplier_rows":
			return fmt.Sprintf("%d", int(config["supplier_rows"]))
		}
		panic(errors.AssertionFailedf("can't handle %q", paramName))
	},
}

// LookupJoinSpec does a lookup join between supplier and lineitem.
var LookupJoinSpec = &opbench.Spec{
	Name: "tpch-lookup-join",
	Plan: `
(Root
	(MakeLookupJoin
		(Scan
			[
				(Table "supplier")
				(Index "supplier@s_nk")
				(Cols "s_suppkey")
				(HardLimit $supplier_rows)
			]
		)
		[
			(JoinType "inner-join")
			(Table "lineitem")
			(Index "lineitem@l_sk")
			(KeyCols "s_suppkey")
			(Cols "l_suppkey")
			(LookupColsAreTableKey "true")
		]
		[
		]
	)
	(Presentation "l_suppkey")
	(NoOrdering)
)`,

	Inputs: []opbench.Options{
		{Field: "supplier_rows", Values: []float64{10, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000, 16000, 17000, 18000, 19000, 20000}},
	},

	GetParam: func(paramName string, config opbench.Configuration) string {
		switch paramName {
		case "supplier_rows":
			return fmt.Sprintf("%d", int(config["supplier_rows"]))
		}
		panic(errors.AssertionFailedf("can't handle %q", paramName))
	},
}

// colPrefix returns a comma-separated list of the first n columns in allCols.
func colPrefix(allCols []string, n int) string {
	var result bytes.Buffer
	for i := 0; i < n; i++ {
		if i > 0 {
			result.WriteByte(',')
		}
		result.WriteString(allCols[i])
	}
	return result.String()
}

func makeScanSpec(
	name string, tableName string, rowCounts []float64, colCounts []float64, colNames []string,
) *opbench.Spec {
	return &opbench.Spec{
		Name: name,
		Plan: fmt.Sprintf(`
(Root
	(Scan
		[
			(Table "%s")
			(Cols "$cols")
			(HardLimit $rows)
		]
	)
	(Presentation "$cols")
	(NoOrdering)
)`, tableName),

		Inputs: []opbench.Options{
			{Field: "rows", Values: rowCounts},
			{Field: "num_cols", Values: colCounts},
		},

		GetParam: func(paramName string, config opbench.Configuration) string {
			switch paramName {
			case "rows":
				return fmt.Sprintf("%d", int(config["rows"]))
			case "cols":
				return colPrefix(colNames, int(config["num_cols"]))
			}
			panic(errors.AssertionFailedf("can't handle %q", paramName))
		},
	}
}

// ScanLineitemSpec scans the lineitem table.
var ScanLineitemSpec = makeScanSpec(
	"scan-lineitem",
	"lineitem",
	[]float64{10, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000, 11000000, 12000000},
	[]float64{1, 2, 3, 4, 16},
	[]string{
		"l_orderkey", "l_partkey", "l_suppkey",
		"l_linenumber", "l_quantity", "l_extendedprice",
		"l_discount", "l_tax", "l_returnflag",
		"l_linestatus", "l_shipdate", "l_commitdate",
		"l_receiptdate", "l_shipinstruct", "l_shipmode",
		"l_comment",
	},
)

// ScanOrdersSpec scans the orders table.
var ScanOrdersSpec = makeScanSpec(
	"scan-orders",
	"orders",
	[]float64{10, 250000, 500000, 750000, 1000000, 1250000, 1500000, 1750000, 2000000, 2250000, 2500000, 2750000, 3000000},
	[]float64{1, 3, 6, 9},
	[]string{
		"o_orderkey", "o_custkey", "o_orderstatus",
		"o_totalprice", "o_orderdate", "o_orderpriority",
		"o_clerk", "o_shippriority", "o_comment",
	},
)

func makeSortSpec(
	name string,
	tableName string,
	rowCounts []float64,
	colCounts []float64,
	colNames []string,
	ordering string,
) *opbench.Spec {
	return &opbench.Spec{
		Name: name,
		Plan: fmt.Sprintf(`
(Root
	(Sort
		(Scan
			[
				(Table "%s")
				(Cols "$cols")
				(HardLimit $rows)
			]
		)
	)
	(Presentation "$cols")
	(OrderingChoice "%s")
)`, tableName, ordering),

		Inputs: []opbench.Options{
			{Field: "rows", Values: rowCounts},
			{Field: "num_cols", Values: colCounts},
		},

		GetParam: func(paramName string, config opbench.Configuration) string {
			switch paramName {
			case "rows":
				return fmt.Sprintf("%d", int(config["rows"]))
			case "cols":
				return colPrefix(colNames, int(config["num_cols"]))
			}
			panic(errors.AssertionFailedf("can't handle %q", paramName))
		},
	}
}

// SortLineitemSpec scans and sorts the lineitem table.
var SortLineitemSpec = makeSortSpec(
	"sort-lineitem",
	"lineitem",
	[]float64{10, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000, 11000000, 12000000},
	[]float64{1, 2, 3},
	[]string{
		"l_orderkey", "l_partkey", "l_suppkey",
		"l_linenumber", "l_quantity", "l_extendedprice",
		"l_discount", "l_tax", "l_returnflag",
		"l_linestatus", "l_shipdate", "l_commitdate",
		"l_receiptdate", "l_shipinstruct", "l_shipmode",
		"l_comment",
	},
	"+l_orderkey",
)

func makeGroupBySpec(
	name string,
	tableName string,
	rowCounts []float64,
	colCounts []float64,
	colNames []string,
	ordering string,
) *opbench.Spec {
	return &opbench.Spec{
		Name: name,
		Plan: fmt.Sprintf(`
(Root
	(GroupBy
		(Scan
			[
				(Table "%s")
				(Cols "$cols")
				(HardLimit $rows)
			]
		)
		[]
		[
      (GroupingCols "$cols")
      (Ordering "%s")
    ]
	)
	(Presentation "$cols")
  (NoOrdering)
)`, tableName, ordering),

		Inputs: []opbench.Options{
			{Field: "rows", Values: rowCounts},
			{Field: "num_cols", Values: colCounts},
		},

		GetParam: func(paramName string, config opbench.Configuration) string {
			switch paramName {
			case "rows":
				return fmt.Sprintf("%d", int(config["rows"]))
			case "cols":
				return colPrefix(colNames, int(config["num_cols"]))
			}
			panic(errors.AssertionFailedf("can't handle %q", paramName))
		},
	}
}

// StreamingGroupByLineitemSpec scans and aggregates the lineitem table using a
// streaming group by.
var StreamingGroupByLineitemSpec = makeGroupBySpec(
	"streaming-group-by-lineitem",
	"lineitem",
	[]float64{10, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000, 11000000, 12000000},
	[]float64{1},
	[]string{
		"l_orderkey", "l_partkey", "l_suppkey",
		"l_linenumber", "l_quantity", "l_extendedprice",
		"l_discount", "l_tax", "l_returnflag",
		"l_linestatus", "l_shipdate", "l_commitdate",
		"l_receiptdate", "l_shipinstruct", "l_shipmode",
		"l_comment",
	},
	"+l_orderkey",
)

// HashGroupByLineitemSpec scans and aggregates the lineitem table using a
// hash group by.
var HashGroupByLineitemSpec = makeGroupBySpec(
	"hash-group-by-lineitem",
	"lineitem",
	[]float64{10, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000, 11000000, 12000000},
	[]float64{2, 3, 4},
	[]string{
		"l_orderkey", "l_partkey", "l_suppkey",
		"l_linenumber", "l_quantity", "l_extendedprice",
		"l_discount", "l_tax", "l_returnflag",
		"l_linestatus", "l_shipdate", "l_commitdate",
		"l_receiptdate", "l_shipinstruct", "l_shipmode",
		"l_comment",
	},
	"",
)

func makeSetOpSpec(
	name string,
	leftTable, rightTable string,
	rowCounts []float64,
	colCounts []float64,
	leftColNames, rightColNames []string,
	ordering string,
) *opbench.Spec {
	return &opbench.Spec{
		Name: name,
		Plan: fmt.Sprintf(`
(Root
  (Intersect
    (Scan
			[
				(Table "%s")
				(Cols "$left_cols")
				(HardLimit $rows)
			]
    )
    (Scan
			[
				(Table "%s")
				(Cols "$right_cols")
				(HardLimit $rows)
			]
    )
  [
    (LeftCols "$left_cols")
    (RightCols "$right_cols")
    (OutCols "$left_cols")
    (Ordering "%s")
  ]
)
	(Presentation "$left_cols")
  (NoOrdering)
)`, leftTable, rightTable, ordering),

		Inputs: []opbench.Options{
			{Field: "rows", Values: rowCounts},
			{Field: "num_cols", Values: colCounts},
		},

		GetParam: func(paramName string, config opbench.Configuration) string {
			switch paramName {
			case "rows":
				return fmt.Sprintf("%d", int(config["rows"]))
			case "left_cols":
				return colPrefix(leftColNames, int(config["num_cols"]))
			case "right_cols":
				return colPrefix(rightColNames, int(config["num_cols"]))
			}
			panic(errors.AssertionFailedf("can't handle %q", paramName))
		},
	}
}

// StreamingSetOpSpec performs a streaming set operation between
// the lineitem and order tables.
var StreamingSetOpSpec = makeSetOpSpec(
	"streaming-set-op",
	"lineitem", "orders",
	[]float64{10, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000, 11000000, 12000000},
	[]float64{1},
	[]string{
		"l_orderkey", "l_partkey", "l_suppkey",
		"l_linenumber", "l_quantity", "l_extendedprice",
		"l_discount", "l_tax", "l_returnflag",
		"l_linestatus", "l_shipdate", "l_commitdate",
		"l_receiptdate", "l_shipinstruct", "l_shipmode",
		"l_comment",
	},
	[]string{
		"o_orderkey", "o_custkey", "o_orderstatus",
		"o_totalprice", "o_orderdate", "o_orderpriority",
		"o_clerk", "o_shippriority", "o_comment",
	},
	"+l_orderkey",
)

// HashSetOpSpec performs a streaming set operation between
// the lineitem and order tables.
var HashSetOpSpec = makeSetOpSpec(
	"hash-set-op",
	"lineitem", "orders",
	[]float64{10, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000, 11000000, 12000000},
	[]float64{1, 2},
	[]string{
		"l_orderkey", "l_partkey", "l_suppkey",
		"l_linenumber", "l_quantity", "l_extendedprice",
		"l_discount", "l_tax", "l_returnflag",
		"l_linestatus", "l_shipdate", "l_commitdate",
		"l_receiptdate", "l_shipinstruct", "l_shipmode",
		"l_comment",
	},
	[]string{
		"o_orderkey", "o_custkey", "o_orderstatus",
		"o_totalprice", "o_orderdate", "o_orderpriority",
		"o_clerk", "o_shippriority", "o_comment",
	},
	"",
)
