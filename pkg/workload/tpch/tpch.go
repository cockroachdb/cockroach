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
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"golang.org/x/exp/rand"
)

const (
	numNation           = 25
	numRegion           = 5
	numPartPerSF        = 200000
	numPartSuppPerPart  = 4
	numSupplierPerSF    = 10000
	numCustomerPerSF    = 150000
	numOrderPerCustomer = 10
	numLineItemPerSF    = 6001215
)

// wrongOutputError indicates that incorrect results were returned for one of
// the TPCH queries.
type wrongOutputError struct {
	error
}

// TPCHWrongOutputErrorPrefix is the string that all errors about the wrong
// output will be prefixed with.
const TPCHWrongOutputErrorPrefix = "TPCH wrong output "

func (e wrongOutputError) Error() string {
	return TPCHWrongOutputErrorPrefix + e.error.Error()
}

type tpch struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed        uint64
	scaleFactor int
	fks         bool

	disableChecks              bool
	vectorize                  string
	useClusterVectorizeSetting bool
	verbose                    bool

	queriesRaw      string
	selectedQueries []int

	textPool   textPool
	localsPool *sync.Pool
}

func init() {
	workload.Register(tpchMeta)
}

// FromScaleFactor returns a tpch generator pre-configured with the specified
// scale factor.
func FromScaleFactor(scaleFactor int) workload.Generator {
	return workload.FromFlags(tpchMeta, fmt.Sprintf(`--scale-factor=%d`, scaleFactor))
}

var tpchMeta = workload.Meta{
	Name:        `tpch`,
	Description: `TPC-H is a read-only workload of "analytics" queries on large datasets.`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &tpch{}
		g.flags.FlagSet = pflag.NewFlagSet(`tpch`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`queries`:       {RuntimeOnly: true},
			`dist-sql`:      {RuntimeOnly: true},
			`enable-checks`: {RuntimeOnly: true},
			`vectorize`:     {RuntimeOnly: true},
		}
		g.flags.Uint64Var(&g.seed, `seed`, 1, `Random number generator seed`)
		g.flags.IntVar(&g.scaleFactor, `scale-factor`, 1,
			`Linear scale of how much data to use (each SF is ~1GB)`)
		g.flags.BoolVar(&g.fks, `fks`, true, `Add the foreign keys`)
		g.flags.StringVar(&g.queriesRaw, `queries`,
			`1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22`,
			`Queries to run. Use a comma separated list of query numbers`)
		g.flags.BoolVar(&g.disableChecks, `enable-checks`, false,
			"Enable checking the output against the expected rows (default false). "+
				"Note that the checks are only supported for scale factor 1 of the backup "+
				"stored at 'gs://cockroach-fixtures/workload/tpch/scalefactor=1/backup'")
		g.flags.StringVar(&g.vectorize, `vectorize`, `on`,
			`Set vectorize session variable`)
		g.flags.BoolVar(&g.useClusterVectorizeSetting, `default-vectorize`, false,
			`Ignore vectorize option and use the current cluster setting sql.defaults.vectorize`)
		g.flags.BoolVar(&g.verbose, `verbose`, false,
			`Prints out the queries being run as well as histograms`)
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
				queryNum, err := strconv.Atoi(queryName)
				if err != nil {
					return err
				}
				if _, ok := QueriesByNumber[queryNum]; !ok {
					return errors.Errorf(`unknown query: %s`, queryName)
				}
				w.selectedQueries = append(w.selectedQueries, queryNum)
			}
			return nil
		},
		PostLoad: func(db *gosql.DB) error {
			if w.fks {
				// We avoid validating foreign keys because we just generated the data
				// set and don't want to scan over the entire thing again.
				// Unfortunately, this means that we leave the foreign keys unvalidated
				// for the duration of the test, so the SQL optimizer can't use them.
				//
				// TODO(lucy-zhang): expose an internal knob to validate fk relations
				// without performing full validation. See #38833.
				fkStmts := []string{
					`ALTER TABLE nation ADD CONSTRAINT nation_fkey_region FOREIGN KEY (n_regionkey) REFERENCES region (r_regionkey) NOT VALID`,
					`ALTER TABLE supplier ADD CONSTRAINT supplier_fkey_nation FOREIGN KEY (s_nationkey) REFERENCES nation (n_nationkey) NOT VALID`,
					`ALTER TABLE partsupp ADD CONSTRAINT partsupp_fkey_part FOREIGN KEY (ps_partkey) REFERENCES part (p_partkey) NOT VALID`,
					`ALTER TABLE partsupp ADD CONSTRAINT partsupp_fkey_supplier FOREIGN KEY (ps_suppkey) REFERENCES supplier (s_suppkey) NOT VALID`,
					`ALTER TABLE customer ADD CONSTRAINT customer_fkey_nation FOREIGN KEY (c_nationkey) REFERENCES nation (n_nationkey) NOT VALID`,
					`ALTER TABLE orders ADD CONSTRAINT orders_fkey_customer FOREIGN KEY (o_custkey) REFERENCES customer (c_custkey) NOT VALID`,
					`ALTER TABLE lineitem ADD CONSTRAINT lineitem_fkey_orders FOREIGN KEY (l_orderkey) REFERENCES orders (o_orderkey) NOT VALID`,
					`ALTER TABLE lineitem ADD CONSTRAINT lineitem_fkey_part FOREIGN KEY (l_partkey) REFERENCES part (p_partkey) NOT VALID`,
					`ALTER TABLE lineitem ADD CONSTRAINT lineitem_fkey_supplier FOREIGN KEY (l_suppkey) REFERENCES supplier (s_suppkey) NOT VALID`,
					// TODO(andyk): This fails with `pq: column "l_partkey" cannot be used
					// by multiple foreign key constraints`. This limitation would appear
					// to violate TPCH rules, as all foreign keys must be defined, or none
					// at all.
					// `ALTER TABLE lineitem ADD CONSTRAINT lineitem_fkey_partsupp FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp (ps_partkey, ps_suppkey) NOT VALID`,
				}

				for _, fkStmt := range fkStmts {
					if _, err := db.Exec(fkStmt); err != nil {
						// If the statement failed because the fk already exists, ignore it.
						// Return the error for any other reason.
						const duplFKErr = "columns cannot be used by multiple foreign key constraints"
						if !strings.Contains(err.Error(), duplFKErr) {
							return errors.Wrapf(err, "while executing %s", fkStmt)
						}
					}
				}
			}
			return nil
		},
	}
}

type generateLocals struct {
	rng *rand.Rand

	// namePerm is a slice of ordinals into randPartNames.
	namePerm []int

	orderData *orderSharedRandomData
}

// Tables implements the Generator interface.
func (w *tpch) Tables() []workload.Table {
	if w.localsPool == nil {
		w.localsPool = &sync.Pool{
			New: func() interface{} {
				namePerm := make([]int, len(randPartNames))
				for i := range namePerm {
					namePerm[i] = i
				}
				return &generateLocals{
					rng:      rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano()))),
					namePerm: namePerm,
					orderData: &orderSharedRandomData{
						partKeys:   make([]int, 0, 7),
						shipDates:  make([]int64, 0, 7),
						quantities: make([]float32, 0, 7),
						discount:   make([]float32, 0, 7),
						tax:        make([]float32, 0, 7),
					},
				}
			},
		}
	}

	// TODO(dan): Make this a flag that points at an official pool.txt?
	w.textPool = &fakeTextPool{seed: w.seed}

	nation := workload.Table{
		Name:   `nation`,
		Schema: tpchNationSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: numNation,
			FillBatch:  w.tpchNationInitialRowBatch,
		},
	}
	region := workload.Table{
		Name:   `region`,
		Schema: tpchRegionSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: numRegion,
			FillBatch:  w.tpchRegionInitialRowBatch,
		},
	}
	numSupplier := numSupplierPerSF * w.scaleFactor
	supplier := workload.Table{
		Name:   `supplier`,
		Schema: tpchSupplierSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: numSupplier,
			FillBatch:  w.tpchSupplierInitialRowBatch,
		},
	}
	numPart := numPartPerSF * w.scaleFactor
	part := workload.Table{
		Name:   `part`,
		Schema: tpchPartSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: numPart,
			FillBatch:  w.tpchPartInitialRowBatch,
		},
	}
	partsupp := workload.Table{
		Name:   `partsupp`,
		Schema: tpchPartSuppSchema,
		InitialRows: workload.BatchedTuples{
			// 1 batch per part, hence numPartPerSF and not numPartSuppPerSF.
			NumBatches: numPart,
			FillBatch:  w.tpchPartSuppInitialRowBatch,
		},
	}
	numCustomer := numCustomerPerSF * w.scaleFactor
	customer := workload.Table{
		Name:   `customer`,
		Schema: tpchCustomerSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: numCustomer,
			FillBatch:  w.tpchCustomerInitialRowBatch,
		},
	}
	orders := workload.Table{
		Name:   `orders`,
		Schema: tpchOrdersSchema,
		InitialRows: workload.BatchedTuples{
			// 1 batch per customer.
			NumBatches: numCustomer,
			FillBatch:  w.tpchOrdersInitialRowBatch,
		},
	}
	lineitem := workload.Table{
		Name:   `lineitem`,
		Schema: tpchLineItemSchema,
		InitialRows: workload.BatchedTuples{
			// 1 batch per customer.
			NumBatches: numCustomer,
			FillBatch:  w.tpchLineItemInitialRowBatch,
		},
	}

	return []workload.Table{
		nation, region, part, supplier, partsupp, customer, orders, lineitem,
	}
}

// Ops implements the Opser interface.
func (w *tpch) Ops(
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
	queryNum := w.config.selectedQueries[w.ops%len(w.config.selectedQueries)]
	w.ops++

	var prefix string
	if !w.config.useClusterVectorizeSetting {
		prefix = fmt.Sprintf("SET vectorize = '%s';", w.config.vectorize)
	}
	query := fmt.Sprintf("%s %s", prefix, QueriesByNumber[queryNum])

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
		return errors.Errorf("[q%d]: %s", queryNum, err)
	}
	var numRows int
	// NOTE: we should *NOT* return an error from this function right away
	// because we might get another, more meaningful error from rows.Err() which
	// can only be accessed after we fully consumed the rows.
	checkExpectedOutput := func() error {
		for rows.Next() {
			if !w.config.disableChecks {
				if _, checkOnlyRowCount := numExpectedRowsByQueryNumber[queryNum]; !checkOnlyRowCount {
					if err = rows.Scan(vals[:numColsByQueryNumber[queryNum]]...); err != nil {
						return errors.Errorf("[q%d]: %s", queryNum, err)
					}

					expectedRow := expectedRowsByQueryNumber[queryNum][numRows]
					for i, expectedValue := range expectedRow {
						if val := *vals[i].(*interface{}); val != nil {
							var actualValue string
							// Currently, lib/pq for query 12 in the second and third columns
							// (which are decimals) returns []byte. In order to compare it
							// against our expected string value, we have this special case.
							if byteArray, ok := val.([]byte); ok {
								actualValue = string(byteArray)
							} else {
								actualValue = fmt.Sprint(val)
							}
							if strings.Compare(expectedValue, actualValue) != 0 {
								var expectedFloat, actualFloat float64
								var expectedFloatRounded, actualFloatRounded float64
								expectedFloat, err = strconv.ParseFloat(expectedValue, 64)
								if err != nil {
									return errors.Errorf("[q%d] failed parsing expected value as float64 with %s\n"+
										"wrong result in row %d in column %d: got %q, expected %q",
										queryNum, err, numRows, i, actualValue, expectedValue)
								}
								actualFloat, err = strconv.ParseFloat(actualValue, 64)
								if err != nil {
									return errors.Errorf("[q%d] failed parsing actual value as float64 with %s\n"+
										"wrong result in row %d in column %d: got %q, expected %q",
										queryNum, err, numRows, i, actualValue, expectedValue)
								}
								// TPC-H spec requires 0.01 precision for DECIMALs, so we will
								// first round the values to use in the comparison. Note that we
								// round to a thousandth so that values like 0.601 and 0.609 were
								// always considered to differ by less than 0.01 (due to the
								// nature of representation of floats, it is possible that those
								// two values when rounded to a hundredth would be represented as
								// something like 0.59999 and 0.610001 which differ by more than
								// 0.01).
								expectedFloatRounded, err = strconv.ParseFloat(fmt.Sprintf("%.3f", expectedFloat), 64)
								if err != nil {
									return errors.Errorf("[q%d] failed parsing rounded expected value as float64 with %s\n"+
										"wrong result in row %d in column %d: got %q, expected %q",
										queryNum, err, numRows, i, actualValue, expectedValue)
								}
								actualFloatRounded, err = strconv.ParseFloat(fmt.Sprintf("%.3f", actualFloat), 64)
								if err != nil {
									return errors.Errorf("[q%d] failed parsing rounded actual value as float64 with %s\n"+
										"wrong result in row %d in column %d: got %q, expected %q",
										queryNum, err, numRows, i, actualValue, expectedValue)
								}
								if math.Abs(expectedFloatRounded-actualFloatRounded) > 0.02 {
									// We only fail the check if the difference is more than 0.02
									// although TPC-H spec requires 0.01 precision for DECIMALs. We
									// are using the expected value that might not be "precisely
									// correct." It is possible for the following situation to
									// occur:
									//   expected < "ideal" < actual
									//   "ideal" - expected < 0.01 && actual - "ideal" < 0.01
									// so in the worst case, actual and expected might differ by
									// 0.02 and still be considered correct.
									return errors.Errorf("[q%d] %f and %f differ by more than 0.02\n"+
										"wrong result in row %d in column %d: got %q, expected %q",
										queryNum, actualFloatRounded, expectedFloatRounded,
										numRows, i, actualValue, expectedValue)
								}
							}
						}
					}
				}
			}
			numRows++
		}
		return nil
	}

	expectedOutputError := checkExpectedOutput()

	// In order to definitely get the error below, we need to fully consume the
	// result set.
	for rows.Next() {
	}

	// We first check whether there is any error that came from the server (for
	// example, an out of memory error). If there is, we return it.
	if err := rows.Err(); err != nil {
		return errors.Errorf("[q%d]: %s", queryNum, err)
	}
	// Now we check whether there was an error while consuming the rows.
	if expectedOutputError != nil {
		return wrongOutputError{error: expectedOutputError}
	}
	if !w.config.disableChecks {
		numRowsExpected, checkOnlyRowCount := numExpectedRowsByQueryNumber[queryNum]
		if checkOnlyRowCount && numRows != numRowsExpected {
			return wrongOutputError{
				error: errors.Errorf(
					"[q%d] returned wrong number of rows: got %d, expected %d",
					queryNum, numRows, numRowsExpected,
				)}
		}
	}
	elapsed := timeutil.Since(start)
	if w.config.verbose {
		w.hists.Get(fmt.Sprintf("%d", queryNum)).Record(elapsed)
		// Note: if you are changing the output format here, please change the
		// regex in roachtest/tpchvec.go accordingly.
		log.Infof(ctx, "[q%d] returned %d rows after %4.2f seconds:\n%s",
			queryNum, numRows, elapsed.Seconds(), query)
	} else {
		// Note: if you are changing the output format here, please change the
		// regex in roachtest/tpchvec.go accordingly.
		log.Infof(ctx, "[q%d] returned %d rows after %4.2f seconds",
			queryNum, numRows, elapsed.Seconds())
	}
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
