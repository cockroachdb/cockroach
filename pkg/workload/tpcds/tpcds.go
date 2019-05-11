// Copyright 2019 The Cockroach Authors.
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

package tpcds

import (
	"context"
	gosql "database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

type tpcds struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	queriesToRunRaw  string
	queriesToOmitRaw string
	queryTimeLimit   time.Duration
	selectedQueries  []int
}

func init() {
	workload.Register(tpcdsMeta)
}

var tpcdsMeta = workload.Meta{
	Name:        `tpcds`,
	Description: `TPC-DS is a read-only workload of "decision support" queries on large datasets.`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &tpcds{}
		g.flags.FlagSet = pflag.NewFlagSet(`tpcds`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`queries-to-omit`:  {RuntimeOnly: true},
			`queries-to-run`:   {RuntimeOnly: true},
			`query-time-limit`: {RuntimeOnly: true},
		}

		// NOTE: we're skipping queries 27, 36, 70, and 86 by default at the moment
		// because they require some modifications.
		g.flags.StringVar(&g.queriesToOmitRaw, `queries-to-omit`,
			`27,36,70,86`,
			`Queries not to run (i.e. all others will be run). Use a comma separated list of query numbers`)
		g.flags.StringVar(&g.queriesToRunRaw, `queries`,
			``,
			`Queries to run. Use a comma separated list of query numbers. If omitted, all queries are run`)
		g.flags.DurationVar(&g.queryTimeLimit, `query-time-limit`, 5*time.Minute,
			`Time limit for a single run of a query`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*tpcds) Meta() workload.Meta { return tpcdsMeta }

// Flags implements the Flagser interface.
func (w *tpcds) Flags() workload.Flags { return w.flags }

// Hooks implements the Hookser interface.
func (w *tpcds) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if w.queryTimeLimit < 0 {
				return errors.Errorf("negative query time limit was set %d", w.queryTimeLimit)
			}
			skipQuery := make([]bool, numQueries+1)
			for _, queryName := range strings.Split(w.queriesToOmitRaw, `,`) {
				queryNum, err := strconv.Atoi(queryName)
				if err != nil {
					return err
				}
				if queryNum < 1 || queryNum > numQueries {
					return errors.Errorf("unknown query %d (only queries in range [1, %d] are supported)",
						queryNum, numQueries)
				}
				skipQuery[queryNum] = true
			}
			if w.queriesToRunRaw != `` {
				for _, queryName := range strings.Split(w.queriesToRunRaw, `,`) {
					queryNum, err := strconv.Atoi(queryName)
					if err != nil {
						return err
					}
					if _, ok := queriesByNumber[queryNum]; !ok {
						return errors.Errorf(`unknown query: %s (probably, the query needs modifications, `+
							`so it is disabled for now)`, queryName)
					}
					if !skipQuery[queryNum] {
						w.selectedQueries = append(w.selectedQueries, queryNum)
					}
				}
				return nil
			}
			for queryNum := 1; queryNum <= numQueries; queryNum++ {
				if !skipQuery[queryNum] {
					w.selectedQueries = append(w.selectedQueries, queryNum)
				}
			}
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (w *tpcds) Tables() []workload.Table { return []workload.Table{} }

// Ops implements the Opser interface.
func (w *tpcds) Ops(urls []string, reg *histogram.Registry) (workload.QueryLoad, error) {
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
			db:     db,
		}
		ql.WorkerFns = append(ql.WorkerFns, worker.run)
	}
	return ql, nil
}

type worker struct {
	config *tpcds
	db     *gosql.DB
	ops    int
}

func (w *worker) run(ctx context.Context) error {
	queryNum := w.config.selectedQueries[w.ops%len(w.config.selectedQueries)]
	w.ops++

	query := queriesByNumber[queryNum]

	var rows *gosql.Rows
	var err error
	start := timeutil.Now()
	if w.config.queryTimeLimit > 0 {
		ch := make(chan interface{}, 2)
		go func() {
			rows, err = w.db.Query(query)
			ch <- false
		}()
		go func() {
			time.Sleep(w.config.queryTimeLimit)
			ch <- true
		}()
		isTimeout := <-ch
		if isTimeout.(bool) {
			log.Infof(ctx, "[Q%d] didn't complete in %4.2f seconds", queryNum, w.config.queryTimeLimit.Seconds())
			// TODO(yuzefovich): how do I cancel the running query here?
			return nil
		} else {
			// TODO(yuzefovich): how do I finish the sleeping goroutine?
		}
	} else {
		rows, err = w.db.Query(query)
	}
	if err != nil {
		log.Infof(ctx, "[Q%d] error: %s", queryNum, err)
		return err
	}
	var numRows int
	for rows.Next() {
		numRows++
	}
	if err := rows.Err(); err != nil {
		log.Infof(ctx, "[Q%d] error: %s", queryNum, err)
		return err
	}
	elapsed := timeutil.Since(start)
	log.Infof(ctx, "[Q%d] return %d rows after %4.2f seconds",
		queryNum, numRows, elapsed.Seconds())
	return nil
}
