// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

var tpccChecksMeta = workload.Meta{
	Name:        `tpcc-checks`,
	Description: `tpcc-checks runs the TPC-C consistency checks as a workload.`,
	Details: `It is primarily intended as a tool to create an overload scenario.
An --as-of flag is exposed to prevent the work from interfering with a
foreground TPC-C workload`,
	Version: `1.0.0`,
	New: func() workload.Generator {
		g := &tpccChecks{}
		g.flags.FlagSet = pflag.NewFlagSet(`tpcc`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`db`:          {RuntimeOnly: true},
			`concurrency`: {RuntimeOnly: true},
			`as-of`:       {RuntimeOnly: true},
		}
		g.flags.IntVar(&g.concurrency, `concurrency`, 1,
			`Number of concurrent workers. Defaults to 1.`,
		)
		g.flags.StringVar(&g.asOfSystemTime, "as-of", "",
			"Timestamp at which the query should be run."+
				" If non-empty the provided value will be used as the expression in an"+
				" AS OF SYSTEM TIME CLAUSE for all checks.")
		checkNames := func() (checkNames []string) {
			for _, c := range tpcc.AllChecks() {
				checkNames = append(checkNames, c.Name)
			}
			return checkNames
		}()
		g.flags.StringSliceVar(&g.checks, "checks", checkNames,
			"Name of checks to be run.")
		g.connFlags = workload.NewConnFlags(&g.flags)
		{ // Set the dbOveride to default to "tpcc".
			dbOverrideFlag := g.flags.Lookup(`db`)
			dbOverrideFlag.DefValue = `tpcc`
			if err := dbOverrideFlag.Value.Set(`tpcc`); err != nil {
				panic(err)
			}
		}
		return g
	},
}

func (w *tpccChecks) Flags() workload.Flags {
	return w.flags
}

func init() {
	workload.Register(tpccChecksMeta)
}

type tpccChecks struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	asOfSystemTime string
	checks         []string
	concurrency    int
}

// The tables should already exist, if they do not an error will occur later.
func (*tpccChecks) Tables() []workload.Table {
	return nil
}

func (*tpccChecks) Meta() workload.Meta {
	return tpccChecksMeta
}

// Ops implements the Opser interface.
func (w *tpccChecks) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(w, w.flags.Lookup("db").Value.String(), urls)
	if err != nil {
		return workload.QueryLoad{}, fmt.Errorf("%v", err)
	}
	dbs := make([]*gosql.DB, len(urls))
	for i, url := range urls {
		dbs[i], err = gosql.Open(`cockroach`, url)
		if err != nil {
			return workload.QueryLoad{}, errors.Wrapf(err, "failed to dial %s", url)
		}
		// Set the maximum number of open connections to 3x the concurrency because
		// that's the maximum number of connections used by any check at once.
		dbs[i].SetMaxOpenConns(3 * w.concurrency)
		dbs[i].SetMaxIdleConns(3 * w.concurrency)
	}
	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	ql.WorkerFns = make([]func(context.Context) error, w.concurrency)
	checks, err := filterChecks(tpcc.AllChecks(), w.checks)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	for i := range ql.WorkerFns {
		worker := newCheckWorker(dbs, checks, reg.GetHandle(), w.asOfSystemTime)
		ql.WorkerFns[i] = worker.run
	}
	// Preregister all of the histograms so they always print.
	for _, c := range checks {
		reg.GetHandle().Get(c.Name)
	}
	return ql, nil
}

type checkWorker struct {
	dbs            []*gosql.DB
	checks         []tpcc.Check
	histograms     *histogram.Histograms
	asOfSystemTime string
	dbPerm         []int
	checkPerm      []int
	i              int
}

func newCheckWorker(
	dbs []*gosql.DB, checks []tpcc.Check, histograms *histogram.Histograms, asOfSystemTime string,
) *checkWorker {
	return &checkWorker{
		dbs:            dbs,
		checks:         checks,
		histograms:     histograms,
		asOfSystemTime: asOfSystemTime,
		dbPerm:         rand.Perm(len(dbs)),
		checkPerm:      rand.Perm(len(checks)),
	}
}

func (w *checkWorker) run(ctx context.Context) error {
	defer func() { w.i++ }()
	c := w.checks[w.checkPerm[w.i%len(w.checks)]]
	db := w.dbs[w.dbPerm[w.i%len(w.dbs)]]
	start := timeutil.Now()
	if err := c.Fn(db, w.asOfSystemTime); err != nil {
		return errors.Wrapf(err, "failed check %s", c.Name)
	}
	w.histograms.Get(c.Name).Record(timeutil.Since(start))
	return nil
}

// filterChecks removes all elements from checks which do not have their name
// in toRun. An error is returned if any elements of toRun do not exist in
// checks. The checks slice is modified in place and returned.
func filterChecks(checks []tpcc.Check, toRun []string) ([]tpcc.Check, error) {
	toRunSet := make(map[string]struct{}, len(toRun))
	for _, s := range toRun {
		toRunSet[s] = struct{}{}
	}
	filtered := checks[:0]
	for _, c := range checks {
		if _, exists := toRunSet[c.Name]; exists {
			filtered = append(filtered, c)
			delete(toRunSet, c.Name)
		}
	}
	if len(toRunSet) > 0 {
		return nil, fmt.Errorf("cannot run checks %v which do not exist", toRun)
	}
	return filtered, nil
}
