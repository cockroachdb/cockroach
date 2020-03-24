// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slow

import (
	"context"
	gosql "database/sql"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	slowSchema = `(
		ts TIMESTAMPTZ
	)`

	defaultRows  = 10
	defaultDelay = 1
)

type slow struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	rows, delay int
}

func init() {
	workload.Register(slowMeta)
}

var slowMeta = workload.Meta{
	Name:         `slow`,
	Description:  `Slow models a slow producer that creates a row after delay seconds`,
	Version:      `1.0.0`,
	PublicFacing: false,
	New: func() workload.Generator {
		g := &slow{}
		g.flags.FlagSet = pflag.NewFlagSet(`slow`, pflag.ContinueOnError)
		g.flags.IntVar(&g.rows, `rows`, defaultRows, `Number of rows to produce.`)
		g.flags.IntVar(&g.delay, `delay`, 0, /* no delay to prevent test timeout */
			`Delay in seconds before producing next row.`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (s *slow) Meta() workload.Meta { return slowMeta }

// Flags implements the Flagser interface.
func (s *slow) Flags() workload.Flags { return s.flags }

// Hooks implements the Hookser interface.
func (s *slow) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if s.delay < 0 {
				return errors.Errorf(`Value of delay must be zero or greater; was %d`, s.delay)
			}
			return nil
		},
	}
}

var slowColTypes = []coltypes.T{coltypes.Timestamp}

// Tables implements the Generator interface.
func (s *slow) Tables() []workload.Table {
	table := workload.Table{
		Name:   `slow`,
		Schema: slowSchema,
		InitialRows: workload.BatchedTuples{
			NumBatches: s.rows,
			FillBatch: func(batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
				cb.Reset(slowColTypes, 1)
				tsCol := cb.ColVec(0).Timestamp()
				time.Sleep(time.Duration(s.delay) * time.Second)
				tsCol[0] = timeutil.Now()
			},
		},
	}
	return []workload.Table{table}
}

// Ops implements the Opser interface.
func (s *slow) Ops(urls []string, reg *histogram.Registry) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(s, s.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}

	insertStmt, err := db.Prepare(`INSERT INTO slow VALUES (now())`)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	workerFn := func(ctx context.Context) error {
		time.Sleep(time.Duration(s.delay) * time.Second)
		_, err := insertStmt.Exec()
		return err
	}
	ql.WorkerFns = append(ql.WorkerFns, workerFn)
	return ql, nil
}
