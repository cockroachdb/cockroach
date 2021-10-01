// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logger

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/spf13/pflag"
)

// logger is a TTL-based workload that inserts "log"-like rows into the database
// which will be expired from TTL after a period of time.
type logger struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	ttl                                time.Duration
	seed                               int64
	minRowsPerInsert, maxRowsPerInsert int
	tsAsPrimaryKey                     bool

	prometheus struct {
		numRows        prometheus.Gauge
		numExpiredRows prometheus.Gauge

		insertedRows prometheus.Counter
	}
}

var loggerMeta = workload.Meta{
	Name:         "ttllogger",
	Description:  "Generates a logger with a TTL",
	Version:      "0.0.1",
	PublicFacing: true,
	New: func() workload.Generator {
		g := &logger{}
		g.flags.FlagSet = pflag.NewFlagSet(`ttllogger`, pflag.ContinueOnError)
		g.flags.DurationVar(&g.ttl, "ttl", time.Minute, `Table TTL duration.`)
		g.flags.Int64Var(&g.seed, `seed`, 1, `Key hash seed.`)
		g.flags.IntVar(&g.minRowsPerInsert, `min-rows-per-insert`, 10, `Minimum rows per insert.`)
		g.flags.IntVar(&g.maxRowsPerInsert, `max-rows-per-insert`, 1000, `Maximum rows per insert.`)
		g.flags.BoolVar(&g.tsAsPrimaryKey, `ts-as-primary-key`, true, `Whether timestamp should be part of the PK`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

func init() {
	workload.Register(loggerMeta)
}

func (l logger) Hooks() workload.Hooks {
	return workload.Hooks{}
}

func (l *logger) setupMetrics(reg prometheus.Registerer) {
	f := promauto.With(reg)
	l.prometheus.numRows = f.NewGauge(
		prometheus.GaugeOpts{
			Namespace: histogram.PrometheusNamespace,
			Subsystem: loggerMeta.Name,
			Name:      "num_rows",
			Help:      "Number of rows in the table",
		},
	)

	l.prometheus.numExpiredRows = f.NewGauge(
		prometheus.GaugeOpts{
			Namespace: histogram.PrometheusNamespace,
			Subsystem: loggerMeta.Name,
			Name:      "num_expired_rows",
			Help:      "Number of TTL expired rows in the table.",
		},
	)

	l.prometheus.insertedRows = f.NewCounter(
		prometheus.CounterOpts{
			Namespace: histogram.PrometheusNamespace,
			Subsystem: loggerMeta.Name,
			Name:      "rows_inserted",
			Help:      "Number of rows inserted.",
		},
	)
}

var logChars = []rune("abdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ !.")

func (l *logger) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(l, l.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(l.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(l.connFlags.Concurrency + 1)

	insertStmt, err := db.Prepare(`
		INSERT INTO logs (message) (SELECT ($2 || s) FROM generate_series(1, $1) s)`,
	)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	selectElemSQL := `SELECT * FROM logs WHERE ts >= now() - $1::interval LIMIT 1`
	if !l.tsAsPrimaryKey {
		selectElemSQL = `SELECT * FROM logs WHERE id >= $1::string LIMIT 1`
	}
	selectElemStmt, err := db.Prepare(selectElemSQL)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < l.connFlags.Concurrency/2; i++ {
		rng := rand.New(rand.NewSource(l.seed + int64(i)))
		hists := reg.GetHandle()
		workerFn := func(ctx context.Context) error {
			strLen := 1 + rng.Intn(100)
			str := make([]rune, strLen)
			for i := 0; i < strLen; i++ {
				str[i] = logChars[rand.Intn(len(logChars))]
			}
			rowsToInsert := l.minRowsPerInsert + rng.Intn(l.maxRowsPerInsert-l.minRowsPerInsert)

			start := timeutil.Now()
			_, err := insertStmt.Exec(rowsToInsert, string(str))
			elapsed := timeutil.Since(start)
			hists.Get(`log`).Record(elapsed)
			l.prometheus.insertedRows.Add(float64(rowsToInsert))
			return err
		}
		selectFn := func(ctx context.Context) error {
			start := timeutil.Now()
			var placeholder interface{}
			placeholder = l.ttl / 2
			if !l.tsAsPrimaryKey {
				id := uuid.MakeV4()
				id.DeterministicV4(uint64(rng.Int63()), uint64(1<<63))
				placeholder = id.String()
			}
			_, err := selectElemStmt.Exec(placeholder)
			elapsed := timeutil.Since(start)
			hists.Get(`select`).Record(elapsed)
			return err
		}
		ql.WorkerFns = append(ql.WorkerFns, selectFn, workerFn)
	}

	l.setupMetrics(reg.Registerer())
	ql.WorkerFns[0] = func(ctx context.Context) error {
		return retry.ForDuration(time.Minute, func() error {
			var numRows int64
			if err := db.QueryRow("SELECT count(1) FROM logs AS OF SYSTEM TIME '-30s'").Scan(&numRows); err != nil {
				return err
			}
			l.prometheus.numRows.Set(float64(numRows))
			var numExpiredRows int64
			if err := db.QueryRow("SELECT count(1) FROM logs AS OF SYSTEM TIME '-30s' WHERE now() > crdb_internal_ttl_expiration").Scan(&numExpiredRows); err != nil {
				return err
			}
			l.prometheus.numExpiredRows.Set(float64(numExpiredRows))
			return nil
		})
	}
	return ql, nil
}

func (l logger) Meta() workload.Meta {
	return loggerMeta
}

func (l logger) Tables() []workload.Table {
	pk := `PRIMARY KEY (ts, id)`
	if !l.tsAsPrimaryKey {
		pk = `PRIMARY KEY (id)`
	}
	return []workload.Table{
		{
			Name: "logs",
			Schema: fmt.Sprintf(`(
	ts TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
	-- total hack to get around pretty print but whatever
	id TEXT NOT NULL DEFAULT gen_random_uuid()::string,
	message TEXT NOT NULL,
	%s
) TTL '%d seconds'`, pk, int(l.ttl.Seconds())),
		},
	}
}

func (l logger) Flags() workload.Flags {
	return l.flags
}
