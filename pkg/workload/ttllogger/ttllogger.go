// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttllogger

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/spf13/pflag"
)

// ttlLogger is a TTL-based workload that inserts "log"-like rows into the
// database which will be expired from TTL after a period of time.
type ttlLogger struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	ttl                                time.Duration
	seed                               int64
	minRowsPerInsert, maxRowsPerInsert int
	tsAsPrimaryKey                     bool

	prometheus struct {
		insertedRows prometheus.Counter
	}
}

var ttlLoggerMeta = workload.Meta{
	Name:        "ttllogger",
	Description: "Generates a simple log table with rows expiring after the given TTL.",
	Version:     "0.0.1",
	New: func() workload.Generator {
		g := &ttlLogger{}
		g.flags.FlagSet = pflag.NewFlagSet(`ttllogger`, pflag.ContinueOnError)
		g.flags.DurationVar(&g.ttl, "ttl", time.Minute, `Duration for the TTL to expire.`)
		g.flags.Int64Var(&g.seed, `seed`, 1, `Seed for randomization operations.`)
		g.flags.IntVar(&g.minRowsPerInsert, `min-rows-per-insert`, 1, `Minimum rows per insert per query.`)
		g.flags.IntVar(&g.maxRowsPerInsert, `max-rows-per-insert`, 100, `Maximum rows per insert per query.`)
		g.flags.BoolVar(&g.tsAsPrimaryKey, `ts-as-primary-key`, true, `Whether timestamp column for the table should be part of the primary key.`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

func init() {
	workload.Register(ttlLoggerMeta)
}

func (l ttlLogger) Hooks() workload.Hooks {
	return workload.Hooks{}
}

func (l *ttlLogger) setupMetrics(reg prometheus.Registerer) {
	p := promauto.With(reg)
	l.prometheus.insertedRows = p.NewCounter(
		prometheus.CounterOpts{
			Namespace: histogram.PrometheusNamespace,
			Subsystem: ttlLoggerMeta.Name,
			Name:      "rows_inserted",
			Help:      "Number of rows inserted.",
		},
	)
}

var logChars = []rune("abdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ !.")

func (l *ttlLogger) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(l.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(l.connFlags.Concurrency + 1)

	// Prepare a query which inserts rows and selects rows.
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

	if l.connFlags.Concurrency%2 != 0 {
		return workload.QueryLoad{}, errors.Newf("concurrency must be divisible by 2")
	}

	ql := workload.QueryLoad{}
	for len(ql.WorkerFns) < l.connFlags.Concurrency {
		rng := rand.New(rand.NewSource(l.seed + int64(len(ql.WorkerFns))))
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
	return ql, nil
}

func (l ttlLogger) Meta() workload.Meta {
	return ttlLoggerMeta
}

func (l ttlLogger) Tables() []workload.Table {
	pk := `PRIMARY KEY (ts, id)`
	if !l.tsAsPrimaryKey {
		pk = `PRIMARY KEY (id)`
	}
	return []workload.Table{
		{
			Name: "logs",
			Schema: fmt.Sprintf(`(
	ts TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
	id TEXT NOT NULL DEFAULT gen_random_uuid()::string,
	message TEXT NOT NULL,
	%s
) WITH (ttl_expire_after = '%s', ttl_label_metrics = true, ttl_row_stats_poll_interval = '15s', ttl_job_cron = '* * * * *')`,
				pk,
				l.ttl.String(),
			),
		},
	}
}

// Flags implements the Flagser interface.
func (l ttlLogger) Flags() workload.Flags { return l.flags }

// ConnFlags implements the ConnFlagser interface.
func (l ttlLogger) ConnFlags() *workload.ConnFlags { return l.connFlags }
