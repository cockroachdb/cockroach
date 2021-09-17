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

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/spf13/pflag"
)

// logger is a TTL-based workload that inserts "log"-like rows into the database
// which will be expired from TTL after a period of time.
type logger struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	ttl  time.Duration
	seed int64
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

var logChars = []rune("abdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ !.")

func (l logger) Ops(
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

	insertStmt, err := db.Prepare(
		`INSERT INTO logs (message) VALUES ($1)`,
	)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < l.connFlags.Concurrency; i++ {
		rng := rand.New(rand.NewSource(l.seed))
		hists := reg.GetHandle()
		workerFn := func(ctx context.Context) error {
			strLen := 1 + rng.Intn(100)
			str := make([]rune, strLen)
			for i := 0; i < strLen; i++ {
				str[i] = logChars[rand.Intn(len(logChars))]
			}

			start := timeutil.Now()
			_, err := insertStmt.Exec(string(str))
			elapsed := timeutil.Since(start)
			hists.Get(`log`).Record(elapsed)
			return err
		}
		ql.WorkerFns = append(ql.WorkerFns, workerFn)
	}
	return ql, nil
}

func (l logger) Meta() workload.Meta {
	return loggerMeta
}

func (l logger) Tables() []workload.Table {
	return []workload.Table{
		{
			Name: "logs",
			Schema: fmt.Sprintf(`(
	ts TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
	id UUID NOT NULL DEFAULT gen_random_uuid(),
	message TEXT NOT NULL,
	PRIMARY KEY (ts, id)
) TTL '%d seconds'`, int(l.ttl.Seconds())),
		},
	}
}

func (l logger) Flags() workload.Flags {
	return l.flags
}
