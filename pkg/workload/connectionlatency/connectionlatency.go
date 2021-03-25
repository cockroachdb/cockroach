// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package connectionlatency

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/jackc/pgx/v4"
	"github.com/spf13/pflag"
)

type connectionLatency struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	locality string
}

func init() {
	workload.Register(connectionLatencyMeta)
}

var connectionLatencyMeta = workload.Meta{
	Name:        `connectionlatency`,
	Description: `Testing Connection Latencies`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		c := &connectionLatency{}
		c.flags.FlagSet = pflag.NewFlagSet(`connectionlatency`, pflag.ContinueOnError)
		c.flags.StringVar(&c.locality, `locality`, ``, `Which locality is the workload running in? (east,west,central)`)
		c.connFlags = workload.NewConnFlags(&c.flags)
		return c
	},
}

// Meta implements the Generator interface.
func (connectionLatency) Meta() workload.Meta { return connectionLatencyMeta }

// Flags implements the Flagser interface.
func (c *connectionLatency) Flags() workload.Flags { return c.flags }

// Tables implements the Generator interface.
func (connectionLatency) Tables() []workload.Table {
	return nil
}

// Ops implements the Opser interface.
func (c *connectionLatency) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	ql := workload.QueryLoad{}
	_, err := workload.SanitizeUrls(c, c.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	for _, url := range urls {
		op := &connectionOp{
			url:   url,
			hists: reg.GetHandle(),
		}

		conn, err := pgx.Connect(ctx, url)
		if err != nil {
			return workload.QueryLoad{}, err
		}

		var locality string
		err = conn.QueryRow(ctx, "SHOW LOCALITY").Scan(&locality)
		if err != nil {
			return workload.QueryLoad{}, err
		}

		op.connectFrom = c.locality
		op.connectTo = locality
		ql.WorkerFns = append(ql.WorkerFns, op.run)
	}
	return ql, nil
}

type connectionOp struct {
	url         string
	hists       *histogram.Histograms
	connectFrom string
	connectTo   string
}

func (o *connectionOp) run(ctx context.Context) error {
	start := timeutil.Now()
	conn, err := pgx.Connect(ctx, o.url)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			log.Warningf(ctx, "%v", err)
		}
	}()
	elapsed := timeutil.Since(start)
	o.hists.Get(fmt.Sprintf(`connect-from-%s-to-%s`, o.connectFrom, o.connectTo)).Record(elapsed)

	if _, err = conn.Exec(ctx, "SELECT 1"); err != nil {
		return err
	}
	// Record the time it takes to do a select after connecting for reference.
	elapsed = timeutil.Since(start)
	o.hists.Get(`select`).Record(elapsed)
	return nil
}
