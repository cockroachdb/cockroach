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

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx"
	"github.com/spf13/pflag"
)

type connectionLatency struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags
	hists     *histogram.Histograms
}

func init() {
	workload.Register(connectionLatencyMeta)
}

var connectionLatencyMeta = workload.Meta{
	Name:         `connectionlatency`,
	Description:  `Testing Connection Latencies`,
	Version:      `1.0.0`,
	PublicFacing: false,
	New: func() workload.Generator {
		c := &connectionLatency{}
		c.flags.FlagSet = pflag.NewFlagSet(`connectionlatency`, pflag.ContinueOnError)
		c.connFlags = workload.NewConnFlags(&c.flags)
		return c
	},
}

// Meta implements the Generator interface.
func (connectionLatency) Meta() workload.Meta { return connectionLatencyMeta }

// Tables implements the Generator interface.
func (connectionLatency) Tables() []workload.Table {
	return nil
}

// Ops implements the Opser interface.
func (c *connectionLatency) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	ql := workload.QueryLoad{}
	if len(urls) != 1 {
		return workload.QueryLoad{}, errors.New("expected urls to be length 1")
	}
	op := &connectionOp{
		url:   urls[0],
		hists: reg.GetHandle(),
	}
	ql.WorkerFns = append(ql.WorkerFns, op.run)
	return ql, nil
}

type connectionOp struct {
	url   string
	hists *histogram.Histograms
}

func (o *connectionOp) run(ctx context.Context) error {
	connCfg, err := pgx.ParseConnectionString(o.url)
	if err != nil {
		return err
	}
	start := timeutil.Now()
	conn, err := pgx.Connect(connCfg)
	if err != nil {
		return err
	}
	elapsed := timeutil.Since(start)
	o.hists.Get(`connect`).Record(elapsed)

	// Record the time it takes to do a select after connecting for reference.
	elapsed = timeutil.Since(start)
	if _, err = conn.Exec("SELECT 1"); err != nil {
		return err
	}

	o.hists.Get(`select`).Record(elapsed)
	return nil
}
