// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func registerQueuefeed(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "queuefeed",
		Owner:   registry.OwnerCDC,
		Cluster: r.MakeClusterSpec(4, spec.WorkloadNode()),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runQueuefeed(ctx, t, c)
		},
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
	})
}

func runQueuefeed(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	_, err := db.ExecContext(ctx, "SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, errors.Wrap(err, "enabling rangefeeds"))

	t.Status("initializing kv workload")
	c.Run(ctx, option.WithNodes(c.WorkloadNode()),
		"./cockroach workload init kv --splits=100 {pgurl:1}")

	var tableID int64
	err = db.QueryRowContext(ctx, "SELECT id FROM system.namespace WHERE name = 'kv' and \"parentSchemaID\" <> 0;").Scan(&tableID)
	require.NoError(t, err)

	t.Status("creating kv_queue")
	_, err = db.ExecContext(ctx, "SELECT crdb_internal.create_queue_feed('kv_queue', $1)", tableID)
	require.NoError(t, err)

	t.Status("running queue feed queries")

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	const numReaders = 10
	counters := make([]*atomic.Int64, numReaders)
	for i := range counters {
		counters[i] = &atomic.Int64{}
	}

	g.Go(func() error {
		return c.RunE(ctx, option.WithNodes(c.WorkloadNode()),
			"./cockroach workload run kv --duration=10m {pgurl:1}")
	})

	g.Go(func() error {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		lastCounts := make([]int64, numReaders)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				qps := make([]string, numReaders)
				for i := 0; i < numReaders; i++ {
					currentCount := counters[i].Load()
					ratePerSec := currentCount - lastCounts[i]
					qps[i] = fmt.Sprintf("%d", ratePerSec)
					lastCounts[i] = currentCount
				}
				t.L().Printf("qps: %s", strings.Join(qps, ","))
			}
		}
	})

	dbNodes := 1 // TODO fix bug that occurs with 3
	nodePool := make([]*sql.DB, numReaders)
	for i := range dbNodes {
		nodePool[i] = c.Conn(ctx, t.L(), i+1)
	}
	defer func() {
		for i := range dbNodes {
			_ = nodePool[i].Close()
		}
	}()

	for i := 0; i < numReaders; i++ {
		readerIndex := i
		g.Go(func() error {
			// Stagger the readers a bit. This helps test re-distribution of
			// partitions.
			// TODO fix bug that occurs with jitter
			// time.Sleep(time.Duration(rand.Intn(int(time.Minute))))

			// Connect to a random node to simulate a tcp load balancer.
			conn, err := nodePool[rand.Intn(dbNodes)].Conn(ctx)
			if err != nil {
				return errors.Wrap(err, "getting connection for the queuefeed reader")
			}
			defer func() { _ = conn.Close() }()

			for ctx.Err() == nil {
				var count int
				err := conn.QueryRowContext(ctx,
					"SELECT count(*) FROM crdb_internal.select_from_queue_feed('kv_queue', 10000)").Scan(&count)
				if err != nil {
					return err
				}
				counters[readerIndex].Add(int64(count))
			}
			return ctx.Err()
		})
	}

	err = g.Wait()
	if err != nil && ctx.Err() == nil {
		t.Fatal(err)
	}
}
