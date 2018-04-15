package main

import (
	"context"
	"fmt"

	"time"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"io/ioutil"
	"os"
	"strconv"
)

func init() {
	// This test sets up a cluster and runs kv on it with high concurrency and a large block size
	// to force a large range. We then make sure that the largest range isn't larger than 196mb and
	// that backpressure is working correctly.
	runHotSpot := func(ctx context.Context, t *test, c *cluster, nodes int, duration time.Duration, concurrency int) {
		c.Put(ctx, workload, "./workload", c.Node(nodes))
		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Start(ctx, c.All())

		c.Run(ctx, c.Node(nodes), `./workload init kv --drop`)

		var m *errgroup.Group // see comment in version.go
		m, ctx = errgroup.WithContext(ctx)

		m.Go(func() error {
			t.Status("starting load generator")

			quietL, err := newLogger("run kv", strconv.Itoa(0), "workload"+strconv.Itoa(0), ioutil.Discard, os.Stderr)
			if err != nil {
				return err
			}
			return c.RunL(ctx, quietL, c.Node(nodes), fmt.Sprintf(
				"./workload run kv --read-percent=0 --splits=0 --concurrency=%d --min-block-bytes=1024000 --duration %s --max-block-bytes=1024001 {pgurl:1} &",
				concurrency, duration.String()))
		})

		m.Go(func() error {
			db := c.Conn(ctx, 1)
			defer db.Close()

			run := func(stmt string) {
				t.Status(stmt)
				_, err := db.ExecContext(ctx, stmt)
				if err != nil {
					t.Fatal(err)
				}
			}

			run(`SET CLUSTER SETTING trace.debug.enable = true`)

			var size = float64(0)
			for tBegin := timeutil.Now(); timeutil.Since(tBegin) <= duration; {
				if err := db.QueryRow(
					`select max(bytes_per_replica->'PMax') from crdb_internal.kv_store_status;`,
				).Scan(&size); err != nil {
					return err
				}

				// 196mb in bytes
				if size > 205520896 {
					return errors.Errorf("range size %s exceeded 196mb", humanizeutil.IBytes(int64(size)))
				} else {
					t.Status(fmt.Sprintf("max range size %s", humanizeutil.IBytes(int64(size))))
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
				}
			}

			return nil
		})
		if err := m.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	minutes := 10 * time.Minute
	numNodes := 4
	concurrency := 100

	tests.Add(testSpec{
		Name:  fmt.Sprintf("hssplits/duration=%d/nodes=%d", minutes, numNodes),
		Nodes: nodes(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			if local {
				fmt.Printf("running with duration=%s in local mode\n", minutes)
				concurrency = 10
			}
			runHotSpot(ctx, t, c, numNodes, minutes, concurrency)
		},
	})
}
