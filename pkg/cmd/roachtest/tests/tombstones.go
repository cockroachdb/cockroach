// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

// registerPointTombstone registers the point tombstone test.
func registerPointTombstone(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "point-tombstone/heterogeneous-value-sizes",
		Owner:            registry.OwnerStorage,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNode()),
		CompatibleClouds: registry.AllExceptAWS,
		// This roachtest is useful but relatively specific. Running it weekly
		// should be fine to ensure we don't regress.
		Suites:            registry.Suites(registry.Weekly),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Timeout:           120 * time.Minute,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			startOpts := option.DefaultStartOpts()
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
				"--vmodule=mvcc_gc_queue=2,replica_protected_timestamp=2") // queue=2 is too verbose
			startSettings := install.MakeClusterSettings()
			startSettings.Env = append(startSettings.Env, "COCKROACH_AUTO_BALLAST=false")

			t.Status("starting cluster")
			c.Start(ctx, t.L(), startOpts, startSettings, c.CRDBNodes())

			// Wait for upreplication.
			conn := c.Conn(ctx, t.L(), 2)
			defer conn.Close()
			require.NoError(t, conn.PingContext(ctx))
			require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

			execSQLOrFail := func(statement string, args ...interface{}) {
				if _, err := conn.ExecContext(ctx, statement, args...); err != nil {
					t.Fatal(err)
				}
			}

			c.Run(ctx, option.WithNodes(c.WorkloadNode()), `./cockroach workload init kv --splits 1000 {pgurl:1}`)

			// Set a low 2m GC ttl.
			execSQLOrFail("alter database kv configure zone using gc.ttlseconds = $1", 120)

			// Run kv0 with massive 1MB values. This writes about ~30 GB of
			// logical value data.
			const numOps1MB = 30720
			t.Status("starting 1MB-value workload")
			m := c.NewMonitor(ctx, c.CRDBNodes())
			m.Go(func(ctx context.Context) error {
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(`./cockroach workload run kv --read-percent 0 `+
					`--concurrency 128 --max-rate 512 --tolerate-errors `+
					` --min-block-bytes=1048576 --max-block-bytes=1048576 `+
					` --max-ops %d `+
					`{pgurl:1-3}`, numOps1MB))
				return nil
			})
			m.Wait()

			// Run kv0 with more typical 4KB values, starting at a later offset
			// to not overwrite the previously written values. This writes about
			// ~500MB of logical value data. The additional per-row overhead
			// will be higher than above, since we're writing more rows.
			t.Status("starting 4KB-value workload")
			m = c.NewMonitor(ctx, c.Range(1, 3))
			m.Go(func(ctx context.Context) error {
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(`./cockroach workload run kv --read-percent 0 `+
					`--concurrency 256 --max-rate 1024 --tolerate-errors `+
					` --min-block-bytes=4096 --max-block-bytes=4096 `+
					` --max-ops 122880 --write-seq R%d `+
					`{pgurl:1-3}`, numOps1MB))
				return nil
			})
			m.Wait()

			// Delete the initially written 1MB values (eg, ~30 GB)
			t.Status("deleting previously-written 1MB values")
			var statsAfterDeletes tableSizeInfo
			m = c.NewMonitor(ctx, c.CRDBNodes())
			m.Go(func(ctx context.Context) error {
				n1Conn := c.Conn(ctx, t.L(), 1)
				defer n1Conn.Close()
				_, err := n1Conn.ExecContext(ctx, `USE kv;`)
				require.NoError(t, err)

				var paginationToken int64 = math.MinInt64
				for done := false; !done; {
					const deleteQuery = `
					WITH deleted_keys AS (
						DELETE FROM kv WHERE k >= $1 AND length(v) = 1048576
						ORDER BY k ASC LIMIT 1000 RETURNING k
					)
					SELECT max(k) FROM deleted_keys;
					`
					row := n1Conn.QueryRowContext(ctx, deleteQuery, paginationToken)
					var nextToken gosql.NullInt64
					require.NoError(t, row.Scan(&nextToken))
					done = !nextToken.Valid
					paginationToken = nextToken.Int64
				}
				statsAfterDeletes = queryTableSize(ctx, t, n1Conn, "kv")
				return nil
			})
			m.Wait()
			fmt.Println(statsAfterDeletes.String())
			require.LessOrEqual(t, statsAfterDeletes.livePercentage, 0.10)

			// Wait for garbage collection to delete the non-live data.
			targetSize := uint64(3 << 30) /* 3 GiB */
			t.Status("waiting for garbage collection and compaction to reduce on-disk size to ", humanize.IBytes(targetSize))
			m = c.NewMonitor(ctx, c.CRDBNodes())
			m.Go(func(ctx context.Context) error {
				ticker := time.NewTicker(10 * time.Second)
				defer ticker.Stop()

				info := queryTableSize(ctx, t, conn, "kv")
				fmt.Println(info.String())
				for info.approxDiskBytes > targetSize {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-ticker.C:
						info = queryTableSize(ctx, t, conn, "kv")
						fmt.Println(info.String())
					}
				}
				return nil
			})
			m.Wait()
		},
	})
}

type tableSizeInfo struct {
	databaseID      uint64
	tableID         uint64
	rangeCount      uint64
	approxDiskBytes uint64
	liveBytes       uint64
	totalBytes      uint64
	livePercentage  float64
}

func (info tableSizeInfo) String() string {
	return fmt.Sprintf("databaseID: %d, tableID: %d, rangeCount: %d, approxDiskBytes: %s, liveBytes: %s, totalBytes: %s, livePercentage: %.2f",
		info.databaseID,
		info.tableID,
		info.rangeCount,
		humanize.IBytes(info.approxDiskBytes),
		humanize.IBytes(info.liveBytes),
		humanize.IBytes(info.totalBytes),
		info.livePercentage)
}

func queryTableSize(
	ctx context.Context, t require.TestingT, conn *gosql.DB, tableName string,
) (info tableSizeInfo) {
	require.NoError(t, conn.QueryRowContext(ctx, `
		SELECT database_id, table_id, range_count, approximate_disk_bytes, live_bytes, total_bytes, live_percentage
		FROM crdb_internal.tenant_span_stats()
		JOIN system.namespace ON (database_id, table_id, name) = ("parentID", id, $1)
	`, tableName).Scan(
		&info.databaseID,
		&info.tableID,
		&info.rangeCount,
		&info.approxDiskBytes,
		&info.liveBytes,
		&info.totalBytes,
		&info.livePercentage,
	))
	return info
}
