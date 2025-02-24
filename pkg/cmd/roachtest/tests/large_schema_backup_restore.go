// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

// registerLargeSchemaBackupRestores registers tests that large schemas can
// backup/restore successfully.
func registerLargeSchemaBackupRestores(r registry.Registry) {
	// 10k is probably the upper limit right now, but we haven't tested further.
	largeSchemaBackupRestore(r, 10000)
}

// The workload:
// (1) Creates and drops N (numTables) tables as fast as possible, then
// (2) Takes an incremental backup with revision history.
// And does this 24 times, simulating daily full backups + hourly incrementals.
// Then, the test attempts to restore from LATEST in that backup chain.
func largeSchemaBackupRestore(r registry.Registry, numTables int) {
	r.Add(registry.TestSpec{
		Name:      fmt.Sprintf("backup-restore/large-schema/tables=%d", numTables),
		Owner:     registry.OwnerDisasterRecovery,
		Benchmark: false,
		Cluster: r.MakeClusterSpec(
			// The roachprod default for GCE and the backup testing bucket are both us-east1,
			// so no special region assignment is required
			9,
			spec.CPU(8),
			spec.GCEMachineType("n2-standard-8"),
		),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Weekly),
		Timeout:          48 * time.Hour, // Each layer takes ~1hr, and we have 24 of them.
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			const numTablesPerDB = 100
			numDbs := numTables/numTablesPerDB + 1
			dbs := make([]string, numDbs)
			tables := make([]string, numTables)

			c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), install.MakeClusterSettings(), c.CRDBNodes())
			conn := c.Conn(ctx, t.L(), 1)
			defer conn.Close()

			t.L().Printf("Creating tables")
			for i := range numDbs {
				t.L().Printf("Building database %d of %d", i+1, numDbs)
				dbName := fmt.Sprintf("test_db_%d", i)
				dbs[i] = dbName
				_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
				require.NoError(t, err)

				for j := range numTablesPerDB {
					tableIndex := i*numTablesPerDB + j
					if tableIndex >= numTables {
						break
					}
					tableName := fmt.Sprintf("%s.kv_%d", dbs[i], j)
					tables[tableIndex] = tableName
					_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (key STRING NOT NULL PRIMARY KEY, value STRING NOT NULL)", tableName))
					require.NoError(t, err)
				}
			}

			dest := destinationName(c)
			uri := `gs://` + backupTestingBucket + `/` + dest + `?AUTH=implicit`
			t.L().Printf("Backing up to %s\n", uri)
			_, err := conn.ExecContext(ctx, "BACKUP INTO $1 WITH REVISION_HISTORY", uri)
			require.NoError(t, err)

			const numLayers = 24
			for i := range numLayers {
				t.L().Printf("Building layer %d of %d", i+1, numLayers)

				for _, tableName := range tables {
					_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", tableName))
					require.NoError(t, err)
					_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (key STRING NOT NULL PRIMARY KEY, value STRING NOT NULL)", tableName))
					require.NoError(t, err)
				}

				_, err = conn.Exec("BACKUP INTO LATEST IN $1 WITH REVISION_HISTORY", uri)
				require.NoError(t, err)
			}

			t.L().Printf("Dropping databases\n") // Needed to do the full cluster restore.
			for _, dbName := range dbs {
				_, err = conn.Exec("DROP DATABASE " + dbName + " CASCADE")
				require.NoError(t, err)
			}

			t.L().Printf("Restoring from %s\n", uri)
			_, err = conn.Exec("RESTORE FROM LATEST IN $1", uri)
			require.NoError(t, err)

			row := conn.QueryRowContext(ctx, "SELECT count(*) FROM system.namespace")
			var namespaceObjectCount int
			require.NoError(t, row.Scan(&namespaceObjectCount))
			// Check that we actually restored N (numTables) objects.
			require.Greater(t, namespaceObjectCount, numTables)
		},
	})
}
