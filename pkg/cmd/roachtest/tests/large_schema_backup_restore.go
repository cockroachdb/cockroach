// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

// registerLargeSchemaBackupRestores registers all permutations of
// multi-region large schema benchmarking (for different scales
// and multi-region).
func registerLargeSchemaBackupRestores(r registry.Registry) {
	// 10k is probably the upper limit right now, but we haven't tested further.
	for _, scale := range []int{1000, 5000, 10000} {
		largeSchemaBackupRestore(r, scale)
	}
}

func largeSchemaBackupRestore(r registry.Registry, numTables int) {
	clusterSpec := []spec.Option{
		spec.CPU(8),
		spec.WorkloadNode(),
		spec.WorkloadNodeCPU(8),
		spec.VolumeSize(800),
		spec.GCEVolumeType("pd-ssd"),
		spec.GCEMachineType("n2-standard-8"),
	}
	testTimeout := 19 * time.Hour

	// The roachprod default for GCE and the backup testing bucket are both us-east1,
	// so no special region assignment is required for the single-region case.

	// 9 CRDB nodes and one will be used for the TPCC workload runner.
	numNodes := 10

	r.Add(registry.TestSpec{
		Name:      fmt.Sprintf("backup-restore/large-schema/tables=%d", numTables),
		Owner:     registry.OwnerDisasterRecovery,
		Benchmark: false,
		Cluster: r.MakeClusterSpec(
			numNodes,
			clusterSpec...,
		),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Weekly),
		Timeout:          testTimeout,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// Number of tables per-database from the TPCC template.
			const numTablesForTPCC = 9
			const populateFileName = "populate_dbs.txt"
			numDbs := numTables / numTablesForTPCC
			dbs := make([]string, numDbs)
			for i := range numDbs {
				dbs[i] = fmt.Sprintf("warehouse_%d", i)
			}
			err := c.PutString(ctx, strings.Join(dbs, "\n"), populateFileName, 0755, c.WorkloadNode())
			require.NoError(t, err)

			// Create all the databases
			importConcurrencyLimit := 32
			options := tpccOptions{
				WorkloadCmd: "tpccmultidb",
				DB:          dbs[0],
				SetupType:   usingInit,
				Warehouses:  numNodes - 1,
				ExtraSetupArgs: fmt.Sprintf("--db-list-file=%s --import-concurrency-limit=%d",
					populateFileName,
					importConcurrencyLimit,
				),
			}
			options.Start = func(ctx context.Context, t test.Test, c cluster.Cluster) {
				settings := install.MakeClusterSettings()
				startOpts := option.DefaultStartOpts()
				startOpts.RoachprodOpts.ScheduleBackups = false
				c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())
				conn := c.Conn(ctx, t.L(), 1)
				defer conn.Close()
				// Since we will be making a large number of databases / tables
				// quickly,on MR the job retention can slow things down. Let's
				// minimize how long jobs are kept, so that the creation / ingest
				// completes in a reasonable amount of time.
				_, err := conn.Exec("SET CLUSTER SETTING jobs.retention_time='1h'")
				require.NoError(t, err)
				// Use a higher number of retries, since we hit retry errors on importing
				// a large number of tables
				_, err = conn.Exec("SET CLUSTER SETTING kv.transaction.internal.max_auto_retries=500")
				require.NoError(t, err)
				// Create a user that will be used for authentication for the REST
				// API calls.
				_, err = conn.Exec("CREATE USER roachadmin password 'roacher'")
				require.NoError(t, err)
				_, err = conn.Exec("GRANT ADMIN to roachadmin")
				require.NoError(t, err)

			}

			setupTPCC(ctx, t, t.L(), c, options)

			// The backup testing bucket is single-region, because that's cheaper.
			// So skip this test in multi-region clusters, to avoid the expense of
			// transferring data across regions.
			conn := c.Conn(ctx, t.L(), 1)
			defer conn.Close()

			dest := destinationName(c)
			uri := `gs://` + backupTestingBucket + `/` + dest + `?AUTH=implicit`
			t.L().Printf("Backing up to %s\n", uri)
			_, err = conn.Exec("BACKUP INTO $1 WITH REVISION_HISTORY", uri)
			require.NoError(t, err)

			dbsAndSchemas := make([]string, len(dbs))
			for i, db := range dbs {
				dbsAndSchemas[i] = fmt.Sprintf("%s.public", db)
			}

			grantPreamble := fmt.Sprintf("GRANT ALL ON ALL TABLES IN SCHEMA %s TO", strings.Join(dbsAndSchemas, ","))

			const numLayers = 24
			for i := range numLayers {
				testUser := fmt.Sprintf("test-user-%d", i)
				_, err = conn.Exec(fmt.Sprintf("CREATE USER %q", testUser))
				require.NoError(t, err)
				cmd := fmt.Sprintf("%s %q", grantPreamble, testUser)
				_, err = conn.Exec(cmd)
				require.NoError(t, err)

				_, err = conn.Exec("BACKUP INTO LATEST IN $1 WITH REVISION_HISTORY", uri)
				require.NoError(t, err)
			}

			//time.Sleep(600 * time.Second)

			t.L().Printf("Dropping databases\n") // Needed to do the full cluster restore.
			for _, dbName := range dbs {
				_, err = conn.Exec("DROP DATABASE IF EXISTS " + dbName + " CASCADE")
				require.NoError(t, err)
			}

			t.L().Printf("Restoring from %s\n", uri)
			_, err = conn.Exec("RESTORE FROM LATEST IN $1", uri)
			require.NoError(t, err)

		},
	})
}
