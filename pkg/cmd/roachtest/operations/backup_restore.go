// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package operations

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

type backupRestoreCleanup struct {
	db string
}

func (cl *backupRestoreCleanup) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	o.Status(fmt.Sprintf("dropping newly created db %s", cl.db))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP DATABASE %s CASCADE", cl.db))
	if err != nil {
		o.Fatal(err)
	}
}

func runBackupRestore(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
	// This operation looks for the district table in a database named cct_tpcc or tpcc.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	dbWhitelist := []string{"cct_tpcc", "tpcc"}
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()
	dbs, err := conn.QueryContext(ctx, "SELECT database_name FROM [SHOW DATABASES]")
	if err != nil {
		o.Fatal(err)
	}
	var dbName string
	for dbs.Next() {
		var dbStr string
		if err := dbs.Scan(&dbStr); err != nil {
			o.Fatal(err)
		}
		for i := range dbWhitelist {
			if dbWhitelist[i] == dbStr {
				// We found a db in the whitelist.
				dbName = dbStr
				break
			}
		}
		if dbName != "" {
			break
		}
	}
	if dbName == "" {
		o.Status("did not find a db in the whitelist")
		return nil
	}

	backupTS := time.Now().Add(-1 * time.Minute).UTC().Format(time.DateTime)

	o.Status(fmt.Sprintf("backing up table district in db %s", dbName))
	bucket := fmt.Sprintf("gs://%s/operation-backup-restore/%d/?AUTH=implicit", testutils.BackupTestingBucket(), rng.Int63())

	_, err = conn.ExecContext(ctx, fmt.Sprintf("BACKUP TABLE %s.district TO '%s' AS OF SYSTEM TIME '%s'", dbName, bucket, backupTS))
	if err != nil {
		o.Fatal(err)
	}

	restoreDBName := fmt.Sprintf("backup_restore_op_%d", rng.Int63())
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", restoreDBName))
	if err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("restoring table district into db %s", dbName))
	_, err = conn.ExecContext(ctx, fmt.Sprintf("RESTORE TABLE %s.district FROM '%s' AS OF SYSTEM TIME '%s' WITH OPTIONS (into_db = '%s', skip_missing_foreign_keys)", dbName, bucket, backupTS, restoreDBName))

	if err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("verifying table district in db %s", restoreDBName))
	backupRow, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT SUM(d_next_o_id) FROM %s.district AS OF SYSTEM TIME '%s'", dbName, backupTS))
	backupRow.Next()
	var backupSum, restoreSum int64
	backupRow.Scan(&backupSum)

	restoredRow, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT SUM(d_next_o_id) FROM %s.district", restoreDBName))
	restoredRow.Next()
	restoredRow.Scan(&restoreSum)

	if backupSum != restoreSum {
		o.Fatalf("backup and restore sums do not match: %d != %d", backupSum, restoreSum)
	}

	return &backupRestoreCleanup{db: restoreDBName}
}

func registerBackupRestore(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:             "backup-restore/tpcc/district",
		Owner:            registry.OwnerDisasterRecovery,
		Timeout:          24 * time.Hour,
		CompatibleClouds: registry.AllClouds,
		Dependencies:     []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:              runBackupRestore,
	})
}
