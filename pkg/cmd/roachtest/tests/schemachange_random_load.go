// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

const (
	txnLogFile = "transactions.ndjson"
)

func registerSchemaChangeRandomLoad(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:      "schemachange/random-load",
		Owner:     registry.OwnerSQLFoundations,
		Benchmark: true,
		Cluster: r.MakeClusterSpec(
			3,
			spec.Geo(),
			spec.GCEZones("us-east1-b,us-west1-b,europe-west2-b"),
			spec.AWSZones("us-east-2b,us-west-1a,eu-west-1a"),
		),
		// TODO(radu): enable this test on AWS.
		CompatibleClouds:           registry.AllExceptAWS,
		Suites:                     registry.Suites(registry.Nightly),
		Leases:                     registry.MetamorphicLeases,
		NativeLibs:                 registry.LibGEOS,
		RequiresDeprecatedWorkload: true, // uses schemachange
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			maxOps := 5000
			concurrency := 20
			if c.IsLocal() {
				maxOps = 200
				concurrency = 2
			}
			runSchemaChangeRandomLoad(ctx, t, c, maxOps, concurrency)
		},
	})
}

func runSchemaChangeRandomLoad(
	ctx context.Context, t test.Test, c cluster.Cluster, maxOps, concurrency int,
) {
	validate := func(db *gosql.DB) {
		var (
			id           int
			databaseName string
			schemaName   string
			objName      string
			objError     string
		)
		numInvalidObjects := 0
		rows, err := db.QueryContext(ctx, `SELECT id, database_name, schema_name, obj_name, error FROM crdb_internal.invalid_objects`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			numInvalidObjects++
			if err := rows.Scan(&id, &databaseName, &schemaName, &objName, &objError); err != nil {
				t.Fatal(err)
			}
			t.L().Errorf(
				"invalid object found: id: %d, database_name: %s, schema_name: %s, obj_name: %s, error: %s",
				id, databaseName, schemaName, objName, objError,
			)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if numInvalidObjects > 0 {
			t.Fatalf("found %d invalid objects", numInvalidObjects)
		}
	}
	loadNode := c.Node(1)
	roachNodes := c.Range(1, c.Spec().NodeCount)
	t.Status("copying binaries")
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", loadNode)

	t.Status("starting cockroach nodes")

	settings := install.MakeClusterSettings(install.ClusterSettingsOption{
		"sql.log.all_statements.enabled": "true",
	})

	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, roachNodes)

	c.Run(ctx, option.WithNodes(loadNode), "./workload init schemachange {pgurl:1}")

	result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(1)), "echo", "-n", "{store-dir}")
	if err != nil {
		t.L().Printf("Failed to retrieve store directory from node 1: %v\n", err.Error())
	}
	storeDirectory := result.Stdout

	runCmd := []string{
		"./workload run schemachange --verbose=1",
		"--tolerate-errors=false",
		fmt.Sprintf("--max-ops %d", maxOps),
		fmt.Sprintf("--concurrency %d", concurrency),
		fmt.Sprintf("--txn-log %s", filepath.Join(storeDirectory, txnLogFile)),
		fmt.Sprintf("{pgurl%s}", loadNode),
	}

	extraLabels := map[string]string{
		"concurrency": fmt.Sprintf("%d", concurrency),
		"max-ops":     fmt.Sprintf("%d", maxOps),
	}

	runCmd = append(runCmd, roachtestutil.GetWorkloadHistogramArgs(t, c, extraLabels))
	t.Status("running schemachange workload")
	err = c.RunE(ctx, option.WithNodes(loadNode), runCmd...)
	if err != nil {
		saveArtifacts(ctx, t, c, storeDirectory)
		t.Fatal(err)
	}

	// Drop the database to test the correctness of DROP DATABASE CASCADE, which
	// has been a source of schema change bugs (mostly orphaned descriptors) in
	// the past.
	// TODO (lucy): When the workload supports multiple databases and running
	// schema changes on them, we may want to push this into the post-run hook for
	// the workload itself (if we even still want it, considering that the
	// workload itself would be running DROP DATABASE CASCADE).

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	t.Status("performing validation after workload")
	validate(db)
	t.Status("dropping database")
	_, err = db.ExecContext(ctx, `USE defaultdb; DROP DATABASE schemachange CASCADE;`)
	if err != nil {
		t.Fatal(err)
	}
	t.Status("performing validation after dropping database")
	validate(db)
}

// saveArtifacts saves important test artifacts in the artifacts directory.
func saveArtifacts(ctx context.Context, t test.Test, c cluster.Cluster, storeDirectory string) {
	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	// Save a backup file called schemachange to the store directory.
	_, err := db.Exec("BACKUP DATABASE schemachange INTO 'nodelocal://1/schemachange'")
	if err != nil {
		t.L().Printf("Failed execute backup command on node 1: %v\n", err.Error())
	}
	var backupPath string
	if err := db.QueryRow("SELECT path FROM [SHOW BACKUPS IN 'nodelocal://1/schemachange']").Scan(&backupPath); err != nil {
		t.L().Printf("Failed to get backup path from node 1: %v\n", err.Error())
	}
	remoteBackupFilePath := filepath.Join(storeDirectory, "extern", "schemachange", backupPath)
	localBackupFilePath := filepath.Join(t.ArtifactsDir(), "backup")
	remoteTransactionsFilePath := filepath.Join(storeDirectory, txnLogFile)
	localTransactionsFilePath := filepath.Join(t.ArtifactsDir(), txnLogFile)

	// Copy the backup from the store directory to the artifacts directory.
	err = c.Get(ctx, t.L(), remoteBackupFilePath, localBackupFilePath, c.Node(1))
	if err != nil {
		t.L().Printf("Failed to copy backup file from node 1 to artifacts directory: %v\n", err.Error())
	}

	// Copy the txn log from the store directory to the artifacts directory.
	err = c.Get(ctx, t.L(), remoteTransactionsFilePath, localTransactionsFilePath, c.Node(1))
	if err != nil {
		t.L().Printf("Failed to copy txn log file from node 1 to artifacts directory: %v\n", err.Error())
	}
}
