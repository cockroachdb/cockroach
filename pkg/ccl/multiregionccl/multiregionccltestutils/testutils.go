// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccltestutils

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/errors"
)

// TestingCreateMultiRegionCluster creates a test cluster with numServers number
// of nodes and the provided testing knobs applied to each of the nodes. Every
// node is placed in its own locality, named "us-east1", "us-east2", and so on.
func TestingCreateMultiRegionCluster(
	t *testing.T, numServers int, knobs base.TestingKnobs, baseDir *string,
) (serverutils.TestClusterInterface, *gosql.DB, func()) {
	serverArgs := make(map[int]base.TestServerArgs)
	regionNames := make([]string, numServers)
	for i := 0; i < numServers; i++ {
		// "us-east1", "us-east2"...
		regionNames[i] = fmt.Sprintf("us-east%d", i+1)
	}

	b := ""
	if baseDir != nil {
		b = *baseDir
	}

	for i := 0; i < numServers; i++ {
		serverArgs[i] = base.TestServerArgs{
			Knobs:         knobs,
			ExternalIODir: b,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "region", Value: regionNames[i]}},
			},
		}
	}

	tc := serverutils.StartNewTestCluster(t, numServers, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
	})

	ctx := context.Background()
	cleanup := func() {
		tc.Stopper().Stop(ctx)
	}

	sqlDB := tc.ServerConn(0)

	return tc, sqlDB, cleanup
}

// TestingEnsureCorrectPartitioning ensures that the table referenced by the
// supplied FQN has the expected indexes and that all of those indexes have the
// expected partitions.
func TestingEnsureCorrectPartitioning(
	sqlDB *gosql.DB, dbName string, tableName string, expectedIndexes []string,
) error {
	rows, err := sqlDB.Query("SELECT region FROM [SHOW REGIONS FROM DATABASE db] ORDER BY region")
	if err != nil {
		return err
	}
	defer rows.Close()
	var expectedPartitions []string
	for rows.Next() {
		var regionName string
		if err := rows.Scan(&regionName); err != nil {
			return err
		}
		expectedPartitions = append(expectedPartitions, regionName)
	}

	rows, err = sqlDB.Query(
		fmt.Sprintf(
			"SELECT index_name, partition_name FROM [SHOW PARTITIONS FROM TABLE %s.%s] ORDER BY partition_name",
			dbName,
			tableName,
		),
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	indexPartitions := make(map[string][]string)
	for rows.Next() {
		var indexName string
		var partitionName string
		if err := rows.Scan(&indexName, &partitionName); err != nil {
			return err
		}

		indexPartitions[indexName] = append(indexPartitions[indexName], partitionName)
	}

	for _, expectedIndex := range expectedIndexes {
		partitions, found := indexPartitions[expectedIndex]
		if !found {
			return errors.AssertionFailedf("did not find index %s", expectedIndex)
		}

		if len(partitions) != len(expectedPartitions) {
			return errors.AssertionFailedf(
				"unexpected number of partitions; expected %d, found %d",
				len(partitions),
				len(expectedPartitions),
			)
		}
		for i, expectedPartition := range expectedPartitions {
			if expectedPartition != partitions[i] {
				return errors.AssertionFailedf(
					"unexpected partitions; expected %v, found %v",
					expectedPartitions,
					partitions,
				)
			}
		}
	}
	return nil
}
