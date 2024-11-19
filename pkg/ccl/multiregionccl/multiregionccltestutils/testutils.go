// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionccltestutils

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/errors"
)

type multiRegionTestClusterParams struct {
	baseDir         string
	replicationMode base.TestClusterReplicationMode
	useDatabase     string
	settings        *cluster.Settings
	scanInterval    time.Duration
}

// MultiRegionTestClusterParamsOption is an option that can be passed to
// TestingCreateMultiRegionCluster.
type MultiRegionTestClusterParamsOption func(params *multiRegionTestClusterParams)

// WithBaseDirectory is an option to include a base directory for the
// created multi-region cluster.
func WithBaseDirectory(baseDir string) MultiRegionTestClusterParamsOption {
	return func(params *multiRegionTestClusterParams) {
		params.baseDir = baseDir
	}
}

// WithReplicationMode is an option to control the replication mode for the
// created multi-region cluster.
func WithReplicationMode(
	replicationMode base.TestClusterReplicationMode,
) MultiRegionTestClusterParamsOption {
	return func(params *multiRegionTestClusterParams) {
		params.replicationMode = replicationMode
	}
}

// WithUseDatabase is an option to set the UseDatabase server option.
func WithUseDatabase(db string) MultiRegionTestClusterParamsOption {
	return func(params *multiRegionTestClusterParams) {
		params.useDatabase = db
	}
}

// WithSettings is used to configure the settings the cluster is created with.
func WithSettings(settings *cluster.Settings) MultiRegionTestClusterParamsOption {
	return func(params *multiRegionTestClusterParams) {
		params.settings = settings
	}
}

// WithScanInterval is used to configure the scan interval for various KV
// queues.
func WithScanInterval(interval time.Duration) MultiRegionTestClusterParamsOption {
	return func(params *multiRegionTestClusterParams) {
		params.scanInterval = interval
	}
}

// TestingCreateMultiRegionCluster creates a test cluster with numServers number
// of nodes and the provided testing knobs applied to each of the nodes. Every
// node is placed in its own locality, named "us-east1", "us-east2", and so on.
func TestingCreateMultiRegionCluster(
	t testing.TB, numServers int, knobs base.TestingKnobs, opts ...MultiRegionTestClusterParamsOption,
) (*testcluster.TestCluster, *gosql.DB, func()) {
	regionNames := make([]string, numServers)
	for i := 0; i < numServers; i++ {
		// "us-east1", "us-east2"...
		regionNames[i] = fmt.Sprintf("us-east%d", i+1)
	}

	return TestingCreateMultiRegionClusterWithRegionList(
		t,
		regionNames,
		1, /* serversPerRegion */
		knobs,
		opts...)
}

// TestingCreateMultiRegionClusterWithRegionList creates a test cluster with
// serversPerRegion number of nodes in each of the provided regions and the
// provided testing knobs applied to each of the nodes. Every node is placed in
// its own locality, named according to the given region names.
func TestingCreateMultiRegionClusterWithRegionList(
	t testing.TB,
	regionNames []string,
	serversPerRegion int,
	knobs base.TestingKnobs,
	opts ...MultiRegionTestClusterParamsOption,
) (*testcluster.TestCluster, *gosql.DB, func()) {
	serverArgs := make(map[int]base.TestServerArgs)

	params := &multiRegionTestClusterParams{}
	for _, opt := range opts {
		opt(params)
	}

	totalServerCount := 0
	for _, region := range regionNames {
		for i := 0; i < serversPerRegion; i++ {
			serverArgs[totalServerCount] = base.TestServerArgs{
				Settings:      params.settings,
				Knobs:         knobs,
				ExternalIODir: params.baseDir,
				UseDatabase:   params.useDatabase,
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "region", Value: region}},
				},
				ScanInterval: params.scanInterval,
			}
			totalServerCount++
		}
	}

	tc := testcluster.StartTestCluster(t, totalServerCount, base.TestClusterArgs{
		ReplicationMode:   params.replicationMode,
		ServerArgsPerNode: serverArgs,
		ServerArgs: base.TestServerArgs{
			// Disabling this due to failures in the rtt_analysis tests. Ideally
			// we could disable multi-tenancy just for those tests, but this function
			// is used to create the MR cluster for all test cases. For
			// bonus points, the code to re-enable this should also provide more
			// flexibility in disabling the default test tenant by callers of this
			// function. Re-enablement is tracked with #76378.
			DefaultTestTenant: base.TODOTestTenantDisabled,
		},
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
