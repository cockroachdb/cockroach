// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachangerccl

import (
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/sctest"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func newCluster(
	t *testing.T, knobs *scexec.TestingKnobs,
) (serverutils.TestServerInterface, *gosql.DB, func()) {
	c, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, base.TestingKnobs{
			SQLDeclarativeSchemaChanger: knobs,
			JobsTestingKnobs:            jobs.NewTestingKnobsWithShortIntervals(),
			SQLExecutor: &sql.ExecutorTestingKnobs{
				StatementFilter:                 nil,
				UseTransactionalDescIDGenerator: true,
			},
		},
	)
	return c.Server(0), sqlDB, cleanup
}

func newClusterMixed(
	t *testing.T, knobs *scexec.TestingKnobs, downlevelVersion bool,
) (serverutils.TestServerInterface, *gosql.DB, func()) {
	targetVersion := clusterversion.TestingBinaryVersion
	if downlevelVersion {
		targetVersion = clusterversion.ByKey(clusterversion.V23_1_SchemaChangerDeprecatedIndexPredicates - 1)
	}
	c, db, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(t,
		3, /* numServers */
		base.TestingKnobs{
			Server: &server.TestingKnobs{
				BinaryVersionOverride:          targetVersion,
				DisableAutomaticVersionUpgrade: make(chan struct{}),
			},
			SQLDeclarativeSchemaChanger: knobs,
			JobsTestingKnobs:            jobs.NewTestingKnobsWithShortIntervals(),
			SQLExecutor: &sql.ExecutorTestingKnobs{
				UseTransactionalDescIDGenerator: true,
			},
		})

	return c.Server(0), db, cleanup
}

func TestDecomposeToElements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sctest.DecomposeToElements(t, datapathutils.TestDataPath(t, "decomp"), newCluster)
}
