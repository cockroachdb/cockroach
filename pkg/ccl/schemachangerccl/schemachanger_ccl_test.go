// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package schemachangerccl

import (
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/sctest"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func newCluster(t *testing.T, knobs *scrun.TestingKnobs) (*gosql.DB, func()) {
	_, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, base.TestingKnobs{
			SQLDeclarativeSchemaChanger: knobs,
			JobsTestingKnobs:            jobs.NewTestingKnobsWithShortIntervals(),
		},
	)
	return sqlDB, cleanup
}

func sharedTestdata(t *testing.T) string {
	testdataDir := "../../sql/schemachanger/testdata/"
	if bazel.BuiltWithBazel() {
		runfile, err := bazel.Runfile("pkg/sql/schemachanger/testdata/")
		if err != nil {
			t.Fatal(err)
		}
		testdataDir = runfile
	}
	return testdataDir
}

func endToEndPath(t *testing.T) string {
	return testutils.TestDataPath(t, "end_to_end")
}

func TestSchemaChangerSideEffects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	sctest.EndToEndSideEffects(t, endToEndPath(t), newCluster)
}

func TestBackupRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	t.Run("ccl", func(t *testing.T) {
		sctest.Backup(t, endToEndPath(t), newCluster)
	})
	t.Run("non-ccl", func(t *testing.T) {
		sctest.Backup(t, sharedTestdata(t), sctest.SingleNodeCluster)
	})
}

func TestRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sctest.Rollback(t, endToEndPath(t), newCluster)
}

func TestPause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sctest.Pause(t, endToEndPath(t), newCluster)
}

func TestDecomposeToElements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sctest.DecomposeToElements(t, testutils.TestDataPath(t, "decomp"), newCluster)
}
