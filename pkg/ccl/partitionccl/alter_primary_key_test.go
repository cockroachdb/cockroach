// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package partitionccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestAlterPrimaryKeyCorrectZoneConfigBeforeBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []testutilsccl.AlterPrimaryKeyCorrectZoneConfigTestCase{
		{
			Desc: "ALTER PRIMARY KEY",
			SetupQuery: `CREATE TABLE t.test (k INT NOT NULL, v INT NOT NULL, INDEX v_idx (v));
ALTER INDEX t.test@v_idx CONFIGURE ZONE USING gc.ttlseconds = 888
			`,
			AlterQuery: `ALTER TABLE t.test ALTER PRIMARY KEY USING COLUMNS (v)`,
			ExpectedIntermediateZoneConfigs: []testutilsccl.AlterPrimaryKeyCorrectZoneConfigIntermediateZoneConfig{
				{
					ShowConfigStatement: `SHOW ZONE CONFIGURATION FOR INDEX t.test@v_idx_rewrite_for_primary_key_change`,
					ExpectedTarget:      `INDEX t.public.test@v_idx_rewrite_for_primary_key_change`,
					ExpectedSQL: `ALTER INDEX t.public.test@v_idx_rewrite_for_primary_key_change CONFIGURE ZONE USING
	range_min_bytes = 134217728,
	range_max_bytes = 536870912,
	gc.ttlseconds = 888,
	num_replicas = 1,
	constraints = '[]',
	lease_preferences = '[]'`,
				},
			},
		},
	}

	testutilsccl.AlterPrimaryKeyCorrectZoneConfigTest(
		t,
		`CREATE DATABASE t`,
		testCases,
	)
}
