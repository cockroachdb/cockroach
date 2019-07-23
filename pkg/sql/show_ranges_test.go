// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
)

func TestShowRangesWithLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numNodes = 2

	zoneConfig := config.DefaultZoneConfig()
	zoneConfig.NumReplicas = proto.Int32(1)

	ctx := context.Background()
	tsArgs := func() base.TestServerArgs {
		return base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DefaultZoneConfigOverride:       &zoneConfig,
					DefaultSystemZoneConfigOverride: &zoneConfig,
				},
			},
		}
	}
	tcArgs := base.TestClusterArgs{ServerArgsPerNode: map[int]base.TestServerArgs{
		0: tsArgs(),
		1: tsArgs(),
	}}

	tc := testcluster.StartTestCluster(t, numNodes, tcArgs)
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE TABLE t (x INT PRIMARY KEY)`)
	sqlDB.Exec(t, `ALTER TABLE t SPLIT AT VALUES (5)`)

	result := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_RANGES FROM TABLE t`)
	for _, row := range result {
		// Because StartTestCluster changes the locality no matter what the
		// arguments are, we expect whatever the test server sets up.
		locality := fmt.Sprintf("region=test,dc=dc%s", row[4])
		if row[5] != locality {
			t.Fatalf("expected %s found %s", locality, row[5])
		}
	}
}
