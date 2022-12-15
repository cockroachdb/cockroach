// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestPinExistingGCTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_1PinExistingGCTTL - 1),
				},
			},
		},
	}

	for _, test := range []struct {
		ttlSecondsPreUpgrade  int
		ttlSecondsPostUpgrade int
	}{
		{25 * 60 * 60, 25 * 60 * 60},
		{23 * 60 * 60, 23 * 60 * 60},
	} {
		t.Run(fmt.Sprintf("pre=%s,post=%s",
			time.Duration(test.ttlSecondsPreUpgrade)*time.Second,
			time.Duration(test.ttlSecondsPostUpgrade)*time.Second,
		), func(t *testing.T) {
			ctx := context.Background()
			tc := testcluster.StartTestCluster(t, 1, clusterArgs)
			defer tc.Stopper().Stop(ctx)

			tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

			tdb.Exec(t, `ALTER RANGE DEFAULT CONFIGURE ZONE USING gc.ttlseconds = $1`,
				test.ttlSecondsPreUpgrade)
			tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
				clusterversion.ByKey(clusterversion.V23_1PinExistingGCTTL).String())

			tdb.CheckQueryResults(t, `
	SELECT
		(crdb_internal.pb_to_json('cockroach.config.zonepb.ZoneConfig', raw_config_protobuf)->'gc'->'ttlSeconds')::INT
	FROM crdb_internal.zones
	WHERE target = 'RANGE default'
	LIMIT 1
`, [][]string{{fmt.Sprintf("%d", test.ttlSecondsPostUpgrade)}})
		})
	}
}
