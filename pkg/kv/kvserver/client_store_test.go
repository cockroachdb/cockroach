// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// TestStoreSetRangesMaxBytes creates a set of ranges via splitting and then
// sets the config zone to a custom max bytes value to verify the ranges' max
// bytes are updated appropriately.
func TestStoreSetRangesMaxBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const expMaxBytes, defaultMaxBytes = 420 << 20, 512 << 20
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					SpanConfig: &spanconfig.TestingKnobs{
						ConfigureScratchRange: true,
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)
	tdb := sqlutils.MakeSQLRunner(tc.Conns[0])

	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`) // speeds up the test

	testKey := tc.ScratchRange(t)
	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(roachpb.RKey(testKey))
		if got := repl.GetMaxBytes(); got != defaultMaxBytes {
			return errors.Errorf("range max bytes values did not start at %d; got %d", defaultMaxBytes, got)
		}
		return nil
	})

	tdb.Exec(t, `ALTER RANGE DEFAULT CONFIGURE ZONE USING range_max_bytes = $1`, expMaxBytes)

	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(roachpb.RKey(testKey))
		if got := repl.GetMaxBytes(); got != expMaxBytes {
			return errors.Errorf("range max bytes values did not change to %d; got %d", expMaxBytes, got)
		}
		return nil
	})
}
