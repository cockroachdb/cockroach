// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package spanconfigkvaccessorccl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestKVAccessorUpdatesHandleLeaseIntervals ensures the KVAccessor updates are
// only performed under a valid lease.
func TestKVAccessorUpdatesHandleLeaseIntervals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	ts, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(ctx)

	tt, _ := serverutils.StartTenant(t, ts, base.TestTenantArgs{
		TenantID: roachpb.MakeTenantID(10),
	})

	const dummySpanConfigurationsFQN = "defaultdb.public.dummy_span_configurations"
	tdb := sqlutils.MakeSQLRunner(sqlDBRaw)
	tdb.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummySpanConfigurationsFQN))

	testCases := []struct {
		name string
		test func(t *testing.T, accessor spanconfig.KVAccessor, clock *hlc.Clock)
	}{
		{
			name: "lease-starts-prior-to-now",
			test: func(t *testing.T, accessor spanconfig.KVAccessor, clock *hlc.Clock) {
				// Issue a request with the lease starting before now -- it should succeed.
				leaseStartTime := clock.Now().Add(-5*time.Second.Nanoseconds(), 0)
				err := accessor.UpdateSpanConfigRecords(
					ctx, nil /* toDelete */, nil /* toUpsert */, leaseStartTime, hlc.MaxTimestamp,
				)
				require.NoError(t, err)
			},
		},
		{
			name: "lease-starts-in-the-future",
			test: func(t *testing.T, accessor spanconfig.KVAccessor, clock *hlc.Clock) {
				// Issue a request with the lease starting after now -- it should wait until
				// the current timestamp advances but eventually succeed.
				leaseStartTime := clock.Now().Add(1*time.Second.Nanoseconds(), 0)
				err := accessor.UpdateSpanConfigRecords(
					ctx, nil /* toDelete */, nil /* toUpsert */, leaseStartTime, hlc.MaxTimestamp,
				)
				require.NoError(t, err)
			},
		},
		{
			name: "expired-lease",
			test: func(t *testing.T, accessor spanconfig.KVAccessor, clock *hlc.Clock) {
				leaseExpiration := clock.Now().Add(-1*time.Second.Nanoseconds(), 0)
				err := accessor.UpdateSpanConfigRecords(
					ctx, nil /* toDelete */, nil /* toUpsert */, hlc.MinTimestamp, leaseExpiration,
				)
				require.Error(t, err)
				require.True(t, spanconfigkvaccessor.IsRetryableLeaseExpiredError(err))
			},
		},
	}

	testutils.RunTrueAndFalse(t, "secondary-tenant", func(t *testing.T, isSecondaryTenant bool) {
		accessor := ts.SpanConfigKVAccessor().(spanconfig.KVAccessor)
		if isSecondaryTenant {
			accessor = tt.SpanConfigKVAccessor().(spanconfig.KVAccessor)
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				tc.test(t, accessor, ts.Clock())
			})
		}
	})
}
