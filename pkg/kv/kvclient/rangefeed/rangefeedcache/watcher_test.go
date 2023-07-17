// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeedcache_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestWatchAuthErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	host, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer host.Stopper().Stop(ctx)

	tenant, _ := serverutils.StartTenant(t, host, base.TestTenantArgs{
		TenantID: serverutils.TestTenantID(),
	})

	hostScratchRange, err := host.ScratchRange()
	require.NoError(t, err)
	hostScratchSpan := roachpb.Span{
		Key:    hostScratchRange,
		EndKey: hostScratchRange.PrefixEnd(),
	}

	w := rangefeedcache.NewWatcher(
		"test",
		tenant.Clock(),
		tenant.RangeFeedFactory().(*rangefeed.Factory),
		1024,
		[]roachpb.Span{hostScratchSpan},
		false, /*=withPrevValue*/
		func(ctx context.Context, kv *kvpb.RangeFeedValue) rangefeedbuffer.Event {
			t.Fatalf("rangefeed should fail before producing results")
			return nil
		},
		func(ctx context.Context, update rangefeedcache.Update) {
			t.Fatalf("rangefeed should fail before producing results")
		},
		&rangefeedcache.TestingKnobs{})

	err = w.Run(ctx)
	require.True(t, grpcutil.IsAuthError(err), "expected %v to be an auth error", err)
}
