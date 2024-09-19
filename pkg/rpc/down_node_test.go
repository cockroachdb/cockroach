// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package rpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestConnectingToDownNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	{
		defer func(prev time.Duration) {
			base.DialTimeout = prev
		}(base.DialTimeout)
		base.DialTimeout = time.Millisecond
	}

	testutils.RunTrueAndFalse(t, "refused", func(t *testing.T, refused bool) {
		ctx := context.Background()

		rpcCtx := newTestContext(uuid.MakeV4(), &timeutil.DefaultTimeSource{}, time.Second, stop.NewStopper())
		defer rpcCtx.Stopper.Stop(ctx)
		rpcCtx.NodeID.Set(context.Background(), 1)

		ln, err := net.Listen("tcp", util.TestAddr.AddressField)
		require.NoError(t, err)
		if refused {
			require.NoError(t, ln.Close())
		} else {
			// Simulate a network black hole by opening a listener but never
			// Accept()ing from it. Not quite the real thing, but close enough.
			defer func() { require.NoError(t, ln.Close()) }()
		}

		var dur time.Duration
		const n = 100
		for i := 0; i < n; i++ {
			tBegin := timeutil.Now()
			_, err = rpcCtx.GRPCDialNode(ln.Addr().String(), 1, roachpb.Locality{}, DefaultClass).
				Connect(ctx)
			require.True(t, errors.HasType(err, (*netutil.InitialHeartbeatFailedError)(nil)), "%+v", err)
			dur += timeutil.Since(tBegin)
		}
		avg := dur / n
		t.Logf("avg duration/attempt: %s", avg)
		require.Less(t, avg, 100*time.Millisecond)
	})
}
