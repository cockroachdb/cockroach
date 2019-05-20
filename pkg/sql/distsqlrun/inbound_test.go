// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// TestOutboxInboundStreamIntegration verifies that if an inbound stream gets
// a draining status from its consumer, that status is propagated back to the
// outbox and there are no goroutine leaks.
func TestOutboxInboundStreamIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	ni := base.NodeIDContainer{}
	ni.Set(ctx, 1)
	st := cluster.MakeTestingClusterSettings()
	mt := MakeDistSQLMetrics(time.Hour /* histogramWindow */)
	srv := NewServer(
		ctx,
		ServerConfig{
			Settings: st,
			Stopper:  stopper,
			Metrics:  &mt,
			NodeID:   &ni,
		},
	)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)

	// We're going to serve multiple node IDs with that one context. Disable node ID checks.
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	rpcSrv := rpc.NewServer(rpcContext)
	defer rpcSrv.Stop()

	distsqlpb.RegisterDistSQLServer(rpcSrv, srv)
	ln, err := netutil.ListenAndServeGRPC(stopper, rpcSrv, util.IsolatedTestAddr)
	if err != nil {
		t.Fatal(err)
	}

	// The outbox uses this stopper to run a goroutine.
	outboxStopper := stop.NewStopper()
	flowCtx := FlowCtx{
		nodeDialer: nodedialer.New(rpcContext, staticAddressResolver(ln.Addr())),
		stopper:    outboxStopper,
	}

	streamID := distsqlpb.StreamID(1)
	outbox := newOutbox(&flowCtx, staticNodeID, distsqlpb.FlowID{}, streamID)
	outbox.init(sqlbase.OneIntCol)

	// WaitGroup for the outbox and inbound stream. If the WaitGroup is done, no
	// goroutines were leaked. Grab the flow's waitGroup to avoid a copy warning.
	f := &Flow{}

	// Use RegisterFlow to register our consumer, which we will control.
	consumer := NewRowBuffer(sqlbase.OneIntCol, nil /* rows */, RowBufferArgs{})
	connectionInfo := map[distsqlpb.StreamID]*inboundStreamInfo{
		streamID: {
			receiver:  rowInboundStreamHandler{consumer},
			waitGroup: &f.waitGroup,
		},
	}
	// Add to the WaitGroup counter for the inbound stream.
	f.waitGroup.Add(1)
	require.NoError(
		t,
		srv.flowRegistry.RegisterFlow(ctx, distsqlpb.FlowID{}, f, connectionInfo, time.Hour /* timeout */),
	)

	outbox.start(ctx, &f.waitGroup, func() {})

	// Put the consumer in draining mode, this should propagate all the way back
	// from the inbound stream to the outbox when it attempts to Push a row
	// below.
	consumer.ConsumerDone()

	row := sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(0)))}

	// Now push a row to the outbox's RowChannel and expect the consumer status
	// returned to be DrainRequested. This is wrapped in a SucceedsSoon because
	// the write to the row channel is asynchronous wrt the outbox sending the
	// row and getting back the updated consumer status.
	testutils.SucceedsSoon(t, func() error {
		if cs := outbox.Push(row, nil /* meta */); cs != DrainRequested {
			return errors.Errorf("unexpected consumer status %s", cs)
		}
		return nil
	})

	// As a producer, we are now required to call ProducerDone after draining. We
	// do so now to simulate the fact that we have no more rows or metadata to
	// send.
	outbox.ProducerDone()

	// Both the outbox and the inbound stream should exit.
	f.waitGroup.Wait()

	// Wait for outstanding tasks to complete. Specifically, we are waiting for
	// the outbox's drain signal listener to return.
	outboxStopper.Quiesce(ctx)
}
