// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracingservice"
	"github.com/stretchr/testify/require"
)

func TestTracingClientGetSpanRecordings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	args := base.TestClusterArgs{}
	tc := testcluster.StartTestCluster(t, 2 /* nodes */, args)
	defer tc.Stopper().Stop(ctx)

	localTracer := tc.Server(0).Tracer().(*tracing.Tracer)
	remoteTracer := tc.Server(1).Tracer().(*tracing.Tracer)

	traceDialer := tracingservice.NewTraceClientDialer(
		tc.Server(0).NodeDialer().(*nodedialer.Dialer),
		tc.Server(0).NodeLiveness().(*liveness.NodeLiveness), localTracer)
	localTraceID, remoteTraceID, cleanup := setupTraces(localTracer, remoteTracer)
	defer cleanup()

	t.Run("fetch-local-recordings", func(t *testing.T) {
		recordedSpan, err := traceDialer.GetSpanRecordingsFromCluster(ctx, localTraceID)
		require.NoError(t, err)

		require.NoError(t, tracing.TestingCheckRecordedSpans(recordedSpan, `
			span: root
				tags: _unfinished=1 _verbose=1
				event: structured=root
				span: root.child
					tags: _unfinished=1 _verbose=1
					span: root.child.remotechild
						tags: _unfinished=1 _verbose=1
						event: structured=root.child.remotechild
					span: root.child.remotechilddone
						tags: _verbose=1
`))
	})

	// The traceDialer is running on node 1, so most of the recordings for this
	// subtest will be passed back by node 2 over RPC.
	t.Run("fetch-remote-recordings", func(t *testing.T) {
		recordedSpan, err := traceDialer.GetSpanRecordingsFromCluster(ctx, remoteTraceID)
		require.NoError(t, err)

		require.NoError(t, tracing.TestingCheckRecordedSpans(recordedSpan, `
			span: root2
				tags: _unfinished=1 _verbose=1
				event: structured=root2
				span: root2.child
					tags: _unfinished=1 _verbose=1
					span: root2.child.remotechild
						tags: _unfinished=1 _verbose=1
					span: root2.child.remotechild2
						tags: _unfinished=1 _verbose=1
`))
	})
}
