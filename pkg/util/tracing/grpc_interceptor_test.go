// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing_test

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/grpcutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// testStructuredImpl is a testing implementation of Structured event.
type testStructuredImpl struct {
	*types.StringValue
}

var _ tracing.Structured = &testStructuredImpl{}

func (t *testStructuredImpl) String() string {
	return fmt.Sprintf("structured=%s", t.Value)
}

func newTestStructured(s string) *testStructuredImpl {
	return &testStructuredImpl{
		&types.StringValue{Value: s},
	}
}

// TestGRPCInterceptors verifies that the streaming and unary tracing
// interceptors work as advertised. We expect to see a span on the client side
// and a span on the server side.
func TestGRPCInterceptors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		magicValue = "magic-value"
	)

	checkForSpanAndReturnRecording := func(ctx context.Context) (*types.Any, error) {
		sp := tracing.SpanFromContext(ctx)
		if sp == nil {
			return nil, errors.New("no span in ctx")
		}
		sp.RecordStructured(newTestStructured(magicValue))
		recs := sp.GetRecording(tracing.RecordingVerbose)
		if len(recs) != 1 {
			return nil, errors.Newf("expected exactly one recorded span, not %+v", recs)
		}
		return types.MarshalAny(&recs[0])
	}

	s := stop.NewStopper()
	defer s.Stop(context.Background())

	tr := tracing.NewTracer()
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(tracing.ServerInterceptor(tr)),
		grpc.StreamInterceptor(tracing.StreamServerInterceptor(tr)),
	)
	impl := &grpcutils.TestServerImpl{
		UU: func(ctx context.Context, any *types.Any) (*types.Any, error) {
			return checkForSpanAndReturnRecording(ctx)
		},
		US: func(_ *types.Any, server grpcutils.GRPCTest_UnaryStreamServer) error {
			any, err := checkForSpanAndReturnRecording(server.Context())
			if err != nil {
				return err
			}
			return server.Send(any)
		},
		SU: func(server grpcutils.GRPCTest_StreamUnaryServer) error {
			_, err := server.Recv()
			if err != nil {
				return err
			}
			any, err := checkForSpanAndReturnRecording(server.Context())
			if err != nil {
				return err
			}
			return server.SendAndClose(any)
		},
		SS: func(server grpcutils.GRPCTest_StreamStreamServer) error {
			_, err := server.Recv()
			if err != nil {
				return err
			}
			any, err := checkForSpanAndReturnRecording(server.Context())
			if err != nil {
				return err
			}
			return server.Send(any)
		},
	}
	grpcutils.RegisterGRPCTestServer(srv, impl)
	defer srv.GracefulStop()
	ln, err := net.Listen(util.TestAddr.Network(), util.TestAddr.String())
	require.NoError(t, err)
	require.NoError(t, s.RunAsyncTask(context.Background(), "serve", func(ctx context.Context) {
		if err := srv.Serve(ln); err != nil {
			t.Error(err)
		}
	}))
	conn, err := grpc.DialContext(context.Background(), ln.Addr().String(),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(tracing.ClientInterceptor(tr, nil, /* init */
			func(_ context.Context) bool { return false }, /* compatibilityMode */
		)),
		grpc.WithStreamInterceptor(tracing.StreamClientInterceptor(tr, nil /* init */)),
	)
	require.NoError(t, err)
	defer func() {
		_ = conn.Close() // nolint:grpcconnclose
	}()

	c := grpcutils.NewGRPCTestClient(conn)
	unusedAny, err := types.MarshalAny(&types.Empty{})
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		do   func(context.Context) (*types.Any, error)
	}{
		{
			name: "UnaryUnary",
			do: func(ctx context.Context) (*types.Any, error) {
				return c.UnaryUnary(ctx, unusedAny)
			},
		},
		{
			name: "UnaryStream",
			do: func(ctx context.Context) (*types.Any, error) {
				sc, err := c.UnaryStream(ctx, unusedAny)
				if err != nil {
					return nil, err
				}
				any, err := sc.Recv()
				if err != nil {
					return nil, err
				}
				return any, sc.CloseSend()
			},
		},
		{
			name: "StreamUnary",
			do: func(ctx context.Context) (*types.Any, error) {
				sc, err := c.StreamUnary(ctx)
				if err != nil {
					return nil, err
				}
				if err := sc.Send(unusedAny); err != nil {
					return nil, err
				}
				return sc.CloseAndRecv()
			},
		},
		{
			name: "StreamStream",
			do: func(ctx context.Context) (*types.Any, error) {
				sc, err := c.StreamStream(ctx)
				if err != nil {
					return nil, err
				}
				if err := sc.Send(unusedAny); err != nil {
					return nil, err
				}
				return sc.Recv()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, sp := tr.StartSpanCtx(context.Background(), "root", tracing.WithRecording(tracing.RecordingVerbose))
			recAny, err := tc.do(ctx)
			require.NoError(t, err)
			var rec tracingpb.RecordedSpan
			require.NoError(t, types.UnmarshalAny(recAny, &rec))
			require.Len(t, rec.StructuredRecords, 1)
			sp.ImportRemoteSpans([]tracingpb.RecordedSpan{rec})
			var n int
			finalRecs := sp.FinishAndGetRecording(tracing.RecordingVerbose)
			sp.SetVerbose(false)
			for _, rec := range finalRecs {
				n += len(rec.StructuredRecords)
				// Remove all of the _unfinished tags. These crop up because
				// in this test we are pulling the recorder in the handler impl,
				// but the span is only closed in the interceptor. Additionally,
				// this differs between the streaming and unary interceptor, and
				// it's not worth having to have a separate expectation for each.
				// Note that we check that we're not leaking spans at the end of
				// the test.
				delete(rec.Tags, "_unfinished")
				delete(rec.Tags, "_verbose")
			}
			require.Equal(t, 1, n)

			exp := fmt.Sprintf(`
				span: root
					span: /cockroach.testutils.grpcutils.GRPCTest/%[1]s
						tags: span.kind=client
					span: /cockroach.testutils.grpcutils.GRPCTest/%[1]s
						tags: span.kind=server
						event: structured=magic-value`, tc.name)
			require.NoError(t, tracing.CheckRecordedSpans(finalRecs, exp))
		})
	}
	// Force a GC so that the finalizer for the stream client span runs and closes
	// the span. Nothing else closes that span in this test. See
	// newTracingClientStream().
	runtime.GC()
	testutils.SucceedsSoon(t, func() error {
		return tr.VisitSpans(func(sp tracing.RegistrySpan) error {
			rec := sp.GetFullRecording(tracing.RecordingVerbose)[0]
			return errors.Newf("leaked span: %s %s", rec.Operation, rec.Tags)
		})
	})
}
