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
		k          = "test-baggage-key"
		v          = "test-baggage-value"
		magicValue = "magic-value"
	)

	checkForSpanAndReturnRecording := func(ctx context.Context) (*types.Any, error) {
		sp := tracing.SpanFromContext(ctx)
		if sp == nil {
			return nil, errors.New("no span in ctx")
		}
		actV, ok := sp.GetRecording()[0].Baggage[k]
		if !ok {
			return nil, errors.Newf("%s not set in baggage", k)
		}
		if v != actV {
			return nil, errors.Newf("expected %v, got %v instead", v, actV)
		}

		sp.RecordStructured(newTestStructured(magicValue))
		sp.SetVerbose(true) // want the tags
		recs := sp.GetRecording()
		sp.SetVerbose(false)
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
		grpc.WithUnaryInterceptor(tracing.ClientInterceptor(
			tr, func(sp *tracing.Span) {
				sp.SetBaggageItem(k, v)
			})),
		grpc.WithStreamInterceptor(tracing.StreamClientInterceptor(tr, func(sp *tracing.Span) {
			sp.SetBaggageItem(k, v)
		})),
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
			ctx, sp := tr.StartSpanCtx(context.Background(), tc.name, tracing.WithForceRealSpan())
			sp.SetVerbose(true) // to set the tags
			recAny, err := tc.do(ctx)
			require.NoError(t, err)
			var rec tracingpb.RecordedSpan
			require.NoError(t, types.UnmarshalAny(recAny, &rec))
			require.Len(t, rec.DeprecatedInternalStructured, 1)
			require.Len(t, rec.StructuredRecords, 1)
			sp.ImportRemoteSpans([]tracingpb.RecordedSpan{rec})
			sp.Finish()
			var deprecatedN int
			var n int
			finalRecs := sp.GetRecording()
			sp.SetVerbose(false)
			for _, rec := range finalRecs {
				deprecatedN += len(rec.DeprecatedInternalStructured)
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
			require.Equal(t, 1, deprecatedN)
			require.Equal(t, 1, n)

			exp := fmt.Sprintf(`
				span: %[1]s
					span: /cockroach.testutils.grpcutils.GRPCTest/%[1]s
						tags: component=gRPC span.kind=client test-baggage-key=test-baggage-value
					span: /cockroach.testutils.grpcutils.GRPCTest/%[1]s
						tags: component=gRPC span.kind=server test-baggage-key=test-baggage-value
						event: structured=magic-value`, tc.name)
			require.NoError(t, tracing.TestingCheckRecordedSpans(finalRecs, exp))
		})
	}
	testutils.SucceedsSoon(t, func() error {
		return tr.VisitSpans(func(sp *tracing.Span) error {
			return errors.Newf("leaked span: %s", sp.GetRecording()[0].Operation)
		})
	})
}
