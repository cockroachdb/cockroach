// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package grpcinterceptor_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/grpcutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/grpcinterceptor"
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
		recs := sp.GetRecording(tracingpb.RecordingVerbose)
		if len(recs) != 1 {
			return nil, errors.Newf("expected exactly one recorded span, not %+v", recs)
		}
		return types.MarshalAny(&recs[0])
	}

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

	unusedAny, err := types.MarshalAny(&types.Empty{})
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		// expSpanName is the expected name of the RPC spans (client-side and
		// server-side). If not specified, the test's name is used.
		expSpanName string
		do          func(context.Context, grpcutils.GRPCTestClient) (*types.Any, error)
	}{
		{
			name: "UnaryUnary",
			do: func(ctx context.Context, c grpcutils.GRPCTestClient) (*types.Any, error) {
				return c.UnaryUnary(ctx, unusedAny)
			},
		},
		{
			name: "UnaryStream",
			do: func(ctx context.Context, c grpcutils.GRPCTestClient) (*types.Any, error) {
				sc, err := c.UnaryStream(ctx, unusedAny)
				if err != nil {
					return nil, err
				}
				if err := sc.CloseSend(); err != nil {
					return nil, err
				}
				var firstResponse *types.Any
				// Consume the stream fully, as mandated by the gRPC API.
				for {
					any, err := sc.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return nil, err
					}
					if firstResponse == nil {
						firstResponse = any
					}
				}
				return firstResponse, nil
			},
		},
		{
			// Test that cancelling the client's ctx finishes the client span. The
			// client span is usually finished either when Recv() receives an error
			// (e.g. when receiving an io.EOF after exhausting the stream). But the
			// client is allowed to not read from the stream any more if it cancels
			// the ctx.
			name:        "UnaryStream_ContextCancel",
			expSpanName: "UnaryStream",
			do: func(ctx context.Context, c grpcutils.GRPCTestClient) (*types.Any, error) {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				sc, err := c.UnaryStream(ctx, unusedAny)
				if err != nil {
					return nil, err
				}
				if err := sc.CloseSend(); err != nil {
					return nil, err
				}
				return sc.Recv()
			},
		},
		{
			name: "StreamUnary",
			do: func(ctx context.Context, c grpcutils.GRPCTestClient) (*types.Any, error) {
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
			do: func(ctx context.Context, c grpcutils.GRPCTestClient) (*types.Any, error) {
				sc, err := c.StreamStream(ctx)
				if err != nil {
					return nil, err
				}
				if err := sc.Send(unusedAny); err != nil {
					return nil, err
				}
				if err := sc.CloseSend(); err != nil {
					return nil, err
				}
				var firstResponse *types.Any
				// Consume the stream fully, as mandated by the gRPC API.
				for {
					any, err := sc.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return nil, err
					}
					if firstResponse == nil {
						firstResponse = any
					}
				}
				return firstResponse, nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bgCtx := context.Background()
			s := stop.NewStopper()
			defer s.Stop(bgCtx)
			tr := tracing.NewTracer()
			srv := grpc.NewServer(
				grpc.UnaryInterceptor(grpcinterceptor.ServerInterceptor(tr)),
				grpc.StreamInterceptor(grpcinterceptor.StreamServerInterceptor(tr)),
			)
			grpcutils.RegisterGRPCTestServer(srv, impl)
			defer srv.GracefulStop()
			ln, err := net.Listen(util.TestAddr.Network(), util.TestAddr.String())
			require.NoError(t, err)
			require.NoError(t, s.RunAsyncTask(bgCtx, "serve", func(ctx context.Context) {
				if err := srv.Serve(ln); err != nil {
					t.Error(err)
				}
			}))
			conn, err := grpc.DialContext(bgCtx, ln.Addr().String(),
				//lint:ignore SA1019 grpc.WithInsecure is deprecated
				grpc.WithInsecure(),
				grpc.WithUnaryInterceptor(grpcinterceptor.ClientInterceptor(tr, nil /* init */)),
				grpc.WithStreamInterceptor(grpcinterceptor.StreamClientInterceptor(tr, nil /* init */)),
			)
			require.NoError(t, err)
			defer func() {
				_ = conn.Close() // nolint:grpcconnclose
			}()

			c := grpcutils.NewGRPCTestClient(conn)
			require.NoError(t, err)

			ctx, sp := tr.StartSpanCtx(bgCtx, "root", tracing.WithRecording(tracingpb.RecordingVerbose))
			recAny, err := tc.do(ctx, c)
			require.NoError(t, err)
			var rec tracingpb.RecordedSpan
			require.NoError(t, types.UnmarshalAny(recAny, &rec))
			require.Len(t, rec.StructuredRecords, 1)
			sp.ImportRemoteRecording([]tracingpb.RecordedSpan{rec})
			var n int
			finalRecs := sp.FinishAndGetRecording(tracingpb.RecordingVerbose)
			for i := range finalRecs {
				rec := &finalRecs[i]
				n += len(rec.StructuredRecords)
				// Remove all of the _unfinished tags. These crop up because
				// in this test we are pulling the recorder in the handler impl,
				// but the span is only closed in the interceptor. Additionally,
				// this differs between the streaming and unary interceptor, and
				// it's not worth having to have a separate expectation for each.
				// Note that we check that we're not leaking spans at the end of
				// the test.
				anonymousTagGroup := rec.FindTagGroup(tracingpb.AnonymousTagGroupName)
				if anonymousTagGroup == nil {
					continue
				}

				filteredAnonymousTagGroup := make([]tracingpb.Tag, 0)
				for _, tag := range anonymousTagGroup.Tags {
					if tag.Key == "_unfinished" {
						continue
					}
					if tag.Key == "_verbose" {
						continue
					}
					filteredAnonymousTagGroup = append(filteredAnonymousTagGroup, tag)
				}
				anonymousTagGroup.Tags = filteredAnonymousTagGroup
			}
			require.Equal(t, 1, n)

			expSpanName := tc.expSpanName
			if expSpanName == "" {
				expSpanName = tc.name
			}
			exp := fmt.Sprintf(`
				span: root
					span: /cockroach.testutils.grpcutils.GRPCTest/%[1]s
						tags: span.kind=client
					span: /cockroach.testutils.grpcutils.GRPCTest/%[1]s
						tags: span.kind=server
						event: structured=magic-value`, expSpanName)
			require.NoError(t, tracing.CheckRecordedSpans(finalRecs, exp))
			// Check that all the RPC spans (client-side and server-side) have been
			// closed. SucceedsSoon because the closing of the span is async (although
			// immediate) in the ctx cancellation subtest.
			testutils.SucceedsSoon(t, func() error {
				return tr.VisitSpans(func(sp tracing.RegistrySpan) error {
					rec := sp.GetFullRecording(tracingpb.RecordingVerbose).Root
					return errors.Newf("leaked span: %s %s", rec.Operation, rec.TagGroups)
				})
			})
		})
	}
}
