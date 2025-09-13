// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package drpcinterceptor_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/drpcinterceptor"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
	"storj.io/drpc"
	"storj.io/drpc/drpcclient"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

// TODO(nukitt): once we have a .proto for this use that.
type protoEncoding struct{}

func (protoEncoding) Marshal(msg drpc.Message) ([]byte, error) {
	return protoutil.Marshal(msg.(protoutil.Message))
}
func (protoEncoding) Unmarshal(buf []byte, msg drpc.Message) error {
	return protoutil.Unmarshal(buf, msg.(protoutil.Message))
}

// drpcDesc is a simple implementation of drpc.Description for registering a
// single RPC method. This is to simplify the test setup.
type drpcDesc struct {
	rpcName  string
	enc      drpc.Encoding
	receiver drpc.Receiver
	method   interface{}
}

func (d drpcDesc) NumMethods() int { return 1 }
func (d drpcDesc) Method(n int) (string, drpc.Encoding, drpc.Receiver, interface{}, bool) {
	if n != 0 {
		return "", nil, nil, nil, false
	}
	return d.rpcName, d.enc, d.receiver, d.method, true
}

type TestServerImpl struct {
	UU func(context.Context, *types.Any) (*types.Any, error) // Unary-Unary
	US func(*types.Any, drpc.Stream) error                   // Unary-Stream
	SU func(drpc.Stream) error                               // Stream-Unary
	SS func(drpc.Stream) error                               // Stream-Stream
}

func (s *TestServerImpl) UnaryUnary(ctx context.Context, any *types.Any) (*types.Any, error) {
	return s.UU(ctx, any)
}
func (s *TestServerImpl) UnaryStream(any *types.Any, stream drpc.Stream) error {
	return s.US(any, stream)
}
func (s *TestServerImpl) StreamUnary(stream drpc.Stream) error {
	return s.SU(stream)
}
func (s *TestServerImpl) StreamStream(stream drpc.Stream) error {
	return s.SS(stream)
}

func registerTestServer(mux *drpcmux.Mux, impl *TestServerImpl) error {
	enc := protoEncoding{}
	if err := mux.Register(impl, drpcDesc{
		rpcName: "/cockroach.testutils.drpcutils.DRPCTest/UnaryUnary",
		enc:     enc,
		method:  (*TestServerImpl).UnaryUnary,
		receiver: func(
			srv interface{},
			ctx context.Context,
			in1, in2 interface{},
		) (drpc.Message, error) {
			return srv.(*TestServerImpl).UnaryUnary(ctx, in1.(*types.Any))
		},
	}); err != nil {
		return err
	}
	if err := mux.Register(impl, drpcDesc{
		rpcName: "/cockroach.testutils.drpcutils.DRPCTest/UnaryStream",
		enc:     enc,
		method:  (*TestServerImpl).UnaryStream,
		receiver: func(
			srv interface{},
			ctx context.Context,
			in1, in2 interface{},
		) (drpc.Message, error) {
			return nil, srv.(*TestServerImpl).UnaryStream(in1.(*types.Any), in2.(drpc.Stream))
		},
	}); err != nil {
		return err
	}
	if err := mux.Register(impl, drpcDesc{
		rpcName: "/cockroach.testutils.drpcutils.DRPCTest/StreamUnary",
		enc:     enc,
		method:  (*TestServerImpl).StreamUnary,
		receiver: func(
			srv interface{},
			ctx context.Context,
			in1, in2 interface{},
		) (drpc.Message, error) {
			return nil, srv.(*TestServerImpl).StreamUnary(in1.(drpc.Stream))
		},
	}); err != nil {
		return err
	}
	return mux.Register(impl, drpcDesc{
		rpcName: "/cockroach.testutils.drpcutils.DRPCTest/StreamStream",
		enc:     enc,
		method:  (*TestServerImpl).StreamStream,
		receiver: func(
			srv interface{},
			ctx context.Context,
			in1, in2 interface{},
		) (drpc.Message, error) {
			return nil, srv.(*TestServerImpl).StreamStream(in1.(drpc.Stream))
		},
	})
}

var _ tracing.Structured = &tracingutil.TestStructuredImpl{}

// TestDRPCInterceptors provides end-to-end verification of the DRPC tracing
// interceptors. It sets up a client and a server, both configured with tracing
// interceptors, and tests all four RPC types (unary, streaming). The test
// ensures that a client-side span is correctly propagated to the server,
// creating a parent-child relationship between the client and server spans.
func TestDRPCInterceptors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const magicValue = "magic-value"

	// checkForSpanAndReturnRecording is a helper used by the server RPC
	// implementations. It verifies that a span is present in the context,
	// records a structured event into it, and returns the serialized span
	// recording. This allows the client to verify that the server-side logic
	// was executed within the correct trace.
	checkForSpanAndReturnRecording := func(ctx context.Context) (*types.Any, error) {
		sp := tracing.SpanFromContext(ctx)
		if sp == nil {
			return nil, errors.New("no span in ctx")
		}
		sp.RecordStructured(tracingutil.NewTestStructured(magicValue))
		recs := sp.GetRecording(tracingpb.RecordingVerbose)
		if len(recs) != 1 {
			return nil, errors.Newf("expected exactly one recorded span, not %+v", recs)
		}
		return types.MarshalAny(&recs[0])
	}

	// The server implementation for each RPC type will check for a span and
	// return its recording.
	impl := &TestServerImpl{
		UU: func(ctx context.Context, any *types.Any) (*types.Any, error) {
			return checkForSpanAndReturnRecording(ctx)
		},
		US: func(_ *types.Any, stream drpc.Stream) error {
			any, err := checkForSpanAndReturnRecording(stream.Context())
			if err != nil {
				return err
			}
			return stream.MsgSend(any, protoEncoding{})
		},
		SU: func(stream drpc.Stream) error {
			var req types.Any
			if err := stream.MsgRecv(&req, protoEncoding{}); err != nil {
				return err
			}
			any, err := checkForSpanAndReturnRecording(stream.Context())
			if err != nil {
				return err
			}
			return stream.MsgSend(any, protoEncoding{})
		},
		SS: func(stream drpc.Stream) error {
			var req types.Any
			if err := stream.MsgRecv(&req, protoEncoding{}); err != nil {
				return err
			}
			any, err := checkForSpanAndReturnRecording(stream.Context())
			if err != nil {
				return err
			}
			return stream.MsgSend(any, protoEncoding{})
		},
	}

	unusedAny, err := types.MarshalAny(&types.Empty{})
	require.NoError(t, err)

	uuRPC := "/cockroach.testutils.drpcutils.DRPCTest/UnaryUnary"
	usRPC := "/cockroach.testutils.drpcutils.DRPCTest/UnaryStream"
	suRPC := "/cockroach.testutils.drpcutils.DRPCTest/StreamUnary"
	ssRPC := "/cockroach.testutils.drpcutils.DRPCTest/StreamStream"

	// Define the test cases for each RPC type.
	for _, tc := range []struct {
		name string
		do   func(context.Context, *drpcclient.ClientConn) (*types.Any, error)
	}{
		{
			name: "UnaryUnary",
			do: func(ctx context.Context, c *drpcclient.ClientConn) (*types.Any, error) {
				out := new(types.Any)
				err := c.Invoke(ctx, uuRPC, protoEncoding{}, unusedAny, out)
				return out, err
			},
		},
		{
			name: "UnaryStream",
			do: func(ctx context.Context, c *drpcclient.ClientConn) (*types.Any, error) {
				sc, err := c.NewStream(ctx, usRPC, protoEncoding{})
				if err != nil {
					return nil, err
				}
				if err := sc.MsgSend(unusedAny, protoEncoding{}); err != nil {
					return nil, err
				}
				if err := sc.CloseSend(); err != nil {
					return nil, err
				}
				var first *types.Any
				for {
					any := new(types.Any)
					if err := sc.MsgRecv(any, protoEncoding{}); err != nil {
						if err == io.EOF {
							break
						}
						return nil, err
					}
					if first == nil {
						first = any
					}
				}
				return first, nil
			},
		},
		{
			name: "StreamUnary",
			do: func(ctx context.Context, c *drpcclient.ClientConn) (*types.Any, error) {
				sc, err := c.NewStream(ctx, suRPC, protoEncoding{})
				if err != nil {
					return nil, err
				}
				if err := sc.MsgSend(unusedAny, protoEncoding{}); err != nil {
					return nil, err
				}
				if err := sc.CloseSend(); err != nil {
					return nil, err
				}
				out := new(types.Any)
				// The single response message is received here.
				if err := sc.MsgRecv(out, protoEncoding{}); err != nil {
					return nil, err
				}
				if err := sc.Close(); err != nil {
					return nil, err
				}
				return out, nil
			},
		},
		{
			name: "StreamStream",
			do: func(ctx context.Context, c *drpcclient.ClientConn) (*types.Any, error) {
				sc, err := c.NewStream(ctx, ssRPC, protoEncoding{})
				if err != nil {
					return nil, err
				}
				if err := sc.MsgSend(unusedAny, protoEncoding{}); err != nil {
					return nil, err
				}
				if err := sc.CloseSend(); err != nil {
					return nil, err
				}
				var first *types.Any
				for {
					any := new(types.Any)
					if err := sc.MsgRecv(any, protoEncoding{}); err != nil {
						if err == io.EOF {
							break
						}
						return nil, err
					}
					if first == nil {
						first = any
					}
				}
				if err := sc.Close(); err != nil {
					return nil, err
				}
				return first, nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Setup: Create a tracer, server mux with interceptors, and a server.
			bgCtx := context.Background()
			tr := tracing.NewTracer()
			mux := drpcmux.NewWithInterceptors(
				[]drpcmux.UnaryServerInterceptor{drpcinterceptor.ServerInterceptor(tr)},
				[]drpcmux.StreamServerInterceptor{drpcinterceptor.StreamServerInterceptor(tr)},
			)
			require.NoError(t, registerTestServer(mux, impl))
			srv := drpcserver.New(mux)
			ln, err := net.Listen(util.TestAddr.Network(), util.TestAddr.String())
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(bgCtx)
			defer cancel()
			go func() {
				// Serve requests in a background goroutine.
				if err := srv.Serve(ctx, ln); err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
			}()

			rawconn, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
			require.NoError(t, err)
			conn := drpcconn.New(rawconn)
			cc, err := drpcclient.NewClientConnWithOptions(bgCtx, conn,
				drpcclient.WithChainUnaryInterceptor(
					drpcinterceptor.ClientInterceptor(tr, nil)),
				drpcclient.WithChainStreamInterceptor(
					drpcinterceptor.StreamClientInterceptor(tr, nil)),
			)
			require.NoError(t, err)
			defer func() { _ = conn.Close() }()

			ctxSpan, sp := tr.StartSpanCtx(
				bgCtx,
				"root",
				tracing.WithRecording(tracingpb.RecordingVerbose),
			)
			recAny, err := tc.do(ctxSpan, cc)
			require.NoError(t, err)

			var rec tracingpb.RecordedSpan
			require.NoError(t, types.UnmarshalAny(recAny, &rec))
			require.Len(t, rec.StructuredRecords, 1)

			sp.ImportRemoteRecording([]tracingpb.RecordedSpan{rec})
			finalRecs := sp.FinishAndGetRecording(tracingpb.RecordingVerbose)

			var n int
			for i := range finalRecs {
				rec := &finalRecs[i]
				n += len(rec.StructuredRecords)
				anonymousTagGroup := rec.FindTagGroup(tracingpb.AnonymousTagGroupName)
				if anonymousTagGroup == nil {
					continue
				}
				filtered := make([]tracingpb.Tag, 0, len(anonymousTagGroup.Tags))
				for _, tag := range anonymousTagGroup.Tags {
					if tag.Key == "_unfinished" || tag.Key == "_verbose" {
						continue
					}
					filtered = append(filtered, tag)
				}
				anonymousTagGroup.Tags = filtered
			}
			require.Equal(t, 1, n)

			expSpanName := tc.name
			exp := fmt.Sprintf(`
                span: root
                    span: /cockroach.testutils.drpcutils.DRPCTest/%[1]s
                        tags: span.kind=client
                    span: /cockroach.testutils.drpcutils.DRPCTest/%[1]s
                        tags: span.kind=server
                        event: structured=magic-value`, expSpanName)
			require.NoError(t, tracing.CheckRecordedSpans(finalRecs, exp))

			// Check that no spans were leaked.
			testutils.SucceedsSoon(t, func() error {
				return tr.VisitSpans(func(sp tracing.RegistrySpan) error {
					rec := sp.GetFullRecording(tracingpb.RecordingVerbose).Root
					return errors.Newf("leaked span: %s %s", rec.Operation, rec.TagGroups)
				})
			})
		})
	}
}
