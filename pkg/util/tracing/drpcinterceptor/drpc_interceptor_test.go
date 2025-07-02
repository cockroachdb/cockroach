// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package drpcinterceptor_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/drpcinterceptor"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/drpcinterceptor/testtracingdrpcpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"storj.io/drpc/drpcclient"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

var _ tracing.Structured = &tracingutil.TestStructuredImpl{}

type testServer struct {
	testtracingdrpcpb.DRPCTestServiceUnimplementedServer
}

// UnaryCall echoes back the received message.
func (s *testServer) UnaryCall(
	ctx context.Context, req *testtracingdrpcpb.RequestMessage,
) (*testtracingdrpcpb.ResponseMessage, error) {
	//TODO check for span in the context when server interceptors are implemented
	return &testtracingdrpcpb.ResponseMessage{
		Message: req.Message,
	}, nil
}

// ClientStreamingCall receives messages from a client stream and returns a response
// with the last received message.
func (s *testServer) ClientStreamingCall(
	stream testtracingdrpcpb.DRPCTestService_ClientStreamingCallStream,
) error {
	//TODO check for span in the context when server interceptors are implemented

	// Receive a message from the client
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	// Send response with the received message
	return stream.SendAndClose(&testtracingdrpcpb.ResponseMessage{
		Message: msg.Message,
	})
}

func TestDRPCInterceptors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name string
		// expSpanName is the expected name of the RPC spans (client-side and
		// server-side).
		expSpanName string
		do          func(
			context.Context,
			testtracingdrpcpb.DRPCTestServiceClient,
		) (*testtracingdrpcpb.ResponseMessage, error)
	}{
		{
			name: "UnaryCall",
			do: func(
				ctx context.Context,
				c testtracingdrpcpb.DRPCTestServiceClient,
			) (*testtracingdrpcpb.ResponseMessage, error) {
				req := &testtracingdrpcpb.RequestMessage{
					Message: "hello world",
				}
				log.Infof(ctx, "client UnaryCall")
				return c.UnaryCall(ctx, req)
			},
		},
		{
			name: "ClientStreamingCall",
			do: func(
				ctx context.Context,
				client testtracingdrpcpb.DRPCTestServiceClient,
			) (*testtracingdrpcpb.ResponseMessage, error) {
				log.Infof(ctx, "client ClientStreamingCall")
				sc, err := client.ClientStreamingCall(ctx)
				if err != nil {
					return nil, err
				}
				req := &testtracingdrpcpb.RequestMessage{
					Message: "hello world",
				}
				if err := sc.Send(req); err != nil {
					return nil, err
				}
				msg, err := sc.CloseAndRecv()
				sc.Close()
				return msg, err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bgCtx := context.Background()
			s := stop.NewStopper()
			defer s.Stop(bgCtx)

			tr := tracing.NewTracer()
			c1, c2 := net.Pipe()

			mux := drpcmux.New()
			err := testtracingdrpcpb.DRPCRegisterTestService(mux, &testServer{})
			require.NoError(t, err)
			server := drpcserver.New(mux)
			err = s.RunAsyncTask(bgCtx, "server", func(ctx context.Context) { _ = server.ServeOne(ctx, c1) })
			require.NoError(t, err)

			conn := drpcconn.New(c2)

			// Create ClientConn with the dialer
			clientConn, err := drpcclient.NewClientConnWithOptions(
				bgCtx,
				conn,
				drpcclient.WithChainUnaryInterceptor(drpcinterceptor.ClientInterceptorDrpc(tr, nil)),
				drpcclient.WithChainStreamInterceptor(drpcinterceptor.StreamClientInterceptorDrpc(tr, nil)),
			)
			require.NoError(t, err)

			// Create the service client using the ClientConn
			client := testtracingdrpcpb.NewDRPCTestServiceClient(clientConn)

			// Create a tracing span for the client operation
			rootCtx, rootSpan := tr.StartSpanCtx(bgCtx, "root", tracing.WithRecording(tracingpb.RecordingVerbose))
			// Execute the test case's function with the client
			response, err := tc.do(rootCtx, client)
			log.Infof(bgCtx, "response: %+v", response)
			require.NoError(t, err)
			defer func() {
				_ = clientConn.Close() // nolint:drpcconnclose
			}()
			finalRecs := rootSpan.FinishAndGetRecording(tracingpb.RecordingVerbose)
			for i := range finalRecs {
				rec := &finalRecs[i]
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
			exp := fmt.Sprintf(`
				span: root
					event: client %s
					span: /testtracingdrpcpb.TestService/%s
						tags: span.kind=client`, tc.name, tc.name)
			require.NoError(t, tracing.CheckRecordedSpans(finalRecs, exp))
			// Check that all the RPC spans (client-side) have been
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
