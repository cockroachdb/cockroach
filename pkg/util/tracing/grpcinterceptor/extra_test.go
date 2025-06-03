package grpcinterceptor

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/grpcinterceptor/tracingdrpcinterceptorspb"
	"github.com/stretchr/testify/require"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

// testServer implements the GRPCTest service.
type testServer struct {
	tracingdrpcinterceptorspb.DRPCTestServiceUnimplementedServer
}

// UnaryUnary echoes back the received message.
func (s *testServer) UnaryCall(
	ctx context.Context, req *tracingdrpcinterceptorspb.RequestMessage,
) (*tracingdrpcinterceptorspb.ResponseMessage, error) {
	return &tracingdrpcinterceptorspb.ResponseMessage{
		Message: req.Message,
	}, nil
}

//func TestDRPCUnaryUnary(t *testing.T) {
//	// Create a listener on a random available port.
//	lis, err := net.Listen("tcp", "127.0.0.1:0")
//	require.NoError(t, err)
//	defer lis.Close()
//
//	// Set up the DRPC multiplexer and register the service.
//	mux := drpcmux.New()
//	err = grpcutils.DRPCRegisterGRPCTest(mux, &testServer{})
//	require.NoError(t, err)
//
//	// Create and start the DRPC server.
//	server := drpcserver.New(mux)
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	go func() {
//		_ = server.Serve(ctx, lis)
//	}()
//
//	// Establish a client connection to the server.
//	conn, err := net.Dial("tcp", lis.Addr().String())
//	require.NoError(t, err)
//	defer conn.Close()
//
//	clientConn := drpcconn.New(conn)
//	client := grpcutils.NewDRPCGRPCTestClient(clientConn)
//
//	// Prepare the test request.
//	testMsg := &any.Any{
//		TypeUrl: "type.googleapis.com/test",
//		Value:   []byte("hello world"),
//	}
//
//	// Invoke the UnaryUnary method.
//	resp, err := client.UnaryUnary(context.Background(), testMsg)
//	require.NoError(t, err)
//	require.Equal(t, testMsg.TypeUrl, resp.TypeUrl)
//	require.Equal(t, testMsg.Value, resp.Value)
//}

//func TestDRPCUnaryUnary(t *testing.T) {
//	// Set up listener
//	lis, err := net.Listen("tcp", "127.0.0.1:0")
//	require.NoError(t, err)
//	defer lis.Close()
//
//	// Set up DRPC server
//	mux := drpcmux.New()
//	err = grpcutils.DRPCRegisterGRPCTest(mux, &testServer{})
//	require.NoError(t, err)
//
//	server := drpcserver.New(mux)
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	go func() {
//		if err := server.Serve(ctx, lis); err != nil {
//			log.Fatalf(ctx, "server exited with error: %v", err)
//		}
//	}()
//
//	// Ensure server is ready
//	time.Sleep(100 * time.Millisecond)
//
//	// Set up client connection
//	conn, err := net.Dial("tcp", lis.Addr().String())
//	require.NoError(t, err)
//	clientConn := drpcconn.New(conn)
//	defer clientConn.Close()
//
//	client := grpcutils.NewDRPCGRPCTestClient(clientConn)
//
//	// Use context.Background() to avoid premature cancellation
//	resp, err := client.UnaryUnary(context.Background(), &any1.Any{
//		TypeUrl: "type.googleapis.com/test",
//		Value:   []byte("hello world"),
//	})
//	require.NoError(t, err)
//	require.Equal(t, "type.googleapis.com/test", resp.TypeUrl)
//	require.Equal(t, []byte("hello world"), resp.Value)
//}

func TestDRPCUnaryUnary(t *testing.T) {
	bgCtx := context.Background()
	s := stop.NewStopper()
	c1, c2 := net.Pipe()
	mux := drpcmux.New()
	err := tracingdrpcinterceptorspb.DRPCRegisterTestService(mux, &testServer{})
	server := drpcserver.New(mux)
	s.RunAsyncTask(bgCtx, "server", func(ctx context.Context) { _ = server.ServeOne(ctx, c1) })

	// Set up client connection
	conn := drpcconn.New(c2)
	require.NoError(t, err)

	client := tracingdrpcinterceptorspb.NewDRPCTestServiceClient(conn)

	time.Sleep(time.Millisecond * 100)
	// Use context.Background() to avoid premature cancellation
	req := &tracingdrpcinterceptorspb.RequestMessage{
		Message: "hello world",
	}
	resp, err := client.UnaryCall(bgCtx, req)
	require.NoError(t, err)
	log.Infof(bgCtx, "response: %v", resp)
}
