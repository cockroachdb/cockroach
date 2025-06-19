// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"storj.io/drpc"
	"storj.io/drpc/drpcclient"
)

var unaryInterceptorCalled bool
var streamInterceptorCalled bool

// dummy unary interceptor for testing.
func mockUnaryInterceptor(
	ctx context.Context,
	rpc string,
	enc drpc.Encoding,
	in, out drpc.Message,
	cc *drpcclient.ClientConn,
	invoker drpcclient.UnaryInvoker,
) error {
	unaryInterceptorCalled = true
	return invoker(ctx, rpc, enc, in, out, cc)
}

// dummy stream interceptor for testing.
func mockStreamInterceptor(
	ctx context.Context,
	rpc string,
	enc drpc.Encoding,
	cc *drpcclient.ClientConn,
	streamer drpcclient.Streamer,
) (drpc.Stream, error) {
	streamInterceptorCalled = true
	return streamer(ctx, rpc, enc, cc)
}

func TestDialDRPC_InterceptorsAreSet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Reset flags
	unaryInterceptorCalled = false
	streamInterceptorCalled = false

	// Setup a minimal rpcCtx with interceptors
	rpcCtx := &Context{
		ContextOptions:               ContextOptions{Insecure: true},
		clientUnaryInterceptorsDRPC:  []drpcclient.UnaryClientInterceptor{mockUnaryInterceptor},
		clientStreamInterceptorsDRPC: []drpcclient.StreamClientInterceptor{mockStreamInterceptor},
	}

	// Call dialDRPC to get the connection function
	getConn := dialDRPC(rpcCtx)

	conn, err := getConn(ctx, "dummy", rpcbase.ConnectionClass(0))
	require.NoError(t, err)
	// The returned conn is a *closeEntirePoolConn wrapping a *drpcclient.ClientConn
	cepConn, ok := conn.(*closeEntirePoolConn)
	require.True(t, ok)
	cc, ok := cepConn.Conn.(*drpcclient.ClientConn)
	require.True(t, ok)
	require.NotNil(t, cc)

	// Call Invoke and NewStream to trigger interceptors
	_ = cc.Invoke(ctx, "", nil, nil, nil)
	_, _ = cc.NewStream(ctx, "", nil)

	require.True(t, unaryInterceptorCalled, "unary interceptor should be called")
	require.True(t, streamInterceptorCalled, "stream interceptor should be called")
}
