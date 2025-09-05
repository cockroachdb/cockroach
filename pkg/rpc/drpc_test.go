// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
	"storj.io/drpc"
	"storj.io/drpc/drpcmux"
)

type dummyStream struct{ ctx context.Context }

func (d dummyStream) Context() context.Context                  { return d.ctx }
func (d dummyStream) MsgSend(drpc.Message, drpc.Encoding) error { return nil }
func (d dummyStream) MsgRecv(drpc.Message, drpc.Encoding) error { return nil }
func (d dummyStream) CloseSend() error                          { return nil }
func (d dummyStream) Close() error                              { return nil }

func TestDRPCStopperInterceptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	st := stop.NewStopper()
	defer st.Stop(context.Background())
	ctx := context.Background()

	unaryInt := func(
		ctx context.Context, req interface{}, rpc string, handler drpcmux.UnaryHandler,
	) (interface{}, error) {
		var resp interface{}
		if err := st.RunTaskWithErr(ctx, rpc, func(ctx context.Context) error {
			var err error
			resp, err = handler(ctx, req)
			return err
		}); err != nil {
			return nil, err
		}
		return resp, nil
	}

	streamInt := func(
		stream drpc.Stream, rpc string, handler drpcmux.StreamHandler,
	) (interface{}, error) {
		var resp interface{}
		if err := st.RunTaskWithErr(stream.Context(), rpc, func(ctx context.Context) error {
			var err error
			resp, err = handler(stream)
			return err
		}); err != nil {
			return nil, err
		}
		return resp, nil
	}

	called := false
	_, err := unaryInt(ctx, nil, "rpc", func(ctx context.Context, req interface{}) (interface{}, error) {
		called = true
		return "ok", nil
	})
	require.NoError(t, err)
	require.True(t, called)

	called = false
	_, err = streamInt(dummyStream{ctx}, "rpc", func(stream drpc.Stream) (interface{}, error) {
		called = true
		return "ok", nil
	})
	require.NoError(t, err)
	require.True(t, called)

	// After Quiesce, interceptors should reject new tasks.
	st.Quiesce(ctx)

	called = false
	_, err = unaryInt(ctx, nil, "rpc", func(ctx context.Context, req interface{}) (interface{}, error) {
		called = true
		return "ok", nil
	})
	require.ErrorIs(t, err, stop.ErrUnavailable)
	require.False(t, called)

	called = false
	_, err = streamInt(dummyStream{ctx}, "rpc", func(stream drpc.Stream) (interface{}, error) {
		called = true
		return "ok", nil
	})
	require.ErrorIs(t, err, stop.ErrUnavailable)
	require.False(t, called)
}
