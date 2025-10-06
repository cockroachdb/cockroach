// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantdirsvr

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// ListenAndServeInMemGRPC is similar to netutil.ListenAndServeGRPC, but uses
// an in-memory listener instead.
func ListenAndServeInMemGRPC(
	ctx context.Context, stopper *stop.Stopper, server *grpc.Server,
) (*bufconn.Listener, error) {
	// defaultListenerBufSize corresponds to the listener's in-memory buffer
	// size. Since this is meant to be used within a test environment, 1 MiB
	// should be sufficient.
	const defaultListenerBufSize = 1 << 20 // 1 MiB

	// Use bufconn to create an in-memory listener.
	ln := bufconn.Listen(defaultListenerBufSize)

	stopper.AddCloser(stop.CloserFn(server.GracefulStop))
	waitQuiesce := func(context.Context) {
		<-stopper.ShouldQuiesce()
		fatalIfUnexpected(ln.Close())
	}
	if err := stopper.RunAsyncTask(ctx, "listen-quiesce", waitQuiesce); err != nil {
		waitQuiesce(ctx)
		return nil, err
	}

	if err := stopper.RunAsyncTask(ctx, "serve", func(context.Context) {
		fatalIfUnexpected(server.Serve(ln))
	}); err != nil {
		return nil, err
	}
	return ln, nil
}

// fatalIfUnexpected calls Log.Fatal(err) unless err is nil, or an error that
// comes from the net package indicating that the listener was closed or from
// the Stopper indicating quiescence.
//
// NOTE: This is the same as netutil.FatalIfUnexpected, but calls our custom
// isClosedConnection instead.
func fatalIfUnexpected(err error) {
	if err != nil && !isClosedConnection(err) && !errors.Is(err, stop.ErrUnavailable) {
		log.Fatalf(context.TODO(), "%+v", err)
	}
}

// isClosedConnection returns true if err's Cause is an error produced by gRPC
// on closed connections. This wraps grpcutil.IsClosedConnection, and includes
// a custom test for a "closed" error, which is returned by the bufconn listener
// in https://github.com/grpc/grpc-go/blob/a82cc96f/test/bufconn/bufconn.go#L49.
func isClosedConnection(err error) bool {
	return grpcutil.IsClosedConnection(err) || strings.Contains(err.Error(), "closed")
}
