// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package grpcutil_test

import (
	"context"
	"net"
	"strings"
	"testing"

	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Implement the grpc health check interface (just because it's the
// simplest predefined RPC service I could find that seems unlikely to
// change out from under us) with an implementation that shuts itself
// down whenever anything calls it. This lets us distinguish errors
// caused by server shutdowns during the request from those when the
// server was already down.
type healthServer struct {
	grpcServer *grpc.Server
}

func (hs healthServer) Check(
	ctx context.Context, req *healthpb.HealthCheckRequest,
) (*healthpb.HealthCheckResponse, error) {
	hs.grpcServer.Stop()

	// Wait for the shutdown to happen before returning from this
	// method.
	<-ctx.Done()
	return nil, errors.New("no one should see this")
}

func (hs healthServer) Watch(*healthpb.HealthCheckRequest, healthpb.Health_WatchServer) error {
	panic("not implemented")
}

func TestRequestDidNotStart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.WithIssue(t, 19708)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = lis.Close()
	}()

	server := grpc.NewServer()
	hs := healthServer{server}
	healthpb.RegisterHealthServer(server, hs)
	go func() {
		_ = server.Serve(lis)
	}()

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = conn.Close() // nolint:grpcconnclose
	}()
	client := healthpb.NewHealthClient(conn)

	// The first time, the request will start and we'll get a
	// "connection is closing" message.
	_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err == nil {
		t.Fatal("did not get expected error")
	} else if grpcutil.RequestDidNotStart(err) {
		t.Fatalf("request should have started, but got %s", err)
	} else if !strings.Contains(err.Error(), "is closing") {
		// This assertion is not essential to this test, but since this
		// logic is sensitive to grpc error handling details it's safer to
		// make the test fail when anything changes. This error could be
		// either "transport is closing" or "connection is closing"
		t.Fatalf("expected 'is closing' error but got %s", err)
	}

	// Afterwards, the request will fail immediately without being sent.
	// But if we try too soon, there's a chance the transport hasn't
	// been put in the "transient failure" state yet and we get a
	// different error.
	testutils.SucceedsSoon(t, func() error {
		_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{})
		if err == nil {
			return errors.New("did not get expected error")
		} else if !grpcutil.RequestDidNotStart(err) {
			return errors.Errorf("request should not have started, but got %s", err)
		}
		return nil
	})

	// Once the transport is in the "transient failure" state it should
	// stay that way, and every subsequent request will fail
	// immediately.
	_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err == nil {
		t.Fatal("did not get expected error")
	} else if !grpcutil.RequestDidNotStart(err) {
		t.Fatalf("request should not have started, but got %s", err)
	}
}

func TestRequestDidNotStart_OpenBreaker(t *testing.T) {
	err := errors.Wrapf(circuit.ErrBreakerOpen, "unable to dial n%d", 42)
	res := grpcutil.RequestDidNotStart(err)
	assert.True(t, res)
}
