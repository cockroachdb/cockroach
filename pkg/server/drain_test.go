// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server_test

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
)

// TestDrain tests the Drain RPC.
func TestDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	doTestDrain(t, true /* newInterface */)
}

// TestDrainLegacy tests the Drain RPC using the pre-20.1 probe signaling.
// TODO(knz): Remove this test when compatibility with pre-20.1 nodes
// is dropped.
func TestDrainLegacy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	doTestDrain(t, false /* newInterface */)
}

// doTestDrain runs the drain test.
// The parameter newInterface indicates whether to use the pre-20.1
// protocol based on "drain modes" or the post-20.1 protocol
// using discrete fields on the request object.
func doTestDrain(t *testing.T, newInterface bool) {
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		// We need to start the cluster insecure in order to not
		// care about TLS settings for the RPC client connection.
		ServerArgs: base.TestServerArgs{
			Insecure: true,
		},
	})
	defer tc.Stopper().Stop(context.TODO())

	ctx := context.TODO()

	// We'll have the RPC talk to the first node.
	c, finish, err := getAdminClientForServer(ctx, tc, 0 /* serverIdx */)
	if err != nil {
		t.Fatal(err)
	}
	defer finish()

	// Issue a probe. We're not draining yet, so the probe should
	// reflect that.
	checkDrainProbe(ctx, t, c, false /* expectedDrainStatus */)

	// Issue a drain without shutdown, so we can probe more afterwards.
	req := &serverpb.DrainRequest{Shutdown: false}
	if newInterface {
		req.DoDrain = true
	} else {
		req.DeprecatedProbeIndicator = server.DeprecatedDrainParameter
	}
	drainStream, err := c.Drain(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := getDrainResponse(t, drainStream)
	if err != nil {
		t.Fatal(err)
	}
	checkDrainStatus(t, resp, true /* expectedDrainStatus */)

	// Issue another probe. This checks that the server is still running
	// (i.e. Shutdown: false was effective) and also that the draining
	// status is still properly reported.
	checkDrainProbe(ctx, t, c, true /* expectedDrainStatus */)

	// Now issue a drain request without drain but with shutdown.
	// We're expecting the node to be shut down after that.
	req = &serverpb.DrainRequest{Shutdown: true}
	drainStream, err = c.Drain(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = getDrainResponse(t, drainStream)
	if err != nil {
		// It's possible we're getting "connection reset by peer" or some
		// gRPC initialization failure because the server is shutting
		// down. Tolerate that.
		t.Logf("RPC error: %v", err)
	}
	if resp != nil {
		checkDrainStatus(t, resp, true /* expectedDrainStatus */)
	}

	// Now expect the server to be shut down.
	testutils.SucceedsSoon(t, func() error {
		_, err := c.Drain(ctx, &serverpb.DrainRequest{Shutdown: false})
		if grpcutil.IsClosedConnection(err) {
			return nil
		}
		return fmt.Errorf("server not yet refusing RPC, got %v", err)
	})
}

// checkDrainProbe issues a drain probe and asserts that the
// server is alive. It also asserts that its drain status
// is the one expected.
func checkDrainProbe(
	ctx context.Context, t *testing.T, c serverpb.AdminClient, expectedDrainStatus bool,
) {
	// Issue a simple drain probe. This should always succeed,
	// and report the server is not currently draining.
	req := &serverpb.DrainRequest{Shutdown: false}
	drainStream, err := c.Drain(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := getDrainResponse(t, drainStream)
	if err != nil {
		t.Fatal(err)
	}
	checkDrainStatus(t, resp, expectedDrainStatus)
}

func checkDrainStatus(t *testing.T, resp *serverpb.DrainResponse, expectedDrainStatus bool) {
	if resp.IsDraining != expectedDrainStatus {
		t.Fatalf("expected node drain status to be %v, got %# v", expectedDrainStatus, pretty.Formatter(resp))
	}

	// Check that the deprecated status field is compatible with expectation.
	// TODO(knz): Remove this test when compatibility with pre-20.1 nodes
	// is dropped.
	if expectedDrainStatus {
		if !reflect.DeepEqual(resp.DeprecatedDrainStatus, server.DeprecatedDrainParameter) {
			t.Fatalf("expected compat drain status, got %# v", pretty.Formatter(resp))
		}
	} else {
		if len(resp.DeprecatedDrainStatus) > 0 {
			t.Fatalf("expected no compat drain status, got %# v", pretty.Formatter(resp))
		}
	}
}

func getDrainResponse(
	t *testing.T, stream serverpb.Admin_DrainClient,
) (*serverpb.DrainResponse, error) {
	resp, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	unexpected, err := stream.Recv()
	if err != io.EOF {
		if unexpected != nil {
			t.Fatalf("unexpected additional response: %# v // %v", pretty.Formatter(unexpected), err)
		}
		if err == nil {
			err = errors.New("unexpected response")
		}
		return nil, err
	}
	return resp, nil
}

func getAdminClientForServer(
	ctx context.Context, tc *testcluster.TestCluster, serverIdx int,
) (c serverpb.AdminClient, closer func(), err error) {
	stopper := stop.NewStopper() // stopper for the client.
	// Retrieve some parameters to initialize the client RPC context.
	cfg := tc.Server(0).RPCContext().Config
	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: execCfg.Settings.Tracer},
		cfg, execCfg.Clock, stopper, &execCfg.Settings.Version,
	)
	conn, err := rpcContext.GRPCDial(tc.Server(serverIdx).ServingAddr()).Connect(ctx)
	if err != nil {
		return nil, nil, err
	}
	return serverpb.NewAdminClient(conn), func() { stopper.Stop(ctx) }, nil
}
