// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/stats"
)

func TestStatsHandlerBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	expResults := map[string]*Stats{
		"10.10.1.3:26257": {},
		"10.10.1.4:26257": {},
	}
	var sh StatsHandler

	ctx := context.Background()
	ctx = sh.TagConn(ctx, &stats.ConnTagInfo{
		RemoteAddr: util.NewUnresolvedAddr("tcp", "10.10.1.3:26257"),
	})
	sh.HandleRPC(ctx, &stats.InHeader{WireLength: 2})
	sh.HandleRPC(ctx, &stats.InPayload{WireLength: 3})
	sh.HandleRPC(ctx, &stats.InTrailer{WireLength: 5})
	sh.HandleRPC(ctx, &stats.End{})
	// Note that we must add 5 bytes here to account for an inaccuracy
	// in the grpc stats computations. See the comment in stats_handler.go.
	expResults["10.10.1.3:26257"].incoming += 15
	expResults["10.10.1.3:26257"].count++

	ctx = context.Background()
	ctx = sh.TagConn(ctx, &stats.ConnTagInfo{
		RemoteAddr: util.NewUnresolvedAddr("tcp", "10.10.1.4:26257"),
	})
	sh.HandleRPC(ctx, &stats.OutPayload{WireLength: 7})
	sh.HandleRPC(ctx, &stats.OutTrailer{WireLength: 11})
	expResults["10.10.1.4:26257"].outgoing += 18

	cStats1 := sh.newClient("10.10.1.3:26257")
	cStats1.HandleRPC(ctx, &stats.InHeader{WireLength: 13})
	cStats1.HandleRPC(ctx, &stats.InPayload{WireLength: 17})
	cStats1.HandleRPC(ctx, &stats.InTrailer{WireLength: 19})
	// See comment above for why we must add 5 bytes here.
	expResults["10.10.1.3:26257"].incoming += 54

	cStats2 := sh.newClient("10.10.1.4:26257")
	cStats2.HandleRPC(ctx, &stats.OutPayload{WireLength: 23})
	cStats2.HandleRPC(ctx, &stats.OutTrailer{WireLength: 29})
	expResults["10.10.1.4:26257"].outgoing += 52

	// Verify the expected results.
	sh.stats.Range(func(k, v interface{}) bool {
		key := k.(string)
		value := v.(*Stats)
		if e, a := expResults[key].Incoming(), value.Incoming(); e != a {
			t.Errorf("for target=%s, expected Incoming=%d, got %d", key, e, a)
		}
		if e, a := expResults[key].Outgoing(), value.Outgoing(); e != a {
			t.Errorf("for target=%s, expected Outgoing=%d, got %d", key, e, a)
		}
		if e, a := expResults[key].Count(), value.Count(); e != a {
			t.Errorf("for target=%s, expected Count=%d, got %d", key, e, a)
		}
		return true
	})
}

// TestStatsHandlerWithHeartbeats verifies the stats handler captures
// incoming and outgoing traffic with real server and client connections.
func TestStatsHandlerWithHeartbeats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Can't be zero because that'd be an empty offset.
	clock := hlc.NewClock(timeutil.Unix(0, 1).UnixNano, time.Nanosecond)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	serverCtx := newTestContext(clusterID, clock, stopper)
	const serverNodeID = 1
	serverCtx.NodeID.Set(context.Background(), serverNodeID)
	s := newTestServer(t, serverCtx)

	heartbeat := &ManualHeartbeatService{
		ready:              make(chan error),
		stopper:            stopper,
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
		settings:           serverCtx.Settings,
		nodeID:             &serverCtx.NodeID,
	}
	RegisterHeartbeatServer(s, heartbeat)

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	clientCtx := newTestContext(clusterID, clock, stopper)
	// Make the interval shorter to speed up the test.
	clientCtx.Config.RPCHeartbeatInterval = 1 * time.Millisecond
	go func() { heartbeat.ready <- nil }()
	if _, err := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass).
		Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Wait for the connection & successful heartbeat.
	testutils.SucceedsSoon(t, func() error {
		err := clientCtx.TestingConnHealth(remoteAddr, serverNodeID)
		if err != nil && !errors.Is(err, ErrNotHeartbeated) {
			t.Fatal(err)
		}
		return err
	})

	// Verify server and client stats in a SucceedsSoon loop to avoid
	// timing-related stats counting problems.
	testutils.SucceedsSoon(t, func() error {
		// Get server stats.
		serverSM := serverCtx.GetStatsMap()
		var serverVal interface{}
		serverSM.Range(func(k, v interface{}) bool {
			serverVal = v
			return true
		})
		if serverVal == nil {
			return fmt.Errorf("expected server map to contain stats for one client connection")
		}
		// Get client stats.
		clientSM := clientCtx.GetStatsMap()
		clientVal, ok := clientSM.Load(remoteAddr)
		if !ok {
			return fmt.Errorf("expected map to contain stats for remote addr %s", remoteAddr)
		}

		// Verify that server stats mirror client stats. Note that because
		// GRPC is no longer reporting outgoing header wire lengths, we
		// can't compare incoming and outgoing stats for equality, but are
		// forced to verify one is less than the other.
		if s, c := serverVal.(*Stats).Incoming(), clientVal.(*Stats).Outgoing(); s == 0 || c == 0 || s <= c {
			return fmt.Errorf("expected server.incoming > client.outgoing; got %d, %d", s, c)
		}
		if s, c := serverVal.(*Stats).Outgoing(), clientVal.(*Stats).Incoming(); s == 0 || c == 0 || s > c {
			return fmt.Errorf("expected server.outgoing < client.incoming; got %d, %d", s, c)
		}
		log.Infof(context.Background(), "server incoming = %v, server outgoing = %v, client incoming = %v, client outgoing = %v",
			serverVal.(*Stats).Incoming(), serverVal.(*Stats).Outgoing(), clientVal.(*Stats).Incoming(), clientVal.(*Stats).Outgoing())
		return nil
	})
}
