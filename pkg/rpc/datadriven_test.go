// Copyright 2023 The Cockroach Authors.
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
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc"
	grpcstatus "google.golang.org/grpc/status"
)

func TestReconnection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	datadriven.Walk(t, datapathutils.TestDataPath(t, t.Name()), func(t *testing.T, path string) {
		env := setupEnv(t)
		defer env.stopper.Stop(context.Background())
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			ctx, collect := tracing.ContextWithRecordingSpan(context.Background(), env.tracer, d.Cmd)

			defer collect()
			env.clock.Advance(time.Second)
			switch d.Cmd {
			case "dial":
				env.handleDial(scanClass(t, d))
			case "connect":
				env.handleConnect(ctx, scanClass(t, d))
			case "set-hb-err":
				env.handleSetHeartbeatError(true)
			case "reset-hb-err":
				env.handleSetHeartbeatError(false)
			case "soon":
				var healthy int64
				var unhealthy int64
				if d.HasArg("healthy") {
					d.ScanArgs(t, "healthy", &healthy)
				}
				if d.HasArg("unhealthy") {
					d.ScanArgs(t, "unhealthy", &unhealthy)
				}
				env.handleSoon(t, healthy, unhealthy)
			default:
				log.Eventf(ctx, "unknown command: %s\n", d.Cmd)
			}
			_ = env
			rec := collect()
			var buf strings.Builder
			for _, sp := range rec {
				for _, ent := range sp.Logs {
					msg := ent.Message
					// This is a crude hack, but it gets the job done: the trace has the
					// file:line printed at the beginning of the message in an unstructured
					// format, we need to get rid of that to have stable output.
					msg = `‹` + redact.RedactableString(regexp.MustCompile(`^‹[^ ]+ `).ReplaceAllString(string(msg), ``))
					_, _ = fmt.Fprintln(&buf, msg)
				}
			}
			if buf.Len() == 0 {
				_, _ = buf.WriteString("ok")
			}
			return buf.String()
		})
	})
}

type ddEnv struct {
	clock        *timeutil.ManualTime
	stopper      *stop.Stopper
	tracer       *tracing.Tracer
	server       *Context
	serverGRPC   *grpc.Server
	serverAddr   string
	serverNodeID roachpb.NodeID
	hbs          *ManualHeartbeatService
	hbErr        *atomic.Int32 // 0 = no error

	client *Context
	k      peerKey
}

func setupEnv(t *testing.T) *ddEnv {
	stopper := stop.NewStopper()
	tracer := tracing.NewTracer()

	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	clock := timeutil.NewManualTime(timeutil.Unix(0, 0))
	maxOffset := time.Duration(250)
	serverCtx := newTestContext(clusterID, clock, maxOffset, stopper)

	const serverNodeID = 1
	serverCtx.NodeID.Set(context.Background(), serverNodeID)
	s := newTestServer(t, serverCtx)

	hbErr := new(atomic.Int32)

	heartbeat := &ManualHeartbeatService{
		readyFn: func() error {
			if hbErr.Load() > 0 {
				return errors.New("injected error")
			}
			return nil
		},
		stopper:            stopper,
		clock:              clock,
		maxOffset:          maxOffset,
		remoteClockMonitor: serverCtx.RemoteClocks,
		version:            serverCtx.Settings.Version,
		nodeID:             serverCtx.NodeID,
	}
	RegisterHeartbeatServer(s, heartbeat)

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remoteAddr := ln.Addr().String()

	clientCtx := newTestContext(clusterID, clock, maxOffset, stopper)
	clientCtx.heartbeatInterval = 10 * time.Millisecond

	conn := clientCtx.GRPCDialNode(remoteAddr, serverNodeID, DefaultClass)
	_, err = conn.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	return &ddEnv{
		clock:        clock,
		stopper:      stopper,
		tracer:       tracer,
		server:       serverCtx,
		serverGRPC:   s,
		serverAddr:   remoteAddr,
		serverNodeID: serverNodeID,
		hbs:          heartbeat,
		hbErr:        hbErr,
		client:       clientCtx,
		k: peerKey{
			TargetAddr: remoteAddr,
			NodeID:     serverNodeID,
			Class:      DefaultClass,
		},
	}
}

func (env *ddEnv) dial(class ConnectionClass) *Connection {
	return env.client.GRPCDialNode(env.k.TargetAddr, env.k.NodeID, class)
}

func (env *ddEnv) handleDial(class ConnectionClass) {
	env.dial(class)
}

func (env *ddEnv) handleConnect(ctx context.Context, class ConnectionClass) {
	if _, err := env.dial(class).Connect(ctx); err != nil {
		// Don't log errors because it introduces too much flakiness. For example,
		// network errors look different on different systems, and in many tests
		// the heartbeat that catches an error may either be the first one or not
		// (and so sometimes there's an InitialHeartbeatFailedError, or not). That's
		// on top of error messages containing nondetermistic text.
		log.Eventf(ctx, "error code: %v", grpcstatus.Code(errors.UnwrapAll(err)))
	}
}

func (env *ddEnv) handleSetHeartbeatError(fail bool) {
	// NB: we can't put nil, so the consumer of the interceptor is
	// set up to interpret interface{}((error)(nil)) like as interface{}(nil).
	if fail {
		env.hbErr.Store(1)
	} else {
		env.hbErr.Store(0)
	}
}

func (env *ddEnv) handleSoon(t *testing.T, healthy, unhealthy int64) {
	m := env.client.Metrics()
	testutils.SucceedsSoon(t, func() error {
		return checkMetrics(m, healthy, unhealthy, true)
	})
	ps := &env.server.peers
	ps.mu.RLock()
	defer ps.mu.RUnlock()

}

func scanClass(t *testing.T, d *datadriven.TestData) ConnectionClass {
	var s string
	d.ScanArgs(t, "class", &s)
	switch s {
	case "def":
		return DefaultClass
	case "sys":
		return SystemClass
	case "rf":
		return RangefeedClass
	default:
		t.Fatalf("no such class: %s", s)
	}
	return 0 // unreachable
}
