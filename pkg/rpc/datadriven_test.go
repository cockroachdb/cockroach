// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
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
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

/*
TestReconnection supports the following input data:
  - tick <duration>
  - dial <node> class=(def|sys|rf)
  - connect <node> class=(def|sys|rf)
  - set-hb-err <node> <decommissioned=(false|true)>
  - reset-hb-err <node>
  - soon [healthy=<n>] [unhealthy=<n>] [inactive=<n>]
    Verify that metrics in report matching number of healthy and unhealthy connections.

Time is automatically ticked by 1s per command. The environment currently sets
up n1 with two listeners, which are addressed via <node> as `n1` and `n1'`.
Additional copies of n1 or additional nodes can be added in setupEnv() or via
(to be added) DSL.
*/
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
			case "tick":
				require.Len(t, d.CmdArgs, 1)
				d, err := time.ParseDuration(d.CmdArgs[0].Key)
				require.NoError(t, err)
				env.clock.Advance(d)
				log.Eventf(ctx, "%s", env.clock.Now())
			case "dial":
				env.handleDial(env.lookupTarget(t, d.CmdArgs...), scanClass(t, d))
			case "connect":
				env.handleConnect(ctx, env.lookupTarget(t, d.CmdArgs...), scanClass(t, d))
			case "show":
				env.handleShow(ctx, env.lookupTarget(t, d.CmdArgs...), scanClass(t, d))
			case "set-hb-err":
				var dc bool
				d.MaybeScanArgs(t, "decommissioned", &dc)
				var err error
				if dc {
					err = kvpb.NewDecommissionedStatusErrorf(codes.PermissionDenied, "injected decommissioned error")
				} else {
					err = errors.New("boom")
				}

				env.handleSetHeartbeatError(err, env.lookupTargets(t, d.CmdArgs...)...)
			case "reset-hb-err":
				env.handleSetHeartbeatError(nil /* err */, env.lookupTargets(t, d.CmdArgs...)...)
			case "soon":
				var healthy int64
				var unhealthy int64
				var inactive int64
				d.MaybeScanArgs(t, "healthy", &healthy)
				d.MaybeScanArgs(t, "unhealthy", &unhealthy)
				d.MaybeScanArgs(t, "inactive", &inactive)
				if err := env.handleSoon(healthy, unhealthy, inactive); err != nil {
					t.Fatalf("%s: %v", d.Pos, err)
				}
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
					msg = redact.RedactableString(regexp.MustCompile(`^([^ ]+) (.*)`).ReplaceAllString(string(msg), `$2`))
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
	clusterID uuid.UUID
	clock     *timeutil.ManualTime
	maxOffset time.Duration
	stopper   *stop.Stopper
	tracer    *tracing.Tracer
	servers   []*ddServer
	client    *Context
}

func setupEnv(t *testing.T) *ddEnv {
	stopper := stop.NewStopper()
	tracer := tracing.NewTracer()
	tracer.SetRedactable(true)
	// Shared cluster ID by all RPC peers (this ensures that the peers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()
	clock := timeutil.NewManualTime(timeutil.Unix(0, 0))
	maxOffset := time.Duration(250)

	clientCtx := newTestContext(clusterID, clock, maxOffset, stopper)

	env := &ddEnv{
		clusterID: clusterID,
		clock:     clock,
		maxOffset: maxOffset,
		stopper:   stopper,
		tracer:    tracer,
		client:    clientCtx,
	}

	// Add two servers with NodeID 1 so that tests can simulate the case of a node
	// restarting under a new address.
	env.addServer(t, roachpb.NodeID(1))
	env.addServer(t, roachpb.NodeID(1))

	return env
}

type ddServer struct {
	nodeID     roachpb.NodeID
	addr       string
	context    *Context
	grpcServer *grpc.Server
	hbService  *ManualHeartbeatService
	hbErr      *atomic.Pointer[error]
}

func (env *ddEnv) addServer(t *testing.T, nodeID roachpb.NodeID) {
	serverCtx := newTestContext(env.clusterID, env.clock, env.maxOffset, env.stopper)
	serverCtx.NodeID.Set(context.Background(), nodeID)
	grpcServer := newTestServer(t, serverCtx)
	hbErr := new(atomic.Pointer[error])
	hbService := &ManualHeartbeatService{
		readyFn: func() error {
			if errp := hbErr.Load(); errp != nil && *errp != nil {
				return *errp
			}
			return nil
		},
		stopper:            env.stopper,
		clock:              env.clock,
		maxOffset:          env.maxOffset,
		remoteClockMonitor: serverCtx.RemoteClocks,
		version:            serverCtx.Settings.Version,
		nodeID:             serverCtx.NodeID,
	}
	RegisterHeartbeatServer(grpcServer, hbService)

	ln, err := netutil.ListenAndServeGRPC(serverCtx.Stopper, grpcServer, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}

	env.servers = append(env.servers, &ddServer{
		nodeID:     nodeID,
		addr:       ln.Addr().String(),
		context:    serverCtx,
		grpcServer: grpcServer,
		hbService:  hbService,
		hbErr:      hbErr,
	})
}

func (env *ddEnv) lookupServerWithSkip(nodeID roachpb.NodeID, skip int) *ddServer {
	// NB: this code is intentionally dumb so that it can in principle handle
	// out-of-order servers, in case we want to go dynamic at some point.
	for i := range env.servers {
		if env.servers[i].nodeID == nodeID {
			if skip > 0 {
				skip--
				continue
			}
			return env.servers[i]
		}
	}
	return nil
}

func (env *ddEnv) dial(srv *ddServer, class ConnectionClass) *Connection {
	// TODO(baptist): Fix the locality for tests.
	return env.client.GRPCDialNode(srv.addr, srv.nodeID, roachpb.Locality{}, class)
}

func (env *ddEnv) handleDial(to *ddServer, class ConnectionClass) {
	env.dial(to, class)
}

func (env *ddEnv) handleConnect(ctx context.Context, srv *ddServer, class ConnectionClass) {
	if _, err := env.dial(srv, class).Connect(ctx); err != nil {
		// Don't log errors because it introduces too much flakiness. For example,
		// network errors look different on different systems, and in many tests
		// the heartbeat that catches an error may either be the first one or not
		// (and so sometimes there's an InitialHeartbeatFailedError, or not). That's
		// on top of error messages containing nondetermistic text.
		tripped := errors.Is(err, circuit.ErrBreakerOpen)
		log.Eventf(ctx, "error code: %v [tripped=%t]", grpcstatus.Code(errors.UnwrapAll(err)), tripped)
	}
}

func (env *ddEnv) handleShow(ctx context.Context, srv *ddServer, class ConnectionClass) {
	sn, _, b, ok := env.client.peers.getWithBreaker(peerKey{NodeID: srv.nodeID, TargetAddr: srv.addr, Class: class})
	if !ok {
		log.Eventf(ctx, "%s", redact.SafeString("<nil>"))
		return
	}
	// Read tripped status without signaling probe.
	var tripped bool
	select {
	case <-b.Signal().C():
		tripped = true
	default:
	}
	// Avoid printing timestamps since they're usually not
	// deterministic; they're set by the probe but time advances
	// by 1s on each datadriven command; they are not tightly
	// synchronized.
	now := env.clock.Now()
	log.Eventf(ctx, `tripped:   %t
inactive:  %t
deletable: %t`,
		redact.Safe(tripped),
		redact.Safe(sn.deleteAfter != 0),
		redact.Safe(sn.deletable(now)))
}

func (env *ddEnv) handleSetHeartbeatError(err error, srvs ...*ddServer) {
	for _, srv := range srvs {
		srv.hbErr.Store(&err)
	}
}

func (env *ddEnv) handleSoon(healthy, unhealthy, inactive int64) error {
	m := env.client.Metrics()
	// NB: returning to caller leads to printing in output, which means failure will
	// be associated with a position in the test file. Much better than failing the
	// test using `t.Fatal`.
	return testutils.SucceedsSoonError(func() error {
		return checkMetrics(m, healthy, unhealthy, inactive, true)
	})
}

// lookupTargets looks up the servers from the slice, where they are specified
// as keys notation `n<node_id>'`. Each `'` skips one match, i.e. it n5' would
// be the second server with NodeID 5.
// Keys that don't match are ignored.
func (env *ddEnv) lookupTargets(t *testing.T, in ...datadriven.CmdArg) []*ddServer {
	var out []*ddServer
	re := regexp.MustCompile(`^n([0-9]+)('*)$`)
	for _, to := range in {
		matches := re.FindStringSubmatch(to.Key)
		if len(matches) != 3 {
			continue
		}
		nodeID, err := strconv.ParseInt(matches[1], 10, 64)
		require.NoError(t, err)
		srv := env.lookupServerWithSkip(roachpb.NodeID(nodeID), len(matches[2]))
		require.NotNil(t, srv)
		out = append(out, srv)
	}
	return out
}

// lookupTarget is like lookupTargets, but asserts that there is exactly one
// target, which is then returned.
func (env *ddEnv) lookupTarget(t *testing.T, in ...datadriven.CmdArg) *ddServer {
	srvs := env.lookupTargets(t, in...)
	require.Len(t, srvs, 1)
	return srvs[0]
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
	case "raft":
		return RaftClass
	default:
		t.Fatalf("no such class: %s", s)
	}
	return 0 // unreachable
}
