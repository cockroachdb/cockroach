// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nodedialer

import (
	"context"
	"fmt"
	"net"
	"time"
	"unsafe"

	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// No more than one failure to connect to a given node will be logged in the given interval.
const logPerNodeFailInterval = time.Minute

type wrappedBreaker struct {
	*circuit.Breaker
	log.EveryN
}

// An AddressResolver translates NodeIDs into addresses.
type AddressResolver func(roachpb.NodeID) (net.Addr, error)

// A Dialer wraps an *rpc.Context for dialing based on node IDs. For each node,
// it maintains a circuit breaker that prevents rapid connection attempts and
// provides hints to the callers on whether to log the outcome of the operation.
type Dialer struct {
	rpcContext   *rpc.Context
	resolver     AddressResolver
	testingKnobs DialerTestingKnobs

	breakers [rpc.NumConnectionClasses]syncutil.IntMap // map[roachpb.NodeID]*wrappedBreaker
}

// DialerOpt contains configuration options for a Dialer.
type DialerOpt struct {
	// TestingKnobs contains testing utilities.
	TestingKnobs DialerTestingKnobs
}

// DialerTestingKnobs contains dialer testing options.
type DialerTestingKnobs struct {
	// TestingNoLocalClientOptimization, if set, disables the optimization about
	// using a direct client for the local node instead of going through gRPC. For
	// one, the behavior on cancellation of the client RPC ctx is different: when
	// going through gRPC, the framework watches for client ctx cancellation and
	// interrupts the RPC. When bypassing gRPC, the client ctx is passed directly
	// to the RPC handler.
	TestingNoLocalClientOptimization bool
}

// ModuleTestingKnobs implements the ModuleTestingKnobs interface.
func (DialerTestingKnobs) ModuleTestingKnobs() {}

// New initializes a Dialer.
func New(rpcContext *rpc.Context, resolver AddressResolver) *Dialer {
	return &Dialer{
		rpcContext: rpcContext,
		resolver:   resolver,
	}
}

// NewWithOpt initializes a Dialer and allows passing in configuration options.
func NewWithOpt(rpcContext *rpc.Context, resolver AddressResolver, opt DialerOpt) *Dialer {
	d := New(rpcContext, resolver)
	d.testingKnobs = opt.TestingKnobs
	return d
}

// Stopper returns this node dialer's Stopper.
// TODO(bdarnell): This is a bit of a hack for kv/transport_race.go
func (n *Dialer) Stopper() *stop.Stopper {
	return n.rpcContext.Stopper
}

// Silence lint warning because this method is only used in race builds.
var _ = (*Dialer).Stopper

// Dial returns a grpc connection to the given node. It logs whenever the
// node first becomes unreachable or reachable.
func (n *Dialer) Dial(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (_ *grpc.ClientConn, err error) {
	if n == nil || n.resolver == nil {
		return nil, errors.New("no node dialer configured")
	}
	// Don't trip the breaker if we're already canceled.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, errors.Wrap(ctxErr, "dial")
	}
	breaker := n.getBreaker(nodeID, class)
	addr, err := n.resolver(nodeID)
	if err != nil {
		err = errors.Wrapf(err, "failed to resolve n%d", nodeID)
		breaker.Fail(err)
		return nil, err
	}
	return n.dial(ctx, nodeID, addr, breaker, true /* checkBreaker */, class)
}

// DialNoBreaker is like Dial, but will not check the circuit breaker before
// trying to connect. The breaker is notified of the outcome. This function
// should only be used when there is good reason to believe that the node is
// reachable.
func (n *Dialer) DialNoBreaker(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (_ *grpc.ClientConn, err error) {
	if n == nil || n.resolver == nil {
		return nil, errors.New("no node dialer configured")
	}
	addr, err := n.resolver(nodeID)
	if err != nil {
		if ctx.Err() == nil {
			n.getBreaker(nodeID, class).Fail(err)
		}
		return nil, err
	}
	return n.dial(ctx, nodeID, addr, n.getBreaker(nodeID, class), false /* checkBreaker */, class)
}

// DialInternalClient is a specialization of DialClass for callers that
// want a kvpb.InternalClient. This supports an optimization to bypass the
// network for the local node.
//
// For a more contextualized explanation, see the comment that decorates
// (*rpc.Context).loopbackDialFn.
func (n *Dialer) DialInternalClient(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (rpc.RestrictedInternalClient, error) {
	if n == nil || n.resolver == nil {
		return nil, errors.New("no node dialer configured")
	}
	{
		// If we're dialing the local node, don't go through gRPC.
		localClient := n.rpcContext.GetLocalInternalClientForAddr(nodeID)
		if localClient != nil && !n.testingKnobs.TestingNoLocalClientOptimization {
			log.VEvent(ctx, 2, kvbase.RoutingRequestLocallyMsg)
			return localClient, nil
		}
	}

	addr, err := n.resolver(nodeID)
	if err != nil {
		return nil, errors.Wrap(err, "resolver error")
	}
	log.VEventf(ctx, 2, "sending request to %s", addr)
	conn, err := n.dial(ctx, nodeID, addr, n.getBreaker(nodeID, class), true /* checkBreaker */, class)
	if err != nil {
		return nil, err
	}
	return TracingInternalClient{InternalClient: kvpb.NewInternalClient(conn)}, nil
}

// dial performs the dialing of the remote connection. If breaker is nil,
// then perform this logic without using any breaker functionality.
func (n *Dialer) dial(
	ctx context.Context,
	nodeID roachpb.NodeID,
	addr net.Addr,
	breaker *wrappedBreaker,
	checkBreaker bool,
	class rpc.ConnectionClass,
) (_ *grpc.ClientConn, err error) {
	const ctxWrapMsg = "dial"
	// Don't trip the breaker if we're already canceled.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, errors.Wrap(ctxErr, ctxWrapMsg)
	}
	if checkBreaker && !breaker.Ready() {
		err = errors.Wrapf(circuit.ErrBreakerOpen, "unable to dial n%d", nodeID)
		return nil, err
	}
	defer func() {
		// Enforce a minimum interval between warnings for failed connections.
		if err != nil && ctx.Err() == nil && breaker != nil && breaker.ShouldLog() {
			log.Health.Warningf(ctx, "unable to connect to n%d: %s", nodeID, err)
		}
	}()
	conn, err := n.rpcContext.GRPCDialNode(addr.String(), nodeID, class).Connect(ctx)
	if err != nil {
		// If we were canceled during the dial, don't trip the breaker.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, errors.Wrap(ctxErr, ctxWrapMsg)
		}
		err = errors.Wrapf(err, "failed to connect to n%d at %v", nodeID, addr)
		if breaker != nil {
			breaker.Fail(err)
		}
		return nil, err
	}

	// TODO(bdarnell): Reconcile the different health checks and circuit breaker
	// behavior in this file. Note that this different behavior causes problems
	// for higher-levels in the system. For example, DistSQL checks for
	// ConnHealth when scheduling processors, but can then see attempts to send
	// RPCs fail when dial fails due to an open breaker. Reset the breaker here
	// as a stop-gap before the reconciliation occurs.
	if breaker != nil {
		breaker.Success()
	}
	return conn, nil
}

// ConnHealth returns nil if we have an open connection of the request
// class to the given node that succeeded on its most recent heartbeat.
// Returns circuit.ErrBreakerOpen if the breaker is tripped, otherwise
// ErrNoConnection if no connection to the node currently exists.
func (n *Dialer) ConnHealth(nodeID roachpb.NodeID, class rpc.ConnectionClass) error {
	if n == nil || n.resolver == nil {
		return errors.New("no node dialer configured")
	}
	// NB: Don't call Ready(). The breaker protocol would require us to follow
	// that up with a dial, which we won't do as this is called in hot paths.
	if n.getBreaker(nodeID, class).Tripped() {
		return circuit.ErrBreakerOpen
	}
	addr, err := n.resolver(nodeID)
	if err != nil {
		return err
	}
	return n.rpcContext.ConnHealth(addr.String(), nodeID, class)
}

// ConnHealthTryDial returns nil if we have an open connection of the request
// class to the given node that succeeded on its most recent heartbeat. If no
// healthy connection is found, it will attempt to dial the node.
//
// This exists for components that do not themselves actively maintain RPC
// connections to remote nodes, e.g. DistSQL. However, it can cause significant
// latency if the remote node is unresponsive (e.g. if the server/VM is shut
// down), and should be avoided in latency-sensitive code paths. Preferably,
// this should be replaced by some other mechanism to maintain RPC connections.
// See also: https://github.com/cockroachdb/cockroach/issues/70111
// TODO(baptist): This method is poorly named and confusing. It is used as a
// "hint" to use a connection if it already exists, but simultaneously kick off
// a connection attempt in the background if it doesn't and always return
// immediately. It is only used today by DistSQL and it should probably be
// removed and moved into that code.
func (n *Dialer) ConnHealthTryDial(nodeID roachpb.NodeID, class rpc.ConnectionClass) error {
	err := n.ConnHealth(nodeID, class)
	if err == nil || !n.getBreaker(nodeID, class).Ready() {
		return err
	}
	addr, err := n.resolver(nodeID)
	if err != nil {
		return err
	}
	// NB: This will always return `ErrNotHeartbeated` since the heartbeat will
	// not be done by the time `Health` is called since GRPCDialNode is async.
	return n.rpcContext.GRPCDialNode(addr.String(), nodeID, class).Health()
}

// GetCircuitBreaker retrieves the circuit breaker for connections to the
// given node. The breaker should not be mutated as this affects all connections
// dialing to that node through this NodeDialer.
func (n *Dialer) GetCircuitBreaker(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) *circuit.Breaker {
	return n.getBreaker(nodeID, class).Breaker
}

func (n *Dialer) getBreaker(nodeID roachpb.NodeID, class rpc.ConnectionClass) *wrappedBreaker {
	breakers := &n.breakers[class]
	value, ok := breakers.Load(int64(nodeID))
	if !ok {
		name := fmt.Sprintf("rpc %v [n%d]", n.rpcContext.Config.Addr, nodeID)
		breaker := &wrappedBreaker{Breaker: n.rpcContext.NewBreaker(name), EveryN: log.Every(logPerNodeFailInterval)}
		value, _ = breakers.LoadOrStore(int64(nodeID), unsafe.Pointer(breaker))
	}
	return (*wrappedBreaker)(value)
}

// Latency returns the exponentially weighted moving average latency to the
// given node ID. Returns a latency of 0 with no error if we don't have enough
// samples to compute a reliable average.
func (n *Dialer) Latency(nodeID roachpb.NodeID) (time.Duration, error) {
	if n == nil || n.resolver == nil {
		return 0, errors.AssertionFailedf("no node dialer configured")
	}
	if n.rpcContext.RemoteClocks == nil {
		return 0, errors.AssertionFailedf("can't call Latency in a client command")
	}
	latency, ok := n.rpcContext.RemoteClocks.Latency(nodeID)
	if !ok {
		latency = 0
	}
	return latency, nil
}

// TracingInternalClient wraps an InternalClient and fills in trace information
// on Batch RPCs.
//
// Note that TracingInternalClient is not used to wrap the internalClientAdapter
// - local RPCs don't need this tracing functionality.
type TracingInternalClient struct {
	kvpb.InternalClient
}

// Batch overrides the Batch RPC client method and fills in tracing information.
func (tic TracingInternalClient) Batch(
	ctx context.Context, ba *kvpb.BatchRequest, opts ...grpc.CallOption,
) (*kvpb.BatchResponse, error) {
	sp := tracing.SpanFromContext(ctx)
	if sp != nil && !sp.IsNoop() {
		ba = ba.ShallowCopy()
		ba.TraceInfo = sp.Meta().ToProto()
	}
	return tic.InternalClient.Batch(ctx, ba, opts...)
}
