// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package nodedialer

import (
	"context"
	"net"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/rubyist/circuitbreaker"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	rpcContext *rpc.Context
	resolver   AddressResolver

	locality roachpb.Locality

	breakers syncutil.IntMap // map[roachpb.NodeID]*wrappedBreaker
}

// New initializes a Dialer.
func New(rpcContext *rpc.Context, resolver AddressResolver) *Dialer {
	return &Dialer{
		rpcContext: rpcContext,
		resolver:   resolver,
	}
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
func (n *Dialer) Dial(ctx context.Context, nodeID roachpb.NodeID) (_ *grpc.ClientConn, err error) {
	if n == nil || n.resolver == nil {
		return nil, errors.New("no node dialer configured")
	}
	breaker := n.getBreaker(nodeID)
	// If this is the first time connecting, or if connections have been failing repeatedly,
	// consider logging.
	if breaker.Successes() == 0 || breaker.ConsecFailures() > 0 {
		defer func() {
			if err != nil {
				// Enforce a minimum interval between warnings for failed connections.
				if breaker.ShouldLog() {
					log.Warningf(ctx, "unable to connect to n%d: %s", nodeID, err)
				}
			} else {
				log.Infof(ctx, "connection to n%d established", nodeID)
			}
		}()
	}

	if !breaker.Ready() {
		err := errors.Wrapf(circuit.ErrBreakerOpen, "unable to dial n%d", nodeID)
		return nil, err
	}

	addr, err := n.resolver(nodeID)
	if err != nil {
		breaker.Fail()
		return nil, err
	}
	conn, err := n.rpcContext.GRPCDial(addr.String()).Connect(ctx)
	if err != nil {
		breaker.Fail()
		return nil, err
	}
	breaker.Success()
	return conn, nil
}

type internalServerAdapter struct {
	roachpb.InternalClient
}

func (a internalServerAdapter) Batch(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	return a.InternalClient.Batch(ctx, ba)
}

var _ roachpb.InternalServer = internalServerAdapter{}

// IsLocal returns true if the given InternalServer is local.
// TODO(bdarnell): This is a bit of a hack. Once RangeFeed has
// settled, consider refactoring this so we return an object that
// wraps all knowledge of local/remote issues instead of returning the
// GRPC roachpb.InternalServer directly.
func IsLocal(iface roachpb.InternalServer) bool {
	_, ok := iface.(internalServerAdapter)
	return !ok // internalServerAdapter is used for remote connections.
}

// DialInternalServer is a specialization of Dial for callers that
// want a roachpb.InternalServer. This supports an optimization to
// bypass the network for the local node. Returns a context.Context
// which should be used when making RPC calls on the returned server
// (This context is annotated to mark this request as in-process and
// bypass ctx.Peer checks).
func (n *Dialer) DialInternalServer(
	ctx context.Context, nodeID roachpb.NodeID,
) (context.Context, roachpb.InternalServer, error) {
	if n == nil || n.resolver == nil {
		return nil, nil, errors.New("no node dialer configured")
	}
	addr, err := n.resolver(nodeID)
	if err != nil {
		return nil, nil, err
	}
	if localServer := n.rpcContext.GetLocalInternalServerForAddr(addr.String()); localServer != nil {
		log.VEvent(ctx, 2, "sending request to local server")

		// Create a new context from the existing one with the "local request" field set.
		// This tells the handler that this is an in-process request, bypassing ctx.Peer checks.
		localCtx := grpcutil.NewLocalRequestContext(ctx)

		return localCtx, localServer, nil
	}

	log.VEventf(ctx, 2, "sending request to %s", addr)
	conn, err := n.rpcContext.GRPCDial(addr.String()).Connect(ctx)
	if err != nil {
		return nil, nil, err
	}
	// TODO(bdarnell): Reconcile the different health checks and circuit
	// breaker behavior in this file
	if err := grpcutil.ConnectionReady(conn); err != nil {
		return nil, nil, err
	}
	return ctx, internalServerAdapter{roachpb.NewInternalClient(conn)}, nil
}

// ConnHealth returns nil if we have an open connection to the given node
// that succeeded on its most recent heartbeat. See the method of the same
// name on rpc.Context for more details.
func (n *Dialer) ConnHealth(nodeID roachpb.NodeID) error {
	if n == nil || n.resolver == nil {
		return errors.New("no node dialer configured")
	}
	addr, err := n.resolver(nodeID)
	if err != nil {
		return err
	}
	// TODO(bdarnell): GRPCDial should detect local addresses and return
	// a dummy connection instead of requiring callers to do this check.
	if n.rpcContext.GetLocalInternalServerForAddr(addr.String()) != nil {
		// The local server is always considered healthy.
		return nil
	}
	conn := n.rpcContext.GRPCDial(addr.String())
	return conn.Health()
}

// GetCircuitBreaker retrieves the circuit breaker for connections to the given
// node. The breaker should not be mutated as this affects all connections
// dialing to that node through this NodeDialer.
func (n *Dialer) GetCircuitBreaker(nodeID roachpb.NodeID) *circuit.Breaker {
	return n.getBreaker(nodeID).Breaker
}

func (n *Dialer) getBreaker(nodeID roachpb.NodeID) *wrappedBreaker {
	value, ok := n.breakers.Load(int64(nodeID))
	if !ok {
		breaker := &wrappedBreaker{Breaker: n.rpcContext.NewBreaker(), EveryN: log.Every(logPerNodeFailInterval)}
		value, _ = n.breakers.LoadOrStore(int64(nodeID), unsafe.Pointer(breaker))
	}
	return (*wrappedBreaker)(value)
}

type dialerAdapter Dialer

func (da *dialerAdapter) Ready(nodeID roachpb.NodeID) bool {
	return (*Dialer)(da).GetCircuitBreaker(nodeID).Ready()
}

func (da *dialerAdapter) Dial(ctx context.Context, nodeID roachpb.NodeID) (ctpb.Client, error) {
	c, err := (*Dialer)(da).Dial(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	return ctpb.NewClosedTimestampClient(c).Get(ctx)
}

var _ closedts.Dialer = (*Dialer)(nil).CTDialer()

// CTDialer wraps the NodeDialer into a closedts.Dialer.
func (n *Dialer) CTDialer() closedts.Dialer {
	return (*dialerAdapter)(n)
}
