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
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	breakers syncutil.IntMap // map[roachpb.NodeID]*wrappedBreaker
}

// New initializes a Dialer.
func New(rpcContext *rpc.Context, resolver AddressResolver) *Dialer {
	return &Dialer{
		rpcContext: rpcContext,
		resolver:   resolver,
	}
}

// Dial returns a grpc connection to the given node. It logs whenever the
// node first becomes unreachable or reachable.
func (n *Dialer) Dial(ctx context.Context, nodeID roachpb.NodeID) (_ *grpc.ClientConn, err error) {
	if n == nil {
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

// ConnHealth returns nil if we have an open connection to the given node
// that succeeded on its most recent heartbeat. See the method of the same
// name on rpc.Context for more details.
func (n *Dialer) ConnHealth(nodeID roachpb.NodeID) error {
	if n == nil {
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
