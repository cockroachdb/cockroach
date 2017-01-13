// Copyright 2014 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type serverInfo struct {
	createdAt time.Time
	peerID    roachpb.NodeID
}

// server maintains an array of connected peers to which it gossips
// newly arrived information on a periodic basis.
type server struct {
	log.AmbientContext

	NodeID *base.NodeIDContainer

	stopper *stop.Stopper

	mu struct {
		syncutil.Mutex
		is       *infoStore                         // The backing infostore
		incoming nodeSet                            // Incoming client node IDs
		nodeMap  map[util.UnresolvedAddr]serverInfo // Incoming client's local address -> serverInfo
		// ready broadcasts a wakeup to waiting gossip requests. This is done
		// via closing the current ready channel and opening a new one. This
		// is required due to the fact that condition variables are not
		// composable. There's an open proposal to add them:
		// https://github.com/golang/go/issues/16620
		ready chan struct{}
	}
	tighten chan struct{} // Channel indicating that we may want to tighten the network

	nodeMetrics   Metrics
	serverMetrics Metrics

	simulationCycler *sync.Cond // Used when simulating the network to signal next cycle
}

// newServer creates and returns a server struct.
func newServer(
	ambient log.AmbientContext,
	nodeID *base.NodeIDContainer,
	stopper *stop.Stopper,
	registry *metric.Registry,
) *server {
	s := &server{
		AmbientContext: ambient,
		NodeID:         nodeID,
		stopper:        stopper,
		tighten:        make(chan struct{}, 1),
		nodeMetrics:    makeMetrics(),
		serverMetrics:  makeMetrics(),
	}

	s.mu.is = newInfoStore(s.AmbientContext, nodeID, util.UnresolvedAddr{}, stopper)
	s.mu.incoming = makeNodeSet(minPeers, metric.NewGauge(MetaConnectionsIncomingGauge))
	s.mu.nodeMap = make(map[util.UnresolvedAddr]serverInfo)
	s.mu.ready = make(chan struct{})

	registry.AddMetric(s.mu.incoming.gauge)
	registry.AddMetricStruct(s.nodeMetrics)

	return s
}

// GetNodeMetrics returns this server's node metrics struct.
func (s *server) GetNodeMetrics() *Metrics {
	return &s.nodeMetrics
}

// Gossip receives gossiped information from a peer node.
// The received delta is combined with the infostore, and this
// node's own gossip is returned to requesting client.
func (s *server) Gossip(stream Gossip_GossipServer) error {
	args, err := stream.Recv()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(s.AnnotateCtx(stream.Context()))
	defer cancel()
	syncChan := make(chan struct{}, 1)
	send := func(reply *Response) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case syncChan <- struct{}{}:
			defer func() { <-syncChan }()

			bytesSent := int64(reply.Size())
			infoCount := int64(len(reply.Delta))
			s.nodeMetrics.BytesSent.Inc(bytesSent)
			s.nodeMetrics.InfosSent.Inc(infoCount)
			s.serverMetrics.BytesSent.Inc(bytesSent)
			s.serverMetrics.InfosSent.Inc(infoCount)

			return stream.Send(reply)
		}
	}

	defer func() { syncChan <- struct{}{} }()

	doneFn, err := s.initGossipReceiver(ctx, args, send)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)

	// Starting workers in a task prevents data races during shutdown.
	if err := s.stopper.RunTask(func() {
		s.stopper.RunWorker(func() {
			errCh <- s.gossipReceiver(ctx, &args, send, stream.Recv, doneFn)
		})
	}); err != nil {
		// If we failed to start the gossip receiver, which normally takes
		// responsibility for running doneFn, we'll have to do it here.
		if doneFn != nil {
			doneFn()
		}
		return err
	}

	reply := new(Response)

	for {
		s.mu.Lock()
		// Store the old ready so that if it gets replaced with a new one
		// (once the lock is released) and is closed, we still trigger the
		// select below.
		ready := s.mu.ready
		delta := s.mu.is.delta(args.HighWaterStamps)

		if infoCount := len(delta); infoCount > 0 {
			if log.V(1) {
				log.Infof(ctx, "returning %d info(s) to node %d: %s",
					infoCount, args.NodeID, extractKeys(delta))
			}

			*reply = Response{
				NodeID:          s.NodeID.Get(),
				HighWaterStamps: s.mu.is.getHighWaterStamps(),
				Delta:           delta,
			}

			s.mu.Unlock()
			if err := send(reply); err != nil {
				return err
			}
			s.mu.Lock()
		}

		s.mu.Unlock()

		select {
		case <-s.stopper.ShouldQuiesce():
			return nil
		case err := <-errCh:
			return err
		case <-ready:
		}
	}
}

// initGossipReceiver handles the establishment of a new incoming
// connection. If it returns an error, the connection should be closed.
// If it returns a non-nil function, that function should be run to clean
// up after the connection once it's done being used.
func (s *server) initGossipReceiver(
	ctx context.Context, args *Request, senderFn func(*Response) error,
) (func(), error) {
	if args.NodeID == 0 {
		log.Infof(ctx, "received initial cluster-verification connection from %s", args.Addr)
		return nil, nil
	}

	// Decide whether or not we can accept the incoming connection
	// as a permanent peer.
	if args.NodeID == s.NodeID.Get() {
		// This is an incoming loopback connection which should be closed by
		// the client.
		if log.V(2) {
			log.Infof(ctx, "ignoring gossip from node %d (loopback)", args.NodeID)
		}
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// This is a duplicate connection from a node.
	// This is a duplicate incoming connection from the same node as an existing
	// connection. This can happen when bootstrap connections are initiated
	// through a load balancer.
	if _, ok := s.mu.nodeMap[args.Addr]; ok {
		if log.V(2) {
			log.Infof(ctx, "duplicate connection received from node %d at %s", args.NodeID, args.Addr)
		}
		return nil, errors.Errorf("duplicate connection from node at %s", args.Addr)
	}

	// If we don't have any space left, forward the client along to a peer.
	if !s.mu.incoming.hasSpace() {
		var alternateAddr util.UnresolvedAddr
		var alternateNodeID roachpb.NodeID
		// Choose a random peer for forwarding.
		altIdx := rand.Intn(len(s.mu.nodeMap))
		for addr, info := range s.mu.nodeMap {
			if altIdx == 0 {
				alternateAddr = addr
				alternateNodeID = info.peerID
				break
			}
			altIdx--
		}

		s.nodeMetrics.ConnectionsRefused.Inc(1)
		log.Infof(ctx, "refusing gossip from node %d (max %d conns); forwarding to %d (%s)",
			args.NodeID, s.mu.incoming.maxSize, alternateNodeID, alternateAddr)

		reply := &Response{
			NodeID:          s.NodeID.Get(),
			AlternateAddr:   &alternateAddr,
			AlternateNodeID: alternateNodeID,
		}

		// Unlock while sending RPC, but re-lock before returning to avoid violating
		// the caller's expectations.
		s.mu.Unlock()
		err := senderFn(reply)
		s.mu.Lock()
		return nil, err
	}

	// If we got here, we must have space to accept the incoming connection.
	if log.V(2) {
		log.Infof(ctx, "adding node %d to incoming set", args.NodeID)
	}
	s.mu.incoming.addNode(args.NodeID)
	s.mu.nodeMap[args.Addr] = serverInfo{
		peerID:    args.NodeID,
		createdAt: timeutil.Now(),
	}

	// Set up the closure to be called when the gossip connection has closed.
	nodeID := args.NodeID
	addr := args.Addr
	return func() {
		if log.V(2) {
			log.Infof(ctx, "removing node %d from incoming set", args.NodeID)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.mu.incoming.removeNode(nodeID)
		delete(s.mu.nodeMap, addr)
	}, nil
}

func (s *server) gossipReceiver(
	ctx context.Context,
	argsPtr **Request,
	senderFn func(*Response) error,
	receiverFn func() (*Request, error),
	doneFn func(),
) error {
	// Defer doneFn before s.mu.Unlock in case doneFn tries to take s.mu.
	if doneFn != nil {
		defer doneFn()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	reply := new(Response)

	// This loop receives gossip from the client. It does not attempt to send the
	// server's gossip to the client.
	for {
		args := *argsPtr

		bytesReceived := int64(args.Size())
		infosReceived := int64(len(args.Delta))
		s.nodeMetrics.BytesReceived.Inc(bytesReceived)
		s.nodeMetrics.InfosReceived.Inc(infosReceived)
		s.serverMetrics.BytesReceived.Inc(bytesReceived)
		s.serverMetrics.InfosReceived.Inc(infosReceived)

		freshCount, err := s.mu.is.combine(args.Delta, args.NodeID)
		if err != nil {
			log.Warningf(ctx, "failed to fully combine gossip delta from node %d: %s", args.NodeID, err)
		}
		if log.V(1) {
			log.Infof(ctx, "received %s from node %d (%d fresh)", extractKeys(args.Delta), args.NodeID, freshCount)
		}
		s.maybeTightenLocked()

		*reply = Response{
			NodeID:          s.NodeID.Get(),
			HighWaterStamps: s.mu.is.getHighWaterStamps(),
		}

		s.mu.Unlock()
		err = senderFn(reply)
		s.mu.Lock()
		if err != nil {
			return err
		}

		if cycler := s.simulationCycler; cycler != nil {
			cycler.Wait()
		}

		s.mu.Unlock()
		recvArgs, err := receiverFn()
		s.mu.Lock()
		if err != nil {
			return err
		}

		// *argsPtr holds the remote peer state; we need to update it whenever we
		// receive a new non-nil request. We avoid assigning to *argsPtr directly
		// because the gossip sender above has closed over *argsPtr and will NPE if
		// *argsPtr were set to nil.
		*argsPtr = recvArgs
	}
}

func (s *server) maybeTightenLocked() {
	select {
	case s.tighten <- struct{}{}:
	default:
		// Do nothing.
	}
}

// start initializes the infostore with the rpc server address and
// then begins processing connecting clients in an infinite select
// loop via goroutine. Periodically, clients connected and awaiting
// the next round of gossip are awoken via the conditional variable.
func (s *server) start(addr net.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.is.NodeAddr = util.MakeUnresolvedAddr(addr.Network(), addr.String())

	broadcast := func() {
		// Close the old ready and open a new one. This will broadcast to all
		// receivers and setup a fresh channel to replace the closed one.
		s.mu.Lock()
		defer s.mu.Unlock()
		ready := make(chan struct{})
		close(s.mu.ready)
		s.mu.ready = ready
	}

	unregister := s.mu.is.registerCallback(".*", func(_ string, _ roachpb.Value) {
		broadcast()
	})

	s.stopper.RunWorker(func() {
		<-s.stopper.ShouldQuiesce()

		s.mu.Lock()
		unregister()
		s.mu.Unlock()

		broadcast()
	})
}

func (s *server) status() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "gossip server (%d/%d cur/max conns, %s)\n",
		s.mu.incoming.gauge.Value(), s.mu.incoming.maxSize, s.serverMetrics)
	for addr, info := range s.mu.nodeMap {
		// TODO(peter): Report per connection sent/received statistics. The
		// structure of server.Gossip and server.gossipReceiver makes this
		// irritating to track.
		fmt.Fprintf(&buf, "  %d: %s (%s)\n",
			info.peerID, addr.AddressField, roundSecs(timeutil.Since(info.createdAt)))
	}
	return buf.String()
}

func roundSecs(d time.Duration) time.Duration {
	return time.Duration(d.Seconds()+0.5) * time.Second
}

// GetNodeAddr returns the node's address stored in the Infostore.
func (s *server) GetNodeAddr() *util.UnresolvedAddr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &s.mu.is.NodeAddr
}
