// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossip

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// client is a client-side RPC connection to a gossip peer node.
type client struct {
	log.AmbientContext

	createdAt             time.Time
	peerID                roachpb.NodeID           // Peer node ID; 0 until first gossip response
	resolvedPlaceholder   bool                     // Whether we've resolved the nodeSet's placeholder for this client
	addr                  net.Addr                 // Peer node network address
	forwardAddr           *util.UnresolvedAddr     // Set if disconnected with an alternate addr
	remoteHighWaterStamps map[roachpb.NodeID]int64 // Remote server's high water timestamps
	closer                chan struct{}            // Client shutdown channel
	clientMetrics         Metrics
	nodeMetrics           Metrics
}

// extractKeys returns a string representation of a gossip delta's keys.
func extractKeys(delta map[string]*Info) string {
	keys := make([]string, 0, len(delta))
	for key := range delta {
		keys = append(keys, key)
	}
	return fmt.Sprintf("%s", keys)
}

// newClient creates and returns a client struct.
func newClient(ambient log.AmbientContext, addr net.Addr, nodeMetrics Metrics) *client {
	return &client{
		AmbientContext:        ambient,
		createdAt:             timeutil.Now(),
		addr:                  addr,
		remoteHighWaterStamps: map[roachpb.NodeID]int64{},
		closer:                make(chan struct{}),
		clientMetrics:         makeMetrics(),
		nodeMetrics:           nodeMetrics,
	}
}

// start dials the remote addr and commences gossip once connected. Upon exit,
// the client is sent on the disconnected channel. This method starts client
// processing in a goroutine and returns immediately.
func (c *client) startLocked(
	g *Gossip,
	disconnected chan *client,
	rpcCtx *rpc.Context,
	stopper *stop.Stopper,
	breaker *circuit.Breaker,
) {
	// Add a placeholder for the new outgoing connection because we may not know
	// the ID of the node we're connecting to yet. This will be resolved in
	// (*client).handleResponse once we know the ID.
	g.outgoing.addPlaceholder()

	ctx, cancel := context.WithCancel(c.AnnotateCtx(context.Background()))
	if err := stopper.RunAsyncTask(ctx, "gossip-client", func(ctx context.Context) {
		var wg sync.WaitGroup
		defer func() {
			// This closes the outgoing stream, causing any attempt to send or
			// receive to return an error.
			//
			// Note: it is still possible for incoming gossip to be processed after
			// this point.
			cancel()

			// The stream is closed, but there may still be some incoming gossip
			// being processed. Wait until that is complete to avoid racing the
			// client's removal against the discovery of its remote's node ID.
			wg.Wait()
			disconnected <- c
		}()

		consecFailures := breaker.ConsecFailures()
		var stream Gossip_GossipClient
		if err := breaker.Call(func() error {
			// Note: avoid using `grpc.WithBlock` here. This code is already
			// asynchronous from the caller's perspective, so the only effect of
			// `WithBlock` here is blocking shutdown - at the time of this writing,
			// that ends ups up making `kv` tests take twice as long.
			conn, err := rpcCtx.GRPCUnvalidatedDial(c.addr.String()).Connect(ctx)
			if err != nil {
				return err
			}
			if stream, err = NewGossipClient(conn).Gossip(ctx); err != nil {
				return err
			}
			return c.requestGossip(g, stream)
		}, 0); err != nil {
			if consecFailures == 0 {
				log.Warningf(ctx, "failed to start gossip client to %s: %s", c.addr, err)
			}
			return
		}

		// Start gossiping.
		log.Infof(ctx, "started gossip client to n%d (%s)", c.peerID, c.addr)
		if err := c.gossip(ctx, g, stream, stopper, &wg); err != nil {
			if !grpcutil.IsClosedConnection(err) {
				g.mu.RLock()
				if c.peerID != 0 {
					log.Infof(ctx, "closing client to n%d (%s): %s", c.peerID, c.addr, err)
				} else {
					log.Infof(ctx, "closing client to %s: %s", c.addr, err)
				}
				g.mu.RUnlock()
			}
		}
	}); err != nil {
		disconnected <- c
	}
}

// close stops the client gossip loop and returns immediately.
func (c *client) close() {
	select {
	case <-c.closer:
	default:
		close(c.closer)
	}
}

// requestGossip requests the latest gossip from the remote server by
// supplying a map of this node's knowledge of other nodes' high water
// timestamps.
func (c *client) requestGossip(g *Gossip, stream Gossip_GossipClient) error {
	g.mu.RLock()
	args := &Request{
		NodeID:          g.NodeID.Get(),
		Addr:            g.mu.is.NodeAddr,
		HighWaterStamps: g.mu.is.getHighWaterStamps(),
		ClusterID:       g.clusterID.Get(),
	}
	g.mu.RUnlock()

	bytesSent := int64(args.Size())
	c.clientMetrics.BytesSent.Inc(bytesSent)
	c.nodeMetrics.BytesSent.Inc(bytesSent)

	return stream.Send(args)
}

// sendGossip sends the latest gossip to the remote server, based on
// the remote server's notion of other nodes' high water timestamps.
func (c *client) sendGossip(g *Gossip, stream Gossip_GossipClient, firstReq bool) error {
	g.mu.Lock()
	delta := g.mu.is.delta(c.remoteHighWaterStamps)
	if firstReq {
		g.mu.is.populateMostDistantMarkers(delta)
	}
	if len(delta) > 0 {
		// Ensure that the high water stamps for the remote server are kept up to
		// date so that we avoid resending the same gossip infos as infos are
		// updated locally.
		for _, i := range delta {
			ratchetHighWaterStamp(c.remoteHighWaterStamps, i.NodeID, i.OrigStamp)
		}

		args := Request{
			NodeID:          g.NodeID.Get(),
			Addr:            g.mu.is.NodeAddr,
			Delta:           delta,
			HighWaterStamps: g.mu.is.getHighWaterStamps(),
			ClusterID:       g.clusterID.Get(),
		}

		bytesSent := int64(args.Size())
		infosSent := int64(len(delta))
		c.clientMetrics.BytesSent.Inc(bytesSent)
		c.clientMetrics.InfosSent.Inc(infosSent)
		c.nodeMetrics.BytesSent.Inc(bytesSent)
		c.nodeMetrics.InfosSent.Inc(infosSent)

		if log.V(1) {
			ctx := c.AnnotateCtx(stream.Context())
			if c.peerID != 0 {
				log.Infof(ctx, "sending %s to n%d (%s)", extractKeys(args.Delta), c.peerID, c.addr)
			} else {
				log.Infof(ctx, "sending %s to %s", extractKeys(args.Delta), c.addr)
			}
		}

		g.mu.Unlock()
		return stream.Send(&args)
	}
	g.mu.Unlock()
	return nil
}

// handleResponse handles errors, remote forwarding, and combines delta
// gossip infos from the remote server with this node's infostore.
func (c *client) handleResponse(ctx context.Context, g *Gossip, reply *Response) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	bytesReceived := int64(reply.Size())
	infosReceived := int64(len(reply.Delta))
	c.clientMetrics.BytesReceived.Inc(bytesReceived)
	c.clientMetrics.InfosReceived.Inc(infosReceived)
	c.nodeMetrics.BytesReceived.Inc(bytesReceived)
	c.nodeMetrics.InfosReceived.Inc(infosReceived)

	// Combine remote node's infostore delta with ours.
	if reply.Delta != nil {
		freshCount, err := g.mu.is.combine(reply.Delta, reply.NodeID)
		if err != nil {
			log.Warningf(ctx, "failed to fully combine delta from n%d: %s", reply.NodeID, err)
		}
		if infoCount := len(reply.Delta); infoCount > 0 {
			if log.V(1) {
				log.Infof(ctx, "received %s from n%d (%d fresh)", extractKeys(reply.Delta), reply.NodeID, freshCount)
			}
		}
		g.maybeTightenLocked()
	}
	c.peerID = reply.NodeID
	mergeHighWaterStamps(&c.remoteHighWaterStamps, reply.HighWaterStamps)

	// If we haven't yet recorded which node ID we're connected to in the outgoing
	// nodeSet, do so now. Note that we only want to do this if the peer has a
	// node ID allocated (i.e. if it's nonzero), because otherwise it could change
	// after we record it.
	if !c.resolvedPlaceholder && c.peerID != 0 {
		c.resolvedPlaceholder = true
		g.outgoing.resolvePlaceholder(c.peerID)
	}

	// Handle remote forwarding.
	if reply.AlternateAddr != nil {
		if g.hasIncomingLocked(reply.AlternateNodeID) || g.hasOutgoingLocked(reply.AlternateNodeID) {
			return errors.Errorf(
				"received forward from n%d to n%d (%s); already have active connection, skipping",
				reply.NodeID, reply.AlternateNodeID, reply.AlternateAddr)
		}
		// We try to resolve the address, but don't actually use the result.
		// The certificates (if any) may only be valid for the unresolved
		// address.
		if _, err := reply.AlternateAddr.Resolve(); err != nil {
			return errors.Errorf("unable to resolve alternate address %s for n%d: %s",
				reply.AlternateAddr, reply.AlternateNodeID, err)
		}
		c.forwardAddr = reply.AlternateAddr
		return errors.Errorf("received forward from n%d to n%d (%s)",
			reply.NodeID, reply.AlternateNodeID, reply.AlternateAddr)
	}

	// Check whether we're connected at this point.
	g.signalConnectedLocked()

	// Check whether this outgoing client is duplicating work already
	// being done by an incoming client, either because an outgoing
	// matches an incoming or the client is connecting to itself.
	if nodeID := g.NodeID.Get(); nodeID == c.peerID {
		return errors.Errorf("stopping outgoing client to n%d (%s); loopback connection", c.peerID, c.addr)
	} else if g.hasIncomingLocked(c.peerID) && nodeID > c.peerID {
		// To avoid mutual shutdown, we only shutdown our client if our
		// node ID is higher than the peer's.
		return errors.Errorf("stopping outgoing client to n%d (%s); already have incoming", c.peerID, c.addr)
	}

	return nil
}

// gossip loops, sending deltas of the infostore and receiving deltas
// in turn. If an alternate is proposed on response, the client addr
// is modified and method returns for forwarding by caller.
func (c *client) gossip(
	ctx context.Context,
	g *Gossip,
	stream Gossip_GossipClient,
	stopper *stop.Stopper,
	wg *sync.WaitGroup,
) error {
	sendGossipChan := make(chan struct{}, 1)

	// Register a callback for gossip updates.
	updateCallback := func(_ string, _ roachpb.Value) {
		select {
		case sendGossipChan <- struct{}{}:
		default:
		}
	}

	errCh := make(chan error, 1)
	initCh := make(chan struct{}, 1)
	// This wait group is used to allow the caller to wait until gossip
	// processing is terminated.
	wg.Add(1)
	if err := stopper.RunAsyncTask(ctx, "client-gossip", func(ctx context.Context) {
		defer wg.Done()

		errCh <- func() error {
			var peerID roachpb.NodeID

			initCh := initCh
			for init := true; ; init = false {
				reply, err := stream.Recv()
				if err != nil {
					return err
				}
				if err := c.handleResponse(ctx, g, reply); err != nil {
					return err
				}
				if init {
					initCh <- struct{}{}
				}
				if peerID == 0 && c.peerID != 0 {
					peerID = c.peerID
					g.updateClients()
				}
			}
		}()
	}); err != nil {
		wg.Done()
		return err
	}

	// We attempt to defer registration of the callback until we've heard a
	// response from the remote node which will contain the remote's high water
	// stamps. This prevents the client from sending all of its infos to the
	// remote (which would happen if we don't know the remote's high water
	// stamps). Unfortunately, versions of cockroach before 2.1 did not always
	// send a response when receiving an incoming connection, so we also start a
	// timer and perform initialization after 1s if we haven't heard from the
	// remote.
	var unregister func()
	defer func() {
		if unregister != nil {
			unregister()
		}
	}()
	maybeRegister := func() {
		if unregister == nil {
			// We require redundant callbacks here as the update callback is
			// propagating gossip infos to other nodes and needs to propagate the new
			// expiration info.
			unregister = g.RegisterCallback(".*", updateCallback, Redundant)
		}
	}
	initTimer := time.NewTimer(time.Second)
	defer initTimer.Stop()

	for count := 0; ; {
		select {
		case <-c.closer:
			return nil
		case <-stopper.ShouldQuiesce():
			return nil
		case err := <-errCh:
			return err
		case <-initCh:
			maybeRegister()
		case <-initTimer.C:
			maybeRegister()
		case <-sendGossipChan:
			if err := c.sendGossip(g, stream, count == 0); err != nil {
				return err
			}
			count++
		}
	}
}
