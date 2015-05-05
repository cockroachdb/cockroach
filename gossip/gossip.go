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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

/*
Each node attempts to contact peer nodes to gather all infos in
the system with minimal total hops. The algorithm is as follows:

 0 Node starts up gossip server to accept incoming gossip requests.
   Continue to step #1 to join the gossip network.

 1 Node selects random peer from bootstrap list, excluding its own
   address for its first outgoing connection. Node starts client and
   continues to step #2.

 2 Node requests gossip from peer. If this is first request, MaxSeq
   will be 0. Otherwise, will be value of MaxSeq from last response to
   gossip request. Requesting node times out at gossipInterval*2. On
   timeout, client is closed and GC'd. If node has no outgoing
   connections, goto #1.

   a. When gossip is received, infostore is augmented. If new info was
      received, the client in question is credited. If nothing new was
      received in maxWaitForNewGossip, client is closed. If node has no
      outgoing connections, goto #1.

   b. If any gossip was received at > maxToleratedHops and num
      connected peers < maxPeers, choose random peer from those
      originating info > maxToleratedHops, start it, and goto #2.

   c. If sentinel gossip keyed by KeySentinel is missing or expired,
      node is considered partitioned; goto #1.

 3 On connect, if node has too many connected clients, gossip requests
   are returned immediately with an alternate address set to a random
   selection from amongst already-connected clients. If MaxSeq is -1
   (initial connection), returns gossip immediately. Otherwise,
   request waits for a randomly jittered interval ~= gossipInterval.
   Node periodically returns empty gossip responses to prevent client
   timeouts. Node receives delta from gossiping client in turn.
*/

package gossip

import (
	"encoding/gob"
	"encoding/json"
	"math"
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// MaxPeers is the maximum number of connected gossip peers.
	MaxPeers = 10
	// defaultNodeCount is the default number of nodes in the gossip
	// network. The actual count of nodes in the cluster is gossiped
	// by the range which contains node statistics.
	//
	// The count of nodes is used to compute the maximum hops allowed
	// for info transmission given the maxPeers parameter by the
	// formula: maxHops = ceil(log(numNodes) / log(maxPeers)) + 1.
	//
	// This default value helps when establishing the gossip network,
	// and is set purposefully high to avoid premature tightening.
	// Once we receive the gossip with actual count, the default count
	// is replaced.
	defaultNodeCount = 1000

	// ttlNodeIDGossip is time-to-live for node ID -> address.
	ttlNodeIDGossip = 0 * time.Second

	// TestInterval is the default gossip interval used for running tests.
	TestInterval = 10 * time.Millisecond
)

var (
	// TestBootstrap is the default gossip bootstrap used for running tests.
	TestBootstrap = []Resolver{}
)

func init() {
	gob.Register(&proto.NodeDescriptor{})
}

// Gossip is an instance of a gossip node. It embeds a gossip server.
// During bootstrapping, the bootstrap list contains candidates for
// entry to the gossip network.
type Gossip struct {
	Connected     chan struct{}       // Closed upon initial connection
	hasConnected  bool                // Set first time network is connected
	RPCContext    *rpc.Context        // The context required for RPC
	bsRPCContext  *rpc.Context        // Context for bootstrap RPCs
	*server                           // Embedded gossip RPC server
	outgoing      *nodeSet            // Set of outgoing client node IDs
	bootstrapping map[string]struct{} // Set of active bootstrap clients
	clientsMu     sync.Mutex          // Mutex protects the clients slice
	clients       []*client           // Slice of clients
	disconnected  chan *client        // Channel of disconnected clients
	stalled       chan struct{}       // Channel to wakeup stalled bootstrap

	// resolvers is a list of resolvers used to determine
	// bootstrap hosts for connecting to the gossip network.
	resolverIdx int
	resolvers   []Resolver
	triedAll    bool // True when all resolvers have been tried once
}

// New creates an instance of a gossip node.
func New(rpcContext *rpc.Context, gossipInterval time.Duration, resolvers []Resolver) *Gossip {
	g := &Gossip{
		Connected:     make(chan struct{}),
		RPCContext:    rpcContext,
		server:        newServer(gossipInterval),
		outgoing:      newNodeSet(MaxPeers),
		bootstrapping: map[string]struct{}{},
		clients:       []*client{},
		disconnected:  make(chan *client, MaxPeers),
		stalled:       make(chan struct{}, 10),
		resolvers:     resolvers,
	}
	// Create the bootstrapping RPC context. This context doesn't
	// measure clock offsets and doesn't cache clients because bootstrap
	// connections may go through a load balancer.
	if rpcContext != nil {
		g.bsRPCContext = rpcContext.Copy()
		g.bsRPCContext.DisableCache = true
		g.bsRPCContext.RemoteClocks = nil
	}
	return g
}

// GetNodeID returns the instance's saved NodeID.
func (g *Gossip) GetNodeID() proto.NodeID {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.is.NodeID
}

// SetNodeDescriptor adds the node descriptor to the gossip network
// and sets the infostore's node ID.
func (g *Gossip) SetNodeDescriptor(desc *proto.NodeDescriptor) error {
	nodeIDKey := MakeNodeIDKey(desc.NodeID)
	log.Infof("gossiping node descriptor %+v", desc)
	if err := g.AddInfo(nodeIDKey, desc, ttlNodeIDGossip); err != nil {
		return util.Errorf("couldn't gossip descriptor for node %d: %v", desc.NodeID, err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.is.NodeID = desc.NodeID
	return nil
}

// SetResolvers initializes the set of gossip resolvers used to
// find nodes to bootstrap the gossip network.
func (g *Gossip) SetResolvers(resolvers []Resolver) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.resolverIdx = 0
	g.resolvers = resolvers
	g.triedAll = false
}

// GetNodeIDAddress looks up the address of the node by ID.
func (g *Gossip) GetNodeIDAddress(nodeID proto.NodeID) (net.Addr, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.getNodeIDAddressLocked(nodeID)
}

// getNodeIDAddressLocked looks up the address of the node by ID. The
// mutex is assumed held by the caller. This method is called by
// externally via GetNodeIDAddress or internally when looking up a
// "distant" node address to connect directly to.
func (g *Gossip) getNodeIDAddressLocked(nodeID proto.NodeID) (net.Addr, error) {
	nodeIDKey := MakeNodeIDKey(nodeID)
	if i := g.is.getInfo(nodeIDKey); i != nil {
		if nd, ok := i.Val.(*proto.NodeDescriptor); ok {
			return util.MakeRawAddr(nd.Address.Network, nd.Address.Address), nil
		}
		return nil, util.Errorf("error in node descriptor gossip: %+v", i.Val)
	}
	return nil, util.Errorf("unable to lookup address for node: %d", nodeID)
}

// AddInfo adds or updates an info object. Returns an error if info
// couldn't be added.
func (g *Gossip) AddInfo(key string, val interface{}, ttl time.Duration) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	err := g.is.addInfo(g.is.newInfo(key, val, ttl))
	if err == nil {
		g.checkHasConnected()
	}
	return err
}

// GetInfo returns an info value by key or an error if specified
// key does not exist or has expired.
func (g *Gossip) GetInfo(key string) (interface{}, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if i := g.is.getInfo(key); i != nil {
		return i.Val, nil
	}
	return nil, util.Errorf("key %q does not exist or has expired", key)
}

// GetInfosAsJSON returns the contents of the infostore, marshalled to
// JSON.
func (g *Gossip) GetInfosAsJSON() ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return json.Marshal(g.is)
}

// GetGroupInfos returns a slice of info values from specified group,
// or an error if group is not registered.
func (g *Gossip) GetGroupInfos(prefix string) ([]interface{}, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	infos := g.is.getGroupInfos(prefix)
	if infos == nil {
		return nil, util.Errorf("group %q doesn't exist", prefix)
	}
	values := make([]interface{}, len(infos))
	for i, info := range infos {
		values[i] = info.Val
	}
	return values, nil
}

// RegisterGroup registers a new group with info store. Returns an
// error if the group was already registered.
func (g *Gossip) RegisterGroup(prefix string, limit int, typeOf GroupType) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.is.registerGroup(newGroup(prefix, limit, typeOf))
}

// Callback is a callback method to be invoked on gossip update
// of info denoted by key. The contentsChanged bool indicates whether
// the info contents were updated. False indicates the info timestamp
// was refreshed, but its contents remained unchanged.
type Callback func(key string, contentsChanged bool)

// RegisterCallback registers a callback for a key pattern to be
// invoked whenever new info for a gossip key matching pattern is
// received. The callback method is invoked with the info key which
// matched pattern.
func (g *Gossip) RegisterCallback(pattern string, method Callback) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.is.registerCallback(pattern, method)
}

// MaxHops returns the maximum number of hops to reach the furthest
// gossiped information currently in the network.
func (g *Gossip) MaxHops() uint32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.is.maxHops()
}

// Incoming returns a slice of incoming gossip client connection
// node IDs.
func (g *Gossip) Incoming() []proto.NodeID {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.incoming.asSlice()
}

// Outgoing returns a slice of outgoing gossip client connection
// node IDs. Note that these outgoing client connections may not
// actually be legitimately connected. They may be in the process
// of trying, or may already have failed, but haven't yet been
// processed by the gossip instance.
func (g *Gossip) Outgoing() []proto.NodeID {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.outgoing.asSlice()
}

// Start launches the gossip instance, which commences joining the
// gossip network using the supplied rpc server and the gossip
// bootstrap addresses specified via command-line flag: --gossip.
//
// This method starts bootstrap loop, gossip server, and client
// management in separate goroutines and returns.
func (g *Gossip) Start(rpcServer *rpc.Server, stopper *util.Stopper) {
	g.server.start(rpcServer, stopper) // serve gossip protocol
	g.bootstrap(stopper)               // bootstrap gossip client
	g.manage(stopper)                  // manage gossip clients
	g.maybeWarnAboutInit(stopper)
}

// maxToleratedHops computes the maximum number of hops which the
// gossip network should allow when optimally configured. It's based
// on the level of fanout (MaxPeers) and the count of nodes in the
// cluster.
func (g *Gossip) maxToleratedHops() uint32 {
	// Get info directly as we have mutex held here.
	var nodeCount = int64(defaultNodeCount)
	if info := g.is.getInfo(KeyNodeCount); info != nil {
		nodeCount = info.Val.(int64)
	}
	return uint32(math.Ceil(math.Log(float64(nodeCount))/math.Log(float64(MaxPeers))))*2 + 1
}

// hasIncoming returns whether the server has an incoming gossip
// client matching the provided node ID.
func (g *Gossip) hasIncoming(nodeID proto.NodeID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.incoming.hasNode(nodeID)
}

// filterExtant removes any nodes from the supplied nodeSet which
// are already connected to this node, either via outgoing or incoming
// client connections.
func (g *Gossip) filterExtant(nodes *nodeSet) *nodeSet {
	return nodes.filter(func(a proto.NodeID) bool {
		return !g.outgoing.hasNode(a)
	}).filter(func(a proto.NodeID) bool {
		return !g.incoming.hasNode(a)
	})
}

// getNextBootstrapAddress returns the next available bootstrap
// address by consulting the first non-exhausted resolver from the
// slice supplied to the constructor or set using setBootstrap().
func (g *Gossip) getNextBootstrapAddress() net.Addr {
	if len(g.resolvers) == 0 {
		log.Fatalf("no resolvers specified for gossip network")
	}

	// Run through resolvers round robin starting at last resolved index.
	for i := 0; i < len(g.resolvers); i++ {
		g.resolverIdx = (g.resolverIdx + 1) % len(g.resolvers)
		if g.resolverIdx == len(g.resolvers)-1 {
			g.triedAll = true
		}
		resolver := g.resolvers[g.resolverIdx]
		addr, err := resolver.GetAddress()
		if err != nil {
			log.Errorf("invalid bootstrap address: %+v, %v", resolver, err)
			continue
		} else if g.is.NodeAddr != nil && addr.String() == g.is.NodeAddr.String() {
			// Skip our own node address.
			continue
		}
		_, addrActive := g.bootstrapping[addr.String()]
		if !resolver.IsExhausted() || !addrActive {
			g.bootstrapping[addr.String()] = struct{}{}
			return addr
		}
	}

	return nil
}

// bootstrap connects the node to the gossip network. Bootstrapping
// commences in the event there are no connected clients or the
// sentinel gossip info is not available. After a successful bootstrap
// connection, this method will block on the stalled condvar, which
// receives notifications that gossip network connectivity has been
// lost and requires re-bootstrapping.
func (g *Gossip) bootstrap(stopper *util.Stopper) {
	stopper.RunWorker(func() {
		for {
			g.mu.Lock()
			if g.closed {
				g.mu.Unlock()
				return
			}
			// Check whether or not we need bootstrap.
			haveClients := g.outgoing.len() > 0
			haveSentinel := g.is.getInfo(KeySentinel) != nil
			if !haveClients || !haveSentinel {
				// Try to get another bootstrap address from the resolvers.
				if addr := g.getNextBootstrapAddress(); addr != nil {
					g.startClient(addr, g.bsRPCContext, stopper)
				}
			}
			g.mu.Unlock()

			// Block until we need bootstrapping again.
			select {
			case <-g.stalled:
				// continue
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// manage manages outgoing clients. Periodically, the infostore is
// scanned for infos with hop count exceeding maxToleratedHops()
// threshold. If the number of outgoing clients doesn't exceed
// MaxPeers, a new gossip client is connected to a randomly selected
// peer beyond maxToleratedHops threshold. Otherwise, the least useful
// peer node is cut off to make room for a replacement. Disconnected
// clients are processed via the disconnected channel and taken out of
// the outgoing address set. If there are no longer any outgoing
// connections or the sentinel gossip is unavailable, the bootstrapper
// is notified via the stalled conditional variable.
func (g *Gossip) manage(stopper *util.Stopper) {
	stopper.RunWorker(func() {
		checkTimeout := time.Tick(g.jitteredGossipInterval())
		// Loop until closed and there are no remaining outgoing connections.
		for {
			select {
			case c := <-g.disconnected:
				g.mu.Lock()
				if c.err != nil {
					log.Infof("client disconnected: %s", c.err)
				}
				g.removeClient(c)

				// If the client was disconnected with a forwarding address, connect now.
				if c.forwardAddr != nil {
					g.startClient(c.forwardAddr, g.RPCContext, stopper)
				}

			case <-checkTimeout:
				g.mu.Lock()
				// Check whether the graph needs to be tightened to
				// accommodate distant infos.
				distant := g.filterExtant(g.is.distant(g.maxToleratedHops()))
				if distant.len() > 0 {
					// If we have space, start a client immediately.
					if g.outgoing.hasSpace() {
						nodeID := distant.selectRandom()
						if nodeAddr, err := g.getNodeIDAddressLocked(nodeID); err != nil {
							log.Errorf("node %d: %s", nodeID, err)
						} else {
							g.startClient(nodeAddr, g.RPCContext, stopper)
						}
					} else {
						// Otherwise, find least useful peer and close it. Make sure
						// here that we only consider outgoing clients which are
						// connected.
						nodeID := g.is.leastUseful(g.outgoing)
						if nodeID != 0 {
							log.Infof("closing least useful client %d to tighten network graph", nodeID)
							g.closeClient(nodeID)
						}
					}
				}

			case <-stopper.ShouldStop():
				return
			}

			// If there are no outgoing hosts or sentinel gossip is missing,
			// and there are still unused bootstrap hosts, signal bootstrapper
			// to try another.
			if g.outgoing.len()+g.incoming.len() == 0 {
				g.stalled <- struct{}{}
			} else if g.is.getInfo(KeySentinel) == nil {
				log.Warningf("missing sentinel gossip %s; assuming partition and reconnecting", g.is.NodeID)
				g.stalled <- struct{}{}
			}

			g.mu.Unlock()
		}
	})
}

// maybeWarnAboutInit looks for signs indicating a cluster which
// hasn't been initialized and warns. There's no absolutely sure way
// to determine whether the current node is simply waiting to be
// bootstrapped to an existing cluster vs. the operator having failed
// to initialize the cluster via the "cockroach init" command, so
// we can only warn.
//
// This method checks whether all gossip bootstrap hosts are
// connected, and whether the node itself is a bootstrap host, but
// there is still no sentinel gossip.
func (g *Gossip) maybeWarnAboutInit(stopper *util.Stopper) {
	stopper.RunWorker(func() {
		// Wait 5s before first check.
		select {
		case <-stopper.ShouldStop():
			return
		case <-time.After(5 * time.Second):
		}
		retryOptions := util.RetryOptions{
			Tag:         "check cluster initialization",
			Backoff:     5 * time.Second,  // first backoff at 5s
			MaxBackoff:  60 * time.Second, // max backoff is 60s
			Constant:    2,                // doubles
			MaxAttempts: 0,                // indefinite retries
			Stopper:     stopper,          // stop no matter what on stopper
		}
		util.RetryWithBackoff(retryOptions, func() (util.RetryStatus, error) {
			g.mu.Lock()
			hasSentinel := g.is.getInfo(KeySentinel) != nil
			g.mu.Unlock()
			// If we have the sentinel, exit the retry loop.
			if hasSentinel {
				return util.RetryBreak, nil
			}
			// Otherwise, if all bootstrap hosts are connected, warn.
			if g.triedAll {
				log.Warningf("connected to gossip but missing sentinel. Has the cluster been initialized? " +
					"Use \"cockroach init\" to initialize.")
			}
			return util.RetryContinue, nil
		})
	})
}

// checkHasConnected checks whether this gossip instance is connected
// to enough of the gossip network that it has received the sentinel
// gossip info. Once connected, the "Connected" channel is closed to
// signal to any waiters that the gossip instance is ready.
func (g *Gossip) checkHasConnected() {
	// Check if we have the sentinel gossip (cluster ID) to start.
	// If so, then mark ourselves as trivially connected to the gossip network.
	if !g.hasConnected && g.is.getInfo(KeySentinel) != nil {
		g.hasConnected = true
		close(g.Connected)
	}
}

// startClient launches a new client connected to remote address.
// The client is added to the outgoing address set and launched in
// a goroutine.
func (g *Gossip) startClient(addr net.Addr, context *rpc.Context, stopper *util.Stopper) {
	log.Infof("starting client to %s", addr)
	c := newClient(addr)
	g.clientsMu.Lock()
	g.clients = append(g.clients, c)
	g.clientsMu.Unlock()
	c.start(g, g.disconnected, context, stopper)
}

// closeClient finds and removes a client from the clients slice.
func (g *Gossip) closeClient(nodeID proto.NodeID) {
	c := g.findClient(func(c *client) bool { return c.peerID == nodeID })
	if c != nil {
		c.close()
	}
}

// removeClient finds and removes the client by nodeID from the clients slice.
func (g *Gossip) removeClient(c *client) {
	g.clientsMu.Lock()
	for i := 0; i < len(g.clients); i++ {
		if g.clients[i] == c {
			g.clients = append(g.clients[0:i], g.clients[i+1:]...)
			break
		}
	}
	delete(g.bootstrapping, c.addr.String())
	g.outgoing.removeNode(c.peerID)
	g.clientsMu.Unlock()
}

func (g *Gossip) findClient(match func(*client) bool) *client {
	g.clientsMu.Lock()
	defer g.clientsMu.Unlock()
	for i := 0; i < len(g.clients); i++ {
		if c := g.clients[i]; match(c) {
			return c
		}
	}
	return nil
}
