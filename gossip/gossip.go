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
	"encoding/json"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	// MaxPeers is the maximum number of connected gossip peers.
	MaxPeers = 10
	// minNodeCount is the minimum number of nodes in the gossip network
	// for the purpose of computing the maximum hops allowed for info
	// transimission. The actual count of nodes in the cluster is
	// computed by counting the number of node IDs in the infoStore.
	//
	// The count of nodes is used to compute the maximum hops allowed
	// for info transmission given the maxPeers parameter by the
	// formula: maxHops = ceil(log(numNodes) / log(maxPeers)) + 1.
	//
	// This minimum value helps when establishing the gossip network,
	// and is set purposefully high to avoid premature tightening.
	minNodeCount int64 = 1000

	// ttlNodeIDGossip is time-to-live for node ID -> address.
	ttlNodeIDGossip time.Duration = 0

	// TestInterval is the default gossip interval used for running tests.
	TestInterval = 10 * time.Millisecond
)

var (
	// TestBootstrap is the default gossip bootstrap used for running tests.
	TestBootstrap = []resolver.Resolver{}
)

// Gossip is an instance of a gossip node. It embeds a gossip server.
// During bootstrapping, the bootstrap list contains candidates for
// entry to the gossip network.
type Gossip struct {
	Connected     chan struct{}       // Closed upon initial connection
	hasConnected  bool                // Set first time network is connected
	RPCContext    *rpc.Context        // The context required for RPC
	bsRPCContext  *rpc.Context        // Context for bootstrap RPCs
	*server                           // Embedded gossip RPC server
	outgoing      nodeSet             // Set of outgoing client node IDs
	bootstrapping map[string]struct{} // Set of active bootstrap clients
	clientsMu     sync.Mutex          // Mutex protects the clients slice
	clients       []*client           // Slice of clients
	disconnected  chan *client        // Channel of disconnected clients
	stalled       chan struct{}       // Channel to wakeup stalled bootstrap

	// resolvers is a list of resolvers used to determine
	// bootstrap hosts for connecting to the gossip network.
	resolverIdx int
	resolvers   []resolver.Resolver
	triedAll    bool // True when all resolvers have been tried once
}

// New creates an instance of a gossip node.
func New(rpcContext *rpc.Context, gossipInterval time.Duration, resolvers []resolver.Resolver) *Gossip {
	g := &Gossip{
		Connected:     make(chan struct{}),
		RPCContext:    rpcContext,
		server:        newServer(gossipInterval),
		outgoing:      makeNodeSet(MaxPeers),
		bootstrapping: map[string]struct{}{},
		clients:       []*client{},
		disconnected:  make(chan *client, MaxPeers),
		stalled:       make(chan struct{}, 1),
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
	log.Infof("gossiping node descriptor %+v", desc)
	if err := g.AddInfoProto(MakeNodeIDKey(desc.NodeID), desc, ttlNodeIDGossip); err != nil {
		return util.Errorf("couldn't gossip descriptor for node %d: %v", desc.NodeID, err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.is.NodeID = desc.NodeID
	return nil
}

// SetResolvers initializes the set of gossip resolvers used to
// find nodes to bootstrap the gossip network.
func (g *Gossip) SetResolvers(resolvers []resolver.Resolver) {
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

// GetNodeDescriptor looks up the descriptor of the node by ID.
func (g *Gossip) GetNodeDescriptor(nodeID proto.NodeID) (*proto.NodeDescriptor, error) {
	nodeDescriptor := &proto.NodeDescriptor{}
	if err := g.GetInfoProto(MakeNodeIDKey(nodeID), nodeDescriptor); err != nil {
		return nil, util.Errorf("unable to lookup descriptor for node %d: %s", nodeID, err)
	}

	return nodeDescriptor, nil
}

// getNodeDescriptorLocked looks up the descriptor of the node by ID. The mutex
// is assumed held by the caller. This method is called externally via
// GetNodeDescriptor and internally by getNodeIDAddressLocked.
func (g *Gossip) getNodeDescriptorLocked(nodeID proto.NodeID) (*proto.NodeDescriptor, error) {
	nodeIDKey := MakeNodeIDKey(nodeID)

	// We can't use GetInfoProto here because that method grabs the lock.
	if i := g.is.getInfo(nodeIDKey); i != nil {
		if err := i.Value.Verify([]byte(nodeIDKey)); err != nil {
			return nil, err
		}
		nodeDescriptor := &proto.NodeDescriptor{}

		if err := gogoproto.Unmarshal(i.Value.Bytes, nodeDescriptor); err != nil {
			return nil, err
		}

		return nodeDescriptor, nil
	}

	return nil, util.Errorf("unable to lookup descriptor for node %d", nodeID)
}

// getNodeIDAddressLocked looks up the address of the node by ID. The mutex is
// assumed held by the caller. This method is called externally via
// GetNodeIDAddress or internally when looking up a "distant" node address to
// connect directly to.
func (g *Gossip) getNodeIDAddressLocked(nodeID proto.NodeID) (net.Addr, error) {
	nd, err := g.getNodeDescriptorLocked(nodeID)
	if err != nil {
		return nil, err
	}
	return nd.Address, nil
}

// AddInfo adds or updates an info object. Returns an error if info
// couldn't be added.
func (g *Gossip) AddInfo(key string, val []byte, ttl time.Duration) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	err := g.is.addInfo(key, g.is.newInfo(val, ttl))
	if err == nil {
		g.checkHasConnected()
	}
	return err
}

// AddInfoProto adds or updates an info object. Returns an error if info
// couldn't be added.
func (g *Gossip) AddInfoProto(key string, proto gogoproto.Message, ttl time.Duration) error {
	bytes, err := gogoproto.Marshal(proto)
	if err != nil {
		return err
	}
	return g.AddInfo(key, bytes, ttl)
}

// GetInfo returns an info value by key or an error if specified
// key does not exist or has expired.
func (g *Gossip) GetInfo(key string) ([]byte, error) {
	g.mu.Lock()
	i := g.is.getInfo(key)
	g.mu.Unlock()

	if i != nil {
		if err := i.Value.Verify([]byte(key)); err != nil {
			return nil, err
		}
		return i.Value.Bytes, nil
	}
	return nil, util.Errorf("key %q does not exist or has expired", key)
}

// GetInfoProto returns an info value by key or an error if specified
// key does not exist or has expired.
func (g *Gossip) GetInfoProto(key string, proto gogoproto.Message) error {
	bytes, err := g.GetInfo(key)
	if err != nil {
		return err
	}
	return gogoproto.Unmarshal(bytes, proto)
}

// GetSystemConfig returns the system config.
func (g *Gossip) GetSystemConfig() (*config.SystemConfig, error) {
	cfg := &config.SystemConfig{}
	if err := g.GetInfoProto(KeySystemConfig, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// GetInfosAsJSON returns the contents of the infostore, marshalled to
// JSON.
func (g *Gossip) GetInfosAsJSON() ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return json.MarshalIndent(g.is, "", "  ")
}

// Callback is a callback method to be invoked on gossip update
// of info denoted by key.
type Callback func(key string, content []byte)

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
func (g *Gossip) Start(rpcServer *rpc.Server, stopper *stop.Stopper) {
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
	var nodeCount int64

	if err := g.is.visitInfos(func(key string, i *info) error {
		if strings.HasPrefix(key, KeyNodeIDPrefix) {
			nodeCount++
		}
		return nil
	}); err != nil {
		panic(err)
	}

	if nodeCount < minNodeCount {
		nodeCount = minNodeCount
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
func (g *Gossip) filterExtant(nodes nodeSet) nodeSet {
	return nodes.filter(func(a proto.NodeID) bool {
		return !g.outgoing.hasNode(a)
	}).filter(func(a proto.NodeID) bool {
		return !g.incoming.hasNode(a)
	})
}

// getNextBootstrapAddress returns the next available bootstrap
// address by consulting the first non-exhausted resolver from the
// slice supplied to the constructor or set using setBootstrap().
// The lock is assumed held.
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
		} else if addr.String() == g.is.NodeAddr.String() {
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
func (g *Gossip) bootstrap(stopper *stop.Stopper) {
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
func (g *Gossip) manage(stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		// Loop until closed and there are no remaining outgoing connections.
		for {
			select {
			case <-stopper.ShouldStop():
				return
			case c := <-g.disconnected:
				g.doDisconnected(stopper, c)
			case <-time.After(g.jitteredGossipInterval()):
				g.doCheckTimeout(stopper)
			}
		}
	})
}

func (g *Gossip) doCheckTimeout(stopper *stop.Stopper) {
	g.mu.Lock()
	defer g.mu.Unlock()
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
	g.maybeSignalStalledLocked()
}

func (g *Gossip) doDisconnected(stopper *stop.Stopper, c *client) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.removeClient(c)

	// If the client was disconnected with a forwarding address, connect now.
	if c.forwardAddr != nil {
		g.startClient(c.forwardAddr, g.RPCContext, stopper)
	}
	g.maybeSignalStalledLocked()
}

func (g *Gossip) maybeSignalStalledLocked() {
	// If there are no outgoing hosts or sentinel gossip is missing,
	// and there are still unused bootstrap hosts, signal bootstrapper
	// to try another.
	if g.outgoing.len()+g.incoming.len() == 0 {
		g.signalStalled()
	} else if g.is.getInfo(KeySentinel) == nil {
		log.Warningf("missing sentinel gossip %s; assuming partition and reconnecting", g.is.NodeID)
		g.signalStalled()
	}
}

func (g *Gossip) signalStalled() {
	select {
	case g.stalled <- struct{}{}:
	default:
	}
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
func (g *Gossip) maybeWarnAboutInit(stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		// Wait 5s before first check.
		select {
		case <-stopper.ShouldStop():
			return
		case <-time.After(5 * time.Second):
		}
		retryOptions := retry.Options{
			InitialBackoff: 5 * time.Second,  // first backoff at 5s
			MaxBackoff:     60 * time.Second, // max backoff is 60s
			Multiplier:     2,                // doubles
			Stopper:        stopper,          // stop no matter what on stopper
		}
		// This will never error because of infinite retries.
		for r := retry.Start(retryOptions); r.Next(); {
			g.mu.Lock()
			hasSentinel := g.is.getInfo(KeySentinel) != nil
			triedAll := g.triedAll
			g.mu.Unlock()
			// If we have the sentinel, exit the retry loop.
			if hasSentinel {
				break
			}
			// Otherwise, if all bootstrap hosts are connected, warn.
			if triedAll {
				log.Warningf("connected to gossip but missing sentinel. Has the cluster been initialized? " +
					"Use \"cockroach init\" to initialize.")
			}
		}
	})
}

// checkHasConnected checks whether this gossip instance is connected
// to enough of the gossip network that it has received the cluster ID
// gossip info. Once connected, the "Connected" channel is closed to
// signal to any waiters that the gossip instance is ready.
func (g *Gossip) checkHasConnected() {
	// Check if we have the cluster ID gossip to start.
	// If so, then mark ourselves as trivially connected to the gossip network.
	if !g.hasConnected && g.is.getInfo(KeyClusterID) != nil {
		g.hasConnected = true
		close(g.Connected)
	}
}

// startClient launches a new client connected to remote address.
// The client is added to the outgoing address set and launched in
// a goroutine.
func (g *Gossip) startClient(addr net.Addr, context *rpc.Context, stopper *stop.Stopper) {
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

// GetUser implements userRequest.
// Gossip messages are always sent by the node user.
func (m *Request) GetUser() string {
	return security.NodeUser
}
