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

/*
Each node attempts to contact peer nodes to gather all Infos in
the system with minimal total hops. The algorithm is as follows:

 0 Node starts up gossip server to accept incoming gossip requests.
   Continue to step #1 to join the gossip network.

 1 Node selects random peer from bootstrap list, excluding its own
   address for its first outgoing connection. Node starts client and
   continues to step #2.

 2 Node requests gossip from peer. Gossip requests contain
   HighWaterStamps, a map from node ID to most recent timestamp of any
   Info originating at that node. Requesting node times out at
   checkInterval. On timeout, client is closed and GC'd. If node
   has no outgoing connections, goto #1.

   a. When gossip is received, infostore is augmented. If new Info was
      received, the client in question is credited. If node has no
      outgoing connections, goto #1.

   b. If any gossip was received at > MaxHops and num connected peers
      < maxPeers(), choose random peer from those originating Info >
      MaxHops, start it, and goto #2.

   c. If sentinel gossip keyed by KeySentinel is missing or expired,
      node is considered partitioned; goto #1.

 3 On connect, if node has too many connected clients, gossip requests
   are returned immediately with an alternate address set to a random
   selection from amongst already-connected clients.
*/

package gossip

import (
	"encoding/json"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

const (
	// MaxHops is the maximum number of hops which any gossip info
	// should require to transit between any two nodes in a gossip
	// network.
	MaxHops = 5

	// minPeers is the minimum number of peers which the maxPeers()
	// function will return. This is set higher than one to prevent
	// excessive tightening of the network.
	minPeers = 3

	// ttlNodeDescriptorGossip is time-to-live for node ID -> address.
	ttlNodeDescriptorGossip = 0 * time.Second

	// checkInterval is the default interval for checking for bad gossip
	// states, including a badly connected network and the least useful
	// clients.
	checkInterval = 60 * time.Second

	// stallInterval is the default interval for checking whether the
	// incoming and outgoing connections to the gossip network are
	// insufficient to keep the network connected.
	stallInterval = 1 * time.Second

	// bootstrapInterval is the minimum time between successive
	// bootstrapping attempts to avoid busy-looping trying to find
	// the sentinel gossip info.
	bootstrapInterval = 1 * time.Second
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
	rpcContext    *rpc.Context        // The context required for RPC
	*server                           // Embedded gossip RPC server
	outgoing      nodeSet             // Set of outgoing client node IDs
	bootstrapping map[string]struct{} // Set of active bootstrap clients
	clientsMu     sync.Mutex          // Mutex protects the clients slice
	clients       []*client           // Slice of clients
	disconnected  chan *client        // Channel of disconnected clients
	stalled       chan struct{}       // Channel to wakeup stalled bootstrap

	// The system config is treated unlike other info objects.
	// It is used so often that we keep an unmarshalled version of it
	// here and its own set of callbacks.
	// We do not use the infostore to avoid unmarshalling under the
	// main gossip lock.
	systemConfig         *config.SystemConfig
	systemConfigMu       sync.RWMutex
	systemConfigChannels []chan<- struct{}

	// resolvers is a list of resolvers used to determine
	// bootstrap hosts for connecting to the gossip network.
	resolverIdx int
	resolvers   []resolver.Resolver
	triedAll    bool // True when all resolvers have been tried once
}

// New creates an instance of a gossip node.
func New(rpcContext *rpc.Context, resolvers []resolver.Resolver) *Gossip {
	g := &Gossip{
		Connected:     make(chan struct{}),
		server:        newServer(),
		outgoing:      makeNodeSet(1),
		bootstrapping: map[string]struct{}{},
		clients:       []*client{},
		disconnected:  make(chan *client, 10),
		stalled:       make(chan struct{}, 1),
		resolverIdx:   len(resolvers) - 1,
		resolvers:     resolvers,
	}
	// The gossip RPC context doesn't measure clock offsets, isn't
	// shared with the other RPC clients which the node may be using,
	// and disables reconnects to make it possible to know for certain
	// which other nodes in the cluster are incoming via gossip.
	if rpcContext != nil {
		g.rpcContext = rpcContext.Copy()
		g.rpcContext.DisableCache = true
		g.rpcContext.DisableReconnects = true
		g.rpcContext.RemoteClocks = nil
	}

	// Add ourselves as a SystemConfig watcher.
	g.is.registerCallback(KeySystemConfig, g.updateSystemConfig)
	return g
}

// GetNodeID returns the instance's saved node ID.
func (g *Gossip) GetNodeID() roachpb.NodeID {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.is.NodeID
}

// SetNodeID sets the infostore's node ID.
func (g *Gossip) SetNodeID(nodeID roachpb.NodeID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.is.NodeID != 0 && g.is.NodeID != nodeID {
		// TODO(spencer): change this to a panic after fixing unittests
		//   which do invoke this with different node IDs.
		log.Errorf("different node IDs were set for the same gossip instance (%d, %d)", g.is.NodeID, nodeID)
	}
	g.is.NodeID = nodeID
}

// SetNodeDescriptor adds the node descriptor to the gossip network
// and sets the infostore's node ID.
func (g *Gossip) SetNodeDescriptor(desc *roachpb.NodeDescriptor) error {
	log.Infof("setting node descriptor %+v", desc)
	if err := g.AddInfoProto(MakeNodeIDKey(desc.NodeID), desc, ttlNodeDescriptorGossip); err != nil {
		return util.Errorf("couldn't gossip descriptor for node %d: %v", desc.NodeID, err)
	}
	return nil
}

// SetResolvers initializes the set of gossip resolvers used to
// find nodes to bootstrap the gossip network.
func (g *Gossip) SetResolvers(resolvers []resolver.Resolver) {
	g.mu.Lock()
	defer g.mu.Unlock()
	// Start index at end because get next address loop logic increments as first step.
	g.resolverIdx = len(resolvers) - 1
	g.resolvers = resolvers
	g.triedAll = false
}

// GetNodeIDAddress looks up the address of the node by ID.
func (g *Gossip) GetNodeIDAddress(nodeID roachpb.NodeID) (net.Addr, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.getNodeIDAddressLocked(nodeID)
}

// GetNodeDescriptor looks up the descriptor of the node by ID.
func (g *Gossip) GetNodeDescriptor(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	nodeDescriptor := &roachpb.NodeDescriptor{}
	if err := g.GetInfoProto(MakeNodeIDKey(nodeID), nodeDescriptor); err != nil {
		return nil, util.Errorf("unable to lookup descriptor for node %d: %s", nodeID, err)
	}

	return nodeDescriptor, nil
}

// EnableSimulationCycler is for TESTING PURPOSES ONLY. It sets a
// condition variable which is signaled at each cycle of the
// simulation via SimulationCycle(). The gossip server makes each
// connecting client wait for the cycler to signal before responding.
func (g *Gossip) EnableSimulationCycler(enable bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if enable {
		g.simulationCycler = sync.NewCond(&g.mu)
	} else {
		g.simulationCycler.Broadcast()
		g.simulationCycler = nil
	}
}

// SimulationCycle cycles this gossip node's server by allowing all
// connected clients to proceed one step.
func (g *Gossip) SimulationCycle() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.simulationCycler.Broadcast()
}

// getNodeDescriptorLocked looks up the descriptor of the node by ID. The mutex
// is assumed held by the caller. This method is called externally via
// GetNodeDescriptor and internally by getNodeIDAddressLocked.
func (g *Gossip) getNodeDescriptorLocked(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	nodeIDKey := MakeNodeIDKey(nodeID)

	// We can't use GetInfoProto here because that method grabs the lock.
	if i := g.is.getInfo(nodeIDKey); i != nil {
		if err := i.Value.Verify([]byte(nodeIDKey)); err != nil {
			return nil, err
		}
		nodeDescriptor := &roachpb.NodeDescriptor{}
		if err := i.Value.GetProto(nodeDescriptor); err != nil {
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
func (g *Gossip) getNodeIDAddressLocked(nodeID roachpb.NodeID) (net.Addr, error) {
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
func (g *Gossip) AddInfoProto(key string, msg proto.Message, ttl time.Duration) error {
	bytes, err := proto.Marshal(msg)
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
		return i.Value.GetBytes()
	}
	return nil, util.Errorf("key %q does not exist or has expired", key)
}

// GetInfoProto returns an info value by key or an error if specified
// key does not exist or has expired.
func (g *Gossip) GetInfoProto(key string, msg proto.Message) error {
	bytes, err := g.GetInfo(key)
	if err != nil {
		return err
	}
	return proto.Unmarshal(bytes, msg)
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
type Callback func(string, roachpb.Value)

// RegisterCallback registers a callback for a key pattern to be
// invoked whenever new info for a gossip key matching pattern is
// received. The callback method is invoked with the info key which
// matched pattern. Returns a function to unregister the callback.
func (g *Gossip) RegisterCallback(pattern string, method Callback) func() {
	if pattern == KeySystemConfig {
		log.Warning("raw gossip callback registered on %s, consider using RegisterSystemConfigCallback",
			KeySystemConfig)
	}

	g.mu.Lock()
	unregister := g.is.registerCallback(pattern, method)
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		unregister()
		g.mu.Unlock()
	}
}

// GetSystemConfig returns the local unmarshalled version of the
// system config. It may be nil if it was never gossiped.
func (g *Gossip) GetSystemConfig() *config.SystemConfig {
	g.systemConfigMu.RLock()
	defer g.systemConfigMu.RUnlock()
	return g.systemConfig
}

type systemConfigCallback func(*config.SystemConfig)

// RegisterSystemConfigChannel registers a channel to signify updates for the
// system config. It is notified after registration, and whenever a new
// system config is successfully unmarshalled.
func (g *Gossip) RegisterSystemConfigChannel() <-chan struct{} {
	g.systemConfigMu.Lock()
	defer g.systemConfigMu.Unlock()

	// Create channel that receives new system config notifications.
	// The channel has a size of 1 to prevent gossip from blocking on it.
	c := make(chan struct{}, 1)
	g.systemConfigChannels = append(g.systemConfigChannels, c)

	// Notify the channel right away if we have a config.
	if g.systemConfig != nil {
		c <- struct{}{}
	}

	return c
}

// updateSystemConfig is the raw gossip info callback.
// Unmarshal the system config, and if successfully, update out
// copy and run the callbacks.
func (g *Gossip) updateSystemConfig(key string, content roachpb.Value) {
	if key != KeySystemConfig {
		log.Fatalf("wrong key received on SystemConfig callback: %s", key)
		return
	}
	cfg := &config.SystemConfig{}
	if err := content.GetProto(cfg); err != nil {
		log.Errorf("could not unmarshal system config on callback: %s", err)
		return
	}

	g.systemConfigMu.Lock()
	defer g.systemConfigMu.Unlock()
	g.systemConfig = cfg
	for _, c := range g.systemConfigChannels {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}

// Incoming returns a slice of incoming gossip client connection
// node IDs.
func (g *Gossip) Incoming() []roachpb.NodeID {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.incoming.asSlice()
}

// Outgoing returns a slice of outgoing gossip client connection
// node IDs. Note that these outgoing client connections may not
// actually be legitimately connected. They may be in the process
// of trying, or may already have failed, but haven't yet been
// processed by the gossip instance.
func (g *Gossip) Outgoing() []roachpb.NodeID {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.outgoing.asSlice()
}

// Start launches the gossip instance, which commences joining the
// gossip network using the supplied rpc server and the gossip
// bootstrap addresses specified via command-line flag: --gossip.
//
// The supplied address is used to identify the gossip instance in the
// gossip network; it will be used by other instances to connect to
// this instance.
//
// This method starts bootstrap loop, gossip server, and client
// management in separate goroutines and returns.
func (g *Gossip) Start(rpcServer *rpc.Server, addr net.Addr, stopper *stop.Stopper) {
	g.server.start(rpcServer, addr, stopper) // serve gossip protocol
	g.bootstrap(stopper)                     // bootstrap gossip client
	g.manage(stopper)                        // manage gossip clients
	g.maybeWarnAboutInit(stopper)
}

// hasIncoming returns whether the server has an incoming gossip
// client matching the provided node ID. Mutex should be held by
// caller.
func (g *Gossip) hasIncoming(nodeID roachpb.NodeID) bool {
	return g.incoming.hasNode(nodeID)
}

// hasOutgoing returns whether the server has an outgoing gossip
// client matching the provided node ID. Mutex should be held by
// caller.
func (g *Gossip) hasOutgoing(nodeID roachpb.NodeID) bool {
	return g.outgoing.hasNode(nodeID)
}

// filterExtant removes any nodes from the supplied nodeSet which
// are already connected to this node, either via outgoing or incoming
// client connections.
func (g *Gossip) filterExtant(nodes nodeSet) nodeSet {
	return nodes.filter(func(a roachpb.NodeID) bool {
		return !g.outgoing.hasNode(a)
	}).filter(func(a roachpb.NodeID) bool {
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
			haveClients := g.outgoing.len() > 0
			haveSentinel := g.is.getInfo(KeySentinel) != nil
			if !haveClients || !haveSentinel {
				// Try to get another bootstrap address from the resolvers.
				if addr := g.getNextBootstrapAddress(); addr != nil {
					g.startClient(addr, stopper)
				}
			}
			g.mu.Unlock()

			// Pause an interval before next possible bootstrap.
			select {
			case <-time.After(bootstrapInterval):
				// continue
			case <-stopper.ShouldStop():
				return
			}
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
// scanned for infos with hop count exceeding the MaxHops
// threshold. If the number of outgoing clients doesn't exceed
// maxPeers(), a new gossip client is connected to a randomly selected
// peer beyond MaxHops threshold. Otherwise, the least useful peer
// node is cut off to make room for a replacement. Disconnected
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
			case <-time.After(g.jitteredInterval(checkInterval)):
				g.doCheckNetwork(stopper)
			case <-time.After(g.jitteredInterval(stallInterval)):
				g.mu.Lock()
				g.maybeSignalStalledLocked()
				g.mu.Unlock()
			}
		}
	})
}

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from checkInterval.
func (g *Gossip) jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.75 + 0.5*rand.Float64()))
}

func (g *Gossip) doCheckNetwork(stopper *stop.Stopper) {
	g.mu.Lock()
	defer g.mu.Unlock()
	// Check whether the graph needs to be tightened to
	// accommodate distant infos.
	distant := g.filterExtant(g.is.distant(MaxHops))
	g.outgoing.setMaxSize(g.maxPeers())
	// If there are distant nodes, start a client if we have space.
	if g.outgoing.hasSpace() && distant.len() > 0 {
		nodeID := distant.selectRandom()
		if nodeAddr, err := g.getNodeIDAddressLocked(nodeID); err != nil {
			log.Errorf("node %d: %s", nodeID, err)
		} else {
			g.startClient(nodeAddr, stopper)
		}
	} else if !g.outgoing.hasSpace() {
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

func (g *Gossip) doDisconnected(stopper *stop.Stopper, c *client) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.removeClient(c)

	// If the client was disconnected with a forwarding address, connect now.
	if c.forwardAddr != nil {
		g.startClient(c.forwardAddr, stopper)
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
		log.Warningf("missing sentinel gossip; assuming partition in gossip network")
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
			InitialBackoff: 5 * time.Second,      // first backoff at 5s
			MaxBackoff:     60 * time.Second,     // max backoff is 60s
			Multiplier:     2,                    // doubles
			Closer:         stopper.ShouldStop(), // stop no matter what on stopper
		}
		// This will never error because of infinite retries.
		for r := retry.Start(retryOptions); r.Next(); {
			g.mu.Lock()
			hasConnections := g.outgoing.len()+g.incoming.len() > 0
			hasSentinel := g.is.getInfo(KeySentinel) != nil
			triedAll := g.triedAll
			g.mu.Unlock()
			// If we have the sentinel, exit the retry loop.
			if hasSentinel {
				break
			}
			if !hasConnections {
				log.Warningf("not connected to gossip; check that gossip flag is set appropriately")
			} else if triedAll {
				log.Warningf("missing gossip sentinel; first range unavailable or cluster not initialized")
			}
		}
	})
}

// checkHasConnected checks whether this gossip instance is connected
// to enough of the gossip network that it has received the cluster ID
// gossip info. Once connected, the "Connected" channel is closed to
// signal to any waiters that the gossip instance is ready. The gossip
// mutex should be held by caller.
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
func (g *Gossip) startClient(addr net.Addr, stopper *stop.Stopper) {
	log.Infof("starting client to %s", addr)
	c := newClient(addr)
	g.clientsMu.Lock()
	g.clients = append(g.clients, c)
	g.clientsMu.Unlock()
	c.start(g, g.disconnected, g.rpcContext, stopper)
}

// closeClient finds and removes a client from the clients slice.
func (g *Gossip) closeClient(nodeID roachpb.NodeID) {
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
			g.clients = append(g.clients[:i], g.clients[i+1:]...)
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
	for _, c := range g.clients {
		if match(c) {
			return c
		}
	}
	return nil
}

var _ security.RequestWithUser = &Request{}

// GetUser implements security.RequestWithUser.
// Gossip messages are always sent by the node user.
func (*Request) GetUser() string {
	return security.NodeUser
}
