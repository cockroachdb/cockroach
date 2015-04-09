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

	// TestInterval is the default gossip interval used for running tests.
	TestInterval = 10 * time.Millisecond
)

var (
	// TestBootstrap is the default gossip bootstrap used for running tests.
	TestBootstrap = []net.Addr{}
)

// Gossip is an instance of a gossip node. It embeds a gossip server.
// During bootstrapping, the bootstrap list contains candidates for
// entry to the gossip network.
type Gossip struct {
	nodeID       proto.NodeID       // Optional node ID
	Connected    chan struct{}      // Closed upon initial connection
	hasConnected bool               // Set first time network is connected
	isBootstrap  bool               // True if this node is a bootstrap host
	haveUnused   bool               // True if there are unused hosts from gossipBootstrap
	RPCContext   *rpc.Context       // The context required for RPC
	*server                         // Embedded gossip RPC server
	bootstraps   *addrSet           // Bootstrap host addresses
	outgoing     *addrSet           // Set of outgoing client addresses
	clientsMu    sync.Mutex         // Mutex protects the clients map
	clients      map[string]*client // Map from address to client
	disconnected chan *client       // Channel of disconnected clients
	stalled      chan struct{}      // Channel to wakeup stalled bootstrap

	// gossipBootstrap is a list of node addresses that act as
	// bootstrap hosts for connecting to the gossip network.
	gossipBootstrap []net.Addr
}

// New creates an instance of a gossip node.
func New(rpcContext *rpc.Context, gossipInterval time.Duration, gossipBootstrap []net.Addr) *Gossip {
	g := &Gossip{
		Connected:       make(chan struct{}),
		RPCContext:      rpcContext,
		server:          newServer(gossipInterval),
		bootstraps:      newAddrSet(MaxPeers),
		outgoing:        newAddrSet(MaxPeers),
		clients:         map[string]*client{},
		disconnected:    make(chan *client, MaxPeers),
		stalled:         make(chan struct{}, 10),
		gossipBootstrap: gossipBootstrap,
	}
	return g
}

// GetNodeID returns the instance's saved NodeID.
func (g *Gossip) GetNodeID() proto.NodeID {
	g.mu.Lock()
	id := g.nodeID
	g.mu.Unlock()
	return id
}

// SetNodeID sets the Gossip instance's NodeID.
// It is typically only set once, but is only obtained
// after creating the instance.
func (g *Gossip) SetNodeID(id proto.NodeID) {
	g.mu.Lock()
	g.nodeID = id
	g.mu.Unlock()
}

// SetBootstrap initializes the set of gossip node addresses used to
// bootstrap the gossip network.
func (g *Gossip) SetBootstrap(bootstraps []net.Addr) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.gossipBootstrap = bootstraps
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
// addresses.
func (g *Gossip) Incoming() []net.Addr {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.incoming.asSlice()
}

// Outgoing returns a slice of outgoing gossip client connection
// addresses. Note that these outgoing client connections may not
// actually be legitimately connected. They may be in the process
// of trying, or may already have failed, but haven't yet been
// processed by the gossip instance.
func (g *Gossip) Outgoing() []net.Addr {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.outgoing.asSlice()
}

// Start launches the gossip instance, which commences joining the
// gossip network using the supplied rpc server and the gossip
// bootstrap addresses specified via command-line flag: -gossip.
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
// client matching the provided address.
func (g *Gossip) hasIncoming(addr net.Addr) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.incoming.hasAddr(addr)
}

// initializeBootstrapAddresses resolves TCP addresses supplied to the
// constructor or set using setBootstrap(). Non-TCP and resolvable TCP
// addresses are added to the bootstraps addrSet, except for the local
// node, which is removed, if listed as a bootstrap host.
func (g *Gossip) initializeBootstrapAddresses() {
	if len(g.gossipBootstrap) == 0 {
		log.Fatalf("no hosts specified for gossip network (use -gossip)")
	}
	g.haveUnused = false
	for _, addr := range g.gossipBootstrap {
		if addr.Network() == "tcp" {
			_, err := net.ResolveTCPAddr("tcp", addr.String())
			if err != nil {
				log.Errorf("invalid gossip bootstrap address %s: %s", addr, err)
				g.haveUnused = true
				continue
			}
		}
		g.bootstraps.addAddr(addr)
	}

	// Remove our own node address.
	if g.bootstraps.hasAddr(g.is.NodeAddr) {
		g.isBootstrap = true
		g.bootstraps.removeAddr(g.is.NodeAddr)
	}
}

// additionalBootstrappingPossible return true if there are any
// remaining bootstrap hosts to connect to...either valid addresses
// which have yet to be connected, or addresses in the original set
// which did not resolve but may yet on repeated attempts.
func (g *Gossip) additionalBootstrappingPossible() bool {
	return g.filterExtant(g.bootstraps).len() > 0 || g.haveUnused
}

// filterExtant removes any addresses from the supplied addrSet which
// are already connected to this node, either via outgoing or incoming
// client connections.
func (g *Gossip) filterExtant(addrs *addrSet) *addrSet {
	return addrs.filter(func(a net.Addr) bool {
		return !g.outgoing.hasAddr(a)
	}).filter(func(a net.Addr) bool {
		return !g.incoming.hasAddr(a)
	})
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
			g.initializeBootstrapAddresses()
			if g.closed {
				g.mu.Unlock()
				break
			}
			// Find list of available bootstrap hosts.
			avail := g.filterExtant(g.bootstraps)
			if avail.len() > 0 {
				// Check whether or not we need bootstrap.
				haveClients := g.outgoing.len() > 0
				haveSentinel := g.is.getInfo(KeySentinel) != nil
				if !haveClients || !haveSentinel {
					// Select a bootstrap address at random and start client.
					addr := avail.selectRandom()
					log.Infof("bootstrapping gossip protocol using host %+v", addr)
					g.startClient(addr, stopper)
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
				g.outgoing.removeAddr(c.addr)

				g.clientsMu.Lock()
				delete(g.clients, c.addr.String())
				g.clientsMu.Unlock()

				// If the client was disconnected with a forwarding address, connect now.
				if c.forwardAddr != nil {
					g.startClient(c.forwardAddr, stopper)
				}

			case <-checkTimeout:
				g.mu.Lock()
				// Check whether the graph needs to be tightened to
				// accommodate distant infos.
				distant := g.filterExtant(g.is.distant(g.maxToleratedHops()))
				if distant.len() > 0 {
					// If we have space, start a client immediately.
					if g.outgoing.hasSpace() {
						g.startClient(distant.selectRandom(), stopper)
					} else {
						// Otherwise, find least useful peer and close it. Make sure
						// here that we only consider outgoing clients which are
						// connected.
						addr := g.is.leastUseful(g.outgoing)
						if addr != nil {
							log.Infof("closing least useful client %+v to tighten network graph", addr)
							g.closeClient(addr)
						}
					}
				}

			case <-stopper.ShouldStop():
				return
			}

			// If there are no outgoing hosts or sentinel gossip is missing,
			// and there are still unused bootstrap hosts, signal bootstrapper
			// to try another.
			if g.outgoing.len()+g.incoming.len() == 0 && g.additionalBootstrappingPossible() {
				log.Infof("no connections; signaling bootstrap")
				g.stalled <- struct{}{}
			} else if g.is.getInfo(KeySentinel) == nil && g.additionalBootstrappingPossible() {
				log.Warningf("missing sentinel gossip %s; assuming partition and reconnecting", KeySentinel)
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
			allConnected := g.filterExtant(g.bootstraps).len() == 0
			g.mu.Unlock()
			// If we have the sentinel, exit the retry loop.
			if hasSentinel {
				return util.RetryBreak, nil
			}
			// Otherwise, if all bootstrap hosts are connected and this
			// node is a bootstrap host, warn.
			if allConnected && g.isBootstrap {
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
func (g *Gossip) startClient(addr net.Addr, stopper *util.Stopper) {
	log.Infof("starting client to %s", addr)
	c := newClient(addr)
	g.outgoing.addAddr(c.addr)
	g.clientsMu.Lock()
	g.clients[c.addr.String()] = c
	g.clientsMu.Unlock()
	c.start(g, g.disconnected, stopper)
}

// closeClient closes an existing client specified by client's
// remote address.
func (g *Gossip) closeClient(addr net.Addr) {
	g.clientsMu.Lock()
	if c, ok := g.clients[addr.String()]; ok {
		c.close()
		delete(g.clients, addr.String())
	}
	g.clientsMu.Unlock()
}
