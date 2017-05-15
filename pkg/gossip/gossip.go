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

 2 Node requests gossip from peer. Gossip requests (and responses)
   contain a map from node ID to info about other nodes in the
   network. Each node maintains its own map as well as the maps of
   each of its peers. The info for each node includes the most recent
   timestamp of any Info originating at that node, as well as the min
   number of hops to reach that node. Requesting node times out at
   checkInterval. On timeout, client is closed and GC'd. If node has
   no outgoing connections, goto #1.

   a. When gossip is received, infostore is augmented. If new Info was
      received, the client in question is credited. If node has no
      outgoing connections, goto #1.

   b. If any gossip was received at > maxHops and num connected peers
      < maxPeers(), choose random peer from those originating Info >
      maxHops, start it, and goto #2.

   c. If sentinel gossip keyed by KeySentinel is missing or expired,
      node is considered partitioned; goto #1.

 3 On connect, if node has too many connected clients, gossip requests
   are returned immediately with an alternate address set to a random
   selection from amongst already-connected clients.
*/

package gossip

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	circuit "github.com/rubyist/circuitbreaker"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	// maxHops is the maximum number of hops which any gossip info
	// should require to transit between any two nodes in a gossip
	// network.
	maxHops = 5

	// minPeers is the minimum number of peers which the maxPeers()
	// function will return. This is set higher than one to prevent
	// excessive tightening of the network.
	minPeers = 3

	// ttlNodeDescriptorGossip is time-to-live for node ID -> descriptor.
	ttlNodeDescriptorGossip = 1 * time.Hour

	// defaultStallInterval is the default interval for checking whether
	// the incoming and outgoing connections to the gossip network are
	// insufficient to keep the network connected.
	defaultStallInterval = 30 * time.Second

	// defaultBootstrapInterval is the minimum time between successive
	// bootstrapping attempts to avoid busy-looping trying to find the
	// sentinel gossip info.
	defaultBootstrapInterval = 1 * time.Second

	// defaultCullInterval is the default interval for culling the least
	// "useful" outgoing gossip connection to free up space for a more
	// efficiently targeted connection to the most distant node.
	defaultCullInterval = 60 * time.Second

	// defaultGossipStoresInterval is the default interval for gossiping
	// store descriptors.
	defaultGossipStoresInterval = 60 * time.Second

	unknownNodeID roachpb.NodeID = 0
)

// Gossip metrics counter names.
var (
	MetaConnectionsIncomingGauge = metric.Metadata{
		Name: "gossip.connections.incoming",
		Help: "Number of active incoming gossip connections"}
	MetaConnectionsOutgoingGauge = metric.Metadata{
		Name: "gossip.connections.outgoing",
		Help: "Number of active outgoing gossip connections"}
	MetaConnectionsRefused = metric.Metadata{
		Name: "gossip.connections.refused",
		Help: "Number of refused incoming gossip connections"}
	MetaInfosSent = metric.Metadata{
		Name: "gossip.infos.sent",
		Help: "Number of sent gossip Info objects"}
	MetaInfosReceived = metric.Metadata{
		Name: "gossip.infos.received",
		Help: "Number of received gossip Info objects"}
	MetaBytesSent = metric.Metadata{
		Name: "gossip.bytes.sent",
		Help: "Number of sent gossip bytes"}
	MetaBytesReceived = metric.Metadata{
		Name: "gossip.bytes.received",
		Help: "Number of received gossip bytes"}
)

var (
	// GossipStoresInterval is the interval for gossipping storage-related info.
	GossipStoresInterval = envutil.EnvOrDefaultDuration("COCKROACH_GOSSIP_STORES_INTERVAL",
		defaultGossipStoresInterval)
)

// Storage is an interface which allows the gossip instance
// to read and write bootstrapping data to persistent storage
// between instantiations.
type Storage interface {
	// ReadBootstrapInfo fetches the bootstrap data from the persistent
	// store into the provided bootstrap protobuf. Returns nil or an
	// error on failure.
	ReadBootstrapInfo(*BootstrapInfo) error
	// WriteBootstrapInfo stores the provided bootstrap data to the
	// persistent store. Returns nil or an error on failure.
	WriteBootstrapInfo(*BootstrapInfo) error
}

// Gossip is an instance of a gossip node. It embeds a gossip server.
// During bootstrapping, the bootstrap list contains candidates for
// entry to the gossip network.
type Gossip struct {
	*server // Embedded gossip RPC server

	Connected     chan struct{}       // Closed upon initial connection
	hasConnected  bool                // Set first time network is connected
	rpcContext    *rpc.Context        // The context required for RPC
	outgoing      nodeSet             // Set of outgoing client node IDs
	storage       Storage             // Persistent storage interface
	bootstrapInfo BootstrapInfo       // BootstrapInfo proto for persistent storage
	bootstrapping map[string]struct{} // Set of active bootstrap clients
	hasCleanedBS  bool

	// Note that access to each client's internal state is serialized by the
	// embedded server's mutex. This is surprising!
	clientsMu struct {
		syncutil.Mutex
		clients []*client
		// One breaker per client for the life of the process.
		breakers map[string]*circuit.Breaker
	}

	disconnected chan *client  // Channel of disconnected clients
	stalled      bool          // True if gossip is stalled (i.e. host doesn't have sentinel)
	stalledCh    chan struct{} // Channel to wake up stalled bootstrap

	stallInterval     time.Duration
	bootstrapInterval time.Duration
	cullInterval      time.Duration

	// The system config is treated unlike other info objects.
	// It is used so often that we keep an unmarshalled version of it
	// here and its own set of callbacks.
	// We do not use the infostore to avoid unmarshalling under the
	// main gossip lock.
	systemConfig         config.SystemConfig
	systemConfigSet      bool
	systemConfigMu       syncutil.RWMutex
	systemConfigChannels []chan<- struct{}

	// resolvers is a list of resolvers used to determine
	// bootstrap hosts for connecting to the gossip network.
	resolverIdx    int
	resolvers      []resolver.Resolver
	resolversTried map[int]struct{} // Set of attempted resolver indexes
	nodeDescs      map[roachpb.NodeID]*roachpb.NodeDescriptor

	// Membership sets for resolvers and bootstrap addresses.
	// bootstrapAddrs also tracks which address is associated with which
	// node ID to enable faster node lookup by address.
	resolverAddrs  map[util.UnresolvedAddr]resolver.Resolver
	bootstrapAddrs map[util.UnresolvedAddr]roachpb.NodeID
}

// New creates an instance of a gossip node.
// The higher level manages the NodeIDContainer instance (which can be shared by
// various server components). The ambient context is expected to already
// contain the node ID.
func New(
	ambient log.AmbientContext,
	nodeID *base.NodeIDContainer,
	rpcContext *rpc.Context,
	grpcServer *grpc.Server,
	stopper *stop.Stopper,
	registry *metric.Registry,
) *Gossip {
	ambient.SetEventLog("gossip", "gossip")
	g := &Gossip{
		server:            newServer(ambient, nodeID, stopper, registry),
		Connected:         make(chan struct{}),
		rpcContext:        rpcContext,
		outgoing:          makeNodeSet(minPeers, metric.NewGauge(MetaConnectionsOutgoingGauge)),
		bootstrapping:     map[string]struct{}{},
		disconnected:      make(chan *client, 10),
		stalledCh:         make(chan struct{}, 1),
		stallInterval:     defaultStallInterval,
		bootstrapInterval: defaultBootstrapInterval,
		cullInterval:      defaultCullInterval,
		resolversTried:    map[int]struct{}{},
		nodeDescs:         map[roachpb.NodeID]*roachpb.NodeDescriptor{},
		resolverAddrs:     map[util.UnresolvedAddr]resolver.Resolver{},
		bootstrapAddrs:    map[util.UnresolvedAddr]roachpb.NodeID{},
	}
	stopper.AddCloser(stop.CloserFn(g.server.AmbientContext.FinishEventLog))

	registry.AddMetric(g.outgoing.gauge)
	g.clientsMu.breakers = map[string]*circuit.Breaker{}

	g.mu.Lock()
	// Add ourselves as a SystemConfig watcher.
	g.mu.is.registerCallback(KeySystemConfig, g.updateSystemConfig)
	// Add ourselves as a node descriptor watcher.
	g.mu.is.registerCallback(MakePrefixPattern(KeyNodeIDPrefix), g.updateNodeAddress)
	g.mu.Unlock()

	RegisterGossipServer(grpcServer, g.server)
	return g
}

// NewTest is a simplified wrapper around New that creates the NodeIDContainer
// internally. Used for testing.
func NewTest(
	nodeID roachpb.NodeID,
	rpcContext *rpc.Context,
	grpcServer *grpc.Server,
	stopper *stop.Stopper,
	registry *metric.Registry,
) *Gossip {
	n := &base.NodeIDContainer{}
	var ac log.AmbientContext
	ac.AddLogTag("n", n)
	gossip := New(ac, n, rpcContext, grpcServer, stopper, registry)
	if nodeID != 0 {
		n.Set(context.TODO(), nodeID)
	}
	return gossip
}

// GetNodeMetrics returns the gossip node metrics.
func (g *Gossip) GetNodeMetrics() *Metrics {
	return g.server.GetNodeMetrics()
}

// SetNodeDescriptor adds the node descriptor to the gossip network.
func (g *Gossip) SetNodeDescriptor(desc *roachpb.NodeDescriptor) error {
	ctx := g.AnnotateCtx(context.TODO())
	log.Infof(ctx, "NodeDescriptor set to %+v", desc)
	if err := g.AddInfoProto(MakeNodeIDKey(desc.NodeID), desc, ttlNodeDescriptorGossip); err != nil {
		return errors.Errorf("node %d: couldn't gossip descriptor: %v", desc.NodeID, err)
	}
	return nil
}

// SetStallInterval sets the interval between successive checks
// to determine whether this host is not connected to the gossip
// network, or else is connected to a partition which doesn't
// include the host which gossips the sentinel info.
func (g *Gossip) SetStallInterval(interval time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stallInterval = interval
}

// SetBootstrapInterval sets a minimum interval between successive
// attempts to connect to new hosts in order to join the gossip
// network.
func (g *Gossip) SetBootstrapInterval(interval time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.bootstrapInterval = interval
}

// SetCullInterval sets the interval between periodic shutdown of
// outgoing gossip client connections in an effort to improve the
// fitness of the network.
func (g *Gossip) SetCullInterval(interval time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.cullInterval = interval
}

// SetStorage provides an instance of the Storage interface
// for reading and writing gossip bootstrap data from persistent
// storage. This should be invoked as early in the lifecycle of a
// gossip instance as possible, but can be called at any time.
func (g *Gossip) SetStorage(storage Storage) error {
	ctx := g.AnnotateCtx(context.TODO())
	// Maintain lock ordering.
	var storedBI BootstrapInfo
	if err := storage.ReadBootstrapInfo(&storedBI); err != nil {
		log.Warningf(ctx, "failed to read gossip bootstrap info: %s", err)
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	g.storage = storage

	// Merge the stored bootstrap info addresses with any we've become
	// aware of through gossip.
	existing := map[string]struct{}{}
	makeKey := func(a util.UnresolvedAddr) string { return fmt.Sprintf("%s,%s", a.Network(), a.String()) }
	for _, addr := range g.bootstrapInfo.Addresses {
		existing[makeKey(addr)] = struct{}{}
	}
	for _, addr := range storedBI.Addresses {
		// If the address is new, and isn't our own address, add it.
		if _, ok := existing[makeKey(addr)]; !ok && addr != g.mu.is.NodeAddr {
			g.maybeAddBootstrapAddressLocked(addr, unknownNodeID)
		}
	}
	// Persist merged addresses.
	if numAddrs := len(g.bootstrapInfo.Addresses); numAddrs > len(storedBI.Addresses) {
		if err := g.storage.WriteBootstrapInfo(&g.bootstrapInfo); err != nil {
			log.Error(ctx, err)
		}
	}

	// Cycle through all persisted bootstrap hosts and add resolvers for
	// any which haven't already been added.
	newResolverFound := false
	for _, addr := range g.bootstrapInfo.Addresses {
		if !g.maybeAddResolverLocked(addr) {
			continue
		}
		// If we find a new resolver, reset the resolver index so that the
		// next resolver we try is the first of the new resolvers.
		if !newResolverFound {
			newResolverFound = true
			g.resolverIdx = len(g.resolvers) - 1
		}
	}

	// If a new resolver was found, immediately signal bootstrap.
	if newResolverFound {
		if log.V(1) {
			log.Infof(ctx, "found new resolvers from storage; signalling bootstrap")
		}
		g.signalStalledLocked()
	}
	return nil
}

// setResolvers initializes the set of gossip resolvers used to find
// nodes to bootstrap the gossip network.
func (g *Gossip) setResolvers(resolvers []resolver.Resolver) {
	if resolvers == nil {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Start index at end because get next address loop logic increments as first step.
	g.resolverIdx = len(resolvers) - 1
	g.resolvers = resolvers
	g.resolversTried = map[int]struct{}{}

	// Start new bootstrapping immediately instead of waiting for next bootstrap interval.
	g.maybeSignalStatusChangeLocked()
}

// GetResolvers returns a copy of the resolvers slice.
func (g *Gossip) GetResolvers() []resolver.Resolver {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]resolver.Resolver(nil), g.resolvers...)
}

// GetNodeIDAddress looks up the address of the node by ID.
func (g *Gossip) GetNodeIDAddress(nodeID roachpb.NodeID) (*util.UnresolvedAddr, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.getNodeIDAddressLocked(nodeID)
}

// GetNodeDescriptor looks up the descriptor of the node by ID.
func (g *Gossip) GetNodeDescriptor(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.getNodeDescriptorLocked(nodeID)
}

// LogStatus logs the current status of gossip such as the incoming and
// outgoing connections.
func (g *Gossip) LogStatus() {
	g.mu.Lock()
	n := len(g.nodeDescs)
	status := "ok"
	if g.mu.is.getInfo(KeySentinel) == nil {
		status = "stalled"
	}
	g.mu.Unlock()

	ctx := g.AnnotateCtx(context.TODO())
	log.Infof(
		ctx, "gossip status (%s, %d node%s)\n%s%s", status, n, util.Pluralize(int64(n)),
		g.clientStatus(), g.server.status(),
	)
}

func (g *Gossip) clientStatus() string {
	var buf bytes.Buffer

	g.mu.Lock()
	defer g.mu.Unlock()
	g.clientsMu.Lock()
	defer g.clientsMu.Unlock()

	fmt.Fprintf(&buf, "gossip client (%d/%d cur/max conns)\n", len(g.clientsMu.clients), g.outgoing.maxSize)
	for _, c := range g.clientsMu.clients {
		fmt.Fprintf(&buf, "  %d: %s (%s: %s)\n",
			c.peerID, c.addr, roundSecs(timeutil.Since(c.createdAt)), c.clientMetrics)
	}
	return buf.String()
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
		// TODO(spencer): remove this nil check when gossip/simulation is no
		// longer used in kv tests.
		if g.simulationCycler != nil {
			g.simulationCycler.Broadcast()
			g.simulationCycler = nil
		}
	}
}

// SimulationCycle cycles this gossip node's server by allowing all
// connected clients to proceed one step.
func (g *Gossip) SimulationCycle() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.simulationCycler != nil {
		g.simulationCycler.Broadcast()
	}
}

// maybeAddResolverLocked creates and adds a resolver for the specified
// address if one does not already exist. Returns whether a new
// resolver was added. The caller must hold the gossip mutex.
func (g *Gossip) maybeAddResolverLocked(addr util.UnresolvedAddr) bool {
	if _, ok := g.resolverAddrs[addr]; ok {
		return false
	}
	ctx := g.AnnotateCtx(context.TODO())
	r, err := resolver.NewResolverFromUnresolvedAddr(addr)
	if err != nil {
		log.Warningf(ctx, "bad address %s: %s", addr, err)
		return false
	}
	g.resolvers = append(g.resolvers, r)
	g.resolverAddrs[addr] = r
	log.Eventf(ctx, "add resolver %s", r)
	return true
}

// maybeAddBootstrapAddressLocked adds the specified address to the list
// of bootstrap addresses if not already present. Returns whether a new
// bootstrap address was added. The caller must hold the gossip mutex.
func (g *Gossip) maybeAddBootstrapAddressLocked(
	addr util.UnresolvedAddr, nodeID roachpb.NodeID,
) bool {
	if existingNodeID, ok := g.bootstrapAddrs[addr]; ok {
		if existingNodeID == unknownNodeID || existingNodeID != nodeID {
			g.bootstrapAddrs[addr] = nodeID
		}
		return false
	}
	g.bootstrapInfo.Addresses = append(g.bootstrapInfo.Addresses, addr)
	g.bootstrapAddrs[addr] = nodeID
	ctx := g.AnnotateCtx(context.TODO())
	log.Eventf(ctx, "add bootstrap %s", addr)
	return true
}

// maybeCleanupBootstrapAddresses cleans up the stored bootstrap addresses to
// include only those currently available via gossip. The gossip mutex must
// be held by the caller.
func (g *Gossip) maybeCleanupBootstrapAddressesLocked() {
	if g.storage == nil || g.hasCleanedBS {
		return
	}
	defer func() { g.hasCleanedBS = true }()
	ctx := g.AnnotateCtx(context.TODO())
	log.Event(ctx, "cleaning up bootstrap addresses")

	g.resolvers = g.resolvers[:0]
	g.resolverIdx = 0
	g.bootstrapInfo.Addresses = g.bootstrapInfo.Addresses[:0]
	g.bootstrapAddrs = map[util.UnresolvedAddr]roachpb.NodeID{}
	g.resolverAddrs = map[util.UnresolvedAddr]resolver.Resolver{}
	g.resolversTried = map[int]struct{}{}

	var desc roachpb.NodeDescriptor
	if err := g.mu.is.visitInfos(func(key string, i *Info) error {
		if strings.HasPrefix(key, KeyNodeIDPrefix) {
			if err := i.Value.GetProto(&desc); err != nil {
				return err
			}
			if desc.Address.IsEmpty() || desc.Address == g.mu.is.NodeAddr {
				return nil
			}
			g.maybeAddResolverLocked(desc.Address)
			g.maybeAddBootstrapAddressLocked(desc.Address, desc.NodeID)
		}
		return nil
	}); err != nil {
		log.Error(ctx, err)
		return
	}

	if err := g.storage.WriteBootstrapInfo(&g.bootstrapInfo); err != nil {
		log.Error(ctx, err)
	}
}

// maxPeers returns the maximum number of peers each gossip node
// may connect to. This is based on maxHops, which is a preset
// maximum for number of hops allowed before the gossip network
// will seek to "tighten" by creating new connections to distant
// nodes.
func maxPeers(nodeCount int) int {
	// This formula uses maxHops-1, instead of maxHops, to provide a
	// "fudge" factor for max connected peers, to account for the
	// arbitrary, decentralized way in which gossip networks are created.
	//
	// Quick derivation of the formula for posterity (without the fudge factor):
	// maxPeers^maxHops > nodeCount
	// maxHops * log(maxPeers) > log(nodeCount)
	// log(maxPeers) > log(nodeCount) / maxHops
	// maxPeers > e^(log(nodeCount) / maxHops)
	// hence maxPeers = ceil(e^(log(nodeCount) / maxHops)) should work
	maxPeers := int(math.Ceil(math.Exp(math.Log(float64(nodeCount)) / float64(maxHops-1))))
	if maxPeers < minPeers {
		return minPeers
	}
	return maxPeers
}

// updateNodeAddress is a gossip callback which fires with each
// update to a node descriptor. This allows us to compute the
// total size of the gossip network (for determining max peers
// each gossip node is allowed to have), as well as to create
// new resolvers for each encountered host and to write the
// set of gossip node addresses to persistent storage when it
// changes.
func (g *Gossip) updateNodeAddress(key string, content roachpb.Value) {
	ctx := g.AnnotateCtx(context.TODO())
	var desc roachpb.NodeDescriptor
	if err := content.GetProto(&desc); err != nil {
		log.Error(ctx, err)
		return
	}
	if log.V(1) {
		log.Infof(ctx, "updateNodeAddress called on %q with desc %+v", key, desc)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// If desc is the empty descriptor, that indicates that the node has been
	// removed from the cluster. If that's the case, remove it from our map of
	// nodes to prevent other parts of the system from trying to talk to it.
	// We can't directly compare the node against the empty descriptor because
	// the proto has a repeated field and thus isn't comparable.
	if desc.NodeID == 0 && desc.Address.IsEmpty() {
		nodeID, err := NodeIDFromKey(key)
		if err != nil {
			log.Errorf(ctx, "unable to update node address for removed node: %s", err)
			return
		}
		log.Infof(ctx, "removed node %d from gossip", nodeID)
		g.removeNodeDescriptorLocked(nodeID)
		return
	}

	existingDesc, ok := g.nodeDescs[desc.NodeID]
	if !ok || !proto.Equal(existingDesc, &desc) {
		g.nodeDescs[desc.NodeID] = &desc
	}
	// Skip all remaining logic if the address hasn't changed, since that's all
	// the logic cares about.
	if ok && existingDesc.Address == desc.Address {
		return
	}
	g.recomputeMaxPeersLocked()

	// Skip if it's our own address.
	if desc.Address == g.mu.is.NodeAddr {
		return
	}

	// Add this new node address (if it's not already there) to our list
	// of resolvers so we can keep connecting to gossip if the original
	// resolvers go offline.
	g.maybeAddResolverLocked(desc.Address)

	// We ignore empty addresses for the sake of not breaking the many tests
	// that don't bother specifying addresses.
	if desc.Address.IsEmpty() {
		return
	}

	// If the new node's address conflicts with another node's address, then it
	// must be the case that the new node has replaced the previous one. Remove
	// it from our set of tracked descriptors to ensure we don't attempt to
	// connect to its previous identity (as came up in issue #10266).
	oldNodeID, ok := g.bootstrapAddrs[desc.Address]
	if ok && oldNodeID != unknownNodeID && oldNodeID != desc.NodeID {
		log.Infof(ctx, "removing node %d which was at same address (%s) as new node %v",
			oldNodeID, desc.Address, desc)
		g.removeNodeDescriptorLocked(oldNodeID)

		// Deleting the local copy isn't enough to remove the node from the gossip
		// network. We also have to clear it out in the infoStore by overwriting
		// it with an empty descriptor, which can be represented as just an empty
		// byte array due to how protocol buffers are serialized.
		// Calling addInfoLocked here is somewhat recursive since
		// updateNodeAddress is typically called in response to the infoStore
		// being updated but won't lead to deadlock because it's called
		// asynchronously.
		key := MakeNodeIDKey(oldNodeID)
		var emptyProto []byte
		if err := g.addInfoLocked(key, emptyProto, ttlNodeDescriptorGossip); err != nil {
			log.Errorf(ctx, "failed to empty node descriptor for node %d: %s", oldNodeID, err)
		}
	}
	// Add new address (if it's not already there) to bootstrap info and
	// persist if possible.
	added := g.maybeAddBootstrapAddressLocked(desc.Address, desc.NodeID)
	if added && g.storage != nil {
		if err := g.storage.WriteBootstrapInfo(&g.bootstrapInfo); err != nil {
			log.Error(ctx, err)
		}
	}
}

func (g *Gossip) removeNodeDescriptorLocked(nodeID roachpb.NodeID) {
	delete(g.nodeDescs, nodeID)
	g.recomputeMaxPeersLocked()
}

// recomputeMaxPeersLocked recomputes max peers based on size of
// network and set the max sizes for incoming and outgoing node sets.
//
// Note: if we notice issues with never-ending connection refused errors
// in real deployments, consider allowing more incoming connections than
// outgoing connections. As of now, the cluster's steady state is to have
// all nodes fill up, which can make rebalancing of connections tough.
// I'm not making this change now since it tends to lead to less balanced
// networks and I'm not sure what all the consequences of that might be.
func (g *Gossip) recomputeMaxPeersLocked() {
	maxPeers := maxPeers(len(g.nodeDescs))
	g.mu.incoming.setMaxSize(maxPeers)
	g.outgoing.setMaxSize(maxPeers)
}

// getNodeDescriptorLocked looks up the descriptor of the node by ID. The mutex
// is assumed held by the caller. This method is called externally via
// GetNodeDescriptor and internally by getNodeIDAddressLocked.
func (g *Gossip) getNodeDescriptorLocked(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	if desc, ok := g.nodeDescs[nodeID]; ok {
		return desc, nil
	}

	// Fallback to retrieving the node info and unmarshalling the node
	// descriptor. This path occurs in tests which add a node descriptor to
	// gossip and then immediately try retrieve it.
	nodeIDKey := MakeNodeIDKey(nodeID)

	// We can't use GetInfoProto here because that method grabs the lock.
	if i := g.mu.is.getInfo(nodeIDKey); i != nil {
		if err := i.Value.Verify([]byte(nodeIDKey)); err != nil {
			return nil, err
		}
		nodeDescriptor := &roachpb.NodeDescriptor{}
		if err := i.Value.GetProto(nodeDescriptor); err != nil {
			return nil, err
		}
		// Don't return node descriptors that are empty, because that's meant to
		// indicate that the node has been removed from the cluster.
		if !(nodeDescriptor.NodeID == 0 && nodeDescriptor.Address.IsEmpty()) {
			return nodeDescriptor, nil
		}
	}

	return nil, errors.Errorf("unable to look up descriptor for node %d", nodeID)
}

// getNodeIDAddressLocked looks up the address of the node by ID. The mutex is
// assumed held by the caller. This method is called externally via
// GetNodeIDAddress or internally when looking up a "distant" node address to
// connect directly to.
func (g *Gossip) getNodeIDAddressLocked(nodeID roachpb.NodeID) (*util.UnresolvedAddr, error) {
	nd, err := g.getNodeDescriptorLocked(nodeID)
	if err != nil {
		return nil, err
	}
	return &nd.Address, nil
}

// AddInfo adds or updates an info object. Returns an error if info
// couldn't be added.
func (g *Gossip) AddInfo(key string, val []byte, ttl time.Duration) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.addInfoLocked(key, val, ttl)
}

// addInfoLocked adds or updates an info object. The mutex is assumed held by
// the caller. Returns an error if info couldn't be added.
func (g *Gossip) addInfoLocked(key string, val []byte, ttl time.Duration) error {
	err := g.mu.is.addInfo(key, g.mu.is.newInfo(val, ttl))
	if err == nil {
		g.signalConnectedLocked()
	}
	return err
}

// AddInfoProto adds or updates an info object. Returns an error if info
// couldn't be added.
func (g *Gossip) AddInfoProto(key string, msg proto.Message, ttl time.Duration) error {
	bytes, err := protoutil.Marshal(msg)
	if err != nil {
		return err
	}
	return g.AddInfo(key, bytes, ttl)
}

// GetInfo returns an info value by key or an error if specified
// key does not exist or has expired.
func (g *Gossip) GetInfo(key string) ([]byte, error) {
	g.mu.Lock()
	i := g.mu.is.getInfo(key)
	g.mu.Unlock()

	if i != nil {
		if err := i.Value.Verify([]byte(key)); err != nil {
			return nil, err
		}
		return i.Value.GetBytes()
	}
	return nil, errors.Errorf("key %q does not exist or has expired", key)
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

// GetInfoStatus returns the a copy of the contents of the infostore.
func (g *Gossip) GetInfoStatus() InfoStatus {
	g.mu.Lock()
	defer g.mu.Unlock()
	is := InfoStatus{
		Infos: make(map[string]Info),
	}
	for k, v := range g.mu.is.Infos {
		is.Infos[k] = *protoutil.Clone(v).(*Info)
	}
	return is
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
		ctx := g.AnnotateCtx(context.TODO())
		log.Warningf(
			ctx,
			"raw gossip callback registered on %s, consider using RegisterSystemConfigChannel",
			KeySystemConfig,
		)
	}

	g.mu.Lock()
	unregister := g.mu.is.registerCallback(pattern, method)
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		unregister()
		g.mu.Unlock()
	}
}

// GetSystemConfig returns the local unmarshalled version of the system config.
// The second return value indicates whether the system config has been set yet.
func (g *Gossip) GetSystemConfig() (config.SystemConfig, bool) {
	g.systemConfigMu.RLock()
	defer g.systemConfigMu.RUnlock()
	return g.systemConfig, g.systemConfigSet
}

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
	if g.systemConfigSet {
		c <- struct{}{}
	}

	return c
}

// updateSystemConfig is the raw gossip info callback.
// Unmarshal the system config, and if successfully, update out
// copy and run the callbacks.
func (g *Gossip) updateSystemConfig(key string, content roachpb.Value) {
	ctx := g.AnnotateCtx(context.TODO())
	if key != KeySystemConfig {
		log.Fatalf(ctx, "wrong key received on SystemConfig callback: %s", key)
	}
	cfg := config.SystemConfig{}
	if err := content.GetProto(&cfg); err != nil {
		log.Errorf(ctx, "could not unmarshal system config on callback: %s", err)
		return
	}

	g.systemConfigMu.Lock()
	defer g.systemConfigMu.Unlock()
	g.systemConfig = cfg
	g.systemConfigSet = true
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
	return g.mu.incoming.asSlice()
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

// MaxHops returns the maximum number of hops to reach any other
// node in the system, according to the infos which have reached
// this node via gossip network.
func (g *Gossip) MaxHops() uint32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, maxHops := g.mu.is.mostDistant(func(_ roachpb.NodeID) bool { return false })
	return maxHops
}

// Start launches the gossip instance, which commences joining the
// gossip network using the supplied rpc server and previously known
// peer addresses in addition to any bootstrap addresses specified via
// --join and passed to this method via the resolvers parameter.
//
// The supplied advertised address is used to identify the gossip
// instance in the gossip network; it will be used by other instances
// to connect to this instance.
//
// This method starts bootstrap loop, gossip server, and client
// management in separate goroutines and returns.
func (g *Gossip) Start(advertAddr net.Addr, resolvers []resolver.Resolver) {
	g.setResolvers(resolvers)
	g.server.start(advertAddr) // serve gossip protocol
	g.bootstrap()              // bootstrap gossip client
	g.manage()                 // manage gossip clients
}

// hasIncomingLocked returns whether the server has an incoming gossip
// client matching the provided node ID. Mutex should be held by
// caller.
func (g *Gossip) hasIncomingLocked(nodeID roachpb.NodeID) bool {
	return g.mu.incoming.hasNode(nodeID)
}

// hasOutgoingLocked returns whether the server has an outgoing gossip
// client matching the provided node ID. Mutex should be held by
// caller.
func (g *Gossip) hasOutgoingLocked(nodeID roachpb.NodeID) bool {
	// We have to use findClient and compare node addresses rather than using the
	// outgoing nodeSet due to the way that outgoing clients' node IDs are only
	// resolved once the connection has been established (rather than as soon as
	// we've created it).
	nodeAddr, err := g.getNodeIDAddressLocked(nodeID)
	if err != nil {
		// If we don't have the address, fall back to using the outgoing nodeSet
		// since at least it's better than nothing.
		ctx := g.AnnotateCtx(context.TODO())
		log.Errorf(ctx, "unable to get address for node %d: %s", nodeID, err)
		return g.outgoing.hasNode(nodeID)
	}
	c := g.findClient(func(c *client) bool {
		return c.addr.String() == nodeAddr.String()
	})
	return c != nil
}

// getNextBootstrapAddress returns the next available bootstrap
// address by consulting the first non-exhausted resolver from the
// slice supplied to the constructor or set using setBootstrap().
// The lock is assumed held.
func (g *Gossip) getNextBootstrapAddressLocked() net.Addr {
	// Run through resolvers round robin starting at last resolved index.
	for i := 0; i < len(g.resolvers); i++ {
		g.resolverIdx++
		g.resolverIdx %= len(g.resolvers)
		defer func(idx int) { g.resolversTried[idx] = struct{}{} }(g.resolverIdx)
		resolver := g.resolvers[g.resolverIdx]
		if addr, err := resolver.GetAddress(); err != nil {
			if _, ok := g.resolversTried[g.resolverIdx]; !ok {
				ctx := g.AnnotateCtx(context.TODO())
				log.Warningf(ctx, "invalid bootstrap address: %+v, %v", resolver, err)
			}
			continue
		} else {
			addrStr := addr.String()
			if _, addrActive := g.bootstrapping[addrStr]; !addrActive {
				g.bootstrapping[addrStr] = struct{}{}
				return addr
			}
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
func (g *Gossip) bootstrap() {
	ctx := g.AnnotateCtx(context.Background())
	g.server.stopper.RunWorker(ctx, func(ctx context.Context) {
		ctx = log.WithLogTag(ctx, "bootstrap", nil)
		var bootstrapTimer timeutil.Timer
		defer bootstrapTimer.Stop()
		for {
			if g.server.stopper.RunTask(ctx, func(ctx context.Context) {
				g.mu.Lock()
				defer g.mu.Unlock()
				haveClients := g.outgoing.len() > 0
				haveSentinel := g.mu.is.getInfo(KeySentinel) != nil
				log.Eventf(ctx, "have clients: %t, have sentinel: %t", haveClients, haveSentinel)
				if !haveClients || !haveSentinel {
					// Try to get another bootstrap address from the resolvers.
					if addr := g.getNextBootstrapAddressLocked(); addr != nil {
						g.startClientLocked(addr)
					} else {
						bootstrapAddrs := make([]string, 0, len(g.bootstrapping))
						for addr := range g.bootstrapping {
							bootstrapAddrs = append(bootstrapAddrs, addr)
						}
						log.Eventf(ctx, "no next bootstrap address; currently bootstrapping: %v", bootstrapAddrs)
						// We couldn't start a client, signal that we're stalled so that
						// we'll retry.
						g.maybeSignalStatusChangeLocked()
					}
				}
			}) != nil {
				return
			}

			// Pause an interval before next possible bootstrap.
			bootstrapTimer.Reset(g.bootstrapInterval)
			log.Eventf(ctx, "sleeping %s until bootstrap", g.bootstrapInterval)
			select {
			case <-bootstrapTimer.C:
				bootstrapTimer.Read = true
				// break
			case <-g.server.stopper.ShouldStop():
				return
			}
			log.Eventf(ctx, "idling until bootstrap required")
			// Block until we need bootstrapping again.
			select {
			case <-g.stalledCh:
				log.Eventf(ctx, "detected stall; commencing bootstrap")
				// break
			case <-g.server.stopper.ShouldStop():
				return
			}
		}
	})
}

// manage manages outgoing clients. Periodically, the infostore is
// scanned for infos with hop count exceeding the maxHops
// threshold. If the number of outgoing clients doesn't exceed
// maxPeers(), a new gossip client is connected to a randomly selected
// peer beyond maxHops threshold. Otherwise, the least useful peer
// node is cut off to make room for a replacement. Disconnected
// clients are processed via the disconnected channel and taken out of
// the outgoing address set. If there are no longer any outgoing
// connections or the sentinel gossip is unavailable, the bootstrapper
// is notified via the stalled conditional variable.
func (g *Gossip) manage() {
	ctx := g.AnnotateCtx(context.Background())
	g.server.stopper.RunWorker(ctx, func(ctx context.Context) {
		cullTicker := time.NewTicker(g.jitteredInterval(g.cullInterval))
		stallTicker := time.NewTicker(g.jitteredInterval(g.stallInterval))
		defer cullTicker.Stop()
		defer stallTicker.Stop()
		for {
			select {
			case <-g.server.stopper.ShouldStop():
				return
			case c := <-g.disconnected:
				g.doDisconnected(c)
			case <-g.tighten:
				g.tightenNetwork(ctx)
			case <-cullTicker.C:
				func() {
					g.mu.Lock()
					if !g.outgoing.hasSpace() {
						leastUsefulID := g.mu.is.leastUseful(g.outgoing)

						if c := g.findClient(func(c *client) bool {
							return c.peerID == leastUsefulID
						}); c != nil {
							if log.V(1) {
								log.Infof(ctx, "closing least useful client %+v to tighten network graph", c)
							}
							log.Eventf(ctx, "culling %s", c.addr)
							c.close()

							// After releasing the lock, block until the client disconnects.
							defer func() {
								g.doDisconnected(<-g.disconnected)
							}()
						} else {
							if log.V(1) {
								g.clientsMu.Lock()
								log.Infof(ctx, "couldn't find least useful client among %+v", g.clientsMu.clients)
								g.clientsMu.Unlock()
							}
						}
					}
					g.mu.Unlock()
				}()
			case <-stallTicker.C:
				g.mu.Lock()
				g.maybeSignalStatusChangeLocked()
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

// tightenNetwork "tightens" the network by starting a new gossip client to the
// client to the most distant node to which we don't already have an outgoing
// connection. Does nothing if we don't have room for any more outgoing
// connections.
func (g *Gossip) tightenNetwork(ctx context.Context) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.outgoing.hasSpace() {
		distantNodeID, distantHops := g.mu.is.mostDistant(g.hasOutgoingLocked)
		log.VEventf(ctx, 2, "distantHops: %d from %d", distantHops, distantNodeID)
		if distantHops <= maxHops {
			return
		}
		if nodeAddr, err := g.getNodeIDAddressLocked(distantNodeID); err != nil {
			log.Errorf(ctx, "unable to get address for distant node %d: %s", distantNodeID, err)
		} else {
			log.Infof(ctx, "starting client to distant node %d (%d > %d) to tighten network graph",
				distantNodeID, distantHops, maxHops)
			log.Eventf(ctx, "tightening network with new client to %s", nodeAddr)
			g.startClientLocked(nodeAddr)
		}
	}
}

func (g *Gossip) doDisconnected(c *client) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.removeClientLocked(c)

	// If the client was disconnected with a forwarding address, connect now.
	if c.forwardAddr != nil {
		g.startClientLocked(c.forwardAddr)
	}
	g.maybeSignalStatusChangeLocked()
}

// maybeSignalStatusChangeLocked checks whether gossip should transition its
// internal state from connected to stalled or vice versa.
func (g *Gossip) maybeSignalStatusChangeLocked() {
	ctx := g.AnnotateCtx(context.TODO())
	orphaned := g.outgoing.len()+g.mu.incoming.len() == 0
	stalled := orphaned || g.mu.is.getInfo(KeySentinel) == nil
	if stalled {
		// We employ the stalled boolean to avoid filling logs with warnings.
		if !g.stalled {
			log.Eventf(ctx, "now stalled")
			if orphaned {
				if len(g.resolvers) == 0 {
					if log.V(1) {
						log.Warningf(ctx, "no resolvers found; use --join to specify a connected node")
					}
				} else {
					log.Warningf(ctx, "no incoming or outgoing connections")
				}
			} else if len(g.resolversTried) == len(g.resolvers) {
				log.Warningf(ctx, "first range unavailable; resolvers exhausted")
			} else {
				log.Warningf(ctx, "first range unavailable; trying remaining resolvers")
			}
		}
		if len(g.resolvers) > 0 {
			g.signalStalledLocked()
		}
	} else {
		if g.stalled {
			log.Eventf(ctx, "connected")
			log.Infof(ctx, "node has connected to cluster via gossip")
			g.signalConnectedLocked()
		}
		g.maybeCleanupBootstrapAddressesLocked()
	}
	g.stalled = stalled
}

func (g *Gossip) signalStalledLocked() {
	select {
	case g.stalledCh <- struct{}{}:
	default:
	}
}

// signalConnectedLocked checks whether this gossip instance is connected to
// enough of the gossip network that it has received the cluster ID gossip
// info. Once connected, the "Connected" channel is closed to signal to any
// waiters that the gossip instance is ready. The gossip mutex should be held
// by caller.
//
// TODO(tschottdorf): this is called from various locations which seem ad-hoc
// (with the exception of the call bootstrap loop) yet necessary. Consolidate
// and add commentary at each callsite.
func (g *Gossip) signalConnectedLocked() {
	// Check if we have the cluster ID gossip to start.
	// If so, then mark ourselves as trivially connected to the gossip network.
	if !g.hasConnected && g.mu.is.getInfo(KeyClusterID) != nil {
		g.hasConnected = true
		close(g.Connected)
	}
}

// startClientLocked launches a new client connected to remote address.
// The client is added to the outgoing address set and launched in
// a goroutine.
func (g *Gossip) startClientLocked(addr net.Addr) {
	g.clientsMu.Lock()
	defer g.clientsMu.Unlock()
	breaker, ok := g.clientsMu.breakers[addr.String()]
	if !ok {
		breaker = g.rpcContext.NewBreaker()
		g.clientsMu.breakers[addr.String()] = breaker
	}
	ctx := g.AnnotateCtx(context.TODO())
	log.Eventf(ctx, "starting new client to %s", addr)
	c := newClient(g.server.AmbientContext, addr, g.serverMetrics)
	g.clientsMu.clients = append(g.clientsMu.clients, c)
	c.startLocked(g, g.disconnected, g.rpcContext, g.server.stopper, breaker)
}

// removeClientLocked removes the specified client. Called when a client
// disconnects.
func (g *Gossip) removeClientLocked(target *client) {
	g.clientsMu.Lock()
	defer g.clientsMu.Unlock()
	for i, candidate := range g.clientsMu.clients {
		if candidate == target {
			ctx := g.AnnotateCtx(context.TODO())
			log.Eventf(ctx, "client %s disconnected", candidate.addr)
			g.clientsMu.clients = append(g.clientsMu.clients[:i], g.clientsMu.clients[i+1:]...)
			delete(g.bootstrapping, candidate.addr.String())
			g.outgoing.removeNode(candidate.peerID)
			break
		}
	}
}

func (g *Gossip) findClient(match func(*client) bool) *client {
	g.clientsMu.Lock()
	defer g.clientsMu.Unlock()
	for _, c := range g.clientsMu.clients {
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

// Metrics contains gossip metrics used per node and server.
type Metrics struct {
	ConnectionsRefused *metric.Counter
	BytesReceived      *metric.Counter
	BytesSent          *metric.Counter
	InfosReceived      *metric.Counter
	InfosSent          *metric.Counter
}

func (m Metrics) String() string {
	return fmt.Sprintf("infos %d/%d sent/received, bytes %dB/%dB sent/received",
		m.InfosSent.Count(), m.InfosReceived.Count(), m.BytesSent.Count(), m.BytesReceived.Count())
}

func makeMetrics() Metrics {
	return Metrics{
		ConnectionsRefused: metric.NewCounter(MetaConnectionsRefused),
		BytesReceived:      metric.NewCounter(MetaBytesReceived),
		BytesSent:          metric.NewCounter(MetaBytesSent),
		InfosReceived:      metric.NewCounter(MetaInfosReceived),
		InfosSent:          metric.NewCounter(MetaInfosSent),
	}
}
