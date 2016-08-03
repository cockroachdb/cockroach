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
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/timeutil"
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

	// DefaultGossipStoresInterval is the default interval for gossiping storage-
	// related info.
	DefaultGossipStoresInterval = 1 * time.Minute
)

// Gossip metrics counter names.
const (
	ConnectionsIncomingGaugeName = "connections.incoming"
	ConnectionsOutgoingGaugeName = "connections.outgoing"
	InfosSentRatesName           = "infos.sent"
	InfosReceivedRatesName       = "infos.received"
	BytesSentRatesName           = "bytes.sent"
	BytesReceivedRatesName       = "bytes.received"
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
	Connected     chan struct{}       // Closed upon initial connection
	hasConnected  bool                // Set first time network is connected
	rpcContext    *rpc.Context        // The context required for RPC
	*server                           // Embedded gossip RPC server
	outgoing      nodeSet             // Set of outgoing client node IDs
	storage       Storage             // Persistent storage interface
	bootstrapInfo BootstrapInfo       // BootstrapInfo proto for persistent storage
	bootstrapping map[string]struct{} // Set of active bootstrap clients
	needBSCleanup bool                // Set if there are invalid bootstrap addresses

	// Note that access to each client's internal state is serialized by the
	// embedded server's mutex. This is surprising!
	clientsMu struct {
		syncutil.Mutex
		clients []*client
	}

	disconnected chan *client  // Channel of disconnected clients
	stalled      bool          // True if gossip is stalled (i.e. host doesn't have sentinel)
	stalledCh    chan struct{} // Channel to wakeup stalled bootstrap

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
	resolverAddrs  map[util.UnresolvedAddr]resolver.Resolver
	bootstrapAddrs map[util.UnresolvedAddr]struct{}
}

// New creates an instance of a gossip node.
func New(rpcContext *rpc.Context, grpcServer *grpc.Server, resolvers []resolver.Resolver, stopper *stop.Stopper, registry *metric.Registry) *Gossip {
	g := &Gossip{
		Connected:         make(chan struct{}),
		rpcContext:        rpcContext,
		server:            newServer(stopper, registry),
		outgoing:          makeNodeSet(minPeers, registry.Gauge(ConnectionsOutgoingGaugeName)),
		bootstrapping:     map[string]struct{}{},
		disconnected:      make(chan *client, 10),
		stalledCh:         make(chan struct{}, 1),
		stallInterval:     defaultStallInterval,
		bootstrapInterval: defaultBootstrapInterval,
		cullInterval:      defaultCullInterval,
		nodeDescs:         map[roachpb.NodeID]*roachpb.NodeDescriptor{},
		resolverAddrs:     map[util.UnresolvedAddr]resolver.Resolver{},
		bootstrapAddrs:    map[util.UnresolvedAddr]struct{}{},
	}
	g.SetResolvers(resolvers)

	// Add ourselves as a SystemConfig watcher.
	g.is.registerCallback(KeySystemConfig, g.updateSystemConfig)
	// Add ourselves as a node descriptor watcher.
	g.is.registerCallback(MakePrefixPattern(KeyNodeIDPrefix), g.updateNodeAddress)

	RegisterGossipServer(grpcServer, g.server)

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
		panic(fmt.Sprintf("different node IDs were set for the same gossip instance (%d, %d)", g.is.NodeID, nodeID))
	}
	g.is.NodeID = nodeID
}

// ResetNodeID resets the infostore's node ID.
// NOTE: use only from unittests.
func (g *Gossip) ResetNodeID(nodeID roachpb.NodeID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.is.NodeID = nodeID
}

// SetNodeDescriptor adds the node descriptor to the gossip network
// and sets the infostore's node ID.
func (g *Gossip) SetNodeDescriptor(desc *roachpb.NodeDescriptor) error {
	if err := g.AddInfoProto(MakeNodeIDKey(desc.NodeID), desc, ttlNodeDescriptorGossip); err != nil {
		return errors.Errorf("couldn't gossip descriptor for node %d: %v", desc.NodeID, err)
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
	// Maintain lock ordering.
	var storedBI BootstrapInfo
	if err := storage.ReadBootstrapInfo(&storedBI); err != nil {
		log.Warningf(context.TODO(), "failed to read gossip bootstrap info: %s", err)
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
		if _, ok := existing[makeKey(addr)]; !ok && addr != g.is.NodeAddr {
			g.maybeAddBootstrapAddress(addr)
		}
	}
	// Persist merged addresses.
	if numAddrs := len(g.bootstrapInfo.Addresses); numAddrs > len(storedBI.Addresses) {
		if err := g.storage.WriteBootstrapInfo(&g.bootstrapInfo); err != nil {
			log.Error(context.TODO(), err)
		}
	}

	// Cycle through all persisted bootstrap hosts and add resolvers for
	// any which haven't already been added.
	newResolverFound := false
	for _, addr := range g.bootstrapInfo.Addresses {
		if !g.maybeAddResolver(addr) {
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
			log.Infof(context.TODO(), "found new resolvers from storage; signalling bootstrap")
		}
		g.signalStalledLocked()
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
	g.resolversTried = map[int]struct{}{}
	// Start new bootstrapping immediately instead of waiting for next bootstrap interval.
	g.maybeSignalStalledLocked()
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
	if g.is.getInfo(KeySentinel) == nil {
		status = "stalled"
	}
	g.mu.Unlock()

	log.Infof(context.TODO(), "gossip status (%s, %d node%s)\n%s%s",
		status, n, util.Pluralize(int64(n)),
		g.clientStatus(), g.server.status())
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
	g.simulationCycler.Broadcast()
}

// maybeAddResolver creates and adds a resolver for the specified
// address if one does not already exist. Returns whether a new
// resolver was added. The caller must hold the gossip mutex.
func (g *Gossip) maybeAddResolver(addr util.UnresolvedAddr) bool {
	if _, ok := g.resolverAddrs[addr]; !ok {
		r, err := resolver.NewResolverFromUnresolvedAddr(addr)
		if err != nil {
			log.Warningf(context.TODO(), "bad address %s: %s", addr, err)
			return false
		}
		g.resolvers = append(g.resolvers, r)
		g.resolverAddrs[addr] = r
		return true
	}
	return false
}

// maybeAddBootstrapAddress adds the specified address to the list of
// bootstrap addresses if not already present. Returns whether a new
// bootstrap address was added. The caller must hold the gossip mutex.
func (g *Gossip) maybeAddBootstrapAddress(addr util.UnresolvedAddr) bool {
	if _, ok := g.bootstrapAddrs[addr]; !ok {
		g.bootstrapInfo.Addresses = append(g.bootstrapInfo.Addresses, addr)
		g.bootstrapAddrs[addr] = struct{}{}
		return true
	}
	return false
}

// maybeCleanupBootstrapAddresses removes any addresses from the
// bootstrap info which fail to resolve successfully.
func (g *Gossip) maybeCleanupBootstrapAddresses(ctx context.Context) {
	if !g.needBSCleanup || g.storage == nil {
		return
	}
	var newAddrs []util.UnresolvedAddr
	for _, addr := range g.bootstrapInfo.Addresses {
		if r, ok := g.resolverAddrs[addr]; ok {
			if _, err := r.GetAddress(); err == nil {
				newAddrs = append(newAddrs, addr)
			} else {
				log.Infof(ctx, "purging invalid bootstrap address %s", addr)
			}
		}
	}
	if len(newAddrs) != len(g.bootstrapInfo.Addresses) {
		g.bootstrapInfo.Addresses = newAddrs
		if err := g.storage.WriteBootstrapInfo(&g.bootstrapInfo); err != nil {
			log.Error(ctx, err)
		}
	}
	g.needBSCleanup = false
}

// maxPeers returns the maximum number of peers each gossip node
// may connect to. This is based on maxHops, which is a preset
// maximum for number of hops allowed before the gossip network
// will seek to "tighten" by creating new connections to distant
// nodes.
func (g *Gossip) maxPeers(nodeCount int) int {
	// This formula uses MaxHops-1, instead of MaxHops, to provide a
	// "fudge" factor for max connected peers, to account for the
	// arbitrary, decentralized way in which gossip networks are created.
	maxPeers := int(math.Ceil(math.Exp(math.Log(float64(nodeCount)) / float64(MaxHops-1))))
	if maxPeers < minPeers {
		return minPeers
	}
	return maxPeers
}

// updateNodeAddress is a gossip callback which fires with each
// update to the node address. This allows us to compute the
// total size of the gossip network (for determining max peers
// each gossip node is allowed to have), as well as to create
// new resolvers for each encountered host and to write the
// set of gossip node addresses to persistent storage when it
// changes.
func (g *Gossip) updateNodeAddress(_ string, content roachpb.Value) {
	var desc roachpb.NodeDescriptor
	if err := content.GetProto(&desc); err != nil {
		log.Error(context.TODO(), err)
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Skip if the node has already been seen.
	if _, ok := g.nodeDescs[desc.NodeID]; ok {
		return
	}

	g.nodeDescs[desc.NodeID] = &desc

	// Recompute max peers based on size of network and set the max
	// sizes for incoming and outgoing node sets.
	maxPeers := g.maxPeers(len(g.nodeDescs))
	g.incoming.setMaxSize(maxPeers)
	g.outgoing.setMaxSize(maxPeers)

	// Skip if it's our own address.
	if desc.Address == g.is.NodeAddr {
		return
	}

	// Add this new node address (if it's not already there) to our list
	// of resolvers so we can keep connecting to gossip if the original
	// resolvers go offline.
	g.maybeAddResolver(desc.Address)

	// Add new address (if it's not already there) to bootstrap info and
	// persist if possible.
	if g.storage != nil && g.maybeAddBootstrapAddress(desc.Address) {
		if err := g.storage.WriteBootstrapInfo(&g.bootstrapInfo); err != nil {
			log.Error(context.TODO(), err)
		}
	}
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

	return nil, errors.Errorf("unable to lookup descriptor for node %d", nodeID)
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
	err := g.is.addInfo(key, g.is.newInfo(val, ttl))
	if err == nil {
		g.checkHasConnected()
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
	i := g.is.getInfo(key)
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
	for k, v := range g.is.Infos {
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
		log.Warningf(context.TODO(), "raw gossip callback registered on %s, consider using RegisterSystemConfigChannel",
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
	if key != KeySystemConfig {
		log.Fatalf(context.TODO(), "wrong key received on SystemConfig callback: %s", key)
		return
	}
	cfg := config.SystemConfig{}
	if err := content.GetProto(&cfg); err != nil {
		log.Errorf(context.TODO(), "could not unmarshal system config on callback: %s", err)
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

// MaxHops returns the maximum number of hops to reach any other
// node in the system, according to the infos which have reached
// this node via gossip network.
func (g *Gossip) MaxHops() uint32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, maxHops := g.is.mostDistant()
	return maxHops
}

// Start launches the gossip instance, which commences joining the
// gossip network using the supplied rpc server and previously known
// peer addresses in addition to any bootstrap addresses specified via
// --join.
//
// The supplied address is used to identify the gossip instance in the
// gossip network; it will be used by other instances to connect to
// this instance.
//
// This method starts bootstrap loop, gossip server, and client
// management in separate goroutines and returns.
func (g *Gossip) Start(addr net.Addr) {
	g.server.start(addr) // serve gossip protocol
	g.bootstrap()        // bootstrap gossip client
	g.manage()           // manage gossip clients
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

// getNextBootstrapAddress returns the next available bootstrap
// address by consulting the first non-exhausted resolver from the
// slice supplied to the constructor or set using setBootstrap().
// The lock is assumed held.
func (g *Gossip) getNextBootstrapAddress() net.Addr {
	needBSCleanup := false
	defer func() {
		g.needBSCleanup = needBSCleanup
	}()

	// Run through resolvers round robin starting at last resolved index.
	for i := 0; i < len(g.resolvers); i++ {
		g.resolverIdx++
		g.resolverIdx %= len(g.resolvers)
		g.resolversTried[g.resolverIdx] = struct{}{}
		resolver := g.resolvers[g.resolverIdx]
		if addr, err := resolver.GetAddress(); err != nil {
			// Resolver has an invalid address. Set needBSCleanup to purge invalid
			// bootstrap addresses once gossip cluster is joined successfully.
			needBSCleanup = true
			if !g.needBSCleanup {
				log.Warningf(context.TODO(), "invalid bootstrap address: %+v, %v", resolver, err)
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
	g.server.stopper.RunWorker(func() {
		var bootstrapTimer timeutil.Timer
		defer bootstrapTimer.Stop()
		for {
			if g.server.stopper.RunTask(func() {
				g.mu.Lock()
				defer g.mu.Unlock()
				haveClients := g.outgoing.len() > 0
				haveSentinel := g.is.getInfo(KeySentinel) != nil
				if !haveClients || !haveSentinel {
					// Try to get another bootstrap address from the resolvers.
					if addr := g.getNextBootstrapAddress(); addr != nil {
						g.startClient(addr)
					} else {
						// We couldn't start a client, signal that we're stalled so that
						// we'll retry.
						g.maybeSignalStalledLocked()
					}
				}
			}) != nil {
				return
			}

			// Pause an interval before next possible bootstrap.
			bootstrapTimer.Reset(g.bootstrapInterval)
			select {
			case <-bootstrapTimer.C:
				bootstrapTimer.Read = true
				// break
			case <-g.server.stopper.ShouldStop():
				return
			}
			// Block until we need bootstrapping again.
			select {
			case <-g.stalledCh:
				// break
			case <-g.server.stopper.ShouldStop():
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
func (g *Gossip) manage() {
	g.server.stopper.RunWorker(func() {
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
			case nodeID := <-g.tighten:
				g.tightenNetwork(nodeID)
			case <-cullTicker.C:
				func() {
					g.mu.Lock()
					if !g.outgoing.hasSpace() {
						leastUsefulID := g.is.leastUseful(g.outgoing)

						if c := g.findClient(func(c *client) bool {
							return c.peerID == leastUsefulID
						}); c != nil {
							if log.V(1) {
								log.Infof(context.TODO(), "closing least useful client %+v to tighten network graph", c)
							}
							c.close()

							// After releasing the lock, block until the client disconnects.
							defer func() {
								g.doDisconnected(<-g.disconnected)
							}()
						} else {
							if log.V(1) {
								g.clientsMu.Lock()
								log.Infof(context.TODO(), "couldn't find least useful client among %+v", g.clientsMu.clients)
								g.clientsMu.Unlock()
							}
						}
					}
					g.mu.Unlock()
				}()
			case <-stallTicker.C:
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

// tightenNetwork "tightens" the network by starting a new gossip
// client to the most distant node as measured in required gossip hops
// to propagate info from the distant node to this node.
func (g *Gossip) tightenNetwork(distantNodeID roachpb.NodeID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.outgoing.hasSpace() {
		if nodeAddr, err := g.getNodeIDAddressLocked(distantNodeID); err != nil {
			log.Errorf(context.TODO(), "node %d: %s", distantNodeID, err)
		} else {
			log.Infof(context.TODO(), "starting client to distant node %d to tighten network graph", distantNodeID)
			g.startClient(nodeAddr)
		}
	}
}

func (g *Gossip) doDisconnected(c *client) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.removeClient(c)

	// If the client was disconnected with a forwarding address, connect now.
	if c.forwardAddr != nil {
		g.startClient(c.forwardAddr)
	}
	g.maybeSignalStalledLocked()
}

// If the sentinel gossip is missing, log and signal bootstrapper to
// try another resolver.
func (g *Gossip) maybeSignalStalledLocked() {
	ctx := context.TODO()
	if g.is.getInfo(KeySentinel) != nil && g.outgoing.len()+g.incoming.len() > 0 {
		g.maybeCleanupBootstrapAddresses(ctx)
		if g.stalled {
			log.Infof(ctx, "node has connected to cluster via gossip")
			g.stalled = false
		}
		return
	}
	// We employ the stalled boolean to avoid filling logs with warnings.
	if !g.stalled {
		g.stalled = true
		g.warnAboutStall()
	}
	g.signalStalledLocked()
}

func (g *Gossip) signalStalledLocked() {
	if len(g.resolvers) > 0 {
		select {
		case g.stalledCh <- struct{}{}:
		default:
		}
	}
}

// warnAboutStall attempts to diagnose the cause of a gossip network
// not being connected to the sentinel. This could happen in a network
// partition, or because of misconfiguration. It's impossible to tell,
// but we can warn appropriately. If there are no incoming or outgoing
// connections, we warn about the --join flag being set. If we've
// connected, and all resolvers have been tried, we warn about either
// the first range not being available or else possible the cluster
// never having been initialized.
func (g *Gossip) warnAboutStall() {
	if g.outgoing.len()+g.incoming.len() == 0 {
		log.Warningf(context.TODO(), "not connected to cluster; use --join to specify a connected node")
	} else if len(g.resolversTried) == len(g.resolvers) {
		log.Warningf(context.TODO(), "first range unavailable or cluster not initialized")
	} else {
		log.Warningf(context.TODO(), "partition in gossip network; attempting new connection")
	}
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
func (g *Gossip) startClient(addr net.Addr) {
	c := newClient(addr, g.serverMetrics)
	g.clientsMu.Lock()
	g.clientsMu.clients = append(g.clientsMu.clients, c)
	g.clientsMu.Unlock()
	c.start(g, g.disconnected, g.rpcContext, g.server.stopper)
}

// removeClient removes the specified client. Called when a client
// disconnects.
func (g *Gossip) removeClient(target *client) {
	g.clientsMu.Lock()
	defer g.clientsMu.Unlock()
	for i, candidate := range g.clientsMu.clients {
		if candidate == target {
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

type metrics struct {
	bytesReceived metric.Rates
	bytesSent     metric.Rates
	infosReceived metric.Rates
	infosSent     metric.Rates
}

func (m metrics) String() string {
	return fmt.Sprintf("infos %d/%d sent/received, bytes %dB/%dB sent/received",
		m.infosSent.Count(), m.infosReceived.Count(), m.bytesSent.Count(), m.bytesReceived.Count())
}

// makeMetrics makes a new metrics object with rates set on the provided
// registry.
func makeMetrics(registry *metric.Registry) metrics {
	return metrics{
		bytesReceived: registry.Rates(BytesReceivedRatesName),
		bytesSent:     registry.Rates(BytesSentRatesName),
		infosReceived: registry.Rates(InfosReceivedRatesName),
		infosSent:     registry.Rates(InfosSentRatesName),
	}
}
