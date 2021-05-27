// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc"
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

	// defaultStallInterval is the default interval for checking whether
	// the incoming and outgoing connections to the gossip network are
	// insufficient to keep the network connected.
	defaultStallInterval = 2 * time.Second

	// defaultBootstrapInterval is the minimum time between successive
	// bootstrapping attempts to avoid busy-looping trying to find the
	// sentinel gossip info.
	defaultBootstrapInterval = 1 * time.Second

	// defaultCullInterval is the default interval for culling the least
	// "useful" outgoing gossip connection to free up space for a more
	// efficiently targeted connection to the most distant node.
	defaultCullInterval = 60 * time.Second

	// defaultClientsInterval is the default interval for updating the gossip
	// clients key which allows every node in the cluster to create a map of
	// gossip connectivity. This value is intentionally small as we want to
	// detect gossip partitions faster that the node liveness timeout (9s).
	defaultClientsInterval = 2 * time.Second

	// NodeDescriptorInterval is the interval for gossiping the node descriptor.
	// Note that increasing this duration may increase the likelihood of gossip
	// thrashing, since node descriptors are used to determine the number of gossip
	// hops between nodes (see #9819 for context).
	NodeDescriptorInterval = 1 * time.Hour

	// NodeDescriptorTTL is time-to-live for node ID -> descriptor.
	NodeDescriptorTTL = 2 * NodeDescriptorInterval

	// StoresInterval is the default interval for gossiping store descriptors.
	StoresInterval = 60 * time.Second

	// StoreTTL is time-to-live for store-related info.
	StoreTTL = 2 * StoresInterval

	unknownNodeID roachpb.NodeID = 0
)

// Gossip metrics counter names.
var (
	MetaConnectionsIncomingGauge = metric.Metadata{
		Name:        "gossip.connections.incoming",
		Help:        "Number of active incoming gossip connections",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaConnectionsOutgoingGauge = metric.Metadata{
		Name:        "gossip.connections.outgoing",
		Help:        "Number of active outgoing gossip connections",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaConnectionsRefused = metric.Metadata{
		Name:        "gossip.connections.refused",
		Help:        "Number of refused incoming gossip connections",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaInfosSent = metric.Metadata{
		Name:        "gossip.infos.sent",
		Help:        "Number of sent gossip Info objects",
		Measurement: "Infos",
		Unit:        metric.Unit_COUNT,
	}
	MetaInfosReceived = metric.Metadata{
		Name:        "gossip.infos.received",
		Help:        "Number of received gossip Info objects",
		Measurement: "Infos",
		Unit:        metric.Unit_COUNT,
	}
	MetaBytesSent = metric.Metadata{
		Name:        "gossip.bytes.sent",
		Help:        "Number of sent gossip bytes",
		Measurement: "Gossip Bytes",
		Unit:        metric.Unit_BYTES,
	}
	MetaBytesReceived = metric.Metadata{
		Name:        "gossip.bytes.received",
		Help:        "Number of received gossip bytes",
		Measurement: "Gossip Bytes",
		Unit:        metric.Unit_BYTES,
	}
)

// KeyNotPresentError is returned by gossip when queried for a key that doesn't
// exist or has expired.
type KeyNotPresentError struct {
	key string
}

// Error implements the error interface.
func (err KeyNotPresentError) Error() string {
	return fmt.Sprintf("KeyNotPresentError: gossip key %q does not exist or has expired", err.key)
}

// NewKeyNotPresentError creates a new KeyNotPresentError.
func NewKeyNotPresentError(key string) error {
	return KeyNotPresentError{key: key}
}

// AddressResolver is a thin wrapper around gossip's GetNodeIDAddress
// that allows it to be used as a nodedialer.AddressResolver.
func AddressResolver(gossip *Gossip) nodedialer.AddressResolver {
	return func(nodeID roachpb.NodeID) (net.Addr, error) {
		return gossip.GetNodeIDAddress(nodeID)
	}
}

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
	started bool // for assertions

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
	// It is used so often that we keep an unmarshaled version of it
	// here and its own set of callbacks.
	// We do not use the infostore to avoid unmarshalling under the
	// main gossip lock.
	systemConfig         *config.SystemConfig
	systemConfigMu       syncutil.RWMutex
	systemConfigChannels []chan<- struct{}

	// resolvers is a list of resolvers used to determine
	// bootstrap hosts for connecting to the gossip network.
	resolverIdx    int
	resolvers      []resolver.Resolver
	resolversTried map[int]struct{} // Set of attempted resolver indexes
	nodeDescs      map[roachpb.NodeID]*roachpb.NodeDescriptor
	// storeMap maps store IDs to node IDs.
	storeMap map[roachpb.StoreID]roachpb.NodeID

	// Membership sets for resolvers and bootstrap addresses.
	// bootstrapAddrs also tracks which address is associated with which
	// node ID to enable faster node lookup by address.
	resolverAddrs  map[util.UnresolvedAddr]resolver.Resolver
	bootstrapAddrs map[util.UnresolvedAddr]roachpb.NodeID

	locality roachpb.Locality

	lastConnectivity redact.RedactableString

	defaultZoneConfig *zonepb.ZoneConfig
}

// New creates an instance of a gossip node.
// The higher level manages the ClusterIDContainer and NodeIDContainer instances
// (which can be shared by various server components). The ambient context is
// expected to already contain the node ID.
//
// grpcServer: The server on which the new Gossip instance will register its RPC
//   service. Can be nil, in which case the Gossip will not register the
//   service.
// rpcContext: The context used to connect to other nodes. Can be nil for tests
//   that also specify a nil grpcServer and that plan on using the Gossip in a
//   restricted way by populating it with data manually.
func New(
	ambient log.AmbientContext,
	clusterID *base.ClusterIDContainer,
	nodeID *base.NodeIDContainer,
	rpcContext *rpc.Context,
	grpcServer *grpc.Server,
	stopper *stop.Stopper,
	registry *metric.Registry,
	locality roachpb.Locality,
	defaultZoneConfig *zonepb.ZoneConfig,
) *Gossip {
	ambient.SetEventLog("gossip", "gossip")
	g := &Gossip{
		server:            newServer(ambient, clusterID, nodeID, stopper, registry),
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
		storeMap:          make(map[roachpb.StoreID]roachpb.NodeID),
		resolverAddrs:     map[util.UnresolvedAddr]resolver.Resolver{},
		bootstrapAddrs:    map[util.UnresolvedAddr]roachpb.NodeID{},
		locality:          locality,
		defaultZoneConfig: defaultZoneConfig,
	}

	stopper.AddCloser(stop.CloserFn(g.server.AmbientContext.FinishEventLog))

	registry.AddMetric(g.outgoing.gauge)
	g.clientsMu.breakers = map[string]*circuit.Breaker{}

	g.mu.Lock()
	// Add ourselves as a SystemConfig watcher.
	g.mu.is.registerCallback(KeySystemConfig, g.updateSystemConfig)
	// Add ourselves as a node descriptor watcher.
	g.mu.is.registerCallback(MakePrefixPattern(KeyNodeIDPrefix), g.updateNodeAddress)
	g.mu.is.registerCallback(MakePrefixPattern(KeyStorePrefix), g.updateStoreMap)
	// Log gossip connectivity whenever we receive an update.
	g.mu.Unlock()

	if grpcServer != nil {
		RegisterGossipServer(grpcServer, g.server)
	}
	return g
}

// NewTest is a simplified wrapper around New that creates the
// ClusterIDContainer and NodeIDContainer internally. Used for testing.
//
// grpcServer: The server on which the new Gossip instance will register its RPC
//   service. Can be nil, in which case the Gossip will not register the
//   service.
// rpcContext: The context used to connect to other nodes. Can be nil for tests
//   that also specify a nil grpcServer and that plan on using the Gossip in a
//   restricted way by populating it with data manually.
func NewTest(
	nodeID roachpb.NodeID,
	rpcContext *rpc.Context,
	grpcServer *grpc.Server,
	stopper *stop.Stopper,
	registry *metric.Registry,
	defaultZoneConfig *zonepb.ZoneConfig,
) *Gossip {
	return NewTestWithLocality(nodeID, rpcContext, grpcServer, stopper, registry, roachpb.Locality{}, defaultZoneConfig)
}

// NewTestWithLocality calls NewTest with an explicit locality value.
func NewTestWithLocality(
	nodeID roachpb.NodeID,
	rpcContext *rpc.Context,
	grpcServer *grpc.Server,
	stopper *stop.Stopper,
	registry *metric.Registry,
	locality roachpb.Locality,
	defaultZoneConfig *zonepb.ZoneConfig,
) *Gossip {
	c := &base.ClusterIDContainer{}
	n := &base.NodeIDContainer{}
	var ac log.AmbientContext
	ac.AddLogTag("n", n)
	gossip := New(ac, c, n, rpcContext, grpcServer, stopper, registry, locality, defaultZoneConfig)
	if nodeID != 0 {
		n.Set(context.TODO(), nodeID)
	}
	return gossip
}

// AssertNotStarted fatals if the Gossip instance was already started.
func (g *Gossip) AssertNotStarted(ctx context.Context) {
	if g.started {
		log.Fatalf(ctx, "gossip instance was already started")
	}
}

// GetNodeMetrics returns the gossip node metrics.
func (g *Gossip) GetNodeMetrics() *Metrics {
	return g.server.GetNodeMetrics()
}

// SetNodeDescriptor adds the node descriptor to the gossip network.
func (g *Gossip) SetNodeDescriptor(desc *roachpb.NodeDescriptor) error {
	ctx := g.AnnotateCtx(context.TODO())
	log.Infof(ctx, "NodeDescriptor set to %+v", desc)
	if desc.Address.IsEmpty() {
		log.Fatalf(ctx, "n%d address is empty", desc.NodeID)
	}
	if err := g.AddInfoProto(MakeNodeIDKey(desc.NodeID), desc, NodeDescriptorTTL); err != nil {
		return errors.Errorf("n%d: couldn't gossip descriptor: %v", desc.NodeID, err)
	}
	g.updateClients()
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
		log.Ops.Warningf(ctx, "failed to read gossip bootstrap info: %s", err)
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
			log.Errorf(ctx, "%v", err)
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
			log.Ops.Infof(ctx, "found new resolvers from storage; signaling bootstrap")
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
	g.mu.RLock()
	defer g.mu.RUnlock()
	return append([]resolver.Resolver(nil), g.resolvers...)
}

// GetNodeIDAddress looks up the RPC address of the node by ID.
func (g *Gossip) GetNodeIDAddress(nodeID roachpb.NodeID) (*util.UnresolvedAddr, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.getNodeIDAddressLocked(nodeID)
}

// GetNodeIDSQLAddress looks up the SQL address of the node by ID.
func (g *Gossip) GetNodeIDSQLAddress(nodeID roachpb.NodeID) (*util.UnresolvedAddr, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.getNodeIDSQLAddressLocked(nodeID)
}

// GetNodeDescriptor looks up the descriptor of the node by ID.
func (g *Gossip) GetNodeDescriptor(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.getNodeDescriptorLocked(nodeID)
}

// LogStatus logs the current status of gossip such as the incoming and
// outgoing connections.
func (g *Gossip) LogStatus() {
	g.mu.RLock()
	n := len(g.nodeDescs)
	status := redact.SafeString("ok")
	if g.mu.is.getInfo(KeySentinel) == nil {
		status = redact.SafeString("stalled")
	}
	g.mu.RUnlock()

	var connectivity redact.RedactableString
	if s := redact.Sprint(g.Connectivity()); s != g.lastConnectivity {
		g.lastConnectivity = s
		connectivity = s
	}

	ctx := g.AnnotateCtx(context.TODO())
	log.Health.Infof(ctx, "gossip status (%s, %d node%s)\n%s%s%s",
		status, n, util.Pluralize(int64(n)),
		g.clientStatus(), g.server.status(),
		connectivity)
}

func (g *Gossip) clientStatus() ClientStatus {
	g.mu.RLock()
	defer g.mu.RUnlock()
	g.clientsMu.Lock()
	defer g.clientsMu.Unlock()

	var status ClientStatus

	status.MaxConns = int32(g.outgoing.maxSize)
	status.ConnStatus = make([]OutgoingConnStatus, 0, len(g.clientsMu.clients))
	for _, c := range g.clientsMu.clients {
		status.ConnStatus = append(status.ConnStatus, OutgoingConnStatus{
			ConnStatus: ConnStatus{
				NodeID:   c.peerID,
				Address:  c.addr.String(),
				AgeNanos: timeutil.Since(c.createdAt).Nanoseconds(),
			},
			MetricSnap: c.clientMetrics.Snapshot(),
		})
	}
	return status
}

// Connectivity returns the current view of the gossip network as seen by this
// node.
func (g *Gossip) Connectivity() Connectivity {
	ctx := g.AnnotateCtx(context.TODO())
	var c Connectivity

	g.mu.RLock()

	if i := g.mu.is.getInfo(KeySentinel); i != nil {
		c.SentinelNodeID = i.NodeID
	}

	for nodeID := range g.nodeDescs {
		i := g.mu.is.getInfo(MakeGossipClientsKey(nodeID))
		if i == nil {
			continue
		}

		v, err := i.Value.GetBytes()
		if err != nil {
			log.Errorf(ctx, "unable to retrieve gossip value for %s: %v",
				MakeGossipClientsKey(nodeID), err)
			continue
		}
		if len(v) == 0 {
			continue
		}

		for _, part := range strings.Split(string(v), ",") {
			id, err := strconv.ParseInt(part, 10 /* base */, 64 /* bitSize */)
			if err != nil {
				log.Errorf(ctx, "unable to parse node ID: %v", err)
			}
			c.ClientConns = append(c.ClientConns, Connectivity_Conn{
				SourceID: nodeID,
				TargetID: roachpb.NodeID(id),
			})
		}
	}

	g.mu.RUnlock()

	sort.Slice(c.ClientConns, func(i, j int) bool {
		a, b := &c.ClientConns[i], &c.ClientConns[j]
		if a.SourceID < b.SourceID {
			return true
		}
		if a.SourceID > b.SourceID {
			return false
		}
		return a.TargetID < b.TargetID
	})

	return c
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
		log.Ops.Warningf(ctx, "bad address %s: %s", addr, err)
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
	}, true /* deleteExpired */); err != nil {
		log.Errorf(ctx, "%v", err)
		return
	}

	if err := g.storage.WriteBootstrapInfo(&g.bootstrapInfo); err != nil {
		log.Errorf(ctx, "%v", err)
	}
}

// maxPeers returns the maximum number of peers each gossip node
// may connect to. This is based on maxHops, which is a preset
// maximum for number of hops allowed before the gossip network
// will seek to "tighten" by creating new connections to distant
// nodes.
func maxPeers(nodeCount int) int {
	// This formula uses maxHops-2, instead of maxHops, to provide a
	// "fudge" factor for max connected peers, to account for the
	// arbitrary, decentralized way in which gossip networks are created.
	// This will return the following maxPeers for the given number of nodes:
	//	 <= 27 nodes -> 3 peers
	//   <= 64 nodes -> 4 peers
	//   <= 125 nodes -> 5 peers
	//   <= n^3 nodes -> n peers
	//
	// Quick derivation of the formula for posterity (without the fudge factor):
	// maxPeers^maxHops > nodeCount
	// maxHops * log(maxPeers) > log(nodeCount)
	// log(maxPeers) > log(nodeCount) / maxHops
	// maxPeers > e^(log(nodeCount) / maxHops)
	// hence maxPeers = ceil(e^(log(nodeCount) / maxHops)) should work
	maxPeers := int(math.Ceil(math.Exp(math.Log(float64(nodeCount)) / float64(maxHops-2))))
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
		log.Errorf(ctx, "%v", err)
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
	if desc.NodeID == 0 || desc.Address.IsEmpty() {
		nodeID, err := NodeIDFromKey(key, KeyNodeIDPrefix)
		if err != nil {
			log.Health.Errorf(ctx, "unable to update node address for removed node: %s", err)
			return
		}
		log.Health.Infof(ctx, "removed n%d from gossip", nodeID)
		g.removeNodeDescriptorLocked(nodeID)
		return
	}

	existingDesc, ok := g.nodeDescs[desc.NodeID]
	if !ok || !existingDesc.Equal(&desc) {
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

	// Add new address (if it's not already there) to bootstrap info and
	// persist if possible.
	added := g.maybeAddBootstrapAddressLocked(desc.Address, desc.NodeID)
	if added && g.storage != nil {
		if err := g.storage.WriteBootstrapInfo(&g.bootstrapInfo); err != nil {
			log.Errorf(ctx, "%v", err)
		}
	}
}

func (g *Gossip) removeNodeDescriptorLocked(nodeID roachpb.NodeID) {
	delete(g.nodeDescs, nodeID)
	g.recomputeMaxPeersLocked()
}

// updateStoreMaps is a gossip callback which is used to update storeMap.
func (g *Gossip) updateStoreMap(key string, content roachpb.Value) {
	ctx := g.AnnotateCtx(context.TODO())
	var desc roachpb.StoreDescriptor
	if err := content.GetProto(&desc); err != nil {
		log.Errorf(ctx, "%v", err)
		return
	}

	if log.V(1) {
		log.Infof(ctx, "updateStoreMap called on %q with desc %+v", key, desc)
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	g.storeMap[desc.StoreID] = desc.Node.NodeID
}

func (g *Gossip) updateClients() {
	nodeID := g.NodeID.Get()
	if nodeID == 0 {
		return
	}

	var buf bytes.Buffer
	var sep string

	g.mu.RLock()
	g.clientsMu.Lock()
	for _, c := range g.clientsMu.clients {
		if c.peerID != 0 {
			fmt.Fprintf(&buf, "%s%d", sep, c.peerID)
			sep = ","
		}
	}
	g.clientsMu.Unlock()
	g.mu.RUnlock()

	if err := g.AddInfo(MakeGossipClientsKey(nodeID), buf.Bytes(), 2*defaultClientsInterval); err != nil {
		log.Errorf(g.AnnotateCtx(context.Background()), "%v", err)
	}
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
		if desc.Address.IsEmpty() {
			log.Fatalf(g.AnnotateCtx(context.Background()), "n%d has an empty address", nodeID)
		}
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
		if nodeDescriptor.NodeID == 0 || nodeDescriptor.Address.IsEmpty() {
			return nil, errors.Errorf("n%d has been removed from the cluster", nodeID)
		}

		return nodeDescriptor, nil
	}

	return nil, errors.Errorf("unable to look up descriptor for n%d", nodeID)
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
	return nd.AddressForLocality(g.locality), nil
}

// getNodeIDAddressLocked looks up the SQL address of the node by ID. The mutex
// is assumed held by the caller. This method is called externally via
// GetNodeIDSQLAddress.
func (g *Gossip) getNodeIDSQLAddressLocked(nodeID roachpb.NodeID) (*util.UnresolvedAddr, error) {
	nd, err := g.getNodeDescriptorLocked(nodeID)
	if err != nil {
		return nil, err
	}
	return &nd.SQLAddress, nil
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
func (g *Gossip) AddInfoProto(key string, msg protoutil.Message, ttl time.Duration) error {
	bytes, err := protoutil.Marshal(msg)
	if err != nil {
		return err
	}
	return g.AddInfo(key, bytes, ttl)
}

// AddClusterID is a convenience method for gossipping the cluster ID. There's
// no TTL - the record lives forever.
func (g *Gossip) AddClusterID(val uuid.UUID) error {
	return g.AddInfo(KeyClusterID, val.GetBytes(), 0 /* ttl */)
}

// GetClusterID returns the cluster ID if it has been gossipped. If it hasn't,
// (so if this gossip instance is not "connected"), an error is returned.
func (g *Gossip) GetClusterID() (uuid.UUID, error) {
	uuidBytes, err := g.GetInfo(KeyClusterID)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "unable to ascertain cluster ID from gossip network")
	}
	clusterID, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "unable to parse cluster ID from gossip network")
	}
	return clusterID, nil
}

// GetInfo returns an info value by key or an KeyNotPresentError if specified
// key does not exist or has expired.
func (g *Gossip) GetInfo(key string) ([]byte, error) {
	g.mu.RLock()
	i := g.mu.is.getInfo(key)
	g.mu.RUnlock()

	if i != nil {
		if err := i.Value.Verify([]byte(key)); err != nil {
			return nil, err
		}
		return i.Value.GetBytes()
	}
	return nil, NewKeyNotPresentError(key)
}

// GetInfoProto returns an info value by key or KeyNotPresentError if specified
// key does not exist or has expired.
func (g *Gossip) GetInfoProto(key string, msg protoutil.Message) error {
	bytes, err := g.GetInfo(key)
	if err != nil {
		return err
	}
	return protoutil.Unmarshal(bytes, msg)
}

// InfoOriginatedHere returns true iff the latest info for the provided key
// originated on this node. This is useful for ensuring that the system config
// is regossiped as soon as possible when its lease changes hands.
func (g *Gossip) InfoOriginatedHere(key string) bool {
	g.mu.RLock()
	info := g.mu.is.getInfo(key)
	g.mu.RUnlock()
	return info != nil && info.NodeID == g.NodeID.Get()
}

// GetInfoStatus returns the a copy of the contents of the infostore.
func (g *Gossip) GetInfoStatus() InfoStatus {
	clientStatus := g.clientStatus()
	serverStatus := g.server.status()
	connectivity := g.Connectivity()

	g.mu.RLock()
	defer g.mu.RUnlock()
	is := InfoStatus{
		Infos:        make(map[string]Info),
		Client:       clientStatus,
		Server:       serverStatus,
		Connectivity: connectivity,
	}
	for k, v := range g.mu.is.Infos {
		is.Infos[k] = *protoutil.Clone(v).(*Info)
	}
	return is
}

// IterateInfos visits all infos matching the given prefix.
func (g *Gossip) IterateInfos(prefix string, visit func(k string, info Info) error) error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for k, v := range g.mu.is.Infos {
		if strings.HasPrefix(k, prefix+separator) {
			if err := visit(k, *(protoutil.Clone(v).(*Info))); err != nil {
				return err
			}
		}
	}
	return nil
}

// Callback is a callback method to be invoked on gossip update
// of info denoted by key.
type Callback func(string, roachpb.Value)

// CallbackOption is a marker interface that callback options must implement.
type CallbackOption interface {
	apply(cb *callback)
}

type redundantCallbacks struct {
}

func (redundantCallbacks) apply(cb *callback) {
	cb.redundant = true
}

// Redundant is a callback option that specifies that the callback should be
// invoked even if the gossip value has not changed.
var Redundant redundantCallbacks

// RegisterCallback registers a callback for a key pattern to be
// invoked whenever new info for a gossip key matching pattern is
// received. The callback method is invoked with the info key which
// matched pattern. Returns a function to unregister the callback.
func (g *Gossip) RegisterCallback(pattern string, method Callback, opts ...CallbackOption) func() {
	g.mu.Lock()
	unregister := g.mu.is.registerCallback(pattern, method, opts...)
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		unregister()
		g.mu.Unlock()
	}
}

// GetSystemConfig returns the local unmarshaled version of the system config.
// Returns nil if the system config hasn't been set yet.
func (g *Gossip) GetSystemConfig() *config.SystemConfig {
	g.systemConfigMu.RLock()
	defer g.systemConfigMu.RUnlock()
	return g.systemConfig
}

// RegisterSystemConfigChannel registers a channel to signify updates for the
// system config. It is notified after registration (if a system config is
// already set), and whenever a new system config is successfully unmarshaled.
func (g *Gossip) RegisterSystemConfigChannel() <-chan struct{} {
	// Create channel that receives new system config notifications.
	// The channel has a size of 1 to prevent gossip from having to block on it.
	c := make(chan struct{}, 1)

	g.systemConfigMu.Lock()
	defer g.systemConfigMu.Unlock()
	g.systemConfigChannels = append(g.systemConfigChannels, c)

	// Notify the channel right away if we have a config.
	if g.systemConfig != nil {
		c <- struct{}{}
	}
	return c
}

// updateSystemConfig is the raw gossip info callback. Unmarshal the
// system config, and if successful, send on each system config
// channel.
func (g *Gossip) updateSystemConfig(key string, content roachpb.Value) {
	ctx := g.AnnotateCtx(context.TODO())
	if key != KeySystemConfig {
		log.Fatalf(ctx, "wrong key received on SystemConfig callback: %s", key)
	}
	cfg := config.NewSystemConfig(g.defaultZoneConfig)
	if err := content.GetProto(&cfg.SystemConfigEntries); err != nil {
		log.Errorf(ctx, "could not unmarshal system config on callback: %s", err)
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
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.mu.incoming.asSlice()
}

// Outgoing returns a slice of outgoing gossip client connection
// node IDs. Note that these outgoing client connections may not
// actually be legitimately connected. They may be in the process
// of trying, or may already have failed, but haven't yet been
// processed by the gossip instance.
func (g *Gossip) Outgoing() []roachpb.NodeID {
	g.mu.RLock()
	defer g.mu.RUnlock()
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
	g.AssertNotStarted(context.Background())
	g.started = true
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
		log.Errorf(ctx, "unable to get address for n%d: %s", nodeID, err)
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
				log.Ops.Warningf(ctx, "invalid bootstrap address: %+v, %v", resolver, err)
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
	_ = g.server.stopper.RunAsyncTask(ctx, "gossip-bootstrap", func(ctx context.Context) {
		ctx = logtags.AddTag(ctx, "bootstrap", nil)
		var bootstrapTimer timeutil.Timer
		defer bootstrapTimer.Stop()
		for {
			func(ctx context.Context) {
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
			}(ctx)

			// Pause an interval before next possible bootstrap.
			bootstrapTimer.Reset(g.bootstrapInterval)
			log.Eventf(ctx, "sleeping %s until bootstrap", g.bootstrapInterval)
			select {
			case <-bootstrapTimer.C:
				bootstrapTimer.Read = true
				// continue
			case <-g.server.stopper.ShouldQuiesce():
				return
			}
			log.Eventf(ctx, "idling until bootstrap required")
			// Block until we need bootstrapping again.
			select {
			case <-g.stalledCh:
				log.Eventf(ctx, "detected stall; commencing bootstrap")
				// continue
			case <-g.server.stopper.ShouldQuiesce():
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
	_ = g.server.stopper.RunAsyncTask(ctx, "gossip-manage", func(ctx context.Context) {
		clientsTimer := timeutil.NewTimer()
		cullTimer := timeutil.NewTimer()
		stallTimer := timeutil.NewTimer()
		defer clientsTimer.Stop()
		defer cullTimer.Stop()
		defer stallTimer.Stop()

		clientsTimer.Reset(defaultClientsInterval)
		cullTimer.Reset(jitteredInterval(g.cullInterval))
		stallTimer.Reset(jitteredInterval(g.stallInterval))
		for {
			select {
			case <-g.server.stopper.ShouldQuiesce():
				return
			case c := <-g.disconnected:
				g.doDisconnected(c)
			case <-g.tighten:
				g.tightenNetwork(ctx)
			case <-clientsTimer.C:
				clientsTimer.Read = true
				g.updateClients()
				clientsTimer.Reset(defaultClientsInterval)
			case <-cullTimer.C:
				cullTimer.Read = true
				cullTimer.Reset(jitteredInterval(g.cullInterval))
				func() {
					g.mu.Lock()
					if !g.outgoing.hasSpace() {
						leastUsefulID := g.mu.is.leastUseful(g.outgoing)

						if c := g.findClient(func(c *client) bool {
							return c.peerID == leastUsefulID
						}); c != nil {
							if log.V(1) {
								log.Health.Infof(ctx, "closing least useful client %+v to tighten network graph", c)
							}
							log.VEventf(ctx, 1, "culling n%d %s", c.peerID, c.addr)
							c.close()

							// After releasing the lock, block until the client disconnects.
							defer func() {
								g.doDisconnected(<-g.disconnected)
							}()
						} else {
							if log.V(1) {
								g.clientsMu.Lock()
								log.Health.Infof(ctx, "couldn't find least useful client among %+v", g.clientsMu.clients)
								g.clientsMu.Unlock()
							}
						}
					}
					g.mu.Unlock()
				}()
			case <-stallTimer.C:
				stallTimer.Read = true
				stallTimer.Reset(jitteredInterval(g.stallInterval))

				g.mu.Lock()
				g.maybeSignalStatusChangeLocked()
				g.mu.Unlock()
			}
		}
	})
}

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from checkInterval.
func jitteredInterval(interval time.Duration) time.Duration {
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
			log.Health.Errorf(ctx, "unable to get address for n%d: %s", distantNodeID, err)
		} else {
			log.Health.Infof(ctx, "starting client to n%d (%d > %d) to tighten network graph",
				distantNodeID, distantHops, maxHops)
			log.Eventf(ctx, "tightening network with new client to %s", nodeAddr)
			g.startClientLocked(nodeAddr)
		}
	}
}

func (g *Gossip) doDisconnected(c *client) {
	defer g.updateClients()

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
	multiNode := len(g.bootstrapInfo.Addresses) > 0
	// We're stalled if we don't have the sentinel key, or if we're a multi node
	// cluster and have no gossip connections.
	stalled := (orphaned && multiNode) || g.mu.is.getInfo(KeySentinel) == nil
	if stalled {
		// We employ the stalled boolean to avoid filling logs with warnings.
		if !g.stalled {
			log.Eventf(ctx, "now stalled")
			if orphaned {
				if len(g.resolvers) == 0 {
					if log.V(1) {
						log.Ops.Warningf(ctx, "no resolvers found; use --join to specify a connected node")
					}
				} else {
					log.Health.Warningf(ctx, "no incoming or outgoing connections")
				}
			} else if len(g.resolversTried) == len(g.resolvers) {
				log.Health.Warningf(ctx, "first range unavailable; resolvers exhausted")
			} else {
				log.Health.Warningf(ctx, "first range unavailable; trying remaining resolvers")
			}
		}
		if len(g.resolvers) > 0 {
			g.signalStalledLocked()
		}
	} else {
		if g.stalled {
			log.Eventf(ctx, "connected")
			log.Ops.Infof(ctx, "node has connected to cluster via gossip")
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
		name := fmt.Sprintf("gossip %v->%v", g.rpcContext.Config.Addr, addr)
		breaker = g.rpcContext.NewBreaker(name)
		g.clientsMu.breakers[addr.String()] = breaker
	}
	ctx := g.AnnotateCtx(context.TODO())
	log.VEventf(ctx, 1, "starting new client to %s", addr)
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
			log.VEventf(ctx, 1, "client %s disconnected", candidate.addr)
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

// A firstRangeMissingError indicates that the first range has not yet
// been gossiped. This will be the case for a node which hasn't yet
// joined the gossip network.
type firstRangeMissingError struct{}

// Error is part of the error interface.
func (f firstRangeMissingError) Error() string {
	return "the descriptor for the first range is not available via gossip"
}

// GetFirstRangeDescriptor implements kvcoord.FirstRangeProvider.
func (g *Gossip) GetFirstRangeDescriptor() (*roachpb.RangeDescriptor, error) {
	desc := &roachpb.RangeDescriptor{}
	if err := g.GetInfoProto(KeyFirstRangeDescriptor, desc); err != nil {
		return nil, firstRangeMissingError{}
	}
	return desc, nil
}

// OnFirstRangeChanged implements kvcoord.FirstRangeProvider.
func (g *Gossip) OnFirstRangeChanged(cb func(*roachpb.RangeDescriptor)) {
	g.RegisterCallback(KeyFirstRangeDescriptor, func(_ string, value roachpb.Value) {
		ctx := context.Background()
		desc := &roachpb.RangeDescriptor{}
		if err := value.GetProto(desc); err != nil {
			log.Errorf(ctx, "unable to parse gossiped first range descriptor: %s", err)
		} else {
			cb(desc)
		}
	})
}

// MakeOptionalGossip initializes an OptionalGossip instance wrapping a
// (possibly nil) *Gossip.
//
// Use of Gossip from within the SQL layer is **deprecated**. Please do not
// introduce new uses of it.
//
// See TenantSQLDeprecatedWrapper for details.
func MakeOptionalGossip(g *Gossip) OptionalGossip {
	return OptionalGossip{
		w: errorutil.MakeTenantSQLDeprecatedWrapper(g, g != nil),
	}
}

// OptionalGossip is a Gossip instance in a SQL tenant server.
//
// Use of Gossip from within the SQL layer is **deprecated**. Please do not
// introduce new uses of it.
//
// See TenantSQLDeprecatedWrapper for details.
type OptionalGossip struct {
	w errorutil.TenantSQLDeprecatedWrapper
}

// OptionalErr returns the Gossip instance if the wrapper was set up to allow
// it. Otherwise, it returns an error referring to the optionally passed in
// issues.
//
// Use of Gossip from within the SQL layer is **deprecated**. Please do not
// introduce new uses of it.
func (og OptionalGossip) OptionalErr(issue int) (*Gossip, error) {
	v, err := og.w.OptionalErr(issue)
	if err != nil {
		return nil, err
	}
	// NB: some tests use a nil Gossip.
	g, _ := v.(*Gossip)
	return g, nil
}

// Optional is like OptionalErr, but returns false if Gossip is not exposed.
//
// Use of Gossip from within the SQL layer is **deprecated**. Please do not
// introduce new uses of it.
func (og OptionalGossip) Optional(issue int) (*Gossip, bool) {
	v, ok := og.w.Optional()
	if !ok {
		return nil, false
	}
	// NB: some tests use a nil Gossip.
	g, _ := v.(*Gossip)
	return g, true
}
