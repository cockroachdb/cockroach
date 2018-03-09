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

package server

import (
	"context"
	"fmt"
	"math"
	"net"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	// gossipStatusInterval is the interval for logging gossip status.
	gossipStatusInterval = 1 * time.Minute
	// gossipNodeDescriptorInterval is the interval for gossiping the node descriptor.
	// Note that increasing this duration may increase the likelihood of gossip
	// thrashing, since node descriptors are used to determine the number of gossip
	// hops between nodes (see #9819 for context).
	gossipNodeDescriptorInterval = 1 * time.Hour

	// FirstNodeID is the node ID of the first node in a new cluster.
	FirstNodeID = 1
)

// Metric names.
var (
	metaExecLatency = metric.Metadata{
		Name: "exec.latency",
		Help: "Latency in nanoseconds of batch KV requests executed on this node"}
	metaExecSuccess = metric.Metadata{
		Name: "exec.success",
		Help: "Number of batch KV requests executed successfully on this node"}
	metaExecError = metric.Metadata{
		Name: "exec.error",
		Help: "Number of batch KV requests that failed to execute on this node"}
)

type nodeMetrics struct {
	Latency *metric.Histogram
	Success *metric.Counter
	Err     *metric.Counter
}

func makeNodeMetrics(reg *metric.Registry, histogramWindow time.Duration) nodeMetrics {
	nm := nodeMetrics{
		Latency: metric.NewLatency(metaExecLatency, histogramWindow),
		Success: metric.NewCounter(metaExecSuccess),
		Err:     metric.NewCounter(metaExecError),
	}
	reg.AddMetricStruct(nm)
	return nm
}

// callComplete records very high-level metrics about the number of completed
// calls and their latency. Currently, this only records statistics at the batch
// level; stats on specific lower-level kv operations are not recorded.
func (nm nodeMetrics) callComplete(d time.Duration, pErr *roachpb.Error) {
	if pErr != nil && pErr.TransactionRestart == roachpb.TransactionRestart_NONE {
		nm.Err.Inc(1)
	} else {
		nm.Success.Inc(1)
	}
	nm.Latency.RecordValue(d.Nanoseconds())
}

// A Node manages a map of stores (by store ID) for which it serves
// traffic. A node is the top-level data structure. There is one node
// instance per process. A node accepts incoming RPCs and services
// them by directing the commands contained within RPCs to local
// stores, which in turn direct the commands to specific ranges. Each
// node has access to the global, monolithic Key-Value abstraction via
// its client.DB reference. Nodes use this to allocate node and store
// IDs for bootstrapping the node itself or new stores as they're added
// on subsequent instantiations.
type Node struct {
	stopper     *stop.Stopper
	clusterID   *base.ClusterIDContainer // UUID for Cockroach cluster
	Descriptor  roachpb.NodeDescriptor   // Node ID, network/physical topology
	storeCfg    storage.StoreConfig      // Config to use and pass to stores
	eventLogger sql.EventLogger
	stores      *storage.Stores // Access to node-local stores
	metrics     nodeMetrics
	recorder    *status.MetricsRecorder
	startedAt   int64
	lastUp      int64
	initialBoot bool // True if this is the first time this node has started.
	txnMetrics  kv.TxnMetrics

	storesServer storage.Server
}

// allocateNodeID increments the node id generator key to allocate
// a new, unique node id.
func allocateNodeID(ctx context.Context, db *client.DB) (roachpb.NodeID, error) {
	val, err := client.IncrementValRetryable(ctx, db, keys.NodeIDGenerator, 1)
	if err != nil {
		return 0, errors.Wrap(err, "unable to allocate node ID")
	}
	return roachpb.NodeID(val), nil
}

// allocateStoreIDs increments the store id generator key for the
// specified node to allocate "inc" new, unique store ids. The
// first ID in a contiguous range is returned on success.
func allocateStoreIDs(
	ctx context.Context, nodeID roachpb.NodeID, inc int64, db *client.DB,
) (roachpb.StoreID, error) {
	val, err := client.IncrementValRetryable(ctx, db, keys.StoreIDGenerator, inc)
	if err != nil {
		return 0, errors.Wrapf(err, "unable to allocate %d store IDs for node %d", inc, nodeID)
	}
	return roachpb.StoreID(val - inc + 1), nil
}

// GetBootstrapSchema returns the schema which will be used to bootstrap a new
// server.
func GetBootstrapSchema() sqlbase.MetadataSchema {
	return sqlbase.MakeMetadataSchema()
}

// bootstrapCluster bootstraps a multiple stores using the provided
// engines and cluster ID. The first bootstrapped store contains a
// single range spanning all keys. Initial range lookup metadata is
// populated for the range. Returns the cluster ID.
func bootstrapCluster(
	ctx context.Context,
	cfg storage.StoreConfig,
	engines []engine.Engine,
	bootstrapVersion cluster.ClusterVersion,
	txnMetrics kv.TxnMetrics,
) (uuid.UUID, error) {
	clusterID := uuid.MakeV4()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Make sure that the store config has a valid clock and that it doesn't
	// try to use gossip, since that can introduce race conditions.
	if cfg.Clock == nil {
		cfg.Clock = hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	}
	cfg.Gossip = nil
	cfg.TestingKnobs = storage.StoreTestingKnobs{}
	cfg.ScanInterval = 10 * time.Minute
	cfg.HistogramWindowInterval = time.Duration(math.MaxInt64)
	tr := cfg.Settings.Tracer
	defer tr.Close()
	cfg.AmbientCtx.Tracer = tr
	// Create a KV DB with a local sender.
	stores := storage.NewStores(cfg.AmbientCtx, cfg.Clock, cfg.Settings.Version.MinSupportedVersion, cfg.Settings.Version.ServerVersion)
	tcsFactory := kv.NewTxnCoordSenderFactory(cfg.AmbientCtx, cfg.Settings, stores, cfg.Clock, false /* linearizable */, stopper, txnMetrics)
	cfg.DB = client.NewDB(tcsFactory, cfg.Clock)
	cfg.Transport = storage.NewDummyRaftTransport(cfg.Settings)
	if err := cfg.Settings.InitializeVersion(bootstrapVersion); err != nil {
		return uuid.UUID{}, errors.Wrap(err, "while initializing cluster version")
	}
	for i, eng := range engines {
		sIdent := roachpb.StoreIdent{
			ClusterID: clusterID,
			NodeID:    FirstNodeID,
			StoreID:   roachpb.StoreID(i + 1),
		}

		// The bootstrapping store will not connect to other nodes so its
		// StoreConfig doesn't really matter.
		s := storage.NewStore(cfg, eng, &roachpb.NodeDescriptor{NodeID: FirstNodeID})

		// Bootstrap store to persist the store ident and cluster version.
		if err := s.Bootstrap(ctx, sIdent, bootstrapVersion); err != nil {
			return uuid.UUID{}, err
		}
		// Create first range, writing directly to engine. Note this does
		// not create the range, just its data. Only do this if this is the
		// first store.
		if i == 0 {
			initialValues := GetBootstrapSchema().GetInitialValues()
			// The MinimumVersion is the ServerVersion when we are bootstrapping
			// a cluster (except in some tests that specifically want to set up
			// an "old-looking" cluster).
			if err := s.BootstrapRange(initialValues, bootstrapVersion.MinimumVersion); err != nil {
				return uuid.UUID{}, err
			}
		}
		if err := s.Start(ctx, stopper); err != nil {
			return uuid.UUID{}, err
		}

		stores.AddStore(s)

		// Initialize node and store ids.  Only initialize the node once.
		if i == 0 {
			if nodeID, err := allocateNodeID(ctx, cfg.DB); nodeID != sIdent.NodeID || err != nil {
				return uuid.UUID{}, errors.Errorf("expected to initialize node id allocator to %d, got %d: %s",
					sIdent.NodeID, nodeID, err)
			}
		}
		if storeID, err := allocateStoreIDs(ctx, sIdent.NodeID, 1, cfg.DB); storeID != sIdent.StoreID || err != nil {
			return uuid.UUID{}, errors.Errorf("expected to initialize store id allocator to %d, got %d: %s",
				sIdent.StoreID, storeID, err)
		}
	}
	return clusterID, nil
}

// duplicateBootstrapError is returned by Node.bootstrap when the node is already initialized.
type duplicateBootstrapError struct {
	ClusterID uuid.UUID
}

func (e *duplicateBootstrapError) Error() string {
	return fmt.Sprintf("cluster has already been initialized with ID %s", e.ClusterID)
}

// NewNode returns a new instance of Node.
//
// execCfg can be nil to help bootstrapping of a Server (the Node is created
// before the ExecutorConfig is initialized). In that case, InitLogger() needs
// to be called before the Node is used.
func NewNode(
	cfg storage.StoreConfig,
	recorder *status.MetricsRecorder,
	reg *metric.Registry,
	stopper *stop.Stopper,
	txnMetrics kv.TxnMetrics,
	execCfg *sql.ExecutorConfig,
	clusterID *base.ClusterIDContainer,
) *Node {
	var eventLogger sql.EventLogger
	if execCfg != nil {
		eventLogger = sql.MakeEventLogger(execCfg)
	}
	n := &Node{
		storeCfg:    cfg,
		stopper:     stopper,
		recorder:    recorder,
		metrics:     makeNodeMetrics(reg, cfg.HistogramWindowInterval),
		stores:      storage.NewStores(cfg.AmbientCtx, cfg.Clock, cfg.Settings.Version.MinSupportedVersion, cfg.Settings.Version.ServerVersion),
		txnMetrics:  txnMetrics,
		eventLogger: eventLogger,
		clusterID:   clusterID,
	}
	n.storesServer = storage.MakeServer(&n.Descriptor, n.stores)
	return n
}

// InitLogger needs to be called if a nil execCfg was passed to NewNode().
func (n *Node) InitLogger(execCfg *sql.ExecutorConfig) {
	n.eventLogger = sql.MakeEventLogger(execCfg)
}

// String implements fmt.Stringer.
func (n *Node) String() string {
	return fmt.Sprintf("node=%d", n.Descriptor.NodeID)
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (n *Node) AnnotateCtx(ctx context.Context) context.Context {
	return n.storeCfg.AmbientCtx.AnnotateCtx(ctx)
}

// AnnotateCtxWithSpan is a convenience wrapper; see AmbientContext.
func (n *Node) AnnotateCtxWithSpan(
	ctx context.Context, opName string,
) (context.Context, opentracing.Span) {
	return n.storeCfg.AmbientCtx.AnnotateCtxWithSpan(ctx, opName)
}

// initDescriptor initializes the node descriptor with the server
// address, the node attributes and locality.
func (n *Node) initDescriptor(addr net.Addr, attrs roachpb.Attributes, locality roachpb.Locality) {
	n.Descriptor.Address = util.MakeUnresolvedAddr(addr.Network(), addr.String())
	n.Descriptor.Attrs = attrs
	n.Descriptor.Locality = locality
	n.Descriptor.ServerVersion = n.storeCfg.Settings.Version.ServerVersion
}

// initNodeID updates the internal NodeDescriptor with the given ID. If zero is
// supplied, a new NodeID is allocated with the first invocation. For all other
// values, the supplied ID is stored into the descriptor (unless one has been
// set previously, in which case a fatal error occurs).
//
// Upon setting a new NodeID, the descriptor is gossiped and the NodeID is
// stored into the gossip instance.
func (n *Node) initNodeID(ctx context.Context, id roachpb.NodeID) {
	ctx = n.AnnotateCtx(ctx)
	if id < 0 {
		log.Fatalf(ctx, "NodeID must not be negative")
	}

	if o := n.Descriptor.NodeID; o > 0 {
		if id == 0 {
			return
		}
		log.Fatalf(ctx, "cannot initialize NodeID to %d, already have %d", id, o)
	}
	var err error
	if id == 0 {
		ctxWithSpan, span := n.AnnotateCtxWithSpan(ctx, "alloc-node-id")
		id, err = allocateNodeID(ctxWithSpan, n.storeCfg.DB)
		if err != nil {
			log.Fatal(ctxWithSpan, err)
		}
		log.Infof(ctxWithSpan, "new node allocated ID %d", id)
		if id == 0 {
			log.Fatal(ctxWithSpan, "new node allocated illegal ID 0")
		}
		span.Finish()
		n.storeCfg.Gossip.NodeID.Set(ctx, id)
	} else {
		log.Infof(ctx, "node ID %d initialized", id)
	}
	// Gossip the node descriptor to make this node addressable by node ID.
	n.Descriptor.NodeID = id
	if err = n.storeCfg.Gossip.SetNodeDescriptor(&n.Descriptor); err != nil {
		log.Fatalf(ctx, "couldn't gossip descriptor for node %d: %s", n.Descriptor.NodeID, err)
	}
}

func (n *Node) bootstrap(
	ctx context.Context, engines []engine.Engine, bootstrapVersion cluster.ClusterVersion,
) error {
	if n.initialBoot || n.clusterID.Get() != uuid.Nil {
		return &duplicateBootstrapError{ClusterID: n.clusterID.Get()}
	}
	n.initialBoot = true
	clusterID, err := bootstrapCluster(ctx, n.storeCfg, engines, bootstrapVersion, n.txnMetrics)
	if err != nil {
		return err
	}
	n.clusterID.Set(ctx, clusterID)

	log.Infof(ctx, "**** cluster %s has been created", clusterID)
	return nil
}

// start starts the node by registering the storage instance for the
// RPC service "Node" and initializing stores for each specified
// engine. Launches periodic store gossiping in a goroutine.
func (n *Node) start(
	ctx context.Context,
	addr net.Addr,
	bootstrappedEngines, emptyEngines []engine.Engine,
	attrs roachpb.Attributes,
	locality roachpb.Locality,
	cv cluster.ClusterVersion,
) error {
	n.initDescriptor(addr, attrs, locality)

	n.storeCfg.Settings.Version.OnChange(func(cv cluster.ClusterVersion) {
		if err := n.stores.OnClusterVersionChange(ctx, cv); err != nil {
			log.Fatal(ctx, errors.Wrapf(err, "updating cluster version to %s", cv))
		}
	})

	if err := n.storeCfg.Settings.InitializeVersion(cv); err != nil {
		return errors.Wrap(err, "while initializing cluster version")
	}

	// Initialize the stores we're going to start.
	stores, err := n.initStores(ctx, bootstrappedEngines, n.stopper)
	if err != nil {
		return err
	}

	// Initialize the stores we need to bootstrap first.
	bootstraps, err := n.initStores(ctx, emptyEngines, n.stopper)
	if err != nil {
		return err
	}

	if n.initialBoot {
		// The cluster was just bootstrapped by this node, explicitly notify the
		// stores that they were bootstrapped.
		for _, s := range stores {
			s.NotifyBootstrapped()
		}
	}

	if err := n.startStores(ctx, stores, n.stopper); err != nil {
		return err
	}

	// Bootstrap any uninitialized stores asynchronously.
	if len(bootstraps) > 0 {
		log.Infof(ctx, "%s: asynchronously bootstrapping engine(s) %v", n, emptyEngines)

		if err := n.stopper.RunAsyncTask(ctx, "node.Node: bootstrapping stores", func(ctx context.Context) {
			n.bootstrapStores(ctx, bootstraps, n.stopper)
		}); err != nil {
			return err
		}
	}

	n.startedAt = n.storeCfg.Clock.Now().WallTime

	n.startComputePeriodicMetrics(n.stopper, DefaultMetricsSampleInterval)
	// Be careful about moving this line above `startStores`; store migrations rely
	// on the fact that the cluster version has not been updated via Gossip (we
	// have migrations that want to run only if the server starts with a given
	// cluster version, but not if the server starts with a lower one and gets
	// bumped immediately, which would be possible if gossip got started earlier).
	n.startGossip(ctx, n.stopper)

	log.Infof(ctx, "%s: started with %v engine(s) and attributes %v", n, bootstrappedEngines, attrs.Attrs)
	return nil
}

// IsDraining returns true if at least one Store housed on this Node is not
// currently allowing range leases to be procured or extended.
func (n *Node) IsDraining() bool {
	var isDraining bool
	if err := n.stores.VisitStores(func(s *storage.Store) error {
		isDraining = isDraining || s.IsDraining()
		return nil
	}); err != nil {
		panic(err)
	}
	return isDraining
}

// SetDraining sets the draining mode on all of the node's underlying stores.
func (n *Node) SetDraining(drain bool) error {
	return n.stores.VisitStores(func(s *storage.Store) error {
		s.SetDraining(drain)
		return nil
	})
}

// SetHLCUpperBound sets the upper bound of the HLC wall time on all of the
// node's underlying stores.
func (n *Node) SetHLCUpperBound(ctx context.Context, hlcUpperBound int64) error {
	return n.stores.VisitStores(func(s *storage.Store) error {
		return s.WriteHLCUpperBound(ctx, hlcUpperBound)
	})
}

// initStores initializes the Stores map from ID to Store. Stores are
// added to the local sender if already bootstrapped. A bootstrapped
// Store has a valid ident with cluster, node and Store IDs set. If
// the Store doesn't yet have a valid ident, it's added to the
// bootstraps list for initialization once the cluster and node IDs
// have been determined.
func (n *Node) initStores(
	ctx context.Context, engines []engine.Engine, stopper *stop.Stopper,
) ([]*storage.Store, error) {
	var stores []*storage.Store
	for _, e := range engines {
		s := storage.NewStore(n.storeCfg, e, &n.Descriptor)
		log.Eventf(ctx, "created store for engine: %s", e)

		stores = append(stores, s)
	}
	return stores, nil
}

func (n *Node) startStores(
	ctx context.Context, stores []*storage.Store, stopper *stop.Stopper,
) error {
	for _, s := range stores {
		if err := s.Start(ctx, stopper); err != nil {
			return errors.Errorf("failed to start store: %s", err)
		}
		if s.Ident.ClusterID == (uuid.UUID{}) || s.Ident.NodeID == 0 {
			return errors.Errorf("unidentified store: %s", s)
		}
		capacity, err := s.Capacity()
		if err != nil {
			return errors.Errorf("could not query store capacity: %s", err)
		}
		log.Infof(ctx, "initialized store %s: %+v", s, capacity)

		n.addStore(s)
	}

	// Verify all initialized stores agree on cluster and node IDs.
	if err := n.validateStores(ctx); err != nil {
		return err
	}
	log.Event(ctx, "validated stores")

	// Compute the time this node was last up; this is done by reading the
	// "last up time" from every store and choosing the most recent timestamp.
	var mostRecentTimestamp hlc.Timestamp
	if err := n.stores.VisitStores(func(s *storage.Store) error {
		timestamp, err := s.ReadLastUpTimestamp(ctx)
		if err != nil {
			return err
		}
		if mostRecentTimestamp.Less(timestamp) {
			mostRecentTimestamp = timestamp
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "failed to read last up timestamp from stores")
	}
	n.lastUp = mostRecentTimestamp.WallTime

	// Set the stores map as the gossip persistent storage, so that
	// gossip can bootstrap using the most recently persisted set of
	// node addresses.
	if err := n.storeCfg.Gossip.SetStorage(n.stores); err != nil {
		return fmt.Errorf("failed to initialize the gossip interface: %s", err)
	}

	// Read persisted ClusterVersion from each configured store to
	// verify there are no stores with data too old or too new for this
	// binary.
	if _, err := n.stores.SynthesizeClusterVersion(ctx); err != nil {
		return err
	}
	// Also populate bootstrap list

	// Connect gossip before starting bootstrap. For new nodes, connecting
	// to the gossip network is necessary to get the cluster ID.
	if err := n.connectGossip(ctx); err != nil {
		return err
	}
	log.Event(ctx, "connected to gossip")

	// If no NodeID has been assigned yet, allocate a new node ID by
	// supplying 0 to initNodeID.
	if n.Descriptor.NodeID == 0 {
		n.initNodeID(ctx, 0)
		n.initialBoot = true
		log.Eventf(ctx, "allocated node ID %d", n.Descriptor.NodeID)
	}

	return nil
}

func (n *Node) addStore(store *storage.Store) {
	n.stores.AddStore(store)
	n.recorder.AddStore(store)
}

// validateStores iterates over all stores, verifying they agree on node ID.
// The node's ident is initialized based on the agreed-upon node ID. Note that
// cluster ID consistency is checked elsewhere in inspectEngines.
func (n *Node) validateStores(ctx context.Context) error {
	return n.stores.VisitStores(func(s *storage.Store) error {
		if n.Descriptor.NodeID == 0 {
			n.initNodeID(ctx, s.Ident.NodeID)
		} else if n.Descriptor.NodeID != s.Ident.NodeID {
			return errors.Errorf("store %s node ID doesn't match node ID: %d", s, n.Descriptor.NodeID)
		}
		return nil
	})
}

// bootstrapStores bootstraps uninitialized stores once the cluster
// and node IDs have been established for this node. Store IDs are
// allocated via a sequence id generator stored at a system key per
// node.
func (n *Node) bootstrapStores(
	ctx context.Context, bootstraps []*storage.Store, stopper *stop.Stopper,
) {
	if n.clusterID.Get() == uuid.Nil {
		panic("ClusterID missing during store bootstrap of auxiliary store")
	}

	// Bootstrap all waiting stores by allocating a new store id for
	// each and invoking store.Bootstrap() to persist.
	inc := int64(len(bootstraps))
	firstID, err := allocateStoreIDs(ctx, n.Descriptor.NodeID, inc, n.storeCfg.DB)
	if err != nil {
		log.Fatalf(ctx, "error allocating store ids: %+v", err)
	}
	sIdent := roachpb.StoreIdent{
		ClusterID: n.clusterID.Get(),
		NodeID:    n.Descriptor.NodeID,
		StoreID:   firstID,
	}

	// FIXME(tschottdorf): what is this version if we're joining a cluster?
	// I think it's our `MinSupportedVersion`, which is the best we can do
	// but isn't technically "correct". We should be able to wait for an
	// authoritative version from gossip here.
	cv, err := n.stores.SynthesizeClusterVersion(ctx)
	if err != nil {
		log.Fatalf(ctx, "error retrieving cluster version for bootstrap: %s", err)
	}

	for _, s := range bootstraps {
		if err := s.Bootstrap(ctx, sIdent, cv); err != nil {
			log.Fatal(ctx, err)
		}
		if err := s.Start(ctx, stopper); err != nil {
			log.Fatal(ctx, err)
		}
		n.addStore(s)
		sIdent.StoreID++
		log.Infof(ctx, "bootstrapped store %s", s)
		// Done regularly in Node.startGossip, but this cuts down the time
		// until this store is used for range allocations.
		if err := s.GossipStore(ctx); err != nil {
			log.Warningf(ctx, "error doing initial gossiping: %s", err)
		}
	}
	// write a new status summary after all stores have been bootstrapped; this
	// helps the UI remain responsive when new nodes are added.
	if err := n.writeSummaries(ctx); err != nil {
		log.Warningf(ctx, "error writing node summary after store bootstrap: %s", err)
	}
}

// connectGossip connects to gossip network and reads cluster ID. If
// this node is already part of a cluster, the cluster ID is verified
// for a match. If not part of a cluster, the cluster ID is set. The
// node's address is gossiped with node ID as the gossip key.
func (n *Node) connectGossip(ctx context.Context) error {
	log.Infof(ctx, "connecting to gossip network to verify cluster ID...")
	select {
	case <-n.stopper.ShouldStop():
		return errors.New("stop called before we could connect to gossip")
	case <-ctx.Done():
		return ctx.Err()
	case <-n.storeCfg.Gossip.Connected:
	}

	uuidBytes, err := n.storeCfg.Gossip.GetInfo(gossip.KeyClusterID)
	if err != nil {
		return errors.Wrap(err, "unable to ascertain cluster ID from gossip network")
	}
	gossipClusterID, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		return errors.Wrap(err, "unable to parse cluster ID from gossip network")
	}

	clusterID := n.clusterID.Get()
	if clusterID == uuid.Nil {
		n.clusterID.Set(ctx, gossipClusterID)
	} else if clusterID != gossipClusterID {
		return errors.Errorf("node %d belongs to cluster %q but is attempting to connect to a gossip network for cluster %q",
			n.Descriptor.NodeID, clusterID, gossipClusterID)
	}
	log.Infof(ctx, "node connected via gossip and verified as part of cluster %q", gossipClusterID)
	return nil
}

// startGossip loops on a periodic ticker to gossip node-related
// information. Starts a goroutine to loop until the node is closed.
func (n *Node) startGossip(ctx context.Context, stopper *stop.Stopper) {
	ctx = n.AnnotateCtx(ctx)
	stopper.RunWorker(ctx, func(ctx context.Context) {
		// This should always return immediately and acts as a sanity check that we
		// don't try to gossip before we're connected.
		select {
		case <-n.storeCfg.Gossip.Connected:
		default:
			panic(fmt.Sprintf("%s: not connected to gossip", n))
		}
		// Verify we've already gossiped our node descriptor.
		if _, err := n.storeCfg.Gossip.GetNodeDescriptor(n.Descriptor.NodeID); err != nil {
			panic(err)
		}

		statusTicker := time.NewTicker(gossipStatusInterval)
		storesTicker := time.NewTicker(gossip.GossipStoresInterval)
		nodeTicker := time.NewTicker(gossipNodeDescriptorInterval)
		defer storesTicker.Stop()
		defer nodeTicker.Stop()
		n.gossipStores(ctx) // one-off run before going to sleep
		for {
			select {
			case <-statusTicker.C:
				n.storeCfg.Gossip.LogStatus()
			case <-storesTicker.C:
				n.gossipStores(ctx)
			case <-nodeTicker.C:
				if err := n.storeCfg.Gossip.SetNodeDescriptor(&n.Descriptor); err != nil {
					log.Warningf(ctx, "couldn't gossip descriptor for node %d: %s", n.Descriptor.NodeID, err)
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// gossipStores broadcasts each store and dead replica to the gossip network.
func (n *Node) gossipStores(ctx context.Context) {
	if err := n.stores.VisitStores(func(s *storage.Store) error {
		if err := s.GossipStore(ctx); err != nil {
			return err
		}
		return s.GossipDeadReplicas(ctx)
	}); err != nil {
		log.Warning(ctx, err)
	}
}

// startComputePeriodicMetrics starts a loop which periodically instructs each
// store to compute the value of metrics which cannot be incrementally
// maintained.
func (n *Node) startComputePeriodicMetrics(stopper *stop.Stopper, interval time.Duration) {
	ctx := n.AnnotateCtx(context.Background())
	stopper.RunWorker(ctx, func(ctx context.Context) {
		// Compute periodic stats at the same frequency as metrics are sampled.
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for tick := 0; ; tick++ {
			select {
			case <-ticker.C:
				if err := n.computePeriodicMetrics(ctx, tick); err != nil {
					log.Errorf(ctx, "failed computing periodic metrics: %s", err)
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// computePeriodicMetrics instructs each store to compute the value of
// complicated metrics.
func (n *Node) computePeriodicMetrics(ctx context.Context, tick int) error {
	return n.stores.VisitStores(func(store *storage.Store) error {
		if err := store.ComputeMetrics(ctx, tick); err != nil {
			log.Warningf(ctx, "%s: unable to compute metrics: %s", store, err)
		}
		return nil
	})
}

// startWriteSummaries begins periodically persisting status summaries for the
// node and its stores.
func (n *Node) startWriteSummaries(frequency time.Duration) {
	ctx := log.WithLogTag(n.AnnotateCtx(context.Background()), "summaries", nil)
	// Immediately record summaries once on server startup.
	n.stopper.RunWorker(ctx, func(ctx context.Context) {
		// Write a status summary immediately; this helps the UI remain
		// responsive when new nodes are added.
		if err := n.writeSummaries(ctx); err != nil {
			log.Warningf(ctx, "error recording initial status summaries: %s", err)
		}
		ticker := time.NewTicker(frequency)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := n.writeSummaries(ctx); err != nil {
					log.Warningf(ctx, "error recording status summaries: %s", err)
				}
			case <-n.stopper.ShouldStop():
				return
			}
		}
	})
}

// writeSummaries retrieves status summaries from the supplied
// NodeStatusRecorder and persists them to the cockroach data store.
func (n *Node) writeSummaries(ctx context.Context) error {
	var err error
	if runErr := n.stopper.RunTask(ctx, "node.Node: writing summary", func(ctx context.Context) {
		err = n.recorder.WriteStatusSummary(ctx, n.storeCfg.DB)
	}); runErr != nil {
		err = runErr
	}
	return err
}

// recordJoinEvent begins an asynchronous task which attempts to log a "node
// join" or "node restart" event. This query will retry until it succeeds or the
// server stops.
func (n *Node) recordJoinEvent() {
	if !n.storeCfg.LogRangeEvents {
		return
	}

	logEventType := sql.EventLogNodeRestart
	lastUp := n.lastUp
	if n.initialBoot {
		logEventType = sql.EventLogNodeJoin
		lastUp = n.startedAt
	}

	n.stopper.RunWorker(context.Background(), func(bgCtx context.Context) {
		ctx, span := n.AnnotateCtxWithSpan(bgCtx, "record-join-event")
		defer span.Finish()
		retryOpts := base.DefaultRetryOptions()
		retryOpts.Closer = n.stopper.ShouldStop()
		for r := retry.Start(retryOpts); r.Next(); {
			if err := n.storeCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				return n.eventLogger.InsertEventRecord(
					ctx,
					txn,
					logEventType,
					int32(n.Descriptor.NodeID),
					int32(n.Descriptor.NodeID),
					struct {
						Descriptor roachpb.NodeDescriptor
						ClusterID  uuid.UUID
						StartedAt  int64
						LastUp     int64
					}{n.Descriptor, n.clusterID.Get(), n.startedAt, lastUp},
				)
			}); err != nil {
				log.Warningf(ctx, "%s: unable to log %s event: %s", n, logEventType, err)
			} else {
				return
			}
		}
	})
}

// If we receive a (proto-marshaled) roachpb.BatchRequest whose Requests contain
// a message type unknown to this node, we will end up with a zero entry in the
// slice. If we don't error out early, this breaks all sorts of assumptions and
// usually ends in a panic.
func checkNoUnknownRequest(reqs []roachpb.RequestUnion) *roachpb.UnsupportedRequestError {
	for _, req := range reqs {
		if req.GetValue() == nil {
			return &roachpb.UnsupportedRequestError{}
		}
	}
	return nil
}

func (n *Node) batchInternal(
	ctx context.Context, args *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if detail := checkNoUnknownRequest(args.Requests); detail != nil {
		var br roachpb.BatchResponse
		br.Error = roachpb.NewError(detail)
		return &br, nil
	}

	isLocalRequest := grpcutil.IsLocalRequestContext(ctx)
	// TODO(marc): grpc's authentication model (which gives credential access in
	// the request handler) doesn't really fit with the current design of the
	// security package (which assumes that TLS state is only given at connection
	// time) - that should be fixed.
	if isLocalRequest {
		// this is a in-process request, bypass checks.
	} else if peer, ok := peer.FromContext(ctx); ok {
		if tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo); ok {
			certUser, err := security.GetCertificateUser(&tlsInfo.State)
			if err != nil {
				return nil, err
			}
			if certUser != security.NodeUser {
				return nil, errors.Errorf("user %s is not allowed", certUser)
			}
		}
	}

	var br *roachpb.BatchResponse

	if err := n.stopper.RunTaskWithErr(ctx, "node.Node: batch", func(ctx context.Context) error {
		var finishSpan func(*roachpb.BatchResponse)
		// Shadow ctx from the outer function. Written like this to pass the linter.
		ctx, finishSpan = n.setupSpanForIncomingRPC(ctx, isLocalRequest)
		defer func(br **roachpb.BatchResponse) {
			finishSpan(*br)
		}(&br)
		if log.HasSpanOrEvent(ctx) {
			log.Event(ctx, args.Summary())
		}

		tStart := timeutil.Now()
		var pErr *roachpb.Error
		br, pErr = n.stores.Send(ctx, *args)
		if pErr != nil {
			br = &roachpb.BatchResponse{}
			log.VErrEventf(ctx, 3, "%T", pErr.GetDetail())
		}
		if br.Error != nil {
			panic(roachpb.ErrorUnexpectedlySet(n.stores, br))
		}
		n.metrics.callComplete(timeutil.Since(tStart), pErr)
		br.Error = pErr
		return nil
	}); err != nil {
		return nil, err
	}
	return br, nil
}

// Batch implements the roachpb.InternalServer interface.
func (n *Node) Batch(
	ctx context.Context, args *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	growStack()

	// NB: Node.Batch is called directly for "local" calls. We don't want to
	// carry the associated log tags forward as doing so makes adding additional
	// log tags more expensive and makes local calls differ from remote calls.
	ctx = n.storeCfg.AmbientCtx.ResetAndAnnotateCtx(ctx)

	br, err := n.batchInternal(ctx, args)

	// We always return errors via BatchResponse.Error so structure is
	// preserved; plain errors are presumed to be from the RPC
	// framework and not from cockroach.
	if err != nil {
		if br == nil {
			br = &roachpb.BatchResponse{}
		}
		if br.Error != nil {
			log.Fatalf(
				ctx, "attempting to return both a plain error (%s) and roachpb.Error (%s)", err, br.Error,
			)
		}
		br.Error = roachpb.NewError(err)
	}
	return br, nil
}

// setupSpanForIncomingRPC takes a context and returns a derived context with a
// new span in it. Depending on the input context, that span might be a root
// span or a child span. If it is a child span, it might be a child span of a
// local or a remote span. Note that supporting both the "child of local span"
// and "child of remote span" cases are important, as this RPC can be called
// either through the network or directly if the caller is local.
//
// It returns the derived context and a cleanup function to be called when
// servicing the RPC is done. The cleanup function will close the span and, in
// case the span was the child of a remote span and "snowball tracing" was
// enabled on that parent span, it serializes the local trace into the
// BatchResponse. The cleanup function takes the BatchResponse in which the
// response is to serialized. The BatchResponse can be nil in case no response
// is to be returned to the rpc caller.
func (n *Node) setupSpanForIncomingRPC(
	ctx context.Context, isLocalRequest bool,
) (context.Context, func(*roachpb.BatchResponse)) {
	// The operation name matches the one created by the interceptor in the
	// remoteTrace case below.
	const opName = "/cockroach.roachpb.Internal/Batch"
	var newSpan, grpcSpan opentracing.Span
	if isLocalRequest {
		// This is a local request which circumvented gRPC. Start a span now.
		ctx, newSpan = tracing.ChildSpan(ctx, opName)
	} else {
		grpcSpan = opentracing.SpanFromContext(ctx)
		if grpcSpan == nil {
			// If tracing information was passed via gRPC metadata, the gRPC interceptor
			// should have opened a span for us. If not, open a span now (if tracing is
			// disabled, this will be a noop span).
			newSpan = n.storeCfg.AmbientCtx.Tracer.StartSpan(opName)
			ctx = opentracing.ContextWithSpan(ctx, newSpan)
		}
	}

	finishSpan := func(br *roachpb.BatchResponse) {
		if newSpan != nil {
			newSpan.Finish()
		}
		if br == nil {
			return
		}
		if grpcSpan != nil {
			// If this is a "snowball trace", we'll need to return all the recorded
			// spans in the BatchResponse at the end of the request.
			// We don't want to do this if the operation is on the same host, in which
			// case everything is already part of the same recording.
			if rec := tracing.GetRecording(grpcSpan); rec != nil {
				br.CollectedSpans = append(br.CollectedSpans, rec...)
			}
		}
	}
	return ctx, finishSpan
}

var growStackGlobal = false

//go:noinline
func growStack() {
	// Goroutine stacks currently start at 2 KB in size. The code paths through
	// the storage package often need a stack that is 32 KB in size. The stack
	// growth is mildly expensive making it useful to trick the runtime into
	// growing the stack early. Since goroutine stacks grow in multiples of 2 and
	// start at 2 KB in size, by placing a 16 KB object on the stack early in the
	// lifetime of a goroutine we force the runtime to use a 32 KB stack for the
	// goroutine.
	var buf [16 << 10] /* 16 KB */ byte
	if growStackGlobal {
		// Make sure the compiler doesn't optimize away buf.
		for i := range buf {
			buf[i] = byte(i)
		}
	}
}
