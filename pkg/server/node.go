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

package server

import (
	"fmt"
	"math"
	"net"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/status"
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
	gossipNodeDescriptorInterval = 1 * time.Hour

	// FirstNodeID is the node ID of the first node in a new cluster.
	FirstNodeID = 1
)

// Metric names.
var (
	metaExecLatency = metric.Metadata{
		Name: "exec.latency",
		Help: "Latency of batch KV requests executed on this node"}
	metaExecSuccess = metric.Metadata{
		Name: "exec.success",
		Help: "Number of batch KV requests executed successfully on this node"}
	metaExecError = metric.Metadata{
		Name: "exec.error",
		Help: "Number of batch KV requests that failed to execute on this node"}
)

// errNeedsBootstrap indicates the node should be used as the seed of
// a new cluster.
var errNeedsBootstrap = errors.New("node has no initialized stores and no instructions for joining an existing cluster")

// errCannotJoinSelf indicates that a node was started with no initialized
// stores but --join specifying itself; there's no way to make forward
// progress in this state.
var errCannotJoinSelf = errors.New("an uninitialized node cannot specify its own address to join a cluster")

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
// its kv.DB reference. Nodes use this to allocate node and store
// IDs for bootstrapping the node itself or new stores as they're added
// on subsequent instantiations.
type Node struct {
	stopper     *stop.Stopper
	ClusterID   uuid.UUID              // UUID for Cockroach cluster
	Descriptor  roachpb.NodeDescriptor // Node ID, network/physical topology
	storeCfg    storage.StoreConfig    // Config to use and pass to stores
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
	val, err := incVal(ctx, db, keys.NodeIDGenerator, 1)
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
	val, err := incVal(ctx, db, keys.StoreIDGenerator, inc)
	if err != nil {
		return 0, errors.Wrapf(err, "unable to allocate %d store IDs for node %d", inc, nodeID)
	}
	return roachpb.StoreID(val - inc + 1), nil
}

// incVal increments a key's value by a specified amount and returns the new
// value.
// It performs the increment as a retryable non-transactional increment. The key
// might be incremented multiple times because of the retries.
func incVal(ctx context.Context, db *client.DB, key roachpb.Key, inc int64) (int64, error) {
	var err error
	var res client.KeyValue
	for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
		res, err = db.Inc(ctx, key, inc)
		switch err.(type) {
		case *roachpb.RetryableTxnError, *roachpb.AmbiguousResultError:
			continue
		}
		break
	}
	return res.ValueInt(), err
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
	cfg storage.StoreConfig, engines []engine.Engine, txnMetrics kv.TxnMetrics,
) (uuid.UUID, error) {
	clusterID := uuid.MakeV4()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	// Make sure that the store config has a valid clock and that it doesn't
	// try to use gossip, since that can introduce race conditions.
	if cfg.Clock == nil {
		cfg.Clock = hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	}
	cfg.Gossip = nil
	cfg.TestingKnobs = storage.StoreTestingKnobs{}
	cfg.ScanInterval = 10 * time.Minute
	cfg.MetricsSampleInterval = time.Duration(math.MaxInt64)
	cfg.HistogramWindowInterval = time.Duration(math.MaxInt64)
	cfg.ConsistencyCheckInterval = 10 * time.Minute
	cfg.AmbientCtx.Tracer = tracing.NewTracer()
	// Create a KV DB with a local sender.
	stores := storage.NewStores(cfg.AmbientCtx, cfg.Clock)
	sender := kv.NewTxnCoordSender(cfg.AmbientCtx, stores, cfg.Clock, false, stopper, txnMetrics)
	cfg.DB = client.NewDB(sender, cfg.Clock)
	cfg.Transport = storage.NewDummyRaftTransport()
	for i, eng := range engines {
		sIdent := roachpb.StoreIdent{
			ClusterID: clusterID,
			NodeID:    FirstNodeID,
			StoreID:   roachpb.StoreID(i + 1),
		}

		// The bootstrapping store will not connect to other nodes so its
		// StoreConfig doesn't really matter.
		s := storage.NewStore(cfg, eng, &roachpb.NodeDescriptor{NodeID: FirstNodeID})

		// Verify the store isn't already part of a cluster.
		if s.Ident.ClusterID != (uuid.UUID{}) {
			return uuid.UUID{}, errors.Errorf("storage engine already belongs to a cluster (%s)", s.Ident.ClusterID)
		}

		// Bootstrap store to persist the store ident.
		if err := s.Bootstrap(sIdent); err != nil {
			return uuid.UUID{}, err
		}
		// Create first range, writing directly to engine. Note this does
		// not create the range, just its data. Only do this if this is the
		// first store.
		if i == 0 {
			initialValues := GetBootstrapSchema().GetInitialValues()
			if err := s.BootstrapRange(initialValues); err != nil {
				return uuid.UUID{}, err
			}
		}
		if err := s.Start(context.Background(), stopper); err != nil {
			return uuid.UUID{}, err
		}

		stores.AddStore(s)

		ctx := context.TODO()
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

// NewNode returns a new instance of Node.
func NewNode(
	cfg storage.StoreConfig,
	recorder *status.MetricsRecorder,
	reg *metric.Registry,
	stopper *stop.Stopper,
	txnMetrics kv.TxnMetrics,
	eventLogger sql.EventLogger,
) *Node {
	n := &Node{
		storeCfg:    cfg,
		stopper:     stopper,
		recorder:    recorder,
		metrics:     makeNodeMetrics(reg, cfg.HistogramWindowInterval),
		stores:      storage.NewStores(cfg.AmbientCtx, cfg.Clock),
		txnMetrics:  txnMetrics,
		eventLogger: eventLogger,
	}
	n.storesServer = storage.MakeServer(&n.Descriptor, n.stores)
	return n
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
}

// initNodeID updates the internal NodeDescriptor with the given ID. If zero is
// supplied, a new NodeID is allocated with the first invocation. For all other
// values, the supplied ID is stored into the descriptor (unless one has been
// set previously, in which case a fatal error occurs).
//
// Upon setting a new NodeID, the descriptor is gossiped and the NodeID is
// stored into the gossip instance.
func (n *Node) initNodeID(id roachpb.NodeID) {
	ctx := n.AnnotateCtx(context.TODO())
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

// start starts the node by registering the storage instance for the
// RPC service "Node" and initializing stores for each specified
// engine. Launches periodic store gossiping in a goroutine.
//
// The canBootstrap parameter indicates whether this node is eligible
// to bootstrap a new cluster. The -join flag must be empty.
func (n *Node) start(
	ctx context.Context,
	addr net.Addr,
	engines []engine.Engine,
	attrs roachpb.Attributes,
	locality roachpb.Locality,
	canBootstrap bool,
) error {
	n.initDescriptor(addr, attrs, locality)

	// Initialize stores, including bootstrapping new ones.
	if err := n.initStores(ctx, engines, n.stopper, false); err != nil {
		if err == errNeedsBootstrap {
			if !canBootstrap {
				return errCannotJoinSelf
			}
			n.initialBoot = true
			// This node has no initialized stores and no way to connect to
			// an existing cluster, so we bootstrap it.
			clusterID, err := bootstrapCluster(n.storeCfg, engines, n.txnMetrics)
			if err != nil {
				return err
			}
			log.Infof(ctx, "**** cluster %s has been created", clusterID)
			log.Infof(ctx, "**** add additional nodes by specifying --join=%s", addr)
			// After bootstrapping, try again to initialize the stores.
			if err := n.initStores(ctx, engines, n.stopper, true); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	n.startedAt = n.storeCfg.Clock.Now().WallTime

	n.startComputePeriodicMetrics(n.stopper, n.storeCfg.MetricsSampleInterval)
	n.startGossip(n.stopper)

	log.Infof(ctx, "%s: started with %v engine(s) and attributes %v", n, engines, attrs.Attrs)
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

// initStores initializes the Stores map from ID to Store. Stores are
// added to the local sender if already bootstrapped. A bootstrapped
// Store has a valid ident with cluster, node and Store IDs set. If
// the Store doesn't yet have a valid ident, it's added to the
// bootstraps list for initialization once the cluster and node IDs
// have been determined.
func (n *Node) initStores(
	ctx context.Context, engines []engine.Engine, stopper *stop.Stopper, bootstrapped bool,
) error {
	var bootstraps []*storage.Store

	if len(engines) == 0 {
		return errors.Errorf("no engines")
	}
	for _, e := range engines {
		s := storage.NewStore(n.storeCfg, e, &n.Descriptor)
		log.Eventf(ctx, "created store for engine: %s", e)
		if bootstrapped {
			s.NotifyBootstrapped()
		}
		// Initialize each store in turn, handling un-bootstrapped errors by
		// adding the store to the bootstraps list.
		if err := s.Start(ctx, stopper); err != nil {
			if _, ok := err.(*storage.NotBootstrappedError); ok {
				log.Infof(ctx, "store %s not bootstrapped", s)
				bootstraps = append(bootstraps, s)
				continue
			}
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

	// If there are no initialized stores and no gossip resolvers,
	// bootstrap this node as the seed of a new cluster.
	if n.stores.GetStoreCount() == 0 && len(n.storeCfg.Gossip.GetResolvers()) == 0 {
		return errNeedsBootstrap
	}

	// Verify all initialized stores agree on cluster and node IDs.
	if err := n.validateStores(); err != nil {
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

	// Connect gossip before starting bootstrap. For new nodes, connecting
	// to the gossip network is necessary to get the cluster ID.
	if err := n.connectGossip(ctx); err != nil {
		return err
	}
	log.Event(ctx, "connected to gossip")

	// If no NodeID has been assigned yet, allocate a new node ID by
	// supplying 0 to initNodeID.
	if n.Descriptor.NodeID == 0 {
		n.initNodeID(0)
		n.initialBoot = true
		log.Eventf(ctx, "allocated node ID %d", n.Descriptor.NodeID)
	}

	// Bootstrap any uninitialized stores asynchronously.
	if len(bootstraps) > 0 {
		if err := stopper.RunAsyncTask(ctx, func(ctx context.Context) {
			n.bootstrapStores(ctx, bootstraps, stopper)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (n *Node) addStore(store *storage.Store) {
	n.stores.AddStore(store)
	n.recorder.AddStore(store)
}

// validateStores iterates over all stores, verifying they agree on
// cluster ID and node ID. The node's ident is initialized based on
// the agreed-upon cluster and node IDs.
func (n *Node) validateStores() error {
	return n.stores.VisitStores(func(s *storage.Store) error {
		if n.ClusterID == (uuid.UUID{}) {
			n.ClusterID = s.Ident.ClusterID
			n.initNodeID(s.Ident.NodeID)
			n.storeCfg.Gossip.SetClusterID(s.Ident.ClusterID)
		} else if n.ClusterID != s.Ident.ClusterID {
			return errors.Errorf("store %s cluster ID doesn't match node cluster %q", s, n.ClusterID)
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
	if n.ClusterID == (uuid.UUID{}) {
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
		ClusterID: n.ClusterID,
		NodeID:    n.Descriptor.NodeID,
		StoreID:   firstID,
	}
	for _, s := range bootstraps {
		if err := s.Bootstrap(sIdent); err != nil {
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

	if n.ClusterID == (uuid.UUID{}) {
		n.ClusterID = gossipClusterID
		n.storeCfg.Gossip.SetClusterID(gossipClusterID)
	} else if n.ClusterID != gossipClusterID {
		return errors.Errorf("node %d belongs to cluster %q but is attempting to connect to a gossip network for cluster %q",
			n.Descriptor.NodeID, n.ClusterID, gossipClusterID)
	}
	log.Infof(ctx, "node connected via gossip and verified as part of cluster %q", gossipClusterID)
	return nil
}

// startGossip loops on a periodic ticker to gossip node-related
// information. Starts a goroutine to loop until the node is closed.
func (n *Node) startGossip(stopper *stop.Stopper) {
	ctx := n.AnnotateCtx(context.Background())
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
		if err := s.GossipDeadReplicas(ctx); err != nil {
			return err
		}
		return nil
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
	if runErr := n.stopper.RunTask(ctx, func(ctx context.Context) {
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
					}{n.Descriptor, n.ClusterID, n.startedAt, lastUp},
				)
			}); err != nil {
				log.Warningf(ctx, "%s: unable to log %s event: %s", n, logEventType, err)
			} else {
				return
			}
		}
	})
}

func (n *Node) batchInternal(
	ctx context.Context, args *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	// TODO(marc): grpc's authentication model (which gives credential access in
	// the request handler) doesn't really fit with the current design of the
	// security package (which assumes that TLS state is only given at connection
	// time) - that should be fixed.
	if grpcutil.IsLocalRequestContext(ctx) {
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

	if err := n.stopper.RunTaskWithErr(ctx, func(ctx context.Context) error {
		var finishSpan func(*roachpb.BatchResponse)
		// Shadow ctx from the outer function. Written like this to pass the linter.
		ctx, finishSpan = n.setupSpanForIncomingRPC(ctx, args.TraceContext)
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
			log.ErrEventf(ctx, "%T", pErr.GetDetail())
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
// remoteTranceContext is the span context of this remote call. Can be
// nil if this call is not remote.
//
// It returns the derived context and a cleanup function to be called when
// servicing the RPC is done. The cleanup function will close the span and, in
// case the span was the child of a remote span and "snowball tracing" was
// enabled on that parent span, it serializes the local trace into the
// BatchResponse. The cleanup function takes the BatchResponse in which the
// response is to serialized. The BatchResponse can be nil in case no response
// is to be returned to the rpc caller.
func (n *Node) setupSpanForIncomingRPC(
	ctx context.Context, remoteTraceContext *tracing.SpanContextCarrier,
) (context.Context, func(*roachpb.BatchResponse)) {
	const opName = "node.Batch"
	tr := n.storeCfg.AmbientCtx.Tracer
	var recordedTrace *tracing.RecordedTrace
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		// Child span of local parent.
		ctx, _ = tracing.ChildSpan(ctx, opName)
	} else {
		if remoteTraceContext == nil {
			// Root span.
			ctx = opentracing.ContextWithSpan(ctx, tr.StartSpan(opName))
		} else {
			// Child span of remote parent.
			var err error
			ctx, recordedTrace, err = tracing.JoinRemoteTrace(ctx, tr, remoteTraceContext, opName)
			if err != nil {
				// Fallback to root span.
				log.Warningf(ctx, "failed to join remote trace: %s", err)
				ctx = opentracing.ContextWithSpan(ctx, tr.StartSpan(opName))
			}
		}
	}

	finishSpan := func(br *roachpb.BatchResponse) {
		opentracing.SpanFromContext(ctx).Finish()
		if br == nil {
			return
		}
		// If this is a "snowball trace", we'll need to encode all the recorded
		// spans in the BatchResponse at the end of the request.
		if recordedTrace != nil {
			// Encode all the spans into the BatchResponse.
			recordedTrace.Done()
			for _, rawSpan := range recordedTrace.GetSpans() {
				encSp, err := tracing.EncodeRawSpan(&rawSpan, nil)
				if err == nil {
					br.CollectedSpans = append(br.CollectedSpans, encSp)
				} else {
					log.Warning(ctx, err)
				}
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
