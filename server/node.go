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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/cockroachdb/cockroach/util/uuid"
)

const (
	// gossipStoresInterval is the interval for gossiping storage-related info.
	gossipStoresInterval = 1 * time.Minute
	// gossipNodeDescriptorInterval is the interval for gossiping the node descriptor.
	gossipNodeDescriptorInterval = 1 * time.Hour
	// publishStatusInterval is the interval for publishing periodic statistics
	// from stores to the internal event feed.
	publishStatusInterval = 10 * time.Second
)

// errNeedsBootstrap indicates the node should be used as the seed of
// a new cluster.
var errNeedsBootstrap = errors.New("node has no initialized stores and no instructions for joining an existing cluster")

// errCannotJoinSelf indicates that a node was started with no initialized
// stores but --join specifying itself; there's no way to make forward
// progress in this state.
var errCannotJoinSelf = errors.New("an uninitialized node cannot specify its own address to join a cluster")

type nodeMetrics struct {
	registry *metric.Registry
	latency  metric.Histograms
	success  metric.Rates
	err      metric.Rates
}

func makeNodeMetrics() nodeMetrics {
	reg := metric.NewRegistry()
	return nodeMetrics{
		registry: reg,
		latency:  reg.Latency("latency"),
		success:  reg.Rates("success"),
		err:      reg.Rates("error"),
	}
}

// callComplete records very high-level metrics about the number of completed
// calls and their latency. Currently, this only records statistics at the batch
// level; stats on specific lower-level kv operations are not recorded.
func (nm nodeMetrics) callComplete(d time.Duration, pErr *roachpb.Error) {
	if pErr != nil && pErr.TransactionRestart == roachpb.TransactionRestart_NONE {
		nm.err.Add(1)
	} else {
		nm.success.Add(1)
	}
	nm.latency.RecordValue(d.Nanoseconds())
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
	ctx         storage.StoreContext   // Context to use and pass to stores
	stores      *storage.Stores        // Access to node-local stores
	metrics     nodeMetrics
	recorder    *status.MetricsRecorder
	startedAt   int64
	initialBoot bool // True if this is the first time this node has started.
	txnMetrics  *kv.TxnMetrics
}

// allocateNodeID increments the node id generator key to allocate
// a new, unique node id.
func allocateNodeID(db *client.DB) (roachpb.NodeID, error) {
	r, err := db.Inc(keys.NodeIDGenerator, 1)
	if err != nil {
		return 0, util.Errorf("unable to allocate node ID: %s", err)
	}
	return roachpb.NodeID(r.ValueInt()), nil
}

// allocateStoreIDs increments the store id generator key for the
// specified node to allocate "inc" new, unique store ids. The
// first ID in a contiguous range is returned on success.
func allocateStoreIDs(nodeID roachpb.NodeID, inc int64, db *client.DB) (roachpb.StoreID, error) {
	r, err := db.Inc(keys.StoreIDGenerator, inc)
	if err != nil {
		return 0, util.Errorf("unable to allocate %d store IDs for node %d: %s", inc, nodeID, err)
	}
	return roachpb.StoreID(r.ValueInt() - inc + 1), nil
}

// GetBootstrapSchema returns the schema which will be used to bootstrap a new
// server.
func GetBootstrapSchema() sql.MetadataSchema {
	schema := sql.MakeMetadataSchema()
	storage.AddEventLogToMetadataSchema(&schema)
	sql.AddEventLogToMetadataSchema(&schema)
	return schema
}

// bootstrapCluster bootstraps a multiple stores using the provided
// engines and cluster ID. The first bootstrapped store contains a
// single range spanning all keys. Initial range lookup metadata is
// populated for the range. Returns the cluster ID.
func bootstrapCluster(engines []engine.Engine, txnMetrics *kv.TxnMetrics) (uuid.UUID, error) {
	clusterID := uuid.MakeV4()
	stopper := stop.NewStopper()
	defer stopper.Stop()

	ctx := storage.StoreContext{}
	ctx.ScanInterval = 10 * time.Minute
	ctx.ConsistencyCheckInterval = 10 * time.Minute
	ctx.Clock = hlc.NewClock(hlc.UnixNano)
	ctx.Tracer = tracing.NewTracer()
	// Create a KV DB with a local sender.
	stores := storage.NewStores(ctx.Clock)
	sender := kv.NewTxnCoordSender(stores, ctx.Clock, false, ctx.Tracer, stopper, txnMetrics)
	ctx.DB = client.NewDB(sender)
	ctx.Transport = storage.NewDummyRaftTransport()
	for i, eng := range engines {
		sIdent := roachpb.StoreIdent{
			ClusterID: clusterID,
			NodeID:    1,
			StoreID:   roachpb.StoreID(i + 1),
		}

		// The bootstrapping store will not connect to other nodes so its
		// StoreConfig doesn't really matter.
		s := storage.NewStore(ctx, eng, &roachpb.NodeDescriptor{NodeID: 1})

		// Verify the store isn't already part of a cluster.
		if s.Ident.ClusterID != *uuid.EmptyUUID {
			return uuid.UUID{}, util.Errorf("storage engine already belongs to a cluster (%s)", s.Ident.ClusterID)
		}

		// Bootstrap store to persist the store ident.
		if err := s.Bootstrap(sIdent, stopper); err != nil {
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
		if err := s.Start(stopper); err != nil {
			return uuid.UUID{}, err
		}

		stores.AddStore(s)

		// Initialize node and store ids.  Only initialize the node once.
		if i == 0 {
			if nodeID, err := allocateNodeID(ctx.DB); nodeID != sIdent.NodeID || err != nil {
				return uuid.UUID{}, util.Errorf("expected to initialize node id allocator to %d, got %d: %s",
					sIdent.NodeID, nodeID, err)
			}
		}
		if storeID, err := allocateStoreIDs(sIdent.NodeID, 1, ctx.DB); storeID != sIdent.StoreID || err != nil {
			return uuid.UUID{}, util.Errorf("expected to initialize store id allocator to %d, got %d: %s",
				sIdent.StoreID, storeID, err)
		}
	}
	return clusterID, nil
}

// NewNode returns a new instance of Node.
func NewNode(ctx storage.StoreContext, recorder *status.MetricsRecorder, stopper *stop.Stopper, txnMetrics *kv.TxnMetrics) *Node {
	n := &Node{
		ctx:        ctx,
		stopper:    stopper,
		recorder:   recorder,
		metrics:    makeNodeMetrics(),
		stores:     storage.NewStores(ctx.Clock),
		txnMetrics: txnMetrics,
	}
	n.recorder.AddNodeRegistry("exec.%s", n.metrics.registry)
	return n
}

// context returns a context encapsulating the NodeID, derived from the
// supplied context (which is not allowed to be nil).
func (n *Node) context(ctx context.Context) context.Context {
	if ctx == nil {
		panic("ctx cannot be nil")
	}
	return log.Add(ctx, log.NodeID, n.Descriptor.NodeID)
}

// initDescriptor initializes the node descriptor with the server
// address and the node attributes.
func (n *Node) initDescriptor(addr net.Addr, attrs roachpb.Attributes) {
	n.Descriptor.Address = util.MakeUnresolvedAddr(addr.Network(), addr.String())
	n.Descriptor.Attrs = attrs
}

// initNodeID updates the internal NodeDescriptor with the given ID. If zero is
// supplied, a new NodeID is allocated with the first invocation. For all other
// values, the supplied ID is stored into the descriptor (unless one has been
// set previously, in which case a fatal error occurs).
//
// Upon setting a new NodeID, the descriptor is gossiped and the NodeID is
// stored into the gossip instance.
func (n *Node) initNodeID(id roachpb.NodeID) {
	if id < 0 {
		log.Fatalf("NodeID must not be negative")
	}

	if o := n.Descriptor.NodeID; o > 0 {
		if id == 0 {
			return
		}
		log.Fatalf("cannot initialize NodeID to %d, already have %d", id, o)
	}
	var err error
	if id == 0 {
		id, err = allocateNodeID(n.ctx.DB)
		log.Infof("new node allocated ID %d", id)
		if err != nil {
			log.Fatal(err)
		}
		if id == 0 {
			log.Fatal("new node allocated illegal ID 0")
		}
		n.ctx.Gossip.SetNodeID(id)
	} else {
		log.Infof("node ID %d initialized", id)
	}
	// Gossip the node descriptor to make this node addressable by node ID.
	n.Descriptor.NodeID = id
	if err = n.ctx.Gossip.SetNodeDescriptor(&n.Descriptor); err != nil {
		log.Fatalf("couldn't gossip descriptor for node %d: %s", n.Descriptor.NodeID, err)
	}
}

// start starts the node by registering the storage instance for the
// RPC service "Node" and initializing stores for each specified
// engine. Launches periodic store gossiping in a goroutine.
func (n *Node) start(addr net.Addr, engines []engine.Engine, attrs roachpb.Attributes) error {
	n.initDescriptor(addr, attrs)

	// Initialize stores, including bootstrapping new ones.
	if err := n.initStores(engines, n.stopper); err != nil {
		if err == errNeedsBootstrap {
			n.initialBoot = true
			// This node has no initialized stores and no way to connect to
			// an existing cluster, so we bootstrap it.
			clusterID, err := bootstrapCluster(engines, n.txnMetrics)
			if err != nil {
				return err
			}
			log.Infof("**** cluster %s has been created", clusterID)
			log.Infof("**** add additional nodes by specifying --join=%s", addr)
			// Make sure we add the node as a resolver.
			selfResolver, err := resolver.NewResolverFromAddress(addr)
			if err != nil {
				return err
			}
			n.ctx.Gossip.SetResolvers([]resolver.Resolver{selfResolver})
			// After bootstrapping, try again to initialize the stores.
			if err := n.initStores(engines, n.stopper); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	n.startedAt = n.ctx.Clock.Now().WallTime

	// Initialize the recorder with the NodeID, which is initialized by initStores().
	n.recorder.NodeStarted(n.Descriptor, n.startedAt)

	n.startComputePeriodicMetrics(n.stopper)
	n.startGossip(n.stopper)

	// Record node started event.
	n.recordJoinEvent()

	log.Infoc(n.context(context.TODO()), "Started node with %v engine(s) and attributes %v", engines, attrs.Attrs)
	return nil
}

// initStores initializes the Stores map from ID to Store. Stores are
// added to the local sender if already bootstrapped. A bootstrapped
// Store has a valid ident with cluster, node and Store IDs set. If
// the Store doesn't yet have a valid ident, it's added to the
// bootstraps list for initialization once the cluster and node IDs
// have been determined.
func (n *Node) initStores(engines []engine.Engine, stopper *stop.Stopper) error {
	var bootstraps []*storage.Store

	if len(engines) == 0 {
		return util.Errorf("no engines")
	}
	for _, e := range engines {
		s := storage.NewStore(n.ctx, e, &n.Descriptor)
		// Initialize each store in turn, handling un-bootstrapped errors by
		// adding the store to the bootstraps list.
		if err := s.Start(stopper); err != nil {
			if _, ok := err.(*storage.NotBootstrappedError); ok {
				log.Infof("store %s not bootstrapped", s)
				bootstraps = append(bootstraps, s)
				continue
			}
			return util.Errorf("failed to start store: %s", err)
		}
		if s.Ident.ClusterID == *uuid.EmptyUUID || s.Ident.NodeID == 0 {
			return util.Errorf("unidentified store: %s", s)
		}
		capacity, err := s.Capacity()
		if err != nil {
			return util.Errorf("could not query store capacity: %s", err)
		}
		log.Infof("initialized store %s: %+v", s, capacity)
		n.addStore(s)
	}

	// If there are no initialized stores and no gossip resolvers,
	// bootstrap this node as the seed of a new cluster.
	if n.stores.GetStoreCount() == 0 {
		resolvers := n.ctx.Gossip.GetResolvers()
		// Check for the case of uninitialized node having only itself specified as join host.
		switch len(resolvers) {
		case 0:
			return errNeedsBootstrap
		case 1:
			if resolvers[0].Addr() == n.Descriptor.Address.String() {
				return errCannotJoinSelf
			}
		}
	}

	// Verify all initialized stores agree on cluster and node IDs.
	if err := n.validateStores(); err != nil {
		return err
	}

	// Set the stores map as the gossip persistent storage, so that
	// gossip can bootstrap using the most recently persisted set of
	// node addresses.
	if err := n.ctx.Gossip.SetStorage(n.stores); err != nil {
		return fmt.Errorf("failed to initialize the gossip interface: %s", err)
	}

	// Connect gossip before starting bootstrap. For new nodes, connecting
	// to the gossip network is necessary to get the cluster ID.
	n.connectGossip()

	// If no NodeID has been assigned yet, allocate a new node ID by
	// supplying 0 to initNodeID.
	if n.Descriptor.NodeID == 0 {
		n.initNodeID(0)
		n.initialBoot = true
	}

	// Bootstrap any uninitialized stores asynchronously.
	if len(bootstraps) > 0 {
		stopper.RunAsyncTask(func() {
			n.bootstrapStores(bootstraps, stopper)
		})
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
		if n.ClusterID == *uuid.EmptyUUID {
			n.ClusterID = s.Ident.ClusterID
			n.initNodeID(s.Ident.NodeID)
		} else if n.ClusterID != s.Ident.ClusterID {
			return util.Errorf("store %s cluster ID doesn't match node cluster %q", s, n.ClusterID)
		} else if n.Descriptor.NodeID != s.Ident.NodeID {
			return util.Errorf("store %s node ID doesn't match node ID: %d", s, n.Descriptor.NodeID)
		}
		return nil
	})
}

// bootstrapStores bootstraps uninitialized stores once the cluster
// and node IDs have been established for this node. Store IDs are
// allocated via a sequence id generator stored at a system key per
// node.
func (n *Node) bootstrapStores(bootstraps []*storage.Store, stopper *stop.Stopper) {
	if n.ClusterID == *uuid.EmptyUUID {
		panic("ClusterID missing during store bootstrap of auxiliary store")
	}

	// Bootstrap all waiting stores by allocating a new store id for
	// each and invoking store.Bootstrap() to persist.
	inc := int64(len(bootstraps))
	firstID, err := allocateStoreIDs(n.Descriptor.NodeID, inc, n.ctx.DB)
	if err != nil {
		log.Fatal(err)
	}
	sIdent := roachpb.StoreIdent{
		ClusterID: n.ClusterID,
		NodeID:    n.Descriptor.NodeID,
		StoreID:   firstID,
	}
	for _, s := range bootstraps {
		if err := s.Bootstrap(sIdent, stopper); err != nil {
			log.Fatal(err)
		}
		if err := s.Start(stopper); err != nil {
			log.Fatal(err)
		}
		n.addStore(s)
		sIdent.StoreID++
		log.Infof("bootstrapped store %s", s)
		// Done regularly in Node.startGossip, but this cuts down the time
		// until this store is used for range allocations.
		s.GossipStore()
	}
	// write a new status summary after all stores have been bootstrapped; this
	// helps the UI remain responsive when new nodes are added.
	if err := n.writeSummaries(); err != nil {
		log.Warningf("error writing node summary after store bootstrap: %s", err)
	}
}

// connectGossip connects to gossip network and reads cluster ID. If
// this node is already part of a cluster, the cluster ID is verified
// for a match. If not part of a cluster, the cluster ID is set. The
// node's address is gossiped with node ID as the gossip key.
func (n *Node) connectGossip() {
	log.Infof("connecting to gossip network to verify cluster ID...")
	// No timeout or stop condition is needed here. Log statements should be
	// sufficient for diagnosing this type of condition.
	<-n.ctx.Gossip.Connected

	uuidBytes, err := n.ctx.Gossip.GetInfo(gossip.KeyClusterID)
	if err != nil {
		log.Fatalf("unable to ascertain cluster ID from gossip network: %s", err)
	}
	gossipClusterIDPtr, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		log.Fatalf("unable to ascertain cluster ID from gossip network: %s", err)
	}
	gossipClusterID := *gossipClusterIDPtr

	if n.ClusterID == *uuid.EmptyUUID {
		n.ClusterID = gossipClusterID
	} else if n.ClusterID != gossipClusterID {
		log.Fatalf("node %d belongs to cluster %q but is attempting to connect to a gossip network for cluster %q",
			n.Descriptor.NodeID, n.ClusterID, gossipClusterID)
	}
	log.Infof("node connected via gossip and verified as part of cluster %q", gossipClusterID)
}

// startGossip loops on a periodic ticker to gossip node-related
// information. Starts a goroutine to loop until the node is closed.
func (n *Node) startGossip(stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		storesTicker := time.NewTicker(gossipStoresInterval)
		nodeTicker := time.NewTicker(gossipNodeDescriptorInterval)
		defer storesTicker.Stop()
		defer nodeTicker.Stop()
		n.gossipStores() // one-off run before going to sleep
		for {
			select {
			case <-storesTicker.C:
				n.gossipStores()
			case <-nodeTicker.C:
				if err := n.ctx.Gossip.SetNodeDescriptor(&n.Descriptor); err != nil {
					log.Warningf("couldn't gossip descriptor for node %d: %s", n.Descriptor.NodeID, err)
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// gossipStores broadcasts each store to the gossip network.
func (n *Node) gossipStores() {
	if err := n.stores.VisitStores(func(s *storage.Store) error {
		s.GossipStore()
		return nil
	}); err != nil {
		panic(err)
	}
}

// startComputePeriodidMetrics starts a loop which periodically instructs each
// store to compute the value of metrics which cannot be incrementally
// maintained.
func (n *Node) startComputePeriodicMetrics(stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		// Publish status at the same frequency as metrics are collected.
		ticker := time.NewTicker(publishStatusInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := n.computePeriodicMetrics()
				if err != nil {
					log.Error(err)
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// computePeriodicMetrics instructs each store to compute the value of
// complicated metrics.
func (n *Node) computePeriodicMetrics() error {
	return n.stores.VisitStores(func(store *storage.Store) error {
		return store.ComputeMetrics()
	})
}

// startWriteSummaries begins periodically persisting status summaries for the
// node and its stores.
func (n *Node) startWriteSummaries(frequency time.Duration) {
	// Immediately record summaries once on server startup.
	n.stopper.RunWorker(func() {
		// Write a status summary immediately; this helps the UI remain
		// responsive when new nodes are added.
		if err := n.writeSummaries(); err != nil {
			log.Warningf("error recording initial status summaries: %s", err)
		}
		ticker := time.NewTicker(frequency)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := n.writeSummaries(); err != nil {
					log.Warningf("error recording status summaries: %s", err)
				}
			case <-n.stopper.ShouldStop():
				return
			}
		}
	})
}

// writeSummaries retrieves status summaries from the supplied
// NodeStatusRecorder and persists them to the cockroach data store.
func (n *Node) writeSummaries() error {
	var err error
	n.stopper.RunTask(func() {
		nodeStatus := n.recorder.GetStatusSummary()
		if nodeStatus != nil {
			key := keys.NodeStatusKey(int32(nodeStatus.Desc.NodeID))
			// We use PutInline to store only a single version of the node
			// status. There's not much point in keeping the historical
			// versions as we keep all of the constituent data as
			// timeseries. Further, due to the size of the build info in the
			// node status, writing one of these every 10s will generate
			// more versions than will easily fit into a range over the
			// course of a day.
			if pErr := n.ctx.DB.PutInline(key, nodeStatus); pErr != nil {
				err = pErr.GoError()
				return
			}
			if log.V(1) {
				statusJSON, err := json.Marshal(nodeStatus)
				if err != nil {
					log.Errorf("error marshaling nodeStatus to json: %s", err)
				}
				log.Infof("node %d status: %s", nodeStatus.Desc.NodeID, statusJSON)
			}
		}
	})
	return err
}

// recordJoinEvent begins an asynchronous task which attempts to log a "node
// join" or "node restart" event. This query will retry until it succeeds or the
// server stops.
func (n *Node) recordJoinEvent() {
	if !n.ctx.LogRangeEvents {
		return
	}

	logEventType := sql.EventLogNodeRestart
	if n.initialBoot {
		logEventType = sql.EventLogNodeJoin
	}

	n.stopper.RunWorker(func() {
		for r := retry.Start(retry.Options{Closer: n.stopper.ShouldStop()}); r.Next(); {
			if err := n.ctx.DB.Txn(func(txn *client.Txn) *roachpb.Error {
				return sql.MakeEventLogger(n.ctx.SQLExecutor.LeaseManager).InsertEventRecord(txn,
					logEventType,
					int32(n.Descriptor.NodeID),
					int32(n.Descriptor.NodeID),
					struct {
						Descriptor roachpb.NodeDescriptor
						ClusterID  uuid.UUID
						StartedAt  int64
					}{n.Descriptor, n.ClusterID, n.startedAt},
				)
			}); err != nil {
				log.Warningc(n.context(context.TODO()), "unable to log %s event for node %d: %s", logEventType, n.Descriptor.NodeID, err)
			} else {
				return
			}
		}
	})
}

// Batch implements the roachpb.KVServer interface.
func (n *Node) Batch(ctx context.Context, args *roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
	// TODO(marc): this code is duplicated in kv/db.go, which should be fixed.
	// Also, grpc's authentication model (which gives credential access in the
	// request handler) doesn't really fit with the current design of the
	// security package (which assumes that TLS state is only given at connection
	// time) - that should be fixed.
	if peer, ok := peer.FromContext(ctx); ok {
		if tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo); ok {
			certUser, err := security.GetCertificateUser(&tlsInfo.State)
			if err != nil {
				return nil, err
			}
			if certUser != security.NodeUser {
				return nil, util.Errorf("user %s is not allowed", certUser)
			}
		}
	}

	var br *roachpb.BatchResponse
	opName := "node " + strconv.Itoa(int(n.Descriptor.NodeID)) // could save allocs here

	fail := func(err error) {
		br = &roachpb.BatchResponse{}
		br.Error = roachpb.NewError(err)
	}

	f := func() {
		sp, err := tracing.JoinOrNew(n.ctx.Tracer, args.Trace, opName)
		if err != nil {
			fail(err)
			return
		}
		// If this is a snowball span, it gets special treatment: It skips the
		// regular tracing machinery, and we instead send the collected spans
		// back with the response. This is more expensive, but then again,
		// those are individual requests traced by users, so they can be.
		if sp.BaggageItem(tracing.Snowball) != "" {
			sp.LogEvent("delegating to snowball tracing")
			sp.Finish()
			if sp, err = tracing.JoinOrNewSnowball(opName, args.Trace, func(rawSpan basictracer.RawSpan) {
				encSp, err := tracing.EncodeRawSpan(&rawSpan, nil)
				if err != nil {
					log.Warning(err)
				}
				br.CollectedSpans = append(br.CollectedSpans, encSp)
			}); err != nil {
				fail(err)
				return
			}
		}
		defer sp.Finish()
		traceCtx := opentracing.ContextWithSpan(n.context(ctx), sp)

		tStart := timeutil.Now()
		var pErr *roachpb.Error
		br, pErr = n.stores.Send(traceCtx, *args)
		if pErr != nil {
			br = &roachpb.BatchResponse{}
			log.Trace(traceCtx, fmt.Sprintf("error: %T", pErr.GetDetail()))
		}
		if br.Error != nil {
			panic(roachpb.ErrorUnexpectedlySet(n.stores, br))
		}
		n.metrics.callComplete(timeutil.Now().Sub(tStart), pErr)
		br.Error = pErr
	}

	if !n.stopper.RunTask(f) {
		return nil, util.Errorf("node %d stopped", n.Descriptor.NodeID)
	}
	return br, nil
}
