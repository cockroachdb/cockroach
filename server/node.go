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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"container/list"
	"net"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// gossipGroupLimit is the size limit for gossip groups with storage
	// topics.
	gossipGroupLimit = 100
	// gossipInterval is the interval for gossiping storage-related info.
	gossipInterval = 1 * time.Minute
	// ttlCapacityGossip is time-to-live for capacity-related info.
	ttlCapacityGossip = 2 * time.Minute
	// ttlNodeIDGossip is time-to-live for node ID -> address.
	ttlNodeIDGossip = 0 * time.Second
)

// A Node manages a map of stores (by store ID) for which it serves
// traffic. A node is the top-level data structure. There is one node
// instance per process. A node accepts incoming RPCs and services
// them by directing the commands contained within RPCs to local
// stores, which in turn direct the commands to specific ranges.  Each
// node has access to the global, monolithic Key-Value abstraction via
// its "distDB" reference. Nodes use this to allocate node and store
// IDs for bootstrapping the node itself or new stores as they're added
// on subsequent instantiations.
type Node struct {
	ClusterID  string                 // UUID for Cockroach cluster
	Descriptor storage.NodeDescriptor // Node ID, network/physical topology
	gossip     *gossip.Gossip         // Nodes gossip cluster ID, node ID -> host:port
	distDB     kv.DB                  // Global KV DB; used to access global id generators
	localDB    *kv.LocalDB            // Local KV DB for access to node-local stores
	closer     chan struct{}

	maxAvailPrefix string // Prefix for max avail capacity gossip topic
}

// allocateNodeID increments the node id generator key to allocate
// a new, unique node id.
func allocateNodeID(db kv.DB) (int32, error) {
	ir := <-db.Increment(&storage.IncrementRequest{
		RequestHeader: storage.RequestHeader{
			Key:  engine.KeyNodeIDGenerator,
			User: storage.UserRoot,
		},
		Increment: 1,
	})
	if ir.Error != nil {
		return 0, util.Errorf("unable to allocate node ID: %v", ir.Error)
	}
	return int32(ir.NewValue), nil
}

// allocateStoreIDs increments the store id generator key for the
// specified node to allocate "inc" new, unique store ids. The
// first ID in a contiguous range is returned on success.
func allocateStoreIDs(nodeID int32, inc int64, db kv.DB) (int32, error) {
	ir := <-db.Increment(&storage.IncrementRequest{
		// The Key is a concatenation of StoreIDGeneratorPrefix and this node's ID.
		RequestHeader: storage.RequestHeader{
			Key:  engine.MakeKey(engine.KeyStoreIDGeneratorPrefix, []byte(strconv.Itoa(int(nodeID)))),
			User: storage.UserRoot,
		},
		Increment: inc,
	})
	if ir.Error != nil {
		return 0, util.Errorf("unable to allocate %d store IDs for node %d: %v", inc, nodeID, ir.Error)
	}
	return int32(ir.NewValue - inc + 1), nil
}

// BootstrapCluster bootstraps a store using the provided engine and
// cluster ID. The bootstrapped store contains a single range spanning
// all keys. Initial range lookup metadata is populated for the range.
//
// Returns a direct-access kv.LocalDB for unittest purposes only.
func BootstrapCluster(clusterID string, eng engine.Engine) (
	*kv.LocalDB, error) {
	sIdent := storage.StoreIdent{
		ClusterID: clusterID,
		NodeID:    1,
		StoreID:   1,
	}
	clock := hlc.NewClock(hlc.UnixNano)
	now := clock.Now()
	s := storage.NewStore(clock, eng, nil)

	// Verify the store isn't already part of a cluster.
	if s.Ident.ClusterID != "" {
		return nil, util.Errorf("storage engine already belongs to a cluster (%s)", s.Ident.ClusterID)
	}

	// Bootstrap store to persist the store ident.
	if err := s.Bootstrap(sIdent); err != nil {
		return nil, err
	}

	if err := s.Init(); err != nil {
		return nil, err
	}

	// Create first range.
	replica := storage.Replica{
		NodeID:  1,
		StoreID: 1,
		RangeID: 1,
		Attrs:   engine.Attributes{},
	}
	rng, err := s.CreateRange(engine.KeyMin, engine.KeyMax, []storage.Replica{replica})
	if err != nil {
		return nil, err
	}
	if rng.Meta.RangeID != 1 {
		return nil, util.Errorf("expected range id of 1, got %d", rng.Meta.RangeID)
	}

	// Create a local DB to directly modify the new range.
	localDB := kv.NewLocalDB()
	localDB.AddStore(s)

	// Initialize range addressing records and default administrative configs.
	desc := storage.RangeDescriptor{
		StartKey: engine.KeyMin,
		Replicas: []storage.Replica{replica},
	}
	if err := kv.BootstrapRangeDescriptor(localDB, desc, now); err != nil {
		return nil, err
	}

	// Write default configs to local DB.
	if err := kv.BootstrapConfigs(localDB, now); err != nil {
		return nil, err
	}

	// Initialize node and store ids after the fact to account
	// for use of node ID = 1 and store ID = 1.
	if nodeID, err := allocateNodeID(localDB); nodeID != sIdent.NodeID || err != nil {
		return nil, util.Errorf("expected to intialize node id allocator to %d, got %d: %v",
			sIdent.NodeID, nodeID, err)
	}
	if storeID, err := allocateStoreIDs(sIdent.NodeID, 1, localDB); storeID != sIdent.StoreID || err != nil {
		return nil, util.Errorf("expected to intialize store id allocator to %d, got %d: %v",
			sIdent.StoreID, storeID, err)
	}

	return localDB, nil
}

// NewNode returns a new instance of Node, interpreting command line
// flags to initialize the appropriate Store or set of
// Stores. Registers the storage instance for the RPC service "Node".
func NewNode(distDB kv.DB, gossip *gossip.Gossip) *Node {
	n := &Node{
		gossip:  gossip,
		distDB:  distDB,
		localDB: kv.NewLocalDB(),
		closer:  make(chan struct{}),
	}
	return n
}

// initDescriptor initializes the physical/network topology attributes
// if possible. Datacenter, PDU & Rack values are taken from environment
// variables or command line flags.
func (n *Node) initDescriptor(addr net.Addr, attrs engine.Attributes) {
	n.Descriptor = storage.NodeDescriptor{
		// NodeID is after invocation of start()
		Address: addr,
		Attrs:   attrs,
	}
}

// start starts the node by initializing network/physical topology
// attributes gleaned from the environment and initializing stores
// for each specified engine. Launches periodic store gossipping
// in a goroutine.
func (n *Node) start(rpcServer *rpc.Server, clock *hlc.Clock,
	engines []engine.Engine, attrs engine.Attributes) error {
	n.initDescriptor(rpcServer.Addr(), attrs)
	rpcServer.RegisterName("Node", n)

	if err := n.initStores(clock, engines); err != nil {
		return err
	}
	go n.startGossip()

	return nil
}

// stop cleanly stops the node.
func (n *Node) stop() {
	close(n.closer)
	n.localDB.Close()
}

// initStores initializes the Stores map from id to Store. Stores are
// added to the localDB if already bootstrapped. A bootstrapped Store
// has a valid ident with cluster, node and Store IDs set. If the
// Store doesn't yet have a valid ident, it's added to the bootstraps
// list for initialization once the cluster and node IDs have been
// determined.
func (n *Node) initStores(clock *hlc.Clock, engines []engine.Engine) error {
	bootstraps := list.New()

	for _, e := range engines {
		s := storage.NewStore(clock, e, n.gossip)
		// If not bootstrapped, add to list.
		if !s.IsBootstrapped() {
			bootstraps.PushBack(s)
			continue
		}
		// Otherwise, initialize each store in turn.
		if err := s.Init(); err != nil {
			return err
		}
		if s.Ident.ClusterID != "" {
			if s.Ident.StoreID == 0 {
				return util.Error("cluster id set for node ident but missing store id")
			}
			capacity, err := s.Capacity()
			if err != nil {
				return err
			}
			log.Infof("initialized store %s: %+v", s, capacity)
			n.localDB.AddStore(s)
		}
	}

	// Verify all initialized stores agree on cluster and node IDs.
	if err := n.validateStores(); err != nil {
		return err
	}

	// Connect gossip before starting bootstrap. For new nodes, connecting
	// to the gossip network is necessary to get the cluster ID.
	n.connectGossip()

	// Bootstrap any uninitialized stores asynchronously.
	if bootstraps.Len() > 0 {
		go n.bootstrapStores(bootstraps)
	}

	return nil
}

// validateStores iterates over all stores, verifying they agree on
// cluster ID and node ID. The node's ident is initialized based on
// the agreed-upon cluster and node IDs.
func (n *Node) validateStores() error {
	return n.localDB.VisitStores(func(s *storage.Store) error {
		if s.Ident.ClusterID == "" || s.Ident.NodeID == 0 {
			return util.Errorf("unidentified store in store map: %s", s)
		}
		if n.ClusterID == "" {
			n.ClusterID = s.Ident.ClusterID
			n.Descriptor.NodeID = s.Ident.NodeID
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
func (n *Node) bootstrapStores(bootstraps *list.List) {
	log.Infof("bootstrapping %d store(s)", bootstraps.Len())

	// Allocate a new node ID if necessary.
	if n.Descriptor.NodeID == 0 {
		var err error
		n.Descriptor.NodeID, err = allocateNodeID(n.distDB)
		log.Infof("new node allocated ID %d", n.Descriptor.NodeID)
		if err != nil {
			log.Fatal(err)
		}
		// Gossip node address keyed by node ID.
		nodeIDKey := gossip.MakeNodeIDGossipKey(n.Descriptor.NodeID)
		if err := n.gossip.AddInfo(nodeIDKey, n.Descriptor.Address, ttlNodeIDGossip); err != nil {
			log.Errorf("couldn't gossip address for node %d: %v", n.Descriptor.NodeID, err)
		}
	}

	// Bootstrap all waiting stores by allocating a new store id for
	// each and invoking store.Bootstrap() to persist.
	inc := int64(bootstraps.Len())
	firstID, err := allocateStoreIDs(n.Descriptor.NodeID, inc, n.distDB)
	if err != nil {
		log.Fatal(err)
	}
	sIdent := storage.StoreIdent{
		ClusterID: n.ClusterID,
		NodeID:    n.Descriptor.NodeID,
		StoreID:   firstID,
	}
	for e := bootstraps.Front(); e != nil; e = e.Next() {
		s := e.Value.(*storage.Store)
		s.Bootstrap(sIdent)
		n.localDB.AddStore(s)
		sIdent.StoreID++
		log.Infof("bootstrapped store %s", s)
	}
}

// connectGossip connects to gossip network and reads cluster ID. If
// this node is already part of a cluster, the cluster ID is verified
// for a match. If not part of a cluster, the cluster ID is set. The
// node's address is gossipped with node ID as the gossip key.
func (n *Node) connectGossip() {
	log.Infof("connecting to gossip network to verify cluster ID...")
	<-n.gossip.Connected

	val, err := n.gossip.GetInfo(gossip.KeyClusterID)
	if err != nil || val == nil {
		log.Fatalf("unable to ascertain cluster ID from gossip network: %v", err)
	}
	gossipClusterID := val.(string)

	if n.ClusterID == "" {
		n.ClusterID = gossipClusterID
	} else if n.ClusterID != gossipClusterID {
		log.Fatalf("node %d belongs to cluster %q but is attempting to connect to a gossip network for cluster %q",
			n.Descriptor.NodeID, n.ClusterID, gossipClusterID)
	}
	log.Infof("node connected via gossip and verified as part of cluster %q", gossipClusterID)

	// Gossip node address keyed by node ID.
	if n.Descriptor.NodeID != 0 {
		nodeIDKey := gossip.MakeNodeIDGossipKey(n.Descriptor.NodeID)
		if err := n.gossip.AddInfo(nodeIDKey, n.Descriptor.Address, ttlNodeIDGossip); err != nil {
			log.Errorf("couldn't gossip address for node %d: %v", n.Descriptor.NodeID, err)
		}
	}
}

// startGossip loops on a periodic ticker to gossip node-related
// information. Loops until the node is closed and should be
// invoked via goroutine.
func (n *Node) startGossip() {
	ticker := time.NewTicker(gossipInterval)
	for {
		select {
		case <-ticker.C:
			n.gossipCapacities()
		case <-n.closer:
			ticker.Stop()
			return
		}
	}
}

// gossipCapacities calls capacity on each store and adds it to the
// gossip network.
func (n *Node) gossipCapacities() {
	n.localDB.VisitStores(func(s *storage.Store) error {
		storeDesc, err := s.Descriptor(&n.Descriptor)
		if err != nil {
			log.Warningf("problem getting store descriptor for store %+v: %v", s.Ident, err)
			return nil
		}
		gossipPrefix := gossip.KeyMaxAvailCapacityPrefix + storeDesc.CombinedAttrs().SortedString()
		keyMaxCapacity := gossipPrefix + strconv.FormatInt(int64(storeDesc.Node.NodeID), 10) + "-" +
			strconv.FormatInt(int64(storeDesc.StoreID), 10)
		// Register gossip group.
		n.gossip.RegisterGroup(gossipPrefix, gossipGroupLimit, gossip.MaxGroup)
		// Gossip store descriptor.
		n.gossip.AddInfo(keyMaxCapacity, *storeDesc, ttlCapacityGossip)
		return nil
	})
}

// executeCmd looks up the store specified by header.Replica, and runs
// Store.ExecuteCmd.
func (n *Node) executeCmd(method string, args storage.Request, reply storage.Response) error {
	store, err := n.localDB.GetStore(&args.Header().Replica)
	if err != nil {
		return err
	}
	store.ExecuteCmd(method, args, reply)
	return nil
}

// Contains .
func (n *Node) Contains(args *storage.ContainsRequest, reply *storage.ContainsResponse) error {
	return n.executeCmd(storage.Contains, args, reply)
}

// Get .
func (n *Node) Get(args *storage.GetRequest, reply *storage.GetResponse) error {
	return n.executeCmd(storage.Get, args, reply)
}

// Put .
func (n *Node) Put(args *storage.PutRequest, reply *storage.PutResponse) error {
	return n.executeCmd(storage.Put, args, reply)
}

// ConditionalPut .
func (n *Node) ConditionalPut(args *storage.ConditionalPutRequest, reply *storage.ConditionalPutResponse) error {
	return n.executeCmd(storage.ConditionalPut, args, reply)
}

// Increment .
func (n *Node) Increment(args *storage.IncrementRequest, reply *storage.IncrementResponse) error {
	return n.executeCmd(storage.Increment, args, reply)
}

// Delete .
func (n *Node) Delete(args *storage.DeleteRequest, reply *storage.DeleteResponse) error {
	return n.executeCmd(storage.Delete, args, reply)
}

// DeleteRange .
func (n *Node) DeleteRange(args *storage.DeleteRangeRequest, reply *storage.DeleteRangeResponse) error {
	return n.executeCmd(storage.DeleteRange, args, reply)
}

// Scan .
func (n *Node) Scan(args *storage.ScanRequest, reply *storage.ScanResponse) error {
	return n.executeCmd(storage.Scan, args, reply)
}

// EndTransaction .
func (n *Node) EndTransaction(args *storage.EndTransactionRequest, reply *storage.EndTransactionResponse) error {
	return n.executeCmd(storage.EndTransaction, args, reply)
}

// AccumulateTS .
func (n *Node) AccumulateTS(args *storage.AccumulateTSRequest, reply *storage.AccumulateTSResponse) error {
	return n.executeCmd(storage.AccumulateTS, args, reply)
}

// ReapQueue .
func (n *Node) ReapQueue(args *storage.ReapQueueRequest, reply *storage.ReapQueueResponse) error {
	return n.executeCmd(storage.ReapQueue, args, reply)
}

// EnqueueUpdate .
func (n *Node) EnqueueUpdate(args *storage.EnqueueUpdateRequest, reply *storage.EnqueueUpdateResponse) error {
	return n.executeCmd(storage.EnqueueUpdate, args, reply)
}

// EnqueueMessage .
func (n *Node) EnqueueMessage(args *storage.EnqueueMessageRequest, reply *storage.EnqueueMessageResponse) error {
	return n.executeCmd(storage.EnqueueMessage, args, reply)
}

// InternalRangeLookup .
func (n *Node) InternalRangeLookup(args *storage.InternalRangeLookupRequest, reply *storage.InternalRangeLookupResponse) error {
	return n.executeCmd(storage.InternalRangeLookup, args, reply)
}
