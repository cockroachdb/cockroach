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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
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

// Node manages a map of stores (by store ID) for which it serves traffic.
type Node struct {
	ClusterID  string                 // UUID for Cockroach cluster
	Attributes storage.NodeAttributes // Node ID, network/physical topology
	gossip     *gossip.Gossip         // Nodes gossip cluster ID, node ID -> host:port
	kvDB       kv.DB                  // Used to access global id generators
	closer     chan struct{}

	mu       sync.RWMutex             // Protects storeMap during bootstrapping
	storeMap map[int32]*storage.Store // Map from StoreID to Store

	maxAvailPrefix string // Prefix for max avail capacity gossip topic
}

// allocateNodeID increments the node id generator key to allocate
// a new, unique node id.
func allocateNodeID(db kv.DB) (int32, error) {
	ir := <-db.Increment(&storage.IncrementRequest{
		Key:       storage.KeyNodeIDGenerator,
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
		Key: storage.MakeKey(storage.KeyStoreIDGeneratorPrefix,
			[]byte(strconv.Itoa(int(nodeID)))),
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
func BootstrapCluster(clusterID string, engine storage.Engine) (*kv.LocalDB, error) {
	sIdent := storage.StoreIdent{
		ClusterID: clusterID,
		NodeID:    1,
		StoreID:   1,
	}
	s := storage.NewStore(engine, nil)
	defer s.Close()

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
		NodeID:     1,
		StoreID:    1,
		RangeID:    1,
		Datacenter: getDatacenter(),
		DiskType:   engine.Type(),
	}
	rng, err := s.CreateRange(storage.KeyMin, storage.KeyMax, []storage.Replica{replica})
	if err != nil {
		return nil, err
	}
	if rng.Meta.RangeID != 1 {
		return nil, util.Errorf("expected range id of 1, got %d", rng.Meta.RangeID)
	}

	// Create a local DB to directly modify the new range.
	localDB := kv.NewLocalDB(rng)

	// Initialize range addressing records and default administrative configs.
	if err := kv.BootstrapRangeLocations(localDB, replica); err != nil {
		return nil, err
	}
	if err := kv.BootstrapConfigs(localDB); err != nil {
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
func NewNode(kvDB kv.DB, gossip *gossip.Gossip) *Node {
	n := &Node{
		gossip:   gossip,
		kvDB:     kvDB,
		storeMap: make(map[int32]*storage.Store),
		closer:   make(chan struct{}),
	}
	return n
}

// initAttributes initializes the physical/network topology attributes
// if possible. Datacenter, PDU & Rack values are taken from environment
// variables or command line flags.
func (n *Node) initAttributes(addr net.Addr) {
	n.Attributes = storage.NodeAttributes{
		// NodeID is after invocation of start()
		Address:    addr,
		Datacenter: getDatacenter(),
		PDU:        getPDU(),
		Rack:       getRack(),
	}
}

// start starts the node by initializing network/physical topology
// attributes gleaned from the environment and initializing stores
// for each specified engine. Launches periodic store gossipping
// in a goroutine.
func (n *Node) start(rpcServer *rpc.Server, engines []storage.Engine) error {
	n.initAttributes(rpcServer.Addr())
	rpcServer.RegisterName("Node", n)

	if err := n.initStoreMap(engines); err != nil {
		return err
	}
	go n.startGossip()

	return nil
}

// stop cleanly stops the node.
func (n *Node) stop() {
	close(n.closer)
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, store := range n.storeMap {
		store.Close()
	}
}

// initStoreMap initializes the Stores map from id to Store. Stores are
// added to the storeMap if the Store is already bootstrapped. A
// bootstrapped Store has a valid ident with cluster, node and Store
// IDs set. If the Store doesn't yet have a valid ident, it's added to
// the bootstraps list for initialization once the cluster and node
// IDs have been determined.
func (n *Node) initStoreMap(engines []storage.Engine) error {
	bootstraps := list.New()

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, engine := range engines {
		s := storage.NewStore(engine, n.gossip)
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
			glog.Infof("initialized store %s: %+v", s, capacity)
			n.storeMap[s.Ident.StoreID] = s
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
	for _, s := range n.storeMap {
		if s.Ident.ClusterID == "" || s.Ident.NodeID == 0 {
			return util.Errorf("unidentified store in store map: %s", s)
		}
		if n.ClusterID == "" {
			n.ClusterID = s.Ident.ClusterID
			n.Attributes.NodeID = s.Ident.NodeID
		} else if n.ClusterID != s.Ident.ClusterID {
			return util.Errorf("store %s cluster ID doesn't match node cluster %q", s, n.ClusterID)
		} else if n.Attributes.NodeID != s.Ident.NodeID {
			return util.Errorf("store %s node ID doesn't match node ID: %d", s, n.Attributes.NodeID)
		}
	}
	return nil
}

// bootstrapStores bootstraps uninitialized stores once the cluster
// and node IDs have been established for this node. Store IDs are
// allocated via a sequence id generator stored at a system key per
// node.
func (n *Node) bootstrapStores(bootstraps *list.List) {
	glog.Infof("bootstrapping %d store(s)", bootstraps.Len())

	// Allocate a new node ID if necessary.
	if n.Attributes.NodeID == 0 {
		var err error
		n.Attributes.NodeID, err = allocateNodeID(n.kvDB)
		glog.Infof("new node allocated ID %d", n.Attributes.NodeID)
		if err != nil {
			glog.Fatal(err)
		}
		// Gossip node address keyed by node ID.
		nodeIDKey := gossip.MakeNodeIDGossipKey(n.Attributes.NodeID)
		if err := n.gossip.AddInfo(nodeIDKey, n.Attributes.Address, ttlNodeIDGossip); err != nil {
			glog.Errorf("couldn't gossip address for node %d: %v", n.Attributes.NodeID, err)
		}
	}

	// Bootstrap all waiting stores by allocating a new store id for
	// each and invoking store.Bootstrap() to persist.
	inc := int64(bootstraps.Len())
	firstID, err := allocateStoreIDs(n.Attributes.NodeID, inc, n.kvDB)
	if err != nil {
		glog.Fatal(err)
	}
	sIdent := storage.StoreIdent{
		ClusterID: n.ClusterID,
		NodeID:    n.Attributes.NodeID,
		StoreID:   firstID,
	}
	for e := bootstraps.Front(); e != nil; e = e.Next() {
		s := e.Value.(*storage.Store)
		s.Bootstrap(sIdent)
		n.mu.Lock()
		n.storeMap[s.Ident.StoreID] = s
		n.mu.Unlock()
		sIdent.StoreID++
		glog.Infof("bootstrapped store %s", s)
	}
}

// connectGossip connects to gossip network and reads cluster ID. If
// this node is already part of a cluster, the cluster ID is verified
// for a match. If not part of a cluster, the cluster ID is set. The
// node's address is gossipped with node ID as the gossip key.
func (n *Node) connectGossip() {
	glog.Infof("connecting to gossip network to verify cluster ID...")
	<-n.gossip.Connected

	val, err := n.gossip.GetInfo(gossip.KeyClusterID)
	if err != nil || val == nil {
		glog.Fatalf("unable to ascertain cluster ID from gossip network: %v", err)
	}
	gossipClusterID := val.(string)

	if n.ClusterID == "" {
		n.ClusterID = gossipClusterID
	} else if n.ClusterID != gossipClusterID {
		glog.Fatalf("node %d belongs to cluster %q but is attempting to connect to a gossip network for cluster %q",
			n.Attributes.NodeID, n.ClusterID, gossipClusterID)
	}
	glog.Infof("node connected via gossip and verified as part of cluster %q", gossipClusterID)

	// Gossip node address keyed by node ID.
	if n.Attributes.NodeID != 0 {
		nodeIDKey := gossip.MakeNodeIDGossipKey(n.Attributes.NodeID)
		if err := n.gossip.AddInfo(nodeIDKey, n.Attributes.Address, ttlNodeIDGossip); err != nil {
			glog.Errorf("couldn't gossip address for node %d: %v", n.Attributes.NodeID, err)
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
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Register gossip groups.
	n.maxAvailPrefix = gossip.KeyMaxAvailCapacityPrefix + n.Attributes.Datacenter
	n.gossip.RegisterGroup(n.maxAvailPrefix, gossipGroupLimit, gossip.MaxGroup)

	for _, store := range n.storeMap {
		capacity, err := store.Capacity()
		if err != nil {
			glog.Warningf("problem getting capacity: %v", err)
			continue
		}

		keyMaxCapacity := n.maxAvailPrefix + strconv.FormatInt(int64(n.Attributes.NodeID), 16) + "-" +
			strconv.FormatInt(int64(store.Ident.StoreID), 16)
		storeAttr := storage.StoreAttributes{
			StoreID:    store.Ident.StoreID,
			Attributes: n.Attributes,
			Capacity:   capacity,
		}
		n.gossip.AddInfo(keyMaxCapacity, storeAttr, ttlCapacityGossip)
	}
}

// storeCount returns the number of stores this node is exporting.
func (n *Node) getStoreCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.storeMap)
}

// getRange looks up the store by Replica.StoreID and then queries it for
// the range specified by Replica.RangeID.
func (n *Node) getRange(r *storage.Replica) (*storage.Range, error) {
	n.mu.RLock()
	store, ok := n.storeMap[r.StoreID]
	n.mu.RUnlock()
	if !ok {
		return nil, util.Errorf("store for replica %+v not found", r)
	}
	rng, err := store.GetRange(r.RangeID)
	if err != nil {
		return nil, err
	}
	return rng, nil
}

// All methods to satisfy the Node RPC service fetch the range
// based on the Replica target provided in the argument header.
// Commands are broken down into read-only and read-write and
// sent along to the range via either Range.readOnlyCmd() or
// Range.readWriteCmd().

// Contains .
func (n *Node) Contains(args *storage.ContainsRequest, reply *storage.ContainsResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return rng.ReadOnlyCmd("Contains", args, reply)
}

// Get .
func (n *Node) Get(args *storage.GetRequest, reply *storage.GetResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return rng.ReadOnlyCmd("Get", args, reply)
}

// Put .
func (n *Node) Put(args *storage.PutRequest, reply *storage.PutResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.ReadWriteCmd("Put", args, reply)
}

// Increment .
func (n *Node) Increment(args *storage.IncrementRequest, reply *storage.IncrementResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.ReadWriteCmd("Increment", args, reply)
}

// Delete .
func (n *Node) Delete(args *storage.DeleteRequest, reply *storage.DeleteResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.ReadWriteCmd("Delete", args, reply)
}

// DeleteRange .
func (n *Node) DeleteRange(args *storage.DeleteRangeRequest, reply *storage.DeleteRangeResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.ReadWriteCmd("DeleteRange", args, reply)
}

// Scan .
func (n *Node) Scan(args *storage.ScanRequest, reply *storage.ScanResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return rng.ReadOnlyCmd("Scan", args, reply)
}

// EndTransaction .
func (n *Node) EndTransaction(args *storage.EndTransactionRequest, reply *storage.EndTransactionResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.ReadWriteCmd("EndTransaction", args, reply)
}

// AccumulateTS .
func (n *Node) AccumulateTS(args *storage.AccumulateTSRequest, reply *storage.AccumulateTSResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.ReadWriteCmd("AccumulateTS", args, reply)
}

// ReapQueue .
func (n *Node) ReapQueue(args *storage.ReapQueueRequest, reply *storage.ReapQueueResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.ReadWriteCmd("ReapQueue", args, reply)
}

// EnqueueUpdate .
func (n *Node) EnqueueUpdate(args *storage.EnqueueUpdateRequest, reply *storage.EnqueueUpdateResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.ReadWriteCmd("EnqueueUpdate", args, reply)
}

// EnqueueMessage .
func (n *Node) EnqueueMessage(args *storage.EnqueueMessageRequest, reply *storage.EnqueueMessageResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.ReadWriteCmd("EnqueueMessage", args, reply)
}

// InternalRangeLookup .
func (n *Node) InternalRangeLookup(args *storage.InternalRangeLookupRequest, reply *storage.InternalRangeLookupResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return rng.ReadOnlyCmd("InternalRangeLookup", args, reply)
}
