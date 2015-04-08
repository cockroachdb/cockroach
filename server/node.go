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

package server

import (
	"container/list"
	"net"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// gossipGroupLimit is the size limit for gossip groups with storage
	// topics.
	gossipGroupLimit = 100
	// gossipInterval is the interval for gossiping storage-related info.
	gossipInterval = 1 * time.Minute
	// ttlNodeIDGossip is time-to-live for node ID -> address.
	ttlNodeIDGossip = 0 * time.Second
)

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
	ClusterID     string                 // UUID for Cockroach cluster
	Descriptor    storage.NodeDescriptor // Node ID, network/physical topology
	storeConfig   storage.StoreConfig    // Store/Raft configuration.
	gossip        *gossip.Gossip         // Nodes gossip cluster ID, node ID -> host:port
	db            *client.KV             // KV DB client; used to access global id generators
	raftTransport multiraft.Transport
	lSender       *kv.LocalSender // Local KV sender for access to node-local stores
	closer        chan struct{}
}

// allocateNodeID increments the node id generator key to allocate
// a new, unique node id.
func allocateNodeID(db *client.KV) (proto.NodeID, error) {
	iReply := &proto.IncrementResponse{}
	if err := db.Call(proto.Increment, &proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:  engine.KeyNodeIDGenerator,
			User: storage.UserRoot,
		},
		Increment: 1,
	}, iReply); err != nil {
		return 0, util.Errorf("unable to allocate node ID: %v", err)
	}
	return proto.NodeID(iReply.NewValue), nil
}

// allocateStoreIDs increments the store id generator key for the
// specified node to allocate "inc" new, unique store ids. The
// first ID in a contiguous range is returned on success.
func allocateStoreIDs(nodeID proto.NodeID, inc int64, db *client.KV) (proto.StoreID, error) {
	iReply := &proto.IncrementResponse{}
	if err := db.Call(proto.Increment, &proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:  engine.MakeKey(engine.KeyStoreIDGeneratorPrefix, []byte(strconv.Itoa(int(nodeID)))),
			User: storage.UserRoot,
		},
		Increment: inc,
	}, iReply); err != nil {
		return 0, util.Errorf("unable to allocate %d store IDs for node %d: %v", inc, nodeID, err)
	}
	return proto.StoreID(iReply.NewValue - inc + 1), nil
}

// BootstrapCluster bootstraps a store using the provided engine and
// cluster ID. The bootstrapped store contains a single range spanning
// all keys. Initial range lookup metadata is populated for the range.
//
// Returns a KV client for unittest purposes. Caller should close
// the returned client.
func BootstrapCluster(clusterID string, eng engine.Engine) (*client.KV, error) {
	sIdent := proto.StoreIdent{
		ClusterID: clusterID,
		NodeID:    1,
		StoreID:   1,
	}
	clock := hlc.NewClock(hlc.UnixNano)
	// Create a KV DB with a local sender.
	lSender := kv.NewLocalSender()
	localDB := client.NewKV(nil, kv.NewTxnCoordSender(lSender, clock, false))
	// TODO(bdarnell): arrange to have the transport closed.
	// The bootstrapping store will not connect to other nodes so its StoreConfig
	// doesn't really matter.
	s := storage.NewStore(clock, eng, localDB, nil, multiraft.NewLocalRPCTransport(),
		storage.StoreConfig{})

	// Verify the store isn't already part of a cluster.
	if len(s.Ident.ClusterID) > 0 {
		return nil, util.Errorf("storage engine already belongs to a cluster (%s)", s.Ident.ClusterID)
	}

	// Bootstrap store to persist the store ident.
	if err := s.Bootstrap(sIdent); err != nil {
		return nil, err
	}
	// Create first range.
	if err := s.BootstrapRange(); err != nil {
		return nil, err
	}
	if err := s.Start(); err != nil {
		return nil, err
	}
	lSender.AddStore(s)

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

// NewNode returns a new instance of Node.
func NewNode(db *client.KV, gossip *gossip.Gossip, storeConfig storage.StoreConfig,
	raftTransport multiraft.Transport) *Node {
	return &Node{
		storeConfig:   storeConfig,
		gossip:        gossip,
		db:            db,
		raftTransport: raftTransport,
		lSender:       kv.NewLocalSender(),
		closer:        make(chan struct{}),
	}
}

// initDescriptor initializes the node descriptor with the server
// address and the node attributes.
func (n *Node) initDescriptor(addr net.Addr, attrs proto.Attributes) {
	n.Descriptor = storage.NodeDescriptor{
		// NodeID is set after invocation of start()
		Address: addr,
		Attrs:   attrs,
	}
}

// start starts the node by registering the storage instance for the
// RPC service "Node" and initializing stores for each specified
// engine. Launches periodic store gossiping in a goroutine.
func (n *Node) start(rpcServer *rpc.Server, clock *hlc.Clock,
	engines []engine.Engine, attrs proto.Attributes) error {
	n.initDescriptor(rpcServer.Addr(), attrs)
	if err := rpcServer.RegisterName("Node", n); err != nil {
		log.Fatalf("unable to register node service with RPC server: %s", err)
	}

	// Initialize stores, including bootstrapping new ones.
	if err := n.initStores(clock, engines); err != nil {
		return err
	}
	go n.startGossip()
	log.Infof("Started node with %v engine(s) and attributes %v", engines, attrs.Attrs)
	return nil
}

// stop cleanly stops the node.
func (n *Node) stop() {
	close(n.closer)
	n.lSender.Close()
}

// initStores initializes the Stores map from id to Store. Stores are
// added to the local sender if already bootstrapped. A bootstrapped
// Store has a valid ident with cluster, node and Store IDs set. If
// the Store doesn't yet have a valid ident, it's added to the
// bootstraps list for initialization once the cluster and node IDs
// have been determined.
func (n *Node) initStores(clock *hlc.Clock, engines []engine.Engine) error {
	bootstraps := list.New()

	if len(engines) == 0 {
		return util.Error("no engines")
	}
	for _, e := range engines {
		// TODO(bdarnell): make StoreConfig configurable.
		s := storage.NewStore(clock, e, n.db, n.gossip, n.raftTransport, n.storeConfig)
		// Initialize each store in turn, handling un-bootstrapped errors by
		// adding the store to the bootstraps list.
		if err := s.Start(); err != nil {
			if _, ok := err.(*storage.NotBootstrappedError); ok {
				bootstraps.PushBack(s)
				continue
			}
			return err
		}
		if s.Ident.ClusterID == "" || s.Ident.NodeID == 0 {
			return util.Errorf("unidentified store: %s", s)
		}
		capacity, err := s.Capacity()
		if err != nil {
			return err
		}
		log.Infof("initialized store %s: %+v", s, capacity)
		n.lSender.AddStore(s)
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
	return n.lSender.VisitStores(func(s *storage.Store) error {
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
		n.Descriptor.NodeID, err = allocateNodeID(n.db)
		log.Infof("new node allocated ID %d", n.Descriptor.NodeID)
		if err != nil {
			log.Fatal(err)
		}
		// Gossip node address keyed by node ID.
		nodeIDKey := gossip.MakeNodeIDKey(n.Descriptor.NodeID)
		if err := n.gossip.AddInfo(nodeIDKey, &n.Descriptor, ttlNodeIDGossip); err != nil {
			log.Errorf("couldn't gossip address for node %d: %v", n.Descriptor.NodeID, err)
		}
	}

	// Bootstrap all waiting stores by allocating a new store id for
	// each and invoking store.Bootstrap() to persist.
	inc := int64(bootstraps.Len())
	firstID, err := allocateStoreIDs(n.Descriptor.NodeID, inc, n.db)
	if err != nil {
		log.Fatal(err)
	}
	sIdent := proto.StoreIdent{
		ClusterID: n.ClusterID,
		NodeID:    n.Descriptor.NodeID,
		StoreID:   firstID,
	}
	for e := bootstraps.Front(); e != nil; e = e.Next() {
		s := e.Value.(*storage.Store)
		s.Bootstrap(sIdent)
		n.lSender.AddStore(s)
		sIdent.StoreID++
		log.Infof("bootstrapped store %s", s)
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
		nodeIDKey := gossip.MakeNodeIDKey(n.Descriptor.NodeID)
		if err := n.gossip.AddInfo(nodeIDKey, &n.Descriptor, ttlNodeIDGossip); err != nil {
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
	n.lSender.VisitStores(func(s *storage.Store) error {
		s.GossipCapacity(&n.Descriptor)
		return nil
	})
}

// executeCmd creates a client.Call struct and sends if via our local sender.
func (n *Node) executeCmd(method string, args proto.Request, reply proto.Response) error {
	call := &client.Call{
		Method: method,
		Args:   args,
		Reply:  reply,
	}
	n.lSender.Send(call)
	return nil
}

// TODO(spencer): fill in method comments below.

// Contains .
func (n *Node) Contains(args *proto.ContainsRequest, reply *proto.ContainsResponse) error {
	return n.executeCmd(proto.Contains, args, reply)
}

// Get .
func (n *Node) Get(args *proto.GetRequest, reply *proto.GetResponse) error {
	return n.executeCmd(proto.Get, args, reply)
}

// Put .
func (n *Node) Put(args *proto.PutRequest, reply *proto.PutResponse) error {
	return n.executeCmd(proto.Put, args, reply)
}

// ConditionalPut .
func (n *Node) ConditionalPut(args *proto.ConditionalPutRequest, reply *proto.ConditionalPutResponse) error {
	return n.executeCmd(proto.ConditionalPut, args, reply)
}

// Increment .
func (n *Node) Increment(args *proto.IncrementRequest, reply *proto.IncrementResponse) error {
	return n.executeCmd(proto.Increment, args, reply)
}

// Delete .
func (n *Node) Delete(args *proto.DeleteRequest, reply *proto.DeleteResponse) error {
	return n.executeCmd(proto.Delete, args, reply)
}

// DeleteRange .
func (n *Node) DeleteRange(args *proto.DeleteRangeRequest, reply *proto.DeleteRangeResponse) error {
	return n.executeCmd(proto.DeleteRange, args, reply)
}

// Scan .
func (n *Node) Scan(args *proto.ScanRequest, reply *proto.ScanResponse) error {
	return n.executeCmd(proto.Scan, args, reply)
}

// EndTransaction .
func (n *Node) EndTransaction(args *proto.EndTransactionRequest, reply *proto.EndTransactionResponse) error {
	return n.executeCmd(proto.EndTransaction, args, reply)
}

// ReapQueue .
func (n *Node) ReapQueue(args *proto.ReapQueueRequest, reply *proto.ReapQueueResponse) error {
	return n.executeCmd(proto.ReapQueue, args, reply)
}

// EnqueueUpdate .
func (n *Node) EnqueueUpdate(args *proto.EnqueueUpdateRequest, reply *proto.EnqueueUpdateResponse) error {
	return n.executeCmd(proto.EnqueueUpdate, args, reply)
}

// EnqueueMessage .
func (n *Node) EnqueueMessage(args *proto.EnqueueMessageRequest, reply *proto.EnqueueMessageResponse) error {
	return n.executeCmd(proto.EnqueueMessage, args, reply)
}

// AdminSplit .
func (n *Node) AdminSplit(args *proto.AdminSplitRequest, reply *proto.AdminSplitResponse) error {
	return n.executeCmd(proto.AdminSplit, args, reply)
}

// AdminMerge .
func (n *Node) AdminMerge(args *proto.AdminMergeRequest, reply *proto.AdminMergeResponse) error {
	return n.executeCmd(proto.AdminMerge, args, reply)
}

// InternalRangeLookup .
func (n *Node) InternalRangeLookup(args *proto.InternalRangeLookupRequest, reply *proto.InternalRangeLookupResponse) error {
	return n.executeCmd(proto.InternalRangeLookup, args, reply)
}

// InternalHeartbeatTxn .
func (n *Node) InternalHeartbeatTxn(args *proto.InternalHeartbeatTxnRequest, reply *proto.InternalHeartbeatTxnResponse) error {
	return n.executeCmd(proto.InternalHeartbeatTxn, args, reply)
}

// InternalGC .
func (n *Node) InternalGC(args *proto.InternalGCRequest, reply *proto.InternalGCResponse) error {
	return n.executeCmd(proto.InternalGC, args, reply)
}

// InternalPushTxn .
func (n *Node) InternalPushTxn(args *proto.InternalPushTxnRequest, reply *proto.InternalPushTxnResponse) error {
	return n.executeCmd(proto.InternalPushTxn, args, reply)
}

// InternalResolveIntent .
func (n *Node) InternalResolveIntent(args *proto.InternalResolveIntentRequest, reply *proto.InternalResolveIntentResponse) error {
	return n.executeCmd(proto.InternalResolveIntent, args, reply)
}

// InternalMerge .
func (n *Node) InternalMerge(args *proto.InternalMergeRequest, reply *proto.InternalMergeResponse) error {
	return n.executeCmd(proto.InternalMerge, args, reply)
}

// InternalTruncateLog .
func (n *Node) InternalTruncateLog(args *proto.InternalTruncateLogRequest, reply *proto.InternalTruncateLogResponse) error {
	return n.executeCmd(proto.InternalTruncateLog, args, reply)
}
