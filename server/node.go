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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/server/status"
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
	ClusterID  string               // UUID for Cockroach cluster
	Descriptor proto.NodeDescriptor // Node ID, network/physical topology
	sCtx       storage.StoreContext // Context to use and pass to stores
	lSender    *kv.LocalSender      // Local KV sender for access to node-local stores
	status     *status.NodeStatusMonitor
	startedAt  int64
	// ScanCount is the number of times through the store scanning loop locked
	// by the completedScan mutex.
	completedScan *sync.Cond
	scanCount     int64
}

// nodeServer is a type alias to separate RPC methods
// (which net/rpc finds via reflection) from others.
type nodeServer Node

// allocateNodeID increments the node id generator key to allocate
// a new, unique node id.
func allocateNodeID(db *client.KV) (proto.NodeID, error) {
	iReply := &proto.IncrementResponse{}
	if err := db.Run(client.Call{
		Args: &proto.IncrementRequest{
			RequestHeader: proto.RequestHeader{
				Key:  engine.KeyNodeIDGenerator,
				User: storage.UserRoot,
			},
			Increment: 1,
		},
		Reply: iReply}); err != nil {
		return 0, util.Errorf("unable to allocate node ID: %s", err)
	}
	return proto.NodeID(iReply.NewValue), nil
}

// allocateStoreIDs increments the store id generator key for the
// specified node to allocate "inc" new, unique store ids. The
// first ID in a contiguous range is returned on success.
func allocateStoreIDs(nodeID proto.NodeID, inc int64, db *client.KV) (proto.StoreID, error) {
	iReply := &proto.IncrementResponse{}
	if err := db.Run(client.Call{
		Args: &proto.IncrementRequest{
			RequestHeader: proto.RequestHeader{
				Key:  engine.KeyStoreIDGenerator,
				User: storage.UserRoot,
			},
			Increment: inc,
		},
		Reply: iReply}); err != nil {
		return 0, util.Errorf("unable to allocate %d store IDs for node %d: %s", inc, nodeID, err)
	}
	return proto.StoreID(iReply.NewValue - inc + 1), nil
}

// BootstrapCluster bootstraps a multiple stores using the provided engines and
// cluster ID. The first bootstrapped store contains a single range spanning
// all keys. Initial range lookup metadata is populated for the range.
//
// Returns a KV client for unittest purposes. Caller should close the returned
// client.
func BootstrapCluster(clusterID string, engines []engine.Engine, stopper *util.Stopper) (*client.KV, error) {
	sCtx := storage.StoreContext{}
	sCtx.ScanInterval = 10 * time.Minute
	sCtx.Clock = hlc.NewClock(hlc.UnixNano)
	// Create a KV DB with a local sender.
	lSender := kv.NewLocalSender()
	localDB := client.NewKV(nil, kv.NewTxnCoordSender(lSender, sCtx.Clock, false, stopper))
	sCtx.DB = localDB
	sCtx.Transport = multiraft.NewLocalRPCTransport()
	for i, eng := range engines {
		sIdent := proto.StoreIdent{
			ClusterID: clusterID,
			NodeID:    1,
			StoreID:   proto.StoreID(i + 1),
		}

		// The bootstrapping store will not connect to other nodes so its
		// StoreConfig doesn't really matter.
		s := storage.NewStore(sCtx, eng, &proto.NodeDescriptor{NodeID: 1})

		// Verify the store isn't already part of a cluster.
		if len(s.Ident.ClusterID) > 0 {
			return nil, util.Errorf("storage engine already belongs to a cluster (%s)", s.Ident.ClusterID)
		}

		// Bootstrap store to persist the store ident.
		if err := s.Bootstrap(sIdent, stopper); err != nil {
			return nil, err
		}
		// Create first range, writing directly to engine. Note this does
		// not create the range, just its data.  Only do this if this is the
		// first store.
		if i == 0 {
			if err := s.BootstrapRange(); err != nil {
				return nil, err
			}
		}
		if err := s.Start(stopper); err != nil {
			return nil, err
		}

		lSender.AddStore(s)

		// Initialize node and store ids.  Only initialize the node once.
		if i == 0 {
			if nodeID, err := allocateNodeID(localDB); nodeID != sIdent.NodeID || err != nil {
				return nil, util.Errorf("expected to initialize node id allocator to %d, got %d: %s",
					sIdent.NodeID, nodeID, err)
			}
		}
		if storeID, err := allocateStoreIDs(sIdent.NodeID, 1, localDB); storeID != sIdent.StoreID || err != nil {
			return nil, util.Errorf("expected to initialize store id allocator to %d, got %d: %s",
				sIdent.StoreID, storeID, err)
		}
	}
	return localDB, nil
}

// NewNode returns a new instance of Node.
func NewNode(sCtx storage.StoreContext) *Node {
	return &Node{
		sCtx:          sCtx,
		status:        status.NewNodeStatusMonitor(),
		lSender:       kv.NewLocalSender(),
		completedScan: sync.NewCond(&sync.Mutex{}),
	}
}

// initDescriptor initializes the node descriptor with the server
// address and the node attributes.
func (n *Node) initDescriptor(addr net.Addr, attrs proto.Attributes) {
	n.Descriptor.Address = proto.Addr{
		Network: addr.Network(),
		Address: addr.String(),
	}
	n.Descriptor.Attrs = attrs
}

// initNodeID updates the internal NodeDescriptor with the given ID. If zero is
// supplied, a new NodeID is allocated with the first invocation. For all other
// values, the supplied ID is stored into the descriptor (unless one has been
// set previously, in which case a fatal error occurs).
//
// Upon setting a new NodeID, the descriptor is gossiped and the NodeID is
// stored into the gossip instance.
func (n *Node) initNodeID(id proto.NodeID) {
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
		id, err = allocateNodeID(n.sCtx.DB)
		if err != nil {
			log.Fatal(err)
		}
		if id == 0 {
			log.Fatal("new node allocated illegal ID 0")
		}

		log.Infof("new node allocated ID %d", n.Descriptor.NodeID)
	}
	// Gossip the node descriptor to make this node addressable by node ID.
	n.Descriptor.NodeID = id
	if err = n.sCtx.Gossip.SetNodeDescriptor(&n.Descriptor); err != nil {
		log.Fatalf("couldn't gossip descriptor for node %d: %s", n.Descriptor.NodeID, err)
	}
}

// start starts the node by registering the storage instance for the
// RPC service "Node" and initializing stores for each specified
// engine. Launches periodic store gossiping in a goroutine.
func (n *Node) start(rpcServer *rpc.Server, engines []engine.Engine,
	attrs proto.Attributes, stopper *util.Stopper) error {
	n.initDescriptor(rpcServer.Addr(), attrs)
	if err := rpcServer.RegisterName("Node", (*nodeServer)(n)); err != nil {
		log.Fatalf("unable to register node service with RPC server: %s", err)
	}

	// Start status monitor.
	n.status.StartMonitorFeed(n.sCtx.EventFeed)

	// Initialize stores, including bootstrapping new ones.
	if err := n.initStores(engines, stopper); err != nil {
		return err
	}

	n.startedAt = n.sCtx.Clock.Now().WallTime
	n.startStoresScanner(stopper)
	n.startGossip(stopper)
	log.Infof("Started node with %v engine(s) and attributes %v", engines, attrs.Attrs)
	return nil
}

// initStores initializes the Stores map from id to Store. Stores are
// added to the local sender if already bootstrapped. A bootstrapped
// Store has a valid ident with cluster, node and Store IDs set. If
// the Store doesn't yet have a valid ident, it's added to the
// bootstraps list for initialization once the cluster and node IDs
// have been determined.
func (n *Node) initStores(engines []engine.Engine, stopper *util.Stopper) error {
	bootstraps := list.New()

	if len(engines) == 0 {
		return util.Error("no engines")
	}
	for _, e := range engines {
		s := storage.NewStore(n.sCtx, e, &n.Descriptor)
		// Initialize each store in turn, handling un-bootstrapped errors by
		// adding the store to the bootstraps list.
		if err := s.Start(stopper); err != nil {
			if _, ok := err.(*storage.NotBootstrappedError); ok {
				log.Infof("store %s not bootstrapped", s)
				bootstraps.PushBack(s)
				continue
			}
			return util.Errorf("failed to start store: %s", err)
		}
		if s.Ident.ClusterID == "" || s.Ident.NodeID == 0 {
			return util.Errorf("unidentified store: %s", s)
		}
		capacity, err := s.Capacity()
		if err != nil {
			return util.Errorf("could not query store capacity: %s", err)
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

	// If no NodeID has been assigned yet, allocate a new node ID by
	// supplying 0 to initNodeID.
	if n.Descriptor.NodeID == 0 {
		n.initNodeID(0)
	}

	// Bootstrap any uninitialized stores asynchronously.
	if bootstraps.Len() > 0 {
		go n.bootstrapStores(bootstraps, stopper)
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
func (n *Node) bootstrapStores(bootstraps *list.List, stopper *util.Stopper) {
	log.Infof("bootstrapping %d store(s)", bootstraps.Len())
	if n.ClusterID == "" {
		panic("ClusterID missing during store bootstrap of auxiliary store")
	}

	// Bootstrap all waiting stores by allocating a new store id for
	// each and invoking store.Bootstrap() to persist.
	inc := int64(bootstraps.Len())
	firstID, err := allocateStoreIDs(n.Descriptor.NodeID, inc, n.sCtx.DB)
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
		if err := s.Bootstrap(sIdent, stopper); err != nil {
			log.Fatal(err)
		}
		if err := s.Start(stopper); err != nil {
			log.Fatal(err)
		}
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
	<-n.sCtx.Gossip.Connected

	val, err := n.sCtx.Gossip.GetInfo(gossip.KeyClusterID)
	if err != nil || val == nil {
		log.Fatalf("unable to ascertain cluster ID from gossip network: %s", err)
	}
	gossipClusterID := val.(string)

	if n.ClusterID == "" {
		n.ClusterID = gossipClusterID
	} else if n.ClusterID != gossipClusterID {
		log.Fatalf("node %d belongs to cluster %q but is attempting to connect to a gossip network for cluster %q",
			n.Descriptor.NodeID, n.ClusterID, gossipClusterID)
	}
	log.Infof("node connected via gossip and verified as part of cluster %q", gossipClusterID)
}

// startGossip loops on a periodic ticker to gossip node-related
// information. Starts a goroutine to loop until the node is closed.
func (n *Node) startGossip(stopper *util.Stopper) {
	stopper.RunWorker(func() {
		ticker := time.NewTicker(gossipInterval)
		for {
			select {
			case <-ticker.C:
				if stopper.StartTask() {
					n.gossipCapacities()
					stopper.FinishTask()
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// gossipCapacities calls capacity on each store and adds it to the
// gossip network.
func (n *Node) gossipCapacities() {
	n.lSender.VisitStores(func(s *storage.Store) error {
		s.GossipCapacity()
		return nil
	})
}

// startStoresScanner will walk through all the stores in the node every
// sCtx.ScanInterval and store the status in the db.
func (n *Node) startStoresScanner(stopper *util.Stopper) {
	stopper.RunWorker(func() {
		for {
			select {
			case <-time.After(n.sCtx.ScanInterval):
				if !stopper.StartTask() {
					continue
				}
				// Walk through all the stores on this node.
				rangeCount := 0
				stats := &proto.MVCCStats{}
				accessedStoreIDs := []int32{}
				n.lSender.VisitStores(func(store *storage.Store) error {
					storeStatus, err := store.GetStatus()
					if err != nil {
						log.Error(err)
						return nil
					}
					if storeStatus == nil {
						// The store scanner hasn't run on this node yet.
						return nil
					}
					accessedStoreIDs = append(accessedStoreIDs, int32(store.Ident.StoreID))
					rangeCount += int(storeStatus.RangeCount)
					stats.Add(&storeStatus.Stats)
					return nil
				})

				// Store the combined stats in the db.
				now := n.sCtx.Clock.Now().WallTime
				status := &proto.NodeStatus{
					Desc:       n.Descriptor,
					StoreIDs:   accessedStoreIDs,
					UpdatedAt:  now,
					StartedAt:  n.startedAt,
					RangeCount: int32(rangeCount),
					Stats:      *stats,
				}
				key := engine.NodeStatusKey(int32(n.Descriptor.NodeID))
				if err := n.sCtx.DB.Run(client.PutProto(key, status)); err != nil {
					log.Error(err)
				}
				// Increment iteration count.
				n.completedScan.L.Lock()
				n.scanCount++
				n.completedScan.Broadcast()
				n.completedScan.L.Unlock()
				if log.V(6) {
					log.Infof("store scan iteration completed")
				}
				stopper.FinishTask()
			case <-stopper.ShouldStop():
				// Exit the loop.
				return
			}
		}
	})
}

// waitForScanCompletion waits until the end of the next store scan and returns
// the total number of scans completed so far.  This is exposed for use in unit
// tests only.
func (n *Node) waitForScanCompletion() int64 {
	n.completedScan.L.Lock()
	defer n.completedScan.L.Unlock()
	initalValue := n.scanCount
	for n.scanCount == initalValue {
		n.completedScan.Wait()
	}
	return n.scanCount
}

// executeCmd creates a client.Call struct and sends if via our local sender.
func (n *nodeServer) executeCmd(args proto.Request, reply proto.Response) error {
	n.lSender.Send(client.Call{Args: args, Reply: reply})
	return nil
}

// TODO(spencer): fill in method comments below.

// Contains .
func (n *nodeServer) Contains(args *proto.ContainsRequest, reply *proto.ContainsResponse) error {
	return n.executeCmd(args, reply)
}

// Get .
func (n *nodeServer) Get(args *proto.GetRequest, reply *proto.GetResponse) error {
	return n.executeCmd(args, reply)
}

// Put .
func (n *nodeServer) Put(args *proto.PutRequest, reply *proto.PutResponse) error {
	return n.executeCmd(args, reply)
}

// ConditionalPut .
func (n *nodeServer) ConditionalPut(args *proto.ConditionalPutRequest, reply *proto.ConditionalPutResponse) error {
	return n.executeCmd(args, reply)
}

// Increment .
func (n *nodeServer) Increment(args *proto.IncrementRequest, reply *proto.IncrementResponse) error {
	return n.executeCmd(args, reply)
}

// Delete .
func (n *nodeServer) Delete(args *proto.DeleteRequest, reply *proto.DeleteResponse) error {
	return n.executeCmd(args, reply)
}

// DeleteRange .
func (n *nodeServer) DeleteRange(args *proto.DeleteRangeRequest, reply *proto.DeleteRangeResponse) error {
	return n.executeCmd(args, reply)
}

// Scan .
func (n *nodeServer) Scan(args *proto.ScanRequest, reply *proto.ScanResponse) error {
	return n.executeCmd(args, reply)
}

// EndTransaction .
func (n *nodeServer) EndTransaction(args *proto.EndTransactionRequest, reply *proto.EndTransactionResponse) error {
	return n.executeCmd(args, reply)
}

// AdminSplit .
func (n *nodeServer) AdminSplit(args *proto.AdminSplitRequest, reply *proto.AdminSplitResponse) error {
	return n.executeCmd(args, reply)
}

// AdminMerge .
func (n *nodeServer) AdminMerge(args *proto.AdminMergeRequest, reply *proto.AdminMergeResponse) error {
	return n.executeCmd(args, reply)
}

// InternalRangeLookup .
func (n *nodeServer) InternalRangeLookup(args *proto.InternalRangeLookupRequest, reply *proto.InternalRangeLookupResponse) error {
	return n.executeCmd(args, reply)
}

// InternalHeartbeatTxn .
func (n *nodeServer) InternalHeartbeatTxn(args *proto.InternalHeartbeatTxnRequest, reply *proto.InternalHeartbeatTxnResponse) error {
	return n.executeCmd(args, reply)
}

// InternalGC .
func (n *nodeServer) InternalGC(args *proto.InternalGCRequest, reply *proto.InternalGCResponse) error {
	return n.executeCmd(args, reply)
}

// InternalPushTxn .
func (n *nodeServer) InternalPushTxn(args *proto.InternalPushTxnRequest, reply *proto.InternalPushTxnResponse) error {
	return n.executeCmd(args, reply)
}

// InternalResolveIntent .
func (n *nodeServer) InternalResolveIntent(args *proto.InternalResolveIntentRequest, reply *proto.InternalResolveIntentResponse) error {
	return n.executeCmd(args, reply)
}

// InternalMerge .
func (n *nodeServer) InternalMerge(args *proto.InternalMergeRequest, reply *proto.InternalMergeResponse) error {
	return n.executeCmd(args, reply)
}

// InternalTruncateLog .
func (n *nodeServer) InternalTruncateLog(args *proto.InternalTruncateLogRequest, reply *proto.InternalTruncateLogResponse) error {
	return n.executeCmd(args, reply)
}

// InternalLeaderLease .
func (n *nodeServer) InternalLeaderLease(args *proto.InternalLeaderLeaseRequest,
	reply *proto.InternalLeaderLeaseResponse) error {
	return n.executeCmd(args, reply)
}
