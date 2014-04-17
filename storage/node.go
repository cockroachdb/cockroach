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

package storage

import (
	"flag"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

const (
	// defaultCacheSize is the default value for the cacheSize command line flag.
	defaultCacheSize = 1 << 30 // GB
)

var (
	// dataDirs is specified to enable durable storage via
	// RocksDB-backed key-value stores.
	dataDirs = flag.String("data_dirs", "", "specify a comma-separated list of paths, "+
		"one per physical storage device; if empty, node will serve out of memory")
	// cacheSize is the amount of memory in bytes to use for caching data.
	// The value is split evenly between the stores if there are more than one.
	// If the node only hosts a single in-memory store, then cacheSize is the
	// maximum size in bytes the store is allowed to grow to before it reaches
	// full capacity.
	cacheSize = flag.Int64("cache_size", defaultCacheSize, "total size in bytes for "+
		"data stored in all caches, shared evenly if there are multiple storage devices")

	// storageGossipInterval is the period at which storage is checked and re-added to gossip
	storageGossipInterval = flag.Duration(
		"storage_gossip_interval", time.Minute,
		"approximate interval (time.Duration) for checking local disk capacity and adding to gossip")
)

// Node holds the set of stores which this roach node serves traffic for.
type Node struct {
	gossip   *gossip.Gossip
	storeMap map[string]*store
	config   NodeConfig
	closer   chan struct{}
}

// NewNode returns a new instance of Node, interpreting command line
// flags to intialize the appropriate store or set of
// stores. Registers the storage instance for the RPC service "Node".
func NewNode(rpcServer *rpc.Server, gossip *gossip.Gossip) *Node {
	// TODO(levon): Read node configuration and set it here.
	n := &Node{
		gossip:   gossip,
		storeMap: make(map[string]*store),
		closer:   make(chan struct{}, 1),
	}
	// TODO(levon): real args here
	allocator := &allocator{}
	rpcServer.RegisterName("Node", n)

	if *dataDirs == "" {
		n.storeMap["in-mem"] = newStore(NewInMem(*cacheSize), allocator)
	} else {
		for _, dir := range strings.Split(*dataDirs, ",") {
			rocksdb, err := NewRocksDB(dir)
			if err != nil {
				glog.Infof("unable to stat data directory %s; skipping...will not serve data", dir)
				continue
			}
			n.storeMap[rocksdb.name] = newStore(rocksdb, allocator)
		}
		// TODO(spencer): set cache sizes on successfully created stores.
		glog.Fatal("rocksdb stores unsupported")
	}
	return n
}

// startGossip starts a goroutine that periodically checks local disk
// capacity and gossips it.
func (n *Node) startGossip() {
	ticker := time.NewTicker(*storageGossipInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				n.gossipCapacities()
			case <-n.closer:
				ticker.Stop()
				return
			}
		}
	}()
}

// gossipCapacities calls capacity on each store and adds it to the gossip
func (n *Node) gossipCapacities() {
	gossipTopic := maxCapacityPrefix + n.config.DataCenter
	for _, store := range n.storeMap {
		capacity, err := store.engine.capacity()
		if err != nil {
			glog.Warningf("Problem getting capacity: %v", err)
			continue
		}

		n.gossip.AddInfo(gossipTopic, &AvailableDiskConfig{DiskCapacity: *capacity, Node: n.config}, time.Duration(2)*(*storageGossipInterval))
	}
}

// getRange looks up the store by Replica.Disk and then queries it for
// the range specified by Replica.Range.
func (n *Node) getRange(r *Replica) (*Range, error) {
	store, ok := n.storeMap[r.Disk]
	if !ok {
		return nil, util.Errorf("disk %s not found", r.Disk)
	}
	rng, err := store.getRange(r)
	if err != nil {
		return nil, err
	}
	return rng, nil
}

// Start starts the node
func (n *Node) Start() {
	n.startGossip()
}

// Stop cleanly stops the node
func (n *Node) Stop() {
	close(n.closer)
}

// All methods to satisfy the Node RPC service fetch the range
// based on the Replica target provided in the argument header.
// Commands are broken down into read-only and read-write and
// sent along to the range via either Range.readOnlyCmd() or
// Range.readWriteCmd().

// Contains .
func (n *Node) Contains(args *ContainsRequest, reply *ContainsResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return rng.readOnlyCmd("Contains", args, reply)
}

// Get .
func (n *Node) Get(args *GetRequest, reply *GetResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return rng.readOnlyCmd("Get", args, reply)
}

// Put .
func (n *Node) Put(args *PutRequest, reply *PutResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.readWriteCmd("Put", args, reply)
}

// Increment .
func (n *Node) Increment(args *IncrementRequest, reply *IncrementResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.readWriteCmd("Increment", args, reply)
}

// Delete .
func (n *Node) Delete(args *DeleteRequest, reply *DeleteResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.readWriteCmd("Delete", args, reply)
}

// DeleteRange .
func (n *Node) DeleteRange(args *DeleteRangeRequest, reply *DeleteRangeResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.readWriteCmd("DeleteRange", args, reply)
}

// Scan .
func (n *Node) Scan(args *ScanRequest, reply *ScanResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return rng.readOnlyCmd("Scan", args, reply)
}

// EndTransaction .
func (n *Node) EndTransaction(args *EndTransactionRequest, reply *EndTransactionResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.readWriteCmd("EndTransaction", args, reply)
}

// AccumulateTS .
func (n *Node) AccumulateTS(args *AccumulateTSRequest, reply *AccumulateTSResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.readWriteCmd("AccumulateTS", args, reply)
}

// ReapQueue .
func (n *Node) ReapQueue(args *ReapQueueRequest, reply *ReapQueueResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.readWriteCmd("ReapQueue", args, reply)
}

// EnqueueUpdate .
func (n *Node) EnqueueUpdate(args *EnqueueUpdateRequest, reply *EnqueueUpdateResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.readWriteCmd("EnqueueUpdate", args, reply)
}

// EnqueueMessage .
func (n *Node) EnqueueMessage(args *EnqueueMessageRequest, reply *EnqueueMessageResponse) error {
	rng, err := n.getRange(&args.Replica)
	if err != nil {
		return err
	}
	return <-rng.readWriteCmd("EnqueueMessage", args, reply)
}
