// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package simulation

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"google.golang.org/grpc"
)

// Node represents a node used in a Network. It includes information
// about the node's gossip instance, network address, and underlying
// server.
type Node struct {
	Gossip    *gossip.Gossip
	Server    *grpc.Server
	Listener  net.Listener
	Registry  *metric.Registry
	Resolvers []resolver.Resolver
}

// Addr returns the address of the connected listener.
func (n *Node) Addr() net.Addr {
	return n.Listener.Addr()
}

// Network provides access to a test gossip network of nodes.
type Network struct {
	Nodes           []*Node
	Stopper         *stop.Stopper
	RPCContext      *rpc.Context
	nodeIDAllocator roachpb.NodeID // provides unique node IDs
	tlsConfig       *tls.Config
	started         bool
}

// NewNetwork creates nodeCount gossip nodes.
func NewNetwork(
	stopper *stop.Stopper, nodeCount int, createResolvers bool, defaultZoneConfig *zonepb.ZoneConfig,
) *Network {
	log.Infof(context.TODO(), "simulating gossip network with %d nodes", nodeCount)

	n := &Network{
		Nodes:   []*Node{},
		Stopper: stopper,
	}
	n.RPCContext = rpc.NewContext(rpc.ContextOptions{
		TenantID:   roachpb.SystemTenantID,
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Config:     &base.Config{Insecure: true},
		Clock:      hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		Stopper:    n.Stopper,
		Settings:   cluster.MakeTestingClusterSettings(),
	})
	var err error
	n.tlsConfig, err = n.RPCContext.GetServerTLSConfig()
	if err != nil {
		log.Fatalf(context.TODO(), "%v", err)
	}

	// Ensure that tests using this test context and restart/shut down
	// their servers do not inadvertently start talking to servers from
	// unrelated concurrent tests.
	n.RPCContext.ClusterID.Set(context.TODO(), uuid.MakeV4())

	for i := 0; i < nodeCount; i++ {
		node, err := n.CreateNode(defaultZoneConfig)
		if err != nil {
			log.Fatalf(context.TODO(), "%v", err)
		}
		// Build a resolver for each instance or we'll get data races.
		if createResolvers {
			r, err := resolver.NewResolverFromAddress(n.Nodes[0].Addr())
			if err != nil {
				log.Fatalf(context.TODO(), "bad gossip address %s: %s", n.Nodes[0].Addr(), err)
			}
			node.Resolvers = []resolver.Resolver{r}
		}
	}
	return n
}

// CreateNode creates a simulation node and starts an RPC server for it.
func (n *Network) CreateNode(defaultZoneConfig *zonepb.ZoneConfig) (*Node, error) {
	server := rpc.NewServer(n.RPCContext)
	ln, err := net.Listen(util.IsolatedTestAddr.Network(), util.IsolatedTestAddr.String())
	if err != nil {
		return nil, err
	}
	node := &Node{Server: server, Listener: ln, Registry: metric.NewRegistry()}
	node.Gossip = gossip.NewTest(0, n.RPCContext, server, n.Stopper, node.Registry, defaultZoneConfig)
	n.Stopper.AddCloser(stop.CloserFn(server.Stop))
	_ = n.Stopper.RunAsyncTask(context.TODO(), "node-wait-quiesce", func(context.Context) {
		<-n.Stopper.ShouldQuiesce()
		netutil.FatalIfUnexpected(ln.Close())
		node.Gossip.EnableSimulationCycler(false)
	})
	n.Nodes = append(n.Nodes, node)
	return node, nil
}

// StartNode initializes a gossip instance for the simulation node and
// starts it.
func (n *Network) StartNode(node *Node) error {
	node.Gossip.Start(node.Addr(), node.Resolvers)
	node.Gossip.EnableSimulationCycler(true)
	n.nodeIDAllocator++
	node.Gossip.NodeID.Set(context.TODO(), n.nodeIDAllocator)
	if err := node.Gossip.SetNodeDescriptor(&roachpb.NodeDescriptor{
		NodeID:  node.Gossip.NodeID.Get(),
		Address: util.MakeUnresolvedAddr(node.Addr().Network(), node.Addr().String()),
	}); err != nil {
		return err
	}
	if err := node.Gossip.AddInfo(node.Addr().String(),
		encoding.EncodeUint64Ascending(nil, 0), time.Hour); err != nil {
		return err
	}
	return n.Stopper.RunAsyncTask(context.TODO(), "start-node", func(context.Context) {
		netutil.FatalIfUnexpected(node.Server.Serve(node.Listener))
	})
}

// GetNodeFromID returns the simulation node associated with
// provided node ID, or nil if there is no such node.
func (n *Network) GetNodeFromID(nodeID roachpb.NodeID) (*Node, bool) {
	for _, node := range n.Nodes {
		if node.Gossip.NodeID.Get() == nodeID {
			return node, true
		}
	}
	return nil, false
}

// SimulateNetwork runs until the simCallback returns false.
//
// At each cycle, every node gossips a key equal to its address (unique)
// with the cycle as the value. The received cycle value can be used
// to determine the aging of information between any two nodes in the
// network.
//
// At each cycle of the simulation, node 0 gossips the sentinel.
//
// The simulation callback receives the cycle and the network as arguments.
func (n *Network) SimulateNetwork(simCallback func(cycle int, network *Network) bool) {
	n.Start()
	nodes := n.Nodes
	for cycle := 1; ; cycle++ {
		// Node 0 gossips sentinel & cluster ID every cycle.
		if err := nodes[0].Gossip.AddInfo(
			gossip.KeySentinel,
			encoding.EncodeUint64Ascending(nil, uint64(cycle)),
			time.Hour,
		); err != nil {
			log.Fatalf(context.TODO(), "%v", err)
		}
		if err := nodes[0].Gossip.AddInfo(
			gossip.KeyClusterID,
			encoding.EncodeUint64Ascending(nil, uint64(cycle)),
			0*time.Second,
		); err != nil {
			log.Fatalf(context.TODO(), "%v", err)
		}
		// Every node gossips every cycle.
		for _, node := range nodes {
			if err := node.Gossip.AddInfo(
				node.Addr().String(),
				encoding.EncodeUint64Ascending(nil, uint64(cycle)),
				time.Hour,
			); err != nil {
				log.Fatalf(context.TODO(), "%v", err)
			}
			node.Gossip.SimulationCycle()
		}
		// If the simCallback returns false, we're done with the
		// simulation; exit the loop. This condition is tested here
		// instead of in the for statement in order to guarantee
		// we run at least one iteration of this loop in order to
		// gossip the cluster ID and sentinel.
		if !simCallback(cycle, n) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	log.Infof(context.TODO(), "gossip network simulation: total infos sent=%d, received=%d", n.infosSent(), n.infosReceived())
}

// Start starts all gossip nodes.
// TODO(spencer): make all methods in Network return errors instead of
// fatal logging.
func (n *Network) Start() {
	if n.started {
		return
	}
	n.started = true
	for _, node := range n.Nodes {
		if err := n.StartNode(node); err != nil {
			log.Fatalf(context.TODO(), "%v", err)
		}
	}
}

// RunUntilFullyConnected blocks until the gossip network has received
// gossip from every other node in the network. It returns the gossip
// cycle at which the network became fully connected.
func (n *Network) RunUntilFullyConnected() int {
	var connectedAtCycle int
	n.SimulateNetwork(func(cycle int, network *Network) bool {
		if network.IsNetworkConnected() {
			connectedAtCycle = cycle
			return false
		}
		return true
	})
	return connectedAtCycle
}

// IsNetworkConnected returns true if the network is fully connected
// with no partitions (i.e. every node knows every other node's
// network address).
func (n *Network) IsNetworkConnected() bool {
	for _, leftNode := range n.Nodes {
		for _, rightNode := range n.Nodes {
			if _, err := leftNode.Gossip.GetInfo(gossip.MakeNodeIDKey(rightNode.Gossip.NodeID.Get())); err != nil {
				return false
			}
		}
	}
	return true
}

// infosSent returns the total count of infos sent from all nodes in
// the network.
func (n *Network) infosSent() int {
	var count int64
	for _, node := range n.Nodes {
		count += node.Gossip.GetNodeMetrics().InfosSent.Counter.Count()
	}
	return int(count)
}

// infosReceived returns the total count of infos received from all
// nodes in the network.
func (n *Network) infosReceived() int {
	var count int64
	for _, node := range n.Nodes {
		count += node.Gossip.GetNodeMetrics().InfosReceived.Counter.Count()
	}
	return int(count)
}
