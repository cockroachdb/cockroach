// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package simulation

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
	Addresses []util.UnresolvedAddr
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
func NewNetwork(stopper *stop.Stopper, nodeCount int, createAddresses bool) *Network {
	ctx := context.TODO()
	log.Infof(ctx, "simulating gossip network with %d nodes", nodeCount)

	n := &Network{
		Nodes:   []*Node{},
		Stopper: stopper,
	}
	opts := rpc.DefaultContextOptions()
	opts.Insecure = true
	opts.Stopper = n.Stopper
	opts.Settings = cluster.MakeTestingClusterSettings()
	opts.Knobs = rpc.ContextTestingKnobs{
		NoLoopbackDialer: true,
	}
	n.RPCContext = rpc.NewContext(ctx, opts)

	var err error
	n.tlsConfig, err = n.RPCContext.GetServerTLSConfig()
	if err != nil {
		log.Fatalf(context.TODO(), "%v", err)
	}

	// Ensure that tests using this test context and restart/shut down
	// their servers do not inadvertently start talking to servers from
	// unrelated concurrent tests.
	n.RPCContext.StorageClusterID.Set(context.TODO(), uuid.MakeV4())

	for i := 0; i < nodeCount; i++ {
		node, err := n.CreateNode()
		if err != nil {
			log.Fatalf(context.TODO(), "%v", err)
		}
		if createAddresses {
			node.Addresses = []util.UnresolvedAddr{
				util.MakeUnresolvedAddr("tcp", n.Nodes[0].Addr().String()),
			}
		}
	}
	return n
}

// CreateNode creates a simulation node and starts an RPC server for it.
func (n *Network) CreateNode() (*Node, error) {
	ctx := context.TODO()
	server, err := rpc.NewServer(ctx, n.RPCContext)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen(util.IsolatedTestAddr.Network(), util.IsolatedTestAddr.String())
	if err != nil {
		return nil, err
	}
	node := &Node{Server: server, Listener: ln, Registry: metric.NewRegistry()}
	node.Gossip = gossip.NewTest(0, n.Stopper, node.Registry)
	gossip.RegisterGossipServer(server, node.Gossip)
	n.Stopper.AddCloser(stop.CloserFn(server.Stop))
	_ = n.Stopper.RunAsyncTask(ctx, "node-wait-quiesce", func(context.Context) {
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
	node.Gossip.Start(node.Addr(), node.Addresses, n.RPCContext)
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
	bgCtx := context.TODO()
	return n.Stopper.RunAsyncTask(bgCtx, "start-node", func(context.Context) {
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
		count += node.Gossip.GetNodeMetrics().InfosSent.Count()
	}
	return int(count)
}

// infosReceived returns the total count of infos received from all
// nodes in the network.
func (n *Network) infosReceived() int {
	var count int64
	for _, node := range n.Nodes {
		count += node.Gossip.GetNodeMetrics().InfosReceived.Count()
	}
	return int(count)
}
