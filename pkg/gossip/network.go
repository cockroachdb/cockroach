package gossip

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"net"
	"sync"
)

type GRPCDialer struct {
	*rpc.Context
}

func (d GRPCDialer) Connect(ctx context.Context, _ *stop.Stopper, addr net.Addr) (EndpointClient, error) {
	// Note: avoid using `grpc.WithBlock` here. This code is already
	// asynchronous from the caller's perspective, so the only effect of
	// `WithBlock` here is blocking shutdown - at the time of this writing,
	// that ends up making `kv` tests take twice as long.
	conn, err := d.GRPCUnvalidatedDial(addr.String()).Connect(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := NewGossipClient(conn).Gossip(ctx)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

// TestNetwork represents the entire gossip network on all nodes.
type TestNetwork struct {
	sync.Mutex
	servers map[string]testServer
	nextId  int
}

// A test server has the gossip struct as well as the
type testServer struct {
	gossip  *Gossip
	server  TestDialerServer
	stopper *stop.Stopper
}

func NewTestNetwork() TestNetwork {
	return TestNetwork{
		servers: make(map[string]testServer),
		nextId:  1,
	}
}

// StartGossip is a simplified wrapper around New that creates the
// ClusterIDContainer and NodeIDContainer internally. Used for testing.
func (tn *TestNetwork) StartGossip(nodeID roachpb.NodeID, stopper *stop.Stopper, registry *metric.Registry) *Gossip {
	return tn.StartGossipWithLocality(nodeID, stopper, registry, roachpb.Locality{})
}

// StartGossipWithLocality calls StartGossip with an explicit locality value.
func (tn *TestNetwork) StartGossipWithLocality(nodeID roachpb.NodeID, stopper *stop.Stopper, registry *metric.Registry, locality roachpb.Locality) *Gossip {
	ctx := context.TODO()
	c := &base.ClusterIDContainer{}
	n := &base.NodeIDContainer{}
	var ac log.AmbientContext
	ac.AddLogTag("n", n)
	gossip := New(ac, c, n, stopper, registry, locality, tn)
	if nodeID != 0 {
		n.Set(ctx, nodeID)
	}
	addr := util.MakeUnresolvedAddr("tcp", net.IP{127, 0, byte(tn.nextId >> 8), byte(tn.nextId)}.String())
	tn.Lock()
	defer tn.Unlock()
	tn.nextId++
	if _, ok := tn.servers[addr.String()]; ok {
		// Don't allow repeats.
		return nil
	}
	ts := testServer{
		gossip,
		TestDialerServer{stopper, addr.String(), tn, make(chan *Request, 100)},
		stopper,
	}
	tn.servers[addr.String()] = ts
	gossip.start(&addr)
	return gossip
}

func (tn *TestNetwork) Connect(ctx context.Context, stopper *stop.Stopper, addr net.Addr) (EndpointClient, error) {
	tn.Lock()
	defer tn.Unlock()
	// Verify the server exists.
	if ts, ok := tn.servers[addr.String()]; ok {
		// Start the server gossip thread
		tc := TestDialerClient{stopper, addr.String(), &ts.server, make(chan *Response, 100)}
		stream := connection{
			client: tc,
			server: ts.server,
		}
		_ = stopper.RunAsyncTask(ctx, "gossip-listener", func(ctx context.Context) {
			if err := ts.gossip.GossipInternal(ctx, stream); err != nil {
				log.Infof(ctx, "Error running gossip %v", err)
			}
		})
		return tc, nil
	}
	return nil, errors.Newf("can't find node for %v", addr)
}

// connection represents a connection between a client and a server. Anything
// the client sends, the server will receive and vice versa.
type connection struct {
	client TestDialerClient
	server TestDialerServer
}

// TestDialerClient represents a TCP connection from a client to a server.
// Each client is connected to exactly one server.
type TestDialerClient struct {
	stopper *stop.Stopper
	addr    string
	server  *TestDialerServer
	ch      chan *Response
}

// TestDialerServer represents a listening port. Each server can have multiple incoming clients that are indexed by ???
type TestDialerServer struct {
	stopper *stop.Stopper
	addr    string
	tn      *TestNetwork
	ch      chan *Request
}

func (d TestDialerClient) Send(request *Request) error {
	log.Infof(context.TODO(), "%s: sending request %v", d.addr, request)
	d.server.ch <- request
	return nil
}

func (d TestDialerClient) Recv() (*Response, error) {
	select {
	case response := <-d.ch:
		log.Infof(context.TODO(), "%s: received response %v", d.addr, response)
		return response, nil
		// continue
	case <-d.stopper.ShouldQuiesce():
		return nil, errors.New("server shutting down")
	}
}

func (d connection) Send(response *Response) error {
	log.Infof(context.TODO(), "%s: sending response %v", d.server.addr, response)
	d.client.ch <- response
	return nil
}

func (d connection) Recv() (*Request, error) {
	select {
	case request := <-d.server.ch:
		log.Infof(context.TODO(), "%s: received request %v", d.server.addr, request)
		return request, nil
		// continue
	case <-d.server.stopper.ShouldQuiesce():
		return nil, errors.New("server shutting down")
	}
}
