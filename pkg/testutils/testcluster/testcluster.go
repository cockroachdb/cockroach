// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testcluster

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
)

// TestCluster represents a set of TestServers. The hope is that it can be used
// analogous to TestServer, but with control over range replication and join
// flags.
type TestCluster struct {
	Servers []serverutils.TestServerInterface
	Conns   []*gosql.DB
	// reusableListeners is populated if (and only if) TestClusterArgs.reusableListeners is set.
	reusableListeners map[int] /* idx */ *listenerutil.ReusableListener

	stopper *stop.Stopper
	mu      struct {
		syncutil.Mutex
		serverStoppers []*stop.Stopper
	}
	serverArgs  []base.TestServerArgs
	clusterArgs base.TestClusterArgs

	defaultTestTenantOptions base.DefaultTestTenantOptions

	t serverutils.TestFataler
}

var _ serverutils.TestClusterInterface = &TestCluster{}

// NumServers is part of TestClusterInterface.
func (tc *TestCluster) NumServers() int {
	return len(tc.Servers)
}

// Server is part of TestClusterInterface.
func (tc *TestCluster) Server(idx int) serverutils.TestServerInterface {
	return tc.Servers[idx]
}

// NodeIDs is part of TestClusterInterface.
func (tc *TestCluster) NodeIDs() []roachpb.NodeID {
	nodeIds := make([]roachpb.NodeID, len(tc.Servers))
	for i, s := range tc.Servers {
		nodeIds[i] = s.StorageLayer().NodeID()
	}
	return nodeIds
}

// ServerConn is part of TestClusterInterface.
func (tc *TestCluster) ServerConn(idx int) *gosql.DB {
	return tc.Conns[idx]
}

// Stopper returns the stopper for this testcluster.
func (tc *TestCluster) Stopper() *stop.Stopper {
	return tc.stopper
}

// StartedDefaultTestTenant returns whether this cluster started a default
// test tenant.
func (tc *TestCluster) StartedDefaultTestTenant() bool {
	return tc.Servers[0].TenantController().StartedDefaultTestTenant()
}

// ApplicationLayer calls .ApplicationLayer() on the ith server in
// the cluster.
func (tc *TestCluster) ApplicationLayer(idx int) serverutils.ApplicationLayerInterface {
	return tc.Server(idx).ApplicationLayer()
}

// SystemLayer calls .SystemLayer() on the ith server in the
// cluster.
func (tc *TestCluster) SystemLayer(idx int) serverutils.ApplicationLayerInterface {
	return tc.Server(idx).SystemLayer()
}

// StorageLayer calls .StorageLayer() on the ith server in the
// cluster.
func (tc *TestCluster) StorageLayer(idx int) serverutils.StorageLayerInterface {
	return tc.Server(idx).StorageLayer()
}

// DefaultTenantDeploymentMode implements TestClusterInterface.
func (tc *TestCluster) DefaultTenantDeploymentMode() serverutils.DeploymentMode {
	return tc.Server(0).DeploymentMode()
}

// stopServers stops the stoppers for each individual server in the cluster.
// This method ensures that servers that were previously stopped explicitly are
// not double-stopped.
func (tc *TestCluster) stopServers(ctx context.Context) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Quiesce the servers in parallel to avoid deadlocks. If we stop servers
	// serially when we lose quorum (2 out of 3 servers have stopped) the last
	// server may never finish due to waiting for a Raft command that can't
	// commit due to the lack of quorum.
	log.Infof(ctx, "TestCluster quiescing nodes")
	var wg sync.WaitGroup
	wg.Add(len(tc.mu.serverStoppers))
	for i, s := range tc.mu.serverStoppers {
		go func(i int, s *stop.Stopper) {
			defer wg.Done()
			if s != nil {
				quiesceCtx := logtags.AddTag(ctx, "n", tc.Servers[i].StorageLayer().NodeID())
				s.Quiesce(quiesceCtx)
			}
		}(i, s)
	}
	wg.Wait()

	for i := 0; i < tc.NumServers(); i++ {
		tc.stopServerLocked(i)
	}

	// TODO(andrei): Instead of checking for empty tracing registries after
	// shutting down each node, we're doing it after shutting down all nodes. This
	// is because all the nodes might share the same cluster (in case the Tracer
	// was passed in at cluster creation time). We should not allow the Tracer to
	// be passed in like this, and we should then also added this registry
	// draining check to individual TestServers.
	for i := 0; i < tc.NumServers(); i++ {
		// Wait until a server's span registry is emptied out. This helps us check
		// to see that there are no un-Finish()ed spans. We need to wrap this in a
		// SucceedsSoon block because it's possible for us to issue requests during
		// server shut down, where the requests in turn would create (registered)
		// spans. Cleaning up temporary objects created by the session[1] is one
		// example of this.
		//
		// [1]: cleanupSessionTempObjects
		tracer := tc.Servers[i].SystemLayer().Tracer()
		testutils.SucceedsSoon(tc.t, func() error {
			var sps []tracing.RegistrySpan
			_ = tracer.VisitSpans(func(span tracing.RegistrySpan) error {
				sps = append(sps, span)
				return nil
			})
			if len(sps) == 0 {
				return nil
			}
			var buf strings.Builder
			fmt.Fprintf(&buf, "unexpectedly found %d active spans:\n", len(sps))
			var ids []uint64
			for _, sp := range sps {
				trace := sp.GetFullRecording(tracingpb.RecordingVerbose)
				for _, rs := range trace.Flatten() {
					// NB: it would be a sight easier to just include these in the output of
					// the string formatted recording, but making a change there presumably requires
					// lots of changes across various testdata in the codebase and the author is
					// trying to be nimble.
					ids = append(ids, rs.GoroutineID)
				}
				fmt.Fprintln(&buf, trace)
				fmt.Fprintln(&buf)
			}
			sl := allstacks.Get()
			return errors.Newf("%s\n\ngoroutines of interest: %v\nstacks:\n\n%s", buf.String(), ids, sl)
		})
	}
	// Force a GC in an attempt to run finalizers. Some finalizers run sanity
	// checks that panic on failure, and ideally we'd run them all before starting
	// the next test.
	runtime.GC()
}

// StopServer stops an individual server in the cluster.
func (tc *TestCluster) StopServer(idx int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.stopServerLocked(idx)
}

func (tc *TestCluster) stopServerLocked(idx int) {
	if tc.mu.serverStoppers[idx] != nil {
		tc.mu.serverStoppers[idx].Stop(context.TODO())
		tc.mu.serverStoppers[idx] = nil
	}
}

var debugTimings = struct {
	StartCalls, StartDuration atomic.Int64
	WaitForRepDuration        atomic.Int64
}{}

// PrintTimings summarizes time spent across invocations of testcluster methods.
func PrintTimings(testMain time.Duration) {
	setupTime := time.Duration(debugTimings.StartDuration.Load())

	if setupTime > 0 {
		tcStartCalls := debugTimings.StartCalls.Load()
		setupTime += time.Duration(debugTimings.WaitForRepDuration.Load())

		// We could break out WaitForReplication time, but the point here is just to
		// show total time spent setting up tests vs testing, so lumping it in to a
		// single more concise number is arguably better.
		fmt.Fprintf(os.Stderr,
			"Setting up %d TestClusters took %s, %.1f%% of TestMain\n",
			tcStartCalls, setupTime.Round(time.Millisecond), float64(setupTime)/float64(testMain)*100,
		)
	}
}

// StartTestCluster creates and starts up a TestCluster made up of `nodes`
// in-memory testing servers.
// The cluster should be stopped using TestCluster.Stopper().Stop().
func StartTestCluster(
	t serverutils.TestFataler, nodes int, args base.TestClusterArgs,
) *TestCluster {
	cluster := NewTestCluster(t, nodes, args)
	cluster.Start(t)
	return cluster
}

// NewTestCluster initializes a TestCluster made up of `nodes` in-memory testing
// servers. It needs to be started separately using the return type.
func NewTestCluster(
	t serverutils.TestFataler, nodes int, clusterArgs base.TestClusterArgs,
) *TestCluster {
	if nodes < 1 {
		t.Fatal("invalid cluster size: ", nodes)
	}
	if clusterArgs.StartSingleNode && nodes > 1 {
		t.Fatal("StartSingleNode implies 1 node only, but asked to create", nodes)
	}

	if err := checkServerArgsForCluster(
		clusterArgs.ServerArgs, clusterArgs.ReplicationMode, disallowJoinAddr,
	); err != nil {
		t.Fatal(err)
	}
	for _, sargs := range clusterArgs.ServerArgsPerNode {
		if err := checkServerArgsForCluster(
			sargs, clusterArgs.ReplicationMode, allowJoinAddr,
		); err != nil {
			t.Fatal(err)
		}
	}

	tc := &TestCluster{
		stopper:     stop.NewStopper(),
		clusterArgs: clusterArgs,
		t:           t,
	}

	// Check if any of the args have a locality set.
	noLocalities := true
	for _, arg := range tc.clusterArgs.ServerArgsPerNode {
		if len(arg.Locality.Tiers) > 0 {
			noLocalities = false
			break
		}
	}
	if len(tc.clusterArgs.ServerArgs.Locality.Tiers) > 0 {
		noLocalities = false
	}

	// Find out how to do the default test tenant.
	// The choice should be made by the top-level ServerArgs.
	defaultTestTenantOptions := tc.clusterArgs.ServerArgs.DefaultTestTenant
	// API check: verify that no non-default choice was made via per-server args,
	// and inform the user otherwise.
	for i := 0; i < nodes; i++ {
		if args, ok := tc.clusterArgs.ServerArgsPerNode[i]; ok &&
			args.DefaultTestTenant != (base.DefaultTestTenantOptions{}) &&
			args.DefaultTestTenant != defaultTestTenantOptions {
			tc.Stopper().Stop(context.Background())
			t.Fatalf("improper use of DefaultTestTenantOptions in per-server args: %v vs %v\n"+
				"Tip: use the top-level ServerArgs to set the default test tenant options.",
				args.DefaultTestTenant, defaultTestTenantOptions)
		}
	}
	tc.defaultTestTenantOptions = serverutils.ShouldStartDefaultTestTenant(t, defaultTestTenantOptions)

	var firstListener net.Listener
	for i := 0; i < nodes; i++ {
		var serverArgs base.TestServerArgs
		if perNodeServerArgs, ok := tc.clusterArgs.ServerArgsPerNode[i]; ok {
			serverArgs = perNodeServerArgs
		} else {
			serverArgs = tc.clusterArgs.ServerArgs
		}

		// We cannot allow multiple nodes to share a Settings object, so we make a
		// clone for each one.
		if serverArgs.Settings != nil && nodes > 1 {
			serverArgs.Settings = cluster.TestingCloneClusterSettings(serverArgs.Settings)
		}

		// If a reusable listener registry is provided, create reusable listeners
		// for every server that doesn't have a custom listener provided. (Only
		// servers with a reusable listener can be restarted).
		if reg := clusterArgs.ReusableListenerReg; reg != nil && serverArgs.Listener == nil {
			ln := reg.MustGetOrCreate(t, i)
			serverArgs.Listener = ln
			if tc.reusableListeners == nil {
				tc.reusableListeners = map[int]*listenerutil.ReusableListener{}
			}
			tc.reusableListeners[i] = ln
		}

		if len(serverArgs.StoreSpecs) == 0 {
			serverArgs.StoreSpecs = []base.StoreSpec{base.DefaultTestStoreSpec}
		}
		if knobs, ok := serverArgs.Knobs.Server.(*server.TestingKnobs); ok && knobs.StickyVFSRegistry != nil {
			for j := range serverArgs.StoreSpecs {
				if serverArgs.StoreSpecs[j].StickyVFSID == "" {
					serverArgs.StoreSpecs[j].StickyVFSID = fmt.Sprintf("auto-node%d-store%d", i+1, j+1)
				}
			}
		}

		// If no localities are specified in the args, we'll generate some
		// automatically.
		if noLocalities {
			tiers := []roachpb.Tier{
				{Key: "region", Value: "test"},
				{Key: "dc", Value: fmt.Sprintf("dc%d", i+1)},
			}
			serverArgs.Locality = roachpb.Locality{Tiers: tiers}
		}

		if i == 0 {
			if serverArgs.Listener != nil {
				// If the test installed a listener for us, use that.
				firstListener = serverArgs.Listener
			} else {
				// Pre-bind a listener for node zero so the kernel can go ahead and
				// assign its address for use in the other nodes' join flags.
				// The Server becomes responsible for closing this.
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					tc.Stopper().Stop(context.Background())
					t.Fatal(err)
				}
				firstListener = listener
				serverArgs.Listener = listener
			}
		} else {
			if serverArgs.JoinAddr == "" {
				// Point to the first listener unless told explicitly otherwise.
				serverArgs.JoinAddr = firstListener.Addr().String()
			}
			serverArgs.NoAutoInitializeCluster = true
		}

		if _, err := tc.AddServer(serverArgs); err != nil {
			tc.Stopper().Stop(context.Background())
			t.Fatal(err)
		}
	}

	return tc
}

// Start is the companion method to NewTestCluster, and is responsible for
// actually starting up the cluster. Start waits for each server to be fully up
// and running.
//
// If looking to test initialization/bootstrap behavior, Start should be invoked
// in a separate thread and with ParallelStart enabled (otherwise it'll block
// on waiting for init for the first server).
func (tc *TestCluster) Start(t serverutils.TestFataler) {
	ctx := context.Background()
	begin := timeutil.Now()
	defer func() {
		debugTimings.StartCalls.Add(1)
		debugTimings.StartDuration.Add(timeutil.Since(begin).Nanoseconds())
		if r := recover(); r != nil {
			// Avoid a stopper leak.
			tc.Stopper().Stop(ctx)
			panic(r)
		}
	}()

	nodes := len(tc.Servers)
	var errCh chan error
	if tc.clusterArgs.ParallelStart {
		errCh = make(chan error, nodes)
	}

	disableLBS := false
	for i := 0; i < nodes; i++ {
		// Disable LBS if any server has a very low scan interval.
		if tc.serverArgs[i].ScanInterval > 0 && tc.serverArgs[i].ScanInterval <= 100*time.Millisecond {
			disableLBS = true
		}

		if tc.clusterArgs.ParallelStart {
			go func(i int) {
				errCh <- tc.Servers[i].PreStart(ctx)
			}(i)
		} else {
			if err := tc.Servers[i].PreStart(ctx); err != nil {
				tc.Stopper().Stop(ctx)
				t.Fatal(err)
			}
			// We want to wait for stores for each server in order to have predictable
			// store IDs. Otherwise, stores can be asynchronously bootstrapped in an
			// unexpected order (#22342).
			tc.WaitForNStores(t, i+1, tc.Servers[0].StorageLayer().GossipI().(*gossip.Gossip))
		}
	}

	if tc.clusterArgs.ParallelStart {
		var cerr error
		for i := 0; i < nodes; i++ {
			if err := <-errCh; err != nil {
				cerr = errors.CombineErrors(cerr, err)
			}
		}
		if cerr != nil {
			tc.Stopper().Stop(ctx)
			t.Fatal(cerr)
		}

		tc.WaitForNStores(t, tc.NumServers(), tc.Servers[0].StorageLayer().GossipI().(*gossip.Gossip))
	}

	if disableLBS {
		if _, err := tc.Servers[0].SystemLayer().
			InternalExecutor().(isql.Executor).
			Exec(ctx, "enable-split-by-load", nil, /*txn */
				`SET CLUSTER SETTING kv.range_split.by_load.enabled = false`); err != nil {
			tc.Stopper().Stop(ctx)
			t.Fatal(err)
		}
	}

	// Create a closer that will stop the individual server stoppers when the
	// cluster stopper is stopped.
	tc.stopper.AddCloser(stop.CloserFn(func() { tc.stopServers(context.TODO()) }))

	if tc.clusterArgs.ReplicationMode == base.ReplicationAuto {
		if err := tc.WaitForFullReplication(); err != nil {
			tc.Stopper().Stop(ctx)
			t.Fatal(err)
		}
	}

	// Wait until a NodeStatus is persisted for every node (see #25488, #25649, #31574).
	tc.WaitForNodeStatuses(t)
	testutils.SucceedsSoon(t, func() error {
		var err error
		for _, ssrv := range tc.Servers {
			for _, dsrv := range tc.Servers {
				stl := dsrv.StorageLayer()
				// Note: we avoid using .RPCClientConn() here to avoid accumulating
				// stopper closures in RAM during the SucceedsSoon iterations.
				_, e := ssrv.SystemLayer().RPCContext().GRPCDialNode(
					dsrv.SystemLayer().AdvRPCAddr(),
					stl.NodeID(),
					roachpb.Locality{},
					rpc.DefaultClass,
				).Connect(context.TODO())
				err = errors.CombineErrors(err, e)
			}
		}
		return err
	})

	// Activate the SQL service and secondary tenants on every server.
	for idx, s := range tc.Servers {
		if err := s.Activate(ctx); err != nil {
			tc.Stopper().Stop(ctx)
			t.Fatal(err)
		}
		dbConn, err := s.ApplicationLayer().SQLConnE(serverutils.DBName(tc.serverArgs[idx].UseDatabase))
		if err != nil {
			tc.Stopper().Stop(ctx)
			t.Fatal(err)
		}
		tc.Conns = append(tc.Conns, dbConn)
	}
}

type checkType bool

const (
	disallowJoinAddr checkType = false
	allowJoinAddr    checkType = true
)

// checkServerArgsForCluster sanity-checks TestServerArgs to work for a cluster
// with a given replicationMode.
func checkServerArgsForCluster(
	args base.TestServerArgs, replicationMode base.TestClusterReplicationMode, checkType checkType,
) error {
	if checkType == disallowJoinAddr && args.JoinAddr != "" {
		return errors.Errorf("can't specify a join addr when starting a cluster: %s",
			args.JoinAddr)
	}
	if args.Stopper != nil {
		return errors.Errorf("can't set individual server stoppers when starting a cluster")
	}
	if args.Knobs.Store != nil {
		storeKnobs := args.Knobs.Store.(*kvserver.StoreTestingKnobs)
		if storeKnobs.DisableSplitQueue || storeKnobs.DisableReplicateQueue {
			return errors.Errorf("can't disable an individual server's queues when starting a cluster; " +
				"the cluster controls replication")
		}
	}

	if replicationMode != base.ReplicationAuto && replicationMode != base.ReplicationManual {
		return errors.Errorf("unexpected replication mode: %s", replicationMode)
	}

	return nil
}

// AddAndStartServer calls through to AddAndStartServerE.
func (tc *TestCluster) AddAndStartServer(
	t serverutils.TestFataler, serverArgs base.TestServerArgs,
) {
	t.Helper()
	if err := tc.AddAndStartServerE(serverArgs); err != nil {
		t.Fatal(err)
	}
}

// AddAndStartServerE creates a server with the specified arguments and appends it to
// the TestCluster. It also starts it.
//
// The new Server's copy of serverArgs might be changed according to the
// cluster's ReplicationMode.
func (tc *TestCluster) AddAndStartServerE(serverArgs base.TestServerArgs) error {
	if serverArgs.JoinAddr == "" && len(tc.Servers) > 0 {
		serverArgs.JoinAddr = tc.Servers[0].SystemLayer().AdvRPCAddr()
	}
	if _, err := tc.AddServer(serverArgs); err != nil {
		return err
	}

	return tc.startServer(len(tc.Servers)-1, serverArgs)
}

// AddServer is like AddAndStartServer, except it does not start it.
func (tc *TestCluster) AddServer(
	serverArgs base.TestServerArgs,
) (serverutils.TestServerInterface, error) {
	if tc.clusterArgs.StartSingleNode && len(tc.Servers) > 0 {
		return nil, errors.Errorf("already added 1 node to a start-single-node instance")
	}
	serverArgs.PartOfCluster = !tc.clusterArgs.StartSingleNode
	if serverArgs.JoinAddr != "" {
		serverArgs.NoAutoInitializeCluster = true
	}

	// Check args even though we have called checkServerArgsForCluster()
	// already in NewTestCluster(). AddServer might be called for servers
	// being added after the cluster was started, in which case the
	// check has not been performed.
	if err := checkServerArgsForCluster(
		serverArgs,
		tc.clusterArgs.ReplicationMode,
		// Allow JoinAddr here; servers being added after the TestCluster has been
		// started should have a JoinAddr filled in at this point.
		allowJoinAddr,
	); err != nil {
		return nil, err
	}
	if tc.clusterArgs.ReplicationMode == base.ReplicationManual {
		var stkCopy kvserver.StoreTestingKnobs
		if stk := serverArgs.Knobs.Store; stk != nil {
			stkCopy = *stk.(*kvserver.StoreTestingKnobs)
		}
		stkCopy.DisableLeaseQueue = true
		stkCopy.DisableSplitQueue = true
		stkCopy.DisableMergeQueue = true
		stkCopy.DisableReplicateQueue = true
		stkCopy.DisableStoreRebalancer = true
		serverArgs.Knobs.Store = &stkCopy
	}

	// Install listener, if non-empty.
	if serverArgs.Listener != nil {
		// Instantiate the server testing knobs if non-empty.
		if serverArgs.Knobs.Server == nil {
			serverArgs.Knobs.Server = &server.TestingKnobs{}
		} else {
			// Copy the knobs so the struct with the listener is not
			// reused for other nodes.
			knobs := *serverArgs.Knobs.Server.(*server.TestingKnobs)
			serverArgs.Knobs.Server = &knobs
		}

		// Install the provided listener.
		serverArgs.Knobs.Server.(*server.TestingKnobs).RPCListener = serverArgs.Listener
		serverArgs.Addr = serverArgs.Listener.Addr().String()
	}

	// Inject the decision that was made about whether or not to start a
	// test tenant server, into this new server's configuration.
	serverArgs.DefaultTestTenant = tc.defaultTestTenantOptions

	s, err := serverutils.NewServer(serverArgs)
	if err != nil {
		return nil, err
	}

	tc.Servers = append(tc.Servers, s)
	tc.serverArgs = append(tc.serverArgs, serverArgs)

	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.serverStoppers = append(tc.mu.serverStoppers, s.Stopper())
	return s, nil
}

// startServer is the companion method to AddServer, and is responsible for
// actually starting the server.
func (tc *TestCluster) startServer(idx int, serverArgs base.TestServerArgs) error {
	server := tc.Servers[idx]
	if err := server.Start(context.Background()); err != nil {
		return err
	}

	dbConn, err := server.ApplicationLayer().SQLConnE(serverutils.DBName(serverArgs.UseDatabase))
	if err != nil {
		return err
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.Conns = append(tc.Conns, dbConn)
	return nil
}

// WaitForNStores waits for N store descriptors to be gossiped. Servers other
// than the first "bootstrap" their stores asynchronously, but we'd like to have
// control over when stores get initialized before returning the TestCluster.
func (tc *TestCluster) WaitForNStores(t serverutils.TestFataler, n int, g *gossip.Gossip) {
	// Register a gossip callback for the store descriptors.
	var storesMu syncutil.Mutex
	stores := map[roachpb.StoreID]struct{}{}
	storesDone := make(chan error)
	storesDoneOnce := storesDone
	unregister := g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStoreDescPrefix),
		func(_ string, content roachpb.Value) {
			storesMu.Lock()
			defer storesMu.Unlock()
			if storesDoneOnce == nil {
				return
			}

			var desc roachpb.StoreDescriptor
			if err := content.GetProto(&desc); err != nil {
				storesDoneOnce <- err
				return
			}

			stores[desc.StoreID] = struct{}{}
			if len(stores) == n {
				close(storesDoneOnce)
				storesDoneOnce = nil
			}
		})
	defer unregister()

	// Wait for the store descriptors to be gossiped.
	var seen int
	for {
		select {
		case err := <-storesDone:
			seen++
			if err == nil {
				return // done
			}
			t.Fatal(err)
		case <-time.After(testutils.DefaultSucceedsSoonDuration):
			func() {
				storesMu.Lock()
				defer storesMu.Unlock()
				t.Fatalf("timed out waiting for %d store descriptors: %v", n-seen, stores)
			}()
		}
	}
}

// LookupRange is part of TestClusterInterface.
func (tc *TestCluster) LookupRange(key roachpb.Key) (roachpb.RangeDescriptor, error) {
	return tc.Servers[0].StorageLayer().LookupRange(key)
}

// LookupRangeOrFatal is part of TestClusterInterface.
func (tc *TestCluster) LookupRangeOrFatal(
	t serverutils.TestFataler, key roachpb.Key,
) roachpb.RangeDescriptor {
	t.Helper()
	desc, err := tc.LookupRange(key)
	if err != nil {
		t.Fatalf(`looking up range for %s: %+v`, key, err)
	}
	return desc
}

// SplitRangeWithExpiration splits the range containing splitKey with a sticky
// bit expiring at expirationTime.
// The right range created by the split starts at the split key and extends to the
// original range's end key.
// Returns the new descriptors of the left and right ranges.
//
// splitKey must correspond to a SQL table key (it must end with a family ID /
// col ID).
func (tc *TestCluster) SplitRangeWithExpiration(
	splitKey roachpb.Key, expirationTime hlc.Timestamp,
) (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error) {
	return tc.Servers[0].StorageLayer().SplitRangeWithExpiration(splitKey, expirationTime)
}

// SplitRange splits the range containing splitKey.
// The right range created by the split starts at the split key and extends to the
// original range's end key.
// Returns the new descriptors of the left and right ranges.
//
// splitKey must correspond to a SQL table key (it must end with a family ID /
// col ID).
func (tc *TestCluster) SplitRange(
	splitKey roachpb.Key,
) (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error) {
	return tc.Servers[0].StorageLayer().SplitRange(splitKey)
}

// SplitRangeOrFatal is the same as SplitRange but will Fatal the test on error.
func (tc *TestCluster) SplitRangeOrFatal(
	t serverutils.TestFataler, splitKey roachpb.Key,
) (roachpb.RangeDescriptor, roachpb.RangeDescriptor) {
	lhsDesc, rhsDesc, err := tc.Servers[0].StorageLayer().SplitRange(splitKey)
	if err != nil {
		t.Fatalf(`splitting at %s: %+v`, splitKey, err)
	}
	return lhsDesc, rhsDesc
}

// MergeRanges merges the range containing leftKey with the range to its right.
func (tc *TestCluster) MergeRanges(leftKey roachpb.Key) (roachpb.RangeDescriptor, error) {
	return tc.Servers[0].StorageLayer().MergeRanges(leftKey)
}

// MergeRangesOrFatal is the same as MergeRanges but will Fatal the test on
// error.
func (tc *TestCluster) MergeRangesOrFatal(
	t serverutils.TestFataler, leftKey roachpb.Key,
) roachpb.RangeDescriptor {
	mergedDesc, err := tc.MergeRanges(leftKey)
	if err != nil {
		t.Fatalf(`merging at %s: %+v`, leftKey, err)
	}
	return mergedDesc
}

// Target returns a ReplicationTarget for the specified server.
func (tc *TestCluster) Target(serverIdx int) roachpb.ReplicationTarget {
	s := tc.Servers[serverIdx].StorageLayer()
	return roachpb.ReplicationTarget{
		NodeID:  s.NodeID(),
		StoreID: s.GetFirstStoreID(),
	}
}

// Targets creates a slice of ReplicationTarget where each entry corresponds to
// a call to tc.Target() for serverIdx in serverIdxs.
func (tc *TestCluster) Targets(serverIdxs ...int) []roachpb.ReplicationTarget {
	ret := make([]roachpb.ReplicationTarget, 0, len(serverIdxs))
	for _, serverIdx := range serverIdxs {
		ret = append(ret, tc.Target(serverIdx))
	}
	return ret
}

func (tc *TestCluster) changeReplicas(
	changeType roachpb.ReplicaChangeType, startKey roachpb.RKey, targets ...roachpb.ReplicationTarget,
) (roachpb.RangeDescriptor, error) {
	tc.t.Helper()
	ctx := context.TODO()

	var returnErr error
	var desc *roachpb.RangeDescriptor
	if err := testutils.SucceedsSoonError(func() error {
		tc.t.Helper()
		var beforeDesc roachpb.RangeDescriptor
		db := tc.Servers[0].SystemLayer().DB()
		if err := db.GetProto(
			ctx, keys.RangeDescriptorKey(startKey), &beforeDesc,
		); err != nil {
			return errors.Wrap(err, "range descriptor lookup error")
		}
		var err error
		desc, err = db.AdminChangeReplicas(
			ctx, startKey.AsRawKey(), beforeDesc, kvpb.MakeReplicationChanges(changeType, targets...),
		)
		if kvserver.IsRetriableReplicationChangeError(err) {
			tc.t.Logf("encountered retriable replication change error: %v", err)
			return err
		}
		// Don't return blindly - if this isn't an error we think is related to a
		// replication error that we can retry, save the error to the outer scope
		// and return nil.
		returnErr = err
		return nil
	}); err != nil {
		returnErr = err
	}

	if returnErr != nil {
		// We mark the error as Handled so that tests that wanted the error in the
		// first attempt but spent a while spinning in the retry loop above will
		// fail. These should invoke ChangeReplicas directly.
		return roachpb.RangeDescriptor{}, errors.Handled(errors.Wrap(returnErr, "AdminChangeReplicas error"))
	}
	return *desc, nil
}

func (tc *TestCluster) addReplica(
	startKey roachpb.Key, typ roachpb.ReplicaChangeType, targets ...roachpb.ReplicationTarget,
) (roachpb.RangeDescriptor, error) {
	rKey := keys.MustAddr(startKey)

	rangeDesc, err := tc.changeReplicas(
		typ, rKey, targets...,
	)
	if err != nil {
		return roachpb.RangeDescriptor{}, err
	}

	if err := tc.waitForNewReplicas(startKey, false /* waitForVoter */, targets...); err != nil {
		return roachpb.RangeDescriptor{}, err
	}

	return rangeDesc, nil
}

// AddVoters is part of TestClusterInterface.
func (tc *TestCluster) AddVoters(
	startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) (roachpb.RangeDescriptor, error) {
	return tc.addReplica(startKey, roachpb.ADD_VOTER, targets...)
}

// AddNonVoters is part of TestClusterInterface.
func (tc *TestCluster) AddNonVoters(
	startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) (roachpb.RangeDescriptor, error) {
	return tc.addReplica(startKey, roachpb.ADD_NON_VOTER, targets...)
}

// AddNonVotersOrFatal is part of TestClusterInterface.
func (tc *TestCluster) AddNonVotersOrFatal(
	t serverutils.TestFataler, startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) roachpb.RangeDescriptor {
	desc, err := tc.addReplica(startKey, roachpb.ADD_NON_VOTER, targets...)
	if err != nil {
		t.Fatal(err)
	}
	return desc
}

// AddVotersMulti is part of TestClusterInterface.
func (tc *TestCluster) AddVotersMulti(
	kts ...serverutils.KeyAndTargets,
) ([]roachpb.RangeDescriptor, []error) {
	var descs []roachpb.RangeDescriptor
	var errs []error
	for _, kt := range kts {
		rKey := keys.MustAddr(kt.StartKey)

		rangeDesc, err := tc.changeReplicas(
			roachpb.ADD_VOTER, rKey, kt.Targets...,
		)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		descs = append(descs, rangeDesc)
	}

	for _, kt := range kts {
		if err := tc.waitForNewReplicas(kt.StartKey, false, kt.Targets...); err != nil {
			errs = append(errs, err)
			continue
		}
	}

	return descs, errs
}

// WaitForVoters waits for the targets to be voters in the range indicated by
// startKey.
func (tc *TestCluster) WaitForVoters(
	startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) error {
	return tc.waitForNewReplicas(startKey, true /* waitForVoter */, targets...)
}

// WaitForVotersOrFatal is the same as WaitForVoters but it will Fatal the test
// on error.
func (tc *TestCluster) WaitForVotersOrFatal(
	t serverutils.TestFataler, startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) {
	require.NoError(t, tc.WaitForVoters(startKey, targets...))
}

// waitForNewReplicas waits for each of the targets to have a fully initialized
// replica of the range indicated by startKey.
//
// startKey is start key of range.
//
// waitForVoter indicates that the method should wait until the targets are full
// voters in the range (and they also know that they're voters - i.e. the
// respective replica has caught up with the config change).
//
// targets are replication target for change replica.
//
// TODO(tbg): it seems silly that most callers pass `waitForVoter==false` even
// when they are adding a voter, and instead well over a dozen tests then go and
// call `.WaitForVoter` instead. It is very rare for a test to want to add a
// voter but not wait for this voter to show up on the target replica (perhaps
// when some strange error is injected) so the rare test should have to do the
// extra work instead.
func (tc *TestCluster) waitForNewReplicas(
	startKey roachpb.Key, waitForVoter bool, targets ...roachpb.ReplicationTarget,
) error {
	rKey := keys.MustAddr(startKey)
	errRetry := errors.Errorf("target not found")

	// Wait for the replication to complete on all destination nodes.
	if err := retry.ForDuration(time.Second*25, func() error {
		for _, target := range targets {
			// Use LookupReplica(keys) instead of GetRange(rangeID) to ensure that the
			// snapshot has been transferred and the descriptor initialized.
			store, err := tc.findMemberStore(target.StoreID)
			if err != nil {
				log.Errorf(context.TODO(), "unexpected error: %s", err)
				return err
			}
			repl := store.LookupReplica(rKey)
			if repl == nil {
				return errors.Wrapf(errRetry, "for target %s", target)
			}
			desc := repl.Desc()
			if replDesc, ok := desc.GetReplicaDescriptor(target.StoreID); !ok {
				return errors.Errorf("target store %d not yet in range descriptor %v", target.StoreID, desc)
			} else if waitForVoter && replDesc.Type != roachpb.VOTER_FULL {
				return errors.Errorf("target store %d not yet voter in range descriptor %v", target.StoreID, desc)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// AddVotersOrFatal is part of TestClusterInterface.
func (tc *TestCluster) AddVotersOrFatal(
	t serverutils.TestFataler, startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) roachpb.RangeDescriptor {
	t.Helper()
	desc, err := tc.AddVoters(startKey, targets...)
	if err != nil {
		t.Fatalf(`could not add %v replicas to range containing %s: %+v`,
			targets, startKey, err)
	}
	return desc
}

// RemoveVoters is part of the TestServerInterface.
func (tc *TestCluster) RemoveVoters(
	startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) (roachpb.RangeDescriptor, error) {
	return tc.changeReplicas(roachpb.REMOVE_VOTER, keys.MustAddr(startKey), targets...)
}

// RemoveVotersOrFatal is part of TestClusterInterface.
func (tc *TestCluster) RemoveVotersOrFatal(
	t serverutils.TestFataler, startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) roachpb.RangeDescriptor {
	t.Helper()
	desc, err := tc.RemoveVoters(startKey, targets...)
	if err != nil {
		t.Fatalf(`could not remove %v replicas from range containing %s: %+v`,
			targets, startKey, err)
	}
	return desc
}

// RemoveNonVoters is part of TestClusterInterface.
func (tc *TestCluster) RemoveNonVoters(
	startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) (roachpb.RangeDescriptor, error) {
	return tc.changeReplicas(roachpb.REMOVE_NON_VOTER, keys.MustAddr(startKey), targets...)
}

// RemoveNonVotersOrFatal is part of TestClusterInterface.
func (tc *TestCluster) RemoveNonVotersOrFatal(
	t serverutils.TestFataler, startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) roachpb.RangeDescriptor {
	desc, err := tc.RemoveNonVoters(startKey, targets...)
	if err != nil {
		t.Fatalf(`could not remove %v replicas from range containing %s: %+v`,
			targets, startKey, err)
	}
	return desc
}

// SwapVoterWithNonVoter is part of TestClusterInterface.
func (tc *TestCluster) SwapVoterWithNonVoter(
	startKey roachpb.Key, voterTarget, nonVoterTarget roachpb.ReplicationTarget,
) (*roachpb.RangeDescriptor, error) {
	ctx := context.Background()
	key := keys.MustAddr(startKey)
	var beforeDesc roachpb.RangeDescriptor
	if err := tc.Servers[0].SystemLayer().DB().GetProto(
		ctx, keys.RangeDescriptorKey(key), &beforeDesc,
	); err != nil {
		return nil, errors.Wrap(err, "range descriptor lookup error")
	}
	changes := []kvpb.ReplicationChange{
		{ChangeType: roachpb.ADD_VOTER, Target: nonVoterTarget},
		{ChangeType: roachpb.REMOVE_NON_VOTER, Target: nonVoterTarget},
		{ChangeType: roachpb.ADD_NON_VOTER, Target: voterTarget},
		{ChangeType: roachpb.REMOVE_VOTER, Target: voterTarget},
	}

	return tc.Servers[0].SystemLayer().DB().AdminChangeReplicas(ctx, key, beforeDesc, changes)
}

// SwapVoterWithNonVoterOrFatal is part of TestClusterInterface.
func (tc *TestCluster) SwapVoterWithNonVoterOrFatal(
	t serverutils.TestFataler,
	startKey roachpb.Key,
	voterTarget, nonVoterTarget roachpb.ReplicationTarget,
) *roachpb.RangeDescriptor {
	afterDesc, err := tc.SwapVoterWithNonVoter(startKey, voterTarget, nonVoterTarget)

	// Verify that the swap actually worked.
	require.NoError(t, err)
	replDesc, ok := afterDesc.GetReplicaDescriptor(voterTarget.StoreID)
	require.True(t, ok)
	require.Equal(t, roachpb.NON_VOTER, replDesc.Type)
	replDesc, ok = afterDesc.GetReplicaDescriptor(nonVoterTarget.StoreID)
	require.True(t, ok)
	require.Equal(t, roachpb.VOTER_FULL, replDesc.Type)

	return afterDesc
}

// RebalanceVoter is part of TestClusterInterface.
func (tc *TestCluster) RebalanceVoter(
	ctx context.Context, startKey roachpb.Key, src, dest roachpb.ReplicationTarget,
) (*roachpb.RangeDescriptor, error) {
	key := keys.MustAddr(startKey)
	var beforeDesc roachpb.RangeDescriptor
	if err := tc.Servers[0].SystemLayer().DB().GetProto(
		ctx, keys.RangeDescriptorKey(key), &beforeDesc,
	); err != nil {
		return nil, errors.Wrap(err, "range descriptor lookup error")
	}
	changes := []kvpb.ReplicationChange{
		{ChangeType: roachpb.REMOVE_VOTER, Target: src},
		{ChangeType: roachpb.ADD_VOTER, Target: dest},
	}
	return tc.Servers[0].SystemLayer().DB().AdminChangeReplicas(ctx, key, beforeDesc, changes)
}

// RebalanceVoterOrFatal is part of TestClusterInterface.
func (tc *TestCluster) RebalanceVoterOrFatal(
	ctx context.Context,
	t serverutils.TestFataler,
	startKey roachpb.Key,
	src, dest roachpb.ReplicationTarget,
) *roachpb.RangeDescriptor {
	afterDesc, err := tc.RebalanceVoter(ctx, startKey, src, dest)
	if err != nil {
		t.Fatalf("could not rebalance voter: %+v", err)
	}
	return afterDesc
}

// TransferRangeLease is part of the TestServerInterface.
func (tc *TestCluster) TransferRangeLease(
	rangeDesc roachpb.RangeDescriptor, dest roachpb.ReplicationTarget,
) error {
	err := tc.Servers[0].SystemLayer().DB().AdminTransferLease(context.TODO(),
		rangeDesc.StartKey.AsRawKey(), dest.StoreID)
	if err != nil {
		return errors.Wrapf(err, "%q: transfer lease unexpected error", rangeDesc.StartKey)
	}
	return nil
}

// TransferRangeLeaseOrFatal is a convenience version of TransferRangeLease
func (tc *TestCluster) TransferRangeLeaseOrFatal(
	t serverutils.TestFataler, rangeDesc roachpb.RangeDescriptor, dest roachpb.ReplicationTarget,
) {
	if err := tc.TransferRangeLease(rangeDesc, dest); err != nil {
		t.Fatalf(`could not transfer lease for range %s error is %+v`, rangeDesc, err)
	}
}

// MaybeWaitForLeaseUpgrade waits until the lease held for the given range
// descriptor is upgraded to an epoch-based or leader lease, but only if we
// expect the lease to be upgraded.
func (tc *TestCluster) MaybeWaitForLeaseUpgrade(
	ctx context.Context, t serverutils.TestFataler, desc roachpb.RangeDescriptor,
) {
	if kvserver.ExpirationLeasesOnly.Get(&tc.Server(0).ClusterSettings().SV) {
		return
	}
	tc.WaitForLeaseUpgrade(ctx, t, desc)
}

// WaitForLeaseUpgrade waits until the lease held for the given range descriptor
// is upgraded to either a leader-lease or an epoch-based lease.
func (tc *TestCluster) WaitForLeaseUpgrade(
	ctx context.Context, t serverutils.TestFataler, desc roachpb.RangeDescriptor,
) (roachpb.Lease, kvserverpb.LeaseStatus) {
	require.False(t, kvserver.ExpirationLeasesOnly.Get(&tc.Server(0).ClusterSettings().SV),
		"cluster configured to only use expiration leases")
	var l roachpb.Lease
	testutils.SucceedsSoon(t, func() error {
		li, _, err := tc.FindRangeLeaseEx(ctx, desc, nil)
		require.NoError(t, err)
		l = li.Current()
		if l.Type() == roachpb.LeaseExpiration {
			return errors.Errorf("lease still an expiration based lease")
		}
		t.Logf("lease is now of type: %s", l.Type())
		return nil
	})

	store := tc.GetFirstStoreFromServer(t, 0)
	repl := store.LookupReplica(desc.StartKey)

	return l, repl.CurrentLeaseStatus(ctx)
}

// RemoveLeaseHolderOrFatal is a convenience version of TransferRangeLease and RemoveVoter
func (tc *TestCluster) RemoveLeaseHolderOrFatal(
	t serverutils.TestFataler,
	rangeDesc roachpb.RangeDescriptor,
	src roachpb.ReplicationTarget,
	dest roachpb.ReplicationTarget,
) {
	testutils.SucceedsSoon(t, func() error {
		if err := tc.TransferRangeLease(rangeDesc, dest); err != nil {
			return err
		}
		if _, err := tc.RemoveVoters(rangeDesc.StartKey.AsRawKey(), src); err != nil {
			if strings.Contains(err.Error(), "to remove self (leaseholder)") {
				return err
			}
			t.Fatal(err)
		}
		return nil
	})
}

// MoveRangeLeaseNonCooperatively is part of the TestClusterInterface.
func (tc *TestCluster) MoveRangeLeaseNonCooperatively(
	t *testing.T,
	ctx context.Context,
	rangeDesc roachpb.RangeDescriptor,
	dest roachpb.ReplicationTarget,
	manual *hlc.HybridManualClock,
) (*roachpb.Lease, error) {
	knobs := tc.clusterArgs.ServerArgs.Knobs.Store.(*kvserver.StoreTestingKnobs)
	if !knobs.AllowLeaseRequestProposalsWhenNotLeader {
		// Without this knob, we'd have to architect a Raft leadership change
		// too in order to let the replica get the lease. It's easier to just
		// require that callers set it.
		return nil, errors.Errorf("must set StoreTestingKnobs.AllowLeaseRequestProposalsWhenNotLeader")
	}

	destServer, err := tc.FindMemberServer(dest.StoreID)
	if err != nil {
		return nil, err
	}
	destStore, err := destServer.GetStores().(*kvserver.Stores).GetStore(dest.StoreID)
	if err != nil {
		return nil, err
	}

	// We are going to advance the manual clock so that the current lease
	// expires and then issue a request to the target in hopes that it grabs the
	// lease. But it is possible that another replica grabs the lease before us
	// when it's up for grabs. To handle that case, we wrap the entire operation
	// in an outer retry loop.
	var newLease *roachpb.Lease
	testutils.SucceedsWithin(t, func() error {
		// Find the current lease.
		prevLease, _, err := tc.FindRangeLease(rangeDesc, nil /* hint */)
		if err != nil {
			return err
		}
		if prevLease.Replica.StoreID == dest.StoreID {
			newLease = &prevLease
			return nil
		}

		// Advance the manual clock past the lease's expiration.
		lhStore, err := tc.findMemberStore(prevLease.Replica.StoreID)
		if err != nil {
			return err
		}
		log.Infof(ctx, "test: advancing clock to lease expiration")
		manual.Increment(lhStore.GetStoreConfig().LeaseExpiration())

		// Heartbeat the destination server's liveness record so that if we are
		// attempting to acquire an epoch-based lease, the server will be live.
		err = destServer.HeartbeatNodeLiveness()
		if err != nil {
			return err
		}

		// Issue a request to the target replica, which should notice that the
		// old lease has expired and that it can acquire the lease.
		r, err := destStore.GetReplica(rangeDesc.RangeID)
		if err != nil {
			return err
		}
		ls, err := r.TestingAcquireLease(ctx)
		if err != nil {
			log.Infof(ctx, "TestingAcquireLease failed: %s", err)
			if lErr := (*kvpb.NotLeaseHolderError)(nil); errors.As(err, &lErr) && lErr.Lease != nil {
				newLease = lErr.Lease
			} else {
				return err
			}
		} else {
			newLease = &ls.Lease
		}

		// Is the lease in the right place?
		if newLease.Replica.StoreID != dest.StoreID {
			if newLease.Type() == roachpb.LeaseLeader {
				// With leader leases, we want to current leader to step down to give
				// the new leader a chance to acquire the lease.
				// TODO(ibrahim): instead of waiting for the leader to step down and
				// hope that the new leader be in the right place, we should instead
				// establish leadership in the right place first.
				if err :=
					tc.ensureLeaderStepsDown(t, ctx, rangeDesc, manual); err != nil {
					return err
				}
			}
			return errors.Errorf("LeaseInfoRequest succeeded, "+
				"but lease in wrong location, want %v, got %v", dest, newLease.Replica)
		}
		return nil
	}, 3*testutils.SucceedsSoonDuration())
	log.Infof(ctx, "MoveRangeLeaseNonCooperatively: acquired lease: %s. err: %v", newLease, err)
	return newLease, err
}

// ensureLeaderStepsDown withdraws store liveness support from the leader, and
// waits for it to step down.
func (tc *TestCluster) ensureLeaderStepsDown(
	t *testing.T,
	ctx context.Context,
	rangeDesc roachpb.RangeDescriptor,
	manual *hlc.HybridManualClock,
) error {
	var leaderStore *kvserver.Store
	var leaderNode serverutils.TestServerInterface
	var leaderReplica *kvserver.Replica

	// Wait until we find a leader for the range, and record it.
	testutils.SucceedsSoon(t, func() error {
		log.Infof(ctx, "waiting for a leader to step up")
		for _, s := range tc.Servers {
			curStore, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
			if err != nil {
				return err
			}

			curR, err := curStore.GetReplica(rangeDesc.RangeID)
			if err != nil {
				return err
			}

			if curR.RaftStatus().RaftState == raftpb.StateLeader {
				log.Infof(ctx, "current leader is %v at term: %d", curR.RaftStatus().ID,
					curR.RaftStatus().Term)
				leaderStore = curStore
				leaderNode = s
				leaderReplica = curR
			}
		}
		// At this point we have iterated over all nodes in the cluster, if we
		// haven't found a leader, wait for a bit for one to step up.
		if leaderStore == nil {
			return errors.Errorf("no leader found")
		}
		return nil
	})

	// Block store liveness messages to the current leader.
	leaderNode.StoreLivenessTransport().(*storeliveness.Transport).
		ListenMessages(leaderStore.StoreID(),
			&storeliveness.UnreliableHandler{
				MessageHandler: leaderStore.TestingStoreLivenessSupportManager(),
				UnreliableHandlerFuncs: storeliveness.UnreliableHandlerFuncs{
					DropStoreLivenessMsg: func(msg *storelivenesspb.Message) bool {
						return true
					},
				},
			})

	// Advance the manual clock past the lease's expiration.
	log.Infof(ctx, "test: advancing clock to lease expiration")
	manual.Increment(leaderStore.GetStoreConfig().LeaseExpiration())

	// Wait for the leader to step down. Sometimes this might take a while since
	// the leader might be replicating to other followers, and it won't step down
	// unless it doesn't receive anything from the followers for a while.
	// TODO(ibrahim): This could be made faster by blocking Raft messages to
	// the leader.
	testutils.SucceedsWithin(t, func() error {
		if leaderReplica.RaftStatus().RaftState == raftpb.StateLeader {
			return errors.Errorf("leader hasn't stepped down yet")
		}
		return nil
	}, 2*testutils.SucceedsSoonDuration())

	// Restore store liveness state to normal.
	leaderNode.StoreLivenessTransport().(*storeliveness.Transport).
		ListenMessages(leaderStore.StoreID(),
			&storeliveness.UnreliableHandler{
				MessageHandler: leaderStore.TestingStoreLivenessSupportManager(),
				UnreliableHandlerFuncs: storeliveness.UnreliableHandlerFuncs{
					DropStoreLivenessMsg: func(msg *storelivenesspb.Message) bool {
						return false
					},
				},
			})

	return nil
}

// FindRangeLease is similar to FindRangeLeaseHolder but returns a Lease proto
// without verifying if the lease is still active. Instead, it returns a time-
// stamp taken off the queried node's clock.
//
// DEPRECATED - use FindRangeLeaseEx instead.
func (tc *TestCluster) FindRangeLease(
	rangeDesc roachpb.RangeDescriptor, hint *roachpb.ReplicationTarget,
) (_ roachpb.Lease, now hlc.ClockTimestamp, _ error) {
	l, now, err := tc.FindRangeLeaseEx(context.TODO(), rangeDesc, hint)
	if err != nil {
		return roachpb.Lease{}, hlc.ClockTimestamp{}, err
	}
	return l.CurrentOrProspective(), now, err
}

// FindRangeLeaseEx is part of TestClusterInterface.
func (tc *TestCluster) FindRangeLeaseEx(
	ctx context.Context, rangeDesc roachpb.RangeDescriptor, hint *roachpb.ReplicationTarget,
) (_ roachpb.LeaseInfo, now hlc.ClockTimestamp, _ error) {
	var queryPolicy roachpb.LeaseInfoOpt
	if hint != nil {
		var ok bool
		if _, ok = rangeDesc.GetReplicaDescriptor(hint.StoreID); !ok {
			return roachpb.LeaseInfo{}, hlc.ClockTimestamp{}, errors.Errorf(
				"bad hint: %+v; store doesn't have a replica of the range", hint)
		}
		queryPolicy = roachpb.QueryLocalNodeOnly
	} else {
		hint = &roachpb.ReplicationTarget{
			NodeID:  rangeDesc.Replicas().Descriptors()[0].NodeID,
			StoreID: rangeDesc.Replicas().Descriptors()[0].StoreID}
		queryPolicy = roachpb.AllowQueryToBeForwardedToDifferentNode
	}

	// Find the server indicated by the hint and send a LeaseInfoRequest through
	// it.
	hintServer, err := tc.FindMemberServer(hint.StoreID)
	if err != nil {
		return roachpb.LeaseInfo{}, hlc.ClockTimestamp{}, errors.Wrapf(err, "bad hint: %+v; no such node", hint)
	}

	return hintServer.StorageLayer().GetRangeLease(ctx, rangeDesc.StartKey.AsRawKey(), queryPolicy)
}

// FindRangeLeaseHolder is part of TestClusterInterface.
func (tc *TestCluster) FindRangeLeaseHolder(
	rangeDesc roachpb.RangeDescriptor, hint *roachpb.ReplicationTarget,
) (roachpb.ReplicationTarget, error) {
	lease, now, err := tc.FindRangeLease(rangeDesc, hint)
	if err != nil {
		return roachpb.ReplicationTarget{}, err
	}
	// Find lease replica in order to examine the lease state.
	store, err := tc.findMemberStore(lease.Replica.StoreID)
	if err != nil {
		return roachpb.ReplicationTarget{}, err
	}
	replica, err := store.GetReplica(rangeDesc.RangeID)
	if err != nil {
		return roachpb.ReplicationTarget{}, err
	}
	if !replica.LeaseStatusAt(context.TODO(), now).IsValid() {
		return roachpb.ReplicationTarget{}, errors.New("no valid lease")
	}
	replicaDesc := lease.Replica
	return roachpb.ReplicationTarget{NodeID: replicaDesc.NodeID, StoreID: replicaDesc.StoreID}, nil
}

// ScratchRange returns the start key of a span of keyspace suitable for use as
// kv scratch space (it doesn't overlap system spans or SQL tables). The range
// is lazily split off on the first call to ScratchRange.
func (tc *TestCluster) ScratchRange(t serverutils.TestFataler) roachpb.Key {
	scratchKey, err := tc.Servers[0].StorageLayer().ScratchRange()
	if err != nil {
		t.Fatal(err)
	}
	return scratchKey
}

// ScratchRangeWithExpirationLease returns the start key of a span of keyspace
// suitable for use as kv scratch space and that has an expiration based lease.
// The range is lazily split off on the first call to ScratchRangeWithExpirationLease.
func (tc *TestCluster) ScratchRangeWithExpirationLease(t serverutils.TestFataler) roachpb.Key {
	scratchKey, err := tc.Servers[0].StorageLayer().ScratchRangeWithExpirationLease()
	if err != nil {
		t.Fatal(err)
	}
	return scratchKey
}

// WaitForSplitAndInitialization waits for a range which starts with startKey
// and then verifies that each replica in the range descriptor has been created.
//
// NB: This doesn't actually wait for full upreplication to whatever the zone
// config specifies.
func (tc *TestCluster) WaitForSplitAndInitialization(startKey roachpb.Key) error {
	return retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
		desc, err := tc.LookupRange(startKey)
		if err != nil {
			return errors.Wrapf(err, "unable to lookup range for %s", startKey)
		}
		// Verify the split first.
		if !desc.StartKey.Equal(startKey) {
			return errors.Errorf("expected range start key %s; got %s",
				startKey, desc.StartKey)
		}
		// Once we've verified the split, make sure that replicas exist.
		for _, rDesc := range desc.Replicas().Descriptors() {
			store, err := tc.findMemberStore(rDesc.StoreID)
			if err != nil {
				return err
			}
			repl, err := store.GetReplica(desc.RangeID)
			if err != nil {
				return err
			}
			actualReplicaDesc, err := repl.GetReplicaDescriptor()
			if err != nil {
				return err
			}
			if !actualReplicaDesc.Equal(rDesc) {
				return errors.Errorf("expected replica %s; got %s", rDesc, actualReplicaDesc)
			}
		}
		return nil
	})
}

// FindMemberServer returns the server containing a given store.
func (tc *TestCluster) FindMemberServer(
	storeID roachpb.StoreID,
) (serverutils.TestServerInterface, error) {
	for _, server := range tc.Servers {
		if server.StorageLayer().GetStores().(*kvserver.Stores).HasStore(storeID) {
			return server, nil
		}
	}
	return nil, errors.Errorf("store not found")
}

// findMemberStore returns the store with a given ID.
func (tc *TestCluster) findMemberStore(storeID roachpb.StoreID) (*kvserver.Store, error) {
	server, err := tc.FindMemberServer(storeID)
	if err != nil {
		return nil, err
	}
	return server.StorageLayer().GetStores().(*kvserver.Stores).GetStore(storeID)
}

// WaitForFullReplication waits until all stores in the cluster
// have no ranges with replication pending.
//
// TODO(andrei): This method takes inexplicably long.
// I think it shouldn't need any retries. See #38565.
func (tc *TestCluster) WaitForFullReplication() error {
	log.Infof(context.TODO(), "WaitForFullReplication")
	start := timeutil.Now()
	defer func() {
		took := timeutil.Since(start)
		debugTimings.WaitForRepDuration.Add(took.Nanoseconds())
		log.Infof(context.TODO(), "WaitForFullReplication took: %s", took)
	}()

	if len(tc.Servers) < 3 {
		// If we have less than three nodes, we will never have full replication.
		return nil
	}

	opts := retry.Options{
		InitialBackoff: time.Millisecond * 10,
		MaxBackoff:     time.Millisecond * 100,
		Multiplier:     2,
	}

	notReplicated := true
	for r := retry.Start(opts); r.Next() && notReplicated; {
		notReplicated = false
		for _, s := range tc.Servers {
			err := s.StorageLayer().GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				if n := s.ClusterNodeCount(); n != len(tc.Servers) {
					log.Infof(context.TODO(), "%s only sees %d/%d available nodes", s, n, len(tc.Servers))
					notReplicated = true
					return nil
				}
				// Force upreplication. Otherwise, if we rely on the scanner to do it,
				// it'll take a while.
				if err := s.ForceReplicationScanAndProcess(); err != nil {
					log.Infof(context.TODO(), "%v", err)
					notReplicated = true
					return nil
				}
				if err := s.ComputeMetrics(context.TODO()); err != nil {
					// This can sometimes fail since ComputeMetrics calls
					// updateReplicationGauges which needs the system config gossiped.
					log.Infof(context.TODO(), "%v", err)
					notReplicated = true
					return nil
				}
				if n := s.Metrics().UnderReplicatedRangeCount.Value(); n > 0 {
					log.Infof(context.TODO(), "%s has %d underreplicated ranges", s, n)
					notReplicated = true
				}
				if n := s.Metrics().OverReplicatedRangeCount.Value(); n > 0 {
					log.Infof(context.TODO(), "%s has %d overreplicated ranges", s, n)
					notReplicated = true
				}
				return nil
			})
			if err != nil {
				return err
			}
			if notReplicated {
				break
			}
		}
	}
	return nil
}

// WaitFor5NodeReplication ensures that zone configs are applied and
// up-replication is performed with new zone configs. This is the case for 5+
// node clusters.
// TODO: This code should be moved into WaitForFullReplication once #99812 is
// fixed so that all test would benefit from this check implicitly.
// This bug currently prevents LastUpdated to tick in metamorphic tests
// with kv.expiration_leases_only.enabled = true.
func (tc *TestCluster) WaitFor5NodeReplication() error {
	if len(tc.Servers) > 4 && tc.ReplicationMode() == base.ReplicationAuto {
		// We need to wait for zone config propagations before we could check
		// conformance since zone configs are propagated synchronously.
		// Generous timeout is added to allow rangefeeds to catch up. On startup
		// they could get delayed making test to fail.
		err := tc.WaitForZoneConfigPropagation()
		if err != nil {
			return err
		}
		return tc.WaitForFullReplication()
	}
	return nil
}

// WaitForZoneConfigPropagation ensures that all span config subscribers caught
// up till now. That would guarantee that zone configs set prior to this call
// are applied.
func (tc *TestCluster) WaitForZoneConfigPropagation() error {
	now := tc.Server(0).SystemLayer().Clock().Now()
	for _, s := range tc.Servers {
		scs := s.SpanConfigKVSubscriber().(spanconfig.KVSubscriber)
		if err := testutils.SucceedsSoonError(func() error {
			if scs.LastUpdated().Less(now) {
				return errors.New("zone configs not propagated")
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// WaitForNodeStatuses waits until a NodeStatus is persisted for every node and
// store in the cluster.
func (tc *TestCluster) WaitForNodeStatuses(t serverutils.TestFataler) {
	testutils.SucceedsSoon(t, func() error {
		// Note: we avoid using .RPCClientConn() here to avoid accumulating
		// stopper closures in RAM during the SucceedsSoon iterations.
		srv := tc.Server(0).SystemLayer()
		conn, err := srv.RPCContext().GRPCDialNode(
			srv.AdvRPCAddr(),
			tc.Server(0).StorageLayer().NodeID(),
			roachpb.Locality{},
			rpc.DefaultClass,
		).Connect(context.TODO())
		if err != nil {
			return err
		}
		client := serverpb.NewStatusClient(conn)
		response, err := client.Nodes(context.Background(), &serverpb.NodesRequest{})
		if err != nil {
			return err
		}

		if len(response.Nodes) < tc.NumServers() {
			return fmt.Errorf("expected %d nodes registered, got %+v", tc.NumServers(), response)
		}

		// Check that all the nodes in the testcluster have a status. We tolerate
		// other nodes having statuses (in some tests the cluster is configured with
		// a pre-existing store).
		nodeIDs := make(map[roachpb.NodeID]bool)
		for _, node := range response.Nodes {
			if len(node.StoreStatuses) == 0 {
				return fmt.Errorf("missing StoreStatuses in NodeStatus: %+v", node)
			}
			nodeIDs[node.Desc.NodeID] = true
		}
		for _, s := range tc.Servers {
			// Not using s.NodeID() here, on purpose. s.NodeID() uses the
			// in-RAM version in the RPC context, which is set earlier than
			// the node descriptor.
			if id := s.StorageLayer().Node().(*server.Node).Descriptor.NodeID; !nodeIDs[id] {
				return fmt.Errorf("missing n%d in NodeStatus: %+v", id, response)
			}
		}
		return nil
	})
}

// WaitForNodeLiveness waits until a liveness record is persisted for every
// node in the cluster.
func (tc *TestCluster) WaitForNodeLiveness(t serverutils.TestFataler) {
	testutils.SucceedsSoon(t, func() error {
		db := tc.Servers[0].SystemLayer().DB()
		for _, s := range tc.Servers {
			nodeID := s.StorageLayer().NodeID()
			key := keys.NodeLivenessKey(nodeID)
			var liveness livenesspb.Liveness
			if err := db.GetProto(context.Background(), key, &liveness); err != nil {
				return err
			}
			if (liveness == livenesspb.Liveness{}) {
				return fmt.Errorf("no liveness record")
			}
			if liveness.Epoch < 1 {
				return fmt.Errorf("liveness not incremented")
			}
			fmt.Printf("n%d: found liveness\n", nodeID)
		}
		return nil
	})
}

// ReplicationMode implements TestClusterInterface.
func (tc *TestCluster) ReplicationMode() base.TestClusterReplicationMode {
	return tc.clusterArgs.ReplicationMode
}

// ToggleReplicateQueues implements TestClusterInterface.
func (tc *TestCluster) ToggleReplicateQueues(active bool) {
	for _, s := range tc.Servers {
		_ = s.StorageLayer().GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
			store.TestingSetReplicateQueueActive(active)
			return nil
		})
	}
}

// ToggleSplitQueues implements TestClusterInterface.
func (tc *TestCluster) ToggleSplitQueues(active bool) {
	for _, s := range tc.Servers {
		_ = s.StorageLayer().GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
			store.TestingSetSplitQueueActive(active)
			return nil
		})
	}
}

// ToggleLeaseQueues implements TestClusterInterface.
func (tc *TestCluster) ToggleLeaseQueues(active bool) {
	for _, s := range tc.Servers {
		_ = s.StorageLayer().GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
			store.TestingSetLeaseQueueActive(active)
			return nil
		})
	}
}

// ReadIntFromStores reads the current integer value at the given key
// from all configured engines on un-stopped servers, filling in zeros
// when the value is not found.
func (tc *TestCluster) ReadIntFromStores(key roachpb.Key) []int64 {
	results := make([]int64, len(tc.Servers))
	for i, server := range tc.Servers {
		// Skip stopped servers, leaving their value as zero.
		if tc.ServerStopped(i) {
			continue
		}
		sl := server.StorageLayer()
		clock := server.SystemLayer().Clock()
		err := sl.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
			valRes, err := storage.MVCCGet(context.Background(), s.TODOEngine(), key,
				clock.Now(), storage.MVCCGetOptions{})
			if err != nil {
				log.VEventf(context.Background(), 1, "store %d: error reading from key %s: %s", s.StoreID(), key, err)
			} else if valRes.Value == nil {
				log.VEventf(context.Background(), 1, "store %d: missing key %s", s.StoreID(), key)
			} else {
				results[i], err = valRes.Value.GetInt()
				if err != nil {
					log.Errorf(context.Background(), "store %d: error decoding %s from key %s: %+v", s.StoreID(), valRes.Value, key, err)
				}
			}
			return nil
		})
		if err != nil {
			log.VEventf(context.Background(), 1, "node %d: error reading from key %s: %s", server.NodeID(), key, err)
		}
	}
	return results
}

// WaitForValues waits up to the given duration for the integer values
// at the given key to match the expected slice (across all stores).
// Fails the test if they do not match.
func (tc *TestCluster) WaitForValues(t serverutils.TestFataler, key roachpb.Key, expected []int64) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		actual := tc.ReadIntFromStores(key)
		if !reflect.DeepEqual(expected, actual) {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})
}

// GetFirstStoreFromServer get the first store from the specified server.
func (tc *TestCluster) GetFirstStoreFromServer(
	t serverutils.TestFataler, server int,
) *kvserver.Store {
	ts := tc.Servers[server].StorageLayer()
	store, pErr := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
	if pErr != nil {
		t.Fatal(pErr)
	}
	return store
}

// Restart stops and then starts all the servers in the cluster.
func (tc *TestCluster) Restart() error {
	for i := range tc.Servers {
		tc.StopServer(i)
		if err := tc.RestartServer(i); err != nil {
			return err
		}
	}
	return nil
}

// RestartServer uses the cached ServerArgs to restart a Server specified by
// the passed index.
func (tc *TestCluster) RestartServer(idx int) error {
	return tc.RestartServerWithInspect(idx, nil)
}

// RestartServerWithInspect uses the cached ServerArgs to restart a Server
// specified by the passed index. We allow an optional inspect function to be
// passed in that can observe the server once its been re-created but before it's
// been started. This is useful for tests that want to capture that the startup
// sequence performs the correct actions i.e. that on startup liveness is gossiped.
func (tc *TestCluster) RestartServerWithInspect(
	idx int, inspect func(s serverutils.TestServerInterface),
) error {
	if !tc.ServerStopped(idx) {
		return errors.Errorf("server %d must be stopped before attempting to restart", idx)
	}
	serverArgs := tc.serverArgs[idx]

	if ln := tc.reusableListeners[idx]; ln != nil {
		serverArgs.Listener = ln
	}

	if serverArgs.Listener == nil {
		if idx == 0 {
			// If it's the first server, then we need to restart the RPC listener by hand.
			// Look at NewTestCluster for more details.
			listener, err := net.Listen("tcp", serverArgs.Listener.Addr().String())
			if err != nil {
				return err
			}
			serverArgs.Listener = listener
			serverArgs.Knobs.Server.(*server.TestingKnobs).RPCListener = serverArgs.Listener
		} else {
			serverArgs.Addr = ""
			// Try and point the server to a live server in the cluster to join.
			for i := range tc.Servers {
				if !tc.ServerStopped(i) {
					serverArgs.JoinAddr = tc.Servers[i].SystemLayer().AdvRPCAddr()
				}
			}
		}
	} else if ln, ok := serverArgs.Listener.(*listenerutil.ReusableListener); !ok {
		// Restarting a server without a reusable listener can cause flakes since the
		// port may be occupied by a different process now. Use a reusable listener
		// to avoid that problem.
		return errors.Errorf(
			"ReusableListeners must be set on ClusterArgs or compatible Listener "+
				"needs to be set in serverArgs to restart server %d", idx,
		)
	} else if err := ln.Reopen(); err != nil {
		return err
	}

	for i, specs := range serverArgs.StoreSpecs {
		if specs.InMemory && specs.StickyVFSID == "" {
			return errors.Errorf("failed to restart Server %d, because a restart can only be used on a server with a sticky VFS", i)
		}
	}
	s, err := serverutils.NewServer(serverArgs)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if err := func() error {
		func() {
			// Only lock the assignment of the server and the stopper and the call to the inspect function.
			// This ensures that the stopper's Stop() method can abort an async Start() call.
			tc.mu.Lock()
			defer tc.mu.Unlock()
			tc.Servers[idx] = s
			tc.mu.serverStoppers[idx] = s.Stopper()

			if inspect != nil {
				inspect(s)
			}
		}()

		if err := s.Start(ctx); err != nil {
			return err
		}

		dbConn, err := s.ApplicationLayer().SQLConnE(serverutils.DBName(serverArgs.UseDatabase))
		if err != nil {
			return err
		}
		tc.Conns[idx] = dbConn
		return nil
	}(); err != nil {
		return err
	}

	// Wait until the other nodes can successfully connect to the newly restarted
	// node. This is useful to avoid flakes: the newly restarted node is now on a
	// different port, and a cycle of gossip is necessary to make all other nodes
	// aware.
	id := s.StorageLayer().NodeID()
	return timeutil.RunWithTimeout(
		ctx, "check-conn", 15*time.Second,
		func(ctx context.Context) error {
			r := retry.StartWithCtx(ctx, retry.Options{
				InitialBackoff: 1 * time.Millisecond,
				MaxBackoff:     100 * time.Millisecond,
			})
			var err error
			for r.Next() {
				err = func() error {
					for idx, s := range tc.Servers {
						if tc.ServerStopped(idx) {
							continue
						}
						for i := 0; i < rpc.NumConnectionClasses; i++ {
							class := rpc.ConnectionClass(i)
							otherID := s.StorageLayer().NodeID()
							if _, err := s.SystemLayer().NodeDialer().(*nodedialer.Dialer).Dial(ctx, id, class); err != nil {
								return errors.Wrapf(err, "connecting n%d->n%d (class %v)", otherID, id, class)
							}
						}
					}
					return nil
				}()
				if err != nil {
					continue
				}
				return nil
			}
			if err != nil {
				return err
			}
			return ctx.Err()
		})
}

// ServerStopped determines if a server has been explicitly
// stopped by StopServer(s).
func (tc *TestCluster) ServerStopped(idx int) bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.serverStoppers[idx] == nil
}

// GetRaftLeader returns the replica that is the current raft leader for the
// specified key.
func (tc *TestCluster) GetRaftLeader(
	t serverutils.TestFataler, key roachpb.RKey,
) *kvserver.Replica {
	t.Helper()
	var raftLeaderRepl *kvserver.Replica
	testutils.SucceedsSoon(t, func() error {
		var latestTerm uint64
		for i := range tc.Servers {
			err := tc.Servers[i].StorageLayer().GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
				repl := store.LookupReplica(key)
				if repl == nil {
					// Replica does not exist on this store or there is no raft
					// status yet.
					return nil
				}
				raftStatus := repl.RaftStatus()
				if raftStatus == nil {
					return errors.Errorf("raft group is not initialized for replica with key %s", key)
				}
				if raftStatus.Term > latestTerm || (raftLeaderRepl == nil && raftStatus.Term == latestTerm) {
					// If we find any newer term, it means any previous election is
					// invalid.
					raftLeaderRepl = nil
					latestTerm = raftStatus.Term
					if raftStatus.RaftState == raftpb.StateLeader {
						raftLeaderRepl = repl
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		if latestTerm == 0 || raftLeaderRepl == nil {
			return errors.Errorf("could not find a raft leader for key %s", key)
		}
		return nil
	})
	return raftLeaderRepl
}

// GetAdminClient gets the severpb.AdminClient for the specified server.
func (tc *TestCluster) GetAdminClient(
	t serverutils.TestFataler, serverIdx int,
) serverpb.AdminClient {
	return tc.Server(serverIdx).GetAdminClient(t)
}

// GetStatusClient gets the severpb.StatusClient for the specified server.
func (tc *TestCluster) GetStatusClient(
	t serverutils.TestFataler, serverIdx int,
) serverpb.StatusClient {
	return tc.Server(serverIdx).GetStatusClient(t)
}

// SplitTable implements TestClusterInterface.
func (tc *TestCluster) SplitTable(
	t serverutils.TestFataler, desc catalog.TableDescriptor, sps []serverutils.SplitPoint,
) {
	if tc.ReplicationMode() != base.ReplicationManual {
		t.Fatal("SplitTable called on a test cluster that was not in manual replication mode")
	}

	rkts := make(map[roachpb.RangeID]rangeAndKT)
	for _, sp := range sps {
		pik, err := randgen.TestingMakePrimaryIndexKey(desc, sp.Vals...)
		if err != nil {
			t.Fatal(err)
		}

		_, rightRange, err := tc.Server(0).SplitRange(pik)
		if err != nil {
			t.Fatal(err)
		}

		rightRangeStartKey := rightRange.StartKey.AsRawKey()
		target := tc.Target(sp.TargetNodeIdx)

		rkts[rightRange.RangeID] = rangeAndKT{
			rightRange,
			serverutils.KeyAndTargets{StartKey: rightRangeStartKey, Targets: []roachpb.ReplicationTarget{target}}}
	}

	var kts []serverutils.KeyAndTargets
	for _, rkt := range rkts {
		kts = append(kts, rkt.kt)
	}
	descs, errs := tc.AddVotersMulti(kts...)
	for _, err := range errs {
		if err != nil && !testutils.IsError(err, "is already present") {
			t.Fatal(err)
		}
	}

	for _, desc := range descs {
		rkt, ok := rkts[desc.RangeID]
		if !ok {
			continue
		}

		for _, target := range rkt.kt.Targets {
			if err := tc.TransferRangeLease(desc, target); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// GrantTenantCapabilities implements TestClusterInterface.
func (tc *TestCluster) GrantTenantCapabilities(
	ctx context.Context,
	t serverutils.TestFataler,
	tenID roachpb.TenantID,
	targetCaps map[tenantcapabilities.ID]string,
) {
	require.NoError(t, tc.Server(0).TenantController().GrantTenantCapabilities(ctx, tenID, targetCaps))
	tc.WaitForTenantCapabilities(t, tenID, targetCaps)
}

// WaitForTenantCapabilities implements TestClusterInterface.
func (tc *TestCluster) WaitForTenantCapabilities(
	t serverutils.TestFataler, tenID roachpb.TenantID, targetCaps map[tenantcapabilities.ID]string,
) {
	for i, ts := range tc.Servers {
		serverutils.WaitForTenantCapabilities(t, ts, tenID, targetCaps, fmt.Sprintf("server %d", i))
	}
}

type rangeAndKT struct {
	rangeDesc roachpb.RangeDescriptor
	kt        serverutils.KeyAndTargets
}

type testClusterFactoryImpl struct{}

// TestClusterFactory can be passed to serverutils.InitTestClusterFactory
var TestClusterFactory serverutils.TestClusterFactory = testClusterFactoryImpl{}

// NewTestCluster is part of the TestClusterFactory interface.
func (testClusterFactoryImpl) NewTestCluster(
	t serverutils.TestFataler, numNodes int, args base.TestClusterArgs,
) serverutils.TestClusterInterface {
	return NewTestCluster(t, numNodes, args)
}
