// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenant_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenantdirsvr"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// To ensure tenant startup code is included.
var _ = kvtenantccl.Connector{}

func TestDirectoryErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	const tenantID = 10

	ctx := context.Background()

	tc, dir, _ := newTestDirectory(t)
	defer tc.Stopper().Stop(ctx)

	_, err := dir.LookupTenantAddrs(ctx, roachpb.MakeTenantID(1000))
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant 1000 not in directory cache")
	_, err = dir.LookupTenantAddrs(ctx, roachpb.MakeTenantID(1001))
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant 1001 not in directory cache")
	_, err = dir.LookupTenantAddrs(ctx, roachpb.MakeTenantID(1002))
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant 1002 not in directory cache")

	// Fail to find tenant that does not exist.
	_, err = dir.EnsureTenantAddr(ctx, roachpb.MakeTenantID(1000), "")
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant 1000 not found")

	// Fail to find tenant when cluster name doesn't match.
	_, err = dir.EnsureTenantAddr(ctx, roachpb.MakeTenantID(tenantID), "unknown")
	require.EqualError(t, err, "rpc error: code = NotFound desc = cluster name unknown doesn't match expected tenant-cluster")

	// No-op when reporting failure for tenant that doesn't exit.
	require.NoError(t, dir.ReportFailure(ctx, roachpb.MakeTenantID(1000), ""))
}

func TestWatchPods(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	// Make pod watcher channel.
	podWatcher := make(chan *tenant.Pod, 1)

	// Create the directory.
	ctx := context.Background()
	tc, dir, tds := newTestDirectory(t, tenant.PodWatcher(podWatcher))
	defer tc.Stopper().Stop(ctx)

	tenantID := roachpb.MakeTenantID(20)
	require.NoError(t, createTenant(tc, tenantID))

	// Call EnsureTenantAddr to start a new tenant and create an entry.
	addr, err := dir.EnsureTenantAddr(ctx, tenantID, "")
	require.NoError(t, err)
	require.NotEmpty(t, addr)

	// Ensure that correct event was sent to watcher channel.
	pod := <-podWatcher
	require.Equal(t, tenantID.ToUint64(), pod.TenantID)
	require.Equal(t, addr, pod.Addr)
	require.Equal(t, tenant.RUNNING, pod.State)

	// Trigger drain of pod.
	tds.Drain()
	pod = <-podWatcher
	require.Equal(t, tenantID.ToUint64(), pod.TenantID)
	require.Equal(t, addr, pod.Addr)
	require.Equal(t, tenant.DRAINING, pod.State)

	// Ensure that all addresses have been cleared from the directory, since
	// it should only return RUNNING addresses.
	addrs, err := dir.LookupTenantAddrs(ctx, tenantID)
	require.NoError(t, err)
	require.Empty(t, addrs)

	// Now shut the tenant directory down.
	processes := tds.Get(tenantID)
	require.NotNil(t, processes)
	require.Len(t, processes, 1)
	// Stop the tenant and ensure its IP address is removed from the directory.
	for _, process := range processes {
		process.Stopper.Stop(ctx)
	}

	// Ensure that correct event was sent to watcher channel.
	pod = <-podWatcher
	require.Equal(t, tenantID.ToUint64(), pod.TenantID)
	require.Equal(t, addr, pod.Addr)
	require.Equal(t, tenant.DELETING, pod.State)

	require.Eventually(t, func() bool {
		addrs, _ := dir.LookupTenantAddrs(ctx, tenantID)
		return len(addrs) == 0
	}, 10*time.Second, 100*time.Millisecond)

	// Resume tenant again by a direct call to the directory server
	_, err = tds.EnsurePod(ctx, &tenant.EnsurePodRequest{tenantID.ToUint64()})
	require.NoError(t, err)

	// Wait for background watcher to populate the initial pod.
	require.Eventually(t, func() bool {
		addrs, _ := dir.LookupTenantAddrs(ctx, tenantID)
		if len(addrs) != 0 {
			addr = addrs[0]
			return true
		}
		return false
	}, 10*time.Second, 100*time.Millisecond)

	// Ensure that correct event was sent to watcher channel.
	pod = <-podWatcher
	require.Equal(t, tenantID.ToUint64(), pod.TenantID)
	require.Equal(t, addr, pod.Addr)
	require.Equal(t, tenant.RUNNING, pod.State)

	// Verify that EnsureTenantAddr returns the pod's IP address.
	addr, err = dir.EnsureTenantAddr(ctx, tenantID, "")
	require.NoError(t, err)
	require.NotEmpty(t, addr)

	processes = tds.Get(tenantID)
	require.NotNil(t, processes)
	require.Len(t, processes, 1)
	for dirAddr := range processes {
		require.Equal(t, addr, dirAddr.String())
	}

	// Stop the tenant and ensure its IP address is removed from the directory.
	for _, process := range processes {
		process.Stopper.Stop(ctx)
	}

	require.Eventually(t, func() bool {
		addrs, _ := dir.LookupTenantAddrs(ctx, tenantID)
		return len(addrs) == 0
	}, 10*time.Second, 100*time.Millisecond)

	// Ensure that correct event was sent to watcher channel.
	pod = <-podWatcher
	require.Equal(t, tenantID.ToUint64(), pod.TenantID)
	require.Equal(t, addr, pod.Addr)
	require.Equal(t, tenant.DELETING, pod.State)

	// Verify that a new call to EnsureTenantAddr will resume again the tenant.
	addr, err = dir.EnsureTenantAddr(ctx, tenantID, "")
	require.NoError(t, err)
	require.NotEmpty(t, addr)

	// Ensure that correct event was sent to watcher channel.
	pod = <-podWatcher
	require.Equal(t, tenantID.ToUint64(), pod.TenantID)
	require.Equal(t, addr, pod.Addr)
	require.Equal(t, tenant.RUNNING, pod.State)
}

func TestCancelLookups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	tenantID := roachpb.MakeTenantID(20)
	const lookupCount = 1

	// Create the directory.
	ctx, cancel := context.WithCancel(context.Background())
	tc, dir, _ := newTestDirectory(t)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, createTenant(tc, tenantID))

	backgroundErrors := make([]error, lookupCount)
	var wait sync.WaitGroup
	for i := 0; i < lookupCount; i++ {
		wait.Add(1)
		go func(i int) {
			_, backgroundErrors[i] = dir.EnsureTenantAddr(ctx, tenantID, "")
			wait.Done()
		}(i)
	}

	// Cancel the lookup and verify errors.
	time.Sleep(10 * time.Millisecond)
	cancel()
	wait.Wait()

	for i := 0; i < lookupCount; i++ {
		require.Error(t, backgroundErrors[i])
		require.Regexp(t, "context canceled", backgroundErrors[i].Error())
	}
}

func TestResume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	tenantID := roachpb.MakeTenantID(40)
	const lookupCount = 5

	// Create the directory.
	ctx := context.Background()
	tc, dir, tds := newTestDirectory(t)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, createTenant(tc, tenantID))

	// No tenant processes running.
	require.Equal(t, 0, len(tds.Get(tenantID)))

	var addrs [lookupCount]string
	var wait sync.WaitGroup
	for i := 0; i < lookupCount; i++ {
		wait.Add(1)
		go func(i int) {
			var err error
			addrs[i], err = dir.EnsureTenantAddr(ctx, tenantID, "")
			require.NoError(t, err)
			wait.Done()
		}(i)
	}

	var processes map[net.Addr]*tenantdirsvr.Process
	// Eventually the tenant process will be resumed.
	require.Eventually(t, func() bool {
		processes = tds.Get(tenantID)
		return len(processes) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Wait until background goroutines complete.
	wait.Wait()

	for addr := range processes {
		for i := 0; i < lookupCount; i++ {
			require.Equal(t, addr.String(), addrs[i])
		}
	}
}

func TestDeleteTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	// Create the directory.
	ctx := context.Background()
	// Disable throttling for this test
	tc, dir, tds := newTestDirectory(t, tenant.RefreshDelay(-1))
	defer tc.Stopper().Stop(ctx)

	tenantID := roachpb.MakeTenantID(50)
	// Create test tenant.
	require.NoError(t, createTenant(tc, tenantID))

	// Perform lookup to create entry in cache.
	addr, err := dir.EnsureTenantAddr(ctx, tenantID, "")
	require.NoError(t, err)
	require.NotEmpty(t, addr)

	// Report failure even though tenant is healthy - refresh should do nothing.
	require.NoError(t, dir.ReportFailure(ctx, tenantID, addr))
	addr, err = dir.EnsureTenantAddr(ctx, tenantID, "")
	require.NoError(t, err)
	require.NotEmpty(t, addr)

	// Stop the tenant
	for _, process := range tds.Get(tenantID) {
		process.Stopper.Stop(ctx)
	}

	// Report failure connecting to the pod to force refresh of addrs.
	require.NoError(t, dir.ReportFailure(ctx, tenantID, addr))

	// Ensure that tenant has no valid IP addresses.
	addrs, err := dir.LookupTenantAddrs(ctx, tenantID)
	require.NoError(t, err)
	require.Empty(t, addrs)

	// Report failure again to ensure that works when there is no ip address.
	require.NoError(t, dir.ReportFailure(ctx, tenantID, addr))

	// Now delete the tenant.
	require.NoError(t, destroyTenant(tc, tenantID))

	// Now EnsureTenantAddr should return an error and the directory should no
	// longer cache the tenant.
	_, err = dir.EnsureTenantAddr(ctx, tenantID, "")
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant 50 not found")
	addrs, err = dir.LookupTenantAddrs(ctx, tenantID)
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant 50 not in directory cache")
	require.Nil(t, addrs)
}

// TestRefreshThrottling checks that throttling works.
func TestRefreshThrottling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	// Create the directory, but with extreme rate limiting so that directory
	// will never refresh.
	ctx := context.Background()
	tc, dir, _ := newTestDirectory(t, tenant.RefreshDelay(60*time.Minute))
	defer tc.Stopper().Stop(ctx)

	// Create test tenant.
	tenantID := roachpb.MakeTenantID(60)
	require.NoError(t, createTenant(tc, tenantID))

	// Perform lookup to create entry in cache.
	addr, err := dir.EnsureTenantAddr(ctx, tenantID, "")
	require.NoError(t, err)
	require.NotEmpty(t, addr)

	// Report a false failure and verify that IP is still present in the cache.
	require.NoError(t, dir.ReportFailure(ctx, tenantID, addr))
	addrs, err := dir.LookupTenantAddrs(ctx, tenantID)
	require.NoError(t, err)
	require.Equal(t, []string{addr}, addrs)

	// Now destroy the tenant and call ReportFailure again. This should be a no-op
	// due to refresh throttling.
	require.NoError(t, destroyTenant(tc, tenantID))
	require.NoError(t, dir.ReportFailure(ctx, tenantID, addr))
	addrs, err = dir.LookupTenantAddrs(ctx, tenantID)
	require.NoError(t, err)
	require.Equal(t, []string{addr}, addrs)
}

func createTenant(tc serverutils.TestClusterInterface, id roachpb.TenantID) error {
	srv := tc.Server(0)
	conn := srv.InternalExecutor().(*sql.InternalExecutor)
	if _, err := conn.Exec(
		context.Background(),
		"testserver-create-tenant",
		nil, /* txn */
		"SELECT crdb_internal.create_tenant($1)",
		id.ToUint64(),
	); err != nil {
		return err
	}
	return nil
}

func destroyTenant(tc serverutils.TestClusterInterface, id roachpb.TenantID) error {
	srv := tc.Server(0)
	conn := srv.InternalExecutor().(*sql.InternalExecutor)
	if _, err := conn.Exec(
		context.Background(),
		"testserver-destroy-tenant",
		nil, /* txn */
		"SELECT crdb_internal.destroy_tenant($1)",
		id.ToUint64(),
	); err != nil {
		return err
	}
	return nil
}

func startTenant(
	ctx context.Context, srv serverutils.TestServerInterface, id uint64,
) (*tenantdirsvr.Process, error) {
	log.TestingClearServerIdentifiers()
	tenantStopper := tenantdirsvr.NewSubStopper(srv.Stopper())
	t, err := srv.StartTenant(
		ctx,
		base.TestTenantArgs{
			Existing:      true,
			TenantID:      roachpb.MakeTenantID(id),
			ForceInsecure: true,
			Stopper:       tenantStopper,
		})
	if err != nil {
		// Remap tenant "not found" error to GRPC NotFound error.
		if err.Error() == "not found" {
			return nil, status.Errorf(codes.NotFound, "tenant %d not found", id)
		}
		return nil, err
	}
	sqlAddr, err := net.ResolveTCPAddr("tcp", t.SQLAddr())
	if err != nil {
		return nil, err
	}
	return &tenantdirsvr.Process{SQL: sqlAddr, Stopper: tenantStopper}, nil
}

// Setup directory that uses a client connected to a test directory server
// that manages tenants connected to a backing KV server.
func newTestDirectory(
	t *testing.T, opts ...tenant.DirOption,
) (
	tc serverutils.TestClusterInterface,
	directory *tenant.Directory,
	tds *tenantdirsvr.TestDirectoryServer,
) {
	tc = serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		// We need to start the cluster insecure in order to not
		// care about TLS settings for the RPC client connection.
		ServerArgs: base.TestServerArgs{
			Insecure: true,
		},
	})
	clusterStopper := tc.Stopper()
	var err error
	tds, err = tenantdirsvr.New(clusterStopper)
	require.NoError(t, err)
	tds.TenantStarterFunc = func(ctx context.Context, tenantID uint64) (*tenantdirsvr.Process, error) {
		t.Logf("starting tenant %d", tenantID)
		process, err := startTenant(ctx, tc.Server(0), tenantID)
		if err != nil {
			return nil, err
		}
		t.Logf("tenant %d started", tenantID)
		return process, nil
	}

	listenPort, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	go func() { _ = tds.Serve(listenPort) }()

	// Setup directory
	directorySrvAddr := listenPort.Addr()
	conn, err := grpc.Dial(directorySrvAddr.String(), grpc.WithInsecure())
	require.NoError(t, err)
	// nolint:grpcconnclose
	clusterStopper.AddCloser(stop.CloserFn(func() { require.NoError(t, conn.Close() /* nolint:grpcconnclose */) }))
	client := tenant.NewDirectoryClient(conn)
	directory, err = tenant.NewDirectory(context.Background(), clusterStopper, client, opts...)
	require.NoError(t, err)

	return
}
