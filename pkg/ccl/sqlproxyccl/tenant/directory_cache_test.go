// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenant_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenantdirsvr"
	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDirectoryErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.ScopeWithoutShowLogs(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	dir, _ := tenantdirsvr.SetupTestDirectory(t, ctx, stopper, nil /* timeSource */)

	// Fail to find a tenant that does not exist.
	_, err := dir.LookupTenant(ctx, roachpb.MustMakeTenantID(1000))
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant does not exist")

	// Fail to find a tenant that does not exist.
	_, err = dir.TryLookupTenantPods(ctx, roachpb.MustMakeTenantID(1000))
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant 1000 not in directory cache")
	_, err = dir.TryLookupTenantPods(ctx, roachpb.MustMakeTenantID(1001))
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant 1001 not in directory cache")
	_, err = dir.TryLookupTenantPods(ctx, roachpb.MustMakeTenantID(1002))
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant 1002 not in directory cache")

	// Fail to find tenant that does not exist.
	_, err = dir.LookupTenantPods(ctx, roachpb.MustMakeTenantID(1000))
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant does not exist")

	// No-op when reporting failure for tenant that doesn't exit.
	require.NoError(t, dir.ReportFailure(ctx, roachpb.MustMakeTenantID(1000), ""))
}

func TestWatchTenants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Make tenant watcher channel.
	tenantWatcher := make(chan *tenant.WatchTenantsResponse, 1)

	// Setup test directory cache and server.
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	dir, tds := tenantdirsvr.SetupTestDirectory(t, ctx, stopper, nil, /* timeSource */
		tenant.TenantWatcher(tenantWatcher))

	// Wait until the tenant watcher has been established.
	testutils.SucceedsSoon(t, func() error {
		if tds.WatchTenantsListenersCount() == 0 {
			return errors.New("watchers have not been established yet")
		}
		return nil
	})

	// Creating a tenant should have no effect if the proxy hasn't initialized
	// it before.
	tenantID := roachpb.MustMakeTenantID(20)
	baseTenant := &tenant.Tenant{
		Version:                 "010",
		TenantID:                tenantID.ToUint64(),
		ClusterName:             "my-tenant",
		AllowedCIDRRanges:       []string{"127.0.0.1/16"},
		AllowedPrivateEndpoints: []string{"a"},
	}
	tds.CreateTenant(tenantID, baseTenant)
	resp := <-tenantWatcher
	require.Equal(t, tenant.EVENT_ADDED, resp.Type)
	require.Equal(t, &tenant.Tenant{
		Version:                 "010",
		TenantID:                20,
		ClusterName:             "my-tenant",
		AllowedCIDRRanges:       []string{"127.0.0.1/16"},
		AllowedPrivateEndpoints: []string{"a"},
	}, resp.Tenant)
	_, err := dir.TryLookupTenantPods(ctx, tenantID)
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant 20 not in directory cache")

	// Now perform the lookup, which will call Initialize.
	tenantObj, err := dir.LookupTenant(ctx, tenantID)
	require.NoError(t, err)
	require.Equal(t, &tenant.Tenant{
		Version:                 "010",
		TenantID:                20,
		ClusterName:             "my-tenant",
		AllowedCIDRRanges:       []string{"127.0.0.1/16"},
		AllowedPrivateEndpoints: []string{"a"},
	}, tenantObj)

	// Update the tenant object.
	updatedTenant := &tenant.Tenant{
		Version:                 "011",
		TenantID:                20,
		ClusterName:             "foo-bar",
		AllowedCIDRRanges:       []string{"127.0.0.1/16", "0.0.0.0/0"},
		AllowedPrivateEndpoints: []string{"a", "b"},
	}
	tds.UpdateTenant(tenantID, updatedTenant)
	resp = <-tenantWatcher
	require.Equal(t, tenant.EVENT_MODIFIED, resp.Type)
	require.Equal(t, updatedTenant, resp.Tenant)

	// The tenant should be updated.
	tenantObj, err = dir.LookupTenant(ctx, tenantID)
	require.NoError(t, err)
	require.Equal(t, &tenant.Tenant{
		Version:                 "011",
		TenantID:                20,
		ClusterName:             "foo-bar",
		AllowedCIDRRanges:       []string{"127.0.0.1/16", "0.0.0.0/0"},
		AllowedPrivateEndpoints: []string{"a", "b"},
	}, tenantObj)

	// Update the tenant object with an old version.
	updatedTenant = &tenant.Tenant{
		Version:     "008",
		TenantID:    tenantID.ToUint64(),
		ClusterName: "foo-bar-baz",
	}
	tds.UpdateTenant(tenantID, updatedTenant)
	resp = <-tenantWatcher
	require.Equal(t, tenant.EVENT_MODIFIED, resp.Type)
	require.Equal(t, updatedTenant, resp.Tenant)

	// Tenant should still stay as v=011.
	tenantObj, err = dir.LookupTenant(ctx, tenantID)
	require.NoError(t, err)
	require.Equal(t, &tenant.Tenant{
		Version:                 "011",
		TenantID:                20,
		ClusterName:             "foo-bar",
		AllowedCIDRRanges:       []string{"127.0.0.1/16", "0.0.0.0/0"},
		AllowedPrivateEndpoints: []string{"a", "b"},
	}, tenantObj)

	// Finally, delete the tenant.
	tds.DeleteTenant(tenantID)
	resp = <-tenantWatcher
	require.Equal(t, tenant.EVENT_DELETED, resp.Type)
	require.Equal(t, &tenant.Tenant{TenantID: 20}, resp.Tenant)
	_, err = dir.LookupTenant(ctx, tenantID)
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant does not exist")

	// Create two tenants: one to delete, and the other to modify.
	tenant10 := roachpb.MustMakeTenantID(10)
	tenant20 := roachpb.MustMakeTenantID(20)
	tenant10Data := &tenant.Tenant{
		Version:     "001",
		TenantID:    tenant10.ToUint64(),
		ClusterName: "tenant-to-delete",
	}
	tenant20Data := &tenant.Tenant{
		Version:     "001",
		TenantID:    tenant20.ToUint64(),
		ClusterName: "tenant-to-modify",
	}
	tds.CreateTenant(tenant10, tenant10Data)
	tds.CreateTenant(tenant20, tenant20Data)

	tenantObj, err = dir.LookupTenant(ctx, tenant10)
	require.NoError(t, err)
	require.Equal(t, tenant10Data, tenantObj)
	resp = <-tenantWatcher
	require.Equal(t, tenant.EVENT_ADDED, resp.Type)
	require.Equal(t, tenant10Data, resp.Tenant)
	tenantObj, err = dir.LookupTenant(ctx, tenant20)
	require.NoError(t, err)
	require.Equal(t, tenant20Data, tenantObj)
	resp = <-tenantWatcher
	require.Equal(t, tenant.EVENT_ADDED, resp.Type)
	require.Equal(t, tenant20Data, resp.Tenant)

	// Trigger the directory server to restart. WatchTenants should handle
	// reconnection properly.
	tds.Stop(ctx)
	testutils.SucceedsSoon(t, func() error {
		if tds.WatchTenantsListenersCount() != 0 {
			return errors.New("watchers have not been removed yet")
		}
		return nil
	})

	// Trigger events, which will be missed by the tenant watcher.
	tds.DeleteTenant(tenant10)
	tenant20Data.Version = "002"
	tenant20Data.ClusterName = "dim-dog"
	tds.UpdateTenant(tenant20, tenant20Data)

	// Make sure that entries are still valid.
	tenantObj, err = dir.LookupTenant(ctx, tenant10)
	require.NoError(t, err)
	require.Equal(t, tenant10Data, tenantObj)

	// Start the directory server again.
	require.NoError(t, tds.Start(ctx))
	testutils.SucceedsSoon(t, func() error {
		if tds.WatchTenantsListenersCount() == 0 {
			return errors.New("watchers have not been established yet")
		}
		return nil
	})

	// Trigger a tenant update.
	tds.UpdateTenant(tenant20, tenant20Data)

	// Eventually, cache should be updated.
	testutils.SucceedsSoon(t, func() error {
		obj, err := dir.LookupTenant(ctx, tenant20)
		if err != nil {
			return err
		}
		if obj.Version != "002" {
			return errors.New("tenant isn't updated yet")
		}
		return nil
	})

	// Tenant 10 should still be valid since deleted tenants are not removed.
	tenantObj, err = dir.LookupTenant(ctx, tenant10)
	require.NoError(t, err)
	require.Equal(t, tenant10Data, tenantObj)

	// Check that tenant is deleted.
	_, err = dir.LookupTenantPods(ctx, tenant10)
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant does not exist")
}

func TestWatchPods(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Make pod watcher channel.
	podWatcher := make(chan *tenant.Pod, 1)

	// Setup test directory cache and server.
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	dir, tds := tenantdirsvr.SetupTestDirectory(t, ctx, stopper, nil /* timeSource */, tenant.PodWatcher(podWatcher))

	// Wait until the watcher has been established.
	testutils.SucceedsSoon(t, func() error {
		if tds.WatchPodsListenersCount() == 0 {
			return errors.New("watchers have not been established yet")
		}
		return nil
	})

	tenantID := roachpb.MustMakeTenantID(20)
	tds.CreateTenant(tenantID, &tenant.Tenant{
		TenantID:    tenantID.ToUint64(),
		ClusterName: "my-tenant",
	})

	// Add a new pod to the tenant.
	runningPod := &tenant.Pod{
		TenantID:       tenantID.ToUint64(),
		Addr:           "127.0.0.10:10",
		State:          tenant.RUNNING,
		StateTimestamp: timeutil.Now(),
	}
	require.True(t, tds.AddPod(tenantID, runningPod))
	pod := <-podWatcher
	requirePodsEqual := func(t *testing.T, pod1, pod2 *tenant.Pod) {
		p1, p2 := *pod1, *pod2
		p1.StateTimestamp = timeutil.StripMono(p1.StateTimestamp)
		p2.StateTimestamp = timeutil.StripMono(p2.StateTimestamp)
		require.Equal(t, p1, p2)
	}
	requirePodsEqual(t, runningPod, pod)

	// Directory cache should have already been updated.
	pods, err := dir.TryLookupTenantPods(ctx, tenantID)
	require.NoError(t, err)
	require.Len(t, pods, 1)
	requirePodsEqual(t, runningPod, pods[0])

	// Drain the pod.
	require.True(t, tds.DrainPod(tenantID, runningPod.Addr))
	pod = <-podWatcher
	require.Equal(t, tenantID.ToUint64(), pod.TenantID)
	require.Equal(t, runningPod.Addr, pod.Addr)
	require.Equal(t, tenant.DRAINING, pod.State)
	require.False(t, pod.StateTimestamp.IsZero())

	// Directory cache should be updated with the DRAINING pod.
	pods, err = dir.TryLookupTenantPods(ctx, tenantID)
	require.NoError(t, err)
	require.Len(t, pods, 1)
	require.Equal(t, pod, pods[0])

	// Trigger the directory server to restart. WatchPods should handle
	// reconnection properly.
	//
	// NOTE: We check for the number of listeners before proceeding with the
	// pod update (e.g. AddPod) because if we don't do that, there could be a
	// situation where AddPod gets called before the watcher gets established,
	// which means that the pod update event will never get emitted. This is
	// only a test directory server issue due to its simple implementation. One
	// way to solve this nicely is to implement checkpointing based on the sent
	// updates (just like how Kubernetes bookmarks work).
	tds.Stop(ctx)
	testutils.SucceedsSoon(t, func() error {
		if tds.WatchPodsListenersCount() != 0 {
			return errors.New("watchers have not been removed yet")
		}
		return nil
	})

	// Trigger a deletion event, which will be missed by the pod watcher.
	require.True(t, tds.RemovePod(tenantID, runningPod.Addr))

	// Start the directory server again.
	require.NoError(t, tds.Start(ctx))
	testutils.SucceedsSoon(t, func() error {
		if tds.WatchPodsListenersCount() == 0 {
			return errors.New("watchers have not been established yet")
		}
		return nil
	})

	// Directory cache should still have the DRAINING pod.
	pods, err = dir.TryLookupTenantPods(ctx, tenantID)
	require.NoError(t, err)
	require.Len(t, pods, 1)
	require.Equal(t, pod, pods[0])

	// Now attempt to perform a resumption. We get an error here, which shows
	// that we attempted to call EnsurePod in the test directory server because
	// the cache has no running pods. In the actual directory server, this
	// should put the draining pod back to running.
	pods, err = dir.LookupTenantPods(ctx, tenantID)
	require.Regexp(t, "tenant has no pods", err)
	require.Empty(t, pods)

	// Put the same pod back to running.
	require.True(t, tds.AddPod(tenantID, runningPod))
	pod = <-podWatcher
	requirePodsEqual(t, runningPod, pod)

	// Directory cache should be updated with the RUNNING pod.
	pods, err = dir.TryLookupTenantPods(ctx, tenantID)
	require.NoError(t, err)
	require.Len(t, pods, 1)
	require.Equal(t, pod, pods[0])

	// Delete the pod.
	require.True(t, tds.RemovePod(tenantID, runningPod.Addr))
	pod = <-podWatcher
	require.Equal(t, tenantID.ToUint64(), pod.TenantID)
	require.Equal(t, runningPod.Addr, pod.Addr)
	require.Equal(t, tenant.DELETING, pod.State)
	require.False(t, pod.StateTimestamp.IsZero())

	// Directory cache should have no pods.
	pods, err = dir.TryLookupTenantPods(ctx, tenantID)
	require.NoError(t, err)
	require.Empty(t, pods)
}

func TestCancelLookups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.ScopeWithoutShowLogs(t).Close(t)

	tenantID := roachpb.MustMakeTenantID(20)
	const lookupCount = 1

	// Create the directory.
	ctx, cancel := context.WithCancel(context.Background())
	tc, dir, _ := newTestDirectoryCache(t)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, createTenant(tc, tenantID))

	backgroundErrors := make([]error, lookupCount)
	var wait sync.WaitGroup
	for i := 0; i < lookupCount; i++ {
		wait.Add(1)
		go func(i int) {
			_, backgroundErrors[i] = dir.LookupTenantPods(ctx, tenantID)
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
	testutilsccl.ServerlessOnly(t)
	defer log.ScopeWithoutShowLogs(t).Close(t)

	tenantID := roachpb.MustMakeTenantID(40)
	const lookupCount = 5

	// Create the directory.
	ctx := context.Background()
	tc, dir, tds := newTestDirectoryCache(t)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, createTenant(tc, tenantID))

	// No tenant processes running.
	require.Equal(t, 0, len(tds.Get(tenantID)))

	var addrs [lookupCount]string
	var wait sync.WaitGroup
	for i := 0; i < lookupCount; i++ {
		wait.Add(1)
		go func(i int) {
			pods, err := dir.LookupTenantPods(ctx, tenantID)
			require.NoError(t, err)
			addrs[i] = pods[0].Addr
			wait.Done()
		}(i)
	}

	// Eventually the tenant process will be resumed.
	var sqlAddr string
	testutils.SucceedsSoon(t, func() error {
		processes := tds.Get(tenantID)
		if len(processes) != 1 {
			return errors.Newf("expected 1 processes found %d", len(processes))
		}
		sqlAddr = processes[0].SQLAddr
		return nil
	})

	// Wait until background goroutines complete.
	wait.Wait()

	for i := 0; i < lookupCount; i++ {
		require.Equal(t, sqlAddr, addrs[i])
	}
}

func TestDeleteTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.ScopeWithoutShowLogs(t).Close(t)

	// Create the directory.
	ctx := context.Background()
	// Disable throttling for this test
	tc, dir, tds := newTestDirectoryCache(t, tenant.RefreshDelay(-1))
	defer tc.Stopper().Stop(ctx)

	tenantID := roachpb.MustMakeTenantID(50)
	// Create test tenant.
	require.NoError(t, createTenant(tc, tenantID))

	// Perform lookup to create entry in cache.
	pods, err := dir.LookupTenantPods(ctx, tenantID)
	require.NoError(t, err)
	require.NotEmpty(t, pods)
	addr := pods[0].Addr

	// LookupTenant should work.
	ten, err := dir.LookupTenant(ctx, tenantID)
	require.NoError(t, err)
	require.Equal(t, &tenant.Tenant{
		TenantID:          50,
		ClusterName:       "tenant-cluster",
		AllowedCIDRRanges: []string{"0.0.0.0/0"},
	}, ten)

	// Report failure even though tenant is healthy - refresh should do nothing.
	require.NoError(t, dir.ReportFailure(ctx, tenantID, addr))
	pods, err = dir.LookupTenantPods(ctx, tenantID)
	require.NoError(t, err)
	require.NotEmpty(t, pods)
	addr = pods[0].Addr

	// Stop the tenant
	for _, process := range tds.Get(tenantID) {
		process.Stopper.Stop(ctx)
	}

	// There is a rare race condition where the watch event can overwrite
	// ListPods inside ReportFailure. If that happens, retrying the ReportFailure
	// should work as expected.
	// See https://github.com/cockroachdb/cockroach/issues/86077
	testutils.SucceedsSoon(t, func() error {
		// Report failure connecting to the pod to force refresh of addrs.
		if err := dir.ReportFailure(ctx, tenantID, addr); err != nil {
			return err
		}

		// Ensure that tenant has no valid IP addresses.
		pods, err = dir.TryLookupTenantPods(ctx, tenantID)
		if err != nil {
			return err
		}

		if len(pods) != 0 {
			return errors.Newf("expected 0 pods found %v", pods)
		}

		return nil
	})

	// Report failure again to ensure that works when there is no ip address.
	require.NoError(t, dir.ReportFailure(ctx, tenantID, addr))

	// Now delete the tenant.
	require.NoError(t, destroyTenant(tc, tenantID))

	// Now LookupTenantPods should return an error and the directory should no
	// longer cache the tenant.
	_, err = dir.LookupTenantPods(ctx, tenantID)
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant 50 not found")
	pods, err = dir.TryLookupTenantPods(ctx, tenantID)
	require.EqualError(t, err, "rpc error: code = NotFound desc = tenant 50 not in directory cache")
	require.Nil(t, pods)
}

// TestRefreshThrottling checks that throttling works.
func TestRefreshThrottling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.ScopeWithoutShowLogs(t).Close(t)
	skip.UnderDeadlockWithIssue(t, 71365)

	// Create the directory, but with extreme rate limiting so that directory
	// will never refresh.
	ctx := context.Background()
	tc, dir, _ := newTestDirectoryCache(t, tenant.RefreshDelay(60*time.Minute))
	defer tc.Stopper().Stop(ctx)

	// Create test tenant.
	tenantID := roachpb.MustMakeTenantID(60)
	require.NoError(t, createTenant(tc, tenantID))

	// Perform lookup to create entry in cache.
	pods, err := dir.LookupTenantPods(ctx, tenantID)
	require.NoError(t, err)
	require.NotEmpty(t, pods)
	addr := pods[0].Addr

	// Report a false failure and verify that IP is still present in the cache.
	require.NoError(t, dir.ReportFailure(ctx, tenantID, addr))
	pods, err = dir.TryLookupTenantPods(ctx, tenantID)
	require.NoError(t, err)
	require.NotEmpty(t, pods)

	// Reset StateTimestamp for deterministic comparison.
	pods[0].StateTimestamp = time.Time{}
	require.Equal(t, []*tenant.Pod{{
		TenantID: tenantID.ToUint64(),
		Addr:     addr,
		State:    tenant.RUNNING,
	}}, pods)
}

func createTenant(tc serverutils.TestClusterInterface, id roachpb.TenantID) error {
	srv := tc.Server(0)
	conn := srv.InternalExecutor().(*sql.InternalExecutor)
	for _, stmt := range []string{
		`CREATE VIRTUAL CLUSTER [$1]`,
		`ALTER VIRTUAL CLUSTER [$1] START SERVICE EXTERNAL`,
	} {
		if _, err := conn.Exec(
			context.Background(),
			"testserver-create-tenant",
			nil, /* txn */
			stmt,
			id.ToUint64(),
		); err != nil {
			return err
		}
	}
	return nil
}

func destroyTenant(tc serverutils.TestClusterInterface, id roachpb.TenantID) error {
	srv := tc.Server(0)
	conn := srv.InternalExecutor().(*sql.InternalExecutor)
	for _, stmt := range []string{
		`ALTER TENANT [$1] STOP SERVICE`,
		`DROP TENANT [$1] IMMEDIATE`,
	} {
		if _, err := conn.Exec(
			context.Background(),
			"testserver-destroy-tenant",
			nil, /* txn */
			stmt,
			id.ToUint64(),
		); err != nil {
			return err
		}
	}
	return nil
}

func startTenant(
	ctx context.Context, tenantStopper *stop.Stopper, srv serverutils.TestServerInterface, id uint64,
) (string, error) {
	t, err := srv.TenantController().StartTenant(
		ctx,
		base.TestTenantArgs{
			TenantID: roachpb.MustMakeTenantID(id),
			// Disable tenant creation, since this function assumes a tenant
			// already exists.
			DisableCreateTenant: true,
			ForceInsecure:       true,
			Stopper:             tenantStopper,
		})
	if err != nil {
		// Remap tenant "not found" error to GRPC NotFound error.
		if testutils.IsError(err, "not found|no tenant found") {
			return "", status.Errorf(codes.NotFound, "tenant %d not found", id)
		}
		return "", err
	}
	return t.SQLAddr(), nil
}

// Setup directory cache that uses a client connected to a test directory server
// that manages tenants connected to a backing KV server.
func newTestDirectoryCache(
	t *testing.T, opts ...tenant.DirOption,
) (
	tc serverutils.TestClusterInterface,
	directoryCache tenant.DirectoryCache,
	tds *tenantdirsvr.TestDirectoryServer,
) {
	tc = serverutils.StartCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// We need to start the cluster insecure in order to not
			// care about TLS settings for the RPC client connection.
			Insecure:          true,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})
	clusterStopper := tc.Stopper()
	var err error
	tds, err = tenantdirsvr.New(clusterStopper, func(ctx context.Context, stopper *stop.Stopper, tenantID uint64) (string, error) {
		t.Logf("starting tenant %d", tenantID)
		sqlAddr, err := startTenant(ctx, stopper, tc.Server(0), tenantID)
		if err != nil {
			return "", err
		}
		t.Logf("tenant %d started", tenantID)
		return sqlAddr, nil
	})
	require.NoError(t, err)

	listenPort, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = tds.Serve(listenPort) }()

	// Setup directory
	directorySrvAddr := listenPort.Addr()
	//lint:ignore SA1019 grpc.WithInsecure is deprecated
	conn, err := grpc.Dial(directorySrvAddr.String(), grpc.WithInsecure())
	require.NoError(t, err)
	// nolint:grpcconnclose
	clusterStopper.AddCloser(stop.CloserFn(func() { require.NoError(t, conn.Close() /* nolint:grpcconnclose */) }))
	client := tenant.NewDirectoryClient(conn)
	directoryCache, err = tenant.NewDirectoryCache(context.Background(), clusterStopper, client, opts...)
	require.NoError(t, err)
	return
}
