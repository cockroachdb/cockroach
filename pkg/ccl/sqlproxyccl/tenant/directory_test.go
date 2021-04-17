// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenant

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestDirectoryErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	const tenantID = 10

	ctx := context.Background()

	tc, dir, _ := newTestDirectory(t)
	defer tc.Stopper().Stop(ctx)

	_, err := dir.LookupTenantIPs(ctx, roachpb.MakeTenantID(1000))
	assert.Regexp(t, "not found", err)
	_, err = dir.LookupTenantIPs(ctx, roachpb.MakeTenantID(1001))
	assert.Regexp(t, "not found", err)
	_, err = dir.LookupTenantIPs(ctx, roachpb.MakeTenantID(1002))
	assert.Regexp(t, "not found", err)

	// Fail to find tenant that does not exist.
	_, err = dir.EnsureTenantIP(ctx, roachpb.MakeTenantID(1000), "")
	assert.Regexp(t, "not found", err)

	// Fail to find tenant when cluster name doesn't match.
	_, err = dir.EnsureTenantIP(ctx, roachpb.MakeTenantID(tenantID), "unknown")
	assert.Regexp(t, "not found", err)

	// No-op when reporting failure for tenant that doesn't exit.
	require.NoError(t, dir.ReportFailure(ctx, roachpb.MakeTenantID(1000), ""))
}

func TestEndpointWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	// Create the directory.
	ctx := context.Background()
	tc, dir, tds := newTestDirectory(t)
	defer tc.Stopper().Stop(ctx)

	tenantID := roachpb.MakeTenantID(20)
	require.NoError(t, createTenant(tc, tenantID))

	// Call EnsureTenantIP to start a new tenant and create an entry
	ip, err := dir.EnsureTenantIP(ctx, tenantID, "")
	require.NoError(t, err)
	require.NotEmpty(t, ip)

	// Now shut it down
	processes := tds.Get(tenantID)
	require.NotNil(t, processes)
	require.Len(t, processes, 1)
	// Stop the tenant and ensure its IP address is removed from the directory.
	for _, process := range processes {
		process.Stopper.Stop(ctx)
	}

	require.Eventually(t, func() bool {
		ips, _ := dir.LookupTenantIPs(ctx, tenantID)
		return len(ips) == 0
	}, 10*time.Second, 100*time.Millisecond)

	// Resume tenant again by a direct call to the directory server
	_, err = tds.EnsureEndpoint(ctx, &EnsureEndpointRequest{tenantID.ToUint64()})
	require.NoError(t, err)

	// Wait for background watcher to populate the initial endpoint.
	require.Eventually(t, func() bool {
		ips, _ := dir.LookupTenantIPs(ctx, tenantID)
		return len(ips) != 0
	}, 10*time.Second, 100*time.Millisecond)

	// Verify that EnsureTenantIP returns the endpoint's IP address.
	ip, err = dir.EnsureTenantIP(ctx, tenantID, "")
	require.NoError(t, err)
	require.NotEmpty(t, ip)

	processes = tds.Get(tenantID)
	require.NotNil(t, processes)
	require.Len(t, processes, 1)
	for dirIP := range processes {
		require.Equal(t, ip, dirIP.String())
	}

	// Stop the tenant and ensure its IP address is removed from the directory.
	for _, process := range processes {
		process.Stopper.Stop(ctx)
	}

	require.Eventually(t, func() bool {
		ips, _ := dir.LookupTenantIPs(ctx, tenantID)
		return len(ips) == 0
	}, 10*time.Second, 100*time.Millisecond)

	// Verify that a new call to EnsureTenantIP will resume again the tenant.
	ip, err = dir.EnsureTenantIP(ctx, tenantID, "")
	require.NoError(t, err)
	require.NotEmpty(t, ip)
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
			_, backgroundErrors[i] = dir.EnsureTenantIP(ctx, tenantID, "")
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

	var ips [lookupCount]string
	var wait sync.WaitGroup
	for i := 0; i < lookupCount; i++ {
		wait.Add(1)
		go func(i int) {
			var err error
			ips[i], err = dir.EnsureTenantIP(ctx, tenantID, "")
			require.NoError(t, err)
			wait.Done()
		}(i)
	}

	var processes map[net.Addr]*Process
	// Eventually the tenant process will be resumed.
	require.Eventually(t, func() bool {
		processes = tds.Get(tenantID)
		return len(processes) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Wait until background goroutines complete.
	wait.Wait()

	for ip := range processes {
		for i := 0; i < lookupCount; i++ {
			require.Equal(t, ip.String(), ips[i])
		}
	}
}

func TestDeleteTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	// Create the directory.
	ctx := context.Background()
	// Disable throttling for this test
	tc, dir, tds := newTestDirectory(t, RefreshDelay(-1))
	defer tc.Stopper().Stop(ctx)

	tenantID := roachpb.MakeTenantID(50)
	// Create test tenant.
	require.NoError(t, createTenant(tc, tenantID))

	// Perform lookup to create entry in cache.
	ip, err := dir.EnsureTenantIP(ctx, tenantID, "")
	require.NoError(t, err)
	require.NotEmpty(t, ip)

	// Report failure even though tenant is healthy - refresh should do nothing.
	require.NoError(t, dir.ReportFailure(ctx, tenantID, ip))
	ip, err = dir.EnsureTenantIP(ctx, tenantID, "")
	require.NoError(t, err)
	require.NotEmpty(t, ip)

	// Stop the tenant
	for _, process := range tds.Get(tenantID) {
		process.Stopper.Stop(ctx)
	}

	// Report failure connecting to the endpoint to force refresh of ips.
	require.NoError(t, dir.ReportFailure(ctx, tenantID, ip))

	// Ensure that tenant has no valid IP addresses.
	ips, err := dir.LookupTenantIPs(ctx, tenantID)
	require.NoError(t, err)
	require.Empty(t, ips)

	// Report failure again to ensure that works when there is no ip address.
	require.NoError(t, dir.ReportFailure(ctx, tenantID, ip))

	// Now delete the tenant.
	require.NoError(t, destroyTenant(tc, tenantID))

	// Now EnsureTenantIP should return an error and the directory should no
	// longer cache the tenant.
	_, err = dir.EnsureTenantIP(ctx, tenantID, "")
	require.Regexp(t, "not found", err)
	ips, err = dir.LookupTenantIPs(ctx, tenantID)
	require.Regexp(t, "not found", err)
	require.Nil(t, ips)
}

// TestRefreshThrottling checks that throttling works.
func TestRefreshThrottling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	// Create the directory, but with extreme rate limiting so that directory
	// will never refresh.
	ctx := context.Background()
	tc, dir, _ := newTestDirectory(t, RefreshDelay(60*time.Minute))
	defer tc.Stopper().Stop(ctx)

	// Create test tenant.
	tenantID := roachpb.MakeTenantID(60)
	require.NoError(t, createTenant(tc, tenantID))

	// Perform lookup to create entry in cache.
	ip, err := dir.EnsureTenantIP(ctx, tenantID, "")
	require.NoError(t, err)
	require.NotEmpty(t, ip)

	// Report a false failure and verify that IP is still present in the cache.
	require.NoError(t, dir.ReportFailure(ctx, tenantID, ip))
	ips, err := dir.LookupTenantIPs(ctx, tenantID)
	require.NoError(t, err)
	require.Equal(t, []string{ip}, ips)

	// Now destroy the tenant and call ReportFailure again. This should be a no-op
	// due to refresh throttling.
	require.NoError(t, destroyTenant(tc, tenantID))
	require.NoError(t, dir.ReportFailure(ctx, tenantID, ip))
	ips, err = dir.LookupTenantIPs(ctx, tenantID)
	require.NoError(t, err)
	require.Equal(t, []string{ip}, ips)
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
) (*Process, error) {
	log.TestingClearServerIdentifiers()
	tenantStopper := NewSubStopper(srv.Stopper())
	tenant, err := srv.StartTenant(
		ctx,
		base.TestTenantArgs{
			Existing:      true,
			TenantID:      roachpb.MakeTenantID(id),
			ForceInsecure: true,
			Stopper:       tenantStopper,
		})
	if err != nil {
		return nil, err
	}
	sqlAddr, err := net.ResolveTCPAddr("tcp", tenant.SQLAddr())
	if err != nil {
		return nil, err
	}
	return &Process{SQL: sqlAddr, Stopper: tenantStopper}, nil
}

// Setup directory that uses a client connected to a test directory server
// that manages tenants connected to a backing KV server.
func newTestDirectory(
	t *testing.T, opts ...DirOption,
) (tc serverutils.TestClusterInterface, directory *Directory, tds *TestDirectoryServer) {
	tc = serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		// We need to start the cluster insecure in order to not
		// care about TLS settings for the RPC client connection.
		ServerArgs: base.TestServerArgs{
			Insecure: true,
		},
	})
	clusterStopper := tc.Stopper()
	tds = NewTestDirectoryServer(clusterStopper)
	tds.TenantStarterFunc = func(ctx context.Context, tenantID uint64) (*Process, error) {
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
	clusterStopper.AddCloser(stop.CloserFn(func() { require.NoError(t, listenPort.Close()) }))
	go func() { _ = tds.Serve(listenPort) }()

	// Setup directory
	directorySrvAddr := listenPort.Addr()
	conn, err := grpc.Dial(directorySrvAddr.String(), grpc.WithInsecure())
	require.NoError(t, err)
	// nolint:grpcconnclose
	clusterStopper.AddCloser(stop.CloserFn(func() { require.NoError(t, conn.Close() /* nolint:grpcconnclose */) }))
	client := NewDirectoryClient(conn)
	directory, err = NewDirectory(context.Background(), clusterStopper, client, opts...)
	require.NoError(t, err)

	return
}
