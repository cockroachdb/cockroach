// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantdirsvr

import (
	"container/list"
	"context"
	"net"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/test/bufconn"
)

// TestStaticDirectoryServer is a directory server that stores a static mapping
// of tenants to their pods. Callers will need to invoke operations on the
// directory server to create/delete/update tenants and pods as necessary.
// Unlike the regular directory server that automatically spins up SQL pods when
// one isn't present, the static directory server does not do that. The caller
// should start the SQL pods up, and register them with the directory server.
type TestStaticDirectoryServer struct {
	// rootStopper is used to create sub-stoppers for directory instances.
	rootStopper *stop.Stopper

	// timeSource is the source of the time. By default, this will be set to
	// timeutil.DefaultTimeSource.
	timeSource timeutil.TimeSource

	// process corresponds to fields that need to be updated together whenever
	// the test directory is started or stopped. Note that when the test
	// directory server gets restarted, we do not reset the tenant data.
	process struct {
		syncutil.Mutex

		// stopper is used to start async tasks within the directory server.
		stopper *stop.Stopper

		// ln corresponds to the in-memory listener for the directory
		// server. This will be used to construct a dialer function.
		ln *bufconn.Listener

		// grpcServer corresponds to the GRPC server instance for the
		// directory server.
		grpcServer *grpc.Server
	}

	mu struct {
		syncutil.Mutex

		// tenants stores a list of pods for every tenant. If a tenant does not
		// exist in the map, the tenant is assumed to be non-existent. Since
		// this is a test directory server, we do not care about fine-grained
		// locking here.
		tenants map[roachpb.TenantID][]*tenant.Pod

		// tenantNames stores a list of clusterNames associated with each tenant.
		tenantNames map[roachpb.TenantID]string

		// eventListeners stores a list of listeners that are watching changes
		// to pods through WatchPods.
		eventListeners *list.List
	}
}

var _ tenant.DirectoryServer = &TestStaticDirectoryServer{}

// NewTestStaticDirectoryServer constructs a new static directory server.
func NewTestStaticDirectoryServer(
	stopper *stop.Stopper, timeSource timeutil.TimeSource,
) *TestStaticDirectoryServer {
	if timeSource == nil {
		timeSource = timeutil.DefaultTimeSource{}
	}
	dir := &TestStaticDirectoryServer{rootStopper: stopper, timeSource: timeSource}
	dir.mu.tenants = make(map[roachpb.TenantID][]*tenant.Pod)
	dir.mu.tenantNames = make(map[roachpb.TenantID]string)
	dir.mu.eventListeners = list.New()
	return dir
}

// ListPods returns a list with all SQL pods associated with the given tenant.
// If the tenant does not exists, no pods will be returned.
//
// ListPods implements the tenant.DirectoryServer interface.
func (d *TestStaticDirectoryServer) ListPods(
	ctx context.Context, req *tenant.ListPodsRequest,
) (*tenant.ListPodsResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	pods, ok := d.mu.tenants[roachpb.MustMakeTenantID(req.TenantID)]
	if !ok {
		return &tenant.ListPodsResponse{}, nil
	}

	// Return a copy of all the pods to avoid any race issues.
	tenantPods := make([]*tenant.Pod, len(pods))
	for i, pod := range pods {
		copyPod := *pod
		tenantPods[i] = &copyPod
	}
	return &tenant.ListPodsResponse{Pods: tenantPods}, nil
}

// WatchPods allows callers to monitor for pod update events.
//
// WatchPods implements the tenant.DirectoryServer interface.
func (d *TestStaticDirectoryServer) WatchPods(
	req *tenant.WatchPodsRequest, server tenant.Directory_WatchPodsServer,
) error {
	d.process.Lock()
	stopper := d.process.stopper
	d.process.Unlock()

	// This cannot happen unless WatchPods was called directly, which we
	// shouldn't since it is meant to be called through a GRPC client.
	if stopper == nil {
		return status.Errorf(codes.FailedPrecondition, "directory server has not been started")
	}

	addListener := func(ch chan *tenant.WatchPodsResponse) *list.Element {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.eventListeners.PushBack(ch)
	}
	removeListener := func(e *list.Element) chan *tenant.WatchPodsResponse {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.eventListeners.Remove(e).(chan *tenant.WatchPodsResponse)
	}

	// Construct the channel with a small buffer to allow for a burst of
	// notifications, and a slow receiver.
	c := make(chan *tenant.WatchPodsResponse, 10)
	chElement := addListener(c)

	return stopper.RunTask(
		context.Background(),
		"watch-pods-server",
		func(ctx context.Context) {
			defer func() {
				if ch := removeListener(chElement); ch != nil {
					close(ch)
				}
			}()

			for watch := true; watch; {
				select {
				case e, ok := <-c:
					// Channel was closed.
					if !ok {
						watch = false
						break
					}
					if err := server.Send(e); err != nil {
						watch = false
					}
				case <-stopper.ShouldQuiesce():
					watch = false
				}
			}
		},
	)
}

// EnsurePod returns an empty response if a tenant with the given tenant ID
// exists, and there is at least one SQL pod. If there are no SQL pods, a GRPC
// FailedPrecondition error will be returned. Similarly, if the tenant does not
// exists, a GRPC NotFound error will be returned. This would mimic the behavior
// that we have in the actual tenant directory.
//
// EnsurePod implements the tenant.DirectoryServer interface.
func (d *TestStaticDirectoryServer) EnsurePod(
	ctx context.Context, req *tenant.EnsurePodRequest,
) (*tenant.EnsurePodResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	pods, ok := d.mu.tenants[roachpb.MustMakeTenantID(req.TenantID)]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "tenant does not exist")
	}
	if len(pods) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "tenant has no pods")
	}
	return &tenant.EnsurePodResponse{}, nil
}

// GetTenant returns tenant metadata associated with the given tenant ID. If the
// tenant isn't in the directory server, a GRPC NotFound error will be returned.
//
// GetTenant implements the tenant.DirectoryServer interface.
func (d *TestStaticDirectoryServer) GetTenant(
	ctx context.Context, req *tenant.GetTenantRequest,
) (*tenant.GetTenantResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	tenantID := roachpb.MustMakeTenantID(req.TenantID)
	if _, ok := d.mu.tenants[tenantID]; !ok {
		return nil, status.Errorf(codes.NotFound, "tenant does not exist")
	}
	return &tenant.GetTenantResponse{ClusterName: d.mu.tenantNames[tenantID]}, nil
}

// CreateTenant creates a tenant with the given tenant ID in the directory
// server. If the tenant already exists, this is a no-op.
func (d *TestStaticDirectoryServer) CreateTenant(tenantID roachpb.TenantID, clusterName string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.mu.tenants[tenantID]; ok {
		return
	}
	d.mu.tenants[tenantID] = make([]*tenant.Pod, 0)
	d.mu.tenantNames[tenantID] = clusterName
}

// DeleteTenant ensures that the tenant with the given tenant ID has been
// removed from the directory server. Doing this would return a NotFound error
// for certain directory server endpoints. This also changes the behavior of
// ListPods so no pods are returned for the given tenant.
func (d *TestStaticDirectoryServer) DeleteTenant(tenantID roachpb.TenantID) {
	d.mu.Lock()
	defer d.mu.Unlock()

	pods, ok := d.mu.tenants[tenantID]
	if !ok {
		return
	}
	for _, pod := range pods {
		// Update pod to DELETING, and emit event.
		pod.State = tenant.DELETING
		pod.StateTimestamp = d.timeSource.Now()
		d.notifyPodUpdateLocked(pod)
	}
	delete(d.mu.tenants, tenantID)
	delete(d.mu.tenantNames, tenantID)
}

// AddPod adds the pod to the given tenant pod's list. If a SQL pod with the
// same address already exists, this updates the existing pod. AddPod returns
// true if the operation was successful, and false otherwise.
//
// NOTE: pod has to be fully populated, and pod.TenantID should be the same as
// the given tenantID.
func (d *TestStaticDirectoryServer) AddPod(tenantID roachpb.TenantID, pod *tenant.Pod) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Tenant does not exist.
	pods, ok := d.mu.tenants[tenantID]
	if !ok {
		return false
	}

	// Emit an event that the pod has been created.
	d.notifyPodUpdateLocked(pod)

	// Make a copy of the pod so that any changes to the pod's state would not
	// mutate the original data.
	copyPod := *pod

	// Check if the pod exists. This would handle pods transitioning from
	// DRAINING to RUNNING.
	for i, existing := range pods {
		if existing.Addr == copyPod.Addr {
			d.mu.tenants[tenantID][i] = &copyPod
			return true
		}
	}

	// A new pod has been added.
	d.mu.tenants[tenantID] = append(d.mu.tenants[tenantID], &copyPod)
	return true
}

// DrainPod puts the tenant associated SQL pod with the given address into the
// DRAINING state. DrainPod returns true if the operation was successful, and
// false otherwise.
func (d *TestStaticDirectoryServer) DrainPod(tenantID roachpb.TenantID, podAddr string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Tenant does not exist.
	pods, ok := d.mu.tenants[tenantID]
	if !ok {
		return false
	}

	// If the pod exists, update its state to DRAINING.
	for _, existing := range pods {
		if existing.Addr == podAddr {
			existing.State = tenant.DRAINING
			existing.StateTimestamp = d.timeSource.Now()
			d.notifyPodUpdateLocked(existing)
			return true
		}
	}
	return false
}

// RemovePod deletes the SQL pod with the given address from the associated
// tenant. RemovePod returns true if the operation was successful, and false
// otherwise.
func (d *TestStaticDirectoryServer) RemovePod(tenantID roachpb.TenantID, podAddr string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Tenant does not exist.
	pods, ok := d.mu.tenants[tenantID]
	if !ok {
		return false
	}

	// If the pod exists, remove it.
	for i, existing := range pods {
		if existing.Addr == podAddr {
			// Remove pod.
			copy(d.mu.tenants[tenantID][i:], d.mu.tenants[tenantID][i+1:])
			d.mu.tenants[tenantID] = d.mu.tenants[tenantID][:len(d.mu.tenants[tenantID])-1]

			// Update pod to DELETING, and emit event.
			existing.State = tenant.DELETING
			existing.StateTimestamp = d.timeSource.Now()
			d.notifyPodUpdateLocked(existing)
			return true
		}
	}
	return false
}

// Start starts the test directory server using an in-memory listener. This
// returns an error if the server cannot be started. If the server has already
// been started, this is a no-op.
func (d *TestStaticDirectoryServer) Start(ctx context.Context) error {
	d.process.Lock()
	defer d.process.Unlock()

	// Server has already been started.
	if d.process.ln != nil {
		return nil
	}

	stopper := NewSubStopper(d.rootStopper)
	grpcServer := grpc.NewServer()
	tenant.RegisterDirectoryServer(grpcServer, d)
	ln, err := ListenAndServeInMemGRPC(ctx, stopper, grpcServer)
	if err != nil {
		return err
	}

	// Update instance fields together.
	d.process.stopper = stopper
	d.process.ln = ln
	d.process.grpcServer = grpcServer
	return nil
}

// Stop stops the test directory server instance. If the server has already
// been stopped, this is a no-op.
func (d *TestStaticDirectoryServer) Stop(ctx context.Context) {
	d.process.Lock()
	defer d.process.Unlock()

	stopper, ln, grpcServer := d.process.stopper, d.process.ln, d.process.grpcServer
	if ln == nil {
		return
	}

	d.process.stopper = nil
	d.process.ln = nil
	d.process.grpcServer = nil

	// Close listener first, followed by GRPC server, and stopper.
	_ = ln.Close()
	grpcServer.Stop()
	stopper.Stop(ctx)
}

// DialerFunc corresponds to the dialer function used to dial the test directory
// server. We do this because the test directory server runs in-memory, and
// does not bind to a physical network address. This will be used within
// grpc.WithContextDialer.
func (d *TestStaticDirectoryServer) DialerFunc(ctx context.Context, addr string) (net.Conn, error) {
	d.process.Lock()
	listener := d.process.ln
	d.process.Unlock()

	if listener == nil {
		return nil, errors.New("directory server has not been started")
	}
	return listener.DialContext(ctx)
}

// WatchListenersCount returns the number of active listeners for pod update
// events.
func (d *TestStaticDirectoryServer) WatchListenersCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.mu.eventListeners.Len()
}

// notifyPodUpdateLocked sends a pod update event to all WatchPods listeners.
func (d *TestStaticDirectoryServer) notifyPodUpdateLocked(pod *tenant.Pod) {
	// Make a copy of the pod to prevent race issues.
	copyPod := *pod
	res := &tenant.WatchPodsResponse{Pod: &copyPod}

	for e := d.mu.eventListeners.Front(); e != nil; {
		select {
		case e.Value.(chan *tenant.WatchPodsResponse) <- res:
			e = e.Next()
		default:
			// The receiver is unable to consume fast enough. Close the channel
			// and remove it from the list.
			eToClose := e
			e = e.Next()
			ch := d.mu.eventListeners.Remove(eToClose)
			close(ch.(chan *tenant.WatchPodsResponse))
		}
	}
}
