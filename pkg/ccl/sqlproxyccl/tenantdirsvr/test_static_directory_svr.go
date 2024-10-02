// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantdirsvr

import (
	"container/list"
	"context"
	"net"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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

		// tenantPods stores a list of pods for every tenant. If a tenant does
		// not exist in the map, the tenant is assumed to be non-existent. Since
		// this is a test directory server, we do not care about fine-grained
		// locking here. Every entry must have a corresponding tenant in tenants.
		tenantPods map[roachpb.TenantID][]*tenant.Pod

		// tenants stores a list of tenant objects. Every tenant object should
		// have a corresponding entry in tenantPods.
		tenants map[roachpb.TenantID]*tenant.Tenant

		// podEventListeners stores a list of listeners that are watching changes
		// to pods through WatchPods.
		podEventListeners *list.List

		// tenantEventListeners stores a list of listeners that are watching
		// changes to tenants through WatchTenants.
		tenantEventListeners *list.List
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
	dir.mu.tenantPods = make(map[roachpb.TenantID][]*tenant.Pod)
	dir.mu.tenants = make(map[roachpb.TenantID]*tenant.Tenant)
	dir.mu.podEventListeners = list.New()
	dir.mu.tenantEventListeners = list.New()
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

	pods, ok := d.mu.tenantPods[roachpb.MustMakeTenantID(req.TenantID)]
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
		return d.mu.podEventListeners.PushBack(ch)
	}
	removeListener := func(e *list.Element) chan *tenant.WatchPodsResponse {
		d.mu.Lock()
		defer d.mu.Unlock()
		// Check for existence before removing the entry.
		//
		// list.Remove(e) will always return e.Value regardless of whether the
		// element exists in the list.
		for el := d.mu.podEventListeners.Front(); el != nil; el = el.Next() {
			if el.Value == e.Value {
				return d.mu.podEventListeners.Remove(el).(chan *tenant.WatchPodsResponse)
			}
		}
		return nil
	}

	// Construct the channel with a small buffer to allow for a burst of
	// notifications, and a slow receiver.
	c := make(chan *tenant.WatchPodsResponse, 10)
	chElement := addListener(c)

	initialPods := func() (result []tenant.WatchPodsResponse) {
		d.mu.Lock()
		defer d.mu.Unlock()
		for _, pods := range d.mu.tenantPods {
			for _, pod := range pods {
				result = append(result, tenant.WatchPodsResponse{
					Pod: protoutil.Clone(pod).(*tenant.Pod),
				})
			}
		}
		return result
	}()

	return stopper.RunTask(
		server.Context(),
		"watch-pods-server",
		func(ctx context.Context) {
			defer func() {
				if ch := removeListener(chElement); ch != nil {
					close(ch)
				}
			}()

			for i := range initialPods {
				if err := server.Send(&initialPods[i]); err != nil {
					return
				}
			}

			for {
				select {
				case e, ok := <-c:
					// Channel was closed.
					if !ok {
						return
					}
					if err := server.Send(e); err != nil {
						return
					}
				case <-stopper.ShouldQuiesce():
					return
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

	pods, ok := d.mu.tenantPods[roachpb.MustMakeTenantID(req.TenantID)]
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
	t, ok := d.mu.tenants[tenantID]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "tenant does not exist")
	}

	return &tenant.GetTenantResponse{Tenant: t}, nil
}

// WatchTenants allows callers to monitor for tenant update events.
//
// WatchTenants implements the tenant.DirectoryServer interface.
func (d *TestStaticDirectoryServer) WatchTenants(
	req *tenant.WatchTenantsRequest, server tenant.Directory_WatchTenantsServer,
) error {
	d.process.Lock()
	stopper := d.process.stopper
	d.process.Unlock()

	// This cannot happen unless WatchTenants was called directly, which we
	// shouldn't since it is meant to be called through a GRPC client.
	if stopper == nil {
		return status.Errorf(codes.FailedPrecondition, "directory server has not been started")
	}

	addListener := func(ch chan *tenant.WatchTenantsResponse) *list.Element {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.tenantEventListeners.PushBack(ch)
	}
	removeListener := func(e *list.Element) chan *tenant.WatchTenantsResponse {
		d.mu.Lock()
		defer d.mu.Unlock()
		// Check for existence before removing the entry.
		//
		// list.Remove(e) will always return e.Value regardless of whether the
		// element exists in the list.
		for el := d.mu.tenantEventListeners.Front(); el != nil; el = el.Next() {
			if el.Value == e.Value {
				return d.mu.tenantEventListeners.Remove(el).(chan *tenant.WatchTenantsResponse)
			}
		}
		return nil
	}

	// Construct the channel with a small buffer to allow for a burst of
	// notifications, and a slow receiver.
	c := make(chan *tenant.WatchTenantsResponse, 10)
	chElement := addListener(c)

	initialTenants := func() (result []tenant.WatchTenantsResponse) {
		d.mu.Lock()
		defer d.mu.Unlock()
		for id := range d.mu.tenants {
			result = append(result, tenant.WatchTenantsResponse{
				Type:   tenant.EVENT_ADDED,
				Tenant: protoutil.Clone(d.mu.tenants[id]).(*tenant.Tenant),
			})
		}
		return result
	}()

	return stopper.RunTask(
		server.Context(),
		"watch-tenants-server",
		func(ctx context.Context) {
			defer func() {
				if ch := removeListener(chElement); ch != nil {
					close(ch)
				}
			}()

			for i := range initialTenants {
				if err := server.Send(&initialTenants[i]); err != nil {
					return
				}
			}

			for {
				select {
				case e, ok := <-c:
					// Channel was closed.
					if !ok {
						return
					}
					if err := server.Send(e); err != nil {
						return
					}
				case <-stopper.ShouldQuiesce():
					return
				}
			}
		},
	)
}

// CreateTenant creates a tenant with the given tenant ID in the directory
// server. If the tenant already exists, this is a no-op.
func (d *TestStaticDirectoryServer) CreateTenant(tenantID roachpb.TenantID, t *tenant.Tenant) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.mu.tenants[tenantID]; ok {
		return
	}

	d.mu.tenants[tenantID] = t
	d.mu.tenantPods[tenantID] = make([]*tenant.Pod, 0)

	// Emit an event indicating that the tenant has been created.
	d.notifyTenantUpdateLocked(tenant.EVENT_ADDED, t)
}

// UpdateTenant updates the tenant with the given tenant ID in the directory
// server. If the tenant does not exist, this is a no-op.
func (d *TestStaticDirectoryServer) UpdateTenant(tenantID roachpb.TenantID, t *tenant.Tenant) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Tenant does not exist.
	if _, ok := d.mu.tenants[tenantID]; !ok {
		return
	}

	d.mu.tenants[tenantID] = t

	// Emit an event indicating that the tenant has been updated.
	d.notifyTenantUpdateLocked(tenant.EVENT_MODIFIED, t)
}

// DeleteTenant ensures that the tenant with the given tenant ID has been
// removed from the directory server. Doing this would return a NotFound error
// for certain directory server endpoints. This also changes the behavior of
// ListPods so no pods are returned for the given tenant.
func (d *TestStaticDirectoryServer) DeleteTenant(tenantID roachpb.TenantID) {
	d.mu.Lock()
	defer d.mu.Unlock()

	pods, ok := d.mu.tenantPods[tenantID]
	if !ok {
		return
	}
	for _, pod := range pods {
		// Update pod to DELETING, and emit event.
		pod.State = tenant.DELETING
		pod.StateTimestamp = d.timeSource.Now()
		d.notifyPodUpdateLocked(pod)
	}
	delete(d.mu.tenantPods, tenantID)
	delete(d.mu.tenants, tenantID)

	// Emit an event indicating that the tenant has been deleted.
	d.notifyTenantUpdateLocked(tenant.EVENT_DELETED, &tenant.Tenant{
		TenantID: tenantID.ToUint64(),
	})
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
	pods, ok := d.mu.tenantPods[tenantID]
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
			d.mu.tenantPods[tenantID][i] = &copyPod
			return true
		}
	}

	// A new pod has been added.
	d.mu.tenantPods[tenantID] = append(d.mu.tenantPods[tenantID], &copyPod)
	return true
}

// DrainPod puts the tenant associated SQL pod with the given address into the
// DRAINING state. DrainPod returns true if the operation was successful, and
// false otherwise.
func (d *TestStaticDirectoryServer) DrainPod(tenantID roachpb.TenantID, podAddr string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Tenant does not exist.
	pods, ok := d.mu.tenantPods[tenantID]
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
	pods, ok := d.mu.tenantPods[tenantID]
	if !ok {
		return false
	}

	// If the pod exists, remove it.
	for i, existing := range pods {
		if existing.Addr == podAddr {
			// Remove pod.
			copy(d.mu.tenantPods[tenantID][i:], d.mu.tenantPods[tenantID][i+1:])
			d.mu.tenantPods[tenantID] = d.mu.tenantPods[tenantID][:len(d.mu.tenantPods[tenantID])-1]

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

// WatchPodsListenersCount returns the number of active listeners for pod update
// events.
func (d *TestStaticDirectoryServer) WatchPodsListenersCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.mu.podEventListeners.Len()
}

// WatchTenantsListenersCount returns the number of active listeners for tenant
// update events.
func (d *TestStaticDirectoryServer) WatchTenantsListenersCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.mu.tenantEventListeners.Len()
}

// notifyPodUpdateLocked sends a pod update event to all WatchPods listeners.
func (d *TestStaticDirectoryServer) notifyPodUpdateLocked(pod *tenant.Pod) {
	// Make a copy of the pod to prevent race issues.
	copyPod := *pod
	res := &tenant.WatchPodsResponse{Pod: &copyPod}

	for e := d.mu.podEventListeners.Front(); e != nil; {
		select {
		case e.Value.(chan *tenant.WatchPodsResponse) <- res:
			e = e.Next()
		default:
			// The receiver is unable to consume fast enough. Close the channel
			// and remove it from the list.
			eToClose := e
			e = e.Next()
			ch := d.mu.podEventListeners.Remove(eToClose)
			close(ch.(chan *tenant.WatchPodsResponse))
		}
	}
}

// notifyTenantUpdateLocked sends a tenant update event to all WatchTenants listeners.
func (d *TestStaticDirectoryServer) notifyTenantUpdateLocked(
	typ tenant.WatchEventType, t *tenant.Tenant,
) {
	// Make a copy of the tenant to prevent race issues.
	res := &tenant.WatchTenantsResponse{
		Type:   typ,
		Tenant: protoutil.Clone(t).(*tenant.Tenant),
	}

	for e := d.mu.tenantEventListeners.Front(); e != nil; {
		select {
		case e.Value.(chan *tenant.WatchTenantsResponse) <- res:
			e = e.Next()
		default:
			// The receiver is unable to consume fast enough. Close the channel
			// and remove it from the list.
			eToClose := e
			e = e.Next()
			ch := d.mu.tenantEventListeners.Remove(eToClose)
			close(ch.(chan *tenant.WatchTenantsResponse))
		}
	}
}
