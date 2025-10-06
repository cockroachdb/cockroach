// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenant

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DirectoryCache is the external interface for the tenant directory cache.
//
// See directoryCache for more information.
type DirectoryCache interface {
	// LookupTenant returns the tenant entry associated to the requested tenant
	// ID. If the tenant cannot be found, this will return a GRPC NotFound
	// error.
	LookupTenant(ctx context.Context, tenantID roachpb.TenantID) (*Tenant, error)

	// LookupTenantPods returns a list of SQL pods in the RUNNING and DRAINING
	// states for the given tenant. This blocks until there is at least one
	// running SQL pod. If the tenant cannot be found, this will return a GRPC
	// NotFound error.
	LookupTenantPods(ctx context.Context, tenantID roachpb.TenantID) ([]*Pod, error)

	// TryLookupTenantPods returns a list of SQL pods in the RUNNING and
	// DRAINING states for the given tenant. It returns a GRPC NotFound error
	// if the tenant does not exist in the cache. Unlike LookupTenantPods which
	// blocks until there is an associated SQL pod, TryLookupTenantPods will
	// just return an empty set if no pods are available for the tenant.
	TryLookupTenantPods(ctx context.Context, tenantID roachpb.TenantID) ([]*Pod, error)

	// ReportFailure is used to indicate to the directory cache that a
	// connection attempt to connect to a particular SQL tenant pod have failed.
	ReportFailure(ctx context.Context, tenantID roachpb.TenantID, addr string) error
}

// dirOptions control the behavior of directoryCache.
type dirOptions struct {
	deterministic bool
	refreshDelay  time.Duration
	podWatcher    chan *Pod
	tenantWatcher chan *WatchTenantsResponse
}

// DirOption defines an option that can be passed to directoryCache in order
// to control its behavior.
type DirOption func(opts *dirOptions)

// RefreshDelay specifies the minimum amount of time that must elapse between
// attempts to refresh pods for a given tenant after ReportFailure is
// called. This delay has the effect of throttling calls to directory server, in
// order to avoid overloading it.
//
// RefreshDelay defaults to 100ms. Use -1 to never throttle.
func RefreshDelay(delay time.Duration) func(opts *dirOptions) {
	return func(opts *dirOptions) {
		opts.refreshDelay = delay
	}
}

// PodWatcher provides a callback channel to which tenant pod change
// notifications will be sent by the directory. Notifications will be sent when
// a tenant pod is created, modified, or destroyed.
// NOTE: The caller is responsible for handling the notifications by receiving
// from the channel; if it does not, it may block the background pod watcher
// goroutine.
func PodWatcher(podWatcher chan *Pod) func(opts *dirOptions) {
	return func(opts *dirOptions) {
		opts.podWatcher = podWatcher
	}
}

// TenantWatcher provides a callback channel to which tenant metadata change
// notifications will be sent by the directory.
//
// NOTE: The caller is responsible for handling the notifications by receiving
// from the channel; if it does not, it may block the background tenant watcher
// goroutine.
func TenantWatcher(tenantWatcher chan *WatchTenantsResponse) func(opts *dirOptions) {
	return func(opts *dirOptions) {
		opts.tenantWatcher = tenantWatcher
	}
}

// directoryCache tracks the network locations of SQL tenant processes. It is
// used by the sqlproxy to route incoming traffic to the correct backend process.
// Process information is populated and kept relatively up-to-date using a
// streaming watcher. However, since watchers deliver slightly stale
// information, the directory will also make direct server calls to fetch the
// latest information about a process that is not yet in the cache, or when a
// process is suspected to have failed. When a new tenant is created, or is
// resumed from suspension, this capability allows the directory to immediately
// return the IP address for the new process.
//
// All methods in the directory are thread-safe. Methods are intended to be
// called concurrently by many threads at once, and so locking is carefully
// designed to minimize contention. While a lock shared across tenants is used
// to synchronize access to shared in-memory data structures, each tenant also
// has its own locks that are used to synchronize per-tenant operations such as
// making directory server calls to fetch updated tenant information.
type directoryCache struct {
	// client is the directory client instance used to make directory server
	// calls.
	client DirectoryClient

	// stopper is used for graceful shutdown of the pod watcher.
	stopper *stop.Stopper

	// options control how the environment operates.
	options dirOptions

	// mut synchronizes access to the in-memory tenant entry caches. Take care
	// to never hold this lock during directory server calls - it should only be
	// used while adding and removing tenant entries to/from the caches.
	mut struct {
		syncutil.Mutex

		// tenants is a cache of tenant entries. Each entry tracks available IP
		// addresses for SQL processes for a given tenant. Entries may not be
		// fully initialized.
		tenants map[roachpb.TenantID]*tenantEntry
	}
}

var _ DirectoryCache = &directoryCache{}

// NewDirectoryCache constructs a new directoryCache instance that tracks SQL
// tenant processes managed by a given directory server. The given context is
// used for tracing pod watcher activity.
//
// NOTE: stopper.Stop must be called on the directory when it is no longer
// needed.
func NewDirectoryCache(
	ctx context.Context, stopper *stop.Stopper, client DirectoryClient, opts ...DirOption,
) (DirectoryCache, error) {
	dir := &directoryCache{client: client, stopper: stopper}

	dir.mut.tenants = make(map[roachpb.TenantID]*tenantEntry)
	for _, opt := range opts {
		opt(&dir.options)
	}
	if dir.options.refreshDelay == 0 {
		// Default to a delay of 100ms between refresh attempts for a given tenant.
		dir.options.refreshDelay = 100 * time.Millisecond
	}

	// Start the pod and tenant watchers on background goroutines.
	if err := dir.watchPods(ctx, stopper); err != nil {
		return nil, err
	}
	if err := dir.watchTenants(ctx, stopper); err != nil {
		return nil, err
	}

	return dir, nil
}

// LookupTenant returns the tenant entry associated to the requested tenant
// ID. If the tenant cannot be found, this will return a GRPC NotFound error.
//
// WARNING: Callers should never attempt to modify values returned by this
// method, or else they may be a race. Other instances may be reading from the
// same object.
//
// LookupTenant implements the DirectoryCache interface.
func (d *directoryCache) LookupTenant(
	ctx context.Context, tenantID roachpb.TenantID,
) (*Tenant, error) {
	// Ensure that a directory entry has been created for this tenant. This will
	// attempt to initialize the tenant in the cache.
	entry, err := d.getEntry(ctx, tenantID, true /* allowCreate */)
	if err != nil {
		return nil, err
	}
	return entry.ToProto(), nil
}

// LookupTenantPods returns a list of SQL pods in the RUNNING and DRAINING
// states for the given tenant. If the tenant was just created or is suspended,
// such that there are no available RUNNING processes, then LookupTenantPods
// will trigger resumption of a new instance (or a conversion of a DRAINING pod
// to a RUNNING one) and block until that happens. If the tenant cannot be
// found, this will return a GRPC NotFound error.
//
// WARNING: Callers should never attempt to modify values returned by this
// method, or else they may be a race. Other instances may be reading from the
// same slice.
//
// LookupTenantPods implements the DirectoryCache interface.
func (d *directoryCache) LookupTenantPods(
	ctx context.Context, tenantID roachpb.TenantID,
) ([]*Pod, error) {
	// Ensure that a directory entry has been created for this tenant.
	entry, err := d.getEntry(ctx, tenantID, true /* allowCreate */)
	if err != nil {
		return nil, err
	}

	ctx, cancel := d.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()

	tenantPods := entry.GetPods()

	// Trigger resumption if there are no RUNNING pods.
	if !hasRunningPod(tenantPods) {
		// There are no known pod IP addresses, so fetch pod information from
		// the directory server. Resume the tenant if it is suspended; that
		// will always result in at least one pod IP address (or an error).
		var err error
		if tenantPods, err = entry.EnsureTenantPod(ctx, d.client, d.options.deterministic); err != nil {
			if status.Code(err) == codes.NotFound {
				d.deleteEntry(entry)
			}
			return nil, err
		}
	}
	return tenantPods, nil
}

// TryLookupTenantPods returns a list of SQL pods in the RUNNING and DRAINING
// states for the given tenant. It returns a GRPC NotFound error if the tenant
// does not exist (e.g. it has not yet been created) or if it has not yet been
// fetched into the directory's cache (TryLookupTenantPods will never attempt to
// fetch it). If no processes are available for the tenant, TryLookupTenantPods
// will return the empty set (unlike LookupTenantPod).
//
// WARNING: Callers should never attempt to modify values returned by this
// method, or else they may be a race. Other instances may be reading from the
// same slice.
//
// TryLookupTenantPods implements the DirectoryCache interface.
func (d *directoryCache) TryLookupTenantPods(
	ctx context.Context, tenantID roachpb.TenantID,
) ([]*Pod, error) {
	entry, err := d.getEntry(ctx, tenantID, false /* allowCreate */)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, status.Errorf(
			codes.NotFound, "tenant %d not in directory cache", tenantID.ToUint64())
	}
	return entry.GetPods(), nil
}

// ReportFailure should be called when attempts to connect to a particular SQL
// tenant pod have failed. Since this could be due to a failed process,
// ReportFailure will attempt to refresh the cache with the latest information
// about available tenant processes.
//
// TODO(andyk): In the future, the ip parameter will be used to mark a
// particular pod as "unhealthy" so that it's less likely to be chosen.
// However, today there can be at most one pod for a given tenant, so it
// must always be chosen. Keep the parameter as a placeholder for the future.
//
// TODO(jaylim-crl): To implement the TODO above, one strawman idea is to add
// a healthy/unhealthy field (or failureCount) to *tenant.Pod. ReportFailure
// sets that field to unhealthy, and we'll have another ReportSuccess API that
// will reset that field to healthy once we have sufficient connection counts.
// When routing a connection to a SQL pod, the balancer could then use that
// field when calculating likelihoods.
//
// ReportFailure implements the DirectoryCache interface.
func (d *directoryCache) ReportFailure(
	ctx context.Context, tenantID roachpb.TenantID, addr string,
) error {
	entry, err := d.getEntry(ctx, tenantID, false /* allowCreate */)
	if err != nil {
		return err
	} else if entry == nil {
		// If no tenant is in the cache, no-op.
		return nil
	}

	// Refresh the entry in case there is a new pod IP address.
	return entry.RefreshPods(ctx, d.client)
}

// getEntry returns a directory entry for the given tenant. If the directory
// does not contain such an entry, then getEntry will create one if allowCreate
// is true. Otherwise, it returns nil. If an entry is returned, then getEntry
// ensures that it is fully initialized with tenant metadata. Obtaining this
// metadata requires making a separate directory server call;
// getEntry will block until that's complete.
func (d *directoryCache) getEntry(
	ctx context.Context, tenantID roachpb.TenantID, allowCreate bool,
) (*tenantEntry, error) {
	entry := func() *tenantEntry {
		// Acquire the directory lock just long enough to check the tenants map
		// for the given tenant ID. Don't complete initialization while holding
		// this lock, since that requires directory server calls.
		d.mut.Lock()
		defer d.mut.Unlock()

		entry, ok := d.mut.tenants[tenantID]
		if ok {
			// Entry exists, so return it.
			return entry
		}

		if !allowCreate {
			// No entry, but not allowed to create one, so done.
			return nil
		}

		// Create the tenant entry and enter it into the tenants map.
		log.Infof(ctx, "creating directory entry for tenant %d", tenantID)
		entry = &tenantEntry{TenantID: tenantID, RefreshDelay: d.options.refreshDelay}
		d.mut.tenants[tenantID] = entry
		return entry
	}()

	if entry == nil {
		return nil, nil
	}

	// Initialize the entry now if not yet done.
	err := entry.Initialize(ctx, d.client)
	if err != nil {
		// Remove the entry from the tenants map, since initialization failed.
		if d.deleteEntry(entry) {
			log.Infof(ctx, "error initializing tenant %d: %v", tenantID, err)
		}
		return nil, err
	}

	return entry, nil
}

// deleteEntry removes the given directory entry for the given tenant, if it
// exists. It returns true if an entry was actually deleted.
func (d *directoryCache) deleteEntry(entry *tenantEntry) bool {
	// Remove the entry from the tenants map, since initialization failed.
	d.mut.Lock()
	defer d.mut.Unlock()

	// Threads can race to add/remove entries, so ensure that right entry is
	// removed.
	existing, ok := d.mut.tenants[entry.TenantID]
	if ok && entry == existing {
		delete(d.mut.tenants, entry.TenantID)
		return true
	}

	return false
}

// watchPods establishes a watcher that looks for changes to tenant pods.
// Whenever tenant pods start or terminate, the watcher will get a notification
// and update the directory to reflect that change.
func (d *directoryCache) watchPods(ctx context.Context, stopper *stop.Stopper) error {
	req := WatchPodsRequest{}

	// The loop that processes the event stream is running in a separate go
	// routine. It is desirable however, before we return, to have a guarantee
	// that the separate go routine started processing events. This wait group
	// helps us achieve this. Without the wait group, it will be possible to:
	//
	// 1. call watchPods
	// 2. call LookupTenantPods
	// 3. wait forever to receive notification about the tenant that just started.
	//
	// The reason why the notification may not ever arrive is because the
	// watchPods goroutine can start listening after the server started the
	// tenant and sent notifications.
	var waitInit sync.WaitGroup
	waitInit.Add(1)

	err := stopper.RunAsyncTask(ctx, "watch-pods-client", func(ctx context.Context) {
		var client Directory_WatchPodsClient
		var err error
		firstRun := true
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		watchPodsErr := log.Every(10 * time.Second)
		recvErr := log.Every(10 * time.Second)

		for ctx.Err() == nil {
			if client == nil {
				client, err = d.client.WatchPods(ctx, &req)
				if firstRun {
					waitInit.Done()
					firstRun = false
				}
				if err != nil {
					if watchPodsErr.ShouldLog() {
						log.Errorf(ctx, "err creating new watch pod client: %s", err)
					}
					sleepContext(ctx, time.Second)
					continue
				} else {
					log.Info(ctx, "established watch on pods")
				}
			}

			// Read the next watcher event.
			resp, err := client.Recv()
			if err != nil {
				if recvErr.ShouldLog() {
					log.Errorf(ctx, "err receiving stream events: %s", err)
				}
				// If stream ends, immediately try to establish a new one. Otherwise,
				// wait for a second to avoid slamming server.
				if err != io.EOF {
					time.Sleep(time.Second)
				}
				client = nil
				continue
			}

			// Update the directory entry for the tenant with the latest
			// information about this pod.
			d.updateTenantPodEntry(ctx, resp.Pod)

			// If caller is watching pods, send to its channel now. Only do this
			// after updating the tenant entry in the directory.
			if d.options.podWatcher != nil {
				select {
				case d.options.podWatcher <- resp.Pod:
				case <-ctx.Done():
					return
				}
			}
		}
	})
	if err != nil {
		return err
	}

	// Block until the initial pod watcher client stream is constructed.
	waitInit.Wait()
	return err
}

// updateTenantPodEntry keeps tenant directory entries up-to-date by handling pod
// watcher events. When a pod is created, destroyed, or modified, it updates the
// tenant's entry to reflect that change.
func (d *directoryCache) updateTenantPodEntry(ctx context.Context, pod *Pod) {
	if pod.Addr == "" || pod.TenantID == 0 {
		// Nothing needs to be done if there is no IP address specified.
		// We also check on TenantID here because roachpb.MustMakeTenantID will
		// panic with TenantID = 0.
		return
	}

	// Ensure that a directory entry exists for this tenant.
	entry, err := d.getEntry(ctx, roachpb.MustMakeTenantID(pod.TenantID), true /* allowCreate */)
	if err != nil {
		if !grpcutil.IsContextCanceled(err) {
			// This should only happen in case of a deleted tenant or a transient
			// error during fetch of tenant metadata (i.e. very rarely).
			log.Errorf(ctx, "ignoring error getting entry for tenant %d: %v", pod.TenantID, err)
		}
		return
	}

	switch pod.State {
	case RUNNING, DRAINING:
		// Add entries of RUNNING and DRAINING pods if they are not already present.
		if entry.AddPod(pod) {
			log.Infof(ctx, "added IP address %s for tenant %d", pod.Addr, pod.TenantID)
		} else {
			log.Infof(ctx, "updated IP address %s for tenant %d", pod.Addr, pod.TenantID)
		}
	case DELETING:
		// Remove addresses of DELETING pods.
		if entry.RemovePodByAddr(pod.Addr) {
			log.Infof(ctx, "deleted IP address %s for tenant %d", pod.Addr, pod.TenantID)
		}
	default:
		// Pods with UNKNOWN state.
		log.Infof(ctx, "invalid pod entry with IP address %s for tenant %d", pod.Addr, pod.TenantID)
	}
}

// watchTenants establishes a watcher that looks for changes to tenants.
// Whenever tenants get created, updated, or deleted, the watcher will get a
// notification and update the directory to reflect that change.
func (d *directoryCache) watchTenants(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "watch-tenants-client", func(ctx context.Context) {
		var client Directory_WatchTenantsClient
		var err error
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		watchTenantsErr := log.Every(10 * time.Second)
		recvErr := log.Every(10 * time.Second)

		for ctx.Err() == nil {
			if client == nil {
				client, err = d.client.WatchTenants(ctx, &WatchTenantsRequest{})
				if err != nil {
					if watchTenantsErr.ShouldLog() {
						log.Errorf(ctx, "err creating new watch tenant client: %s", err)
					}
					sleepContext(ctx, time.Second)
					continue
				} else {
					log.Info(ctx, "established watch on tenants")
				}
			}

			// Read the next watcher event.
			resp, err := client.Recv()
			if err != nil {
				if recvErr.ShouldLog() {
					log.Errorf(ctx, "err receiving stream events: %s", err)
				}
				// If stream ends, immediately try to establish a new one.
				// Otherwise, wait for a second to avoid slamming server.
				if err != io.EOF {
					time.Sleep(time.Second)
				}
				client = nil
				// Whenever the watcher errors, we will continue serving stale
				// entries (until the watcher catches up). There will not be
				// any rollback issues since entries are only replaced if the
				// Version field holds a newer value. Note that deleted tenants
				// will only be removed once the proxy receives a new connection
				// request to that tenant.
				//
				// We have decided to serve stale entries because there's a
				// possibility where the directory server is failing, but
				// routing to existing SQL pods still works.
				continue
			}

			// Update the directory entry for this tenant.
			d.updateTenantMetadataEntry(ctx, resp.Tenant, resp.Type)

			// If caller is watching tenants, send to its channel now. Only do
			// this after updating the tenant in the directory cache.
			if d.options.tenantWatcher != nil {
				select {
				case d.options.tenantWatcher <- resp:
				case <-ctx.Done():
					return
				}
			}
		}
	})
}

// updateTenantMetadataEntry keeps tenant directory entries up-to-date by
// handling tenant watcher events.
func (d *directoryCache) updateTenantMetadataEntry(
	ctx context.Context, tenant *Tenant, typ WatchEventType,
) {
	if tenant.TenantID == 0 {
		// Check on TenantID because roachpb.MustMakeTenantID will panic with
		// TenantID = 0.
		return
	}

	// Use allowCreate=false here since we're only interested in tenants that
	// are already in the cache.
	entry, err := d.getEntry(ctx, roachpb.MustMakeTenantID(tenant.TenantID), false /* allowCreate */)
	if err != nil {
		if !grpcutil.IsContextCanceled(err) {
			// This should only happen in case of a deleted tenant or a transient
			// error during fetch of tenant metadata (i.e. very rarely).
			log.Errorf(ctx, "ignoring error getting entry for tenant %d: %v", tenant.TenantID, err)
		}
		return
	}

	// Tenant does not exist in the cache. Do not attempt to update.
	if entry == nil {
		return
	}

	switch typ {
	case EVENT_ADDED, EVENT_MODIFIED:
		entry.UpdateTenant(tenant)
		log.Infof(ctx, "updated entry for tenant %d: %v", tenant.TenantID, tenant)
	case EVENT_DELETED:
		entry.MarkInvalid()
		log.Infof(ctx, "invalidating entry for tenant %d", tenant.TenantID)
	default:
		// Watch events with EVENT_UNKNOWN type
		log.Infof(ctx, "invalid watcher entry for tenant %d: %v", tenant.TenantID, tenant)
	}
}

// sleepContext sleeps for the given duration or until the given context is
// canceled, whichever comes first.
func sleepContext(ctx context.Context, delay time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}
