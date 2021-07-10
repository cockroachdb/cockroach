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

// dirOptions control the behavior of tenant.Directory.
type dirOptions struct {
	deterministic bool
	refreshDelay  time.Duration
	podWatcher    chan *Pod
}

// DirOption defines an option that can be passed to tenant.Directory in order
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

// Directory tracks the network locations of SQL tenant processes. It is used by
// the sqlproxy to route incoming traffic to the correct backend process.
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
type Directory struct {
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

// NewDirectory constructs a new Directory instance that tracks SQL tenant
// processes managed by a given Directory server. The given context is used for
// tracing pod watcher activity.
//
// NOTE: stopper.Stop must be called on the directory when it is no longer
// needed.
func NewDirectory(
	ctx context.Context, stopper *stop.Stopper, client DirectoryClient, opts ...DirOption,
) (*Directory, error) {
	dir := &Directory{client: client, stopper: stopper}

	dir.mut.tenants = make(map[roachpb.TenantID]*tenantEntry)
	for _, opt := range opts {
		opt(&dir.options)
	}
	if dir.options.refreshDelay == 0 {
		// Default to a delay of 100ms between refresh attempts for a given tenant.
		dir.options.refreshDelay = 100 * time.Millisecond
	}

	// Start the pod watcher on a background goroutine.
	if err := dir.watchPods(ctx, stopper); err != nil {
		return nil, err
	}

	return dir, nil
}

// EnsureTenantAddr returns the IP address of one of the given tenant's SQL
// processes. If the tenant was just created or is suspended, such that there
// are no available processes, then EnsureTenantAddr will trigger resumption of a
// new instance and block until the process is ready. If there are multiple
// processes for the tenant, then LookupTenantAddrs will choose one of them (note
// that currently there is always at most one SQL process per tenant).
//
// If clusterName is non-empty, then a GRPC NotFound error is returned if no
// pods match the cluster name. This can be used to ensure that the
// incoming SQL connection "knows" some additional information about the tenant,
// such as the name of the cluster, before being allowed to connect. Similarly,
// if the tenant does not exist (e.g. because it was deleted), EnsureTenantAddr
// returns a GRPC NotFound error.
func (d *Directory) EnsureTenantAddr(
	ctx context.Context, tenantID roachpb.TenantID, clusterName string,
) (string, error) {
	// Ensure that a directory entry has been created for this tenant.
	entry, err := d.getEntry(ctx, tenantID, true /* allowCreate */)
	if err != nil {
		return "", err
	}

	// Check the cluster name matches, if specified.
	if clusterName != "" && clusterName != entry.ClusterName {
		// Return a GRPC NotFound error.
		log.Errorf(ctx, "cluster name %s doesn't match expected %s", clusterName, entry.ClusterName)
		return "", status.Errorf(codes.NotFound,
			"cluster name %s doesn't match expected %s", clusterName, entry.ClusterName)
	}

	ctx, _ = d.stopper.WithCancelOnQuiesce(ctx)
	addr, err := entry.ChoosePodAddr(ctx, d.client, d.options.deterministic)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			d.deleteEntry(entry)
		}
	}
	return addr, err
}

// LookupTenantAddrs returns the IP addresses for all available SQL processes for
// the given tenant. It returns a GRPC NotFound error if the tenant does not
// exist (e.g. it has not yet been created) or if it has not yet been fetched
// into the directory's cache (LookupTenantAddrs will never attempt to fetch it).
// If no processes are available for the tenant, LookupTenantAddrs will return the
// empty set (unlike EnsureTenantAddr).
func (d *Directory) LookupTenantAddrs(
	ctx context.Context, tenantID roachpb.TenantID,
) ([]string, error) {
	// Ensure that a directory entry has been created for this tenant.
	entry, err := d.getEntry(ctx, tenantID, false /* allowCreate */)
	if err != nil {
		return nil, err
	}

	if entry == nil {
		return nil, status.Errorf(
			codes.NotFound, "tenant %d not in directory cache", tenantID.ToUint64())
	}
	return entry.getPodAddrs(), nil
}

// ReportFailure should be called when attempts to connect to a particular SQL
// tenant pod have failed. Since this could be due to a failed process,
// ReportFailure will attempt to refresh the cache with the latest information
// about available tenant processes.
// TODO(andyk): In the future, the ip parameter will be used to mark a
// particular pod as "unhealthy" so that it's less likely to be chosen.
// However, today there can be at most one pod for a given tenant, so it
// must always be chosen. Keep the parameter as a placeholder for the future.
func (d *Directory) ReportFailure(
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
func (d *Directory) getEntry(
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
func (d *Directory) deleteEntry(entry *tenantEntry) bool {
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
func (d *Directory) watchPods(ctx context.Context, stopper *stop.Stopper) error {
	req := WatchPodsRequest{}

	// The loop that processes the event stream is running in a separate go
	// routine. It is desirable however, before we return, to have a guarantee
	// that the separate go routine started processing events. This wait group
	// helps us achieve this. Without the wait group, it will be possible to:
	//
	// 1. call watchPods
	// 2. call EnsureTenantAddr
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
		ctx, _ = stopper.WithCancelOnQuiesce(ctx)

		watchPodsErr := log.Every(10 * time.Second)
		recvErr := log.Every(10 * time.Second)

		for {
			if client == nil {
				client, err = d.client.WatchPods(ctx, &req)
				if firstRun {
					waitInit.Done()
					firstRun = false
				}
				if err != nil {
					if grpcutil.IsContextCanceled(err) {
						break
					}
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
				if grpcutil.IsContextCanceled(err) {
					break
				}
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

			// If caller is watching pods, send to its channel now.
			if d.options.podWatcher != nil {
				select {
				case d.options.podWatcher <- resp.Pod:
				case <-ctx.Done():
					break
				}
			}

			// Update the directory entry for the tenant with the latest
			// information about this pod.
			d.updateTenantEntry(ctx, resp.Pod)
		}
	})
	if err != nil {
		return err
	}

	// Block until the initial pod watcher client stream is constructed.
	waitInit.Wait()
	return err
}

// updateTenantEntry keeps tenant directory entries up-to-date by handling pod
// watcher events. When a pod is created, destroyed, or modified, it updates the
// tenant's entry to reflect that change.
func (d *Directory) updateTenantEntry(ctx context.Context, pod *Pod) {
	podAddr := pod.Addr
	if podAddr == "" {
		// Nothing needs to be done if there is no IP address specified.
		return
	}

	// Ensure that a directory entry exists for this tenant.
	entry, err := d.getEntry(ctx, roachpb.MakeTenantID(pod.TenantID), true /* allowCreate */)
	if err != nil {
		if !grpcutil.IsContextCanceled(err) {
			// This should only happen in case of a deleted tenant or a transient
			// error during fetch of tenant metadata (i.e. very rarely).
			log.Errorf(ctx, "ignoring error getting entry for tenant %d: %v", pod.TenantID, err)
		}
		return
	}

	if pod.State == RUNNING {
		// Add addresses of RUNNING pods if they are not already present.
		if entry.AddPodAddr(podAddr) {
			log.Infof(ctx, "added IP address %s for tenant %d", podAddr, pod.TenantID)
		}
	} else {
		// Remove addresses of DRAINING and DELETING pods.
		if entry.RemovePodAddr(podAddr) {
			log.Infof(ctx, "deleted IP address %s for tenant %d", podAddr, pod.TenantID)
		}
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
