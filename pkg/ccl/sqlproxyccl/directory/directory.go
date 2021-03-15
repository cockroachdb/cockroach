package directory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// dirOptions control the behavior of tenant.Directory.
type dirOptions struct {
	deterministic bool
	refreshDelay  time.Duration
}

// DirOption defines an option that can be passed to tenant.Directory in order
// to control its behavior.
type DirOption func(opts *dirOptions)

// DeterministicTestMode has the following impact:
//   1. The pod watcher is not started, since this runs in a background
//      goroutine that is non-deterministic.
//   2. If no pods are ready for a tenant, then LookupPodByID returns an error
//      rather than blocking until a pod is ready. This allows tests to regain
//      control on the foreground goroutine in order to create a pod.
var DeterministicTestMode = func(opts *dirOptions) {
	opts.deterministic = true
}

// RefreshDelay specifies the minimum amount of time that must elapse between
// attempts to refresh pods for a given tenant after ReportFailure is called.
// This delay has the effect of throttling calls to the K8s API, in order to
// avoid overloading it.
//
// RefreshDelay defaults to 100ms. Use -1 to never throttle.
func RefreshDelay(delay time.Duration) func(opts *dirOptions) {
	return func(opts *dirOptions) {
		opts.refreshDelay = delay
	}
}

// Directory tracks the network locations of SQL tenant pods. It is used by the
// sqlproxy to route incoming traffic to the correct pod. Pod information is
// populated and kept relatively up-to-date using a K8s watcher. However, since
// watchers deliver slightly stale information, the directory will also make
// direct K8s calls to fetch the latest information about a pod that is not yet
// in the cache, or when a pod is suspected to have failed. When a new tenant is
// created, or is resumed from suspension, this capability allows the directory
// to immediately return the IP address for the new pod.
//
// All methods in the directory are thread-safe. Methods are intended to be
// called concurrently by many threads at once, and so locking is carefully
// designed to minimize contention. While a lock shared across tenants is used
// to synchronize access to shared in-memory data structures, each tenant also
// has its own locks that are used to synchronize per-tenant operations such as
// making K8s calls to fetch updated tenant information.
type Directory struct {
	// ctl is the kube.Ctl instance used to make K8s API calls.
	ctl DirectoryClient

	// options control how the environment operates.
	options dirOptions

	// mut synchronizes access to the in-memory tenant entry caches. Take care
	// to never hold this lock during K8s API calls - it should only be used
	// while adding and removing tenant entries to/from the caches.
	mut struct {
		sync.Mutex

		// tenants is a cache of tenant entries. Each entry tracks available IP
		// addresses for SQL pods assigned to a given tenant. Entries may not be
		// fully initialized.
		tenants map[TenantID]*tenantEntry
	}
}

// NewDirectory constructs a new Directory instance that tracks SQL tenant pods
// in the K8s cluster managed by the given kube.Ctl instance. The given context
// is used for tracing pod watcher activity.
//
// NOTE: Close must be called on the directory when it is no longer needed.
func NewDirectory(ctx context.Context, directoryHost string, opts ...DirOption) (*Directory, error) {
	dir := &Directory{}

	conn, err := grpc.Dial(directoryHost, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	dir.ctl = NewDirectoryClient(conn)

	dir.mut.tenants = make(map[TenantID]*tenantEntry)
	for _, opt := range opts {
		opt(&dir.options)
	}
	if dir.options.refreshDelay == 0 {
		// Default to a delay of 100ms between refresh attempts for a given tenant.
		dir.options.refreshDelay = 1000 * time.Millisecond
	}

	// Don't start pod watcher in test mode, because it's not deterministic.
	if !dir.options.deterministic {
		// Establish initial watch outside the watchPods goroutine so that any
		// establishment errors can be detected right away. The watcher will be
		// closed Directory.Close is called.
		go dir.watchPods(ctx)
	}

	return dir, nil
}

// EnsureTenantIP returns the IP address of one of the given tenant's SQL pods.
// If the tenant was just created or is suspended, such that there are no
// available pods, then EnsureTenantIP will trigger resumption of the tenant and
// block until at least one pod is available. If there are multiple SQL pods for
// the tenant, then LookupPodByID will choose one of them (note that currently
// there is always at most one SQL pod per tenant).
//
// If labelSelector is non-empty, then an error is returned if no pods match the
// selector. This can be used to ensure that the incoming SQL connection "knows"
// some additional information about the tenant, such as the name of the
// cluster, before being allowed to connect.
func (d *Directory) EnsureTenantIP(
	ctx context.Context, tenantID TenantID, labelSelector string,
) (string, error) {
	// Ensure that a directory entry has been created for this tenant.
	entry, err := d.getEntry(ctx, tenantID, true /* allowCreate */)
	if err != nil {
		return "", err
	}

	ip, err := entry.ChoosePodIP(ctx, d.ctl, d.options.deterministic)
	if err != nil && err.Error() == "not found" {
		// This can happen when the tenant has been deleted.
		d.deleteEntry(tenantID)
	}
	return ip, err
}

// LookupTenantIPs returns the IP addresses for all available SQL pods for the
// given tenant. It returns an error if the tenant has not yet been created. If
// no pods are available for the tenant, LookupTenantIPs will return the empty
// set (unlike EnsureTenantIP).
func (d *Directory) LookupTenantIPs(ctx context.Context, tenantID TenantID) ([]string, error) {
	// Ensure that a directory entry has been created for this tenant.
	entry, err := d.getEntry(ctx, tenantID, false /* allowCreate */)
	if err != nil {
		return nil, err
	}

	if entry == nil {
		return nil, fmt.Errorf("tenant %d not found", tenantID)
	}
	return entry.getPodIPs(), nil
}

// ReportFailure should be called when attempts to connect to a particular SQL
// tenant pod have failed. Since this could be due to a failed pod,
// ReportFailure will attempt to refresh the cache with the latest information
// about available tenant pods.
// TODO(andyk): In the future, the ip parameter will be used to mark a
// particular pod as "unhealthy" so that it's less likely to be chosen. However,
// today there can be at most one pod for a given tenant, so it must always be
// chosen. Keep the parameter as a placeholder for the future.
func (d *Directory) ReportFailure(ctx context.Context, tenantID TenantID, ip string) error {
	entry, err := d.getEntry(ctx, tenantID, false /* allowCreate */)
	if err != nil {
		return err
	} else if entry == nil {
		// If no tenant is in the cache, no-op.
		return nil
	}

	// Refresh the entry in case there is a new pod IP address.
	return entry.RefreshPods(ctx, d.ctl)
}

// getEntry returns a directory entry for the given tenant. If the directory
// does not contain such an entry, then getEntry will create one if allowCreate
// is true. Otherwise, it returns nil. If an entry is returned, then getEntry
// ensures that it is fully initialized with tenant metadata. Obtaining this
// metadata requires making a K8s call; getEntry will block until that's
// complete.
func (d *Directory) getEntry(
	ctx context.Context, tenantID TenantID, allowCreate bool,
) (*tenantEntry, error) {
	entry := func() *tenantEntry {
		// Acquire the directory lock just long enough to check the tenants map
		// for the given tenant ID. Don't complete initialization while holding
		// this lock, since that requires K8s API calls.
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
	err := entry.Initialize(ctx, d.ctl)
	if err != nil {
		// Remove the entry from the tenants map, since initialization failed.
		d.mut.Lock()
		defer d.mut.Unlock()

		// Threads can race to add/remove entries, so ensure that right entry is
		// removed.
		existing, ok := d.mut.tenants[tenantID]
		if ok && entry == existing {
			delete(d.mut.tenants, tenantID)
			log.Infof(ctx, "error initializing tenant %d: %v", tenantID, err)
		}

		return nil, err
	}

	return entry, nil
}

// deleteEntry removes the directory entry for the given tenant, if it exists.
func (d *Directory) deleteEntry(tenantID TenantID) {
	d.mut.Lock()
	defer d.mut.Unlock()
	delete(d.mut.tenants, tenantID)
}

// watchPods establishes a K8s watcher that looks for changes to pods that have
// the "tenant-id" label. This label is attached to a pod during the stamping
// process. Whenever pods are stamped, updated, or deleted, the watcher will get
// a notification and update the directory to reflect that change.
func (d *Directory) watchPods(ctx context.Context) {
	req := EventReq{}
	client, err := d.ctl.WaitEvent(ctx, &req)
	if err != nil {
		log.Errorf(ctx, "err constructing WaitEvent client: %s", err)
		return
	}
	for {
		// Read the next watcher event.
		resp, err := client.Recv()
		time.Sleep(time.Second)
		if err != nil {
			log.Errorf(ctx, "err receiving events: %s", err)
			continue
		}

		podIP := resp.Ip
		if podIP == "" {
			// Nothing needs to be done if there is no IP address specified.
			continue
		}

		// Ensure that a directory entry exists for this tenant. Note that this
		// may require making K8s call to initialize the new entry.
		entry, err := d.getEntry(ctx, TenantID(resp.TenantId), true /* allowCreate */)
		if err != nil {
			// This should never happen.
			log.Errorf(ctx, "ignoring error getting entry for tenant %d: %v", resp.TenantId, err)
			continue
		}

		// For now, all we care about is the IP addresses of the tenant pod.
		switch resp.Typ {
		case ADDED, MODIFIED:
			if entry.AddPodIP(podIP) {
				log.Infof(ctx, "added IP address %s for tenant %d", podIP, resp.TenantId)
			}

		case DELETED:
			if entry.RemovePodIP(podIP) {
				log.Infof(ctx, "deleted IP address %s for tenant %d", podIP, resp.TenantId)
			}
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
