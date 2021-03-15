package directory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"

	"github.com/cockroachdb/errors"
)

// tenantEntry is an entry in the tenant directory that records information
// about a single tenant, including its ID, labels, and the IP addresses for
// available pods.
type tenantEntry struct {
	// These fields can be read by callers without synchronization, since
	// they're written once during initialization, and are immutable thereafter.

	// TenantID is the identifier for this tenant which is unique within a CRDB
	// cluster.
	TenantID TenantID

	// Full name of the tenant i.e. dim-dog-28
	TenantName string

	// RefreshDelay is the minimum amount of time that must elapse between
	// attempts to refresh pods for this tenant after ReportFailure is called.
	RefreshDelay time.Duration

	// initialized is set to true once Initialized has been called.
	initialized bool

	// initError is set to any error that occurs in Initialized (or nil if no
	// error occurred).
	initError error

	// pods synchronizes access to information about the tenant's SQL pods.
	// These fields can be updated over time, so a lock must be obtained before
	// accessing them.
	pods struct {
		sync.Mutex
		ips []string
	}

	// calls synchronizes calls to the K8s API for this tenant (e.g. calls to
	// RefreshPods). Synchronization is needed to ensure that only one thread at
	// a time is calling on behalf of a tenant, and that calls are rate limited
	// to prevent storms.
	calls struct {
		sync.Mutex
		lastRefresh time.Time
	}
}

// Initialize fetches metadata about a tenant, such as its labels, and stores
// that in the entry. After this is called once, all future calls return the
// same result (and do nothing).
func (e *tenantEntry) Initialize(ctx context.Context, ctl DirectoryClient) error {
	// Synchronize multiple threads trying to initialize. Only the first thread
	// does the initialization.
	e.calls.Lock()
	defer e.calls.Unlock()

	// If Initialize has already been called, return any error that occurred.
	if e.initialized {
		return e.initError
	}

	e.initialized = true
	return nil
}

// RefreshPods makes a synchronous K8s API call to fetch the latest information
// about the tenant's available pods, such as their IP addresses.
func (e *tenantEntry) RefreshPods(ctx context.Context, ctl DirectoryClient) error {
	if !e.initialized {
		return errors.AssertionFailedf("entry for tenant %d is not initialized", e.TenantID)
	}

	// Lock so that only one thread at a time will refresh, since there's no
	// point in multiple threads doing it within a short span of time - it's
	// likely nothing has changed.
	e.calls.Lock()
	defer e.calls.Unlock()

	// If refreshed recently, no-op.
	if !e.canRefreshLocked() {
		return nil
	}

	log.Infof(ctx, "refreshing tenant %d pods", e.TenantID)

	_, err := e.fetchPodsLocked(ctx, ctl)
	return err
}

// ChoosePodIP returns the IP address of one of this tenant's available pods.
// If a tenant has multiple pods, then ChoosePodIP returns the IP address of one
// of those pods. If the tenant is suspended and no pods are available, then
// ChoosePodIP will trigger resumption of the tenant and return the IP address
// of the new pod. Note that resuming a tenant requires K8s API calls, so
// ChoosePodIP can block for some time, until the resumption process is
// complete. However, if errorIfNoPods is true, then ChoosePodIP returns an
// error if there are no pods available rather than blocking.
//
// TODO(andyk): Use better load-balancing algorithm once tenants can have more
// than one pod.
func (e *tenantEntry) ChoosePodIP(
	ctx context.Context, ctl DirectoryClient, errorIfNoPods bool,
) (string, error) {
	if !e.initialized {
		return "", errors.AssertionFailedf("entry for tenant %d is not initialized", e.TenantID)
	}

	ips := e.getPodIPs()
	if len(ips) == 0 {
		// There are no known pod IP addresses, so fetch pod information from
		// the K8s server. Resume the tenant if it is suspended; that will
		// always result in at least one pod IP address (or an error).
		var err error
		if ips, err = e.ensureTenantPod(ctx, ctl, errorIfNoPods); err != nil {
			return "", err
		}
	}
	return ips[0], nil
}

// AddPodIP inserts the given IP address into the tenant's list of Pod IPs. If
// it is already present, then AddPodIP returns false.
func (e *tenantEntry) AddPodIP(ip string) bool {
	e.pods.Lock()
	defer e.pods.Unlock()

	for _, existing := range e.pods.ips {
		if existing == ip {
			return false
		}
	}

	e.pods.ips = append(e.pods.ips, ip)
	return true
}

// RemovePodIP removes the given IP address from the tenant's list of Pod IPs.
// If it was not present, RemovePodIP returns false.
func (e *tenantEntry) RemovePodIP(ip string) bool {
	e.pods.Lock()
	defer e.pods.Unlock()

	for i, existing := range e.pods.ips {
		if existing == ip {
			copy(e.pods.ips[i:], e.pods.ips[i+1:])
			e.pods.ips = e.pods.ips[:len(e.pods.ips)-1]
			return true
		}
	}
	return false
}

// getPodIPs gets the current list of pod IP addresses within scope of lock and
// returns them.
func (e *tenantEntry) getPodIPs() []string {
	e.pods.Lock()
	defer e.pods.Unlock()
	return e.pods.ips
}

// ensureTenantPod ensures that at least one SQL pod has been stamped for this
// tenant, and is ready for connection attempts to its IP address. If
// errorIfNoPods is true, then ensureTenantPod returns an error if there are no
// pods available rather than blocking.
func (e *tenantEntry) ensureTenantPod(
	ctx context.Context, ctl DirectoryClient, errorIfNoPods bool,
) (ips []string, err error) {
	const retryDelay = 100 * time.Millisecond

	e.calls.Lock()
	defer e.calls.Unlock()

	// If an IP address is already available, nothing more to do. Check this
	// immediately after obtaining the lock so that only the first thread does
	// the work to get information about the tenant.
	ips = e.getPodIPs()
	if len(ips) != 0 {
		return ips, nil
	}

	// Get up-to-date count of pods for the tenant from the K8s server.
	resp, err := ctl.List(ctx, &TenantReq{TenantId: uint64(e.TenantID)})
	if err != nil {
		return nil, err
	}

	for {
		// Check for context cancellation or timeout.
		if err = ctx.Err(); err != nil {
			return nil, err
		}

		// Check if tenant needs to be resumed.
		if len(resp.Pods) == 0 {
			log.Infof(ctx, "resuming tenant %d", e.TenantID)

			// Setting Pods = 1 will trigger resumption via the CrdbTenant
			// webhook.
			if _, err := ctl.SetPodsCount(ctx, &SetPodsCountReq{uint64(e.TenantID), 1}); err != nil {
				// If the Update call failed with a ResourceVersion conflict, then
				// loop around and try again with a new ResourceVersion.
				if err.Error() == "conflict" {
					return nil, err
				}
				continue
			}
		}

		// Get pod information for the newly resumed tenant. Except in rare race
		// conditions, this is expected to immediately find an IP address, since
		// the above Update synchronously stamped a pre-warmed pod that already
		// has an IP address.
		ips, err = e.fetchPodsLocked(ctx, ctl)
		if err != nil {
			return nil, err
		}
		if len(ips) != 0 {
			break
		}

		// In rare case where no IP address is ready, wait for a bit before
		// retrying.
		if errorIfNoPods {
			return nil, fmt.Errorf("no pods available for tenant %s", e.TenantID)
		}
		sleepContext(ctx, retryDelay)
	}

	return ips, nil
}

// fetchPodsLocked makes a synchronous K8s API call to get the latest
// information about the tenant's available pods, such as their IP addresses.
//
// NOTE: Caller must lock the "calls" mutex before calling fetchPodsLocked.
func (e *tenantEntry) fetchPodsLocked(
	ctx context.Context, ctl DirectoryClient,
) (ips []string, err error) {
	// List pods with a "tenant-id" label that matches this tenant's ID.
	list, err := ctl.List(ctx, &TenantReq{TenantId: uint64(e.TenantID)})
	if err != nil {
		return nil, err
	}

	// Get updated list of running pod IP addresses and save it to the entry.
	ips = make([]string, 0, len(list.Pods))
	for i := range list.Pods {
		pod := list.Pods[i]
		if pod.Phase == RUNNING {
			ips = append(ips, pod.Ip)
		}
	}

	// Need to lock in case another thread is reading the IP addresses (e.g. in
	// ChoosePodIP).
	e.pods.Lock()
	defer e.pods.Unlock()
	e.pods.ips = ips

	if len(ips) != 0 {
		log.Infof(ctx, "fetched IP addresses for tenant %d: %v", e.TenantID, ips)
	}

	return ips, nil
}

// canRefreshLocked returns true if it's been at least X milliseconds since the
// last time the tenant pod information was refreshed. This has the effect of
// rate limiting RefreshPods calls.
//
// NOTE: Caller must lock the "calls" mutex before calling canRefreshLocked.
func (e *tenantEntry) canRefreshLocked() bool {
	now := time.Now()
	if now.Sub(e.calls.lastRefresh) < e.RefreshDelay {
		return false
	}
	e.calls.lastRefresh = now
	return true
}
