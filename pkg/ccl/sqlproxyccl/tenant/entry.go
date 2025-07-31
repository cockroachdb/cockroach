// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenant

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// tenantEntry is an entry in the tenant directory that records information
// about a single tenant, including its ID, cluster name, and the IP addresses
// for available pods.
type tenantEntry struct {
	// These fields can be read by callers without synchronization, since
	// they're written once during initialization, and are immutable thereafter.

	// TenantID is the identifier for this tenant which is unique within a CRDB
	// cluster.
	TenantID roachpb.TenantID

	// RefreshDelay is the minimum amount of time that must elapse between
	// attempts to refresh pods for this tenant after ReportFailure is
	// called.
	RefreshDelay time.Duration

	// These fields can be updated over time, so a lock must be obtained before
	// accessing them.
	mu struct {
		syncutil.Mutex

		// pods represents the tenant's SQL pods. This field is copy-on-write.
		// Callers should make a copy of the existing objects if they intend to
		// modify the object to avoid any race conditions.
		pods []*Pod

		// tenant represents the tenant's metadata. This field is copy-on-write.
		// Callers should make a copy of the existing object if they intend to
		// modify the object to avoid any race conditions.
		tenant *Tenant

		// valid is set to true if the tenant entry is up-to-date (i.e. once
		// Initialized or Update has been called).
		valid bool

		// initError is set to any error that occurs in Initialize (or nil if no
		// error occurred).
		initError error
	}

	// calls synchronizes calls to the Directory service for this tenant (e.g.
	// calls to GetTenant or ListPods). Synchronization is needed to ensure that
	// only one thread at a time is calling on behalf of a tenant, and that
	// calls are rate limited to prevent storms.
	calls struct {
		syncutil.Mutex

		// lastPodRefresh is the last time the list of pods for the tenant have
		// been fetched from the server. It's used to rate limit pod refreshes.
		lastPodRefresh time.Time
	}
}

// Initialize fetches metadata about a tenant, such as its cluster name, and
// stores that in the entry. If the tenant's metadata is stale, they will be
// refreshed.
func (e *tenantEntry) Initialize(ctx context.Context, client DirectoryClient) error {
	e.calls.Lock()
	defer e.calls.Unlock()

	// Check if tenant entry is valid.
	initDone, initErr := func() (bool, error) {
		e.mu.Lock()
		defer e.mu.Unlock()
		if e.mu.valid {
			return true, e.mu.initError
		}
		return false, nil
	}()
	if initDone {
		return initErr
	}

	log.Infof(ctx, "refreshing tenant %d metadata", e.TenantID)

	tenantResp, err := client.GetTenant(ctx, &GetTenantRequest{TenantID: e.TenantID.ToUint64()})
	if err != nil {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.mu.valid = true
		e.mu.initError = err
		return err
	}

	// TODO(jaylim-crl): Once tenant directories have been updated to return
	// the Tenant field, we should remove this case, and return an error if the
	// Tenant field is nil.
	if tenantResp.Tenant == nil {
		tenantResp.Tenant = &Tenant{ClusterName: tenantResp.ClusterName}
	}
	e.UpdateTenant(tenantResp.Tenant)
	return nil
}

// RefreshPods makes a synchronous directory server call to fetch the latest
// information about the tenant's available pods, such as their IP addresses.
func (e *tenantEntry) RefreshPods(ctx context.Context, client DirectoryClient) error {
	// Lock so that only one thread at a time will refresh, since there's no
	// point in multiple threads doing it within a short span of time - it's
	// likely nothing has changed.
	e.calls.Lock()
	defer e.calls.Unlock()

	// If refreshed recently, no-op.
	if !e.canRefreshPodsLocked() {
		return nil
	}

	log.Infof(ctx, "refreshing tenant %d pods", e.TenantID)

	_, err := e.fetchPodsLocked(ctx, client)
	return err
}

// AddPod inserts the given pod into the tenant's list of pods. If it is
// already present, then AddPod updates the pod entry and returns false.
func (e *tenantEntry) AddPod(pod *Pod) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, existing := range e.mu.pods {
		if existing.Addr == pod.Addr {
			// e.pods.pods is copy on write. Whenever modifications are made,
			// we must make a copy to avoid accidentally mutating the slice
			// retrieved by GetPods.
			pods := e.mu.pods
			e.mu.pods = make([]*Pod, len(pods))
			copy(e.mu.pods, pods)
			e.mu.pods[i] = pod
			return false
		}
	}

	e.mu.pods = append(e.mu.pods, pod)
	return true
}

// RemovePodByAddr removes the pod with the given IP address from the tenant's
// list of pod addresses. If it was not present, RemovePodByAddr returns false.
func (e *tenantEntry) RemovePodByAddr(addr string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, existing := range e.mu.pods {
		if existing.Addr == addr {
			copy(e.mu.pods[i:], e.mu.pods[i+1:])
			e.mu.pods = e.mu.pods[:len(e.mu.pods)-1]
			return true
		}
	}
	return false
}

// GetPods gets the current list of pods within scope of lock and returns them.
func (e *tenantEntry) GetPods() []*Pod {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.pods
}

// EnsureTenantPod ensures that at least one RUNNING SQL process exists for this
// tenant, and is ready for connection attempts to its IP address. If
// errorIfNoPods is true, then EnsureTenantPod returns an error if there are no
// pods available rather than blocking.
func (e *tenantEntry) EnsureTenantPod(
	ctx context.Context, client DirectoryClient, errorIfNoPods bool,
) (pods []*Pod, err error) {
	const retryDelay = 100 * time.Millisecond

	e.calls.Lock()
	defer e.calls.Unlock()

	// If an IP address for a RUNNING pod is already available, nothing more to
	// do. Check this immediately after obtaining the lock so that only the
	// first thread does the work to get information about the tenant.
	pods = e.GetPods()
	if hasRunningPod(pods) {
		return pods, nil
	}

	for {
		// Check for context cancellation or timeout.
		if err = ctx.Err(); err != nil {
			return nil, err
		}

		// Try to resume the tenant if not yet resumed.
		_, err = client.EnsurePod(ctx, &EnsurePodRequest{e.TenantID.ToUint64()})
		if err != nil {
			return nil, err
		}

		// Get pod information for the newly resumed tenant. Except in rare
		// race conditions, this is expected to immediately find an IP address,
		// since the above call started a tenant process that already has an IP
		// address.
		pods, err = e.fetchPodsLocked(ctx, client)
		if err != nil {
			return nil, err
		}
		if hasRunningPod(pods) {
			log.Infof(ctx, "resumed tenant %d", e.TenantID)
			break
		}

		// In rare case where no IP address is ready, wait for a bit before
		// retrying.
		if errorIfNoPods {
			return nil, fmt.Errorf("no pods available for tenant %s", e.TenantID)
		}
		sleepContext(ctx, retryDelay)
	}

	return pods, nil
}

// UpdateMetadata updates the current entry with the given state.
func (e *tenantEntry) UpdateTenant(tenant *Tenant) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// It is possible that GetTenant returns an error (which leaves e.mu.tenant
	// as nil). When that happens, the tenant watcher may race to update a
	// tenant, so we'd have to perform a nil check here. Consider the following
	// case:
	//   1. GetTenant returns NotFound, so e.mu.valid=true and e.mu.tenant=nil.
	//   2. Tenant gets created, and WatchTenants receive an event.
	//   3. UpdateTenant gets called with the new tenant, so we would want to
	//      update the tenant.
	if e.mu.valid {
		if e.mu.tenant != nil && tenant.Version < e.mu.tenant.Version {
			return
		}
	}
	e.mu.tenant = tenant
	e.mu.valid = true
	e.mu.initError = nil
}

// IsValid returns true if the tenant's metadata is valid.
func (e *tenantEntry) IsValid() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.valid
}

// MarkInvalid invalidates the tenant's metadata. This forces Initialize to
// perform a GetTenant if it's called.
func (e *tenantEntry) MarkInvalid() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.valid = false
}

// ToProto returns a tenant.Tenant object representing the tenant entry.
func (e *tenantEntry) ToProto() *Tenant {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.tenant
}

// fetchPodsLocked makes a synchronous directory server call to get the latest
// information about the tenant's available pods, such as their IP addresses.
//
// NOTE: Caller must lock the "calls" mutex before calling fetchPodsLocked.
func (e *tenantEntry) fetchPodsLocked(
	ctx context.Context, client DirectoryClient,
) (tenantPods []*Pod, err error) {
	// List the pods for the given tenant.
	//
	// TODO(andyk): This races with the pod watcher, which may receive updates
	// that are newer than what ListPods returns. This could be fixed by adding
	// version values to the pods in order to detect races.
	list, err := client.ListPods(ctx, &ListPodsRequest{e.TenantID.ToUint64()})
	if err != nil {
		return nil, err
	}

	// Need to lock in case another thread is reading the IP addresses (e.g. in
	// ChoosePodAddr).
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.pods = list.Pods

	if len(e.mu.pods) != 0 {
		log.Infof(ctx, "fetched IP addresses: %v", e.mu.pods)
	}

	return e.mu.pods, nil
}

// canRefreshPodsLocked returns true if it's been at least X milliseconds since
// the last time the tenant pod information was refreshed. This has the effect
// of rate limiting RefreshPods calls.
//
// NOTE: Caller must lock the "calls" mutex before calling canRefreshPodsLocked.
func (e *tenantEntry) canRefreshPodsLocked() bool {
	now := timeutil.Now()
	if now.Sub(e.calls.lastPodRefresh) < e.RefreshDelay {
		return false
	}
	e.calls.lastPodRefresh = now
	return true
}

// hasRunningPod returns true if there is at least one RUNNING pod, or false
// otherwise.
func hasRunningPod(pods []*Pod) bool {
	for _, pod := range pods {
		if pod.State == RUNNING {
			return true
		}
	}
	return false
}
