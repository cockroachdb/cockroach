// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenant

import (
	"context"
	"fmt"
	"sync"
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

	// Full name of the tenant's cluster i.e. dim-dog.
	ClusterName string

	// RefreshDelay is the minimum amount of time that must elapse between
	// attempts to refresh pods for this tenant after ReportFailure is
	// called.
	RefreshDelay time.Duration

	// initialized is set to true once Initialized has been successfully called
	// (i.e. with no resulting error). Access is synchronized via atomics.
	initialized sync.Once

	// initError is set to any error that occurs in Initialized (or nil if no
	// error occurred).
	initError error

	// pods synchronizes access to information about the tenant's SQL pods.
	// These fields can be updated over time, so a lock must be obtained before
	// accessing them.
	pods struct {
		syncutil.Mutex
		pods []*Pod
	}

	// calls synchronizes calls to the Directory service for this tenant (e.g.
	// calls to GetTenant or ListPods). Synchronization is needed to ensure that
	// only one thread at a time is calling on behalf of a tenant, and that
	// calls are rate limited to prevent storms.
	calls struct {
		syncutil.Mutex

		// lastRefresh is the last time the list of pods for the tenant have been
		// fetched from the server. It's used to rate limit refreshes.
		lastRefresh time.Time
	}
}

// Initialize fetches metadata about a tenant, such as its cluster name, and
// stores that in the entry. After this is called once, all future calls return
// the same result (and do nothing).
func (e *tenantEntry) Initialize(ctx context.Context, client DirectoryClient) error {
	// If Initialize has already been successfully called, nothing to do.
	e.initialized.Do(func() {
		tenantResp, err := client.GetTenant(ctx, &GetTenantRequest{TenantID: e.TenantID.ToUint64()})
		if err != nil {
			e.initError = err
			return
		}

		e.ClusterName = tenantResp.ClusterName
	})

	// If Initialize has already been called, return any error that occurred.
	return e.initError
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
	if !e.canRefreshLocked() {
		return nil
	}

	log.Infof(ctx, "refreshing tenant %d pods", e.TenantID)

	_, err := e.fetchPodsLocked(ctx, client)
	return err
}

// AddPod inserts the given pod into the tenant's list of pods. If it is
// already present, then AddPod updates the pod entry and returns false.
func (e *tenantEntry) AddPod(pod *Pod) bool {
	e.pods.Lock()
	defer e.pods.Unlock()

	for i, existing := range e.pods.pods {
		if existing.Addr == pod.Addr {
			// e.pods.pods is copy on write. Whenever modifications are made,
			// we must make a copy to avoid accidentally mutating the slice
			// retrieved by GetPods.
			pods := e.pods.pods
			e.pods.pods = make([]*Pod, len(pods))
			copy(e.pods.pods, pods)
			e.pods.pods[i] = pod
			return false
		}
	}

	e.pods.pods = append(e.pods.pods, pod)
	return true
}

// RemovePodByAddr removes the pod with the given IP address from the tenant's
// list of pod addresses. If it was not present, RemovePodByAddr returns false.
func (e *tenantEntry) RemovePodByAddr(addr string) bool {
	e.pods.Lock()
	defer e.pods.Unlock()

	for i, existing := range e.pods.pods {
		if existing.Addr == addr {
			copy(e.pods.pods[i:], e.pods.pods[i+1:])
			e.pods.pods = e.pods.pods[:len(e.pods.pods)-1]
			return true
		}
	}
	return false
}

// GetPods gets the current list of pods within scope of lock and returns them.
func (e *tenantEntry) GetPods() []*Pod {
	e.pods.Lock()
	defer e.pods.Unlock()
	return e.pods.pods
}

// EnsureTenantPod ensures that at least one SQL process exists for this tenant,
// and is ready for connection attempts to its IP address. If errorIfNoPods is
// true, then EnsureTenantPod returns an error if there are no pods available
// rather than blocking.
func (e *tenantEntry) EnsureTenantPod(
	ctx context.Context, client DirectoryClient, errorIfNoPods bool,
) (pods []*Pod, err error) {
	const retryDelay = 100 * time.Millisecond

	e.calls.Lock()
	defer e.calls.Unlock()

	// If an IP address is already available, nothing more to do. Check this
	// immediately after obtaining the lock so that only the first thread does
	// the work to get information about the tenant.
	pods = e.GetPods()
	if len(pods) != 0 {
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
		if len(pods) != 0 {
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
	e.pods.Lock()
	defer e.pods.Unlock()
	e.pods.pods = list.Pods

	if len(e.pods.pods) != 0 {
		log.Infof(ctx, "fetched IP addresses: %v", e.pods.pods)
	}

	return e.pods.pods, nil
}

// canRefreshLocked returns true if it's been at least X milliseconds since the
// last time the tenant pod information was refreshed. This has the effect of
// rate limiting RefreshPods calls.
//
// NOTE: Caller must lock the "calls" mutex before calling canRefreshLocked.
func (e *tenantEntry) canRefreshLocked() bool {
	now := timeutil.Now()
	if now.Sub(e.calls.lastRefresh) < e.RefreshDelay {
		return false
	}
	e.calls.lastRefresh = now
	return true
}
