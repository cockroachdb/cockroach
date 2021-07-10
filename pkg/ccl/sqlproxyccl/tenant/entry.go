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

		// addrs is the set of IP:port addresses of the tenant's currently
		// RUNNING pods.
		addrs []string
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

// ChoosePodAddr returns the IP address of one of this tenant's available pods.
// If a tenant has multiple pods, then ChoosePodAddr returns the IP address of
// one of those pods. If the tenant is suspended and no pods are available, then
// ChoosePodAddr will trigger resumption of the tenant and return the IP address
// of the new pod. Note that resuming a tenant requires directory server calls,
// so ChoosePodAddr can block for some time, until the resumption process is
// complete. However, if errorIfNoPods is true, then ChoosePodAddr returns an
// error if there are no pods available rather than blocking.
//
// TODO(andyk): Use better load-balancing algorithm once tenants can have more
// than one pod.
func (e *tenantEntry) ChoosePodAddr(
	ctx context.Context, client DirectoryClient, errorIfNoPods bool,
) (string, error) {
	addrs := e.getPodAddrs()
	if len(addrs) == 0 {
		// There are no known pod IP addresses, so fetch pod information
		// from the directory server. Resume the tenant if it is suspended; that
		// will always result in at least one pod IP address (or an error).
		var err error
		if addrs, err = e.ensureTenantPod(ctx, client, errorIfNoPods); err != nil {
			return "", err
		}
	}
	return addrs[0], nil
}

// AddPodAddr inserts the given IP address into the tenant's list of pod IPs. If
// it is already present, then AddPodAddr returns false.
func (e *tenantEntry) AddPodAddr(addr string) bool {
	e.pods.Lock()
	defer e.pods.Unlock()

	for _, existing := range e.pods.addrs {
		if existing == addr {
			return false
		}
	}

	e.pods.addrs = append(e.pods.addrs, addr)
	return true
}

// RemovePodAddr removes the given IP address from the tenant's list of pod
// addresses. If it was not present, RemovePodAddr returns false.
func (e *tenantEntry) RemovePodAddr(addr string) bool {
	e.pods.Lock()
	defer e.pods.Unlock()

	for i, existing := range e.pods.addrs {
		if existing == addr {
			copy(e.pods.addrs[i:], e.pods.addrs[i+1:])
			e.pods.addrs = e.pods.addrs[:len(e.pods.addrs)-1]
			return true
		}
	}
	return false
}

// getPodAddrs gets the current list of pod IP addresses within scope of lock
// and returns them.
func (e *tenantEntry) getPodAddrs() []string {
	e.pods.Lock()
	defer e.pods.Unlock()
	return e.pods.addrs
}

// ensureTenantPod ensures that at least one SQL process exists for this tenant,
// and is ready for connection attempts to its IP address. If errorIfNoPods is
// true, then ensureTenantPod returns an error if there are no pods available
// rather than blocking.
func (e *tenantEntry) ensureTenantPod(
	ctx context.Context, client DirectoryClient, errorIfNoPods bool,
) (addrs []string, err error) {
	const retryDelay = 100 * time.Millisecond

	e.calls.Lock()
	defer e.calls.Unlock()

	// If an IP address is already available, nothing more to do. Check this
	// immediately after obtaining the lock so that only the first thread does
	// the work to get information about the tenant.
	addrs = e.getPodAddrs()
	if len(addrs) != 0 {
		return addrs, nil
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
		addrs, err = e.fetchPodsLocked(ctx, client)
		if err != nil {
			return nil, err
		}
		if len(addrs) != 0 {
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

	return addrs, nil
}

// fetchPodsLocked makes a synchronous directory server call to get the latest
// information about the tenant's available pods, such as their IP addresses.
//
// NOTE: Caller must lock the "calls" mutex before calling fetchPodsLocked.
func (e *tenantEntry) fetchPodsLocked(
	ctx context.Context, client DirectoryClient,
) (addrs []string, err error) {
	// List the pods for the given tenant.
	// TODO(andyk): This races with the pod watcher, which may receive updates
	// that are newer than what ListPods returns. This could be fixed by adding
	// version values to the pods in order to detect races.
	list, err := client.ListPods(ctx, &ListPodsRequest{e.TenantID.ToUint64()})
	if err != nil {
		return nil, err
	}

	// Get updated list of RUNNING pod IP addresses and save it to the entry.
	addrs = make([]string, 0, len(list.Pods))
	for i := range list.Pods {
		pod := list.Pods[i]
		if pod.State == RUNNING {
			addrs = append(addrs, pod.Addr)
		}
	}

	// Need to lock in case another thread is reading the IP addresses (e.g. in
	// ChoosePodAddr).
	e.pods.Lock()
	defer e.pods.Unlock()
	e.pods.addrs = addrs

	if len(addrs) != 0 {
		log.Infof(ctx, "fetched IP addresses: %v", addrs)
	}

	return addrs, nil
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
