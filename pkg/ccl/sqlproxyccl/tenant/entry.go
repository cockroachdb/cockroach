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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// tenantEntry is an entry in the tenant directory that records information
// about a single tenant, including its ID, cluster name, and the IP addresses for
// available endpoints.
type tenantEntry struct {
	// These fields can be read by callers without synchronization, since
	// they're written once during initialization, and are immutable thereafter.

	// TenantID is the identifier for this tenant which is unique within a CRDB
	// cluster.
	TenantID roachpb.TenantID

	// Full name of the tenant's cluster i.e. dim-dog.
	ClusterName string

	// RefreshDelay is the minimum amount of time that must elapse between
	// attempts to refresh endpoints for this tenant after ReportFailure is called.
	RefreshDelay time.Duration

	// initialized is set to true once Initialized has been called.
	initialized bool

	// initError is set to any error that occurs in Initialized (or nil if no
	// error occurred).
	initError error

	// endpoints synchronizes access to information about the tenant's SQL endpoints.
	// These fields can be updated over time, so a lock must be obtained before
	// accessing them.
	endpoints struct {
		syncutil.Mutex
		ips []string
	}

	// calls synchronizes calls to the K8s API for this tenant (e.g. calls to
	// RefreshEndpoints). Synchronization is needed to ensure that only one thread at
	// a time is calling on behalf of a tenant, and that calls are rate limited
	// to prevent storms.
	calls struct {
		syncutil.Mutex
		lastRefresh time.Time
	}
}

// Initialize fetches metadata about a tenant, such as its cluster name, and stores
// that in the entry. After this is called once, all future calls return the
// same result (and do nothing).
func (e *tenantEntry) Initialize(ctx context.Context, client DirectoryClient) error {
	// Synchronize multiple threads trying to initialize. Only the first thread
	// does the initialization.
	e.calls.Lock()
	defer e.calls.Unlock()

	// If Initialize has already been called, return any error that occurred.
	if e.initialized {
		return e.initError
	}

	tenantResp, err := client.GetTenant(ctx, &GetTenantRequest{TenantID: e.TenantID.ToUint64()})
	if err != nil {
		e.initialized = true
		e.initError = err
		return err
	}

	e.ClusterName = tenantResp.ClusterName

	e.initialized = true
	return nil
}

// RefreshEndpoints makes a synchronous directory server call to fetch the latest information
// about the tenant's available endpoints, such as their IP addresses.
func (e *tenantEntry) RefreshEndpoints(ctx context.Context, client DirectoryClient) error {
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

	log.Infof(ctx, "refreshing tenant %d endpoints", e.TenantID)

	_, err := e.fetchEndpointsLocked(ctx, client)
	return err
}

// ChooseEndpointIP returns the IP address of one of this tenant's available endpoints.
// If a tenant has multiple endpoints, then ChooseEndpointIP returns the IP address of one
// of those endpoints. If the tenant is suspended and no endpoints are available, then
// ChooseEndpointIP will trigger resumption of the tenant and return the IP address
// of the new endpoint. Note that resuming a tenant requires directory server calls, so
// ChooseEndpointIP can block for some time, until the resumption process is
// complete. However, if errorIfNoEndpoints is true, then ChooseEndpointIP returns an
// error if there are no endpoints available rather than blocking.
//
// TODO(andyk): Use better load-balancing algorithm once tenants can have more
// than one endpoint.
func (e *tenantEntry) ChooseEndpointIP(
	ctx context.Context, client DirectoryClient, errorIfNoEndpoints bool,
) (string, error) {
	if !e.initialized {
		return "", errors.AssertionFailedf("entry for tenant %d is not initialized", e.TenantID)
	}

	ips := e.getEndpointIPs()
	if len(ips) == 0 {
		// There are no known endpoint IP addresses, so fetch endpoint information from
		// the directory server. Resume the tenant if it is suspended; that will
		// always result in at least one endpoint IP address (or an error).
		var err error
		if ips, err = e.ensureTenantEndpoint(ctx, client, errorIfNoEndpoints); err != nil {
			return "", err
		}
	}
	return ips[0], nil
}

// AddEndpointIP inserts the given IP address into the tenant's list of Endpoint IPs. If
// it is already present, then AddEndpointIP returns false.
func (e *tenantEntry) AddEndpointIP(ip string) bool {
	e.endpoints.Lock()
	defer e.endpoints.Unlock()

	for _, existing := range e.endpoints.ips {
		if existing == ip {
			return false
		}
	}

	e.endpoints.ips = append(e.endpoints.ips, ip)
	return true
}

// RemoveEndpointIP removes the given IP address from the tenant's list of Endpoint IPs.
// If it was not present, RemoveEndpointIP returns false.
func (e *tenantEntry) RemoveEndpointIP(ip string) bool {
	e.endpoints.Lock()
	defer e.endpoints.Unlock()

	for i, existing := range e.endpoints.ips {
		if existing == ip {
			copy(e.endpoints.ips[i:], e.endpoints.ips[i+1:])
			e.endpoints.ips = e.endpoints.ips[:len(e.endpoints.ips)-1]
			return true
		}
	}
	return false
}

// getEndpointIPs gets the current list of endpoint IP addresses within scope of lock and
// returns them.
func (e *tenantEntry) getEndpointIPs() []string {
	e.endpoints.Lock()
	defer e.endpoints.Unlock()
	return e.endpoints.ips
}

// ensureTenantEndpoint ensures that at least one SQL process exists for this
// tenant, and is ready for connection attempts to its IP address. If
// errorIfNoEndpoints is true, then ensureTenantEndpoint returns an error if there are no
// endpoints available rather than blocking.
func (e *tenantEntry) ensureTenantEndpoint(
	ctx context.Context, client DirectoryClient, errorIfNoEndpoints bool,
) (ips []string, err error) {
	const retryDelay = 100 * time.Millisecond

	e.calls.Lock()
	defer e.calls.Unlock()

	// If an IP address is already available, nothing more to do. Check this
	// immediately after obtaining the lock so that only the first thread does
	// the work to get information about the tenant.
	ips = e.getEndpointIPs()
	if len(ips) != 0 {
		return ips, nil
	}

	// Get up-to-date count of endpoints for the tenant from the K8s server.
	resp, err := client.ListEndpoints(ctx, &ListEndpointsRequest{TenantID: e.TenantID.ToUint64()})
	if err != nil {
		return nil, err
	}

	for {
		// Check for context cancellation or timeout.
		if err = ctx.Err(); err != nil {
			return nil, err
		}

		// Check if tenant needs to be resumed.
		if len(resp.Endpoints) == 0 {
			log.Infof(ctx, "resuming tenant %d", e.TenantID)

			if _, err := client.EnsureEndpoint(
				ctx, &EnsureEndpointRequest{e.TenantID.ToUint64()},
			); err != nil {
				return nil, err
			}
		}

		// Get endpoint information for the newly resumed tenant. Except in rare race
		// conditions, this is expected to immediately find an IP address, since
		// the above call started a tenant process that already has an IP address.
		ips, err = e.fetchEndpointsLocked(ctx, client)
		if err != nil {
			return nil, err
		}
		if len(ips) != 0 {
			break
		}

		// In rare case where no IP address is ready, wait for a bit before
		// retrying.
		if errorIfNoEndpoints {
			return nil, fmt.Errorf("no endpoints available for tenant %s", e.TenantID)
		}
		sleepContext(ctx, retryDelay)
	}

	return ips, nil
}

// fetchEndpointsLocked makes a synchronous directory server call to get the latest
// information about the tenant's available endpoints, such as their IP addresses.
//
// NOTE: Caller must lock the "calls" mutex before calling fetchEndpointsLocked.
func (e *tenantEntry) fetchEndpointsLocked(
	ctx context.Context, client DirectoryClient,
) (ips []string, err error) {
	// List the endpoints for the given tenant.
	list, err := client.ListEndpoints(ctx, &ListEndpointsRequest{e.TenantID.ToUint64()})
	if err != nil {
		return nil, err
	}

	// Get updated list of running process endpoint IP addresses and save it to the entry.
	ips = make([]string, 0, len(list.Endpoints))
	for i := range list.Endpoints {
		endpoint := list.Endpoints[i]
		ips = append(ips, endpoint.IP)
	}

	// Need to lock in case another thread is reading the IP addresses (e.g. in
	// ChooseEndpointIP).
	e.endpoints.Lock()
	defer e.endpoints.Unlock()
	e.endpoints.ips = ips

	if len(ips) != 0 {
		log.Infof(ctx, "fetched IP addresses for tenant %d: %v", e.TenantID, ips)
	}

	return ips, nil
}

// canRefreshLocked returns true if it's been at least X milliseconds since the
// last time the tenant endpoint information was refreshed. This has the effect of
// rate limiting RefreshEndpoints calls.
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
