// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TenantCache represents the tenant cache that contains cached data about the
// tenant's connections (e.g. number of active or idle connections).
type TenantCache interface {
	// ActiveCountByAddr returns the number of active connections based on the
	// given pod's address for the tenant.
	ActiveCountByAddr(podAddr string) int

	// IdleCountByAddr returns the number of idle connections based on the
	// given pod's address for the tenant.
	IdleCountByAddr(podAddr string) int
}

// tenantCache represents an implementation of the TenantCache interface.
type tenantCache struct {
	mu   syncutil.Mutex
	data *tenantCacheData
}

var _ TenantCache = &tenantCache{}

// newTenantCache returns a new instance of tenantCache.
func newTenantCache() *tenantCache {
	return &tenantCache{data: newTenantCacheData()}
}

// ActiveCountByAddr returns the number of active connections based on the given
// pod's address for the tenant. If the pod's address is invalid, 0 will be
// returned.
//
// ActiveCountByAddr implements the TenantCache interface.
func (c *tenantCache) ActiveCountByAddr(podAddr string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data.activeConnsCount[podAddr]
}

// IdleCountByAddr returns the number of idle connections based on the given
// pod's address for the tenant. If the pod's address is invalid, 0 will be
// returned.
//
// IdleCountByAddr implements the TenantCache interface.
func (c *tenantCache) IdleCountByAddr(podAddr string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data.idleConnsCount[podAddr]
}

// updateActiveCount updates the activeConnsCount map by incrementing the pod's
// number of connections by val. val may be negative, and updateActiveCount
// ensures that the number of connections will never be negative.
func (c *tenantCache) updateActiveCount(podAddr string, val int) {
	if val == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data.activeConnsCount[podAddr] = int(math.Max(float64(c.data.activeConnsCount[podAddr]+val), 0))
}

// updateIdleCount updates the idleConnsCount map by incrementing the pod's
// number of connections by val. val may be negative, and updateActiveCount
// ensures that the number of connections will never be negative.
func (c *tenantCache) updateIdleCount(podAddr string, val int) {
	if val == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data.idleConnsCount[podAddr] = int(math.Max(float64(c.data.idleConnsCount[podAddr]+val), 0))
}

// refreshData replaces the cache data with the input data.
func (c *tenantCache) refreshData(data *tenantCacheData) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = data
}

// tenantCacheData represents the data stored in the tenant cache. When working
// with fields, it is assumed that a lock has been obtained.
type tenantCacheData struct {
	// activeConnsCount is the mapping of pod's address to the number of active
	// connections in that pod.
	activeConnsCount map[string]int

	// idleConnsCount is the mapping of pod's address to the number of idle
	// connections in that pod.
	idleConnsCount map[string]int
}

// newTenantCacheData creates an empty instance of the tenantCacheData.
func newTenantCacheData() *tenantCacheData {
	return &tenantCacheData{
		activeConnsCount: make(map[string]int),
		idleConnsCount:   make(map[string]int),
	}
}
