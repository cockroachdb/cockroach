// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantsettingswatcher

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// overridesStore is the data structure that maintains all the tenant overrides
// in memory.
//
// It is designed for efficient read access and to allow multiple goroutines to
// listen for changes. We expect changes to setting overrides to be rare (it is
// a manual operation), so the efficiency of updates is not very important.
//
// This data structure treats the special tenant 0 (which stores the all-tenant
// overrides) as any other tenant.
type overridesStore struct {
	mu struct {
		syncutil.RWMutex

		// The tenants map stores the current overrides for each tenant that either
		// has had an override set in the tenant_settings table, or whose overrides
		// were requested at any point.
		// As such, this map might contain objects for tenants that no longer exist.
		// These objects would be very small (they would contain no overrides). If
		// this ever becomes a problem, we can periodically purge entries with no
		// overrides.
		tenants map[roachpb.TenantID]*tenantOverrides
	}
}

// tenantOverrides stores the current overrides for a tenant (or the current
// all-tenant overrides). It is an immutable data structure.
type tenantOverrides struct {
	// overrides, ordered by Name.
	overrides []roachpb.TenantSetting

	// changeCh is a channel that is closed when the tenant overrides change (in
	// which case a new tenantOverrides object will contain the updated settings).
	changeCh chan struct{}
}

func newTenantOverrides(overrides []roachpb.TenantSetting) *tenantOverrides {
	return &tenantOverrides{
		overrides: overrides,
		changeCh:  make(chan struct{}),
	}
}

func (s *overridesStore) Init() {
	s.mu.tenants = make(map[roachpb.TenantID]*tenantOverrides)
}

// SetAll initializes the overrides for all tenants. Any existing overrides are
// replaced.
//
// The store takes ownership of the overrides slices in the map (the caller can
// no longer modify them).
//
// This method is called once we complete a full initial scan of the
// tenant_setting table.
func (s *overridesStore) SetAll(allOverrides map[roachpb.TenantID][]roachpb.TenantSetting) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Notify any listeners that the overrides are changing (potentially).
	for _, existing := range s.mu.tenants {
		close(existing.changeCh)
	}
	s.mu.tenants = make(map[roachpb.TenantID]*tenantOverrides, len(allOverrides))

	for tenantID, overrides := range allOverrides {
		// Make sure settings are sorted.
		sort.Slice(overrides, func(i, j int) bool {
			return overrides[i].Name < overrides[j].Name
		})
		// Sanity check.
		for i := 1; i < len(overrides); i++ {
			if overrides[i].Name == overrides[i-1].Name {
				panic("duplicate setting")
			}
		}
		s.mu.tenants[tenantID] = newTenantOverrides(overrides)
	}
}

// GetTenantOverrides retrieves the overrides for a given tenant.
//
// The caller can listen for closing of changeCh, which is guaranteed to happen
// if the tenant's overrides change.
func (s *overridesStore) GetTenantOverrides(tenantID roachpb.TenantID) *tenantOverrides {
	s.mu.RLock()
	res, ok := s.mu.tenants[tenantID]
	s.mu.RUnlock()
	if ok {
		return res
	}
	// If we did not find it, we initialize an empty structure.
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check again though, maybe it was initialized just now.
	if res, ok = s.mu.tenants[tenantID]; ok {
		return res
	}
	res = newTenantOverrides(nil /* overrides */)
	s.mu.tenants[tenantID] = res
	return res
}

// SetTenantOverride changes an override for the given tenant. If the setting
// has an empty value, the existing override is removed; otherwise a new
// override is added.
func (s *overridesStore) SetTenantOverride(
	tenantID roachpb.TenantID, setting roachpb.TenantSetting,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var before []roachpb.TenantSetting
	if existing, ok := s.mu.tenants[tenantID]; ok {
		before = existing.overrides
		close(existing.changeCh)
	}
	after := make([]roachpb.TenantSetting, 0, len(before)+1)
	// 1. Add all settings up to setting.Name.
	for len(before) > 0 && before[0].Name < setting.Name {
		after = append(after, before[0])
		before = before[1:]
	}
	// 2. Add the after setting, unless we are removing it.
	if setting.Value != (settings.EncodedValue{}) {
		after = append(after, setting)
	}
	// Skip any existing setting for this name.
	if len(before) > 0 && before[0].Name == setting.Name {
		before = before[1:]
	}
	// 3. Append all settings after setting.Name.
	after = append(after, before...)
	s.mu.tenants[tenantID] = newTenantOverrides(after)
}
