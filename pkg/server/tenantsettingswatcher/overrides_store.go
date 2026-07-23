// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantsettingswatcher

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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

		// alternateDefaults defines values to use for the
		// allTenantOverrides settings when there is no override set via
		// tenant_settings.
		//
		// The slice is sorted by InternalKey.
		//
		// At the time of this writing, this is used for SystemVisible
		// settings.
		alternateDefaults []kvpb.TenantSetting
	}
}

// tenantOverrides stores the current overrides for a tenant (or the current
// all-tenant overrides). It is an immutable data structure.
type tenantOverrides struct {
	// overrides, ordered by InternalKey.
	overrides []kvpb.TenantSetting

	// explicitKeys contains override keys that got their value from the
	// rangefeed over system.tenant_settings, i.e. NOT those inherited
	// from alternateDefaults.
	//
	// This map is used when an alternate default is changed
	// asynchronously after the overrides have been loaded from the
	// rangefeed already, to detect which overrides need to be updated
	// from alternate defaults vs those that need to stay as-is (because
	// they come from an override in .tenant_settings).
	explicitKeys map[settings.InternalKey]struct{}

	// changeCh is a channel that is closed when the tenant overrides change (in
	// which case a new tenantOverrides object will contain the updated settings).
	changeCh chan struct{}
}

func newTenantOverrides(
	ctx context.Context,
	tenID roachpb.TenantID,
	overrides []kvpb.TenantSetting,
	explicitKeys map[settings.InternalKey]struct{},
) *tenantOverrides {
	if log.V(1) {
		var buf redact.StringBuilder
		buf.Printf("loaded overrides for tenant %d (%s)\n", tenID.InternalValue, util.GetSmallTrace(2))
		for _, v := range overrides {
			buf.Printf("%v = %+v", v.InternalKey, v.Value)
			if _, exists := explicitKeys[v.InternalKey]; exists {
				buf.Printf(" (explicit)")
			}
			buf.SafeRune('\n')
		}
		log.VEventf(ctx, 1, "%v", buf)
	}
	return &tenantOverrides{
		overrides:    overrides,
		explicitKeys: explicitKeys,
		changeCh:     make(chan struct{}),
	}
}

func (s *overridesStore) Init() {
	s.mu.tenants = make(map[roachpb.TenantID]*tenantOverrides)
}

// setAll initializes the overrides for all tenants. Any existing overrides are
// replaced.
//
// The store takes ownership of the overrides slices in the map (the caller can
// no longer modify them).
//
// This method is called once we complete a full initial scan of the
// tenant_setting table.
func (s *overridesStore) setAll(
	ctx context.Context, allOverrides map[roachpb.TenantID][]kvpb.TenantSetting,
) {
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
			return overrides[i].InternalKey < overrides[j].InternalKey
		})

		providedKeys := make(map[settings.InternalKey]struct{}, len(overrides))
		for _, v := range overrides {
			providedKeys[v.InternalKey] = struct{}{}
		}
		// If we are setting the all-tenant overrides, ensure there is a
		// pseudo-override for every SystemVisible setting with an
		// alternate default.
		if tenantID == allTenantOverridesID && len(s.mu.alternateDefaults) > 0 {
			// We can set copyOverrides==false because we took ownership
			// over the incoming allOverrides map.
			overrides = spliceOverrideDefaults(providedKeys, overrides, s.mu.alternateDefaults, false /* copyOverrides */)
		}
		s.mu.tenants[tenantID] = newTenantOverrides(ctx, tenantID, overrides, providedKeys)
	}
}

func checkSortedByKey(a []kvpb.TenantSetting) {
	if !buildutil.CrdbTestBuild {
		return
	}
	for i := 1; i < len(a); i++ {
		if a[i].InternalKey <= a[i-1].InternalKey {
			panic(errors.AssertionFailedf("duplicate setting: %s", a[i].InternalKey))
		}
	}
}

// getTenantOverrides retrieves the overrides for a given tenant.
//
// The caller can listen for closing of changeCh, which is guaranteed to happen
// if the tenant's overrides change.
func (s *overridesStore) getTenantOverrides(
	ctx context.Context, tenantID roachpb.TenantID,
) *tenantOverrides {
	s.mu.RLock()
	res, ok := s.mu.tenants[tenantID]
	s.mu.RUnlock()
	if ok {
		return res
	}
	// If we did not find it, we initialize an empty structure.
	// This is necessary because the caller of GetTenantOverrides wants
	// to register as a listener on the change channel, and we need to
	// return a valid channel that will get closed later when we
	// actually find data for this tenant.
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check again though, maybe it was initialized just now.
	if res, ok = s.mu.tenants[tenantID]; ok {
		return res
	}
	var overrides []kvpb.TenantSetting
	if tenantID == allTenantOverridesID {
		// Inherit alternate defaults.
		overrides = make([]kvpb.TenantSetting, len(s.mu.alternateDefaults))
		copy(overrides, s.mu.alternateDefaults)
	}
	res = newTenantOverrides(ctx, tenantID, overrides, nil /* explicitKeys */)
	s.mu.tenants[tenantID] = res
	return res
}

// setTenantOverride changes an override for the given tenant. If the setting
// has an empty value, the existing override is removed; otherwise a new
// override is added.
func (s *overridesStore) setTenantOverride(
	ctx context.Context, tenantID roachpb.TenantID, setting kvpb.TenantSetting,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var before []kvpb.TenantSetting
	var providedKeys map[settings.InternalKey]struct{}
	if existing, ok := s.mu.tenants[tenantID]; ok {
		before = existing.overrides
		providedKeys = existing.explicitKeys
		close(existing.changeCh)
	}
	if providedKeys == nil {
		providedKeys = make(map[settings.InternalKey]struct{})
	}
	after := make([]kvpb.TenantSetting, 0, len(before)+1)
	// 1. Add all settings up to setting.InternalKey.
	for len(before) > 0 && before[0].InternalKey < setting.InternalKey {
		after = append(after, before[0])
		before = before[1:]
	}
	// 2. Add the after setting, unless we are removing it.
	if setting.Value != (settings.EncodedValue{}) {
		after = append(after, setting)
		providedKeys[setting.InternalKey] = struct{}{}
	} else {
		// The override is being removed. If we have an alternate default,
		// use it instead.
		delete(providedKeys, setting.InternalKey)
		if tenantID == allTenantOverridesID {
			if defaultVal, ok := findAlternateDefault(setting.InternalKey, s.mu.alternateDefaults); ok {
				after = append(after, kvpb.TenantSetting{
					InternalKey: setting.InternalKey,
					Value:       defaultVal,
				})
			}
		}
	}
	// Skip any existing setting for this setting key.
	if len(before) > 0 && before[0].InternalKey == setting.InternalKey {
		before = before[1:]
	}
	// 3. Append all settings after setting.InternalKey.
	after = append(after, before...)

	// Sanity check.
	checkSortedByKey(after)

	s.mu.tenants[tenantID] = newTenantOverrides(ctx, tenantID, after, providedKeys)
}

// setAlternateDefaults defines alternate defaults, to use
// when there is no stored default in .tenant_settings.
//
// At the time of this writing, this is called when a change is made
// to a SystemVisible setting in the system tenant's system.settings
// table. Values set this way serve as default value if there is no
// override in system.tenant_settings.
//
// The input slice must be sorted by key already.
func (s *overridesStore) setAlternateDefaults(
	ctx context.Context, alternateDefaultSlice []kvpb.TenantSetting,
) {
	// Sanity check.
	checkSortedByKey(alternateDefaultSlice)

	s.mu.Lock()
	defer s.mu.Unlock()

	alternateDefaults := s.mu.alternateDefaults
	alternateDefaults = updateDefaults(alternateDefaults, alternateDefaultSlice)
	s.mu.alternateDefaults = alternateDefaults

	// Inject the new read-only default into the all-tenants
	// override map.
	var overrides []kvpb.TenantSetting
	var explicitKeys map[settings.InternalKey]struct{}
	copyOverrides := false
	if existing, ok := s.mu.tenants[allTenantOverridesID]; ok {
		explicitKeys = existing.explicitKeys
		overrides = existing.overrides
		// Need to pass copyOverrides==true because we don't want to
		// modify the overrides slice in-place -- there may be listeners
		// that are consuming the slice asynchronously.
		copyOverrides = true
		close(existing.changeCh)
	}

	overrides = spliceOverrideDefaults(explicitKeys, overrides, alternateDefaults, copyOverrides)
	s.mu.tenants[allTenantOverridesID] = newTenantOverrides(ctx, allTenantOverridesID, overrides, explicitKeys)
}

// findAlternateDefault searches the value associated with the given key
// in the alternate default slice, which is assumed to be sorted.
func findAlternateDefault(
	key settings.InternalKey, defaults []kvpb.TenantSetting,
) (val settings.EncodedValue, found bool) {
	idx := sort.Search(len(defaults), func(i int) bool {
		return defaults[i].InternalKey >= key
	})
	if idx >= len(defaults) {
		return val, false
	}
	if defaults[idx].InternalKey != key {
		return val, false
	}
	return defaults[idx].Value, true
}

// spliceOverrideDefaults adds overrides into the 'overrides' slice
// for each key that doesn't have an override yet but is present in
// alternateDefaults.
//
// If the key already has an override, but wasn't set explicitly via
// the rangefeed from tenant_settings (as informed by explicitKeys),
// this means the override was already a fallback to the alternate
// default before; in which case we update it to the new value of the
// alternate default.
//
// The alternateDefaults slice must be sorted already.
//
// If the copyOverrides flag is set, the overrides slice is modified
// in-place. If false, it is copied first. This is necessary when
// updating the overrides slice that was already stored in the tenants
// map before (it can be accessed concurrently).
func spliceOverrideDefaults(
	explicitKeys map[settings.InternalKey]struct{},
	overrides []kvpb.TenantSetting,
	alternateDefaults []kvpb.TenantSetting,
	copyOverrides bool,
) []kvpb.TenantSetting {
	if copyOverrides && len(overrides) > 0 {
		dst := make([]kvpb.TenantSetting, len(overrides))
		copy(dst, overrides)
		overrides = dst
	}
	aIter, bIter := 0, 0
	for aIter < len(overrides) && bIter < len(alternateDefaults) {
		if overrides[aIter].InternalKey == alternateDefaults[bIter].InternalKey {
			if _, ok := explicitKeys[overrides[aIter].InternalKey]; !ok {
				// The key is not explicitly set (via the rangefeed), this means
				// that we were relying on the default to start with.
				// Update the override from the new value of the default.
				overrides[aIter] = alternateDefaults[bIter]
			}
			aIter++
			bIter++
		} else if overrides[aIter].InternalKey < alternateDefaults[bIter].InternalKey {
			if _, ok := explicitKeys[overrides[aIter].InternalKey]; !ok {
				// The key is not explicitly set (via the rangefeed), this
				// means that we were relying on the default to start with.
				// But now, there is no default any more. Remove the override.
				copy(overrides[aIter:], overrides[aIter+1:])
				overrides = overrides[:len(overrides)-1]
			} else {
				aIter++
			}
		} else {
			// The following is an optimization, to append as many missing elements
			// from the alternateDefaults slice as possible in one go.
			// This can be implemented also (albeit less efficiently) as:
			//  tail := overrides[aIter:]
			//  dst = append(overrides[:aIter:aIter], alternateDefaults[bIter])
			//  overrides = append(overrides, tail...)
			//  aIter++
			//  bIter++
			//  continue
			numOverrides := 0
			for ; (bIter+numOverrides) < len(alternateDefaults) &&
				overrides[aIter].InternalKey > alternateDefaults[bIter+numOverrides].InternalKey; numOverrides++ {
			}
			tail := overrides[aIter:]
			overrides = append(overrides[:aIter:aIter], alternateDefaults[bIter:bIter+numOverrides]...)
			overrides = append(overrides, tail...)
			aIter += numOverrides
			bIter += numOverrides
		}
	}
	for aIter < len(overrides) {
		if _, ok := explicitKeys[overrides[aIter].InternalKey]; !ok {
			// The key is not explicitly set (via the rangefeed), this
			// means that we were relying on the default to start with.
			// But now, there is no default any more. Remove the override.
			copy(overrides[aIter:], overrides[aIter+1:])
			overrides = overrides[:len(overrides)-1]
		} else {
			aIter++
		}
	}
	if bIter < len(alternateDefaults) {
		overrides = append(overrides, alternateDefaults[bIter:]...)
	}
	// Sanity check.
	checkSortedByKey(overrides)
	return overrides
}

// updateDefaults extends the slice in the first argument with the
// elements in the second argument. If the same setting key is present
// in both, the first slice is updated from the second.
//
// NB: the dst slice is modified in-place.
func updateDefaults(dst, src []kvpb.TenantSetting) []kvpb.TenantSetting {
	aIter, bIter := 0, 0
	for aIter < len(dst) && bIter < len(src) {
		if dst[aIter].InternalKey == src[bIter].InternalKey {
			dst[aIter] = src[bIter]
			aIter++
			bIter++
		} else if dst[aIter].InternalKey < src[bIter].InternalKey {
			aIter++
		} else {
			// The following is an optimization, to append as many missing elements
			// from the src slice as possible in one go.
			// This can be implemented also (albeit less efficiently) as:
			//  tail := dst[aIter:]
			//  dst = append(dst[:aIter:aIter], src[bIter])
			//  dst = append(dst, tail...)
			//  aIter++
			//  bIter++
			//  continue
			numDst := 0
			for ; (bIter+numDst) < len(src) &&
				dst[aIter].InternalKey > src[bIter+numDst].InternalKey; numDst++ {
			}
			tail := dst[aIter:]
			dst = append(dst[:aIter:aIter], src[bIter:bIter+numDst]...)
			dst = append(dst, tail...)
			aIter += numDst
			bIter += numDst
		}
	}
	if bIter < len(src) {
		dst = append(dst, src[bIter:]...)
	}

	// Sanity check.
	checkSortedByKey(dst)
	return dst
}
