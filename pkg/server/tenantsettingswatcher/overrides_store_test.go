// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantsettingswatcher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestOverridesStore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var s overridesStore
	s.Init()
	t1 := roachpb.MustMakeTenantID(1)
	t2 := roachpb.MustMakeTenantID(2)
	expect := func(o *tenantOverrides, expected string) {
		t.Helper()
		var vals []string
		for _, s := range o.overrides {
			vals = append(vals, fmt.Sprintf("%s=%s", s.InternalKey, s.Value.Value))
		}
		if actual := strings.Join(vals, " "); actual != expected {
			t.Errorf("expected: %s; got: %s", expected, actual)
		}
	}
	expectChange := func(o *tenantOverrides) {
		t.Helper()
		select {
		case <-o.changeCh:
		case <-time.After(15 * time.Second):
			t.Fatalf("channel did not close")
		}
	}
	o1 := s.getTenantOverrides(ctx, t1)
	expect(o1, "")
	s.setAll(ctx, map[roachpb.TenantID][]kvpb.TenantSetting{
		t1: {st("a", "aa"), st("b", "bb"), st("d", "dd")},
		t2: {st("x", "xx")},
	})
	expectChange(o1)
	o1 = s.getTenantOverrides(ctx, t1)
	expect(o1, "a=aa b=bb d=dd")
	o2 := s.getTenantOverrides(ctx, t2)
	expect(o2, "x=xx")

	s.setTenantOverride(ctx, t1, st("b", "changed"))
	expectChange(o1)
	o1 = s.getTenantOverrides(ctx, t1)
	expect(o1, "a=aa b=changed d=dd")

	s.setTenantOverride(ctx, t1, st("b", ""))
	expectChange(o1)
	o1 = s.getTenantOverrides(ctx, t1)
	expect(o1, "a=aa d=dd")

	s.setTenantOverride(ctx, t1, st("c", "cc"))
	expectChange(o1)
	o1 = s.getTenantOverrides(ctx, t1)
	expect(o1, "a=aa c=cc d=dd")

	// Set an override for a tenant that has no existing data.
	t3 := roachpb.MustMakeTenantID(3)
	s.setTenantOverride(ctx, t3, st("x", "xx"))
	o3 := s.getTenantOverrides(ctx, t3)
	expect(o3, "x=xx")

	// Verify that overrides also work for the special "all tenants" ID.
	expect(s.getTenantOverrides(ctx, allTenantOverridesID), "")
	s.setTenantOverride(ctx, allTenantOverridesID, st("a", "aa"))
	s.setTenantOverride(ctx, allTenantOverridesID, st("d", "dd"))
	expect(s.getTenantOverrides(ctx, allTenantOverridesID), "a=aa d=dd")

	// Set an alternate default.
	s.setAlternateDefaults(ctx, []kvpb.TenantSetting{st("d", "d2")})

	// If a custom override is provided, the alternate default is
	// hidden.
	expect(s.getTenantOverrides(ctx, allTenantOverridesID), "a=aa d=dd")

	// Alternate defaults do not show up for regular tenant IDs.
	expect(s.getTenantOverrides(ctx, t3), "x=xx")

	// If the custom override is removed, the alternate appears.
	s.setTenantOverride(ctx, allTenantOverridesID, kvpb.TenantSetting{InternalKey: "d"})
	expect(s.getTenantOverrides(ctx, allTenantOverridesID), "a=aa d=d2")

	// If there was no override to start with, the new alternate is
	// added as pseudo-override. If not mentioned in new calls to
	// `setAlternateDefaults()`, existing alternate defaults are
	// preserved.
	s.setAlternateDefaults(ctx, []kvpb.TenantSetting{st("e", "ee")})
	expect(s.getTenantOverrides(ctx, allTenantOverridesID), "a=aa d=d2 e=ee")

	// If a custom override is added again, the alternate default gets
	// hidden again.
	s.setTenantOverride(ctx, allTenantOverridesID, st("d", "dd"))
	expect(s.getTenantOverrides(ctx, allTenantOverridesID), "a=aa d=dd e=ee")
}

func st(key settings.InternalKey, val string) kvpb.TenantSetting {
	return kvpb.TenantSetting{
		InternalKey: key,
		Value: settings.EncodedValue{
			Value: val,
		},
	}
}

func TestFindAlternateDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type kts = kvpb.TenantSetting

	for idx, tc := range []struct {
		defaults []kvpb.TenantSetting
		key      settings.InternalKey

		expectedVal string
	}{
		{[]kts{st("a", "aa"), st("c", "cc")}, "d", ""},
		{[]kts{st("a", "aa"), st("c", "cc")}, "b", ""},
		{[]kts{st("c", "cc"), st("d", "dd")}, "a", ""},
		{[]kts{st("a", "aa"), st("c", "cc")}, "a", "aa"},
		{[]kts{st("a", "aa"), st("c", "cc")}, "c", "cc"},
	} {
		t.Run(fmt.Sprint(idx), func(t *testing.T) {
			val, found := findAlternateDefault(tc.key, tc.defaults)
			require.Equal(t, tc.expectedVal != "", found)
			require.Equal(t, tc.expectedVal, val.Value)
		})
	}
}

func TestUpdateDefaults(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type kts = kvpb.TenantSetting

	for idx, tc := range []struct {
		defaults     []kvpb.TenantSetting
		moreDefaults []kvpb.TenantSetting

		expected []kvpb.TenantSetting
	}{
		// Empty case as sanity check.
		{nil, nil, nil},
		// No defaults to start with: produce at least the new defaults.
		{nil, []kts{st("a", "aa")}, []kts{st("a", "aa")}},
		// Some defaults already known.
		// update from the new defaults.
		{[]kts{st("a", "aa")}, []kts{st("a", "bb")}, []kts{st("a", "bb")}},

		// Some defaults known. No new defaults.
		{[]kts{st("a", "aa")}, nil, []kts{st("a", "aa")}},

		// Alternatives to the above, with more elements.
		{[]kts{st("a", "aa"), st("c", "cc")},
			[]kts{st("b", "bb")},
			[]kts{st("a", "aa"), st("b", "bb"), st("c", "cc")}},
		{[]kts{st("a", "aa"), st("c", "cc")},
			[]kts{st("b", "bb"), st("c", "c2"), st("d", "dd")},
			[]kts{st("a", "aa"), st("b", "bb"), st("c", "c2"), st("d", "dd")}},
		{[]kts{st("b", "bb"), st("d", "dd")},
			// Edge case: the number of alternate defaults before the first
			// explicit value, is larger than the number of explicit values
			// to start with. This test catches a pitfall in the splice
			// logic.
			[]kts{st("a", "aa"), st("a2", "aa"), st("a3", "aa"),
				st("b", "b2"), st("c", "cc"), st("c2", "cc"), st("d", "d2"), st("e", "ee")},
			[]kts{st("a", "aa"), st("a2", "aa"), st("a3", "aa"),
				st("b", "b2"), st("c", "cc"), st("c2", "cc"), st("d", "d2"), st("e", "ee")}},
	} {
		t.Run(fmt.Sprint(idx), func(t *testing.T) {
			result := updateDefaults(tc.defaults, tc.moreDefaults)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestSpliceOverrideDefaults(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mexp := func(keys ...settings.InternalKey) map[settings.InternalKey]struct{} {
		m := make(map[settings.InternalKey]struct{}, len(keys))
		for _, k := range keys {
			m[k] = struct{}{}
		}
		return m
	}

	type kts = kvpb.TenantSetting

	for idx, tc := range []struct {
		explicitKeys      map[settings.InternalKey]struct{}
		overrides         []kvpb.TenantSetting
		alternateDefaults []kvpb.TenantSetting

		expected []kvpb.TenantSetting
	}{
		// Empty case as sanity check.
		{nil, nil, nil, nil},
		// No overrides to start with: produce at least the alternate defaults.
		{nil, nil, []kts{st("a", "aa")}, []kts{st("a", "aa")}},
		// Overrides previously set from alternate defaults (no explicitKeys):
		// update from the new alternate defaults.
		{nil, []kts{st("a", "aa")}, []kts{st("a", "bb")}, []kts{st("a", "bb")}},
		// Overrides previously set from rangefeed (key in explicitKeys):
		// keep the override.
		{mexp("a"), []kts{st("a", "aa")}, []kts{st("a", "bb")}, []kts{st("a", "aa")}},

		// Override previously set from alternate default (no explicitKeys)
		// and the alternate default is removed. Remove the override.
		{nil, []kts{st("a", "aa")}, nil, []kts{}},

		// Alternatives to the above, with more elements.
		{nil,
			[]kts{st("a", "aa"), st("c", "cc")},
			[]kts{st("b", "bb")},
			[]kts{st("b", "bb")}},
		{mexp("a", "c"),
			[]kts{st("a", "aa"), st("c", "cc")},
			[]kts{st("b", "bb")},
			[]kts{st("a", "aa"), st("b", "bb"), st("c", "cc")}},
		{mexp("a"),
			[]kts{st("a", "aa"), st("c", "cc")},
			[]kts{st("b", "bb"), st("c", "c2"), st("d", "dd")},
			[]kts{st("a", "aa"), st("b", "bb"), st("c", "c2"), st("d", "dd")}},
		{mexp("b", "d"),
			[]kts{st("b", "bb"), st("d", "dd")},
			// Edge case: the number of alternate defaults before the first
			// explicit value, is larger than the number of explicit values
			// to start with. This test catches a pitfall in the splice
			// logic.
			[]kts{st("a", "aa"), st("a2", "aa"), st("a3", "aa"),
				st("b", "b2"), st("c", "cc"), st("c2", "cc"), st("d", "d2"), st("e", "ee")},
			[]kts{st("a", "aa"), st("a2", "aa"), st("a3", "aa"),
				st("b", "bb"), st("c", "cc"), st("c2", "cc"), st("d", "dd"), st("e", "ee")}},
	} {
		testutils.RunTrueAndFalse(t, "copy", func(t *testing.T, copy bool) {
			t.Run(fmt.Sprint(idx), func(t *testing.T) {
				result := spliceOverrideDefaults(tc.explicitKeys, tc.overrides, tc.alternateDefaults, copy)
				require.Equal(t, tc.expected, result)
			})
		})
	}
}
