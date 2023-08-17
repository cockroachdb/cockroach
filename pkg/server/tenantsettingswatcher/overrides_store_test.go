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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestOverridesStore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var s overridesStore
	s.Init()
	t1 := roachpb.MustMakeTenantID(1)
	t2 := roachpb.MustMakeTenantID(2)
	st := func(key settings.InternalKey, val string) kvpb.TenantSetting {
		return kvpb.TenantSetting{
			InternalKey: key,
			Value: settings.EncodedValue{
				Value: val,
			},
		}
	}
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
	o1 := s.GetTenantOverrides(t1)
	expect(o1, "")
	s.SetAll(map[roachpb.TenantID][]kvpb.TenantSetting{
		t1: {st("a", "aa"), st("b", "bb"), st("d", "dd")},
		t2: {st("x", "xx")},
	})
	expectChange(o1)
	o1 = s.GetTenantOverrides(t1)
	expect(o1, "a=aa b=bb d=dd")
	o2 := s.GetTenantOverrides(t2)
	expect(o2, "x=xx")

	s.SetTenantOverride(t1, st("b", "changed"))
	expectChange(o1)
	o1 = s.GetTenantOverrides(t1)
	expect(o1, "a=aa b=changed d=dd")

	s.SetTenantOverride(t1, st("b", ""))
	expectChange(o1)
	o1 = s.GetTenantOverrides(t1)
	expect(o1, "a=aa d=dd")

	s.SetTenantOverride(t1, st("c", "cc"))
	expectChange(o1)
	o1 = s.GetTenantOverrides(t1)
	expect(o1, "a=aa c=cc d=dd")

	// Set an override for a tenant that has no existing data.
	t3 := roachpb.MustMakeTenantID(3)
	s.SetTenantOverride(t3, st("x", "xx"))
	o3 := s.GetTenantOverrides(t3)
	expect(o3, "x=xx")
}
