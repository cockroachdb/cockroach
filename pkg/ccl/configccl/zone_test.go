// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package configccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestValidateZoneConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		cfg      config.ZoneConfig
		expected string
	}{
		{
			config.ZoneConfig{
				LeasePreferences: []config.LeasePreference{
					{
						Constraints: []config.Constraint{},
					},
				},
			},
			"every lease preference must include at least one constraint",
		},
		{
			config.ZoneConfig{
				LeasePreferences: []config.LeasePreference{
					{
						Constraints: []config.Constraint{{Value: "a", Type: config.Constraint_DEPRECATED_POSITIVE}},
					},
				},
			},
			"lease preference constraints must either be required .+ or prohibited .+",
		},
		{
			config.ZoneConfig{
				LeasePreferences: []config.LeasePreference{
					{
						Constraints: []config.Constraint{{Value: "a", Type: config.Constraint_REQUIRED}},
					},
					{
						Constraints: []config.Constraint{{Value: "b", Type: config.Constraint_PROHIBITED}},
					},
				},
			},
			"",
		},
	}

	for i, tc := range testCases {
		err := validateLeasePreferencesImpl(tc.cfg)
		if !testutils.IsError(err, tc.expected) {
			t.Errorf("%d: expected %q, got %v", i, tc.expected, err)
		}
	}
}
