// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package uisettings

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestUIHandler_DefaultTimezone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testSettings := cluster.MakeTestingClusterSettings()

	DefaultTimezone.Override(context.Background(), &testSettings.SV, "America/New_York")
	require.Equal(t, "America/New_York", DefaultTimezone.Get(&testSettings.SV))

	testcases := []struct {
		name     string
		timezone string
		valid    bool
	}{
		{"empty", "", true},
		{"canonical", "Africa/Abidjan", true},
		{"link", "US/Eastern", true},
		{"invalid", "invalid/timezone", false},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			DefaultTimezone.Override(context.Background(), &testSettings.SV, tc.timezone)
			if tc.valid {
				require.NoError(t, DefaultTimezone.Validate(&testSettings.SV, tc.timezone))
			} else {
				require.Error(t, DefaultTimezone.Validate(&testSettings.SV, tc.timezone))
			}
		})
	}
}
