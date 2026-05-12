// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestLicenseAuditCommand verifies the license audit command executes without
// errors and validates the format flag.
func TestLicenseAuditCommand(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name        string
		args        []string
		expectedErr string
	}{
		{
			name: "default format",
			args: []string{"license", "audit"},
		},
		{
			name: "yaml format",
			args: []string{"license", "audit", "--format=yaml"},
		},
		{
			name: "json format",
			args: []string{"license", "audit", "--format=json"},
		},
		{
			name:        "invalid format",
			args:        []string{"license", "audit", "--format=xml"},
			expectedErr: "invalid format",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Avoid leaking configuration changes after the test ends.
			defer initCLIDefaults()

			err := Run(tc.args)

			if tc.expectedErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
