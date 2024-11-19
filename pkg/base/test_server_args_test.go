// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDefaultTestTenantOptionsBehavior(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name               string
		tb                 testBehavior
		expectedNoDecision bool
	}{
		// Decision missing enabled or disabled flag
		{name: "no decision made", tb: 0, expectedNoDecision: true},
		// Decision missing process mode
		{name: "no decision on tenant process mode", tb: ttEnabled, expectedNoDecision: true},
		// Decision made to not run test tenant
		{name: "decision to not run test tenant", tb: ttDisabled, expectedNoDecision: false},
		// Decision made to run test tenant as an external process
		{name: "decision to run external test tenant", tb: ttEnabled | ttExternalProcess, expectedNoDecision: false},
		// Decision made to run test tenant as a shared process
		{name: "decision to run shared test tenant", tb: ttEnabled | ttSharedProcess, expectedNoDecision: false},
		// Decision missing enabled or disabled flag with additional erroneous flag
		{name: "no decision made with erroneous flag", tb: ttExternalProcess, expectedNoDecision: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := DefaultTestTenantOptions{testBehavior: tc.tb}
			require.Equal(t, tc.expectedNoDecision, d.TestTenantNoDecisionMade())
		})
	}
}
