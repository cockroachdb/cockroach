// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitiespb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesapi"
	"github.com/stretchr/testify/require"
)

func TestBoolCapabilityNames(t *testing.T) {
	capabilities := TenantCapabilities{}
	for _, capabilityName := range tenantcapabilitiesapi.BoolCapabilityNames {
		// Verify starting value.
		require.Falsef(t, capabilities.GetBoolCapability(capabilityName), "%s", capabilityName)

		// Set true.
		capabilities.SetBoolCapability(capabilityName, true)
		require.Truef(t, capabilities.GetBoolCapability(capabilityName), "%s", capabilityName)

		// Set false.
		capabilities.SetBoolCapability(capabilityName, false)
		require.Falsef(t, capabilities.GetBoolCapability(capabilityName), "%s", capabilityName)
	}
}
