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
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
)

func TestCapabilityGetSet(t *testing.T) {
	capabilities := TenantCapabilities{}
	for _, capID := range tenantcapabilities.CapabilityIDs {
		switch typedCapID := capID.(type) {
		case tenantcapabilities.BoolCapabilityID:
			require.Falsef(t, capabilities.GetBool(typedCapID), "%s", typedCapID)
			capabilities.SetBool(typedCapID, true)
			require.Truef(t, capabilities.GetBool(typedCapID), "%s", typedCapID)
			capabilities.SetBool(typedCapID, false)
			require.Falsef(t, capabilities.GetBool(typedCapID), "%s", typedCapID)
		case tenantcapabilities.SpanConfigCapabilityID:
			// TODO
		default:
			require.Fail(t, "unknown capability %q", typedCapID)
	}
}
