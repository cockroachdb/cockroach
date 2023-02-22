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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesapi"
)

func (t *TenantCapabilities) getBoolFieldRef(
	capabilityName tenantcapabilitiesapi.BoolCapabilityName,
) *bool {
	switch capabilityName {
	case tenantcapabilitiesapi.CanAdminSplit:
		return &t.CanAdminSplit
	case tenantcapabilitiesapi.CanViewNodeInfo:
		return &t.CanViewNodeInfo
	case tenantcapabilitiesapi.CanViewTSDBMetrics:
		return &t.CanViewTSDBMetrics
	default:
		panic(fmt.Sprintf("unknown capability: %q", capabilityName.String()))
	}
}

// GetBoolCapability implements tenantcapabilitiesapi.TenantCapabilities.
func (t *TenantCapabilities) GetBoolCapability(
	capabilityName tenantcapabilitiesapi.BoolCapabilityName,
) bool {
	return *t.getBoolFieldRef(capabilityName)
}

// SetBoolCapability implements tenantcapabilitiesapi.TenantCapabilities.
func (t *TenantCapabilities) SetBoolCapability(
	capabilityName tenantcapabilitiesapi.BoolCapabilityName, capabilityValue bool,
) {
	*t.getBoolFieldRef(capabilityName) = capabilityValue
}
