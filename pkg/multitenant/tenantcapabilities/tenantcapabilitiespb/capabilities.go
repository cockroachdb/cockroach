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

func (t *TenantCapabilities) getInt32RangeFieldRef(
	capabilityName tenantcapabilitiesapi.Int32RangeCapabilityName,
) **Int32Range {
	switch capabilityName {
	case tenantcapabilitiesapi.TestRange1:
		return &t.TestRange_1
	case tenantcapabilitiesapi.TestRange2:
		return &t.TestRange_2
	default:
		panic(fmt.Sprintf("unknown capability: %q", capabilityName))
	}
}

// GetInt32RangeCapability returns the value of the corresponding Int32Range
// capability.
func (t *TenantCapabilities) GetInt32RangeCapability(
	capabilityName tenantcapabilitiesapi.Int32RangeCapabilityName,
) Int32Range {
	ref := *t.getInt32RangeFieldRef(capabilityName)
	if ref == nil {
		return Int32Range{}
	}
	return *ref
}

// SetInt32RangeCapability returns the value of the corresponding Int32Range
// capability.
func (t *TenantCapabilities) SetInt32RangeCapability(
	capabilityName tenantcapabilitiesapi.Int32RangeCapabilityName, capabilityValue Int32Range,
) {
	*t.getInt32RangeFieldRef(capabilityName) = &capabilityValue
}
