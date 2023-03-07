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
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesapi"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// boolCapValue is a wrapper around bool that ensures that values can
// be included in reportables.
type boolCapValue bool

func (b boolCapValue) String() string { return redact.Sprint(b).StripMarkers() }
func (b boolCapValue) SafeFormat(p redact.SafePrinter, verb rune) {
	p.Print(redact.Safe(bool(b)))
}

// Unwrap implements the tenantcapabilitiesapi.Value interface.
func (b boolCapValue) Unwrap() interface{} { return bool(b) }

// boolCap is an accessor struct for boolean capabilities.
type boolCap struct {
	cap *bool
}

// Get implements the tenantcapabilitiesapi.Capability interface.
func (b boolCap) Get() tenantcapabilitiesapi.Value {
	return boolCapValue(*b.cap)
}

// Set implements the tenantcapabilitiesapi.Capability interface.
func (b boolCap) Set(val interface{}) {
	bval, ok := val.(bool)
	if !ok {
		panic(errors.AssertionFailedf("invalid value type: %T", val))
	}
	*b.cap = bval
}

// For implements the tenantcapabilitiesapi.TenantCapabilities interface.
func (t *TenantCapabilities) Cap(
	capabilityID tenantcapabilitiesapi.CapabilityID,
) tenantcapabilitiesapi.Capability {
	switch capabilityID {
	case tenantcapabilitiesapi.CanAdminSplit:
		return boolCap{&t.CanAdminSplit}
	case tenantcapabilitiesapi.CanViewNodeInfo:
		return boolCap{&t.CanViewNodeInfo}
	case tenantcapabilitiesapi.CanViewTSDBMetrics:
		return boolCap{&t.CanViewTSDBMetrics}

	default:
		panic(errors.AssertionFailedf("unknown capability: %q", capabilityID.String()))
	}
}

// GetBool implements the tenantcapabilitiesapi.TenantCapabilities interface. It is an optimization.
func (t *TenantCapabilities) GetBool(capabilityID tenantcapabilitiesapi.CapabilityID) bool {
	switch capabilityID {
	case tenantcapabilitiesapi.CanAdminSplit:
		return t.CanAdminSplit
	case tenantcapabilitiesapi.CanViewNodeInfo:
		return t.CanViewNodeInfo
	case tenantcapabilitiesapi.CanViewTSDBMetrics:
		return t.CanViewTSDBMetrics

	default:
		panic(errors.AssertionFailedf("unknown or non-bool capability: %q", capabilityID.String()))
	}
}
