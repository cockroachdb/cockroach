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
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

func init() {
	tenantcapabilities.RegisterDefaultCapabilities(&TenantCapabilities{})
}

// boolCapValue is a wrapper around bool that ensures that values can
// be included in reportables.
type boolCapValue bool

func (b boolCapValue) String() string { return redact.Sprint(b).StripMarkers() }
func (b boolCapValue) SafeFormat(p redact.SafePrinter, verb rune) {
	p.Print(redact.Safe(bool(b)))
}

// Unwrap implements the tenantcapabilities.Value interface.
func (b boolCapValue) Unwrap() interface{} { return bool(b) }

// boolCap is an accessor struct for boolean capabilities.
type boolCap struct {
	cap *bool
}

// Get implements the tenantcapabilities.Capability interface.
func (b boolCap) Get() tenantcapabilities.Value {
	return boolCapValue(*b.cap)
}

// Set implements the tenantcapabilities.Capability interface.
func (b boolCap) Set(val interface{}) {
	bval, ok := val.(bool)
	if !ok {
		panic(errors.AssertionFailedf("invalid value type: %T", val))
	}
	*b.cap = bval
}

// invertedBoolCap is an accessor struct for boolean capabilities that are
// stored as "disabled" in the underlying proto. Layers above this package
// interact are oblivious to this detail.
type invertedBoolCap struct {
	cap *bool
}

// Get implements the tenantcapabilities.Capability interface.
func (i invertedBoolCap) Get() tenantcapabilities.Value {
	val := *i.cap
	return boolCapValue(!val) // inverted
}

// Set implements the tenantcapabilities.Capability interface.
func (i invertedBoolCap) Set(val interface{}) {
	bval, ok := val.(bool)
	if !ok {
		panic(errors.AssertionFailedf("invalid value type: %T", val))
	}
	*i.cap = !bval
}

// Cap implements the tenantcapabilities.TenantCapabilities interface.
func (t *TenantCapabilities) Cap(
	capabilityID tenantcapabilities.CapabilityID,
) tenantcapabilities.Capability {
	switch capabilityID {
	case tenantcapabilities.CanAdminScatter:
		return invertedBoolCap{&t.DisableAdminScatter}
	case tenantcapabilities.CanAdminSplit:
		return invertedBoolCap{&t.DisableAdminSplit}
	case tenantcapabilities.CanAdminUnsplit:
		return boolCap{&t.CanAdminUnsplit}
	case tenantcapabilities.CanViewNodeInfo:
		return boolCap{&t.CanViewNodeInfo}
	case tenantcapabilities.CanViewTSDBMetrics:
		return boolCap{&t.CanViewTSDBMetrics}

	default:
		panic(errors.AssertionFailedf("unknown capability: %q", capabilityID.String()))
	}
}

// GetBool implements the tenantcapabilities.TenantCapabilities interface.
// It is an optimization.
func (t *TenantCapabilities) GetBool(capabilityID tenantcapabilities.CapabilityID) bool {
	switch capabilityID {
	case tenantcapabilities.CanAdminScatter:
		return !t.DisableAdminScatter
	case tenantcapabilities.CanAdminSplit:
		return !t.DisableAdminSplit
	case tenantcapabilities.CanAdminUnsplit:
		return t.CanAdminUnsplit
	case tenantcapabilities.CanViewNodeInfo:
		return t.CanViewNodeInfo
	case tenantcapabilities.CanViewTSDBMetrics:
		return t.CanViewTSDBMetrics

	default:
		panic(errors.AssertionFailedf("unknown or non-bool capability: %q", capabilityID.String()))
	}
}
