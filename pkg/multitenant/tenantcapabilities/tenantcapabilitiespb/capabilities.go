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

func (t *TenantCapabilities) getBoolRef(capID tenantcapabilities.BoolCapabilityID) *bool {
	switch capID {
	case tenantcapabilities.CanAdminSplit:
		return &t.CanAdminSplit
	case tenantcapabilities.CanViewNodeInfo:
		return &t.CanViewNodeInfo
	case tenantcapabilities.CanViewTSDBMetrics:
		return &t.CanViewTSDBMetrics
	default:
		panic(errors.AssertionFailedf("unknown capability: %q", capID))
	}
}

// GetBool implements the tenantcapabilities.TenantCapabilities interface.
func (t *TenantCapabilities) GetBool(capID tenantcapabilities.BoolCapabilityID) bool {
	return *t.getBoolRef(capID)
}

// SetBool implements the tenantcapabilities.TenantCapabilities interface.
func (t *TenantCapabilities) SetBool(capID tenantcapabilities.BoolCapabilityID, value bool) {
	*t.getBoolRef(capID) = value
}

// boolValue is a wrapper around bool that ensures that values can
// be included in reportables.
type boolValue bool

func (v boolValue) String() string { return redact.Sprint(v).StripMarkers() }
func (v boolValue) SafeFormat(p redact.SafePrinter, verb rune) {
	p.Print(redact.Safe(bool(v)))
}

func (t *TenantCapabilities) GetBoolValue(
	capID tenantcapabilities.BoolCapabilityID,
) tenantcapabilities.Value {
	return boolValue(t.GetBool(capID))
}

// spanConfigValue is a wrapper around bool that ensures that values can
// be included in reportables.
type spanConfigValue SpanConfigBounds

func (v spanConfigValue) String() string { return redact.Sprint(v).StripMarkers() }
func (v spanConfigValue) SafeFormat(p redact.SafePrinter, verb rune) {
	p.Print(redact.Safe(SpanConfigBounds(v)))
}

func (t *TenantCapabilities) GetSpanConfigValue(
	tenantcapabilities.SpanConfigCapabilityID,
) tenantcapabilities.Value {
	spanConfigBounds := SpanConfigBounds{}
	if t.SpanConfigBounds != nil {
		spanConfigBounds = *t.SpanConfigBounds
	}
	return spanConfigValue(spanConfigBounds)
}
