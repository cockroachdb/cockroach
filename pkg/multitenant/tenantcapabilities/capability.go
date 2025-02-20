// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tenantcapabilities describes the privileges allotted to tenants.
package tenantcapabilities

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigbounds"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// Capability is an individual capability.
type Capability interface {
	fmt.Stringer
	redact.SafeFormatter
	ID() tenantcapabilitiespb.ID
}

// TypedCapability is a convenience interface to describe the specializations
// of Capability for extracting typed values from a set of capabilities.
type TypedCapability[T any] interface {
	Capability
	Value(*tenantcapabilitiespb.TenantCapabilities) TypedValue[T]
}

type (
	BoolCapability             = TypedCapability[bool]
	SpanConfigBoundsCapability = TypedCapability[*spanconfigbounds.Bounds]
)

type boolCapability tenantcapabilitiespb.ID

func (b boolCapability) String() string { return tenantcapabilitiespb.ID(b).String() }
func (b boolCapability) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.Print(tenantcapabilitiespb.ID(b))
}
func (b boolCapability) ID() tenantcapabilitiespb.ID { return tenantcapabilitiespb.ID(b) }
func (b boolCapability) Value(t *tenantcapabilitiespb.TenantCapabilities) BoolValue {
	return MustGetValueByID(t, b.ID()).(BoolValue)
}

type spanConfigBoundsCapability tenantcapabilitiespb.ID

func (b spanConfigBoundsCapability) String() string { return tenantcapabilitiespb.ID(b).String() }
func (b spanConfigBoundsCapability) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.Print(tenantcapabilitiespb.ID(b))
}
func (b spanConfigBoundsCapability) ID() tenantcapabilitiespb.ID { return tenantcapabilitiespb.ID(b) }
func (b spanConfigBoundsCapability) Value(
	t *tenantcapabilitiespb.TenantCapabilities,
) SpanConfigBoundValue {
	return MustGetValueByID(t, b.ID()).(SpanConfigBoundValue)
}

var _ TypedCapability[bool] = boolCapability(0)

// FromName looks up a capability by name.
func FromName(s string) (Capability, bool) {
	if id, ok := tenantcapabilitiespb.FromName(s); ok {
		return FromID(id)
	}
	return nil, false
}

// FromID looks up a capability by ID.
func FromID(id tenantcapabilitiespb.ID) (Capability, bool) {
	if id.IsValid() {
		return capabilities[id], true
	}
	return nil, false
}

var capabilities = [tenantcapabilitiespb.MaxCapabilityID + 1]Capability{
	tenantcapabilitiespb.CanAdminRelocateRange:  boolCapability(tenantcapabilitiespb.CanAdminRelocateRange),
	tenantcapabilitiespb.CanAdminScatter:        boolCapability(tenantcapabilitiespb.CanAdminScatter),
	tenantcapabilitiespb.CanAdminSplit:          boolCapability(tenantcapabilitiespb.CanAdminSplit),
	tenantcapabilitiespb.CanAdminUnsplit:        boolCapability(tenantcapabilitiespb.CanAdminUnsplit),
	tenantcapabilitiespb.CanCheckConsistency:    boolCapability(tenantcapabilitiespb.CanCheckConsistency),
	tenantcapabilitiespb.CanUseNodelocalStorage: boolCapability(tenantcapabilitiespb.CanUseNodelocalStorage),
	tenantcapabilitiespb.CanViewNodeInfo:        boolCapability(tenantcapabilitiespb.CanViewNodeInfo),
	tenantcapabilitiespb.CanViewTSDBMetrics:     boolCapability(tenantcapabilitiespb.CanViewTSDBMetrics),
	tenantcapabilitiespb.ExemptFromRateLimiting: boolCapability(tenantcapabilitiespb.ExemptFromRateLimiting),
	tenantcapabilitiespb.TenantSpanConfigBounds: spanConfigBoundsCapability(tenantcapabilitiespb.TenantSpanConfigBounds),
	tenantcapabilitiespb.CanDebugProcess:        boolCapability(tenantcapabilitiespb.CanDebugProcess),
	tenantcapabilitiespb.CanViewAllMetrics:      boolCapability(tenantcapabilitiespb.CanViewAllMetrics),
	tenantcapabilitiespb.CanPrepareTxns:         boolCapability(tenantcapabilitiespb.CanPrepareTxns),
}

// EnableAll enables maximum access to services.
func EnableAll(t *tenantcapabilitiespb.TenantCapabilities) {
	for i := tenantcapabilitiespb.ID(1); i <= tenantcapabilitiespb.MaxCapabilityID; i++ {
		val, err := GetValueByID(t, i)
		if err != nil {
			panic(err)
		}
		switch v := val.(type) {
		case TypedValue[bool]:
			// Access to the service is enabled.
			v.Set(true)

		case TypedValue[*spanconfigbounds.Bounds]:
			// No bound.
			v.Set(nil)

		default:
			panic(errors.AssertionFailedf("unhandled type: %T", val))
		}
	}
}
