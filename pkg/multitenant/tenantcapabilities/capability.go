// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tenantcapabilities describes the privileges allotted to tenants.
package tenantcapabilities

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigbounds"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// Capability is an individual capability.
type Capability interface {
	fmt.Stringer
	redact.SafeFormatter
	ID() ID
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

type boolCapability ID

func (b boolCapability) String() string                                 { return ID(b).String() }
func (b boolCapability) SafeFormat(s interfaces.SafePrinter, verb rune) { s.Print(ID(b)) }
func (b boolCapability) ID() ID                                         { return ID(b) }
func (b boolCapability) Value(t *tenantcapabilitiespb.TenantCapabilities) BoolValue {
	return MustGetValueByID(t, b.ID()).(BoolValue)
}

type spanConfigBoundsCapability ID

func (b spanConfigBoundsCapability) String() string                                 { return ID(b).String() }
func (b spanConfigBoundsCapability) SafeFormat(s interfaces.SafePrinter, verb rune) { s.Print(ID(b)) }
func (b spanConfigBoundsCapability) ID() ID                                         { return ID(b) }
func (b spanConfigBoundsCapability) Value(
	t *tenantcapabilitiespb.TenantCapabilities,
) SpanConfigBoundValue {
	return MustGetValueByID(t, b.ID()).(SpanConfigBoundValue)
}

var _ TypedCapability[bool] = boolCapability(0)
