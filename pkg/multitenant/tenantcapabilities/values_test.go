// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcapabilities

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func someCaps() *tenantcapabilitiespb.TenantCapabilities {
	return &tenantcapabilitiespb.TenantCapabilities{}
}

// TestIDs ensures that iterating IDs always works for the ID lookup functions.
func TestIDs(t *testing.T) {
	for _, id := range IDs {
		_, err := GetValueByID(someCaps(), id)
		require.NoError(t, err, id)
		_, ok := FromID(id)
		require.True(t, ok, id)
	}
}

// TestGetSet ensures that Get and Set are implemented for all
// capability types, and all default Get results are valid input for
// Set.
func TestGetSet(t *testing.T) {
	var v tenantcapabilitiespb.TenantCapabilities
	for _, id := range IDs {
		switch c, _ := FromID(id); c := c.(type) {
		case BoolCapability:
			c.Value(&v).Set(c.Value(someCaps()).Get())
		case SpanConfigBoundsCapability:
			c.Value(&v).Set(c.Value(someCaps()).Get())
		default:
			panic(errors.AssertionFailedf("unknown capability type %T", c))
		}
	}
}

// TestSpanConfigBoundsSetGet ensures calling Get on SpanConfigBounds that have
// been previously modified using a Set returns the correct value.
func TestSpanConfigBoundsSetGet(t *testing.T) {
	capability := spanConfigBoundsCapability(TenantSpanConfigBounds)
	val := capability.Value(someCaps())

	// Construct some span config bounds that apply to GC TTLs.
	var v tenantcapabilitiespb.TenantCapabilities
	const ttlStart = int32(500)
	const ttlEnd = int32(1000)
	v.SpanConfigBounds = &tenantcapabilitiespb.SpanConfigBounds{
		GCTTLSeconds: &tenantcapabilitiespb.SpanConfigBounds_Int32Range{
			Start: ttlStart,
			End:   ttlEnd,
		},
	}

	// Modify the default capabilities we're working with by setting the bounds.
	val.Set(capability.Value(&v).Get())
	// Ensure getting the bounds returns the correct values.
	require.Equal(t, val.Get().GCTTLSeconds.Start, ttlStart)
	require.Equal(t, val.Get().GCTTLSeconds.End, ttlEnd)
}
