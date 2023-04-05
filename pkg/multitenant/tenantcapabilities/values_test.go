// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilities

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/stretchr/testify/require"
)

// TestIDs ensures that iterating IDs always works for the ID lookup functions.
func TestIDs(t *testing.T) {
	for _, id := range IDs {
		_, err := GetValueByID(DefaultCapabilities(), id)
		require.NoError(t, err, id)
		_, ok := FromID(id)
		require.True(t, ok, id)
	}
}

func TestGetSet(t *testing.T) {
	var v tenantcapabilitiespb.TenantCapabilities
	for _, id := range IDs {
		switch c, _ := FromID(id); c := c.(type) {
		case BoolCapability:
			c.Value(&v).Set(c.Value(DefaultCapabilities()).Get())
		case SpanConfigBoundsCapability:
			c.Value(&v).Set(c.Value(DefaultCapabilities()).Get())
		}
	}
}

// TestSpanConfigBoundsSetGet ensures calling Get on SpanConfigBounds that have
// been previously modified using a Set returns the correct value.
func TestSpanConfigBoundsSetGet(t *testing.T) {
	capability := spanConfigBoundsCapability(TenantSpanConfigBounds)
	val := capability.Value(DefaultCapabilities())

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
