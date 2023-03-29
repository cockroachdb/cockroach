// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigbounds

import (
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Reader maintains an in-memory view of the global SpanConfigBounds state.
//
// SpanConfigBounds are stored as tenant capabilities, the state of which is
// surfaced by the tenantcapabilities.Reader. BoundsReader serves as a narrow,
// adapter interface for the same.
type Reader interface {
	// Bounds returns span config bounds set for a given tenant. If no bounds have
	// been configured for the given tenant, found returns false.
	Bounds(tenID roachpb.TenantID) (_ Bounds, found bool)
}

type boundsReader struct {
	capabilitiesReader tenantcapabilities.Reader
}

// NewReader constructs and returns a new Reader.
func NewReader(reader tenantcapabilities.Reader) Reader {
	return &boundsReader{
		capabilitiesReader: reader,
	}
}

// Bounds implements the BoundsReader interface.
func (r *boundsReader) Bounds(tenID roachpb.TenantID) (_ Bounds, found bool) {
	capabilities, found := r.capabilitiesReader.GetCapabilities(tenID)
	if !found {
		return Bounds{}, false
	}

	boundspb := capabilities.Cap(tenantcapabilities.TenantSpanConfigBounds).Get().Unwrap().(*tenantcapabilitiespb.SpanConfigBounds)
	if boundspb == nil {
		return Bounds{}, false
	}
	return MakeBounds(boundspb), true
}

// NewEmptyReader returns a new Reader which corresponds to an empty span config
// bounds state. It's only intended for testing.
func NewEmptyReader() Reader {
	return emptyReader(true)
}

type emptyReader bool

// Bounds implements the Reader interface.
func (emptyReader) Bounds(roachpb.TenantID) (Bounds, bool) {
	return Bounds{}, false
}
