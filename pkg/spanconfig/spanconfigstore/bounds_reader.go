// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigstore

import (
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigbounds"
)

// BoundsReader maintains an in-memory view of the global SpanConfigBounds state.
//
// SpanConfigBounds are stored as tenant capabilities, the state of which is
// surfaced by the tenantcapabilities.Reader. BoundsReader serves as a narrow,
// adapter interface for the same.
type BoundsReader interface {
	// Bounds returns span config bounds set for a given tenant. If no bounds have
	// been configured for the given tenant, found returns false.
	Bounds(tenID roachpb.TenantID) (_ *spanconfigbounds.Bounds, found bool)
}

type boundsReader struct {
	capabilitiesReader tenantcapabilities.Reader
}

// NewBoundsReader constructs and returns a new BoundsReader.
func NewBoundsReader(reader tenantcapabilities.Reader) BoundsReader {
	return &boundsReader{
		capabilitiesReader: reader,
	}
}

// Bounds implements the BoundsReader interface.
func (r *boundsReader) Bounds(tenID roachpb.TenantID) (_ *spanconfigbounds.Bounds, found bool) {
	capabilities, found := r.capabilitiesReader.GetCapabilities(tenID)
	if !found {
		return nil, false
	}

	b := tenantcapabilities.MustGetValueByID(
		capabilities, tenantcapabilities.TenantSpanConfigBounds,
	).(tenantcapabilities.SpanConfigBoundValue).Get()
	return b, b != nil
}

// NewEmptyBoundsReader returns a new Reader which corresponds to an empty span config
// bounds state. It's only intended for testing.
func NewEmptyBoundsReader() BoundsReader {
	return emptyReader(true)
}

type emptyReader bool

// Bounds implements the Reader interface.
func (emptyReader) Bounds(roachpb.TenantID) (*spanconfigbounds.Bounds, bool) {
	return nil, false
}
