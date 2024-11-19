// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigkvsubscriber

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// noopKVSubscriber is a KVSubscriber that no-ops and is always up-to-date.
// Intended for tests that do not make use of the span configurations
// infrastructure.
type noopKVSubscriber struct {
	clock *hlc.Clock
}

var _ spanconfig.KVSubscriber = &noopKVSubscriber{}

// NewNoopSubscriber returns a new no-op KVSubscriber.
func NewNoopSubscriber(clock *hlc.Clock) spanconfig.KVSubscriber {
	return &noopKVSubscriber{
		clock: clock,
	}
}

// Subscribe is part of the spanconfig.KVSubsriber interface.
func (n *noopKVSubscriber) Subscribe(func(context.Context, roachpb.Span)) {}

// LastUpdated is part of the spanconfig.KVSubscriber interface.
func (n *noopKVSubscriber) LastUpdated() hlc.Timestamp {
	return n.clock.Now()
}

// NeedsSplit is part of the spanconfig.KVSubscriber interface.
func (n *noopKVSubscriber) NeedsSplit(context.Context, roachpb.RKey, roachpb.RKey) (bool, error) {
	return false, nil
}

// ComputeSplitKey is part of the spanconfig.KVSubscriber interface.
func (n *noopKVSubscriber) ComputeSplitKey(
	context.Context, roachpb.RKey, roachpb.RKey,
) (roachpb.RKey, error) {
	return roachpb.RKey{}, nil
}

// GetSpanConfigForKey is part of the spanconfig.KVSubscriber interface.
func (n *noopKVSubscriber) GetSpanConfigForKey(
	context.Context, roachpb.RKey,
) (roachpb.SpanConfig, roachpb.Span, error) {
	return roachpb.SpanConfig{}, roachpb.Span{}, nil
}

// GetProtectionTimestamps is part of the spanconfig.KVSubscriber interface.
func (n *noopKVSubscriber) GetProtectionTimestamps(
	context.Context, roachpb.Span,
) ([]hlc.Timestamp, hlc.Timestamp, error) {
	return nil, n.LastUpdated(), nil
}
