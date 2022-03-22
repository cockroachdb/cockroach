// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
func (n *noopKVSubscriber) NeedsSplit(context.Context, roachpb.RKey, roachpb.RKey) bool {
	return false
}

// ComputeSplitKey is part of the spanconfig.KVSubscriber interface.
func (n *noopKVSubscriber) ComputeSplitKey(
	context.Context, roachpb.RKey, roachpb.RKey,
) roachpb.RKey {
	return roachpb.RKey{}
}

// GetSpanConfigForKey is part of the spanconfig.KVSubscriber interface.
func (n *noopKVSubscriber) GetSpanConfigForKey(
	context.Context, roachpb.RKey,
) (roachpb.SpanConfig, error) {
	return roachpb.SpanConfig{}, nil
}

// GetProtectionTimestamps is part of the spanconfig.KVSubscriber interface.
func (n *noopKVSubscriber) GetProtectionTimestamps(
	context.Context, roachpb.Span,
) ([]hlc.Timestamp, hlc.Timestamp, error) {
	return nil, n.LastUpdated(), nil
}
