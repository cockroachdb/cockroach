// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// RangeFeedMessage is a type that encapsulates the kvpb.RangeFeedEvent.
type RangeFeedMessage struct {

	// RangeFeed event this message holds.
	*kvpb.RangeFeedEvent

	// The span of the rangefeed registration that overlaps with the key or span
	// in the RangeFeed event.
	RegisteredSpan roachpb.Span
}
