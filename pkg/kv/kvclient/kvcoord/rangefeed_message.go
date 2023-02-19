// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
