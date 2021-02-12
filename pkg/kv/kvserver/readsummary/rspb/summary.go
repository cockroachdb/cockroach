// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rspb

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// FromTimestamp constructs a read summary from the provided timestamp, treating
// the argument as the low water mark of each segment in the summary.
func FromTimestamp(ts hlc.Timestamp) ReadSummary {
	seg := Segment{LowWater: ts}
	return ReadSummary{
		Local:  seg,
		Global: seg,
	}
}

// Clone performs a deep-copy of the receiver.
func (c ReadSummary) Clone() *ReadSummary {
	// NOTE: When ReadSummary is updated to include pointers to non-contiguous
	// memory, this will need to be updated.
	return &c
}

// Merge combines two read summaries, resulting in a single summary that
// reflects the combination of all reads in each original summary. The merge
// operation is commutative and idempotent.
func (c *ReadSummary) Merge(o ReadSummary) {
	c.Local.merge(o.Local)
	c.Global.merge(o.Global)
}

func (c *Segment) merge(o Segment) {
	c.LowWater.Forward(o.LowWater)
}

// AssertNoRegression asserts that all reads in the parameter's summary are
// reflected in the receiver's summary with at least as high of a timestamp.
func (c *ReadSummary) AssertNoRegression(ctx context.Context, o ReadSummary) {
	c.Local.assertNoRegression(ctx, o.Local, "local")
	c.Global.assertNoRegression(ctx, o.Global, "global")
}

func (c *Segment) assertNoRegression(ctx context.Context, o Segment, name string) {
	if c.LowWater.Less(o.LowWater) {
		log.Fatalf(ctx, "read summary regression in %s segment, was %s, now %s",
			name, o.LowWater, c.LowWater)
	}
}

// Ignore unused warning.
var _ = (*ReadSummary).AssertNoRegression
