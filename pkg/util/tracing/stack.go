// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"go.opentelemetry.io/otel/attribute"
)

// String implements the stringer interface.
func (c *CapturedStack) String() string {
	age := c.Age.Seconds()
	stackPrefix := fmt.Sprintf("Op:%s, NodeID: %d, RecordedAt: %s", c.Op, c.NodeID, timeutil.FromUnixNanos(c.RecordedAt).String())
	if c.Stack == "" && c.SharedSuffix > 0 {
		return fmt.Sprintf("%s\nstack as of %.1fs ago had not changed from previous stack", stackPrefix, age)
	}
	if c.SharedLines > 0 {
		return fmt.Sprintf("%s\nstack as of %.1fs ago: %s\n ...+%d lines matching previous stack", stackPrefix, age, c.Stack, c.SharedLines)
	}
	return fmt.Sprintf("%s\nstack as of %.1fs ago: %s", stackPrefix, age, c.Stack)
}

var _ AggregatorEvent = &CapturedStack{}

// Render implements the AggregatorEvent interface.
//
// TODO(adityamaru): It does not make sense to render entire stacks as LazyTags
// on the aggregator span. As we move towards a world where AggregatorEvents are
// persisted and consumed by independent observability tools we may want to
// reconsider the use of LazyTags at all.
func (c *CapturedStack) Render() []attribute.KeyValue {
	return nil
}

// Identity implements the AggregatorEvent interface.
func (c *CapturedStack) Identity() AggregatorEvent {
	return &CapturedStack{}
}

// Combine implements the AggregatorEvent interface.
//
// CapturedStacks cannot be combined since they represent different stages of a
// goroutines execution. We already perform deduplication between stacks when
// capturing the stacks in MaybeFetchCapturedStackHistory. For this reason each
// CapturedStack has a unique tag as seen in the implementation of `Tag()`
// below. Therefore, we should never see two stacks being combined except for
// during initialization when the AggregatorEvent is combined with the
// Identity().
func (c *CapturedStack) Combine(other AggregatorEvent) {
	otherCapturedStack, ok := other.(*CapturedStack)
	if !ok {
		panic(fmt.Sprintf("`other` is not of type CapturedStack: %T", other))
	}
	c.Stack = otherCapturedStack.Stack
	c.Age = otherCapturedStack.Age
	c.SharedLines = otherCapturedStack.SharedLines
	c.SharedSuffix = otherCapturedStack.SharedSuffix
	c.Op = otherCapturedStack.Op
	c.NodeID = otherCapturedStack.NodeID
	c.RecordedAt = otherCapturedStack.RecordedAt
}

// Tag implements the AggregatorEvent interface.
//
// Each CapturedStack gets a unique timestamped tag, as we do not want to
// combine stacks that are collected during the execution.
func (c *CapturedStack) Tag() string {
	return fmt.Sprintf("CapturedStack-%d", timeutil.Now().UnixNano())
}
