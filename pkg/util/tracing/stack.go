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

func (c *CapturedStack) String() string {
	age := c.Age.Seconds()
	if c.Stack == "" && c.SharedSuffix > 0 {
		return fmt.Sprintf("stack as of %.1fs ago had not changed from previous stack", age)
	}
	if c.SharedLines > 0 {
		return fmt.Sprintf("stack as of %.1fs ago: %s\n ...+%d lines matching previous stack", age, c.Stack, c.SharedLines)
	}
	return fmt.Sprintf("stack as of %.1fs ago: %s", age, c.Stack)
}

var _ AggregatorEvent = &CapturedStack{}

func (c *CapturedStack) Render() []attribute.KeyValue {
	return nil
}

func (c *CapturedStack) Identity() AggregatorEvent {
	return &CapturedStack{}
}

func (c *CapturedStack) Combine(other AggregatorEvent) {
	otherCapturedStack, ok := other.(*CapturedStack)
	if !ok {
		panic(fmt.Sprintf("`other` is not of type CapturedStack: %T", other))
	}
	c.Stack = otherCapturedStack.Stack
	c.Age = otherCapturedStack.Age
	c.SharedLines = otherCapturedStack.SharedLines
	c.SharedSuffix = otherCapturedStack.SharedSuffix
}

func (c *CapturedStack) Tag() string {
	return fmt.Sprintf("CapturedStack-%d", timeutil.Now().UnixNano())
}
