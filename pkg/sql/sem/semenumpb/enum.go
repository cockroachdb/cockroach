// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package semenumpb

import "github.com/cockroachdb/redact"

var (
	_ redact.SafeValue = ForeignKeyAction(0)
	_ redact.SafeValue = TriggerActionTime(0)
	_ redact.SafeValue = TriggerEventType(0)
)

// SafeValue implements redact.SafeValue.
func (x ForeignKeyAction) SafeValue() {}

// SafeValue implements redact.SafeValue
func (TriggerActionTime) SafeValue() {}

// SafeValue implements redact.SafeValue
func (TriggerEventType) SafeValue() {}
