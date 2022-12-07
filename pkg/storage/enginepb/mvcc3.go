// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package enginepb

import (
	_ "github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil" // see MVCCValueHeader
	"github.com/cockroachdb/errors"
)

// SafeValue implements the redact.SafeValue interface.
func (MVCCStatsDelta) SafeValue() {}

// ToStats converts the receiver to an MVCCStats.
func (ms *MVCCStatsDelta) ToStats() MVCCStats {
	return MVCCStats(*ms)
}

// ToStatsDelta converts the receiver to an MVCCStatsDelta.
func (ms *MVCCStats) ToStatsDelta() MVCCStatsDelta {
	return MVCCStatsDelta(*ms)
}

// ToStats converts the receiver to an MVCCStats.
func (ms *MVCCPersistentStats) ToStats() MVCCStats {
	return MVCCStats(*ms)
}

// ToStatsPtr converts the receiver to a *MVCCStats.
func (ms *MVCCPersistentStats) ToStatsPtr() *MVCCStats {
	return (*MVCCStats)(ms)
}

// SafeValue implements the redact.SafeValue interface.
func (ms *MVCCStats) SafeValue() {}

// ToPersistentStats converts the receiver to an MVCCPersistentStats.
func (ms *MVCCStats) ToPersistentStats() MVCCPersistentStats {
	return MVCCPersistentStats(*ms)
}

// MustSetValue is like SetValue, except it resets the enum and panics if the
// provided value is not a valid variant type.
func (op *MVCCLogicalOp) MustSetValue(value interface{}) {
	op.Reset()
	if !op.SetValue(value) {
		panic(errors.AssertionFailedf("%T excludes %T", op, value))
	}
}
