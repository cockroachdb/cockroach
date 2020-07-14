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

import "fmt"

// ToStats converts the receiver to an MVCCStats.
func (ms *MVCCStatsDelta) ToStats() MVCCStats {
	s := MVCCStats{
		ContainsEstimates: ms.ContainsEstimates,
		LastUpdateNanos:   ms.LastUpdateNanos,
		IntentAge:         ms.IntentAge,
		GCBytesAge:        ms.GCBytesAge,
		LiveBytes:         ms.LiveBytes,
		LiveCount:         ms.LiveCount,
		KeyBytes:          ms.KeyBytes,
		KeyCount:          ms.KeyCount,
		ValBytes:          ms.ValBytes,
		ValCount:          ms.ValCount,
		IntentBytes:       ms.IntentBytes,
		IntentCount:       ms.IntentCount,
		SysBytes:          ms.SysBytes,
		SysCount:          ms.SysCount,
		AbortSpanBytes:    0,
	}
	return s
}

// ToStatsDelta converts the receiver to an MVCCStatsDelta.
func (ms *MVCCStats) ToStatsDelta() MVCCStatsDelta {
	s := MVCCStatsDelta{
		ContainsEstimates: ms.ContainsEstimates,
		LastUpdateNanos:   ms.LastUpdateNanos,
		IntentAge:         ms.IntentAge,
		GCBytesAge:        ms.GCBytesAge,
		LiveBytes:         ms.LiveBytes,
		LiveCount:         ms.LiveCount,
		KeyBytes:          ms.KeyBytes,
		KeyCount:          ms.KeyCount,
		ValBytes:          ms.ValBytes,
		ValCount:          ms.ValCount,
		IntentBytes:       ms.IntentBytes,
		IntentCount:       ms.IntentCount,
		SysBytes:          ms.SysBytes,
		SysCount:          ms.SysCount,
	}
	return s
}

// ToStats converts the receiver to an MVCCStats.
func (ms *MVCCPersistentStats) ToStats() MVCCStats {
	s := MVCCStats{
		ContainsEstimates: ms.ContainsEstimates,
		LastUpdateNanos:   ms.LastUpdateNanos,
		IntentAge:         ms.IntentAge,
		GCBytesAge:        ms.GCBytesAge,
		LiveBytes:         ms.LiveBytes,
		LiveCount:         ms.LiveCount,
		KeyBytes:          ms.KeyBytes,
		KeyCount:          ms.KeyCount,
		ValBytes:          ms.ValBytes,
		ValCount:          ms.ValCount,
		IntentBytes:       ms.IntentBytes,
		IntentCount:       ms.IntentCount,
		SysBytes:          ms.SysBytes,
		SysCount:          ms.SysCount,
		AbortSpanBytes:    0,
	}
	return s
}

// ToPersistentStats converts the receiver to an MVCCPersistentStats.
func (ms *MVCCStats) ToPersistentStats() MVCCPersistentStats {
	s := MVCCPersistentStats{
		ContainsEstimates: ms.ContainsEstimates,
		LastUpdateNanos:   ms.LastUpdateNanos,
		IntentAge:         ms.IntentAge,
		GCBytesAge:        ms.GCBytesAge,
		LiveBytes:         ms.LiveBytes,
		LiveCount:         ms.LiveCount,
		KeyBytes:          ms.KeyBytes,
		KeyCount:          ms.KeyCount,
		ValBytes:          ms.ValBytes,
		ValCount:          ms.ValCount,
		IntentBytes:       ms.IntentBytes,
		IntentCount:       ms.IntentCount,
		SysBytes:          ms.SysBytes,
		SysCount:          ms.SysCount,
	}
	return s
}

// MustSetValue is like SetValue, except it resets the enum and panics if the
// provided value is not a valid variant type.
func (op *MVCCLogicalOp) MustSetValue(value interface{}) {
	op.Reset()
	if !op.SetValue(value) {
		panic(fmt.Sprintf("%T excludes %T", op, value))
	}
}
