// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package enginepb

// Short returns a prefix of the transaction's ID.
func (t TxnMeta) Short() string {
	return t.ID.Short()
}

// Total returns the range size as the sum of the key and value
// bytes. This includes all non-live keys and all versioned values.
func (ms MVCCStats) Total() int64 {
	return ms.KeyBytes + ms.ValBytes
}

// GCBytes is a convenience function which returns the number of gc bytes,
// that is the key and value bytes excluding the live bytes.
func (ms MVCCStats) GCBytes() int64 {
	return ms.KeyBytes + ms.ValBytes - ms.LiveBytes
}

// AvgIntentAge returns the average age of outstanding intents,
// based on current wall time specified via nowNanos.
func (ms MVCCStats) AvgIntentAge(nowNanos int64) float64 {
	if ms.IntentCount == 0 {
		return 0
	}
	// Advance age by any elapsed time since last computed. Note that
	// we operate on a copy.
	ms.AgeTo(nowNanos)
	return float64(ms.IntentAge) / float64(ms.IntentCount)
}

// GCByteAge returns the total age of outstanding gc'able
// bytes, based on current wall time specified via nowNanos.
// nowNanos is ignored if it's a past timestamp as seen by
// rs.LastUpdateNanos.
func (ms MVCCStats) GCByteAge(nowNanos int64) int64 {
	ms.AgeTo(nowNanos) // we operate on a copy
	return ms.GCBytesAge
}

// Forward is like AgeTo, but if nowNanos is not ahead of ms.LastUpdateNanos,
// this method is a noop.
func (ms *MVCCStats) Forward(nowNanos int64) {
	if ms.LastUpdateNanos >= nowNanos {
		return
	}
	ms.AgeTo(nowNanos)
}

// AgeTo encapsulates the complexity of computing the increment in age
// quantities contained in MVCCStats. Two MVCCStats structs only add and
// subtract meaningfully if their LastUpdateNanos matches, so aging them to
// the max of their LastUpdateNanos is a prerequisite, though Add() takes
// care of this internally.
func (ms *MVCCStats) AgeTo(nowNanos int64) {
	// Seconds are counted every time each individual nanosecond timestamp
	// crosses a whole second boundary (i.e. is zero mod 1E9). Thus it would
	// be a mistake to use the (nonequivalent) expression (a-b)/1E9.
	diffSeconds := nowNanos/1E9 - ms.LastUpdateNanos/1E9

	ms.GCBytesAge += ms.GCBytes() * diffSeconds
	ms.IntentAge += ms.IntentCount * diffSeconds
	ms.LastUpdateNanos = nowNanos
}

// Add adds values from oms to ms. The ages will be moved forward to the
// larger of the LastUpdateNano timestamps involved.
func (ms *MVCCStats) Add(oms MVCCStats) {
	// Enforce the max LastUpdateNanos for both ages based on their
	// pre-addition state.
	ms.Forward(oms.LastUpdateNanos)
	oms.Forward(ms.LastUpdateNanos) // on local copy
	// If either stats object contains estimates, their sum does too.
	ms.ContainsEstimates = ms.ContainsEstimates || oms.ContainsEstimates
	// Now that we've done that, we may just add them.
	ms.IntentAge += oms.IntentAge
	ms.GCBytesAge += oms.GCBytesAge
	ms.LiveBytes += oms.LiveBytes
	ms.KeyBytes += oms.KeyBytes
	ms.ValBytes += oms.ValBytes
	ms.IntentBytes += oms.IntentBytes
	ms.LiveCount += oms.LiveCount
	ms.KeyCount += oms.KeyCount
	ms.ValCount += oms.ValCount
	ms.IntentCount += oms.IntentCount
	ms.SysBytes += oms.SysBytes
	ms.SysCount += oms.SysCount
}

// Subtract removes oms from ms. The ages will be moved forward to the larger of
// the LastUpdateNano timestamps involved.
func (ms *MVCCStats) Subtract(oms MVCCStats) {
	// Enforce the max LastUpdateNanos for both ages based on their
	// pre-subtraction state.
	ms.Forward(oms.LastUpdateNanos)
	oms.Forward(ms.LastUpdateNanos)
	// If either stats object contains estimates, their difference does too.
	ms.ContainsEstimates = ms.ContainsEstimates || oms.ContainsEstimates
	// Now that we've done that, we may subtract.
	ms.IntentAge -= oms.IntentAge
	ms.GCBytesAge -= oms.GCBytesAge
	ms.LiveBytes -= oms.LiveBytes
	ms.KeyBytes -= oms.KeyBytes
	ms.ValBytes -= oms.ValBytes
	ms.IntentBytes -= oms.IntentBytes
	ms.LiveCount -= oms.LiveCount
	ms.KeyCount -= oms.KeyCount
	ms.ValCount -= oms.ValCount
	ms.IntentCount -= oms.IntentCount
	ms.SysBytes -= oms.SysBytes
	ms.SysCount -= oms.SysCount
}

// IsInline returns true if the value is inlined in the metadata.
func (meta MVCCMetadata) IsInline() bool {
	return meta.RawBytes != nil
}
