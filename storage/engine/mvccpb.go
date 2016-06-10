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
//
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package engine

import (
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
)

// GCBytes is a convenience function which returns the number of gc bytes,
// that is the key and value bytes excluding the live bytes.
func (ms MVCCStats) GCBytes() int64 {
	return ms.KeyBytes + ms.ValBytes - ms.LiveBytes
}

// AgeTo encapsulates the complexity of computing the increment in age
// quantities contained in MVCCStats. Two MVCCStats structs only add and
// subtract meaningfully if their LastUpdateNanos matches, so aging them to
// the max of their LastUpdateNanos is a prerequisite.
// If nowNanos is behind ms.LastUpdateNanos, this method is a noop.
func (ms *MVCCStats) AgeTo(nowNanos int64) {
	if ms.LastUpdateNanos >= nowNanos {
		return
	}
	diffSeconds := nowNanos/1E9 - ms.LastUpdateNanos/1E9 // not (...)/1E9!

	ms.GCBytesAge += ms.GCBytes() * diffSeconds
	ms.IntentAge += ms.IntentCount * diffSeconds
	ms.LastUpdateNanos = nowNanos
}

// Add adds values from oms to ms. The ages will be moved forward to the
// larger of the LastUpdateNano timestamps involved.
func (ms *MVCCStats) Add(oms MVCCStats) {
	// Enforce the max LastUpdateNanos for both ages based on their
	// pre-addition state.
	ms.AgeTo(oms.LastUpdateNanos)
	oms.AgeTo(ms.LastUpdateNanos)
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
	ms.AgeTo(oms.LastUpdateNanos)
	oms.AgeTo(ms.LastUpdateNanos)
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

// AccountForSelf adjusts ms to account for the predicted impact it will have on
// the values that it records when the structure is initially stored. Specifically,
// MVCCStats is stored on the RangeStats key, which means that its creation will
// have an impact on system-local data size and key count.
func (ms *MVCCStats) AccountForSelf(rangeID roachpb.RangeID) error {
	key := keys.RangeStatsKey(rangeID)
	metaKey := MakeMVCCMetadataKey(key)

	// MVCCStats is stored inline, so compute MVCCMetadata accordingly.
	value := roachpb.Value{}
	if err := value.SetProto(ms); err != nil {
		return err
	}
	meta := MVCCMetadata{RawBytes: value.RawBytes}

	updateStatsForInline(ms, key, 0, 0, int64(metaKey.EncodedSize()), int64(meta.Size()))
	return nil
}

// Value returns the inline value.
func (meta MVCCMetadata) Value() roachpb.Value {
	return roachpb.Value{RawBytes: meta.RawBytes}
}

// IsInline returns true if the value is inlined in the metadata.
func (meta MVCCMetadata) IsInline() bool {
	return meta.RawBytes != nil
}

// IsIntentOf returns true if the meta record is an intent of the supplied
// transaction.
func (meta MVCCMetadata) IsIntentOf(txn *roachpb.Transaction) bool {
	return meta.Txn != nil && txn != nil && roachpb.TxnIDEqual(meta.Txn.ID, txn.ID)
}
