// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
)

const (
	mvccLogicalOp      = int64(unsafe.Sizeof(enginepb.MVCCLogicalOp{}))
	mvccWriteValueOp   = int64(unsafe.Sizeof(enginepb.MVCCWriteValueOp{}))
	mvccDeleteRangeOp  = int64(unsafe.Sizeof(enginepb.MVCCDeleteRangeOp{}))
	mvccWriteIntentOp  = int64(unsafe.Sizeof(enginepb.MVCCWriteIntentOp{}))
	mvccUpdateIntentOp = int64(unsafe.Sizeof(enginepb.MVCCUpdateIntentOp{}))
	mvccCommitIntentOp = int64(unsafe.Sizeof(enginepb.MVCCCommitIntentOp{}))
	mvccAbortIntentOp  = int64(unsafe.Sizeof(enginepb.MVCCAbortIntentOp{}))
	mvccAbortTxnOp     = int64(unsafe.Sizeof(enginepb.MVCCAbortTxnOp{}))
)

const (
	eventOverhead = int64(unsafe.Sizeof(&event{})) + int64(unsafe.Sizeof(event{}))
)

const (
	sharedEventPtrOverhead  = int64(unsafe.Sizeof(&sharedEvent{}))
	sharedEventOverhead     = int64(unsafe.Sizeof(sharedEvent{}))
	sharedEventBaseOverhead = sharedEventPtrOverhead + sharedEventOverhead
)

const (
	rangeFeedEventOverhead     = int64(unsafe.Sizeof(kvpb.RangeFeedEvent{}))
	allocEventOverhead         = int64(unsafe.Sizeof(SharedBudgetAllocation{}))
	rangeFeedEventBaseOverhead = rangeFeedEventOverhead + allocEventOverhead
)

const (
	rangefeedValueOverhead       = int64(unsafe.Sizeof(kvpb.RangeFeedValue{}))
	rangefeedDeleteRangeOverhead = int64(unsafe.Sizeof(kvpb.RangeFeedDeleteRange{}))
	rangefeedCheckpointOverhead  = int64(unsafe.Sizeof(kvpb.RangeFeedCheckpoint{}))
	rangefeedSSTTableOverhead    = int64(unsafe.Sizeof(kvpb.RangeFeedSSTable{}))
)

const (
	sstEventOverhead  = int64(unsafe.Sizeof(sstEvent{}))
	syncEventOverhead = int64(unsafe.Sizeof(syncEvent{}))
)

func TotalSharedEventOverhead(numOfRegistrations int) int64 {
	return sharedEventBaseOverhead * int64(numOfRegistrations)
}

func UnderlyingDataSize(op enginepb.MVCCLogicalOp) (totalAmount int64) {
	switch t := op.GetValue().(type) {
	case *enginepb.MVCCWriteValueOp:
		// Publish the new value directly. Do not expect this to be called.
	case *enginepb.MVCCDeleteRangeOp:
		// Publish the range deletion directly. Do not expect this to be called.
	case *enginepb.MVCCWriteIntentOp:
		// No updates to publish.
		return int64(cap(t.TxnID)) + int64(cap(t.TxnKey))
	case *enginepb.MVCCUpdateIntentOp:
		// No updates to publish.
		return int64(cap(t.TxnID))
	case *enginepb.MVCCCommitIntentOp:
		// Publish the newly committed value. Do not expect this to be called.
	case *enginepb.MVCCAbortIntentOp:
		// No updates to publish.
		return int64(cap(t.TxnID))
	case *enginepb.MVCCAbortTxnOp:
		// No updates to publish.
		return int64(cap(t.TxnID))
	}
	return 0
}

func (ops opsEvent) underlyingData() (totalAmount int64) {
	for _, op := range ops {
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			totalAmount += int64(cap(t.Key)) + int64(cap(t.Value)) + int64(cap(t.PrevValue))
		case *enginepb.MVCCDeleteRangeOp:
			totalAmount += int64(cap(t.StartKey)) + int64(cap(t.EndKey))
		case *enginepb.MVCCWriteIntentOp:
			totalAmount += int64(cap(t.TxnID)) + int64(cap(t.TxnKey))
		case *enginepb.MVCCUpdateIntentOp:
			totalAmount += int64(cap(t.TxnID))
		case *enginepb.MVCCCommitIntentOp:
			totalAmount += int64(cap(t.TxnID)) + int64(cap(t.Key)) + int64(cap(t.Value)) + int64(cap(t.PrevValue))
		case *enginepb.MVCCAbortIntentOp:
			totalAmount += int64(cap(t.TxnID))
		case *enginepb.MVCCAbortTxnOp:
			totalAmount += int64(cap(t.TxnID))
		}
	}
	return totalAmount
}

func (ops opsEvent) baseOverhead() int64 {
	totalAmount := mvccLogicalOp * int64(cap(ops))
	for _, op := range ops {
		switch op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			totalAmount += mvccWriteValueOp
		case *enginepb.MVCCDeleteRangeOp:
			totalAmount += mvccDeleteRangeOp
		case *enginepb.MVCCWriteIntentOp:
			totalAmount += mvccWriteIntentOp
		case *enginepb.MVCCUpdateIntentOp:
			totalAmount += mvccUpdateIntentOp
		case *enginepb.MVCCCommitIntentOp:
			totalAmount += mvccCommitIntentOp
		case *enginepb.MVCCAbortIntentOp:
			totalAmount += mvccAbortIntentOp
		case *enginepb.MVCCAbortTxnOp:
			totalAmount += mvccAbortTxnOp
		}
	}
	return totalAmount
}

func (ops opsEvent) rangefeedEventOverhead(numOfRegistrations int) (totalAmount int64) {
	totalSharedEventOverhead := TotalSharedEventOverhead(numOfRegistrations)
	eachRangefeedEventOverhead := totalSharedEventOverhead + rangeFeedEventBaseOverhead
	for _, op := range ops {
		switch op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			totalAmount += eachRangefeedEventOverhead + rangefeedValueOverhead
		case *enginepb.MVCCDeleteRangeOp:
			totalAmount += eachRangefeedEventOverhead + rangefeedDeleteRangeOverhead
		case *enginepb.MVCCWriteIntentOp:
			// No updates to publish in consumeLogicalOps.
			totalAmount += eachRangefeedEventOverhead
		case *enginepb.MVCCUpdateIntentOp:
			// No updates to publish in consumeLogicalOps.
			totalAmount += eachRangefeedEventOverhead
		case *enginepb.MVCCCommitIntentOp:
			totalAmount += eachRangefeedEventOverhead + rangefeedValueOverhead
		case *enginepb.MVCCAbortIntentOp:
			// No updates to publish in consumeLogicalOps.
			totalAmount += eachRangefeedEventOverhead
		case *enginepb.MVCCAbortTxnOp:
			// No updates to publish in consumeLogicalOps.
			totalAmount += eachRangefeedEventOverhead
		}
		totalAmount += TotalCheckpointEventOverhead(numOfRegistrations)
	}
	return totalAmount
}

func (e *event) EventBaseOverhead() int64 {
	switch {
	case e.ops != nil:
		return eventOverhead + e.ops.baseOverhead()
	case !e.ct.IsEmpty():
		return eventOverhead
	case bool(e.initRTS):
		return eventOverhead
	case e.sst != nil:
		return eventOverhead + sstEventOverhead
	case e.sync != nil:
		return eventOverhead + syncEventOverhead
	}
	return 0
}

func TotalCheckpointEventOverhead(numOfRegistrations int) int64 {
	totalSharedEventOverhead := TotalSharedEventOverhead(numOfRegistrations)
	eachRangefeedEventOverhead := totalSharedEventOverhead + rangeFeedEventBaseOverhead
	return eachRangefeedEventOverhead + rangefeedCheckpointOverhead
}

func (e *event) RangefeedEventOverhead(numOfRegistrations int) int64 {
	totalSharedEventOverhead := TotalSharedEventOverhead(numOfRegistrations)
	eachRangefeedEventOverhead := totalSharedEventOverhead + rangeFeedEventBaseOverhead
	switch {
	case e.ops != nil:
		return e.ops.rangefeedEventOverhead(numOfRegistrations)
	case !e.ct.IsEmpty():
		return TotalCheckpointEventOverhead(numOfRegistrations)
	case bool(e.initRTS):
		return TotalCheckpointEventOverhead(numOfRegistrations)
	case e.sst != nil:
		return eachRangefeedEventOverhead + rangefeedSSTTableOverhead
	case e.sync != nil:
		return 0
	}
	return 0
}

func (e *event) UnderlyingEventData() int64 {
	switch {
	case e.ops != nil:
		return e.ops.underlyingData()
	case !e.ct.IsEmpty():
		return 0
	case bool(e.initRTS):
		return 0
	case e.sst != nil:
		return int64(cap(e.sst.data)) + int64(cap(e.sst.span.Key)) + int64(cap(e.sst.span.EndKey))
	case e.sync != nil:
		return 0
	}
	return 0
}

func (e *event) MemoryToAllocate(numOfRegistrations int) int64 {
	return e.EventBaseOverhead() + e.RangefeedEventOverhead(numOfRegistrations) + e.UnderlyingEventData()
}
