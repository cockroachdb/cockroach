// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

	eventOverhead = int64(unsafe.Sizeof(&event{})) + int64(unsafe.Sizeof(event{}))

	sstEventOverhead  = int64(unsafe.Sizeof(sstEvent{}))
	syncEventOverhead = int64(unsafe.Sizeof(syncEvent{}))

	// futureEventBaseOverhead accounts for the base struct overhead of
	// sharedEvent{} and its pointer. Each sharedEvent contains a
	// *kvpb.RangeFeedEvent and *SharedBudgetAllocation. futureEventBaseOverhead
	// also accounts for the underlying base struct memory of RangeFeedEvent and
	// SharedBudgetAllocation. Underlying data for SharedBudgetAllocation includes
	// a pointer to the FeedBudget, but that points to the same data structure
	// across all rangefeeds, so we opted out in the calculation.
	sharedEventPtrOverhead  = int64(unsafe.Sizeof(&sharedEvent{}))
	sharedEventOverhead     = int64(unsafe.Sizeof(sharedEvent{}))
	rangeFeedEventOverhead  = int64(unsafe.Sizeof(kvpb.RangeFeedEvent{}))
	allocEventOverhead      = int64(unsafe.Sizeof(SharedBudgetAllocation{}))
	futureEventBaseOverhead = sharedEventPtrOverhead + sharedEventOverhead + rangeFeedEventOverhead + allocEventOverhead

	rangefeedValueOverhead       = int64(unsafe.Sizeof(kvpb.RangeFeedValue{}))
	rangefeedDeleteRangeOverhead = int64(unsafe.Sizeof(kvpb.RangeFeedDeleteRange{}))
	rangefeedCheckpointOverhead  = int64(unsafe.Sizeof(kvpb.RangeFeedCheckpoint{}))
	rangefeedSSTTableOverhead    = int64(unsafe.Sizeof(kvpb.RangeFeedSSTable{}))
)

// No future memory usages have been accounted so far.
// rangefeedSSTTableOpMemUsage accounts for the entire memory usage of a new
// RangeFeedSSTable event.
func rangefeedSSTTableOpMemUsage(data []byte, startKey, endKey roachpb.Key) int64 {
	// Pointer to RangeFeedSSTable has already been accounted in
	// futureEventBaseOverhead as part of the base struct overhead of
	// RangeFeedEvent. rangefeedValueOverhead includes the memory usage of the
	// underlying RangeFeedSSTable base struct.
	memUsage := futureEventBaseOverhead + rangefeedSSTTableOverhead

	// RangeFeedSSTable has Data, Span{startKey,endKey}, and WriteTS. Only Data,
	// startKey, and endKey has underlying memory usage in []byte. Timestamp and
	// other base structs of Value have no underlying data and are already
	// accounted in rangefeedValueOverhead.
	memUsage += int64(cap(data))
	memUsage += int64(cap(startKey))
	memUsage += int64(cap(endKey))
	return memUsage
}

// No future memory usages have been accounted so far.
// rangefeedCheckpointOpMemUsage accounts for the entire memory usage of a new
// RangeFeedCheckpoint event.
func rangefeedCheckpointOpMemUsage() int64 {
	// Pointer to RangeFeedCheckpoint has already been accounted in
	// futureEventBaseOverhead as part of the base struct overhead of
	// RangeFeedEvent. rangefeedCheckpointOverhead includes the memory usage of
	// the underlying RangeFeedCheckpoint base struct.

	// RangeFeedCheckpoint has Span{p.Span} and Timestamp{rts.Get()}. Timestamp is
	// already accounted in rangefeedCheckpointOverhead. Ignore bytes under
	// checkpoint.span here since it comes from p.Span which always points at the
	// same underlying data.
	return futureEventBaseOverhead + rangefeedCheckpointOverhead
}

// Pointer to the MVCCWriteValueOp was already accounted in mvccLogicalOp in the
// caller. writeValueOpMemUsage accounts for the memory usage of
// MVCCWriteValueOp.
func writeValueOpMemUsage(key roachpb.Key, value, prevValue []byte) int64 {
	// MVCCWriteValueOp has Key, Timestamp, Value, PrevValue, OmitInRangefeeds.
	// Only key, value, and prevValue has underlying memory usage in []byte.
	// Timestamp and OmitInRangefeeds have no underlying data and are already
	// accounted in MVCCWriteValueOp.
	currMemUsage := mvccWriteValueOp
	currMemUsage += int64(cap(key))
	currMemUsage += int64(cap(value))
	currMemUsage += int64(cap(prevValue))
	return currMemUsage
}

// Pointer to the MVCCDeleteRangeOp was already accounted in mvccLogicalOp in
// the caller. deleteRangeOpMemUsage accounts for the memory usage of
// MVCCDeleteRangeOp.
func deleteRangeOpMemUsage(startKey, endKey roachpb.Key) int64 {
	// MVCCDeleteRangeOp has StartKey, EndKey, and Timestamp. Only StartKey and
	// EndKey has underlying memory usage in []byte. Timestamp has no underlying
	// data and was already accounted in MVCCDeleteRangeOp.
	currMemUsage := mvccDeleteRangeOp
	currMemUsage += int64(cap(startKey))
	currMemUsage += int64(cap(endKey))
	return currMemUsage
}

// Pointer to the MVCCWriteIntentOp was already accounted in mvccLogicalOp in
// the caller. writeIntentOpMemUsage accounts for the memory usage of
// MVCCWriteIntentOp.
func writeIntentOpMemUsage(txnID uuid.UUID, txnKey []byte) int64 {
	// MVCCWriteIntentOp has TxnID, TxnKey, TxnIsoLevel, TxnMinTimestamp, and
	// Timestamp. Only TxnID and TxnKey has underlying memory usage in []byte.
	// TxnIsoLevel, TxnMinTimestamp, and Timestamp have no underlying data and was
	// already accounted in MVCCWriteIntentOp.
	currMemUsage := mvccWriteIntentOp
	currMemUsage += int64(cap(txnID))
	currMemUsage += int64(cap(txnKey))
	return currMemUsage
}

// Pointer to the MVCCUpdateIntentOp was already accounted in mvccLogicalOp in
// the caller. updateIntentOpMemUsage accounts for the memory usage of
// MVCCUpdateIntentOp.
func updateIntentOpMemUsage(txnID uuid.UUID) int64 {
	// MVCCUpdateIntentOp has TxnID and Timestamp. Only TxnID has underlying
	// memory usage in []byte. Timestamp has no underlying data and was already
	// accounted in MVCCUpdateIntentOp.
	currMemUsage := mvccUpdateIntentOp
	currMemUsage += int64(cap(txnID))
	return currMemUsage
}

// Pointer to the MVCCCommitIntentOp was already accounted in mvccLogicalOp in
// the caller. commitIntentOpMemUsage accounts for the memory usage of
// MVCCCommitIntentOp.
func commitIntentOpMemUsage(txnID uuid.UUID, key []byte, value []byte, prevValue []byte) int64 {
	// MVCCCommitIntentOp has TxnID, Key, Timestamp, Value, PrevValue, and
	// OmintInRangefeeds. Only TxnID, Key, Value, and PrevValue has underlying
	// memory usage in []byte. Timestamp and OmintInRangefeeds have no underlying
	// data and was already accounted in MVCCCommitIntentOp.
	currMemUsage := mvccCommitIntentOp
	currMemUsage += int64(cap(txnID))
	currMemUsage += int64(cap(key))
	currMemUsage += int64(cap(value))
	currMemUsage += int64(cap(prevValue))
	return currMemUsage
}

// Pointer to the MVCCAbortIntentOp was already accounted in mvccLogicalOp in
// the caller. abortIntentOpMemUsage accounts for the memory usage of
// MVCCAbortIntentOp.
func abortIntentOpMemUsage(txnID uuid.UUID) int64 {
	// MVCCAbortIntentOp has TxnID which has underlying memory usage in []byte.
	currMemUsage := mvccAbortIntentOp
	currMemUsage += int64(cap(txnID))
	return currMemUsage
}

// Pointer to the MVCCAbortTxnOp was already accounted in mvccLogicalOp in the
// caller. abortTxnOpMemUsage accounts for the memory usage of MVCCAbortTxnOp.
func abortTxnOpMemUsage(txnID uuid.UUID) int64 {

	// MVCCAbortTxnOp has TxnID which has underlying memory usage in []byte.
	currMemUsage := mvccAbortTxnOp
	currMemUsage += int64(cap(txnID))
	return currMemUsage
}

// currMemUsage returns the current memory usage of the opsEvent including base
// structs overhead and underlying memory usage.
func opsCurrMemUsage(ops opsEvent) int64 {
	// currMemUsage: eventOverhead already accounts for slice overhead in
	// opsEvent, []enginepb.MVCCLogicalOp. For each cap(ops), the underlying
	// memory include a MVCCLogicalOp overhead.
	currMemUsage := mvccLogicalOp * int64(cap(ops))
	for _, op := range ops {
		switch t := op.GetValue().(type) {
		// currMemUsage: for each op, the pointer to the op is already accounted in
		// mvccLogicalOp. We now account for the underlying memory usage inside the
		// op below.
		case *enginepb.MVCCWriteValueOp:
			currMemUsage += writeValueOpMemUsage(t.Key, t.Value, t.PrevValue)
		case *enginepb.MVCCDeleteRangeOp:
			currMemUsage += deleteRangeOpMemUsage(t.StartKey, t.EndKey)
		case *enginepb.MVCCWriteIntentOp:
			currMemUsage += writeIntentOpMemUsage(t.TxnID, t.TxnKey)
		case *enginepb.MVCCUpdateIntentOp:
			currMemUsage += updateIntentOpMemUsage(t.TxnID)
		case *enginepb.MVCCCommitIntentOp:
			currMemUsage += commitIntentOpMemUsage(t.TxnID, t.Key, t.Value, t.PrevValue)
		case *enginepb.MVCCAbortIntentOp:
			currMemUsage += abortIntentOpMemUsage(t.TxnID)
		case *enginepb.MVCCAbortTxnOp:
			currMemUsage += abortTxnOpMemUsage(t.TxnID)
		default:
			log.Fatalf(context.Background(), "unknown logical op %T", t)
		}
	}
	// For each op, a checkpoint may or may not be published depending on whether
	// the operation caused resolved timestamp to update. Since these are very
	// rare, we disregard them to avoid the complexity.
	return currMemUsage
}

func (e *event) String() string {
	if e == nil {
		return ""
	}
	switch {
	case e.ops != nil:
		str := strings.Builder{}
		str.WriteString("event: logicalops\n")
		for _, op := range e.ops {
			switch t := op.GetValue().(type) {
			case *enginepb.MVCCWriteValueOp, *enginepb.MVCCDeleteRangeOp, *enginepb.MVCCWriteIntentOp,
				*enginepb.MVCCUpdateIntentOp, *enginepb.MVCCCommitIntentOp, *enginepb.MVCCAbortIntentOp, *enginepb.MVCCAbortTxnOp:
				str.WriteString(fmt.Sprintf("op: %T\n", t))
			default:
				str.WriteString("unknown logical op")
			}
		}
		return str.String()
	case !e.ct.IsEmpty():
		return "event: checkpoint"
	case bool(e.initRTS):
		return "event: initrts"
	case e.sst != nil:
		return "event: sst"
	case e.sync != nil:
		return "event: sync"
	default:
		return "missing event variant"
	}
}

// MemUsage estimates the total memory usage of the event, including its
// underlying data. The memory usage is estimated in bytes.
func MemUsage(e event) int64 {
	// currMemUsage: pointer to e is passed to p.eventC channel. Each e pointer is
	// &event{}, and each pointer points at an underlying event{}.
	switch {
	case e.ops != nil:
		// For logical ops events, current memory usage is usually larger than
		// rangefeed events. Note that we assume no checkpoint events are caused by
		// ops since they are pretty rare to avoid the complexity.
		return eventOverhead + opsCurrMemUsage(e.ops)
	case !e.ct.IsEmpty():
		// For ct event, rangefeed checkpoint event usually takes more memory than
		// current memory usage. Note that we assume checkpoint event will happen.
		// If it does not happen, the memory will be released soon after
		// p.ConsumeEvent returns.
		return rangefeedCheckpointOpMemUsage()
	case bool(e.initRTS):
		// For initRTS event, rangefeed checkpoint event usually takes more memory
		// than current memory usage. Note that we assume checkpoint event will
		// happen. If it does not happen, the memory will be released soon after
		// p.ConsumeEvent returns.
		return rangefeedCheckpointOpMemUsage()
	case e.sst != nil:
		// For sst event, rangefeed event usually takes more memory than current
		// memory usage.
		return rangefeedSSTTableOpMemUsage(e.sst.data, e.sst.span.Key, e.sst.span.EndKey)
	case e.sync != nil:
		// For sync event, no rangefeed events will be published.
		return eventOverhead + syncEventOverhead
	default:
		log.Fatalf(context.Background(), "missing event variant: %+v", e)
	}
	// For empty event, only eventOverhead is accounted.
	return eventOverhead
}
