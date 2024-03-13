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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
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
)

const (
	eventOverhead = int64(unsafe.Sizeof(&event{})) + int64(unsafe.Sizeof(event{}))
)

const (
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

// sstEvent returns the future memory usage that will be created based on the
// received sstEvent.
func (sst sstEvent) futureMemUsage() int64 {
	// RangeFeedSSTable event will be published.
	return rangefeedSSTTableOpMemUsage(sst.data, sst.span.Key, sst.span.EndKey)
}

// futureMemUsage returns the future memory usage that will be created based on
// the received ctEvent.
func (ct ctEvent) futureMemUsage() int64 {
	// A checkpoint may or may not be published depending on whether
	// p.rts.ForwardClosedTS returns true.  Since our strategy is over-account >>
	// under-account, account for memory assuming a checkpoint event will be
	// created. If it turns out that no checkpoint event is created, the memory
	// will be soonly released after p.ConsumeEvent returns and no registration
	// published a shared event referencing it.
	return rangefeedCheckpointOpMemUsage()
}

// futureMemUsage returns the future memory usage that will be created based on
// the received initRTSEvent.
func (initRTS initRTSEvent) futureMemUsage() int64 {
	// A checkpoint may or may not be published depending on whether p.rts.Init
	// returns true. Since our strategy is over-account >> under-account, account
	// for memory assuming a checkpoint event will be created. If it turns out
	// that no checkpoint event is created, the memory will be soonly released
	// after p.ConsumeEvent returns and no registration published a shared event
	// referencing it.
	return rangefeedCheckpointOpMemUsage()
}

// futureMemUsage returns the future memory usage that will be created based on
// the received syncEvent.
func (sync syncEvent) futureMemUsage() int64 {
	// No updates to publish in for synvEvent.
	return 0
}

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
// rangefeedWriteValueOpMemUsage accounts for the entire memory usage of a new
// RangeFeedValue event.
func rangefeedWriteValueOpMemUsage(key roachpb.Key, value, prevValue []byte) int64 {
	// Pointer to RangeFeedValue has already been accounted in
	// futureEventBaseOverhead as part of the base struct overhead of
	// RangeFeedEvent. rangefeedValueOverhead includes the memory usage of the
	// underlying RangeFeedValue base struct.
	memUsage := futureEventBaseOverhead + rangefeedValueOverhead

	// RangeFeedValue has Key, Value{RawBytes,Timestamp}, PrevValue. Only Key,
	// RawBytes, and PrevValue has underlying memory usage in []byte. Timestamp
	// and other base structs of Value have no underlying data and are already
	// accounted in rangefeedValueOverhead.
	memUsage += int64(cap(key))
	memUsage += int64(cap(value))
	memUsage += int64(cap(prevValue))
	return memUsage
}

// No future memory usages have been accounted so far.
// rangefeedWriteValueOpMemUsage accounts for the entire memory usage of a new
// RangefeedDeleteRange event.
func rangefeedDeleteRangeOpMemUsage(startKey, endKey roachpb.Key) int64 {
	// Pointer to RangeFeedDeleteRange has already been accounted in
	// futureEventBaseOverhead as part of the base struct overhead of
	// RangeFeedEvent. rangefeedDeleteRangeOverhead includes the memory usage of
	// the underlying RangeFeedDeleteRange base struct.
	memUsage := futureEventBaseOverhead + rangefeedDeleteRangeOverhead

	// RangeFeedDeleteRange has Span{Start,EndKey} and Timestamp. Only StartKey
	// and EndKey have underlying memory usage in []byte. Timestamp and other base
	// structs of Span have no underlying data and are already accounted in
	// rangefeedDeleteRangeOverhead.
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

// currMemUsage returns the current memory usage of the ctEvent. eventOverhead
// already accounts for the base struct overhead of ctEvent{}. ctEvent has
// Timestamp, so it has no additional underlying memory.
func (ct ctEvent) currMemUsage() int64 {
	return 0
}

// initRTSEvent returns the current memory usage of the initRTSEvent.
// eventOverhead already accounts for the memory of initRTSEvent(bool), and
// initRTSEvent has no underlying memory.
func (initRTS initRTSEvent) currMemUsage() int64 {
	return 0
}

// eventOverhead accounts for the base struct of event{} which only included
// pointer to sstEvent. currMemUsage accounts the base struct memory of
// sstEvent{} and its underlying memory.
func (sst sstEvent) currMemUsage() int64 {
	// sstEvent has data, span, and timestamp. Only data and span has underlying
	// memory usage. Base structs memory for span and timestamp have already been
	// accounted in sstEventOverhead.
	return sstEventOverhead + int64(cap(sst.data)+cap(sst.span.Key)+cap(sst.span.EndKey))
}

// eventOverhead accounts for the base struct of event{} which only included
// pointer to syncEvent. currMemUsage accounts the base struct memory of
// syncEvent{} and its underlying memory.
func (sync syncEvent) currMemUsage() int64 {
	// syncEvent has a channel and a pointer to testRegCatchupSpan.
	// testRegCatchupSpan is never set in production. Channel sent is always
	// struct{}, so the memory has already been accounted in syncEventOverhead.
	return syncEventOverhead
}

// currAndFutureMemUsage returns the current and future memory usage of the
// opsEvent. The function calculates them both in one for loop to avoid
// iterating over ops twice.
func (ops opsEvent) currAndFutureMemUsage() (currMemUsage int64, futureMemUsage int64) {
	// currMemUsage: eventOverhead already accounts for slice overhead in
	// opsEvent, []enginepb.MVCCLogicalOp. For each cap(ops), the underlying
	// memory include a MVCCLogicalOp overhead.
	currMemUsage = mvccLogicalOp * int64(cap(ops))
	// futureMemUsage: starts as zero since we do not know how many events will be
	// created in the future yet.
	futureMemUsage = int64(0)
	for _, op := range ops {
		switch t := op.GetValue().(type) {
		// currMemUsage: for each op, the pointer to the op is already accounted in
		// mvccLogicalOp. We now account for the underlying memory usage inside the
		// op below.
		// futureMemUsage: for each op, find out how much future memory usage will
		// be created.
		case *enginepb.MVCCWriteValueOp:
			currMemUsage += writeValueOpMemUsage(t.Key, t.Value, t.PrevValue)
			// key, value, prevValue are passed in to construct RangeFeedValue {key,
			// value.RawBytes, prevValue.RawBytes} later.
			futureMemUsage += rangefeedWriteValueOpMemUsage(t.Key, t.Value, t.PrevValue)
		case *enginepb.MVCCDeleteRangeOp:
			currMemUsage += deleteRangeOpMemUsage(t.StartKey, t.EndKey)
			// startKey andKey end are passed in to construct
			// RangeFeedDeleteRange{span{startKey, endKey}} later.
			futureMemUsage += rangefeedDeleteRangeOpMemUsage(t.StartKey, t.EndKey)
		case *enginepb.MVCCWriteIntentOp:
			currMemUsage += writeIntentOpMemUsage(t.TxnID, t.TxnKey)
			// No updates to publish in consumeLogicalOps.
		case *enginepb.MVCCUpdateIntentOp:
			currMemUsage += updateIntentOpMemUsage(t.TxnID)
			// No updates to publish in consumeLogicalOps.
		case *enginepb.MVCCCommitIntentOp:
			currMemUsage += commitIntentOpMemUsage(t.TxnID, t.Key, t.Value, t.PrevValue)
			// key, value, prevValue are passed in to construct RangeFeedValue {key,
			// value.RawBytes, prevValue.RawBytes} later.
			futureMemUsage += rangefeedWriteValueOpMemUsage(t.Key, t.Value, t.PrevValue)
		case *enginepb.MVCCAbortIntentOp:
			currMemUsage += abortIntentOpMemUsage(t.TxnID)
			// No updates to publish in consumeLogicalOps.
		case *enginepb.MVCCAbortTxnOp:
			currMemUsage += abortTxnOpMemUsage(t.TxnID)
			// No updates to publish in consumeLogicalOps.
		}
	}
	// For each op, a checkpoint may or may not be published depending on whether
	// the operation caused resolved timestamp to update. Since our strategy is
	// over-account >> under-account, account for memory assuming a checkpoint
	// event will be created.
	futureMemUsage += rangefeedCheckpointOpMemUsage() * int64(len(ops))
	return currMemUsage, futureMemUsage
}

func (e event) currAndFutureMemUsage() (int64, int64) {
	// currMemUsage: pointer to e is passed to p.eventC channel. Each e pointer is
	// &event{}, and each pointer points at an underlying event{}.
	// futureMemUsage: Future event starts as 0 since we don't know if a future
	// event will be created yet.
	currMemUsage, futureMemUsage := eventOverhead, int64(0)
	switch {
	case e.ops != nil:
		curr, future := e.ops.currAndFutureMemUsage()
		currMemUsage += curr
		futureMemUsage += future
	case !e.ct.IsEmpty():
		currMemUsage += e.ct.currMemUsage()
		futureMemUsage += e.ct.futureMemUsage()
	case bool(e.initRTS):
		currMemUsage += e.initRTS.currMemUsage()
		futureMemUsage += e.initRTS.futureMemUsage()
	case e.sst != nil:
		currMemUsage += e.sst.currMemUsage()
		futureMemUsage += e.sst.futureMemUsage()
	case e.sync != nil:
		currMemUsage += e.sync.currMemUsage()
		futureMemUsage += e.sync.futureMemUsage()
	}
	return currMemUsage, futureMemUsage
}

// EventMemUsage returns the current memory usage of the event including the
// underlying data memory this event holds.
func EventMemUsage(e *event) int64 {
	currMemUsage, futureMemUsage := e.currAndFutureMemUsage()
	return max(currMemUsage, futureMemUsage)
}

// RangefeedEventMemUsage returns the memory usage of a RangeFeedEvent including
// the underlying data memory this event holds.
func RangefeedEventMemUsage(re *kvpb.RangeFeedEvent) int64 {
	memUsage := int64(0)
	switch t := re.GetValue().(type) {
	case *kvpb.RangeFeedValue:
		memUsage += rangefeedWriteValueOpMemUsage(t.Key, t.Value.RawBytes, t.PrevValue.RawBytes)
	case *kvpb.RangeFeedDeleteRange:
		memUsage += rangefeedDeleteRangeOpMemUsage(t.Span.Key, t.Span.EndKey)
	case *kvpb.RangeFeedSSTable:
		memUsage += rangefeedSSTTableOpMemUsage(t.Data, t.Span.Key, t.Span.EndKey)
	case *kvpb.RangeFeedCheckpoint:
		memUsage += rangefeedCheckpointOpMemUsage()
	}
	return memUsage
}
