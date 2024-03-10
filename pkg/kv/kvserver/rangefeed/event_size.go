// Copyright 2018 The Cockroach Authors.
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
	rangeFeedEventOverhead  = int64(unsafe.Sizeof(kvpb.RangeFeedEvent{}))
	allocEventOverhead      = int64(unsafe.Sizeof(SharedBudgetAllocation{}))
	feedBudgetOverhead      = int64(unsafe.Sizeof(FeedBudget{}))
	futureEventBaseOverhead = sharedEventPtrOverhead + sharedEventOverhead + rangeFeedEventOverhead + allocEventOverhead + feedBudgetOverhead
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

func estimateCheckpointEventMemUsage() (futureMemUsage int64) {
	// eventOverhead should have included the overhead of the ctEvent{} already.
	// account for checkpoint event: it is possible that new checkpoint event is needed to be published, but we overaccount for now and release later on.
	return futureEventBaseOverhead + rangefeedCheckpointOverhead
}

func estimateSSTEventMemUsage() (currMemUsage, futureMemUsage int64) {
	currMemUsage += sstEventOverhead
	futureMemUsage += futureEventBaseOverhead + rangefeedSSTTableOverhead
	return currMemUsage, futureMemUsage
}

func estimateSyncEventMemUsage() (currMemUsage int64) {
	return syncEventOverhead
}

// can free more once we finish current
func estimateOpsMemUsage(ops opsEvent, spanSize int64) (currMemUsage, futureMemUsage int64) {
	// ops: []enginepb.MVCCLogicalOp
	currMemUsage += mvccLogicalOp * int64(cap(ops))
	// For each op, some probability to publish checkpoint
	// TODO(wenyihu6): this memory is over-held if no checkpoint events and they
	// should be separated out fromn the other events budget
	futureMemUsage += estimateCheckpointEventMemUsage() * int64(len(ops))
	for _, op := range ops {
		// May publish checkpoint (add a probability) and adjust the budget
		switch op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			currMemUsage += mvccWriteValueOp + int64(op.Size())
			futureMemUsage += futureEventBaseOverhead + rangefeedValueOverhead + int64(op.Size())
		case *enginepb.MVCCDeleteRangeOp:
			currMemUsage += mvccDeleteRangeOp + int64(op.Size())
			futureMemUsage += futureEventBaseOverhead + rangefeedDeleteRangeOverhead + int64(op.Size())
		case *enginepb.MVCCWriteIntentOp:
			currMemUsage += mvccWriteIntentOp + int64(op.Size())
			// No updates to publish.
		case *enginepb.MVCCUpdateIntentOp:
			currMemUsage += mvccUpdateIntentOp + int64(op.Size())
			// No updates to publish.
		case *enginepb.MVCCCommitIntentOp:
			currMemUsage += mvccCommitIntentOp + int64(op.Size())
			futureMemUsage += futureEventBaseOverhead + rangefeedValueOverhead + int64(op.Size())
		case *enginepb.MVCCAbortIntentOp:
			currMemUsage += mvccAbortIntentOp + int64(op.Size())
			// No updates to publish.
		case *enginepb.MVCCAbortTxnOp:
			currMemUsage += mvccAbortTxnOp + int64(op.Size())
			// No updates to publish.
		}
	}
	return currMemUsage, futureMemUsage
}

func estimateEventMemUsage(e event, spanSize int64) int64 {
	currMemUsage, futureMemUsage := int64(0), int64(0)
	switch {
	case e.ops != nil:
		currMemUsage, futureMemUsage = estimateOpsMemUsage(e.ops, spanSize)
	case !e.ct.IsEmpty():
		// no current extra memory usage
		futureMemUsage = estimateCheckpointEventMemUsage() + spanSize
	case bool(e.initRTS):
		// no current extra memory usage
		// may publish checkpoint but we overaccount for now and release later on right after we know we dont need it.
		futureMemUsage = estimateCheckpointEventMemUsage() + spanSize
	case e.sst != nil:
		underlyingDataSize := int64(len(e.sst.data)) + spanSize
		currMemUsage, futureMemUsage = estimateSSTEventMemUsage()
		currMemUsage += underlyingDataSize
		futureMemUsage += underlyingDataSize
	case e.sync != nil:
		currMemUsage = estimateSyncEventMemUsage()
	}
	return max(currMemUsage+eventOverhead, futureMemUsage)
}
