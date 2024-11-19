// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// MVCCLogicalOpType is an enum with values corresponding to each of the
// enginepb.MVCCLogicalOp variants.
//
// LogLogicalOp takes an MVCCLogicalOpType and a corresponding
// MVCCLogicalOpDetails instead of an enginepb.MVCCLogicalOp variant for two
// reasons. First, it serves as a form of abstraction so that callers of the
// method don't need to construct protos themselves. More importantly, it also
// avoids allocations in the common case where Writer.LogLogicalOp is a no-op.
// This makes LogLogicalOp essentially free for cases where logical op logging
// is disabled.
type MVCCLogicalOpType int

const (
	// MVCCWriteValueOpType corresponds to the MVCCWriteValueOp variant.
	MVCCWriteValueOpType MVCCLogicalOpType = iota
	// MVCCWriteIntentOpType corresponds to the MVCCWriteIntentOp variant.
	MVCCWriteIntentOpType
	// MVCCUpdateIntentOpType corresponds to the MVCCUpdateIntentOp variant.
	MVCCUpdateIntentOpType
	// MVCCCommitIntentOpType corresponds to the MVCCCommitIntentOp variant.
	MVCCCommitIntentOpType
	// MVCCAbortIntentOpType corresponds to the MVCCAbortIntentOp variant.
	MVCCAbortIntentOpType
	// MVCCDeleteRangeOpType corresponds to the MVCCDeleteRangeOp variant.
	MVCCDeleteRangeOpType
)

// MVCCLogicalOpDetails contains details about the occurrence of an MVCC logical
// operation.
type MVCCLogicalOpDetails struct {
	Txn       enginepb.TxnMeta
	Key       roachpb.Key
	EndKey    roachpb.Key // only set for MVCCDeleteRangeOpType
	Timestamp hlc.Timestamp

	// Safe indicates that the values in this struct will never be invalidated
	// at a later point. If the details object cannot promise that its values
	// will never be invalidated, an OpLoggerBatch will make a copy of all
	// references before adding it to the log. TestMVCCOpLogWriter fails without
	// this.
	Safe bool
}

// OpLoggerBatch records a log of logical MVCC operations.
type OpLoggerBatch struct {
	Batch

	ops      []enginepb.MVCCLogicalOp
	opsAlloc bufalloc.ByteAllocator
}

// NewOpLoggerBatch creates a new batch that logs logical mvcc operations and
// wraps the provided batch.
func NewOpLoggerBatch(b Batch) *OpLoggerBatch {
	ol := &OpLoggerBatch{Batch: b}
	return ol
}

var _ Batch = &OpLoggerBatch{}

// LogLogicalOp implements the Writer interface.
func (ol *OpLoggerBatch) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	ol.LogLogicalOpOnly(op, details)
	ol.Batch.LogLogicalOp(op, details)
}

func (ol *OpLoggerBatch) LogLogicalOpOnly(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	if keys.IsLocal(details.Key) {
		// Ignore mvcc operations on local keys.
		if bytes.HasPrefix(details.Key, keys.LocalRangeLockTablePrefix) {
			panic(fmt.Sprintf("seeing locktable key %s", details.Key.String()))
		}
		return
	}

	switch op {
	case MVCCWriteValueOpType:
		// Disallow inline values. Emitting these across rangefeeds doesn't make
		// sense, since they can't be ordered and won't be handled by time-bound
		// iterators in catchup scans. We could include them in the log and ignore
		// them (or error) in rangefeeds, but the cost doesn't seem worth it.
		if details.Timestamp.IsEmpty() {
			panic(errors.AssertionFailedf("received inline key %s in MVCC logical op log", details.Key))
		}

		if !details.Safe {
			ol.opsAlloc, details.Key = ol.opsAlloc.Copy(details.Key, 0)
		}

		ol.recordOp(&enginepb.MVCCWriteValueOp{
			Key:       details.Key,
			Timestamp: details.Timestamp,
		})
	case MVCCWriteIntentOpType:
		if !details.Safe {
			ol.opsAlloc, details.Txn.Key = ol.opsAlloc.Copy(details.Txn.Key, 0)
		}

		ol.recordOp(&enginepb.MVCCWriteIntentOp{
			TxnID:           details.Txn.ID,
			TxnKey:          details.Txn.Key,
			TxnIsoLevel:     details.Txn.IsoLevel,
			TxnMinTimestamp: details.Txn.MinTimestamp,
			Timestamp:       details.Timestamp,
		})
	case MVCCUpdateIntentOpType:
		ol.recordOp(&enginepb.MVCCUpdateIntentOp{
			TxnID:     details.Txn.ID,
			Timestamp: details.Timestamp,
		})
	case MVCCCommitIntentOpType:
		if !details.Safe {
			ol.opsAlloc, details.Key = ol.opsAlloc.Copy(details.Key, 0)
		}

		ol.recordOp(&enginepb.MVCCCommitIntentOp{
			TxnID:     details.Txn.ID,
			Key:       details.Key,
			Timestamp: details.Timestamp,
		})
	case MVCCAbortIntentOpType:
		ol.recordOp(&enginepb.MVCCAbortIntentOp{
			TxnID: details.Txn.ID,
		})
	case MVCCDeleteRangeOpType:
		if !details.Safe {
			ol.opsAlloc, details.Key = ol.opsAlloc.Copy(details.Key, 0)
			ol.opsAlloc, details.EndKey = ol.opsAlloc.Copy(details.EndKey, 0)
		}
		ol.recordOp(&enginepb.MVCCDeleteRangeOp{
			StartKey:  details.Key,
			EndKey:    details.EndKey,
			Timestamp: details.Timestamp,
		})
	default:
		panic(fmt.Sprintf("unexpected op type %v", op))
	}
}

func (ol *OpLoggerBatch) recordOp(op interface{}) {
	ol.ops = append(ol.ops, enginepb.MVCCLogicalOp{})
	ol.ops[len(ol.ops)-1].MustSetValue(op)
}

// LogicalOps returns the list of all logical MVCC operations that have been
// recorded by the logger.
func (ol *OpLoggerBatch) LogicalOps() []enginepb.MVCCLogicalOp {
	if ol == nil {
		return nil
	}
	return ol.ops
}

// DisableOpLogger disables op logging for the given read/writer.
func DisableOpLogger(rw ReadWriter) ReadWriter {
	return &noOpLogger{ReadWriter: rw}
}

type noOpLogger struct {
	ReadWriter
}

func (n *noOpLogger) LogLogicalOp(MVCCLogicalOpType, MVCCLogicalOpDetails) {
}
