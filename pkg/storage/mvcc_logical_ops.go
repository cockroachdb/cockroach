// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
)

// MVCCLogicalOpDetails contains details about the occurrence of an MVCC logical
// operation.
type MVCCLogicalOpDetails struct {
	Txn       enginepb.TxnMeta
	Key       roachpb.Key
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
	ol.logLogicalOp(op, details)
	ol.Batch.LogLogicalOp(op, details)
}

func (ol *OpLoggerBatch) logLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	if keys.IsLocal(details.Key) {
		// Ignore mvcc operations on local keys.
		if bytes.HasPrefix(details.Key, keys.LocalRangeLockTablePrefix) {
			panic(fmt.Sprintf("seeing locktable key %s", details.Key.String()))
		}
		return
	}

	switch op {
	case MVCCWriteValueOpType:
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
