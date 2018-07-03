// Copyright 2018 The Cockroach Authors.
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

package engine

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
)

// MVCCLogicalOpType is an enum with values corresponding to each of the
// enginepb.MVCCLogicalOp variants.
//
// LogLogicalOp takes an MVCCLogicalOpType and a corresponding
// MVCCLogicalOpDetails instead of an enginepb.MVCCLogicalOp variant for two
// reasons. First, it serves as a form of abstraction so that callers of the
// method don't need to construct protos themselves. More importantly, it avoids
// allocations in the common case where Writer.LogLogicalOp is a no-op. This
// makes LogLogicalOp essentially free for cases where logical op logging is
// disabled.
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
	Txn   *enginepb.TxnMeta
	Key   roachpb.Key
	Value roachpb.Value
}

// MVCCOpLogWriter records a log of logical MVCC operations.
type MVCCOpLogWriter struct {
	Batch
	ops []enginepb.MVCCLogicalOp
}

// NewOpLoggerBatch creates ... WIP:
func NewOpLoggerBatch(b Batch) *MVCCOpLogWriter {
	return &MVCCOpLogWriter{Batch: b}
}

var _ Batch = &MVCCOpLogWriter{}

// LogLogicalOp implements the Writer interface.
func (ol *MVCCOpLogWriter) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	ol.logLogicalOp(op, details)
	ol.Batch.LogLogicalOp(op, details)
}

func (ol *MVCCOpLogWriter) logLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	switch op {
	case MVCCWriteValueOpType:
		ol.recordOp(&enginepb.MVCCWriteValueOp{
			Key:       details.Key,
			Timestamp: details.Value.Timestamp,
			Value:     details.Value.RawBytes,
		})
	case MVCCWriteIntentOpType:
		ol.recordOp(&enginepb.MVCCWriteIntentOp{
			TxnID:     details.Txn.ID,
			TxnKey:    details.Txn.Key,
			Timestamp: details.Value.Timestamp,
		})
	case MVCCUpdateIntentOpType:
		ol.recordOp(&enginepb.MVCCUpdateIntentOp{
			TxnID:     details.Txn.ID,
			Timestamp: details.Value.Timestamp,
		})
	case MVCCCommitIntentOpType:
		ol.recordOp(&enginepb.MVCCCommitIntentOp{
			TxnID:     details.Txn.ID,
			Key:       details.Key,
			Timestamp: details.Value.Timestamp,
		})
	case MVCCAbortIntentOpType:
		ol.recordOp(&enginepb.MVCCAbortIntentOp{
			TxnID: details.Txn.ID,
		})
	default:
		panic(fmt.Sprintf("unexpected op type %v", op))
	}
}

func (ol *MVCCOpLogWriter) recordOp(op interface{}) {
	ol.ops = append(ol.ops, enginepb.MVCCLogicalOp{})
	if !ol.ops[len(ol.ops)-1].SetValue(op) {
		panic(fmt.Sprintf("unknown logical mvcc op: %v", op))
	}
}

// LogicalOps returns the list of all logical MVCC operations that have been
// recorded by the logger.
func (ol *MVCCOpLogWriter) LogicalOps() []enginepb.MVCCLogicalOp {
	if ol == nil {
		return nil
	}
	return ol.ops
}

// Distinct implements the Batch interface.
func (ol *MVCCOpLogWriter) Distinct() ReadWriter {
	dist := ol.Batch.Distinct()
	return &distinctMVCCOpLogWriter{ReadWriter: dist, parent: ol}
}

type distinctMVCCOpLogWriter struct {
	ReadWriter
	parent *MVCCOpLogWriter
}

// LogLogicalOp implements the Writer interface.
func (dol *distinctMVCCOpLogWriter) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	dol.parent.logLogicalOp(op, details)
	dol.ReadWriter.LogLogicalOp(op, details)
}
