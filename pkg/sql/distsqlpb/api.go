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

package distsqlpb

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// StreamID identifies a stream; it may be local to a flow or it may cross
// machine boundaries. The identifier can only be used in the context of a
// specific flow.
type StreamID int

// FlowID identifies a flow. It is most importantly used when setting up streams
// between nodes.
type FlowID struct {
	uuid.UUID
}

// DistSQLVersion identifies DistSQL engine versions.
type DistSQLVersion uint32

// MakeEvalContext serializes some of the fields of a tree.EvalContext into a
// distsqlpb.EvalContext proto.
func MakeEvalContext(evalCtx tree.EvalContext) EvalContext {
	var be BytesEncodeFormat
	switch evalCtx.SessionData.DataConversion.BytesEncodeFormat {
	case sessiondata.BytesEncodeHex:
		be = BytesEncodeFormat_HEX
	case sessiondata.BytesEncodeEscape:
		be = BytesEncodeFormat_ESCAPE
	case sessiondata.BytesEncodeBase64:
		be = BytesEncodeFormat_BASE64
	default:
		panic("unknown format")
	}
	res := EvalContext{
		StmtTimestampNanos: evalCtx.StmtTimestamp.UnixNano(),
		TxnTimestampNanos:  evalCtx.TxnTimestamp.UnixNano(),
		Location:           evalCtx.GetLocation().String(),
		Database:           evalCtx.SessionData.Database,
		User:               evalCtx.SessionData.User,
		ApplicationName:    evalCtx.SessionData.ApplicationName,
		BytesEncodeFormat:  be,
		ExtraFloatDigits:   int32(evalCtx.SessionData.DataConversion.ExtraFloatDigits),
	}

	// Populate the search path. Make sure not to include the implicit pg_catalog,
	// since the remote end already knows to add the implicit pg_catalog if
	// necessary, and sending it over would make the remote end think that
	// pg_catalog was explicitly included by the user.
	res.SearchPath = evalCtx.SessionData.SearchPath.GetPathArray()

	// Populate the sequences state.
	latestValues, lastIncremented := evalCtx.SessionData.SequenceState.Export()
	if len(latestValues) > 0 {
		res.SeqState.LastSeqIncremented = &lastIncremented
		for seqID, latestVal := range latestValues {
			res.SeqState.Seqs = append(res.SeqState.Seqs, &SequenceState_Seq{SeqID: seqID, LatestVal: latestVal})
		}
	}
	return res
}
