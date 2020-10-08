// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfrapb

import (
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
func MakeEvalContext(evalCtx *tree.EvalContext) EvalContext {
	var be BytesEncodeFormat
	switch evalCtx.SessionData.DataConversion.BytesEncodeFormat {
	case lex.BytesEncodeHex:
		be = BytesEncodeFormat_HEX
	case lex.BytesEncodeEscape:
		be = BytesEncodeFormat_ESCAPE
	case lex.BytesEncodeBase64:
		be = BytesEncodeFormat_BASE64
	default:
		panic("unknown format")
	}
	res := EvalContext{
		StmtTimestampNanos:  evalCtx.StmtTimestamp.UnixNano(),
		TxnTimestampNanos:   evalCtx.TxnTimestamp.UnixNano(),
		Location:            evalCtx.GetLocation().String(),
		Database:            evalCtx.SessionData.Database,
		TemporarySchemaName: evalCtx.SessionData.SearchPath.GetTemporarySchemaName(),
		User:                evalCtx.SessionData.User,
		ApplicationName:     evalCtx.SessionData.ApplicationName,
		BytesEncodeFormat:   be,
		ExtraFloatDigits:    int32(evalCtx.SessionData.DataConversion.ExtraFloatDigits),
		Vectorize:           int32(evalCtx.SessionData.VectorizeMode),
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
