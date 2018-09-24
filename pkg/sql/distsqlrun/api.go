// Copyright 2017 The Cockroach Authors.
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

package distsqlrun

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// MakeEvalContext serializes some of the fields of a tree.EvalContext into a
// distsqlrun.EvalContext proto.
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

var trSpecPool = sync.Pool{
	New: func() interface{} {
		return &TableReaderSpec{}
	},
}

// NewTableReaderSpec returns a new TableReaderSpec.
func NewTableReaderSpec() *TableReaderSpec {
	return trSpecPool.Get().(*TableReaderSpec)
}

// Release puts this TableReaderSpec back into its sync pool. It may not be used
// again after Release returns.
func (s *TableReaderSpec) Release() {
	s.Reset()
	trSpecPool.Put(s)
}

// Release releases the resources of this SetupFlowRequest, putting them back
// into their respective object pools.
func (s *SetupFlowRequest) Release() {
	if s == nil {
		return
	}
	for i := range s.Flow.Processors {
		if tr := s.Flow.Processors[i].Core.TableReader; tr != nil {
			tr.Release()
		}
	}
	s.Flow.Release()
}
