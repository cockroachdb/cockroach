// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfrapb

import (
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// ProcessorID identifies a processor in the context of a specific flow.
type ProcessorID int

// StreamID identifies a stream; it may be local to a flow or it may cross
// machine boundaries. The identifier can only be used in the context of a
// specific flow.
type StreamID int

func (sid StreamID) String() string {
	return strconv.Itoa(int(sid))
}

// FlowID identifies a flow. It is most importantly used when setting up streams
// between nodes.
type FlowID struct {
	uuid.UUID
}

// Equal returns whether the two FlowIDs are equal.
func (f FlowID) Equal(other FlowID) bool {
	return f.UUID.Equal(other.UUID)
}

// IsUnset returns whether the FlowID is unset.
func (f FlowID) IsUnset() bool {
	return f.UUID.Equal(uuid.Nil)
}

// MakeEvalContext serializes some of the fields of a eval.Context into a
// execinfrapb.EvalContext proto.
func MakeEvalContext(evalCtx *eval.Context) EvalContext {
	sessionDataProto := evalCtx.SessionData().SessionData
	sessiondata.MarshalNonLocal(evalCtx.SessionData(), &sessionDataProto)
	return EvalContext{
		SessionData:        sessionDataProto,
		StmtTimestampNanos: evalCtx.StmtTimestamp.UnixNano(),
		TxnTimestampNanos:  evalCtx.TxnTimestamp.UnixNano(),
	}
}

// User accesses the user field.
func (m *BackupDataSpec) User() username.SQLUsername {
	return m.UserProto.Decode()
}

// User accesses the user field.
func (m *ExportSpec) User() username.SQLUsername {
	return m.UserProto.Decode()
}

// User accesses the user field.
func (m *ReadImportDataSpec) User() username.SQLUsername {
	return m.UserProto.Decode()
}

// User accesses the user field.
func (m *ChangeAggregatorSpec) User() username.SQLUsername {
	return m.UserProto.Decode()
}

// User accesses the user field.
func (m *ChangeFrontierSpec) User() username.SQLUsername {
	return m.UserProto.Decode()
}

func (m *GenerativeSplitAndScatterSpec) User() username.SQLUsername {
	return m.UserProto.Decode()
}
