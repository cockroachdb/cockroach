// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/planbase"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/redact"
)

// SessionVar represents a session variable configuration.
// This struct mirrors the one in pkg/sql/vars.go but is defined here
// to avoid circular dependencies.
type SessionVar struct {
	Hidden             bool
	NoResetAll         bool
	Get                func(evalCtx planbase.ExtendedEvalContextI, kv *kv.Txn) (string, error)
	Unit               string
	Exists             func(evalCtx planbase.ExtendedEvalContextI, kv *kv.Txn) bool
	GetFromSessionData func(sd *sessiondata.SessionData) string
	GetStringVal       func(values []string, s string) (string, error)
	Set                func(ctx context.Context, m interface{}, s string) error
	RuntimeSet         func(ctx context.Context, evalCtx planbase.ExtendedEvalContextI, s string) error
	SetGlobal          func(ctx context.Context, sv interface{}, s string) error
	GlobalDefault      func(sv interface{}) string
	SetWithPlanner     func(ctx context.Context, p planbase.PlannerAccessor, local bool, s string) error
}

// Lowercase alias for backward compatibility within plannode package
type sessionVar = SessionVar

// MetadataForwarder is used to forward metadata in distributed execution.
type MetadataForwarder interface {
	ForwardMetadata(metadata interface{})
}

// Lowercase alias
type metadataForwarder = MetadataForwarder

// JoinPredicate implements predicate logic for joins.
type JoinPredicate struct {
	// We only need the struct definition for field declarations
	// The actual implementation stays in pkg/sql/join_predicate.go
	joinType           descpb.JoinType
	numLeftCols        int
	numRightCols       int
	leftEqualityIndices []int
	rightEqualityIndices []int
	leftColNames       tree.NameList
	rightColNames      tree.NameList
}

// Lowercase alias
type joinPredicate = JoinPredicate

// RowContainerHelper wraps a disk-backed row container.
type RowContainerHelper struct {
	memMonitor          *mon.BytesMonitor
	unlimitedMemMonitor *mon.BytesMonitor
	diskMonitor         *mon.BytesMonitor
	rows                *rowcontainer.DiskBackedRowContainer
	scratch             []interface{} // rowenc.EncDatumRow
}

// Init initializes the row container helper.
func (c *RowContainerHelper) Init(
	ctx context.Context,
	typs []*types.T,
	evalCtx planbase.ExtendedEvalContextI,
	opName redact.SafeString,
) {
	// Stub - actual implementation in pkg/sql/buffer_util.go
}

// Lowercase alias
type rowContainerHelper = RowContainerHelper

// RowContainerIterator iterates over rows in a container.
type RowContainerIterator struct {
	rowContainer *rowcontainer.DiskBackedRowContainer
	curRow       tree.Datums
	encoding     []encoding.Direction
}

// Lowercase alias
type rowContainerIterator = RowContainerIterator

// RowsAffectedOutputHelper tracks the number of rows affected.
type RowsAffectedOutputHelper struct {
	rowCount int64
}

// rowsAffected returns the number of rows affected.
func (h *RowsAffectedOutputHelper) rowsAffected() int64 {
	return h.rowCount
}

// Lowercase alias
type rowsAffectedOutputHelper = RowsAffectedOutputHelper

// CloudCheckParams holds parameters for cloud connection checking.
type CloudCheckParams = execinfrapb.CloudStorageTestSpec_Params
