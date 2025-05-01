// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package isession

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

type internalCommandResult struct {
	pos     sql.CmdPos
	err     error
	numRows int
	rows    []tree.Datums

	resultBuffer *resultBuffer
}

// RowsAffected implements sql.CommandResult.
func (i *internalCommandResult) RowsAffected() int {
	return i.numRows
}

var _ sql.SyncResult = &internalCommandResult{}

func (i *internalCommandResult) SetError(err error) {
	i.err = err
}

func (i *internalCommandResult) Err() error {
	return i.err
}

func (i *internalCommandResult) Close(
	ctx context.Context, txnStatus sql.TransactionStatusIndicator,
) {
	i.resultBuffer.ready(i)
}

func (i *internalCommandResult) Discard() {
	i.resultBuffer.Lock()
	defer i.resultBuffer.Unlock()
	i.resultBuffer.freeLocked(i)
}

var _ sql.CommandResult = &internalCommandResult{}

func (i *internalCommandResult) BufferParamStatusUpdate(string, string) {
	// Ignore notices
}

func (i *internalCommandResult) BufferNotice(notice pgnotice.Notice) {
	// Ignore notices
}

func (i *internalCommandResult) SendNotice(
	ctx context.Context, notice pgnotice.Notice, immediateFlush bool,
) error {
	return nil
}

func (i *internalCommandResult) SetColumns(ctx context.Context, cols colinfo.ResultColumns) {
	// We don't need this because the datums include the type.
}

func (i *internalCommandResult) ResetStmtType(stmt tree.Statement) {
	// Session executor doesn't care about the statement type.
}

func (i *internalCommandResult) GetFormatCode(colIdx int) (pgwirebase.FormatCode, error) {
	// Rows aren't serialized in by the internal session, so this format code
	// doesn't really matter - return the default.
	return pgwirebase.FormatText, nil
}

func (i *internalCommandResult) AddRow(ctx context.Context, row tree.Datums) error {
	i.numRows++
	var copy tree.Datums
	copy = append(copy, row...)
	i.rows = append(i.rows, copy)
	return nil
}

func (i *internalCommandResult) AddBatch(ctx context.Context, batch coldata.Batch) error {
	return errors.AssertionFailedf("internal session does not support columnar batches")
}

func (i *internalCommandResult) SupportsAddBatch() bool {
	return false
}

func (i *internalCommandResult) BufferedResultsLen() int {
	return i.numRows
}

func (i *internalCommandResult) TruncateBufferedResults(idx int) bool {
	if idx < len(i.rows) {
		i.rows = i.rows[:idx]
		return true
	}
	return false
}

func (i *internalCommandResult) SetRowsAffected(ctx context.Context, n int) {
	i.numRows = n
}

func (i *internalCommandResult) DisableBuffering() {
}

func (i *internalCommandResult) GetBulkJobId() uint64 {
	// This is kind of a dumb API. The bulk id is decoded by pgwire, but its only
	// used by conn executor, which has the relationship between the types reversed.
	i.err = errors.AssertionFailedf("GetBulkJobId is not supported by the internal session")
	return 0
}

func (i *internalCommandResult) ErrAllowReleased() error {
	return i.err
}

func (i *internalCommandResult) RevokePortalPausability() error {
	return errors.AssertionFailedf("RevokePortalPausability is for limitedCommandResult only")
}

func (i *internalCommandResult) SendCopyData(ctx context.Context, data []byte, flush bool) error {
	return errors.AssertionFailedf("SendCopyData not supported by internal session")
}

func (i *internalCommandResult) SetInferredTypes(types []oid.Oid) {
	// No-op for internal session
}

func (i *internalCommandResult) SendCopyDone(ctx context.Context) error {
	return errors.AssertionFailedf("SendCopyDone not supported by internal session")
}

func (i *internalCommandResult) SetNoDataRowDescription() {
	// No-op for internal session
}

func (i *internalCommandResult) SendCopyOut(
	ctx context.Context, cols colinfo.ResultColumns, format pgwirebase.FormatCode,
) error {
	return errors.AssertionFailedf("SendCopyOut not supported by internal session")
}

func (i *internalCommandResult) SetPortalOutput(
	ctx context.Context, cols colinfo.ResultColumns, formatCodes []pgwirebase.FormatCode,
) {
	// No-op for internal session
}

func (i *internalCommandResult) SetPrepStmtOutput(ctx context.Context, cols colinfo.ResultColumns) {
	// No-op for internal session
}
