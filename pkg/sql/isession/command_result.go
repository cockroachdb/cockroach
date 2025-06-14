package isession

import (
	"context"
	"log"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
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
	i.resultBuffer.free(i)
}

var _ sql.CommandResult = &internalCommandResult{}

func (i *internalCommandResult) BufferParamStatusUpdate(string, string) {
	// Unimplemented: the internal executor does not support status updated.
	log.Fatal(context.Background(), "DO NOT SUBMIT: is this called?")
}

func (i *internalCommandResult) BufferNotice(notice pgnotice.Notice) {
	// Unimplemented: the internal executor does not support status updated.
	log.Fatal(context.Background(), "DO NOT SUBMIT: is this called?")
}

func (i *internalCommandResult) SendNotice(
	ctx context.Context, notice pgnotice.Notice, immediateFlush bool,
) error {
	// Unimplemented: the internal executor does not support notices.
	log.Fatal(context.Background(), "DO NOT SUBMIT: is this called?")
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
	// TODO(jeffswenson): copy the row
	// The argument row is owned by the caller
	return nil
}

func (i *internalCommandResult) AddBatch(ctx context.Context, batch coldata.Batch) error {
	// TODO(jeffswenson): support coldata batches
	return errors.AssertionFailedf("internal session does not support columnar batches")
}

func (i *internalCommandResult) SupportsAddBatch() bool {
	return false
}

func (i *internalCommandResult) BufferedResultsLen() int {
	return i.numRows
}

func (i *internalCommandResult) TruncateBufferedResults(idx int) bool {
	// TODO(jeffswenson): ensure this is covered
	truncated := idx < len(i.rows)
	i.rows = i.rows[:idx]
	return truncated
}

func (i *internalCommandResult) SetRowsAffected(ctx context.Context, n int) {
	i.numRows = n
}

func (i *internalCommandResult) DisableBuffering() {
	// TODO(jeffswenson): fail the connection
	// This is used by sinkless changefeeds
}

func (i *internalCommandResult) GetBulkJobId() uint64 {
	// TODO(jeffswenson): fail the connection
	// This is kind of a dumb API. The bulk id is decoded by pgwire, but its only
	// used by conn executor, which has the relationship between the types reversed.
	return 0
}

func (i *internalCommandResult) ErrAllowReleased() error {
	return i.err
}

func (i *internalCommandResult) RevokePortalPausability() error {
	return errors.AssertionFailedf("RevokePortalPausability is for limitedCommandResult only")
}
