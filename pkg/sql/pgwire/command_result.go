// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

type completionMsgType int

const (
	_ completionMsgType = iota
	commandComplete
	bindComplete
	closeComplete
	parseComplete
	emptyQueryResponse
	readyForQuery
	flush
	// Some commands, like Describe, don't need a completion message.
	noCompletionMsg
)

// commandResult is an implementation of sql.CommandResult that streams a
// commands results over a pgwire network connection.
type commandResult struct {
	// conn is the parent connection of this commandResult.
	conn *conn
	// conv and location indicate the conversion settings for SQL values.
	conv     sessiondatapb.DataConversionConfig
	location *time.Location
	// pos identifies the position of the command within the connection.
	pos sql.CmdPos

	// buffer contains items that are sent before the connection is closed.
	buffer struct {
		notices            []pgnotice.Notice
		paramStatusUpdates []paramStatusUpdate
	}

	err error
	// errExpected, if set, enforces that an error had been set when Close is
	// called.
	errExpected bool

	typ completionMsgType
	// If typ == commandComplete, this is the tag to be written in the
	// CommandComplete message.
	cmdCompleteTag string

	stmtType tree.StatementReturnType
	descOpt  sql.RowDescOpt

	// rowsAffected doesn't reflect the number of changed rows for bulk job
	// (IMPORT, BACKUP and RESTORE). For these jobs, see the usages of
	// log/logutil.LogJobCompletion().
	rowsAffected int

	// formatCodes describes the encoding of each column of result rows. It is nil
	// for statements not returning rows (or for results for commands other than
	// executing statements). It can also be nil for queries returning rows,
	// meaning that all columns will be encoded in the text format (this is the
	// case for queries executed through the simple protocol). Otherwise, it needs
	// to have an entry for every column.
	formatCodes []pgwirebase.FormatCode

	// types is a map from result column index to its type T, similar to formatCodes
	// (except types must always be set).
	types []*types.T

	// bufferingDisabled is conditionally set during planning of certain
	// statements.
	bufferingDisabled bool

	// released is set when the command result has been released so that its
	// memory can be reused. It is also used to assert against use-after-free
	// errors.
	released bool

	// bulkJobInfo stores id for bulk jobs (IMPORT, BACKUP, RESTORE),
	// It's written in commandResult.AddRow() and only if the query is
	// IMPORT, BACKUP or RESTORE.
	bulkJobId uint64
}

// paramStatusUpdate is a status update to send to the client when a parameter is
// updated.
type paramStatusUpdate struct {
	// param is the parameter name.
	param string
	// val is the new value of the given parameter.
	val string
}

var _ sql.CommandResult = &commandResult{}

// RevokePortalPausability is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) RevokePortalPausability() error {
	return errors.AssertionFailedf("RevokePortalPausability is only implemented by limitedCommandResult only")
}

// Close is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) Close(ctx context.Context, t sql.TransactionStatusIndicator) {
	r.assertNotReleased()
	defer r.release()
	if r.errExpected && r.err == nil {
		panic("expected err to be set on result by Close, but wasn't")
	}

	r.conn.writerState.fi.registerCmd(r.pos)
	if r.err != nil {
		r.conn.bufferErr(ctx, r.err)
		// Sync is the only client message that results in ReadyForQuery, and it
		// must *always* result in ReadyForQuery, even if there are errors during
		// Sync.
		if r.typ != readyForQuery {
			return
		}
	}

	for _, notice := range r.buffer.notices {
		if err := r.conn.bufferNotice(ctx, notice); err != nil {
			panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected err when sending notice"))
		}
	}

	// Send a completion message, specific to the type of result.
	switch r.typ {
	case commandComplete:
		tag := cookTag(
			r.cmdCompleteTag, r.conn.writerState.tagBuf[:0], r.stmtType, r.rowsAffected,
		)
		r.conn.bufferCommandComplete(tag)
	case parseComplete:
		r.conn.bufferParseComplete()
	case bindComplete:
		r.conn.bufferBindComplete()
	case closeComplete:
		r.conn.bufferCloseComplete()
	case readyForQuery:
		r.conn.bufferReadyForQuery(byte(t))
		// The error is saved on conn.err.
		_ /* err */ = r.conn.Flush(r.pos)
		r.conn.maybeReallocate()
	case emptyQueryResponse:
		r.conn.bufferEmptyQueryResponse()
	case flush:
		// The error is saved on conn.err.
		_ /* err */ = r.conn.Flush(r.pos)
		r.conn.maybeReallocate()
	case noCompletionMsg:
		// nothing to do
	default:
		panic(errors.AssertionFailedf("unknown type: %v", r.typ))
	}

	for _, paramStatusUpdate := range r.buffer.paramStatusUpdates {
		if err := r.conn.bufferParamStatus(
			paramStatusUpdate.param,
			paramStatusUpdate.val,
		); err != nil {
			panic(
				errors.NewAssertionErrorWithWrappedErrf(err, "unexpected err when sending parameter status update"),
			)
		}
	}
}

// Discard is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) Discard() {
	r.assertNotReleased()
	defer r.release()
}

// Err is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) Err() error {
	r.assertNotReleased()
	return r.err
}

// ErrAllowReleased is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) ErrAllowReleased() error {
	return r.err
}

// SetError is part of the sql.RestrictedCommandResult interface.
//
// We're not going to write any bytes to the buffer in order to support future
// SetError() calls. The error will only be serialized at Close() time.
func (r *commandResult) SetError(err error) {
	r.assertNotReleased()
	r.err = err
}

// GetFormatCode is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) GetFormatCode(colIdx int) (pgwirebase.FormatCode, error) {
	fmtCode := pgwirebase.FormatText
	if r.formatCodes != nil {
		if colIdx >= len(r.formatCodes) {
			if len(r.formatCodes) == 1 && r.cmdCompleteTag == "EXPLAIN" {
				// For EXPLAIN ANALYZE statements the format codes describe how
				// to serialize the EXPLAIN ANALYZE output (which has just one
				// column) and not the result of the query being executed (which
				// can have any number of columns). In such a scenario we won't
				// serialize the data rows (since we will produce stringified
				// EXPLAIN ANALYZE output and the rows will be actually
				// discarded by the DistSQLReceiver), so the format code here
				// doesn't matter.
				return fmtCode, nil
			}
			return 0, errors.AssertionFailedf("could not find format code for column %d in %v", colIdx, r.formatCodes)
		}
		fmtCode = r.formatCodes[colIdx]
	}
	return fmtCode, nil
}

// beforeAdd should be called before rows are buffered.
func (r *commandResult) beforeAdd() error {
	r.assertNotReleased()
	if r.err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(r.err, "can't call AddRow after having set error"))
	}
	r.conn.writerState.fi.registerCmd(r.pos)
	if err := r.conn.GetErr(); err != nil {
		return err
	}
	if r.err != nil {
		panic("can't send row after error")
	}
	return nil
}

// JobIdColIdx is based on jobs.BulkJobExecutionResultHeader and
// jobs.DetachedJobExecutionResultHeader.
var JobIdColIdx int

func (r *commandResult) SupportsPausing() bool {
	return false
}

func (r *commandResult) ResumeAfterPause(newLimit int) {
	panic("unimplemented")
}

func (r *commandResult) CloseAfterPause() {
	panic("unimplemented")
}

func (r *commandResult) WaitForRows() (fsm.Event, fsm.EventPayload, error) {
	panic("unimplemented")
}

func (r *commandResult) RowsExhausted(ev fsm.Event, payload fsm.EventPayload, err error) {
	panic("unimplemented")
}

// AddRow is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) AddRow(ctx context.Context, row tree.Datums) error {
	if err := r.beforeAdd(); err != nil {
		return err
	}
	switch r.cmdCompleteTag {
	case tree.ImportTag, tree.RestoreTag, tree.BackupTag:
		r.bulkJobId = uint64(*row[JobIdColIdx].(*tree.DInt))
	default:
		r.rowsAffected++
	}
	return r.conn.bufferRow(ctx, row, r)
}

// AddBatch is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) AddBatch(ctx context.Context, batch coldata.Batch) error {
	if err := r.beforeAdd(); err != nil {
		return err
	}
	switch r.cmdCompleteTag {
	case tree.ImportTag, tree.RestoreTag, tree.BackupTag:
		r.bulkJobId = uint64(batch.ColVec(JobIdColIdx).Int64()[0])
	default:
		r.rowsAffected += batch.Length()
	}
	return r.conn.bufferBatch(ctx, batch, r)
}

// SupportsAddBatch is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) SupportsAddBatch() bool {
	return true
}

// BufferedResultsLen is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) BufferedResultsLen() int {
	return r.conn.writerState.buf.Len()
}

// TruncateBufferedResults is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) TruncateBufferedResults(idx int) bool {
	r.assertNotReleased()
	if r.conn.writerState.fi.lastFlushed >= r.pos {
		return false
	}
	if idx < 0 {
		return false
	}
	if idx > r.conn.writerState.buf.Len() {
		return true
	}
	r.conn.writerState.buf.Truncate(idx)
	return true
}

// DisableBuffering is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) DisableBuffering() {
	r.assertNotReleased()
	r.bufferingDisabled = true
}

// BufferParamStatusUpdate is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) BufferParamStatusUpdate(param string, val string) {
	r.buffer.paramStatusUpdates = append(
		r.buffer.paramStatusUpdates,
		paramStatusUpdate{param: param, val: val},
	)
}

// BufferNotice is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) BufferNotice(notice pgnotice.Notice) {
	r.buffer.notices = append(r.buffer.notices, notice)
}

// SendNotice is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) SendNotice(ctx context.Context, notice pgnotice.Notice) error {
	if err := r.conn.bufferNotice(ctx, notice); err != nil {
		return err
	}
	return r.conn.Flush(r.pos)
}

// SetColumns is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) SetColumns(ctx context.Context, cols colinfo.ResultColumns) {
	r.assertNotReleased()
	r.conn.writerState.fi.registerCmd(r.pos)
	if r.descOpt == sql.NeedRowDesc {
		_ /* err */ = r.conn.writeRowDescription(ctx, cols, r.formatCodes, &r.conn.writerState.buf)
	}
	r.types = make([]*types.T, len(cols))
	for i, col := range cols {
		r.types[i] = col.Typ
	}
}

// SetInferredTypes is part of the sql.DescribeResult interface.
func (r *commandResult) SetInferredTypes(types []oid.Oid) {
	r.assertNotReleased()
	r.conn.writerState.fi.registerCmd(r.pos)
	r.conn.bufferParamDesc(types)
}

// SetNoDataRowDescription is part of the sql.DescribeResult interface.
func (r *commandResult) SetNoDataRowDescription() {
	r.assertNotReleased()
	r.conn.writerState.fi.registerCmd(r.pos)
	r.conn.bufferNoDataMsg()
}

// SetPrepStmtOutput is part of the sql.DescribeResult interface.
func (r *commandResult) SetPrepStmtOutput(ctx context.Context, cols colinfo.ResultColumns) {
	r.assertNotReleased()
	r.conn.writerState.fi.registerCmd(r.pos)
	_ /* err */ = r.conn.writeRowDescription(ctx, cols, nil /* formatCodes */, &r.conn.writerState.buf)
}

// SetPortalOutput is part of the sql.DescribeResult interface.
func (r *commandResult) SetPortalOutput(
	ctx context.Context, cols colinfo.ResultColumns, formatCodes []pgwirebase.FormatCode,
) {
	r.assertNotReleased()
	r.conn.writerState.fi.registerCmd(r.pos)
	_ /* err */ = r.conn.writeRowDescription(ctx, cols, formatCodes, &r.conn.writerState.buf)
}

// SendCopyOut is part of the sql.CopyOutResult interface.
func (r *commandResult) SendCopyOut(
	ctx context.Context, cols colinfo.ResultColumns, format pgwirebase.FormatCode,
) error {
	r.assertNotReleased()
	r.conn.writerState.fi.registerCmd(r.pos)
	return r.conn.bufferCopyOut(cols, format)
}

// SendCopyData is part of the sql.CopyOutResult interface.
func (r *commandResult) SendCopyData(ctx context.Context, copyData []byte, isHeader bool) error {
	if err := r.beforeAdd(); err != nil {
		return err
	}
	if err := r.conn.bufferCopyData(copyData, r); err != nil {
		return err
	}
	if !isHeader {
		r.rowsAffected++
	}
	return nil
}

// SendCopyDone is part of the pgwirebase.Conn interface.
func (r *commandResult) SendCopyDone(ctx context.Context) error {
	r.assertNotReleased()
	r.conn.writerState.fi.registerCmd(r.pos)
	return r.conn.bufferCopyDone()
}

// SetRowsAffected is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) SetRowsAffected(ctx context.Context, n int) {
	r.assertNotReleased()
	r.rowsAffected = n
}

// RowsAffected is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) RowsAffected() int {
	r.assertNotReleased()
	return r.rowsAffected
}

// ResetStmtType is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) ResetStmtType(stmt tree.Statement) {
	r.assertNotReleased()
	r.stmtType = stmt.StatementReturnType()
	r.cmdCompleteTag = stmt.StatementTag()
}

// GetBulkJobId is part of the sql.RestrictedCommandResult interface.
func (r *commandResult) GetBulkJobId() uint64 {
	return r.bulkJobId
}

// release frees the commandResult and allows its memory to be reused.
func (r *commandResult) release() {
	*r = commandResult{released: true}
}

// assertNotReleased asserts that the commandResult is not being used after
// being freed by one of the methods in the CommandResultClose interface. The
// assertion can have false negatives, where it fails to detect a use-after-free
// condition, but will never result in a false positive.
func (r *commandResult) assertNotReleased() {
	if r.released {
		panic("commandResult used after being released")
	}
}

func (c *conn) allocCommandResult() *commandResult {
	r := &c.res
	if r.released {
		r.released = false
	} else {
		// In practice, each conn only ever uses a single commandResult at a
		// time, so we could make this panic. However, doing so would entail
		// complicating the ClientComm interface, which doesn't seem worth it.
		r = new(commandResult)
	}
	return r
}

func (c *conn) newCommandResult(
	descOpt sql.RowDescOpt,
	pos sql.CmdPos,
	stmt tree.Statement,
	formatCodes []pgwirebase.FormatCode,
	conv sessiondatapb.DataConversionConfig,
	location *time.Location,
	limit int,
	portalName string,
	implicitTxn bool,
	portalPausability sql.PortalPausablity,
) sql.CommandResult {
	r := c.allocCommandResult()
	*r = commandResult{
		conn:           c,
		conv:           conv,
		location:       location,
		pos:            pos,
		typ:            commandComplete,
		cmdCompleteTag: stmt.StatementTag(),
		stmtType:       stmt.StatementReturnType(),
		descOpt:        descOpt,
		formatCodes:    formatCodes,
	}
	if limit == 0 {
		return r
	}
	telemetry.Inc(sqltelemetry.PortalWithLimitRequestCounter)
	return &limitedCommandResult{
		limit:            limit,
		portalName:       portalName,
		implicitTxn:      implicitTxn,
		commandResult:    r,
		portalPausablity: portalPausability,
		resumeCh:         make(chan error),
		blockCECh:        make(chan blockChPayload),
	}
}

func (c *conn) newMiscResult(pos sql.CmdPos, typ completionMsgType) *commandResult {
	r := c.allocCommandResult()
	*r = commandResult{
		conn: c,
		pos:  pos,
		typ:  typ,
	}
	return r
}

// limitedCommandResult is a commandResult that has a limit, after which calls
// to AddRow will block until the associated client connection asks for more
// rows. It essentially implements the "execute portal with limit" part of the
// Postgres protocol.
type limitedCommandResult struct {
	*commandResult
	portalName  string
	implicitTxn bool

	seenTuples int
	// If set, an error will be sent to the client if more rows are produced than
	// this limit.
	limit            int
	portalPausablity sql.PortalPausablity

	closed    bool
	resumeCh  chan error
	blockCECh chan blockChPayload
}

type blockChPayload struct {
	ev      fsm.Event
	payload fsm.EventPayload
	err     error
}

var _ sql.RestrictedCommandResult = &limitedCommandResult{}

func (r *limitedCommandResult) SupportsPausing() bool {
	return true
}

func (r *limitedCommandResult) ResumeAfterPause(newLimit int) {
	r.seenTuples = 0
	r.limit = newLimit
	r.resumeCh <- nil
}

func (r *limitedCommandResult) CloseAfterPause() {
	r.closed = true
	r.resumeCh <- sql.ErrLimitedResultClosed
}

func (r *limitedCommandResult) WaitForRows() (fsm.Event, fsm.EventPayload, error) {
	p := <-r.blockCECh
	return p.ev, p.payload, p.err
}

func (r *limitedCommandResult) RowsExhausted(ev fsm.Event, payload fsm.EventPayload, err error) {
	r.blockCECh <- blockChPayload{ev: ev, payload: payload, err: err}
	close(r.blockCECh)
}

// AddRow is part of the sql.RestrictedCommandResult interface.
func (r *limitedCommandResult) AddRow(ctx context.Context, row tree.Datums) error {
	if err := r.commandResult.AddRow(ctx, row); err != nil {
		return err
	}
	r.seenTuples++
	if r.seenTuples < r.limit {
		return nil
	}

	// If we've seen up to the limit of rows, send a "portal suspended" message
	// and block until we're told to resume.
	r.conn.bufferPortalSuspended()
	if err := r.conn.Flush(r.pos); err != nil {
		return err
	}
	r.blockCECh <- blockChPayload{}
	select {
	case err := <-r.resumeCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SupportsAddBatch is part of the sql.RestrictedCommandResult interface.
// TODO(yuzefovich): implement limiting behavior for AddBatch.
func (r *limitedCommandResult) SupportsAddBatch() bool {
	return false
}

// RevokePortalPausability is part of the sql.RestrictedCommandResult interface.
func (r *limitedCommandResult) RevokePortalPausability() error {
	r.portalPausablity = sql.NotPausablePortalForUnsupportedStmt
	return nil
}

// TODO: think about Discard.

func (r *limitedCommandResult) Close(ctx context.Context, t sql.TransactionStatusIndicator) {
	if r.closed {
		r.commandResult.Close(ctx, t)
	}
}

// Get the column index for job id based on the result header defined in
// jobs.BulkJobExecutionResultHeader and jobs.DetachedJobExecutionResultHeader.
func init() {
	jobIdIdxInBulkJobExecutionResultHeader := -1
	jobIdIdxInDetachedJobExecutionResultHeader := -1
	for i, col := range jobs.BulkJobExecutionResultHeader {
		if col.Name == "job_id" {
			jobIdIdxInBulkJobExecutionResultHeader = i
			break
		}
	}
	if jobIdIdxInBulkJobExecutionResultHeader == -1 {
		panic("cannot find the job id column in BulkJobExecutionResultHeader")
	}

	for i, col := range jobs.DetachedJobExecutionResultHeader {
		if col.Name == "job_id" {
			if i != jobIdIdxInBulkJobExecutionResultHeader {
				panic("column index of job_id in DetachedJobExecutionResultHeader and" +
					" BulkJobExecutionResultHeader should be the same")
			} else {
				jobIdIdxInDetachedJobExecutionResultHeader = i
				break
			}
		}
	}
	if jobIdIdxInDetachedJobExecutionResultHeader == -1 {
		panic("cannot find the job id column in DetachedJobExecutionResultHeader")
	}
	JobIdColIdx = jobIdIdxInBulkJobExecutionResultHeader
}
