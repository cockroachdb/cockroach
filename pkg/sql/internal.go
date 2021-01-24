// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var _ sqlutil.InternalExecutor = &InternalExecutor{}

// InternalExecutor can be used internally by code modules to execute SQL
// statements without needing to open a SQL connection.
//
// InternalExecutor can execute one statement at a time. As of 03/2018, it
// doesn't offer a session interface for maintaining session state or for
// running explicit SQL transactions. However, it supports running SQL
// statements inside a higher-lever (KV) txn and inheriting session variables
// from another session.
//
// Methods not otherwise specified are safe for concurrent execution.
type InternalExecutor struct {
	s *Server

	// mon is the monitor used by all queries executed through the
	// InternalExecutor.
	mon *mon.BytesMonitor

	// memMetrics is the memory metrics that queries executed through the
	// InternalExecutor will contribute to.
	memMetrics MemoryMetrics

	// sessionData, if not nil, represents the session variables used by
	// statements executed on this internalExecutor. Note that queries executed by
	// the executor will run on copies of this data.
	sessionData *sessiondata.SessionData

	// The internal executor uses its own Collection. A Collection
	// is a schema cache for each transaction and contains data like the schema
	// modified by a transaction. Occasionally an internal executor is called
	// within the context of a transaction that has modified the schema, the
	// internal executor should see the modified schema. This interface allows
	// the internal executor to modify its Collection to match the
	// Collection of the parent executor.
	tcModifier descs.ModifiedCollectionCopier
}

// MakeInternalExecutor creates an InternalExecutor.
func MakeInternalExecutor(
	ctx context.Context, s *Server, memMetrics MemoryMetrics, settings *cluster.Settings,
) InternalExecutor {
	monitor := mon.NewUnlimitedMonitor(
		ctx,
		"internal SQL executor",
		mon.MemoryResource,
		memMetrics.CurBytesCount,
		memMetrics.MaxBytesHist,
		math.MaxInt64, /* noteworthy */
		settings,
	)
	return InternalExecutor{
		s:          s,
		mon:        monitor,
		memMetrics: memMetrics,
	}
}

// SetSessionData binds the session variables that will be used by queries
// performed through this executor from now on.
//
// SetSessionData cannot be called concurently with query execution.
func (ie *InternalExecutor) SetSessionData(sessionData *sessiondata.SessionData) {
	ie.s.populateMinimalSessionData(sessionData)
	ie.sessionData = sessionData
}

// initConnEx creates a connExecutor and runs it on a separate goroutine. It
// returns a StmtBuf into which commands can be pushed and a WaitGroup that will
// be signaled when connEx.run() returns.
//
// If txn is not nil, the statement will be executed in the respective txn.
//
// sd will constitute the executor's session state.
func (ie *InternalExecutor) initConnEx(
	ctx context.Context,
	txn *kv.Txn,
	ch chan ieIteratorResult,
	sd *sessiondata.SessionData,
	syncCallback func([]resWithPos),
	errCallback func(error),
) (*StmtBuf, *sync.WaitGroup) {
	clientComm := &internalClientComm{
		sync: syncCallback,
		// init lastDelivered below the position of the first result (0).
		lastDelivered: -1,
		ch:            ch,
	}

	// When the connEx is serving an internal executor, it can inherit the
	// application name from an outer session. This happens e.g. during ::regproc
	// casts and built-in functions that use SQL internally. In that case, we do
	// not want to record statistics against the outer application name directly;
	// instead we want to use a separate bucket. However we will still want to
	// have separate buckets for different applications so that we can measure
	// their respective "pressure" on internal queries. Hence the choice here to
	// add the delegate prefix to the current app name.
	var appStatsBucketName string
	if !strings.HasPrefix(sd.ApplicationName, catconstants.InternalAppNamePrefix) {
		appStatsBucketName = catconstants.DelegatedAppNamePrefix + sd.ApplicationName
	} else {
		// If this is already an "internal app", don't put more prefix.
		appStatsBucketName = sd.ApplicationName
	}
	appStats := ie.s.sqlStats.getStatsForApplication(appStatsBucketName)

	stmtBuf := NewStmtBuf()
	var ex *connExecutor
	if txn == nil {
		ex = ie.s.newConnExecutor(
			ctx,
			sd,
			nil, /* sdDefaults */
			stmtBuf,
			clientComm,
			ie.memMetrics,
			&ie.s.InternalMetrics,
			appStats,
		)
	} else {
		ex = ie.s.newConnExecutorWithTxn(
			ctx,
			sd,
			nil, /* sdDefaults */
			stmtBuf,
			clientComm,
			ie.mon,
			ie.memMetrics,
			&ie.s.InternalMetrics,
			txn,
			ie.tcModifier,
			appStats,
		)
	}
	ex.executorType = executorTypeInternal

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if err := ex.run(ctx, ie.mon, mon.BoundAccount{} /*reserved*/, nil /* cancel */); err != nil {
			sqltelemetry.RecordError(ctx, err, &ex.server.cfg.Settings.SV)
			errCallback(err)
		}
		closeMode := normalClose
		if txn != nil {
			closeMode = externalTxnClose
		}
		ex.close(ctx, closeMode)
		wg.Done()
	}()
	return stmtBuf, &wg
}

type ieIteratorResult struct {
	// Exactly one of these fields will be set.
	row  tree.Datums
	cols colinfo.ResultColumns
	err  error
}

type rowsIterator struct {
	ch chan ieIteratorResult

	lastRow tree.Datums
	lastErr error
	done    bool

	resultCols colinfo.ResultColumns

	// errCallback is an optional callback that will be called exactly once on
	// an error returned by Next() or Close().
	errCallback func(err error) error

	// stmtBuf will be closed on Close(). This is necessary in order to tell
	// the connExecutor's goroutine to exit when the iterator's user wants to
	// short-circuit the iteration (i.e. before Next() returns false).
	stmtBuf *StmtBuf

	// wg can be used to wait for the connExecutor's goroutine to exit.
	wg *sync.WaitGroup

	// sp will finished on Close().
	sp *tracing.Span
}

var _ sqlutil.InternalRows = &rowsIterator{}

func (r *rowsIterator) Next(ctx context.Context) (_ bool, retErr error) {
	if r.done {
		return false, r.lastErr
	}

	defer func() {
		// If the iterator has just reached its terminal state, we'll close it
		// automatically.
		if r.done {
			// We can ignore the returned error because Close() will update
			// r.lastErr if necessary.
			_ /* err */ = r.Close()
		}
		if r.errCallback != nil {
			r.lastErr = r.errCallback(r.lastErr)
			r.errCallback = nil
		}
		retErr = r.lastErr
	}()

	select {
	case next, ok := <-r.ch:
		if !ok {
			r.done = true
			return false, nil
		}
		if next.row != nil {
			// No need to make a copy because streamingCommandResult does that
			// for us.
			r.lastRow = next.row
			return true, nil
		}
		if next.cols != nil {
			// Ignore the result columns if they are already set on the
			// iterator: it is possible for ROWS statement type to be executed
			// in a 'rows affected' mode, in such case the correct columns are
			// set manually when instantiating an iterator, but the result
			// columns of the statement are also sent by SetColumns() (we need
			// to keep the former).
			if r.resultCols == nil {
				r.resultCols = next.cols
			}
			return r.Next(ctx)
		}
		if next.err == nil {
			next.err = errors.AssertionFailedf("unexpectedly empty ieRowResult object")
		}
		r.lastErr = next.err
		r.done = true
		return false, r.lastErr

	case <-ctx.Done():
		r.lastErr = ctx.Err()
		r.done = true
		return false, r.lastErr
	}
}

func (r *rowsIterator) Cur() tree.Datums {
	return r.lastRow
}

func (r *rowsIterator) Close() error {
	// Closing the stmtBuf will tell the connExecutor to stop executing commands
	// (if it hasn't exited yet).
	r.stmtBuf.Close()
	// We need to finish the span but only after the connExecutor goroutine is
	// done.
	defer func() {
		if r.sp != nil {
			r.wg.Wait()
			r.sp.Finish()
			r.sp = nil
		}
	}()

	// We also need to receive from the channel since the connExecutor goroutine
	// might be blocked on sending the row in AddRow().
	for {
		select {
		case res, ok := <-r.ch:
			if !ok {
				return r.lastErr
			}
			// We are only interested in possible errors if we haven't already
			// seen one. All other things are simply ignored.
			if res.err != nil && r.lastErr == nil {
				r.lastErr = res.err
				if r.errCallback != nil {
					r.lastErr = r.errCallback(r.lastErr)
					r.errCallback = nil
				}
			}
		default:
			return r.lastErr
		}
	}
}

func (r *rowsIterator) Types() colinfo.ResultColumns {
	return r.resultCols
}

// Query executes the supplied SQL statement and returns the resulting rows.
// If no user has been previously set through SetSessionData, the statement is
// executed as the root user.
//
// If txn is not nil, the statement will be executed in the respective txn.
//
// Query is deprecated because it may transparently execute a query as root. Use
// QueryEx instead.
func (ie *InternalExecutor) Query(
	ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
) ([]tree.Datums, error) {
	return ie.QueryEx(ctx, opName, txn, ie.maybeRootSessionDataOverride(opName), stmt, qargs...)
}

// QueryEx is like Query, but allows the caller to override some session data
// fields (e.g. the user).
//
// The fields set in session that are set override the respective fields if they
// have previously been set through SetSessionData().
func (ie *InternalExecutor) QueryEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, error) {
	datums, _, err := ie.queryInternalBuffered(ctx, opName, txn, session, stmt, 0 /* limit */, qargs...)
	return datums, err
}

// QueryWithCols is like QueryEx, but it also returns the computed ResultColumns
// of the input query.
func (ie *InternalExecutor) QueryWithCols(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, colinfo.ResultColumns, error) {
	return ie.queryInternalBuffered(ctx, opName, txn, session, stmt, 0 /* limit */, qargs...)
}

func (ie *InternalExecutor) queryInternalBuffered(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	sessionDataOverride sessiondata.InternalExecutorOverride,
	stmt string,
	// Non-zero limit specifies the limit on the number of rows returned.
	limit int,
	qargs ...interface{},
) ([]tree.Datums, colinfo.ResultColumns, error) {
	it, err := ie.execInternal(ctx, opName, txn, sessionDataOverride, stmt, false /* emitRowsAffected */, qargs...)
	if err != nil {
		return nil, nil, err
	}
	var rows []tree.Datums
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		rows = append(rows, it.Cur())
		if limit != 0 && len(rows) == limit {
			// We have accumulated the requested number of rows, so we can
			// short-circuit the iteration.
			err = it.Close()
			break
		}
	}
	if err != nil {
		return nil, nil, err
	}
	return rows, it.Types(), nil
}

// QueryRow is like Query, except it returns a single row, or nil if not row is
// found, or an error if more that one row is returned.
//
// QueryRow is deprecated (like Query). Use QueryRowEx() instead.
func (ie *InternalExecutor) QueryRow(
	ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
) (tree.Datums, error) {
	return ie.QueryRowEx(ctx, opName, txn, ie.maybeRootSessionDataOverride(opName), stmt, qargs...)
}

// QueryRowEx is like QueryRow, but allows the caller to override some session data
// fields (e.g. the user).
//
// The fields set in session that are set override the respective fields if they
// have previously been set through SetSessionData().
func (ie *InternalExecutor) QueryRowEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	rows, _, err := ie.queryInternalBuffered(ctx, opName, txn, session, stmt, 2 /* limit */, qargs...)
	if err != nil {
		return nil, err
	}
	switch len(rows) {
	case 0:
		return nil, nil
	case 1:
		return rows[0], nil
	default:
		return nil, &tree.MultipleResultsError{SQL: stmt}
	}
}

// Exec executes the supplied SQL statement and returns the number of rows
// affected (not like the results; see Query()). If no user has been previously
// set through SetSessionData, the statement is executed as the root user.
//
// If txn is not nil, the statement will be executed in the respective txn.
//
// Exec is deprecated because it may transparently execute a query as root. Use
// ExecEx instead.
func (ie *InternalExecutor) Exec(
	ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
) (int, error) {
	return ie.ExecEx(ctx, opName, txn, ie.maybeRootSessionDataOverride(opName), stmt, qargs...)
}

// ExecEx is like Exec, but allows the caller to override some session data
// fields (e.g. the user).
//
// The fields set in session that are set override the respective fields if they
// have previously been set through SetSessionData().
func (ie *InternalExecutor) ExecEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (int, error) {
	it, err := ie.execInternal(ctx, opName, txn, session, stmt, true /* emitRowsAffected */, qargs...)
	if err != nil {
		return 0, err
	}
	// 'rows affected' row is the very last one emitted by the iterator.
	var ok bool
	var lastRow tree.Datums
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		lastRow = it.Cur()
	}
	if err != nil {
		return 0, err
	}
	if lastRow == nil {
		return 0, errors.AssertionFailedf("unexpectedly didn't find 'rows affected' row")
	}
	return int(tree.MustBeDInt(lastRow[0])), nil
}

// QueryIterator executes the query, returning an iterator that can be used
// to get the results.
//
// QueryIterator is deprecated because it may transparently execute a query
// as root. Use QueryIteratorEx instead.
func (ie *InternalExecutor) QueryIterator(
	ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
) (sqlutil.InternalRows, error) {
	return ie.QueryIteratorEx(ctx, opName, txn, ie.maybeRootSessionDataOverride(opName), stmt, qargs...)
}

// QueryIteratorEx executes the query, returning an iterator that can be used
// to get the results.
func (ie *InternalExecutor) QueryIteratorEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (sqlutil.InternalRows, error) {
	return ie.execInternal(ctx, opName, txn, session, stmt, false /* emitRowsAffected */, qargs...)
}

// applyOverrides overrides the respective fields from sd for all the fields set on o.
func applyOverrides(o sessiondata.InternalExecutorOverride, sd *sessiondata.SessionData) {
	if !o.User.Undefined() {
		sd.UserProto = o.User.EncodeProto()
	}
	if o.Database != "" {
		sd.Database = o.Database
	}
	if o.ApplicationName != "" {
		sd.ApplicationName = o.ApplicationName
	}
	if o.SearchPath != nil {
		sd.SearchPath = *o.SearchPath
	}
	if o.DatabaseIDToTempSchemaID != nil {
		sd.DatabaseIDToTempSchemaID = o.DatabaseIDToTempSchemaID
	}
}

func (ie *InternalExecutor) maybeRootSessionDataOverride(
	opName string,
) sessiondata.InternalExecutorOverride {
	if ie.sessionData == nil {
		return sessiondata.InternalExecutorOverride{
			User:            security.RootUserName(),
			ApplicationName: catconstants.InternalAppNamePrefix + "-" + opName,
		}
	}
	o := sessiondata.InternalExecutorOverride{}
	if ie.sessionData.User().Undefined() {
		o.User = security.RootUserName()
	}
	if ie.sessionData.ApplicationName == "" {
		o.ApplicationName = catconstants.InternalAppNamePrefix + "-" + opName
	}
	return o
}

var rowsAffectedResultColumns = colinfo.ResultColumns{
	colinfo.ResultColumn{
		Name: "rows_affected",
		Typ:  types.Int,
	},
}

var ieIteratorChannelBufferSize = util.ConstantWithMetamorphicTestRange(
	"iterator-channel-buffer-size",
	1, /* defaultValue */
	1, /* min */
	1, /* max */
)

// execInternal executes a statement.
//
// sessionDataOverride can be used to control select fields in the executor's
// session data. It overrides what has been previously set through
// SetSessionData(), if anything.
func (ie *InternalExecutor) execInternal(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	sessionDataOverride sessiondata.InternalExecutorOverride,
	stmt string,
	// If true, the very last row emitted by the iterator will be
	// 'rows affected' row.
	emitRowsAffected bool,
	qargs ...interface{},
) (r *rowsIterator, retErr error) {
	ctx = logtags.AddTag(ctx, "intExec", opName)

	var sd *sessiondata.SessionData
	if ie.sessionData != nil {
		// TODO(andrei): Properly clone (deep copy) ie.sessionData.
		sdCopy := *ie.sessionData
		sd = &sdCopy
	} else {
		sd = ie.s.newSessionData(SessionArgs{})
	}
	applyOverrides(sessionDataOverride, sd)
	if sd.User().Undefined() {
		return nil, errors.AssertionFailedf("no user specified for internal query")
	}
	if sd.ApplicationName == "" {
		sd.ApplicationName = catconstants.InternalAppNamePrefix + "-" + opName
	}

	// The returned span is finished by this function in all error paths, but if
	// an iterator is returned, then we transfer the responsibility of closing
	// the span to the iterator. This is necessary so that the connExecutor
	// exits before the span is finished.
	ctx, sp := tracing.EnsureChildSpan(ctx, ie.s.cfg.AmbientCtx.Tracer, opName)

	var stmtBuf *StmtBuf
	var wg *sync.WaitGroup

	defer func() {
		// We wrap errors with the opName, but not if they're retriable - in that
		// case we need to leave the error intact so that it can be retried at a
		// higher level.
		//
		// TODO(knz): track the callers and check whether opName could be turned
		// into a type safe for reporting.
		if retErr != nil {
			if !errIsRetriable(retErr) {
				retErr = errors.Wrapf(retErr, "%s", opName)
			}
			if stmtBuf != nil {
				// If stmtBuf is non-nil, then the connExecutor goroutine has
				// been spawn up - we gotta wait for it to exit.
				stmtBuf.Close()
				wg.Wait()
			}
			sp.Finish()
		} else {
			// r must be non-nil here.
			r.errCallback = func(err error) error {
				if err != nil && !errIsRetriable(err) {
					err = errors.Wrapf(err, "%s", opName)
				}
				return err
			}
			r.sp = sp
		}
	}()

	timeReceived := timeutil.Now()
	parseStart := timeReceived
	parsed, err := parser.ParseOne(stmt)
	if err != nil {
		return nil, err
	}
	parseEnd := timeutil.Now()

	emitRowsAffected = emitRowsAffected || parsed.AST.StatementType() != tree.Rows

	// resPos will be set to the position of the command that represents the
	// statement we care about before that command is sent for execution.
	var resPos CmdPos

	ch := make(chan ieIteratorResult, ieIteratorChannelBufferSize)
	var resultsReceived bool
	syncCallback := func(results []resWithPos) {
		resultsReceived = true
		defer close(ch)
		for _, res := range results {
			if res.pos == resPos {
				if emitRowsAffected {
					ch <- ieIteratorResult{row: tree.Datums{tree.NewDInt(tree.DInt(res.RowsAffected()))}}
				}
				return
			}
			if res.Err() != nil {
				// If we encounter an error, there's no point in looking
				// further; the	rest of the commands in the batch have been
				// skipped.
				ch <- ieIteratorResult{err: res.Err()}
				return
			}
		}
		ch <- ieIteratorResult{err: errors.AssertionFailedf("missing result for pos: %d and no previous error", resPos)}
	}
	errCallback := func(err error) {
		if resultsReceived {
			return
		}
		ch <- ieIteratorResult{err: err}
		close(ch)
	}
	stmtBuf, wg = ie.initConnEx(ctx, txn, ch, sd, syncCallback, errCallback)

	// Transforms the args to datums. The datum types will be passed as type hints
	// to the PrepareStmt command.
	datums, err := golangFillQueryArguments(qargs...)
	if err != nil {
		return nil, err
	}
	typeHints := make(tree.PlaceholderTypes, len(datums))
	for i, d := range datums {
		// Arg numbers start from 1.
		typeHints[tree.PlaceholderIdx(i)] = d.ResolvedType()
	}
	if len(qargs) == 0 {
		resPos = 0
		if err := stmtBuf.Push(
			ctx,
			ExecStmt{
				Statement:    parsed,
				TimeReceived: timeReceived,
				ParseStart:   parseStart,
				ParseEnd:     parseEnd,
			}); err != nil {
			return nil, err
		}
	} else {
		resPos = 2
		if err := stmtBuf.Push(
			ctx,
			PrepareStmt{
				Statement:  parsed,
				ParseStart: parseStart,
				ParseEnd:   parseEnd,
				TypeHints:  typeHints,
			},
		); err != nil {
			return nil, err
		}

		if err := stmtBuf.Push(ctx, BindStmt{internalArgs: datums}); err != nil {
			return nil, err
		}

		if err := stmtBuf.Push(ctx, ExecPortal{TimeReceived: timeReceived}); err != nil {
			return nil, err
		}
	}
	if err := stmtBuf.Push(ctx, Sync{}); err != nil {
		return nil, err
	}

	var resultColumns colinfo.ResultColumns
	if emitRowsAffected {
		resultColumns = rowsAffectedResultColumns
	}
	return &rowsIterator{
		ch:         ch,
		resultCols: resultColumns,
		stmtBuf:    stmtBuf,
		wg:         wg,
	}, nil
}

// internalClientComm is an implementation of ClientComm used by the
// InternalExecutor. Result rows are buffered in memory.
type internalClientComm struct {
	// results will contain the results of the commands executed by an
	// InternalExecutor.
	results []resWithPos

	lastDelivered CmdPos

	// sync, if set, is called whenever a Sync is executed.
	sync func([]resWithPos)
	ch   chan ieIteratorResult
}

var _ ClientComm = &internalClientComm{}

type resWithPos struct {
	*streamingCommandResult
	pos CmdPos
}

// CreateStatementResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateStatementResult(
	_ tree.Statement,
	_ RowDescOpt,
	pos CmdPos,
	_ []pgwirebase.FormatCode,
	_ sessiondatapb.DataConversionConfig,
	_ *time.Location,
	_ int,
	_ string,
	_ bool,
) CommandResult {
	return icc.createRes(pos, nil /* onClose */)
}

// createRes creates a result. onClose, if not nil, is called when the result is
// closed.
func (icc *internalClientComm) createRes(pos CmdPos, onClose func(error)) *streamingCommandResult {
	res := &streamingCommandResult{
		ch: icc.ch,
		closeCallback: func(res *streamingCommandResult, typ resCloseType, err error) {
			if typ == discarded {
				return
			}
			icc.results = append(icc.results, resWithPos{streamingCommandResult: res, pos: pos})
			if onClose != nil {
				onClose(err)
			}
		},
	}
	return res
}

// CreatePrepareResult is part of the ClientComm interface.
func (icc *internalClientComm) CreatePrepareResult(pos CmdPos) ParseResult {
	return icc.createRes(pos, nil /* onClose */)
}

// CreateBindResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateBindResult(pos CmdPos) BindResult {
	return icc.createRes(pos, nil /* onClose */)
}

// CreateSyncResult is part of the ClientComm interface.
//
// The returned SyncResult will call the sync callback when its closed.
func (icc *internalClientComm) CreateSyncResult(pos CmdPos) SyncResult {
	return icc.createRes(pos, func(err error) {
		results := make([]resWithPos, len(icc.results))
		copy(results, icc.results)
		icc.results = icc.results[:0]
		icc.sync(results)
		icc.lastDelivered = pos
	} /* onClose */)
}

// LockCommunication is part of the ClientComm interface.
func (icc *internalClientComm) LockCommunication() ClientLock {
	return (*noopClientLock)(icc)
}

// Flush is part of the ClientComm interface.
func (icc *internalClientComm) Flush(pos CmdPos) error {
	return nil
}

// CreateDescribeResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateDescribeResult(pos CmdPos) DescribeResult {
	return icc.createRes(pos, nil /* onClose */)
}

// CreateDeleteResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateDeleteResult(pos CmdPos) DeleteResult {
	panic("unimplemented")
}

// CreateFlushResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateFlushResult(pos CmdPos) FlushResult {
	panic("unimplemented")
}

// CreateErrorResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateErrorResult(pos CmdPos) ErrorResult {
	panic("unimplemented")
}

// CreateEmptyQueryResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateEmptyQueryResult(pos CmdPos) EmptyQueryResult {
	panic("unimplemented")
}

// CreateCopyInResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateCopyInResult(pos CmdPos) CopyInResult {
	panic("unimplemented")
}

// CreateDrainResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateDrainResult(pos CmdPos) DrainResult {
	panic("unimplemented")
}

// noopClientLock is an implementation of ClientLock that says that no results
// have been communicated to the client.
type noopClientLock internalClientComm

// Close is part of the ClientLock interface.
func (ncl *noopClientLock) Close() {}

// ClientPos is part of the ClientLock interface.
func (ncl *noopClientLock) ClientPos() CmdPos {
	return ncl.lastDelivered
}

// RTrim is part of the ClientLock interface.
func (ncl *noopClientLock) RTrim(_ context.Context, pos CmdPos) {
	var i int
	var r resWithPos
	for i, r = range ncl.results {
		if r.pos >= pos {
			break
		}
	}
	ncl.results = ncl.results[:i]
}
