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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
// from another session (for the latter see SessionBoundInternalExecutor).
type InternalExecutor struct {
	internalExecutorImpl
}

// SessionBoundInternalExecutor is like InternalExecutor, except that it is
// initialized with values for session variables. Conversely, it doesn't offer
// the *WithUser methods of the InternalExecutor.
type SessionBoundInternalExecutor struct {
	impl internalExecutorImpl
}

// internalExecutorImpl supports the implementation of InternalExecutor and
// SessionBoundInternalExecutor.
type internalExecutorImpl struct {
	s *Server

	// mon is the monitor used by all queries executed through the
	// InternalExecutor.
	mon *mon.BytesMonitor

	// memMetrics is the memory metrics that queries executed through the
	// InternalExecutor will contribute to.
	memMetrics MemoryMetrics

	// sessionData, if not nil, represents the session variables used by
	// statements executed on this internalExecutor. This field supports the
	// implementation of SessionBoundInternalExecutor. Note that a session bound
	// internal executor cannot modify session data.
	sessionData *sessiondata.SessionData

	// The internal executor uses its own TableCollection. A TableCollection
	// is a schema cache for each transaction and contains data like the schema
	// modified by a transaction. Occasionally an internal executor is called
	// within the context of a transaction that has modified the schema, the
	// internal executor should see the modified schema. This interface allows
	// the internal executor to modify its TableCollection to match the
	// TableCollection of the parent executor.
	tcModifier tableCollectionModifier
}

// MakeInternalExecutor creates an InternalExecutor.
func MakeInternalExecutor(
	ctx context.Context, s *Server, memMetrics MemoryMetrics, settings *cluster.Settings,
) InternalExecutor {
	monitor := mon.MakeUnlimitedMonitor(
		ctx,
		"internal SQL executor",
		mon.MemoryResource,
		memMetrics.CurBytesCount,
		memMetrics.MaxBytesHist,
		math.MaxInt64, /* noteworthy */
		settings,
	)
	return InternalExecutor{
		internalExecutorImpl: internalExecutorImpl{
			s:          s,
			mon:        &monitor,
			memMetrics: memMetrics,
		},
	}
}

// NewSessionBoundInternalExecutor creates a SessionBoundInternalExecutor.
func NewSessionBoundInternalExecutor(
	ctx context.Context,
	sessionData *sessiondata.SessionData,
	s *Server,
	memMetrics MemoryMetrics,
	settings *cluster.Settings,
) *SessionBoundInternalExecutor {
	monitor := mon.MakeUnlimitedMonitor(
		ctx,
		"internal SQL executor",
		mon.MemoryResource,
		memMetrics.CurBytesCount,
		memMetrics.MaxBytesHist,
		math.MaxInt64, /* noteworthy */
		settings,
	)
	return &SessionBoundInternalExecutor{
		impl: internalExecutorImpl{
			s:           s,
			mon:         &monitor,
			memMetrics:  memMetrics,
			sessionData: sessionData,
		},
	}
}

// initConnEx creates a connExecutor and runs it on a separate goroutine. It
// returns a StmtBuf into which commands can be pushed and a WaitGroup that will
// be signaled when connEx.run() returns.
//
// If txn is not nil, the statement will be executed in the respective txn.
//
// sargs, if not nil, is used to initialize the executor's session data. If nil,
// then ie.sessionData must be set and it will be used (i.e. the executor must
// be "session bound").
func (ie *internalExecutorImpl) initConnEx(
	ctx context.Context,
	txn *client.Txn,
	sargs SessionArgs,
	syncCallback func([]resWithPos),
	errCallback func(error),
) (*StmtBuf, *sync.WaitGroup, error) {
	clientComm := &internalClientComm{
		sync: syncCallback,
		// init lastDelivered below the position of the first result (0).
		lastDelivered: -1,
	}

	var sd *sessiondata.SessionData
	var sdMut *sessionDataMutator
	if sargs.isDefined() {
		if ie.sessionData != nil {
			log.Fatal(ctx, "sargs used on a session bound executor")
		}
		sd, sdMut = ie.s.newSessionDataAndMutator(sargs)
	} else {
		if ie.sessionData == nil {
			log.Fatal(ctx, "initConnEx called with no sargs, and the executor is also not session bound")
		}
		sd = ie.sessionData
		// sdMut stays nil for session bound executors.
	}

	stmtBuf := NewStmtBuf()
	var ex *connExecutor
	var err error
	if txn == nil {
		ex, err = ie.s.newConnExecutor(
			ctx,
			sd, sdMut,
			stmtBuf,
			clientComm,
			ie.memMetrics,
			&ie.s.InternalMetrics)
	} else {
		ex, err = ie.s.newConnExecutorWithTxn(
			ctx,
			sd, sdMut,
			stmtBuf,
			clientComm,
			ie.mon,
			ie.memMetrics,
			&ie.s.InternalMetrics,
			txn,
			ie.tcModifier)
	}
	if err != nil {
		return nil, nil, err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if err := ex.run(ctx, ie.mon, mon.BoundAccount{} /*reserved*/, nil /* cancel */); err != nil {
			ex.recordError(ctx, err)
			errCallback(err)
		}
		closeMode := normalClose
		if txn != nil {
			closeMode = externalTxnClose
		}
		ex.close(ctx, closeMode)
		wg.Done()
	}()
	return stmtBuf, &wg, nil
}

// Query executes the supplied SQL statement and returns the resulting rows.
// The statement is executed as the root user.
//
// If txn is not nil, the statement will be executed in the respective txn.
func (ie *InternalExecutor) Query(
	ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
) ([]tree.Datums, error) {
	datums, _, err := ie.queryInternal(
		ctx, opName, txn,
		internalExecRootSession,
		SessionArgs{},
		stmt, qargs...)
	return datums, err
}

// QueryWithCols is like Query, but it also returns the computed ResultColumns
// of the input query.
func (ie *InternalExecutor) QueryWithCols(
	ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	return ie.queryInternal(
		ctx, opName, txn,
		internalExecRootSession,
		SessionArgs{},
		stmt, qargs...)
}

func (ie *internalExecutorImpl) queryInternal(
	ctx context.Context,
	opName string,
	txn *client.Txn,
	sessionMode internalExecSessionMode,
	sargs SessionArgs,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	res, err := ie.execInternal(ctx, opName, txn, sessionMode, sargs, stmt, qargs...)
	if err != nil {
		return nil, nil, err
	}
	return res.rows, res.cols, res.err
}

// QueryWithUser is like Query, except it changes the username to that
// specified.
func (ie *InternalExecutor) QueryWithUser(
	ctx context.Context,
	opName string,
	txn *client.Txn,
	userName string,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	return ie.queryInternal(ctx,
		opName, txn, internalExecFixedUserSession, SessionArgs{User: userName}, stmt, qargs...)
}

// QueryRow is like Query, except it returns a single row, or nil if not row is
// found, or an error if more that one row is returned.
func (ie *InternalExecutor) QueryRow(
	ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
) (tree.Datums, error) {
	rows, err := ie.Query(ctx, opName, txn, stmt, qargs...)
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

// Exec executes the supplied SQL statement. Statements are currently executed
// as the root user with the system database as current database.
//
// If txn is not nil, the statement will be executed in the respective txn.
//
// Returns the number of rows affected.
func (ie *InternalExecutor) Exec(
	ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
) (int, error) {
	res, err := ie.execInternal(
		ctx, opName, txn, internalExecRootSession, SessionArgs{}, stmt, qargs...,
	)
	if err != nil {
		return 0, err
	}
	return res.rowsAffected, res.err
}

// ExecWithUser is like Exec, except it changes the username to that
// specified.
func (ie *InternalExecutor) ExecWithUser(
	ctx context.Context,
	opName string,
	txn *client.Txn,
	userName string,
	stmt string,
	qargs ...interface{},
) (int, error) {
	res, err := ie.execInternal(ctx,
		opName, txn, internalExecFixedUserSession, SessionArgs{User: userName}, stmt, qargs...)
	if err != nil {
		return 0, err
	}
	return res.rowsAffected, res.err
}

// Query executes the supplied SQL statement and returns the resulting rows.
// The statement is executed as the root user.
//
// If txn is not nil, the statement will be executed in the respective txn.
func (ie *SessionBoundInternalExecutor) Query(
	ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
) ([]tree.Datums, error) {
	rows, _, err := ie.impl.queryInternal(
		ctx, opName, txn,
		internalExecInheritSession,
		SessionArgs{},
		stmt, qargs...)
	return rows, err
}

// QueryWithCols is like Query, but it also returns the computed ResultColumns
// of the input query.
func (ie *SessionBoundInternalExecutor) QueryWithCols(
	ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	return ie.impl.queryInternal(
		ctx, opName, txn,
		internalExecInheritSession,
		SessionArgs{},
		stmt, qargs...)
}

// QueryRow is like Query, except it returns a single row, or nil if not row is
// found, or an error if more that one row is returned.
func (ie *SessionBoundInternalExecutor) QueryRow(
	ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
) (tree.Datums, error) {
	rows, _ /* cols */, err := ie.impl.queryInternal(
		ctx, opName, txn,
		internalExecInheritSession,
		SessionArgs{},
		stmt, qargs...)
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

// Exec executes the supplied SQL statement.
//
// If txn is not nil, the statement will be executed in the respective txn.
//
// Returns the number of rows affected.
func (ie *SessionBoundInternalExecutor) Exec(
	ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
) (int, error) {
	res, err := ie.impl.execInternal(
		ctx, opName, txn,
		internalExecInheritSession,
		SessionArgs{},
		stmt, qargs...,
	)
	if err != nil {
		return 0, err
	}
	return res.rowsAffected, res.err
}

type result struct {
	rows         []tree.Datums
	rowsAffected int
	cols         sqlbase.ResultColumns
	err          error
}

type internalExecSessionMode int

const (
	// internalExecInheritSession will pick up the internalExecutor's
	// existing sessionData.
	internalExecInheritSession internalExecSessionMode = iota
	// internalExecRootSession will create a new session from scratch
	// with the root user, the system database as current database, an
	// auto-generated internal application_name and all session defaults
	// otherwise.
	// This is equivalent to passing internalExecUseFixedUserSession with
	// the result of newInternalSessionUserArgs(security.RootUser).
	internalExecRootSession
	// internalExecFixedUser will use the provided username in
	// SessionArgs and reset everything else as per internalExecRootSession.
	internalExecFixedUserSession
)

// execInternal executes a statement.
//
// sargs, if not nil, is used to initialize the executor's session data. If nil,
// then ie.sessionData must be set and it will be used (i.e. the executor must
// be "session bound").
func (ie *internalExecutorImpl) execInternal(
	ctx context.Context,
	opName string,
	txn *client.Txn,
	sessionMode internalExecSessionMode,
	sargs SessionArgs,
	stmt string,
	qargs ...interface{},
) (retRes result, retErr error) {
	ctx = logtags.AddTag(ctx, "intExec", opName)

	switch sessionMode {
	case internalExecInheritSession, internalExecRootSession:
		if sargs.isDefined() {
			log.Fatalf(ctx, "programming error: session args provided with mode %d", sessionMode)
		}
		if sessionMode == internalExecRootSession {
			sargs = SessionArgs{User: security.RootUser}
		}
	}

	switch sessionMode {
	case internalExecFixedUserSession:
		if !sargs.isDefined() {
			log.Fatal(ctx, "programming error: mode fixed user with undefined sargs")
		}
		// Clear all fields except user.
		sargs = SessionArgs{User: sargs.User}
		fallthrough
	case internalExecRootSession:
		sargs.SessionDefaults = map[string]string{
			"database":         "system",
			"application_name": sqlbase.InternalAppNamePrefix + "-" + opName,
		}
	}

	defer func() {
		// We wrap errors with the opName, but not if they're retriable - in that
		// case we need to leave the error intact so that it can be retried at a
		// higher level.
		if retErr != nil && !errIsRetriable(retErr) {
			retErr = errors.Wrapf(retErr, opName)
		}
		if retRes.err != nil && !errIsRetriable(retRes.err) {
			retRes.err = errors.Wrapf(retRes.err, opName)
		}
	}()

	ctx, sp := tracing.EnsureChildSpan(ctx, ie.s.cfg.AmbientCtx.Tracer, opName)
	defer sp.Finish()

	timeReceived := timeutil.Now()
	parseStart := timeReceived
	parsed, err := parser.ParseOne(stmt)
	if err != nil {
		return result{}, err
	}
	parseEnd := timeutil.Now()

	// resPos will be set to the position of the command that represents the
	// statement we care about before that command is sent for execution.
	var resPos CmdPos

	resCh := make(chan result)
	var resultsReceived bool
	syncCallback := func(results []resWithPos) {
		resultsReceived = true
		for _, res := range results {
			if res.pos == resPos {
				resCh <- result{rows: res.rows, rowsAffected: res.RowsAffected(), cols: res.cols, err: res.Err()}
				return
			}
			if res.err != nil {
				// If we encounter an error, there's no point in looking further; the
				// rest of the commands in the batch have been skipped.
				resCh <- result{err: res.Err()}
				return
			}
		}
		resCh <- result{err: errors.AssertionFailedf("missing result for pos: %d and no previous error", resPos)}
	}
	errCallback := func(err error) {
		if resultsReceived {
			return
		}
		resCh <- result{err: err}
	}
	stmtBuf, wg, err := ie.initConnEx(ctx, txn, sargs, syncCallback, errCallback)
	if err != nil {
		return result{}, err
	}

	// Transforms the args to datums. The datum types will be passed as type hints
	// to the PrepareStmt command.
	datums := golangFillQueryArguments(qargs...)
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
			return result{}, err
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
			return result{}, err
		}

		if err := stmtBuf.Push(ctx, BindStmt{internalArgs: datums}); err != nil {
			return result{}, err
		}

		if err := stmtBuf.Push(ctx, ExecPortal{TimeReceived: timeReceived}); err != nil {
			return result{}, err
		}
	}
	if err := stmtBuf.Push(ctx, Sync{}); err != nil {
		return result{}, err
	}

	res := <-resCh
	stmtBuf.Close()
	wg.Wait()
	return res, nil
}

// internalClientComm is an implementation of ClientComm used by the
// InternalExecutor. Result rows are buffered in memory.
type internalClientComm struct {
	// results will contain the results of the commands executed by an
	// InternalExecutor.
	results []resWithPos

	lastDelivered CmdPos

	// sync, if set, is called whenever a Sync is executed. It returns all the
	// results since the previous Sync.
	sync func([]resWithPos)
}

type resWithPos struct {
	*bufferedCommandResult
	pos CmdPos
}

// CreateStatementResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateStatementResult(
	_ tree.Statement,
	_ RowDescOpt,
	pos CmdPos,
	_ []pgwirebase.FormatCode,
	_ sessiondata.DataConversionConfig,
	_ int,
	_ string,
	_ bool,
) CommandResult {
	return icc.createRes(pos, nil /* onClose */)
}

// createRes creates a result. onClose, if not nil, is called when the result is
// closed.
func (icc *internalClientComm) createRes(pos CmdPos, onClose func(error)) *bufferedCommandResult {
	res := &bufferedCommandResult{
		closeCallback: func(res *bufferedCommandResult, typ resCloseType, err error) {
			if typ == discarded {
				return
			}
			icc.results = append(icc.results, resWithPos{bufferedCommandResult: res, pos: pos})
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
	return &noopClientLock{
		clientComm: icc,
	}
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
type noopClientLock struct {
	clientComm *internalClientComm
}

// Close is part of the ClientLock interface.
func (ncl *noopClientLock) Close() {}

// ClientPos is part of the ClientLock interface.
func (ncl *noopClientLock) ClientPos() CmdPos {
	return ncl.clientComm.lastDelivered
}

// RTrim is part of the ClientLock interface.
func (ncl *noopClientLock) RTrim(_ context.Context, pos CmdPos) {
	var i int
	var r resWithPos
	for i, r = range ncl.clientComm.results {
		if r.pos >= pos {
			break
		}
	}
	ncl.clientComm.results = ncl.clientComm.results[:i]
}
