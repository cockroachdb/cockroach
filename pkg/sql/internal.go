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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
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
		s:          s,
		mon:        &monitor,
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
	sd *sessiondata.SessionData,
	syncCallback func([]resWithPos),
	errCallback func(error),
) (*StmtBuf, *sync.WaitGroup, error) {
	clientComm := &internalClientComm{
		sync: syncCallback,
		// init lastDelivered below the position of the first result (0).
		lastDelivered: -1,
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
	if !strings.HasPrefix(sd.ApplicationName, sqlbase.InternalAppNamePrefix) {
		appStatsBucketName = sqlbase.DelegatedAppNamePrefix + sd.ApplicationName
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
	return stmtBuf, &wg, nil
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
	session sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, error) {
	datums, _, err := ie.queryInternal(ctx, opName, txn, session, stmt, qargs...)
	return datums, err
}

// QueryWithCols is like QueryEx, but it also returns the computed ResultColumns
// of the input query.
func (ie *InternalExecutor) QueryWithCols(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	return ie.queryInternal(ctx, opName, txn, session, stmt, qargs...)
}

func (ie *InternalExecutor) queryInternal(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	sessionDataOverride sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	res, err := ie.execInternal(ctx, opName, txn, sessionDataOverride, stmt, qargs...)
	if err != nil {
		return nil, nil, err
	}
	return res.rows, res.cols, res.err
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
	session sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	rows, err := ie.QueryEx(ctx, opName, txn, session, stmt, qargs...)
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
	session sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) (int, error) {
	res, err := ie.execInternal(ctx, opName, txn, session, stmt, qargs...)
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

// applyOverrides overrides the respective fields from sd for all the fields set on o.
func applyOverrides(o sqlbase.InternalExecutorSessionDataOverride, sd *sessiondata.SessionData) {
	if o.User != "" {
		sd.User = o.User
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
}

func (ie *InternalExecutor) maybeRootSessionDataOverride(
	opName string,
) sqlbase.InternalExecutorSessionDataOverride {
	if ie.sessionData == nil {
		return sqlbase.InternalExecutorSessionDataOverride{
			User:            security.RootUser,
			ApplicationName: sqlbase.InternalAppNamePrefix + "-" + opName,
		}
	}
	o := sqlbase.InternalExecutorSessionDataOverride{}
	if ie.sessionData.User == "" {
		o.User = security.RootUser
	}
	if ie.sessionData.ApplicationName == "" {
		o.ApplicationName = sqlbase.InternalAppNamePrefix + "-" + opName
	}
	return o
}

// execInternal executes a statement.
//
// sessionDataOverride can be used to control select fields in the executor's
// session data. It overrides what has been previously set through
// SetSessionData(), if anything.
func (ie *InternalExecutor) execInternal(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	sessionDataOverride sqlbase.InternalExecutorSessionDataOverride,
	stmt string,
	qargs ...interface{},
) (retRes result, retErr error) {
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
	if sd.User == "" {
		return result{}, errors.AssertionFailedf("no user specified for internal query")
	}
	if sd.ApplicationName == "" {
		sd.ApplicationName = sqlbase.InternalAppNamePrefix + "-" + opName
	}

	defer func() {
		// We wrap errors with the opName, but not if they're retriable - in that
		// case we need to leave the error intact so that it can be retried at a
		// higher level.
		//
		// TODO(knz): track the callers and check whether opName could be turned
		// into a type safe for reporting.
		if retErr != nil && !errIsRetriable(retErr) {
			retErr = errors.Wrapf(retErr, "%s", opName)
		}
		if retRes.err != nil && !errIsRetriable(retRes.err) {
			retRes.err = errors.Wrapf(retRes.err, "%s", opName)
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
	stmtBuf, wg, err := ie.initConnEx(ctx, txn, sd, syncCallback, errCallback)
	if err != nil {
		return result{}, err
	}

	// Transforms the args to datums. The datum types will be passed as type hints
	// to the PrepareStmt command.
	datums, err := golangFillQueryArguments(qargs...)
	if err != nil {
		return result{}, err
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
