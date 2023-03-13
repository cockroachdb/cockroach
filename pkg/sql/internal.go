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
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catsessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var _ isql.Executor = &InternalExecutor{}

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

	// sessionDataStack, if not nil, represents the session variable stack used by
	// statements executed on this internalExecutor. Note that queries executed
	// by the executor will run on copies of the top element of this data.
	sessionDataStack *sessiondata.Stack

	// syntheticDescriptors stores the synthetic descriptors to be injected into
	// each query/statement's descs.Collection upon initialization.
	//
	// Warning: Not safe for concurrent use from multiple goroutines.
	syntheticDescriptors []catalog.Descriptor

	// extraTxnState is to store extra transaction state info that
	// will be passed to an internal executor. It should only be set when the
	// internal executor is used under a not-nil txn.
	// TODO (janexing): we will deprecate this field with *connExecutor ASAP.
	// An internal executor, if used with a not nil txn, should be always coupled
	// with a single connExecutor which runs all passed sql statements.
	extraTxnState *extraTxnState
}

// WithSyntheticDescriptors sets the synthetic descriptors before running the
// the provided closure and resets them afterward. Used for queries/statements
// that need to use in-memory synthetic descriptors different from descriptors
// written to disk. These descriptors override all other descriptors on the
// immutable resolution path.
//
// Warning: Not safe for concurrent use from multiple goroutines. This API is
// flawed in that the internal executor is meant to function as a stateless
// wrapper, and creates a new connExecutor and descs.Collection on each query/
// statement, so these descriptors should really be specified at a per-query/
// statement level. See #34304.
func (ie *InternalExecutor) WithSyntheticDescriptors(
	descs []catalog.Descriptor, run func() error,
) error {
	ie.syntheticDescriptors = descs
	defer func() {
		ie.syntheticDescriptors = nil
	}()
	return run()
}

// MakeInternalExecutor creates an InternalExecutor.
// TODO (janexing): usage of it should be deprecated with `DescsTxnWithExecutor()`
// or `Executor()`.
func MakeInternalExecutor(
	s *Server, memMetrics MemoryMetrics, monitor *mon.BytesMonitor,
) InternalExecutor {
	return InternalExecutor{
		s:          s,
		mon:        monitor,
		memMetrics: memMetrics,
	}
}

// MakeInternalExecutorMemMonitor creates and starts memory monitor for an
// InternalExecutor.
func MakeInternalExecutorMemMonitor(
	memMetrics MemoryMetrics, settings *cluster.Settings,
) *mon.BytesMonitor {
	return mon.NewMonitor(
		"internal SQL executor",
		mon.MemoryResource,
		memMetrics.CurBytesCount,
		memMetrics.MaxBytesHist,
		-1,            /* use default increment */
		math.MaxInt64, /* noteworthy */
		settings,
	)
}

// SetSessionData binds the session variables that will be used by queries
// performed through this executor from now on. This creates a new session stack.
// It is recommended to use SetSessionDataStack.
//
// SetSessionData cannot be called concurrently with query execution.
func (ie *InternalExecutor) SetSessionData(sessionData *sessiondata.SessionData) {
	if sessionData != nil {
		populateMinimalSessionData(sessionData)
		ie.sessionDataStack = sessiondata.NewStack(sessionData)
	}
}

func (ie *InternalExecutor) runWithEx(
	ctx context.Context,
	txn *kv.Txn,
	w ieResultWriter,
	sd *sessiondata.SessionData,
	stmtBuf *StmtBuf,
	wg *sync.WaitGroup,
	syncCallback func([]resWithPos),
	errCallback func(error),
) error {
	ex, err := ie.initConnEx(ctx, txn, w, sd, stmtBuf, syncCallback)
	if err != nil {
		return err
	}
	wg.Add(1)
	go func() {
		if err := ex.run(ctx, ie.mon, &mon.BoundAccount{} /*reserved*/, nil /* cancel */); err != nil {
			sqltelemetry.RecordError(ctx, err, &ex.server.cfg.Settings.SV)
			errCallback(err)
		}
		w.finish()
		closeMode := normalClose
		if txn != nil {
			closeMode = externalTxnClose
		}
		ex.close(ctx, closeMode)
		wg.Done()
	}()
	return nil
}

// initConnEx creates a connExecutor and runs it on a separate goroutine. It
// takes in a StmtBuf into which commands can be pushed and a WaitGroup that
// will be signaled when connEx.run() returns.
//
// If txn is not nil, the statement will be executed in the respective txn.
//
// The ieResultWriter coordinates communicating results to the client. It may
// block execution when rows are being sent in order to prevent hazardous
// concurrency.
//
// sd will constitute the executor's session state.
func (ie *InternalExecutor) initConnEx(
	ctx context.Context,
	txn *kv.Txn,
	w ieResultWriter,
	sd *sessiondata.SessionData,
	stmtBuf *StmtBuf,
	syncCallback func([]resWithPos),
) (*connExecutor, error) {
	clientComm := &internalClientComm{
		w: w,
		// init lastDelivered below the position of the first result (0).
		lastDelivered: -1,
		sync:          syncCallback,
	}

	applicationStats := ie.s.sqlStats.GetApplicationStats(sd.ApplicationName, true /* internal */)
	sds := sessiondata.NewStack(sd)
	sdMutIterator := ie.s.makeSessionDataMutatorIterator(sds, nil /* sessionDefaults */)
	var ex *connExecutor
	var err error
	if txn == nil {
		ex = ie.s.newConnExecutor(
			ctx,
			sdMutIterator,
			stmtBuf,
			clientComm,
			ie.memMetrics,
			&ie.s.InternalMetrics,
			applicationStats,
			nil, /* postSetupFn */
		)
	} else {
		ex, err = ie.newConnExecutorWithTxn(
			ctx,
			txn,
			sdMutIterator,
			stmtBuf,
			clientComm,
			applicationStats,
		)
		if err != nil {
			return nil, err
		}
	}

	ex.executorType = executorTypeInternal
	return ex, nil

}

// newConnExecutorWithTxn creates a connExecutor that will execute statements
// under a higher-level txn. This connExecutor runs with a different state
// machine, much reduced from the regular one. It cannot initiate or end
// transactions (so, no BEGIN, COMMIT, ROLLBACK, no auto-commit, no automatic
// retries). It may inherit the descriptor collection and txn state from the
// internal executor.
//
// If there is no error, this function also activate()s the returned
// executor, so the caller does not need to run the
// activation. However this means that run() or close() must be called
// to release resources.
// TODO (janexing): txn should be passed to ie.extraTxnState rather than
// as a parameter to this function.
func (ie *InternalExecutor) newConnExecutorWithTxn(
	ctx context.Context,
	txn *kv.Txn,
	sdMutIterator *sessionDataMutatorIterator,
	stmtBuf *StmtBuf,
	clientComm ClientComm,
	applicationStats sqlstats.ApplicationStats,
) (ex *connExecutor, _ error) {

	// If the internal executor has injected synthetic descriptors, we will
	// inject them into the descs.Collection below, and we'll note that
	// fact so that the synthetic descriptors are reset when the statement
	// finishes. This logic is in support of the legacy schema changer's
	// execution of schema changes in a transaction. If the declarative
	// schema changer is in use, the descs.Collection in the extraTxnState
	// may have synthetic descriptors, but their lifecycle is controlled
	// externally, and they should not be reset after executing a statement
	// here.
	shouldResetSyntheticDescriptors := len(ie.syntheticDescriptors) > 0

	// If an internal executor is run with a not-nil txn, we may want to
	// let it inherit the descriptor collection, schema change job records
	// and job collections from the caller.
	postSetupFn := func(ex *connExecutor) {
		if ie.extraTxnState != nil {
			ex.extraTxnState.descCollection = ie.extraTxnState.descCollection
			ex.extraTxnState.fromOuterTxn = true
			ex.extraTxnState.jobs = ie.extraTxnState.jobs
			ex.extraTxnState.schemaChangerState = ie.extraTxnState.schemaChangerState
			ex.extraTxnState.shouldResetSyntheticDescriptors = shouldResetSyntheticDescriptors
			ex.initPlanner(ctx, &ex.planner)
		}
	}

	ex = ie.s.newConnExecutor(
		ctx,
		sdMutIterator,
		stmtBuf,
		clientComm,
		ie.memMetrics,
		&ie.s.InternalMetrics,
		applicationStats,
		postSetupFn,
	)

	if txn.Type() == kv.LeafTxn {
		// If the txn is a leaf txn it is not allowed to perform mutations. For
		// sanity, set read only on the session.
		ex.dataMutatorIterator.applyOnEachMutator(func(m sessionDataMutator) {
			m.SetReadOnly(true)
		})
	}

	// The new transaction stuff below requires active monitors and traces, so
	// we need to activate the executor now.
	ex.activate(ctx, ie.mon, &mon.BoundAccount{})

	// Perform some surgery on the executor - replace its state machine and
	// initialize the state, and its jobs and schema change job records if
	// they are passed by the caller.
	// The txn is always set as explicit, because when running in an outer txn,
	// the conn executor inside an internal executor is generally not at liberty
	// to commit the transaction.
	// Thus, to disallow auto-commit and auto-retries, we make the txn
	// here an explicit one.
	ex.machine = fsm.MakeMachine(
		BoundTxnStateTransitions,
		stateOpen{ImplicitTxn: fsm.False, WasUpgraded: fsm.False},
		&ex.state,
	)

	ex.state.resetForNewSQLTxn(
		ctx,
		explicitTxn,
		txn.ReadTimestamp().GoTime(),
		nil, /* historicalTimestamp */
		roachpb.UnspecifiedUserPriority,
		tree.ReadWrite,
		txn,
		ex.transitionCtx,
		ex.QualityOfService())

	// Modify the Collection to match the parent executor's Collection.
	// This allows the Executor to see schema changes made by the
	// parent executor.
	if shouldResetSyntheticDescriptors {
		ex.extraTxnState.descCollection.SetSyntheticDescriptors(ie.syntheticDescriptors)
	}
	return ex, nil
}

type ieIteratorResult struct {
	// Exactly one of these 4 fields will be set.
	row                   tree.Datums
	rowsAffectedIncrement *int
	cols                  colinfo.ResultColumns
	err                   error
}

type rowsIterator struct {
	r ieResultReader

	rowsAffected int
	resultCols   colinfo.ResultColumns

	// first, if non-nil, is the first object read from r. We block the return
	// of the created rowsIterator in execInternal() until the producer writes
	// something into the corresponding ieResultWriter because this indicates
	// that the query planning has been fully performed (we want to prohibit the
	// concurrent usage of the transactions).
	first *ieIteratorResult

	lastRow tree.Datums
	lastErr error
	done    bool

	// errCallback is an optional callback that will be called exactly once
	// before an error is returned by Next() or Close().
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

var _ isql.Rows = &rowsIterator{}
var _ eval.InternalRows = &rowsIterator{}

func (r *rowsIterator) Next(ctx context.Context) (_ bool, retErr error) {
	// Due to recursive calls to Next() below, this deferred function might get
	// executed multiple times, yet it is not a problem because Close() is
	// idempotent and we're unsetting the error callback.
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

	if r.done {
		return false, r.lastErr
	}

	// handleDataObject processes a single object read from ieResultReader and
	// returns the result to be returned by Next. It also might call Next
	// recursively if the object is a piece of metadata.
	handleDataObject := func(data ieIteratorResult) (bool, error) {
		if data.row != nil {
			r.rowsAffected++
			// No need to make a copy because streamingCommandResult does that
			// for us.
			r.lastRow = data.row
			return true, nil
		}
		if data.rowsAffectedIncrement != nil {
			r.rowsAffected += *data.rowsAffectedIncrement
			return r.Next(ctx)
		}
		if data.cols != nil {
			// Ignore the result columns if they are already set on the
			// iterator: it is possible for ROWS statement type to be executed
			// in a 'rows affected' mode, in such case the correct columns are
			// set manually when instantiating the iterator, but the result
			// columns of the statement are also sent by SetColumns() (we need
			// to keep the former).
			if r.resultCols == nil {
				r.resultCols = data.cols
			}
			return r.Next(ctx)
		}
		if data.err == nil {
			data.err = errors.AssertionFailedf("unexpectedly empty ieIteratorResult object")
		}
		r.lastErr = data.err
		r.done = true
		return false, r.lastErr
	}

	if r.first != nil {
		// This is the very first call to Next() and we have already buffered
		// up the first piece of data before returning rowsIterator to the
		// caller.
		first := r.first
		r.first = nil
		return handleDataObject(*first)
	}

	var next ieIteratorResult
	next, r.done, r.lastErr = r.r.nextResult(ctx)
	if r.done || r.lastErr != nil {
		return false, r.lastErr
	}
	return handleDataObject(next)
}

func (r *rowsIterator) Cur() tree.Datums {
	return r.lastRow
}

func (r *rowsIterator) RowsAffected() int {
	return r.rowsAffected
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
	// Close the ieResultReader to tell the writer that we're done.
	if err := r.r.close(); err != nil && r.lastErr == nil {
		r.lastErr = err
	}
	return r.lastErr
}

func (r *rowsIterator) Types() colinfo.ResultColumns {
	return r.resultCols
}

// QueryBuffered executes the supplied SQL statement and returns the resulting
// rows (meaning all of them are buffered at once). If no user has been
// previously set through SetSessionData, the statement is executed as the root
// user.
//
// If txn is not nil, the statement will be executed in the respective txn.
//
// QueryBuffered is deprecated because it may transparently execute a query as
// root. Use QueryBufferedEx instead.
func (ie *InternalExecutor) QueryBuffered(
	ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
) ([]tree.Datums, error) {
	return ie.QueryBufferedEx(ctx, opName, txn, ie.maybeRootSessionDataOverride(opName), stmt, qargs...)
}

// QueryBufferedEx executes the supplied SQL statement and returns the resulting
// rows (meaning all of them are buffered at once).
//
// If txn is not nil, the statement will be executed in the respective txn.
//
// The fields set in session that are set override the respective fields if they
// have previously been set through SetSessionData().
func (ie *InternalExecutor) QueryBufferedEx(
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

// QueryBufferedExWithCols is like QueryBufferedEx, additionally returning the computed
// ResultColumns of the input query.
func (ie *InternalExecutor) QueryBufferedExWithCols(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, colinfo.ResultColumns, error) {
	datums, cols, err := ie.queryInternalBuffered(ctx, opName, txn, session, stmt, 0 /* limit */, qargs...)
	return datums, cols, err
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
	// We will run the query to completion, so we can use an async result
	// channel.
	rw := newAsyncIEResultChannel()
	it, err := ie.execInternal(ctx, opName, rw, txn, sessionDataOverride, stmt, qargs...)
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
	rows, _, err := ie.QueryRowExWithCols(ctx, opName, txn, session, stmt, qargs...)
	return rows, err
}

// QueryRowExWithCols is like QueryRowEx, additionally returning the computed
// ResultColumns of the input query.
func (ie *InternalExecutor) QueryRowExWithCols(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, colinfo.ResultColumns, error) {
	rows, cols, err := ie.queryInternalBuffered(ctx, opName, txn, session, stmt, 2 /* limit */, qargs...)
	if err != nil {
		return nil, nil, err
	}
	switch len(rows) {
	case 0:
		return nil, nil, nil
	case 1:
		return rows[0], cols, nil
	default:
		return nil, nil, &tree.MultipleResultsError{SQL: stmt}
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
	// We will run the query to completion, so we can use an async result
	// channel.
	rw := newAsyncIEResultChannel()
	it, err := ie.execInternal(ctx, opName, rw, txn, session, stmt, qargs...)
	if err != nil {
		return 0, err
	}
	// We need to exhaust the iterator so that it can count the number of rows
	// affected.
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
	}
	if err != nil {
		return 0, err
	}
	return it.rowsAffected, nil
}

// QueryIterator executes the query, returning an iterator that can be used
// to get the results. If the call is successful, the returned iterator
// *must* be closed.
//
// QueryIterator is deprecated because it may transparently execute a query
// as root. Use QueryIteratorEx instead.
func (ie *InternalExecutor) QueryIterator(
	ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
) (isql.Rows, error) {
	return ie.QueryIteratorEx(ctx, opName, txn, ie.maybeRootSessionDataOverride(opName), stmt, qargs...)
}

// QueryIteratorEx executes the query, returning an iterator that can be used
// to get the results. If the call is successful, the returned iterator
// *must* be closed.
func (ie *InternalExecutor) QueryIteratorEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (isql.Rows, error) {
	return ie.execInternal(
		ctx, opName, newSyncIEResultChannel(), txn, session, stmt, qargs...,
	)
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
	if o.QualityOfService != nil {
		sd.DefaultTxnQualityOfService = o.QualityOfService.ValidateInternal()
	}
}

func (ie *InternalExecutor) maybeRootSessionDataOverride(
	opName string,
) sessiondata.InternalExecutorOverride {
	if ie.sessionDataStack == nil {
		return sessiondata.InternalExecutorOverride{
			User:            username.RootUserName(),
			ApplicationName: catconstants.InternalAppNamePrefix + "-" + opName,
		}
	}
	o := sessiondata.NoSessionDataOverride
	sd := ie.sessionDataStack.Top()
	if sd.User().Undefined() {
		o.User = username.RootUserName()
	}
	if sd.ApplicationName == "" {
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

// execInternal executes a statement.
//
// sessionDataOverride can be used to control select fields in the executor's
// session data. It overrides what has been previously set through
// SetSessionData(), if anything.
func (ie *InternalExecutor) execInternal(
	ctx context.Context,
	opName string,
	rw *ieResultChannel,
	txn *kv.Txn,
	sessionDataOverride sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (r *rowsIterator, retErr error) {
	if err := ie.checkIfTxnIsConsistent(txn); err != nil {
		return nil, err
	}

	ctx = logtags.AddTag(ctx, "intExec", opName)

	var sd *sessiondata.SessionData
	if ie.sessionDataStack != nil {
		// TODO(andrei): Properly clone (deep copy) ie.sessionData.
		sd = ie.sessionDataStack.Top().Clone()
		// Even if session queries are told to error on non-home region accesses,
		// internal queries spawned from the same context should never do so.
		sd.LocalOnlySessionData.EnforceHomeRegion = false
	} else {
		sd = newSessionData(SessionArgs{})
	}
	applyOverrides(sessionDataOverride, sd)
	sd.Internal = true
	if sd.User().Undefined() {
		return nil, errors.AssertionFailedf("no user specified for internal query")
	}
	// When the connEx is serving an internal executor, it can inherit the
	// application name from an outer session. This happens e.g. during ::regproc
	// casts and built-in functions that use SQL internally. In that case, we do
	// not want to record statistics against the outer application name directly;
	// instead we want to use a separate bucket. However we will still want to
	// have separate buckets for different applications so that we can measure
	// their respective "pressure" on internal queries. Hence the choice here to
	// add the delegate prefix to the current app name.
	if sd.ApplicationName == "" || sd.ApplicationName == catconstants.InternalAppNamePrefix {
		sd.ApplicationName = catconstants.InternalAppNamePrefix + "-" + opName
	} else if !strings.HasPrefix(sd.ApplicationName, catconstants.InternalAppNamePrefix) {
		// If this is already an "internal app", don't put more prefix.
		sd.ApplicationName = catconstants.DelegatedAppNamePrefix + sd.ApplicationName
	}
	// If the caller has injected a mapping to temp schemas, install it, and
	// leave it installed for the rest of the transaction.
	if ie.extraTxnState != nil && sd.DatabaseIDToTempSchemaID != nil {
		p := catsessiondata.NewDescriptorSessionDataStackProvider(sessiondata.NewStack(sd))
		ie.extraTxnState.descCollection.SetDescriptorSessionDataProvider(p)
	}

	// The returned span is finished by this function in all error paths, but if
	// an iterator is returned, then we transfer the responsibility of closing
	// the span to the iterator. This is necessary so that the connExecutor
	// exits before the span is finished.
	ctx, sp := tracing.EnsureChildSpan(ctx, ie.s.cfg.AmbientCtx.Tracer, opName)
	stmtBuf := NewStmtBuf()
	var wg sync.WaitGroup

	defer func() {
		// We wrap errors with the opName, but not if they're retriable - in that
		// case we need to leave the error intact so that it can be retried at a
		// higher level.
		//
		// TODO(knz): track the callers and check whether opName could be turned
		// into a type safe for reporting.
		if retErr != nil || r == nil {
			// Both retErr and r can be nil in case of panic.
			if retErr != nil && !errIsRetriable(retErr) {
				retErr = errors.Wrapf(retErr, "%s", opName)
			}
			stmtBuf.Close()
			wg.Wait()
			sp.Finish()
		} else {
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
	if err := ie.checkIfStmtIsAllowed(parsed.AST, txn); err != nil {
		return nil, err
	}
	parseEnd := timeutil.Now()

	// Transforms the args to datums. The datum types will be passed as type
	// hints to the PrepareStmt command below.
	datums, err := golangFillQueryArguments(qargs...)
	if err != nil {
		return nil, err
	}

	// resPos will be set to the position of the command that represents the
	// statement we care about before that command is sent for execution.
	var resPos CmdPos

	syncCallback := func(results []resWithPos) {
		// Close the stmtBuf so that the connExecutor exits its run() loop.
		stmtBuf.Close()
		for _, res := range results {
			if res.Err() != nil {
				// If we encounter an error, there's no point in looking
				// further; the rest of the commands in the batch have been
				// skipped.
				_ = rw.addResult(ctx, ieIteratorResult{err: res.Err()})
				return
			}
			if res.pos == resPos {
				return
			}
		}
		_ = rw.addResult(ctx, ieIteratorResult{
			err: errors.AssertionFailedf(
				"missing result for pos: %d and no previous error", resPos,
			),
		})
	}
	// errCallback is called if an error is returned from the connExecutor's
	// run() loop.
	errCallback := func(err error) {
		_ = rw.addResult(ctx, ieIteratorResult{err: err})
	}
	err = ie.runWithEx(ctx, txn, rw, sd, stmtBuf, &wg, syncCallback, errCallback)
	if err != nil {
		return nil, err
	}

	// We take max(len(s.Types), stmt.NumPlaceHolders) as the length of types.
	numParams := len(datums)
	if parsed.NumPlaceholders > numParams {
		numParams = parsed.NumPlaceholders
	}
	typeHints := make(tree.PlaceholderTypes, numParams)
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
				// This is the only and last statement in the batch, so that this
				// transaction can be autocommited as a single statement transaction.
				LastInBatch: true,
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

		if err := stmtBuf.Push(ctx, BindStmt{internalArgs: datums, isInternal: true}); err != nil {
			return nil, err
		}

		if err := stmtBuf.Push(ctx,
			ExecPortal{
				TimeReceived: timeReceived,
				// Next command will be a sync, so this can be considered as another single
				// statement transaction.
				FollowedBySync: true,
			},
		); err != nil {
			return nil, err
		}
	}
	if err := stmtBuf.Push(ctx, Sync{}); err != nil {
		return nil, err
	}
	r = &rowsIterator{
		r:       rw,
		stmtBuf: stmtBuf,
		wg:      &wg,
	}

	if parsed.AST.StatementReturnType() != tree.Rows {
		r.resultCols = rowsAffectedResultColumns
	}

	// Now we need to block the reader goroutine until the query planning has
	// been performed by the connExecutor goroutine. We do so by waiting until
	// the first object is sent on the data channel.
	{
		var first ieIteratorResult
		if first, r.done, r.lastErr = rw.firstResult(ctx); !r.done {
			r.first = &first
		}
	}
	if !r.done && r.first.cols != nil {
		// If the query is of ROWS statement type, the very first thing sent on
		// the channel will be the column schema. This will occur before the
		// query is given to the execution engine, so we actually need to get
		// the next piece from the data channel.
		//
		// Note that only statements of ROWS type should send the cols, but we
		// choose to be defensive and don't assert that.
		if r.resultCols == nil {
			r.resultCols = r.first.cols
		}
		var first ieIteratorResult
		first, r.done, r.lastErr = rw.nextResult(ctx)
		if !r.done {
			r.first = &first
		}
	}

	// Note that if a context cancellation error has occurred, we still return
	// the iterator and nil retErr so that the iterator is properly closed by
	// the caller which will cleanup the connExecutor goroutine.
	return r, nil
}

// commitTxn is to commit the txn bound to the internal executor.
// It should only be used in CollectionFactory.TxnWithExecutor().
func (ie *InternalExecutor) commitTxn(ctx context.Context) error {
	if ie.extraTxnState == nil || ie.extraTxnState.txn == nil {
		return errors.New("no txn to commit")
	}

	var sd *sessiondata.SessionData
	if ie.sessionDataStack != nil {
		sd = ie.sessionDataStack.Top().Clone()
	} else {
		sd = newSessionData(SessionArgs{})
	}

	rw := newAsyncIEResultChannel()
	stmtBuf := NewStmtBuf()

	ex, err := ie.initConnEx(ctx, ie.extraTxnState.txn, rw, sd, stmtBuf, nil /* syncCallback */)
	if err != nil {
		return errors.Wrap(err, "cannot create conn executor to commit txn")
	}
	// TODO(janexing): is this correct?
	ex.planner.txn = ie.extraTxnState.txn

	defer ex.close(ctx, externalTxnClose)
	if ie.extraTxnState.txn.IsCommitted() {
		// TODO(ajwerner): assert that none of the other extraTxnState is
		// occupied with state. Namely, we want to make sure that no jobs or
		// schema changes occurred. If that had, it'd violate various invariants
		// we'd like to uphold.
		return nil
	}
	return ex.commitSQLTransactionInternal(ctx)
}

// checkIfStmtIsAllowed returns an error if the internal executor is not bound
// with the outer-txn-related info but is used to run DDL statements within an
// outer txn.
// TODO (janexing): this will be deprecate soon since it's not a good idea
// to have `extraTxnState` to store the info from a outer txn.
func (ie *InternalExecutor) checkIfStmtIsAllowed(stmt tree.Statement, txn *kv.Txn) error {
	if stmt == nil {
		return nil
	}
	if tree.CanModifySchema(stmt) && txn != nil && ie.extraTxnState == nil {
		return errors.New("DDL statement is disallowed if internal " +
			"executor is not bound with txn metadata")
	}
	return nil
}

// checkIfTxnIsConsistent returns true if the given txn is not nil and is not
// the same txn that is used to construct the internal executor.
// TODO(janexing): this will be deprecated soon as we will only use
// ie.extraTxnState.txn, and the txn argument in query functions will be
// deprecated.
func (ie *InternalExecutor) checkIfTxnIsConsistent(txn *kv.Txn) error {
	if txn == nil && ie.extraTxnState != nil {
		return errors.New("the current internal executor was contructed with " +
			"a txn. To use an internal executor without a txn, call " +
			"insql.DB.Executor()")
	}

	if txn != nil && ie.extraTxnState != nil && ie.extraTxnState.txn != txn {
		return errors.New("txn is inconsistent with the one when " +
			"constructing the internal executor")
	}
	return nil
}

// internalClientComm is an implementation of ClientComm used by the
// InternalExecutor. Result rows are buffered in memory.
type internalClientComm struct {
	// results will contain the results of the commands executed by an
	// InternalExecutor.
	results []resWithPos

	// The results of the query execution will be written into w.
	w ieResultWriter

	lastDelivered CmdPos

	// sync, if set, is called whenever a Sync is executed.
	sync func([]resWithPos)
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
	_ bool,
) CommandResult {
	return icc.createRes(pos, nil /* onClose */)
}

// createRes creates a result. onClose, if not nil, is called when the result is
// closed.
func (icc *internalClientComm) createRes(pos CmdPos, onClose func()) *streamingCommandResult {
	res := &streamingCommandResult{
		w: icc.w,
		closeCallback: func(res *streamingCommandResult, typ resCloseType) {
			if typ == discarded {
				return
			}
			icc.results = append(icc.results, resWithPos{streamingCommandResult: res, pos: pos})
			if onClose != nil {
				onClose()
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
	return icc.createRes(pos, func() {
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

// CreateCopyOutResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateCopyOutResult(pos CmdPos) CopyOutResult {
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

// extraTxnState is to store extra transaction state info that
// will be passed to an internal executor when it's used under a txn context.
// It should not be exported from the sql package.
// TODO (janexing): we will deprecate this struct ASAP. It only exists as a
// stop-gap before we implement Executor.ConnExecutor to run all
// sql statements under a transaction. This struct is not ideal for an internal
// executor in that it may lead to surprising bugs whereby we forget to add
// fields here and keep them in sync.
type extraTxnState struct {
	txn                *kv.Txn
	descCollection     *descs.Collection
	jobs               *txnJobsCollection
	schemaChangerState *SchemaChangerState
}

// InternalDB stored information needed to construct a new
// internal executor.
type InternalDB struct {
	server     *Server
	db         *kv.DB
	cf         *descs.CollectionFactory
	lm         *lease.Manager
	memMetrics MemoryMetrics
	monitor    *mon.BytesMonitor
}

// NewShimInternalDB is used to bootstrap the server which needs access to
// components which will ultimately have a handle to an InternalDB. Some of
// those components may attempt to access the *kv.DB before the InternalDB
// has been fully initialized. To get around this, we initially construct
// an InternalDB with just a handle to a *kv.DB and then we'll fill in the
// object during sql server construction.
func NewShimInternalDB(db *kv.DB) *InternalDB {
	return &InternalDB{db: db}
}

func (ief *InternalDB) CloneWithMemoryMonitor(
	metrics MemoryMetrics, monitor *mon.BytesMonitor,
) *InternalDB {
	clone := *ief
	clone.memMetrics = metrics
	clone.monitor = monitor
	return &clone
}

func (ief *InternalDB) KV() *kv.DB {
	return ief.db
}

// NewInternalDB returns a new InternalDB.
func NewInternalDB(s *Server, memMetrics MemoryMetrics, monitor *mon.BytesMonitor) *InternalDB {
	return &InternalDB{
		server:     s,
		cf:         s.cfg.CollectionFactory,
		db:         s.cfg.DB,
		lm:         s.cfg.LeaseManager,
		memMetrics: memMetrics,
		monitor:    monitor,
	}
}

var _ isql.DB = &InternalDB{}

type internalTxn struct {
	internalExecutor
	txn *kv.Txn
}

func (txn *internalTxn) Descriptors() *descs.Collection {
	return txn.extraTxnState.descCollection
}

func (txn *internalTxn) SessionData() *sessiondata.SessionData {
	return txn.sessionDataStack.Top()
}

func (txn *internalTxn) KV() *kv.Txn { return txn.txn }

func (txn *internalTxn) init(kvTxn *kv.Txn, ie InternalExecutor) {
	txn.txn = kvTxn
	txn.InternalExecutor = ie
}

type internalExecutor struct {
	InternalExecutor
}

// NewInternalExecutor constructs a new internal executor.
// TODO (janexing): usage of it should be deprecated with `DescsTxnWithExecutor()`
// or `Executor()`.
func (ief *InternalDB) NewInternalExecutor(sd *sessiondata.SessionData) isql.Executor {
	ie := MakeInternalExecutor(ief.server, ief.memMetrics, ief.monitor)
	ie.SetSessionData(sd)
	return &ie
}

// internalExecutorCommitTxnFunc is to commit the txn associated with an
// internal executor.
type internalExecutorCommitTxnFunc func(ctx context.Context) error

// newInternalExecutorWithTxn creates an internal executor with txn-related info,
// such as descriptor collection and schema change job records, etc.
// This function should only be used under
// InternalDB.DescsTxnWithExecutor().
// TODO (janexing): This function will be soon refactored after we change
// the internal executor infrastructure with a single conn executor for all
// sql statement executions within a txn.
func (ief *InternalDB) newInternalExecutorWithTxn(
	sd *sessiondata.SessionData, sv *settings.Values, txn *kv.Txn, descCol *descs.Collection,
) (InternalExecutor, internalExecutorCommitTxnFunc) {
	// By default, if not given session data, we initialize a sessionData that
	// would be the same as what would be created if root logged in.
	// The sessionData's user can be override when calling the query
	// functions of internal executor.
	// TODO(janexing): since we can be running queries with a higher privilege
	// than the actual user, a security boundary should be added to the error
	// handling of internal executor.
	if sd == nil {
		sd = NewFakeSessionData(sv, "" /* opName */)
		sd.UserProto = username.RootUserName().EncodeProto()
	}

	schemaChangerState := &SchemaChangerState{
		mode: sd.NewSchemaChangerMode,
	}
	ie := InternalExecutor{
		s:          ief.server,
		mon:        ief.monitor,
		memMetrics: ief.memMetrics,
		extraTxnState: &extraTxnState{
			txn:                txn,
			descCollection:     descCol,
			jobs:               newTxnJobsCollection(),
			schemaChangerState: schemaChangerState,
		},
	}
	populateMinimalSessionData(sd)
	ie.sessionDataStack = sessiondata.NewStack(sd)

	commitTxnFunc := func(ctx context.Context) error {
		defer func() {
			ie.extraTxnState.jobs.reset()
		}()
		if err := ie.commitTxn(ctx); err != nil {
			return err
		}
		return ie.s.cfg.JobRegistry.Run(ctx, ie.extraTxnState.jobs.created)
	}

	return ie, commitTxnFunc
}

// Executor returns an Executor not bound with any txn.
func (ief *InternalDB) Executor(opts ...isql.ExecutorOption) isql.Executor {
	var cfg isql.ExecutorConfig
	cfg.Init(opts...)
	ie := MakeInternalExecutor(ief.server, ief.memMetrics, ief.monitor)
	if sd := cfg.GetSessionData(); sd != nil {
		ie.SetSessionData(sd)
	}
	return &ie
}

type kvTxnFunc = func(context.Context, *kv.Txn) error

// DescsTxn enables callers to run transactions with explicit access to the
// *descs.Collection which is bound to the isql.Txn in the Txn method.
func (ief *InternalDB) DescsTxn(
	ctx context.Context, f func(context.Context, descs.Txn) error, opts ...isql.TxnOption,
) error {
	return ief.txn(
		ctx,
		func(ctx context.Context, txn *internalTxn) error { return f(ctx, txn) },
		opts...,
	)
}

// Txn is used to run queries with internal executor in a transactional
// manner.
func (ief *InternalDB) Txn(
	ctx context.Context, f func(context.Context, isql.Txn) error, opts ...isql.TxnOption,
) error {
	wrapped := func(ctx context.Context, txn *internalTxn) error { return f(ctx, txn) }
	return ief.txn(ctx, wrapped, opts...)
}

func (ief *InternalDB) txn(
	ctx context.Context, f func(context.Context, *internalTxn) error, opts ...isql.TxnOption,
) error {
	var cfg isql.TxnConfig
	cfg.Init(opts...)

	db := ief.server.cfg.DB

	// Wait for descriptors that were modified or dropped. If the descriptor
	// was not dropped, wait for one version. Otherwise, wait for no versions.
	waitForDescriptors := func(
		modifiedDescriptors []lease.IDVersion,
		deletedDescs catalog.DescriptorIDSet,
	) error {
		retryOpts := retry.Options{
			InitialBackoff: time.Millisecond,
			Multiplier:     1.5,
			MaxBackoff:     time.Second,
		}
		lm := ief.server.cfg.LeaseManager
		for _, ld := range modifiedDescriptors {
			if deletedDescs.Contains(ld.ID) { // we'll wait below
				continue
			}
			_, err := lm.WaitForOneVersion(ctx, ld.ID, retryOpts)
			// If the descriptor has been deleted, just wait for leases to drain.
			if errors.Is(err, catalog.ErrDescriptorNotFound) {
				err = lm.WaitForNoVersion(ctx, ld.ID, retryOpts)
			}
			if err != nil {
				return err
			}
		}
		for _, id := range deletedDescs.Ordered() {
			if err := lm.WaitForNoVersion(ctx, id, retryOpts); err != nil {
				return err
			}
		}
		return nil
	}

	run := db.Txn
	if priority, hasPriority := cfg.GetAdmissionPriority(); hasPriority {
		steppingMode := kv.SteppingDisabled
		if cfg.GetSteppingEnabled() {
			steppingMode = kv.SteppingEnabled
		}
		run = func(ctx context.Context, f kvTxnFunc) error {
			return db.TxnWithAdmissionControl(
				ctx, kvpb.AdmissionHeader_FROM_SQL, priority, steppingMode, f,
			)
		}
	} else if cfg.GetSteppingEnabled() {
		run = func(ctx context.Context, f kvTxnFunc) error {
			return db.TxnWithSteppingEnabled(ctx, sessiondatapb.Normal, f)
		}
	}

	cf := ief.server.cfg.CollectionFactory
	for {
		var withNewVersion []lease.IDVersion
		var deletedDescs catalog.DescriptorIDSet
		if err := run(ctx, func(ctx context.Context, kvTxn *kv.Txn) (err error) {
			withNewVersion, deletedDescs = nil, catalog.DescriptorIDSet{}
			descsCol := cf.NewCollection(ctx, descs.WithMonitor(ief.monitor))
			defer descsCol.ReleaseAll(ctx)
			ie, commitTxnFn := ief.newInternalExecutorWithTxn(
				cfg.GetSessionData(),
				&cf.GetClusterSettings().SV,
				kvTxn,
				descsCol,
			)
			txn := internalTxn{txn: kvTxn}
			txn.InternalExecutor = ie
			if err := f(ctx, &txn); err != nil {
				return err
			}
			deletedDescs = descsCol.GetDeletedDescs()
			withNewVersion, err = descsCol.GetOriginalPreviousIDVersionsForUncommitted()
			if err != nil {
				return err
			}
			return commitTxnFn(ctx)
		}); descs.IsTwoVersionInvariantViolationError(err) {
			continue
		} else {
			if err == nil {
				err = waitForDescriptors(withNewVersion, deletedDescs)
			}
			return err
		}
	}
}
