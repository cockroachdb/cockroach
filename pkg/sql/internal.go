// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
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
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/growstack"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

// NewInternalSessionData returns a session data for use in internal queries
// that are not run on behalf of a user session, such as those run during the
// steps of background jobs and schema changes. Each session variable is
// initialized using the correct default value.
func NewInternalSessionData(
	ctx context.Context, settings *cluster.Settings, opName redact.SafeString,
) *sessiondata.SessionData {
	appName := catconstants.InternalAppNamePrefix
	if opName != "" {
		appName = catconstants.InternalAppNamePrefix + "-" + string(opName)
	}

	sd := &sessiondata.SessionData{}
	sds := sessiondata.NewStack(sd)
	defaults := SessionDefaults(map[string]string{
		"application_name": appName,
	})
	sdMutIterator := makeSessionDataMutatorIterator(sds, defaults, settings)

	sdMutIterator.applyOnEachMutator(func(m sessionDataMutator) {
		for varName, v := range varGen {
			if varName == "optimizer_use_histograms" {
				// Do not use histograms when optimizing internal executor
				// queries. This causes a significant performance regression.
				// TODO(#102954): Diagnose and fix this.
				continue
			}
			if v.Set != nil {
				hasDefault, defVal := getSessionVarDefaultString(varName, v, m.sessionDataMutatorBase)
				if hasDefault {
					if err := v.Set(ctx, m, defVal); err != nil {
						log.Warningf(ctx, "error setting default for %s: %v", varName, err)
					}
				}
			}
		}
	})

	sd.UserProto = username.NodeUserName().EncodeProto()
	sd.Internal = true
	sd.SearchPath = sessiondata.DefaultSearchPathForUser(username.NodeUserName())
	sd.SequenceState = sessiondata.NewSequenceState()
	sd.Location = time.UTC
	sd.StmtTimeout = 0
	sd.DisallowFullTableScans = false
	return sd
}

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
	return mon.NewMonitor(mon.Options{
		Name:       mon.MakeMonitorName("internal SQL executor"),
		CurCount:   memMetrics.CurBytesCount,
		MaxHist:    memMetrics.MaxBytesHist,
		Settings:   settings,
		LongLiving: true,
	})
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

var ieRowsAffectedRetryLimit = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.internal_executor.rows_affected_retry_limit",
	"limit on the number of retries that can be transparently performed "+
		"by the InternalExecutor's Exec{Ex} methods",
	5,
	settings.NonNegativeInt,
)

func (ie *InternalExecutor) runWithEx(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	w ieResultWriter,
	mode ieExecutionMode,
	sd *sessiondata.SessionData,
	stmtBuf *StmtBuf,
	wg *sync.WaitGroup,
	syncCallback func([]*streamingCommandResult),
	errCallback func(error),
	attributeToUser bool,
	growStackSize bool,
) error {
	ex, err := ie.initConnEx(ctx, txn, w, mode, sd, stmtBuf, syncCallback, attributeToUser)
	if err != nil {
		return err
	}
	wg.Add(1)
	cleanup := func(ctx context.Context) {
		closeMode := normalClose
		if txn != nil {
			closeMode = externalTxnClose
		}
		ex.close(ctx, closeMode)
		wg.Done()
	}
	if err = ie.s.cfg.Stopper.RunAsyncTaskEx(
		ctx,
		stop.TaskOpts{
			TaskName: opName.StripMarkers(),
			SpanOpt:  stop.ChildSpan,
		},
		func(ctx context.Context) {
			defer cleanup(ctx)
			// TODO(yuzefovich): benchmark whether we should be growing the
			// stack size unconditionally.
			if growStackSize {
				growstack.Grow()
			}
			if err := ex.run(
				ctx,
				ie.mon,
				&mon.BoundAccount{}, /*reserved*/
				nil,                 /* cancel */
			); err != nil {
				sqltelemetry.RecordError(ctx, err, &ex.server.cfg.Settings.SV)
				errCallback(err)
			}
			w.finish()
		},
	); err != nil {
		// The goroutine wasn't started, so we need to perform the cleanup
		// ourselves.
		cleanup(ctx)
		return err
	}
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
	mode ieExecutionMode,
	sd *sessiondata.SessionData,
	stmtBuf *StmtBuf,
	syncCallback func([]*streamingCommandResult),
	attributeToUser bool,
) (*connExecutor, error) {
	clientComm := &internalClientComm{
		w:    w,
		mode: mode,
		sync: syncCallback,
	}
	clientComm.results = clientComm.resultsScratch[:0]
	clientComm.rowsAffectedState.rewind = func() {
		var zero int
		_ = w.addResult(ctx, ieIteratorResult{rowsAffected: &zero})
	}
	clientComm.rowsAffectedState.numRewindsLimit = ieRowsAffectedRetryLimit.Get(&ie.s.cfg.Settings.SV)

	applicationStats := ie.s.localSqlStats.GetApplicationStats(sd.ApplicationName)
	sds := sessiondata.NewStack(sd)
	defaults := SessionDefaults(map[string]string{
		"application_name": sd.ApplicationName,
	})
	sdMutIterator := makeSessionDataMutatorIterator(sds, defaults, ie.s.cfg.Settings)
	var ex *connExecutor
	var err error
	if txn == nil {
		var postSetupFn func(*connExecutor)
		// Inject any synthetic descriptors into the internal executor after
		// it's created.
		if ie.syntheticDescriptors != nil {
			postSetupFn = func(ex *connExecutor) {
				// Note that we don't need to set shouldResetSyntheticDescriptors
				// since ReleaseAll will be called on the descs.Collection which
				// will also release synthetic descriptors.
				ex.extraTxnState.descCollection.SetSyntheticDescriptors(ie.syntheticDescriptors)
			}
		}
		srvMetrics := &ie.s.InternalMetrics
		if attributeToUser {
			srvMetrics = &ie.s.Metrics
		}
		ex = ie.s.newConnExecutor(
			ctx,
			executorTypeInternal,
			sdMutIterator,
			stmtBuf,
			clientComm,
			// memMetrics is only about attributing memory monitoring to the
			// right metric, so we choose to ignore the 'attributeToUser'
			// boolean and use "internal memory metrics" unconditionally. (We
			// will be using the internal sql executor as the parent during
			// query execution, using different metrics here could lead to
			// confusion.)
			ie.memMetrics,
			srvMetrics,
			applicationStats,
			ie.s.cfg.GenerateID(),
			false, /* underOuterTxn */
			postSetupFn,
		)
	} else {
		ex, err = ie.newConnExecutorWithTxn(
			ctx,
			txn,
			sdMutIterator,
			stmtBuf,
			clientComm,
			applicationStats,
			attributeToUser,
		)
		if err != nil {
			return nil, err
		}
	}

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
	applicationStats *ssmemstorage.Container,
	attributeToUser bool,
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

	var postSetupFn func(*connExecutor)
	// If an internal executor is run with a not-nil txn and has some extra txn
	// state already set up, we may want to let it inherit the descriptor
	// collection, schema change job records and job collections from the
	// caller.
	if ie.extraTxnState != nil {
		postSetupFn = func(ex *connExecutor) {
			ex.extraTxnState.skipResettingSchemaObjects = true
			ex.extraTxnState.descCollection = ie.extraTxnState.descCollection
			ex.extraTxnState.jobs = ie.extraTxnState.jobs
			ex.extraTxnState.schemaChangerState = ie.extraTxnState.schemaChangerState
			ex.extraTxnState.shouldResetSyntheticDescriptors = shouldResetSyntheticDescriptors
		}
	}

	srvMetrics := &ie.s.InternalMetrics
	if attributeToUser {
		srvMetrics = &ie.s.Metrics
	}
	ex = ie.s.newConnExecutor(
		ctx,
		executorTypeInternal,
		sdMutIterator,
		stmtBuf,
		clientComm,
		// memMetrics is only about attributing memory monitoring to the right
		// metric, so we choose to ignore the 'attributeToUser' boolean and use
		// "internal memory metrics" unconditionally. (We will be using the
		// internal sql executor as the parent during query execution, using
		// different metrics here could lead to confusion.)
		ie.memMetrics,
		srvMetrics,
		applicationStats,
		ie.s.cfg.GenerateID(),
		true, /* underOuterTxn */
		postSetupFn,
	)

	if txn.Type() == kv.LeafTxn {
		// If the txn is a leaf txn it is not allowed to perform mutations. For
		// sanity, set read only on the session.
		if err := ex.dataMutatorIterator.applyOnEachMutatorError(func(m sessionDataMutator) error {
			return m.SetReadOnly(true)
		}); err != nil {
			return nil, err
		}
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
		ex.QualityOfService(),
		isolation.Serializable,
		txn.GetOmitInRangefeeds(),
		// TODO(yuzefovich): re-evaluate whether we want to allow buffered
		// writes for internal executor.
		false, /* bufferedWritesEnabled */
	)

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
	row          tree.Datums
	rowsAffected *int
	cols         colinfo.ResultColumns
	err          error
}

type rowsIterator struct {
	r ieResultReader

	rowsAffected int
	resultCols   colinfo.ResultColumns

	mode ieExecutionMode

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
}

var _ isql.Rows = &rowsIterator{}
var _ eval.InternalRows = &rowsIterator{}

func (r *rowsIterator) Next(ctx context.Context) (bool, error) {
	for !r.done && r.lastErr == nil {
		var data ieIteratorResult
		if r.first != nil {
			// This is the very first call to Next() and we have already buffered
			// up the first piece of data before returning rowsIterator to the caller.
			data = *r.first
			r.first = nil
		} else {
			nextItem, done, err := r.r.nextResult(ctx)
			if err != nil || done {
				r.lastErr = err
				break
			}
			data = nextItem
		}

		if data.row != nil {
			r.rowsAffected++
			// No need to make a copy because streamingCommandResult does that for us.
			r.lastRow = data.row
			return true, nil
		}

		if data.rowsAffected != nil {
			r.rowsAffected = *data.rowsAffected
			continue
		}

		// In "rows affected" execution mode we simply ignore the column schema
		// since we always return the number of rows affected (i.e. a single
		// integer column).
		if r.mode == rowsAffectedIEExecutionMode && data.cols != nil {
			continue
		}

		if data.cols != nil {
			r.lastErr = errors.AssertionFailedf("unexpectedly received non-nil cols in Next: %v", data)
		} else if data.err == nil {
			r.lastErr = errors.AssertionFailedf("unexpectedly empty ieIteratorResult object")
		} else {
			r.lastErr = data.err
		}
	}

	r.done = true
	// r.Close() is idempotent, so it's okay to call multiple times.
	_ = r.Close()
	return false, r.lastErr
}

func (r *rowsIterator) Cur() tree.Datums {
	return r.lastRow
}

func (r *rowsIterator) RowsAffected() int {
	return r.rowsAffected
}

func (r *rowsIterator) Close() error {
	// Ensure that we wait for the connExecutor goroutine to exit.
	defer r.wg.Wait()
	// Closing the stmtBuf will tell the connExecutor to stop executing commands
	// (if it hasn't exited yet).
	r.stmtBuf.Close()
	// Close the ieResultReader to tell the writer that we're done.
	if err := r.r.close(); err != nil && r.lastErr == nil {
		r.lastErr = err
	}

	if r.lastErr != nil && r.errCallback != nil {
		r.lastErr = r.errCallback(r.lastErr)
		r.errCallback = nil
	}
	return r.lastErr
}

func (r *rowsIterator) Types() colinfo.ResultColumns {
	return r.resultCols
}

func (r *rowsIterator) HasResults() bool {
	return r.first != nil && r.first.row != nil
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
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, error) {
	return ie.QueryBufferedEx(ctx, opName, txn, ie.maybeNodeSessionDataOverride(opName), stmt, qargs...)
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
	opName redact.RedactableString,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, error) {
	datums, _, err := ie.queryInternalBuffered(ctx, opName, txn, session, ieStmt{stmt: stmt}, 0 /* limit */, qargs...)
	return datums, err
}

// QueryBufferedExWithCols is like QueryBufferedEx, additionally returning the computed
// ResultColumns of the input query.
func (ie *InternalExecutor) QueryBufferedExWithCols(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, colinfo.ResultColumns, error) {
	datums, cols, err := ie.queryInternalBuffered(ctx, opName, txn, session, ieStmt{stmt: stmt}, 0 /* limit */, qargs...)
	return datums, cols, err
}

func (ie *InternalExecutor) queryInternalBuffered(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	sessionDataOverride sessiondata.InternalExecutorOverride,
	stmt ieStmt,
	// Non-zero limit specifies the limit on the number of rows returned.
	limit int,
	qargs ...interface{},
) ([]tree.Datums, colinfo.ResultColumns, error) {
	// We will run the query to completion, so we can use an async result
	// channel.
	rw := newAsyncIEResultChannel()
	it, err := ie.execInternal(ctx, opName, rw, defaultIEExecutionMode, txn, sessionDataOverride, stmt, qargs...)
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
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	return ie.QueryRowEx(ctx, opName, txn, ie.maybeNodeSessionDataOverride(opName), stmt, qargs...)
}

// QueryRowEx is like QueryRow, but allows the caller to override some session data
// fields (e.g. the user).
//
// The fields set in session that are set override the respective fields if they
// have previously been set through SetSessionData().
func (ie *InternalExecutor) QueryRowEx(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	rows, _, err := ie.QueryRowExWithCols(ctx, opName, txn, session, stmt, qargs...)
	return rows, err
}

// QueryRowExParsed is like QueryRowEx, but takes a parsed statement.
func (ie *InternalExecutor) QueryRowExParsed(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	parsedStmt statements.Statement[tree.Statement],
	qargs ...interface{},
) (tree.Datums, error) {
	rows, _, err := ie.queryRowExWithCols(ctx, opName, txn, session, ieStmt{parsed: parsedStmt}, qargs...)
	return rows, err
}

// QueryRowExWithCols is like QueryRowEx, additionally returning the computed
// ResultColumns of the input query.
func (ie *InternalExecutor) QueryRowExWithCols(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, colinfo.ResultColumns, error) {
	return ie.queryRowExWithCols(ctx, opName, txn, session, ieStmt{stmt: stmt}, qargs...)
}

// QueryRowExWithCols is like QueryRowEx, additionally returning the computed
// ResultColumns of the input query.
func (ie *InternalExecutor) queryRowExWithCols(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt ieStmt,
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
		return nil, nil, &tree.MultipleResultsError{SQL: stmt.SQL()}
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
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	stmt string,
	qargs ...interface{},
) (int, error) {
	return ie.ExecEx(ctx, opName, txn, ie.maybeNodeSessionDataOverride(opName), stmt, qargs...)
}

// ExecEx is like Exec, but allows the caller to override some session data
// fields (e.g. the user).
//
// The fields set in session that are set override the respective fields if they
// have previously been set through SetSessionData().
func (ie *InternalExecutor) ExecEx(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (int, error) {
	return ie.execIEStmt(ctx, opName, txn, session, ieStmt{stmt: stmt}, qargs...)
}

// ExecParsed is like Exec but allows the caller to provide an already parsed
// statement.
func (ie *InternalExecutor) ExecParsed(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	o sessiondata.InternalExecutorOverride,
	parsedStmt statements.Statement[tree.Statement],
	qargs ...interface{},
) (int, error) {
	return ie.execIEStmt(ctx, opName, txn, o, ieStmt{parsed: parsedStmt}, qargs...)
}

type ieStmt struct {
	// Only one should be set.
	stmt   string
	parsed statements.Statement[tree.Statement]
}

func (s *ieStmt) SQL() string {
	if s.stmt != "" {
		return s.stmt
	}
	return s.parsed.SQL
}

// execIEStmt extracts the shared logic between ExecEx and ExecParsed.
func (ie *InternalExecutor) execIEStmt(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt ieStmt,
	qargs ...interface{},
) (int, error) {
	// We will run the query to completion, so we can use an async result
	// channel.
	rw := newAsyncIEResultChannel()
	// Since we only return the number of rows affected as given by the
	// rowsIterator, we execute this stmt in "rows affected" mode allowing the
	// internal executor to transparently retry.
	const mode = rowsAffectedIEExecutionMode
	it, err := ie.execInternal(ctx, opName, rw, mode, txn, session, stmt, qargs...)
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
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	stmt string,
	qargs ...interface{},
) (isql.Rows, error) {
	return ie.QueryIteratorEx(ctx, opName, txn, ie.maybeNodeSessionDataOverride(opName), stmt, qargs...)
}

// QueryIteratorEx executes the query, returning an iterator that can be used
// to get the results. If the call is successful, the returned iterator
// *must* be closed.
func (ie *InternalExecutor) QueryIteratorEx(
	ctx context.Context,
	opName redact.RedactableString,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (isql.Rows, error) {
	return ie.execInternal(
		ctx, opName, newSyncIEResultChannel(), defaultIEExecutionMode, txn, session, ieStmt{stmt: stmt}, qargs...,
	)
}

// applyInternalExecutorSessionExceptions overrides values from
// the session data that may have been set from a user-session but
// which don't make sense to use in the InternalExecutor.
func applyInternalExecutorSessionExceptions(sd *sessiondata.SessionData) {
	// Even if session queries are told to error on non-home region accesses,
	// internal queries spawned from the same context should never do so.
	sd.LocalOnlySessionData.EnforceHomeRegion = false
	// DisableBuffering is not supported by the InternalExecutor
	// which uses streamingCommandResults.
	sd.LocalOnlySessionData.AvoidBuffering = false
	// If the internal executor creates a new transaction, then it runs in
	// SERIALIZABLE. If it's used in an existing transaction, then it inherits the
	// isolation level of the existing transaction.
	sd.DefaultTxnIsolationLevel = int64(tree.SerializableIsolation)
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
	// We always override the injection knob based on the override struct.
	sd.InjectRetryErrorsEnabled = o.InjectRetryErrorsEnabled
	if o.OptimizerUseHistograms {
		sd.OptimizerUseHistograms = true
	}
	if o.OriginIDForLogicalDataReplication != 0 {
		sd.OriginIDForLogicalDataReplication = o.OriginIDForLogicalDataReplication
	}
	if o.OriginTimestampForLogicalDataReplication.IsSet() {
		sd.OriginTimestampForLogicalDataReplication = o.OriginTimestampForLogicalDataReplication
	}
	if o.PlanCacheMode != nil {
		sd.PlanCacheMode = *o.PlanCacheMode
	}
	if o.DisablePlanGists {
		sd.DisablePlanGists = true
	}

	if o.MultiOverride != "" {
		overrides := strings.Split(o.MultiOverride, ",")
		for _, override := range overrides {
			parts := strings.Split(override, "=")
			if len(parts) == 2 {
				sd.Update(parts[0], parts[1])
			}
		}
	}
	// Add any new overrides above the MultiOverride.
}

var ieMultiOverride = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"sql.internal_executor.session_overrides",
	"comma-separated list of 'variable=value' pairs that change the corresponding "+
		"session variables used by the InternalExecutor (performed on a best-effort basis)",
	"",
	settings.WithValidateString(func(_ *settings.Values, val string) error {
		if val == "" {
			return nil
		}
		overrides := strings.Split(val, ",")
		for _, override := range overrides {
			parts := strings.Split(override, "=")
			if len(parts) != 2 {
				return errors.Newf("invalid override format: expected 'variable=value', found %q", override)
			}
		}
		return nil
	}),
)

func (ie *InternalExecutor) maybeNodeSessionDataOverride(
	opName redact.RedactableString,
) sessiondata.InternalExecutorOverride {
	if ie.sessionDataStack == nil {
		return sessiondata.InternalExecutorOverride{
			User:            username.NodeUserName(),
			ApplicationName: catconstants.InternalAppNamePrefix + "-" + opName.StripMarkers(),
		}
	}
	o := sessiondata.NoSessionDataOverride
	sd := ie.sessionDataStack.Top()
	if sd.User().Undefined() {
		o.User = username.NodeUserName()
	}
	if sd.ApplicationName == "" {
		o.ApplicationName = catconstants.InternalAppNamePrefix + "-" + opName.StripMarkers()
	}
	return o
}

var rowsAffectedResultColumns = colinfo.ResultColumns{
	colinfo.ResultColumn{
		Name: "rows_affected",
		Typ:  types.Int,
	},
}

const opNameKey = "intExec"

// GetInternalOpName returns the "opName" parameter that was specified when
// issuing a query via the Internal Executor.
func GetInternalOpName(ctx context.Context) (opName string, ok bool) {
	tag, ok := logtags.FromContext(ctx).GetTag(opNameKey)
	if !ok {
		return "", false
	}
	return tag.ValueStr(), true
}

var attributeToUserEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.internal_executor.attribute_to_user.enabled",
	"controls whether internally-executed queries with the AttributeToUser "+
		"override should actually be attributed to user or not",
	true,
)

// execInternal is the main entry point for executing a statement via the
// InternalExecutor. From the high level it does the following:
// - parses the statement as well as its arguments
// - creates an "internal" connExecutor that runs in a separate goroutine
// - pushes a few commands onto the StmtBuf of the connExecutor to be evaluated
// - blocks until the first row of data is sent by the connExecutor
// - returns the rowsIterator that can consume the result of the statement.
//
// Only a single statement is supported. If there are no query arguments, then
// {ExecStmt, Sync} commands are pushed onto the StmtBuf, if there are some
// query arguments, then {PrepareStmt, BindStmt, ExecPortal, Sync} are pushed.
//
// The coordination between the rowsIterator and the connExecutor is managed by
// the internalClientComm as well as the ieResultChannel. The rowsIterator is
// the reader of the ieResultChannel while the connExecutor is the writer. The
// connExecutor goroutine exits (achieved by closing the StmtBuf) once the
// result for the Sync command evaluation is closed.
//
// execInternal defines two callbacks that are passed into the connExecutor
// machinery:
// - syncCallback is called when the result for the Sync command evaluation is
// closed. It is responsible for closing the StmtBuf (to allow the connExecutor
// to exit its 'run' loop) as well iterating over the results to see whether an
// error was encountered. Note that, unlike rows that are sent directly from the
// streamingCommandResult (the writer) to the rowsIterator (the reader), errors
// are buffered in the results - this is needed since the errors might be
// updated by the connExecutor after they have been generated (e.g. replacing
// context cancellation error with a nice "statement timed out" error).
// - errCallback is called when the connExecutor's 'run' returns an error in
// order to propagate the error to the rowsIterator.
//
// It's worth noting that rows as well some metadata (column schema as well as
// "rows affected" number) are sent directly from the streamingCommandResult to
// the rowsIterator, meaning that this communication doesn't go through the
// internalClientComm.
//
// The returned rowsIterator can be synchronized with the connExecutor goroutine
// if "synchronous" ieResultChannel is provided. In this case, only one
// goroutine (among the rowsIterator and the connExecutor) is active at any
// point in time since each read / write is blocked until the "send" / "receive"
// happens on the ieResultChannel.
//
// It's also worth noting that execInternal doesn't return until the
// connExecutor reaches the execution engine (i.e. until after the query
// planning has been performed). This blocking behavior is still respected in
// case a retry error occurs after the column schema is communicated, but before
// the stmt reaches the execution engine. This is needed in order to avoid
// concurrent access to the txn by the rowsIterator and the connExecutor
// goroutines. In particular, this blocking allows us to avoid invalid
// concurrent txn access when during the stmt evaluation the internal executor
// needs to run "nested" internally-executed stmt  (see #62415 for an example).
//
// An additional responsibility of the internalClientComm is handling the retry
// errors. If a retry error is encountered with an implicit txn (i.e. nil txn
// is passed to execInternal), then we do our best to retry the execution
// transparently; however, we can **not** do so in all cases, so sometimes the
// retry error will be propagated to the user of the rowsIterator. In
// particular, here is the summary of how retries are handled:
// - If the retry error occurs after some rows have been sent from the
//   streamingCommandResult to the rowsIterator, we have no choice but to return
//   the retry error to the caller.
//   - The only exception to this is when the stmt of "Rows" type was issued via
//     ExecEx call. In such a scenario, we only need to report the number of
//     "rows affected" that we obtain by counting all rows seen by the
//     rowsIterator. With such a setup, we can transparently retry the execution
//     of the corresponding command by simply resetting the counter when
//     discarding the result of Sync command after the retry error occurs.
// - If the retry error occurs after the "rows affected" metadata was sent for
//   stmts of "RowsAffected" type, then we will always retry transparently. This
//   is achieved by overriding the "rows affected" number, stored in the
//   rowsIterator, with the latest information. With such setup, even if the
//   stmt execution before the retry communicated its incorrect "rows affected"
//   information, that info is overridden accordingly after the connExecutor
//   re-executes the corresponding command.
// - If the retry error occurs after the column schema is sent, then - similar
//   to how we handle the "rows affected" metadata - we always transparently
//   retry by keeping the latest information.
//
// Note that only implicit txns can be retried internally. If an explicit txn is
// passed to execInternal, then the retry error is propagated to the
// rowsIterator in the following manner (say we use {ExecStmt, Sync} commands):
// - ExecStmt evaluation encounters a retry error
// - the error is stored in internalClientComm.results[0] (since it's not
//   propagated right away to the rowsIterator)
// - the connExecutor's state machine rolls back the stmt
// - the connExecutor then processes the Sync command, and when the
//   corresponding result is closed, syncCallback is called
// - in the syncCallback we iterate over two results and find the error in the
//   zeroth result - the error is sent on the ieResultChannel
// - the rowsIterator receives the error and returns it to the caller of
//   execInternal.

// execInternal executes a statement.
//
// sessionDataOverride can be used to control select fields in the executor's
// session data. It overrides what has been previously set through
// SetSessionData(), if anything.
func (ie *InternalExecutor) execInternal(
	ctx context.Context,
	opName redact.RedactableString,
	rw *ieResultChannel,
	mode ieExecutionMode,
	txn *kv.Txn,
	sessionDataOverride sessiondata.InternalExecutorOverride,
	ieStmt ieStmt,
	qargs ...interface{},
) (r *rowsIterator, retErr error) {
	startup.AssertStartupRetry(ctx)

	if err := ie.checkIfTxnIsConsistent(txn); err != nil {
		return nil, err
	}

	ctx = logtags.AddTag(ctx, opNameKey, opName)

	var sd *sessiondata.SessionData
	if ie.sessionDataStack != nil {
		// TODO(andrei): Properly clone (deep copy) ie.sessionData.
		sd = ie.sessionDataStack.Top().Clone()
	} else {
		sd = NewInternalSessionData(context.Background(), ie.s.cfg.Settings, "" /* opName */)
	}
	if globalOverride := ieMultiOverride.Get(&ie.s.cfg.Settings.SV); globalOverride != "" {
		globalOverride = strings.TrimSpace(globalOverride)
		// Prepend the "global" setting overrides to ensure that caller's
		// overrides take precedence.
		if localOverride := sessionDataOverride.MultiOverride; localOverride != "" {
			sessionDataOverride.MultiOverride = globalOverride + "," + localOverride
		} else {
			sessionDataOverride.MultiOverride = globalOverride
		}
	}

	applyInternalExecutorSessionExceptions(sd)
	applyOverrides(sessionDataOverride, sd)
	attributeToUser := sessionDataOverride.AttributeToUser && attributeToUserEnabled.Get(&ie.s.cfg.Settings.SV)
	growStackSize := sessionDataOverride.GrowStackSize
	if !rw.async() && (txn != nil && txn.Type() == kv.RootTxn) {
		// If the "outer" query uses the RootTxn and the sync result channel is
		// requested, then we must disable both DistSQL and Streamer to ensure
		// that the "inner" query doesn't use the LeafTxn (which could result in
		// illegal concurrency).
		sd.DistSQLMode = sessiondatapb.DistSQLOff
		sd.StreamerEnabled = false
	}
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
		sd.ApplicationName = catconstants.InternalAppNamePrefix + "-" + opName.StripMarkers()
	} else if !strings.HasPrefix(sd.ApplicationName, catconstants.InternalAppNamePrefix) {
		// If this is already an "internal app", don't put more prefix.
		sd.ApplicationName = catconstants.DelegatedAppNamePrefix + sd.ApplicationName
	}
	if attributeToUser {
		// If this query should be attributable to user, then we discard
		// previous app name heuristics and use a separate prefix. This is
		// needed since we hard-code filters that exclude queries with '$
		// internal' in their app names on the UI.
		sd.ApplicationName = catconstants.AttributedToUserInternalAppNamePrefix + "-" + opName.StripMarkers()
	}
	// If the caller has injected a mapping to temp schemas, install it, and
	// leave it installed for the rest of the transaction.
	if ie.extraTxnState != nil && sd.DatabaseIDToTempSchemaID != nil {
		p := catsessiondata.NewDescriptorSessionDataStackProvider(sessiondata.NewStack(sd))
		ie.extraTxnState.descCollection.SetDescriptorSessionDataProvider(p)
	}

	numCommands := 2 // ExecStmt -> Sync
	if len(qargs) > 0 {
		numCommands = 4 // PrepareStmt -> BindStmt -> ExecPortal -> Sync
	}
	stmtBuf := NewStmtBuf(numCommands)
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
		} else {
			r.errCallback = func(err error) error {
				if err != nil && !errIsRetriable(err) {
					err = errors.Wrapf(err, "%s", opName)
				}
				return err
			}
		}
	}()

	timeReceived := crtime.NowMono()
	parseStart := timeReceived
	parsed := ieStmt.parsed
	if parsed.AST == nil {
		var err error
		parsed, err = parser.ParseOne(ieStmt.stmt)
		if err != nil {
			return nil, err
		}
	}
	if err := ie.checkIfStmtIsAllowed(parsed.AST, txn); err != nil {
		return nil, err
	}
	parseEnd := crtime.NowMono()

	// Transforms the args to datums. The datum types will be passed as type
	// hints to the PrepareStmt command below.
	datums, err := golangFillQueryArguments(qargs...)
	if err != nil {
		return nil, err
	}

	syncCallback := func(results []*streamingCommandResult) {
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
		}
	}
	// errCallback is called if an error is returned from the connExecutor's
	// run() loop.
	errCallback := func(err error) {
		_ = rw.addResult(ctx, ieIteratorResult{err: err})
	}
	err = ie.runWithEx(ctx, opName, txn, rw, mode, sd, stmtBuf, &wg, syncCallback, errCallback, attributeToUser, growStackSize)
	if err != nil {
		return nil, err
	}

	// We take max(len(s.Types), stmt.NumPlaceHolders) as the length of types.
	numParams := len(datums)
	if parsed.NumPlaceholders > numParams {
		numParams = parsed.NumPlaceholders
	}
	if len(qargs) == 0 {
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
		if err := stmtBuf.Push(ctx, Sync{
			// This is a Sync in the simple protocol, so it isn't marked as explicit.
			ExplicitFromClient: false,
		}); err != nil {
			return nil, err
		}
	} else {
		typeHints := make(tree.PlaceholderTypes, numParams)
		for i, d := range datums {
			typeHints[tree.PlaceholderIdx(i)] = d.ResolvedType()
		}
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
		if err := stmtBuf.Push(ctx, Sync{
			// This is a Sync in the extended protocol, so it's marked as explicit.
			ExplicitFromClient: true,
		}); err != nil {
			return nil, err
		}
	}
	r = &rowsIterator{
		r:       rw,
		mode:    mode,
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
	for !r.done && r.first.cols != nil {
		// If the query is of ROWS statement type, the very first thing sent on
		// the channel will be the column schema. This will occur before the
		// query is given to the execution engine, so we actually need to get
		// the next piece from the data channel.
		//
		// We also need to keep on looping until we get the first actual result
		// with rows. In theory, it is possible for a stmt of ROWS type to
		// encounter a retry error after sending the column schema but before
		// going into the execution engine. In such a scenario we want to keep
		// the latest column schema (in case there was a schema change
		// in-between retries).
		//
		// Note that only statements of ROWS type should send the cols, but we
		// choose to be defensive and don't assert that.
		if parsed.AST.StatementReturnType() == tree.Rows {
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
	// the caller which will clean up the connExecutor goroutine.
	// TODO(yuzefovich): reconsider this and return an error explicitly if
	// r.lastErr is non-nil.
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
		sd = NewInternalSessionData(ctx, ie.s.cfg.Settings, "" /* opName */)
	}

	rw := newAsyncIEResultChannel()
	stmtBuf := NewStmtBuf(0 /* toReserve */)

	// Create a fresh conn executor simply for the purpose of committing the
	// txn.
	// TODO(#124935): this probably will need to change.
	ex, err := ie.initConnEx(
		ctx, ie.extraTxnState.txn, rw, defaultIEExecutionMode, sd, stmtBuf,
		nil /* syncCallback */, false, /* attributeToUser */
	)
	if err != nil {
		return errors.Wrap(err, "cannot create conn executor to commit txn")
	}
	// TODO(janexing): is this correct?
	ex.planner.txn = ie.extraTxnState.txn
	// TODO(#124935): might need to set ex.extraTxnState.shouldExecuteOnTxnFinish
	// to true.

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

// ieExecutionMode determines how the internal executor consumes the results of
// the statement evaluation.
type ieExecutionMode int

const (
	// defaultIEExecutionMode is the execution mode in which the results of the
	// statement evaluation are consumed according to the statement's type.
	defaultIEExecutionMode ieExecutionMode = iota
	// rowsAffectedIEExecutionMode is the execution mode in which the internal
	// executor is only interested in the number of rows affected, regardless of
	// the statement's type.
	//
	// With this mode, if a stmt encounters a retry error, the internal executor
	// will proceed to transparently reset the number of rows affected (if any
	// have been seen by the rowsIterator) and retry the corresponding command.
	// Such behavior makes sense given that in production code at most one
	// command in the StmtBuf results in "rows affected".
	rowsAffectedIEExecutionMode
)

// internalClientComm is an implementation of ClientComm used by the
// InternalExecutor. Result rows are streamed on the channel to the
// ieResultWriter.
type internalClientComm struct {
	// results contains the results of the commands executed by the
	// InternalExecutor.
	//
	// In production setting we expect either two (ExecStmt, Sync) or four
	// (PrepareStmt, BindStmt, ExecPortal, Sync) commands pushed to the StmtBuf,
	// after which point the internalClientComm is no longer used. We also take
	// advantage of the invariant that only a single command is being evaluated
	// at any point in time (i.e. any command is created, evaluated, and then
	// closed / discarded, and only after that a new command can be processed).
	results []*streamingCommandResult
	// resultsScratch is the underlying storage for results.
	resultsScratch [4]*streamingCommandResult

	// The results of the query execution will be written into w.
	w ieResultWriter

	// mode determines how the results of the query execution are consumed.
	mode ieExecutionMode

	// rowsAffectedState is only used in rowsAffectedIEExecutionMode.
	rowsAffectedState struct {
		// rewind is a callback that sends a single ieIteratorResult object to w
		// in order to set the number of rows affected to zero. Used when
		// discarding a result (indicating that a command will be retried).
		rewind func()
		// numRewinds tracks the number of times rewind() has been called.
		numRewinds int64
		// numRewindsLimit is the limit on the number of times we will perform
		// the transparent retry. Once numRewinds reaches numRewindsLimit, the
		// internal executor machinery will no longer retry and, instead, will
		// return the error to the client.
		numRewindsLimit int64
	}

	// sync, if set, is called whenever a Sync is executed with all accumulated
	// results since the last Sync.
	sync func([]*streamingCommandResult)
}

var _ ClientComm = &internalClientComm{}
var _ ClientLock = &internalClientComm{}

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
	_ PortalPausablity,
) CommandResult {
	return icc.createRes(pos)
}

// createRes creates a result.
func (icc *internalClientComm) createRes(pos CmdPos) *streamingCommandResult {
	res := &streamingCommandResult{
		pos: pos,
		w:   icc.w,
		discardCallback: func() {
			// If this result is being discarded, then we can simply remove the
			// last item from the slice. Such behavior is valid since we don't
			// create a new result until the previous one is either closed or
			// discarded (i.e. we are always processing the last entry in the
			// results slice at the moment and all previous results have been
			// "finalized").
			icc.results = icc.results[:len(icc.results)-1]
			if icc.mode == rowsAffectedIEExecutionMode {
				icc.rowsAffectedState.numRewinds++
				icc.rowsAffectedState.rewind()
			}
		},
	}
	icc.results = append(icc.results, res)
	return res
}

// CreatePrepareResult is part of the ClientComm interface.
func (icc *internalClientComm) CreatePrepareResult(pos CmdPos) ParseResult {
	return icc.createRes(pos)
}

// CreateBindResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateBindResult(pos CmdPos) BindResult {
	return icc.createRes(pos)
}

// CreateSyncResult is part of the ClientComm interface.
//
// The returned SyncResult will call the sync callback when it's closed.
func (icc *internalClientComm) CreateSyncResult(pos CmdPos) SyncResult {
	res := icc.createRes(pos)
	if icc.sync != nil {
		res.closeCallback = func() {
			// sync might communicate with the reader, so we defensively mark
			// this result as no longer being able to rewind. This shouldn't be
			// that important though - we shouldn't be trying to rewind the Sync
			// command anyway, so we're being conservative here.
			icc.results[len(icc.results)-1].cannotRewind = true
			icc.sync(icc.results)
			icc.results = icc.results[:0]
		}
	}
	return res
}

// LockCommunication is part of the ClientComm interface.
//
// The current implementation writes results from the same goroutine as the one
// calling LockCommunication (main connExecutor's goroutine). Therefore, there's
// nothing to "lock" - communication is naturally blocked as the command
// processor won't write any more results.
func (icc *internalClientComm) LockCommunication() ClientLock {
	return icc
}

// Flush is part of the ClientComm interface.
func (icc *internalClientComm) Flush(pos CmdPos) error {
	return nil
}

// CreateDescribeResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateDescribeResult(pos CmdPos) DescribeResult {
	return icc.createRes(pos)
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
func (icc *internalClientComm) CreateCopyInResult(cmd CopyIn, pos CmdPos) CopyInResult {
	panic("unimplemented")
}

// CreateCopyOutResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateCopyOutResult(cmd CopyOut, pos CmdPos) CopyOutResult {
	panic("unimplemented")
}

// CreateDrainResult is part of the ClientComm interface.
func (icc *internalClientComm) CreateDrainResult(pos CmdPos) DrainResult {
	panic("unimplemented")
}

// Close is part of the ClientLock interface.
func (icc *internalClientComm) Close() {}

// ClientPos is part of the ClientLock interface.
func (icc *internalClientComm) ClientPos() CmdPos {
	if icc.mode == rowsAffectedIEExecutionMode &&
		icc.rowsAffectedState.numRewinds < icc.rowsAffectedState.numRewindsLimit {
		// With the "rows affected" mode, any command can be rewound since we
		// assume that only a single command results in actual "rows affected",
		// and in Discard we will reset the number to zero (if we were in
		// process of evaluation that command when we encountered the retry
		// error).
		//
		// However, to prevent stack overflow due to large (infinite?) number of
		// retries we also need to check that we haven't reached the limit yet.
		// If we have, then we fall back to the general logic below of
		// determining whether we can retry.
		return -1
	}
	// Find the latest result that cannot be rewound.
	lastDelivered := CmdPos(-1)
	for _, r := range icc.results {
		if r.cannotRewind {
			lastDelivered = r.pos
		}
	}
	return lastDelivered
}

// RTrim is part of the ClientLock interface.
func (icc *internalClientComm) RTrim(_ context.Context, pos CmdPos) {
	for i, r := range icc.results {
		if r.pos >= pos {
			icc.results = icc.results[:i]
			return
		}
	}
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

	// regionsProvider is populated lazily.
	regionsProvider *regions.Provider
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

func (txn *internalTxn) Regions() descs.RegionProvider {
	if txn.extraTxnState.regionsProvider == nil {
		txn.extraTxnState.regionsProvider = regions.NewProvider(
			txn.s.cfg.Codec,
			txn.s.cfg.TenantStatusServer,
			txn.txn,
			txn.extraTxnState.descCollection,
		)
	}
	return txn.extraTxnState.regionsProvider
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

// GetSystemSchemaVersion exposes the schema version from the system db desc.
func (txn *internalTxn) GetSystemSchemaVersion(ctx context.Context) (roachpb.Version, error) {
	sysDB, err := txn.extraTxnState.descCollection.ByIDWithLeased(txn.txn).
		WithoutNonPublic().
		Get().Database(ctx, keys.SystemDatabaseID)

	if err != nil {
		return roachpb.Version{}, err
	}

	v := sysDB.DatabaseDesc().GetSystemDatabaseSchemaVersion()
	if v == nil {
		return roachpb.Version{}, nil
	}
	return *v, nil
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
	ctx context.Context,
	sd *sessiondata.SessionData,
	settings *cluster.Settings,
	txn *kv.Txn,
	descCol *descs.Collection,
) (InternalExecutor, internalExecutorCommitTxnFunc) {
	// By default, if not given session data, we initialize a sessionData that is
	// for the internal "node" user. The sessionData's user can be override when
	// calling the query functions of internal executor.
	// TODO(janexing): since we can be running queries with a higher privilege
	// than the actual user, a security boundary should be added to the error
	// handling of internal executor.
	if sd == nil {
		sd = NewInternalSessionData(ctx, settings, "" /* opName */)
		sd.UserProto = username.NodeUserName().EncodeProto()
		sd.SearchPath = sessiondata.DefaultSearchPathForUser(sd.User())
	}

	schemaChangerState := &SchemaChangerState{
		mode:   sd.NewSchemaChangerMode,
		memAcc: ief.monitor.MakeBoundAccount(),
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
			ie.extraTxnState.schemaChangerState.memAcc.Clear(ctx)
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
		// No descriptors to wait for.
		if len(modifiedDescriptors) == 0 && deletedDescs.Len() == 0 {
			return nil
		}
		retryOpts := retry.Options{
			InitialBackoff: time.Millisecond,
			Multiplier:     1.5,
			MaxBackoff:     time.Second,
		}
		lm := ief.server.cfg.LeaseManager
		cachedRegions, err := regions.NewCachedDatabaseRegions(ctx, ief.server.cfg.DB, ief.server.cfg.LeaseManager)
		if err != nil {
			return err
		}
		for _, ld := range modifiedDescriptors {
			if deletedDescs.Contains(ld.ID) { // we'll wait below
				continue
			}
			_, err := lm.WaitForOneVersion(ctx, ld.ID, cachedRegions, retryOpts)
			// If the descriptor has been deleted, just wait for leases to drain.
			if errors.Is(err, catalog.ErrDescriptorNotFound) {
				err = lm.WaitForNoVersion(ctx, ld.ID, cachedRegions, retryOpts)
			}
			if err != nil {
				return err
			}
		}
		for _, id := range deletedDescs.Ordered() {
			if err := lm.WaitForNoVersion(ctx, id, cachedRegions, retryOpts); err != nil {
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
				ctx,
				cfg.GetSessionData(),
				cf.GetClusterSettings(),
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
			// We check this testing condition here since a retry cannot be generated
			// after a successful commit. Since we commit below, this is our last
			// chance to generate a retry for users of (*InternalDB).Txn.
			if kvTxn.TestingShouldRetry() {
				return kvTxn.GenerateForcedRetryableErr(ctx, "injected retriable error")
			}
			return commitTxnFn(ctx)
		}); errIsRetriable(err) {
			continue
		} else {
			if err == nil {
				err = waitForDescriptors(withNewVersion, deletedDescs)
			}
			return err
		}
	}
}

// SessionDataOverride is a function that can be used to override some
// fields in the session data through all uses of a isql.DB.
//
// This override is applied first; then any additional overrides from
// the sessiondata.InternalExecutorOverride passed to the "*Ex()"
// methods of Executor are applied on top.
//
// This particular override mechanism is useful for packages that do
// not use the "Ex*()" methods or to safeguard the same set of
// overrides throughout all uses (prevents mistakes due to
// inconsistent overrides in different places).
type SessionDataOverride = func(sd *sessiondata.SessionData)

type internalDBWithOverrides struct {
	baseDB               *InternalDB
	sessionDataOverrides []SessionDataOverride
}

var _ isql.DB = (*internalDBWithOverrides)(nil)

// NewInternalDBWithSessionDataOverrides creates a new DB that wraps
// the given DB and customizes the session data. The customizations
// passed here are applied *before* any other customizations via the
// sessiondata.InternalExecutorOverride parameter to the "*Ex()"
// methods of Executor.
func NewInternalDBWithSessionDataOverrides(
	baseDB *InternalDB, sessionDataOverrides ...SessionDataOverride,
) isql.DB {
	return &internalDBWithOverrides{
		baseDB:               baseDB,
		sessionDataOverrides: sessionDataOverrides,
	}
}

// KV is part of the isql.DB interface.
func (db *internalDBWithOverrides) KV() *kv.DB {
	return db.baseDB.KV()
}

// Txn is part of the isql.DB interface.
func (db *internalDBWithOverrides) Txn(
	ctx context.Context, fn func(context.Context, isql.Txn) error, opts ...isql.TxnOption,
) error {
	return db.baseDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		for _, o := range db.sessionDataOverrides {
			o(txn.SessionData())
		}
		return fn(ctx, txn)
	}, opts...)
}

// Executor is part of the isql.DB interface.
func (db *internalDBWithOverrides) Executor(opts ...isql.ExecutorOption) isql.Executor {
	var cfg isql.ExecutorConfig
	cfg.Init(opts...)
	sd := cfg.GetSessionData()
	if sd == nil {
		// NewInternalSessionData is the default value used by InternalExecutor
		// when no session data is provided otherwise.
		sd = NewInternalSessionData(context.Background(), db.baseDB.server.cfg.Settings, "" /* opName */)
	}
	for _, o := range db.sessionDataOverrides {
		o(sd)
	}
	return db.baseDB.Executor(isql.WithSessionData(sd))
}
