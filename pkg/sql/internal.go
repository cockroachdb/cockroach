// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

var _ sqlutil.InternalExecutor = &InternalExecutor{}

// InternalExecutor can be used internally by code modules to execute SQL
// statements without needing to open a SQL connection.
//
// InternalExecutor can execute one statement at a time. As of 03/2018, it
// doesn't offer a session interface for maintaining session state or for
// running explicit SQL transactions. However, it supports running SQL
// statements inside a higher-lever (KV) txn.
type InternalExecutor struct {
	s *Server

	// mon is the monitor used by all queries executed through the
	// InternalExecutor.
	mon *mon.BytesMonitor

	// memMetrics is the memory metrics that queries executed through the
	// InternalExecutor will contribute to.
	memMetrics *MemoryMetrics
}

// MakeInternalExecutor creates an InternalExecutor.
func MakeInternalExecutor(
	ctx context.Context, s *Server, memMetrics *MemoryMetrics, settings *cluster.Settings,
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

// initConnEx creates a connExecutor and runs it on a separate goroutine. It
// returns a StmtBuf into which commands can be pushed and a WaitGroup that will
// be signaled when connEx.run() returns.
func (ie *InternalExecutor) initConnEx(
	ctx context.Context,
	txn *client.Txn,
	sargs SessionArgs,
	syncCallback func([]resWithPos),
	errCallback func(error),
) (*StmtBuf, *sync.WaitGroup) {
	clientComm := &internalClientComm{
		sync: syncCallback,
		// init lastDelivered below the position of the first result (0).
		lastDelivered: -1,
	}

	stmtBuf := NewStmtBuf()
	var ex *connExecutor
	if txn == nil {
		ex = ie.s.newConnExecutor(
			ctx,
			sargs,
			stmtBuf,
			clientComm,
			ie.mon,
			mon.BoundAccount{}, /* reserved */
			ie.memMetrics)
	} else {
		ex = ie.s.newConnExecutorWithTxn(
			ctx,
			sargs,
			stmtBuf,
			clientComm,
			ie.mon,
			mon.BoundAccount{}, /* reserved */
			ie.memMetrics,
			txn)
	}
	ex.stmtCounterDisabled = true

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// Once ex.run() returns, we call ex.resetExtraTxnState() to release the
		// leases that might have been acquired by the connEx. If there is no
		// higher-level txn, then the leases were already released when the implicit
		// txn committed. But if there is a higher-level txn, they don't get
		// released.
		defer func() {
			if err := ex.resetExtraTxnState(ctx, txnAborted, ex.server.dbCache); err != nil {
				log.Warningf(ctx, "error while cleaning up connExecutor: %s", err)
			}
		}()
		if err := ex.run(ctx, nil /* cancel */); err != nil {
			errCallback(err)
		}
		wg.Done()
	}()
	return stmtBuf, &wg
}

// Query executes the supplied SQL statement and returns the resulting rows.
// The statement is executed as the root user.
//
// If txn is not nil, the statement will be executed in the respective txn.
func (ie *InternalExecutor) Query(
	ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	res, err := ie.execInternal(
		ctx, opName, txn, SessionArgs{User: security.RootUser, Database: "system"}, stmt, qargs...)
	if err != nil {
		return nil, nil, err
	}
	return res.rows, res.cols, res.err
}

// QueryWithSessionArgs is like Query, except it takes the session arguments.
// Nothing is filled in by default, not even the user or database.
func (ie *InternalExecutor) QueryWithSessionArgs(
	ctx context.Context,
	opName string,
	txn *client.Txn,
	sargs SessionArgs,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	res, err := ie.execInternal(ctx, opName, txn, sargs, stmt, qargs...)
	if err != nil {
		return nil, nil, err
	}
	return res.rows, res.cols, res.err
}

// QueryRow is like Query, except it returns a single row, or nil if not row is
// found, or an error if more that one row is returned.
func (ie *InternalExecutor) QueryRow(
	ctx context.Context, opName string, txn *client.Txn, stmt string, qargs ...interface{},
) (tree.Datums, error) {
	rows, _, err := ie.Query(ctx, opName, txn, stmt, qargs...)
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
		ctx, opName, txn, SessionArgs{User: security.RootUser, Database: "system"}, stmt, qargs...,
	)
	if err != nil {
		return 0, err
	}
	return res.rowsAffected, res.err
}

// ExecWithSessionArgs is like Exec, except it takes the session arguments.
// Nothing is filled in by default, not even the user or database.
func (ie *InternalExecutor) ExecWithSessionArgs(
	ctx context.Context,
	opName string,
	txn *client.Txn,
	sargs SessionArgs,
	stmt string,
	qargs ...interface{},
) (int, error) {
	res, err := ie.execInternal(ctx, opName, txn, sargs, stmt, qargs...)
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

func (ie *InternalExecutor) execInternal(
	ctx context.Context,
	opName string,
	txn *client.Txn,
	sargs SessionArgs,
	stmt string,
	qargs ...interface{},
) (retRes result, retErr error) {

	defer func() {
		if retErr != nil {
			retErr = errors.Wrap(retErr, opName)
		}
		if retRes.err != nil {
			retRes.err = errors.Wrap(retRes.err, opName)
		}
	}()

	ctx, finishSp := tracing.EnsureChildSpan(ctx, ie.s.cfg.AmbientCtx.Tracer, opName)
	defer finishSp()
	ctx = log.WithLogTag(ctx, "intExec", opName)

	timeReceived := timeutil.Now()
	parseStart := timeReceived
	s, err := parser.ParseOne(stmt)
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
		resCh <- result{err: errors.Errorf(
			"programming error: missing result for pos: %d and no previous error", resPos)}
	}
	errCallback := func(err error) {
		if resultsReceived {
			return
		}
		resCh <- result{err: err}
	}
	stmtBuf, wg := ie.initConnEx(ctx, txn, sargs, syncCallback, errCallback)

	// Transforms the args to datums. The datum types will be passed as type hints
	// to the PrepareStmt command.
	pinfo := tree.MakePlaceholderInfo()
	golangFillQueryArguments(&pinfo, qargs)
	if len(qargs) == 0 {
		resPos = 0
		if err := stmtBuf.Push(
			ctx,
			ExecStmt{
				Stmt:         s,
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
				Stmt:       s,
				ParseStart: parseStart,
				ParseEnd:   parseEnd,
				TypeHints:  pinfo.Types,
			},
		); err != nil {
			return result{}, err
		}

		args := make([]tree.Datum, len(pinfo.Values))
		for k, v := range pinfo.Values {
			i, err := strconv.Atoi(k)
			i--
			if err != nil {
				return result{}, err
			}
			args[i] = v.(tree.Datum)
		}

		if err := stmtBuf.Push(ctx, BindStmt{internalArgs: args}); err != nil {
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
	_ tree.Statement, _ RowDescOpt, pos CmdPos, _ []pgwirebase.FormatCode, _ *time.Location,
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
	panic("unimplemented")
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
