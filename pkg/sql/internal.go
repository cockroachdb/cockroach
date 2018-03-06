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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

var _ sqlutil.InternalSQLExecutor = &InternalSQLExecutor{}

// InternalSQLExecutor can be used internally by code modules to execute SQL
// statements without needing to open a SQL connection.
//
// InternalSQLExecutor can execute one statement at a time. As of 03/2018, it
// doesn't offer a session interface for maintaining session state or for
// running explicit SQL transaction. However, it supports running SQL statements
// inside a higher-lever (KV) txn.
type InternalSQLExecutor struct {
	s *Server

	// mon is the monitor used by all queries executed through the
	// InternalSQLExecutor.
	mon *mon.BytesMonitor

	// memMetrics is the memory metrics that queries executed through the
	// InternalSQLExecutor will contribute to.
	memMetrics *MemoryMetrics
}

// MakeInternalSQLExecutor creates an InternalSQLExecutor.
func MakeInternalSQLExecutor(
	ctx context.Context, s *Server, memMetrics *MemoryMetrics, settings *cluster.Settings,
) InternalSQLExecutor {
	monitor := mon.MakeUnlimitedMonitor(
		ctx,
		"internal SQL executor",
		mon.MemoryResource,
		memMetrics.CurBytesCount,
		memMetrics.MaxBytesHist,
		math.MaxInt64, /* noteworthy */
		settings,
	)
	return InternalSQLExecutor{
		s:          s,
		mon:        &monitor,
		memMetrics: memMetrics,
	}
}

// initConnEx creates a connExecutor and runs it on a separate goroutine. It
// returns a StmtBuf into which commands can be pushed and a WaitGroup that will
// be signaled when connEx.run() returns.
func (ie *InternalSQLExecutor) initConnEx(
	ctx context.Context,
	txn *client.Txn,
	sargs SessionArgs,
	syncCallback func([]resWithPos),
	errCallback func(error),
) (*StmtBuf, *sync.WaitGroup) {
	clientComm := &internalClientComm{sync: syncCallback}

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
		if err := ex.run(ctx); err != nil {
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
func (ie *InternalSQLExecutor) Query(
	ctx context.Context, txn *client.Txn, stmt string, qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	res, err := ie.execInternal(ctx, txn, SessionArgs{User: security.RootUser, Database: "system"}, stmt, qargs...)
	if err != nil {
		return nil, nil, err
	}
	return res.rows, res.cols, res.err
}

// QueryWithSessionArgs is like Query, except it takes the session arguments.
// Nothing is filled in by default, not even the user or database.
func (ie *InternalSQLExecutor) QueryWithSessionArgs(
	ctx context.Context, txn *client.Txn, sargs SessionArgs, stmt string, qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	res, err := ie.execInternal(ctx, txn, sargs, stmt, qargs...)
	if err != nil {
		return nil, nil, err
	}
	return res.rows, res.cols, res.err
}

// QueryRow is like Query, except it returns a single row, or nil if not row is
// found, or an error if more that one row is returned.
func (ie *InternalSQLExecutor) QueryRow(
	ctx context.Context, txn *client.Txn, stmt string, qargs ...interface{},
) (tree.Datums, error) {
	rows, _, err := ie.Query(ctx, txn, stmt, qargs...)
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
func (ie *InternalSQLExecutor) Exec(
	ctx context.Context, txn *client.Txn, stmt string, qargs ...interface{},
) (int, error) {
	res, err := ie.execInternal(ctx, txn, SessionArgs{User: security.RootUser, Database: "system"}, stmt, qargs...)
	if err != nil {
		return 0, err
	}
	return res.rowsAffected, res.err
}

// ExecWithSessionArgs is like Exec, except it takes the session arguments.
// Nothing is filled in by default, not even the user or database.
func (ie *InternalSQLExecutor) ExecWithSessionArgs(
	ctx context.Context, txn *client.Txn, sargs SessionArgs, stmt string, qargs ...interface{},
) (int, error) {
	res, err := ie.execInternal(ctx, txn, sargs, stmt, qargs...)
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

func (ie *InternalSQLExecutor) execInternal(
	ctx context.Context, txn *client.Txn, sargs SessionArgs, stmt string, qargs ...interface{},
) (result, error) {
	timeReceived := timeutil.Now()
	parseStart := timeutil.Now()
	s, err := parser.ParseOne(stmt)
	if err != nil {
		return result{}, err
	}
	parseEnd := timeutil.Now()

	// resPos will be set to the position of the command that represents the
	// statement we care about.
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
		resCh <- result{err: errors.Errorf("missing result for pos: %d and no previous error. How could that be?", resPos)}
	}
	errCallback := func(err error) {
		if resultsReceived {
			return
		}
		resCh <- result{err: err}
	}
	stmtBuf, wg := ie.initConnEx(ctx, txn, sargs, syncCallback, errCallback)

	if len(qargs) == 0 {
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
		resPos = 0
	} else {
		if err := stmtBuf.Push(
			ctx,
			PrepareStmt{
				Stmt:       s,
				ParseStart: parseStart,
				ParseEnd:   parseEnd,
			},
		); err != nil {
			return result{}, err
		}

		pinfo := tree.MakePlaceholderInfo()
		golangFillQueryArguments(&pinfo, qargs)
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
		resPos = 2
	}
	if err := stmtBuf.Push(ctx, Sync{}); err != nil {
		return result{}, err
	}

	res := <-resCh
	stmtBuf.Close()
	wg.Wait()
	return res, nil
}

// GetTableSpan gets the key span for a SQL table, including any indices.
func GetTableSpan(
	ctx context.Context, user string, dbName, tableName string, cfg *ExecutorConfig,
) (roachpb.Span, error) {
	var desc *sqlbase.TableDescriptor
	err := cfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// Lookup the table ID.
		p, cleanup := newInternalPlanner(
			"get-table-span", txn, user, cfg.LeaseManager.memMetrics, cfg)
		defer cleanup()
		p.extendedEvalCtx.NodeID = cfg.LeaseManager.LeaseStore.execCfg.NodeID.Get()
		p.extendedEvalCtx.Tables.leaseMgr = cfg.LeaseManager

		tn := tree.MakeTableName(tree.Name(dbName), tree.Name(tableName))
		var err error
		// We avoid the cache because we want to be able to inspect spans
		// without blocking on a lease.
		p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
			desc, err = ResolveExistingObject(ctx, p, &tn, true /*required*/, anyDescType)
		})
		return err
	})
	if err != nil {
		return roachpb.Span{}, err
	}
	tableID := desc.ID

	// Determine table data span.
	tablePrefix := keys.MakeTablePrefix(uint32(tableID))
	tableStartKey := roachpb.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	return roachpb.Span{Key: tableStartKey, EndKey: tableEndKey}, nil
}

// internalClientComm is an implementation of ClientComm used by the
// InternalSQLExecutor. Result rows are buffered in memory.
type internalClientComm struct {
	// results will contain the results of the commands executed by an
	// InternalSQLExecutor.
	results []resWithPos

	lastDelivered CmdPos

	// sync, if set, is called whenever a Sync is executed. It returns all the
	// results since the previous Sync.
	sync func([]resWithPos)
}

type resWithPos struct {
	*bufferingCommandResult
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
func (icc *internalClientComm) createRes(pos CmdPos, onClose func(error)) *bufferingCommandResult {
	res := &bufferingCommandResult{
		closeCallback: func(res *bufferingCommandResult, typ resCloseType, err error) {
			if typ == discarded {
				return
			}
			icc.results = append(icc.results, resWithPos{bufferingCommandResult: res, pos: pos})
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
func (icc *internalClientComm) CreateSyncResult(pos CmdPos) SyncResult {
	return icc.createRes(pos, func(err error) {
		results := make([]resWithPos, len(icc.results))
		copy(results, icc.results)
		icc.results = icc.results[:0]
		icc.sync(results)
		icc.lastDelivered = pos
	})
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
