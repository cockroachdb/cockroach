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
)

// InternalExecutor can be used internally by cockroach to execute SQL
// statements without needing to open a SQL connection. InternalExecutor assumes
// that the caller has access to a cockroach KV client to handle connection and
// transaction management.
type InternalExecutor struct {
	ExecCfg *ExecutorConfig
}

var _ sqlutil.InternalExecutor = &InternalExecutor{}

// ExecuteStatementInTransaction executes the supplied SQL statement as part of
// the supplied transaction. Statements are currently executed as the root user
// with the system database as current database.
func (ie *InternalExecutor) ExecuteStatementInTransaction(
	ctx context.Context, opName string, txn *client.Txn, statement string, qargs ...interface{},
) (int, error) {
	// TODO(andrei): The use of the LeaseManager's memMetrics is very dubious. We
	// should probably pass in the metrics to use.
	p, cleanup := newInternalPlanner(
		opName, txn, security.RootUser, ie.ExecCfg.LeaseManager.memMetrics, ie.ExecCfg)
	defer cleanup()
	ie.initSession(p)
	return p.exec(ctx, statement, qargs...)
}

// QueryRowInTransaction executes the supplied SQL statement as part of the
// supplied transaction and returns the result. Statements are currently
// executed as the root user.
func (ie *InternalExecutor) QueryRowInTransaction(
	ctx context.Context, opName string, txn *client.Txn, statement string, qargs ...interface{},
) (tree.Datums, error) {
	p, cleanup := newInternalPlanner(
		opName, txn, security.RootUser, ie.ExecCfg.LeaseManager.memMetrics, ie.ExecCfg)
	defer cleanup()
	ie.initSession(p)
	return p.QueryRow(ctx, statement, qargs...)
}

// QueryRowsInTransaction executes the supplied SQL statement as part of the
// supplied transaction and returns the resulting rows. Statements are currently
// executed as the root user.
func (ie *InternalExecutor) QueryRowsInTransaction(
	ctx context.Context, opName string, txn *client.Txn, statement string, qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	p, cleanup := newInternalPlanner(
		opName, txn, security.RootUser, ie.ExecCfg.LeaseManager.memMetrics, ie.ExecCfg)
	defer cleanup()
	ie.initSession(p)
	rows, cols, err := p.queryRows(ctx, statement, qargs...)
	if err != nil {
		return nil, nil, err
	}
	return rows, cols, nil
}

// QueryRows is like QueryRowsInTransaction, except it runs a transaction
// internally. Committing the transaction and any required retries are handled
// transparently.
func (ie InternalExecutor) QueryRows(
	ctx context.Context, opName string, statement string, qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {
	var rows []tree.Datums
	var cols sqlbase.ResultColumns
	err := ie.ExecCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		rows, cols, err = ie.QueryRowsInTransaction(ctx, opName, txn, statement, qargs...)
		return err
	})
	return rows, cols, err
}

// GetTableSpan gets the key span for a SQL table, including any indices.
func (ie *InternalExecutor) GetTableSpan(
	ctx context.Context, user string, txn *client.Txn, dbName, tableName string,
) (roachpb.Span, error) {
	// Lookup the table ID.
	p, cleanup := newInternalPlanner(
		"get-table-span", txn, user, ie.ExecCfg.LeaseManager.memMetrics, ie.ExecCfg)
	defer cleanup()
	ie.initSession(p)

	tn := tree.MakeTableName(tree.Name(dbName), tree.Name(tableName))
	var desc *TableDescriptor
	var err error
	// We avoid the cache because we want to be able to inspect spans
	// without blocking on a lease.
	p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
		desc, err = ResolveExistingObject(ctx, p, &tn, true /*required*/, anyDescType)
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

func (ie *InternalExecutor) initSession(p *planner) {
	p.extendedEvalCtx.NodeID = ie.ExecCfg.LeaseManager.LeaseStore.execCfg.NodeID.Get()
	p.extendedEvalCtx.Tables.leaseMgr = ie.ExecCfg.LeaseManager
}

// InternalSQLExecutor can be used internally by code modules to execute SQL
// statements without needing to open a SQL connection.
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

// Query executes the supplied SQL statement and returns the resulting rows.
// The statement is executed as the root user.
func (ie *InternalSQLExecutor) Query(
	ctx context.Context, opName string, stmt string, qargs ...interface{},
) ([]tree.Datums, sqlbase.ResultColumns, error) {

	timeReceived := timeutil.Now()
	parseStart := timeutil.Now()
	s, err := parser.ParseOne(stmt)
	if err != nil {
		return nil, nil, err
	}
	parseEnd := timeutil.Now()

	type result struct {
		rows []tree.Datums
		cols sqlbase.ResultColumns
		err  error
	}

	// resIdx will be set to the index of the result in clientComm that contains
	// corresponds to the statement.
	var resIdx int

	resCh := make(chan result)

	clientComm := &internalClientComm{
		sync: func(results []resWithPos) {
			res := results[resIdx]
			resCh <- result{rows: res.rows, cols: res.cols, err: res.Err()}
		},
	}

	stmtBuf := NewStmtBuf()
	ex := ie.s.newConnExecutor(
		ctx,
		SessionArgs{User: security.RootUser},
		stmtBuf,
		clientComm,
		ie.mon,
		mon.BoundAccount{}, /* reserved */
		ie.memMetrics,
	)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		ex.run(ctx)
		wg.Done()
	}()
	if len(qargs) == 0 {
		if err := stmtBuf.Push(
			ctx,
			ExecStmt{
				Stmt:         s,
				TimeReceived: timeReceived,
				ParseStart:   parseStart,
				ParseEnd:     parseEnd,
			}); err != nil {
			return nil, nil, err
		}
		resIdx = 0
	} else {
		if err := stmtBuf.Push(
			ctx,
			PrepareStmt{
				Stmt:       s,
				ParseStart: parseStart,
				ParseEnd:   parseEnd,
			},
		); err != nil {
			return nil, nil, err
		}

		pinfo := tree.MakePlaceholderInfo()
		golangFillQueryArguments(&pinfo, qargs)
		args := make([]tree.Datum, len(pinfo.Values))
		for k, v := range pinfo.Values {
			i, err := strconv.Atoi(k)
			i--
			if err != nil {
				return nil, nil, err
			}
			args[i] = v.(tree.Datum)
		}

		if err := stmtBuf.Push(ctx, BindStmt{internalArgs: args}); err != nil {
			return nil, nil, err
		}

		if err := stmtBuf.Push(ctx, ExecPortal{TimeReceived: timeReceived}); err != nil {
			return nil, nil, err
		}
		resIdx = 2
	}
	if err := stmtBuf.Push(ctx, Sync{}); err != nil {
		return nil, nil, err
	}

	res := <-resCh
	stmtBuf.Close()
	wg.Wait()
	return res.rows, res.cols, res.err
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
