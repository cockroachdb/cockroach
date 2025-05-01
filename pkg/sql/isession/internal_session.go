// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package isession

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// MUSING: we could have two internal sessions.
// 1. Optimized verision that operates using internal types.
// 2. Convinient version that operates within

// TODO disable buffered writes

type InternalStatement struct {
	Name string
}

type InternalSession struct {
	conn       sql.ConnectionHandler
	connStatus chan error

	stmtBuf *sql.StmtBuf
	results *resultBuffer
}

func NewInternalSession(
	ctx context.Context,
	sessionName redact.SafeString,
	connectionHandlerFactory sql.ConnectionHandlerFactory,
	metrics sql.MemoryMetrics,
	config *sql.ExecutorConfig,
) (*InternalSession, error) {
	// ExecStmt + Sync
	stmtBuf := sql.NewStmtBuf(2)

	i := &InternalSession{
		stmtBuf:    stmtBuf,
		results:    newResultBuffer(),
		connStatus: make(chan error, 1),
	}

	args := sql.SessionArgs{
		// TODO(jeffswenson): what user does the internal executor use?
		User:        username.RootUserName(),
		IsSuperuser: true,
		SessionDefaults: sql.SessionDefaults{
			// TODO(jeffswenson): should we use a database in the session or should
			// we require every name to be fully qualified?
			"database":                               "defaultdb",
			"kv_transaction_buffered_writes_enabled": "false",
			"plan_cache_mode":                        "force_generic",
		},
	}

	// TODO(jeffswenson): ensure we handle fatal session errors
	conn, err := connectionHandlerFactory.SetupConn(
		ctx,
		args,
		i.stmtBuf,
		i.results,
		metrics,
		func(newSize int32) {}, // onDefaultIntSizeChange
		config.GenerateID(),    // sessionID
	)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create conn for internal session")
	}

	i.conn = conn

	// TODO(jeffswenson): we can get rid of this goroutine by adding a `Step()`
	// function to ServerConn that consumes the next command in the statement
	// buffer and returns immediately if the buffer is empty. That would be nice
	// because it makes it easier to measure LDR SQL cpu usage elastic CPU usage
	// and allows exposing the KV transaction owned by the session to a goroutine
	// since we can guarantee that the conn executor is not doing any work.
	err = config.Stopper.RunAsyncTaskEx(
		ctx,
		stop.TaskOpts{
			TaskName: string(sessionName),
			// TODO(jeffswenson): should I set child span here? It's set on internal
			// executor but this isn't scoped in the same way.
		},
		func(ctx context.Context) {
			i.connStatus <- connectionHandlerFactory.ServeConn(
				ctx,
				i.conn,
				&mon.BoundAccount{},
				nil,
			)
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "unable to start conn executor")
	}

	return i, nil
}

func (i *InternalSession) Txn(ctx context.Context, do func(ctx context.Context) error) error {
	begin := statements.Statement[tree.Statement]{
		AST: &tree.BeginTransaction{},
	}
	commit := statements.Statement[tree.Statement]{
		AST: &tree.CommitTransaction{},
	}
	rollback := statements.Statement[tree.Statement]{
		AST: &tree.RollbackTransaction{},
	}

	// TODO add retries

	err := i.executeRaw(ctx, begin)
	if err != nil {
		// If we fail to begin the transaction, we should not retry.
		return errors.Wrap(err, "unable to begin transaction")
	}

	err = do(ctx)
	if err != nil {
		rollbackErr := i.executeRaw(ctx, rollback)
		return errors.CombineErrors(err, rollbackErr)
	} else {
		err = i.executeRaw(ctx, commit)
		if err != nil {
			// If we fail to commit the transaction, we should not retry.
			return errors.Wrap(err, "unable to commit transaction")
		}
	}

	return nil
}

func (i *InternalSession) Prepare(
	ctx context.Context, name string, stmt statements.Statement[tree.Statement], types []*types.T,
) (InternalStatement, error) {
	var typReferences []tree.ResolvableTypeReference
	for _, typ := range types {
		typReferences = append(typReferences, typ)
	}

	now := crtime.NowMono()
	err := i.stmtBuf.Push(ctx, sql.PrepareStmt{
		Name:       name,
		TypeHints:  types,
		Statement:  stmt,
		ParseStart: now,
		ParseEnd:   now,
	})
	if err != nil {
		return InternalStatement{}, errors.Wrap(err, "unable to push prepare statement")
	}

	// NOTE: we need a sync statement here in order to get the error for the
	// prepared statement. If the prepare fails, the conn executor will skip
	// messages until the next sync statement.
	err = i.stmtBuf.Push(ctx, sql.Sync{
		ExplicitFromClient: true,
	})
	if err != nil {
		return InternalStatement{}, errors.Wrap(err, "unable to push sync statement")
	}

	_, err = i.readResults(ctx, i.stmtBuf.Last())
	if err != nil {
		return InternalStatement{}, err
	}

	return InternalStatement{Name: name}, nil
}

func (i *InternalSession) Execute(
	ctx context.Context, prepared InternalStatement, qargs tree.Datums,
) (int, error) {
	// TODO: if we are in a transaction, automatically create a
	// savepoint for each statement.
	err := i.stmtBuf.Push(ctx, sql.BindStmt{
		PreparedStatementName: prepared.Name,
		InternalArgs:          qargs,
	})
	if err != nil {
		return 0, errors.Wrap(err, "unable to push bind statement")
	}

	err = i.stmtBuf.Push(ctx, sql.ExecPortal{
		TimeReceived:   crtime.NowMono(),
		FollowedBySync: true,
	})
	if err != nil {
		return 0, errors.Wrap(err, "unable to push exec statement")
	}

	err = i.stmtBuf.Push(ctx, sql.Sync{
		ExplicitFromClient: true,
	})
	if err != nil {
		return 0, errors.Wrap(err, "unable to push sync statement")
	}

	return i.readResults(ctx, i.stmtBuf.Last())
}

func (i *InternalSession) executeRaw(
	ctx context.Context, stmt statements.Statement[tree.Statement],
) error {
	err := i.stmtBuf.Push(ctx, sql.ExecStmt{
		Statement: stmt,
	})
	if err != nil {
		return errors.Wrap(err, "unable to push raw statement")
	}
	err = i.stmtBuf.Push(ctx, sql.Sync{
		ExplicitFromClient: true,
	})
	if err != nil {
		return errors.Wrap(err, "unable to push sync statement")
	}
	_, err = i.readResults(ctx, i.stmtBuf.Last())
	return errors.Wrap(err, "unable to execute raw statement")
}

func (i *InternalSession) readResults(ctx context.Context, pos sql.CmdPos) (int, error) {
	var resultErr error
	var rowCount int

	for {
		result := i.results.Next()
		if result.err != nil {
			resultErr = errors.CombineErrors(result.err, result.err)
		}

		rowCount += result.RowsAffected()

		if result.pos == pos {
			break
		}
	}

	return rowCount, resultErr
}

func (i *InternalSession) Close(ctx context.Context) {
	i.stmtBuf.Close()
	// TODO(jeffswenson): close the stmt buffer
	// TODO(jeffswenson): wait for connection to shutdown
}
