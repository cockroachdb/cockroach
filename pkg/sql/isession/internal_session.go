// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package isession

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionmutator"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
)

// InternalSession is a stateful alternative to the isql.Executor. For now, its
// focused on high performance use cases that need prepared statements and
// generic query plans like LDR. Long term, it should become the implementation
// of the InternalExecutor.
//
// TODO(jeffswenson): consider adding implicit save points to internal session
// or a save point helper. This isn't as important for the internal executor
// because statement failures like unique constraint validation failure will
// poison the txn like a normal pgwire connection.
type InternalSession struct {
	csm     *sql.ConnectionStateMachine
	results *resultBuffer

	// poison is set if the session has encountered a fatal error and can no
	// longer be used. It must be checked before every operation.
	poison error
}

// Statements that are used internally by the InternalSession.
var (
	begin = statements.Statement[tree.Statement]{
		AST: &tree.BeginTransaction{},
		SQL: "BEGIN",
	}
	commit = statements.Statement[tree.Statement]{
		AST: &tree.CommitTransaction{},
		SQL: "COMMIT",
	}
	rollback = statements.Statement[tree.Statement]{
		AST: &tree.RollbackTransaction{},
		SQL: "ROLLBACK",
	}

	savepointBegin = statements.Statement[tree.Statement]{
		AST: &tree.Savepoint{
			Name: tree.Name("internal_session"),
		},
		SQL: "SAVEPOINT internal_session",
	}
	savepointRelease = statements.Statement[tree.Statement]{
		AST: &tree.ReleaseSavepoint{
			Savepoint: tree.Name("internal_session"),
		},
		SQL: "RELEASE SAVEPOINT internal_session",
	}
	savepointRollback = statements.Statement[tree.Statement]{
		AST: &tree.RollbackToSavepoint{
			Savepoint: tree.Name("internal_session"),
		},
		SQL: "ROLLBACK TO SAVEPOINT internal_session",
	}
)

var _ isql.Session = &InternalSession{}

type StateMachineFactory func(ctx context.Context, sessionName string, args sql.SessionArgs) sql.ConnectionStateMachine

func NewInternalSession(
	ctx context.Context, csm *sql.ConnectionStateMachine,
) (isql.Session, error) {
	is := &InternalSession{
		results: newResultBuffer(),
		csm:     csm,
	}
	if err := csm.Init(ctx, is.results); err != nil {
		is.Close(ctx)
		return nil, errors.Wrap(err, "unable to initialize connection state machine")
	}
	return is, nil
}

func (i *InternalSession) Txn(ctx context.Context, do func(ctx context.Context) error) error {
	if i.poison != nil {
		return i.poison
	}

	try := func() error {
		err := i.executeStatement(ctx, begin)
		if err != nil {
			return errors.Wrap(err, "unable to begin transaction")
		}

		err = do(ctx)
		if err != nil {
			rollbackErr := errors.Wrap(
				i.executeStatement(ctx, rollback),
				"rollback failed")
			return errors.CombineErrors(err, rollbackErr)
		}

		return i.executeStatement(ctx, commit)
	}

	var err error
	retryOpts := base.DefaultRetryOptions()
	retryOpts.InitialBackoff = 1 * time.Millisecond
	retryOpts.MaxBackoff = 200 * time.Millisecond
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		if err := ctx.Err(); err != nil {
			return err
		}
		err = try()
		if err == nil {
			return nil
		}
		if !sql.ErrIsRetryable(err) {
			return err
		}
		// Continue retry loop for serialization failures
	}

	return err
}

func (i *InternalSession) Savepoint(ctx context.Context, do func(ctx context.Context) error) error {
	if i.poison != nil {
		return i.poison
	}

	if err := i.executeStatement(ctx, savepointBegin); err != nil {
		return errors.Wrap(err, "failed to create savepoint")
	}

	innerErr := do(ctx)
	if innerErr != nil {
		// Return the rollback error as primary since it indicates a more
		// serious problem than the original error.
		savePointErr := i.executeStatement(ctx, savepointRollback)
		if savePointErr != nil {
			return errors.CombineErrors(savePointErr, innerErr)
		}
		// NOTE: Rollback does not release the savepoint. We need to release the
		// savepoint to avoid leaking it and causing weird behavior if the user is
		// nesting savepoint calls.
		releaseErr := i.executeStatement(ctx, savepointRelease)
		if releaseErr != nil {
			return errors.CombineErrors(releaseErr, innerErr)
		}
		return innerErr
	}

	if err := i.executeStatement(ctx, savepointRelease); err != nil {
		return errors.Wrap(err, "failed to release the savepoint")
	}

	return nil
}

func (i *InternalSession) Prepare(
	ctx context.Context, name string, stmt statements.Statement[tree.Statement], types []*types.T,
) (isql.PreparedStatement, error) {
	if i.poison != nil {
		return isql.PreparedStatement{}, i.poison
	}

	now := crtime.NowMono()
	err := i.csm.Push(ctx, sql.PrepareStmt{
		Name:       name,
		TypeHints:  types,
		Statement:  stmt,
		ParseStart: now,
		ParseEnd:   now,
	})
	if err != nil {
		return isql.PreparedStatement{}, errors.Wrap(err, "unable to push prepare statement")
	}

	err = i.csm.Push(ctx, sql.Sync{
		ExplicitFromClient: true,
	})
	if err != nil {
		return isql.PreparedStatement{}, errors.Wrap(err, "unable to push sync statement")
	}

	_, _, err = i.readResults(ctx)
	if err != nil {
		return isql.PreparedStatement{}, err
	}

	return isql.PreparedStatement{Name: name}, nil
}

func (i *InternalSession) ExecutePrepared(
	ctx context.Context, prepared isql.PreparedStatement, qargs tree.Datums,
) (int, error) {
	if i.poison != nil {
		return 0, i.poison
	}

	err := i.csm.Push(ctx, sql.BindStmt{
		PreparedStatementName: prepared.Name,
		InternalArgs:          qargs,
	})
	if err != nil {
		return 0, errors.Wrap(err, "unable to push bind statement")
	}

	err = i.csm.Push(ctx, sql.ExecPortal{
		TimeReceived:   crtime.NowMono(),
		FollowedBySync: true,
	})
	if err != nil {
		return 0, errors.Wrap(err, "unable to push exec statement")
	}

	err = i.csm.Push(ctx, sql.Sync{
		ExplicitFromClient: true,
	})
	if err != nil {
		return 0, errors.Wrap(err, "unable to push sync statement")
	}

	_, rowCount, err := i.readResults(ctx)
	return rowCount, err
}

func (i *InternalSession) QueryPrepared(
	ctx context.Context, prepared isql.PreparedStatement, qargs tree.Datums,
) ([]tree.Datums, error) {
	if i.poison != nil {
		return nil, i.poison
	}

	err := i.csm.Push(ctx, sql.BindStmt{
		PreparedStatementName: prepared.Name,
		InternalArgs:          qargs,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to push bind statement")
	}

	err = i.csm.Push(ctx, sql.ExecPortal{
		TimeReceived:   crtime.NowMono(),
		FollowedBySync: true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to push exec statement")
	}

	err = i.csm.Push(ctx, sql.Sync{
		ExplicitFromClient: true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to push sync statement")
	}

	rows, _, err := i.readResults(ctx)
	return rows, err
}

func (i *InternalSession) executeStatement(
	ctx context.Context, stmt statements.Statement[tree.Statement],
) error {
	err := i.csm.Push(ctx, sql.ExecStmt{
		Statement: stmt,
	})
	if err != nil {
		return errors.Wrap(err, "unable to push raw statement")
	}
	err = i.csm.Push(ctx, sql.Sync{
		ExplicitFromClient: true,
	})
	if err != nil {
		return errors.Wrap(err, "unable to push sync statement")
	}
	_, _, err = i.readResults(ctx)
	return errors.Wrapf(err, "unable to execute raw statement %s", stmt.SQL)
}

func (i *InternalSession) readResults(ctx context.Context) ([]tree.Datums, int, error) {
	var resultErr error
	var rowCount int

	// Consume every event that was pushed into connection state machine before
	// we read the results.
	for {
		done, err := i.csm.RunOneCmd(ctx)
		if err != nil {
			i.poison = errors.Wrap(err, "poisoned internal session")
			return nil, 0, errors.Wrap(err, "error stepping connection state machine")
		}
		if done {
			break
		}
	}

	// We fully consume the results so that the result buffer is ready for the
	// next operation.
	var rows []tree.Datums
	for {
		result, ok := i.results.Next()
		if !ok {
			break
		}
		if result.err != nil {
			resultErr = errors.CombineErrors(resultErr, result.err)
		}

		rowCount += result.RowsAffected()
		rows = append(rows, result.rows...)
	}

	return rows, rowCount, resultErr
}

func (i *InternalSession) ModifySession(
	ctx context.Context, mutate func(mutator sessionmutator.SessionDataMutator),
) error {
	if i.poison != nil {
		return i.poison
	}

	sdMutIterator := i.csm.SessionDataMutatorIterator()
	return sdMutIterator.ApplyOnTopMutator(func(m sessionmutator.SessionDataMutator) error {
		mutate(m)
		return nil
	})
}

func (i *InternalSession) Close(ctx context.Context) {
	i.csm.Close(ctx)
}

func init() {
	sql.ISessionFactoryHook = NewInternalSession
}
