// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package isql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionmutator"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// PreparedStatement is a prepared statement in the session. Prepared statements
// are not shared between sessions.
type PreparedStatement struct {
	Name string
}

type Session interface {
	// Prepare creates a prepared statement with the given name in the session. The types slice are the types of the
	// prepared statement's parameters.
	//
	// Example:
	// stmt, err := parser.ParseOne("SELECT name FROM users WHERE id = $1")
	// if err != nil {
	// 	return err
	// }
	// stmt, err := session.Prepare(ctx, "my_prepared_stmt", stmt, []*types.T{types.Int})
	// if err != nil {
	// 	return err
	// }
	Prepare(ctx context.Context, name string, stmt statements.Statement[tree.Statement], types []*types.T) (PreparedStatement, error)

	// Execute executes a prepared statement with the given arguments.
	//
	// Example:
	// result, err := session.Execute(ctx, stmt, []tree.Datum{tree.NewDInt(1)})
	// if err != nil {
	// 	return err
	// }
	ExecutePrepared(ctx context.Context, stmt PreparedStatement, qargs tree.Datums) (int, error)

	// Query executes a prepared statement and returns the result.
	//
	// Example:
	// result, err := session.Query(ctx, stmt, []tree.Datum{tree.NewDInt(1)})
	// if err != nil {
	// 	return err
	// }
	QueryPrepared(ctx context.Context, stmt PreparedStatement, qargs tree.Datums) ([]tree.Datums, error)

	// Txn executes a transaction with the given function. It will retry
	// serialization failures until the context is cancelled.
	//
	// Example:
	// err := session.Txn(ctx, func(ctx context.Context) error {
	// 	return session.Execute(ctx, stmt, []tree.Datum{tree.NewDInt(1)})
	// })
	// if err != nil {
	// 	return err
	// }
	Txn(ctx context.Context, do func(context.Context) error) error

	// Savepoint creates a savepoint within an existing transaction and executes
	// the given function. If the function returns an error, the savepoint is
	// rolled back. If the function succeeds, the savepoint is released.
	// Savepoints must be used within a transaction.
	//
	// TODO(jeffswenson): we should have Savepoint return an error if it is called
	// outside of a transaction or transparently open a transaction. Right now,
	// using a savepoint outside of a transaction returns an error, but after the
	// `do` function is executed. For some reason Postgres does not produce an
	// error if you create a savepoint outside of a transaction, but it does not
	// remember the savepoint and returns an error when you attempt to release it.
	//
	// Example:
	// err := session.Txn(ctx, func(ctx context.Context) error {
	// 	return session.Savepoint(ctx, func(ctx context.Context) error {
	// 		return session.ExecutePrepared(ctx, stmt, []tree.Datum{tree.NewDInt(1)})
	// 	})
	// })
	// if err != nil {
	// 	return err
	// }
	Savepoint(ctx context.Context, do func(context.Context) error) error

	// ModifySession executes a function that mutates the session using the
	// sessionmutator.SessionDataMutator argument.
	//
	// Example:
	// err := session.ModifySession(ctx, func(mutator sessionmutator.SessionDataMutator) {
	// 	mutator.SetApplicationName("my_app")
	// 	mutator.SetDatabase("my_database")
	// })
	// if err != nil {
	// 	return err
	// }
	ModifySession(ctx context.Context, mutate func(mutator sessionmutator.SessionDataMutator)) error

	// Close closes the session and cleans up internal resources.
	Close(ctx context.Context)
}
