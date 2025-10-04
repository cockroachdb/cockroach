// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package isql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	Execute(ctx context.Context, stmt PreparedStatement, qargs tree.Datums) (int, error)

	// Query executes a prepared statement and returns the result.
	//
	// Example:
	// result, err := session.Query(ctx, stmt, []tree.Datum{tree.NewDInt(1)})
	// if err != nil {
	// 	return err
	// }
	Query(ctx context.Context, stmt PreparedStatement, qargs tree.Datums) ([]tree.Datums, error)

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

	// Close closes the session and cleans up internal resources.
	Close(ctx context.Context)
}
