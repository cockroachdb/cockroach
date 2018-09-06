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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/lib/pq/oid"
)

// PreparedStatement is a SQL statement that has been parsed and the types
// of arguments and results have been determined.
type PreparedStatement struct {
	// Str is the statement string prior to parsing, used to generate
	// error messages. This may be used in
	// the future to present a contextual error message based on location
	// information.
	Str string
	// AnonymizedStr is the anonymized statement string suitable for recording
	// in statement statistics.
	AnonymizedStr string
	// Statement is the parsed, prepared SQL statement. It may be nil if the
	// prepared statement is empty.
	Statement tree.Statement
	// Memo is the memoized data structure constructed by the cost-based optimizer
	// during prepare of a SQL statement. It can significantly speed up execution
	// if it is used by the optimizer as a starting point.
	Memo memo.Memo
	// TypeHints contains the types of the placeholders set by the client. It
	// dictates how input parameters for those placeholders will be parsed. If a
	// placeholder has no type hint, it will be populated during type checking.
	TypeHints tree.PlaceholderTypes
	// Types contains the final types of the placeholders, after type checking.
	// These may differ from the types in TypeHints, if a user provides an
	// imprecise type hint like sending an int for an oid comparison.
	Types   tree.PlaceholderTypes
	Columns sqlbase.ResultColumns

	// InTypes represents the inferred types for placeholder, using protocol
	// identifiers. Used for reporting on Describe.
	InTypes []oid.Oid

	memAcc mon.BoundAccount
}

func (p *PreparedStatement) close(ctx context.Context) {
	p.memAcc.Close(ctx)
}

// preparedStatementsAccessor gives a planner access to a session's collection
// of prepared statements.
type preparedStatementsAccessor interface {
	// Get returns the prepared statement with the given name. The returned bool
	// is false if a statement with the given name doesn't exist.
	Get(name string) (*PreparedStatement, bool)
	// Delete removes the PreparedStatement with the provided name from the
	// collection. If a portal exists for that statement, it is also removed.
	// The method returns true if statement with that name was found and removed,
	// false otherwise.
	Delete(ctx context.Context, name string) bool
	// DeleteAll removes all prepared statements and portals from the coolection.
	DeleteAll(ctx context.Context)
}

// PreparedPortal is a PreparedStatement that has been bound with query arguments.
type PreparedPortal struct {
	Stmt  *PreparedStatement
	Qargs tree.QueryArguments

	// OutFormats contains the requested formats for the output columns.
	OutFormats []pgwirebase.FormatCode

	memAcc mon.BoundAccount
}

// newPreparedPortal creates a new PreparedPortal.
//
// When no longer in use, the PreparedPortal needs to be close()d.
func (ex *connExecutor) newPreparedPortal(
	ctx context.Context,
	name string,
	stmt *PreparedStatement,
	qargs tree.QueryArguments,
	outFormats []pgwirebase.FormatCode,
) (*PreparedPortal, error) {
	portal := &PreparedPortal{
		Stmt:       stmt,
		Qargs:      qargs,
		OutFormats: outFormats,
		memAcc:     ex.sessionMon.MakeBoundAccount(),
	}
	sz := int64(uintptr(len(name)) + unsafe.Sizeof(*portal))
	if err := portal.memAcc.Grow(ctx, sz); err != nil {
		return nil, err
	}
	return portal, nil
}

func (p *PreparedPortal) close(ctx context.Context) {
	p.memAcc.Close(ctx)
}
