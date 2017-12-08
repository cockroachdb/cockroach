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

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
	// TypeHints contains the types of the placeholders set by the client. It
	// dictates how input parameters for those placeholders will be parsed. If a
	// placeholder has no type hint, it will be populated during type checking.
	TypeHints tree.PlaceholderTypes
	// Types contains the final types of the placeholders, after type checking.
	// These may differ from the types in TypeHints, if a user provides an
	// imprecise type hint like sending an int for an oid comparison.
	Types   tree.PlaceholderTypes
	Columns sqlbase.ResultColumns
	// TODO(andrei): The connExecutor doesn't use this. Delete it once the
	// Executor is gone.
	portalNames map[string]struct{}

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

// PreparedStatements is a mapping of PreparedStatement names to their
// corresponding PreparedStatements.
type PreparedStatements struct {
	session *Session
	stmts   map[string]*PreparedStatement
}

var _ preparedStatementsAccessor = &PreparedStatements{}

func makePreparedStatements(s *Session) PreparedStatements {
	return PreparedStatements{
		session: s,
		stmts:   make(map[string]*PreparedStatement),
	}
}

// Get returns the PreparedStatement with the provided name.
func (ps *PreparedStatements) Get(name string) (*PreparedStatement, bool) {
	stmt, ok := ps.stmts[name]
	return stmt, ok
}

// Exists returns whether a PreparedStatement with the provided name exists.
func (ps PreparedStatements) Exists(name string) bool {
	_, ok := ps.Get(name)
	return ok
}

// NewFromString creates a new PreparedStatement with the provided name and
// corresponding query string, using the given PlaceholderTypes hints to assist
// in inferring placeholder types.
//
// ps.session.Ctx() is used as the logging context for the prepare operation.
func (ps PreparedStatements) NewFromString(
	e *Executor, name, query string, placeholderHints tree.PlaceholderTypes,
) (*PreparedStatement, error) {
	sessionEventf(ps.session, "parsing: %s", query)

	stmts, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}

	var st Statement
	switch len(stmts) {
	case 1:
		st.AST = stmts[0]
	case 0:
		// ignore: nil (empty) statement.
	default:
		return nil, pgerror.NewWrongNumberOfPreparedStatements(len(stmts))
	}

	return ps.New(e, name, st, query, placeholderHints)
}

// New creates a new PreparedStatement with the provided name and corresponding
// query statements, using the given PlaceholderTypes hints to assist in
// inferring placeholder types.
//
// ps.session.Ctx() is used as the logging context for the prepare operation.
func (ps PreparedStatements) New(
	e *Executor, name string, stmt Statement, stmtStr string, placeholderHints tree.PlaceholderTypes,
) (*PreparedStatement, error) {
	// Prepare the query. This completes the typing of placeholders.
	pStmt, err := e.Prepare(stmt, stmtStr, ps.session, placeholderHints)
	if err != nil {
		return nil, err
	}

	// For now we are just counting the size of the query string and
	// statement name. When we start storing the prepared query plan
	// during prepare, this should be tallied up to the monitor as well.
	sz := int64(uintptr(len(name)+len(stmtStr)) + unsafe.Sizeof(*pStmt))
	if err := pStmt.memAcc.Grow(ps.session.Ctx(), sz); err != nil {
		return nil, err
	}

	if prevStmt, ok := ps.Get(name); ok {
		prevStmt.close(ps.session.Ctx())
	}

	pStmt.Str = stmtStr
	ps.stmts[name] = pStmt
	return pStmt, nil
}

// Delete is part of the preparedStatementsAccessor interface.
func (ps *PreparedStatements) Delete(ctx context.Context, name string) bool {
	if stmt, ok := ps.Get(name); ok {
		if ps.session.PreparedPortals.portals != nil {
			for portalName := range stmt.portalNames {
				if portal, ok := ps.session.PreparedPortals.Get(name); ok {
					delete(ps.session.PreparedPortals.portals, portalName)
					portal.memAcc.Close(ctx)
				}
			}
		}
		stmt.close(ctx)
		delete(ps.stmts, name)
		return true
	}
	return false
}

// closeAll de-registers all statements and portals from the monitor.
func (ps PreparedStatements) closeAll(ctx context.Context, s *Session) {
	for _, stmt := range ps.stmts {
		stmt.close(ctx)
	}
	for _, portal := range s.PreparedPortals.portals {
		portal.close(ctx)
	}
}

// ClearStatementsAndPortals de-registers all statements and
// portals. Afterwards none can be added any more.
func (s *Session) ClearStatementsAndPortals(ctx context.Context) {
	s.PreparedStatements.closeAll(ctx, s)
	s.PreparedStatements.stmts = nil
	s.PreparedPortals.portals = nil
}

// DeleteAll is part of the preparedStatementsAccessor interface.
func (ps *PreparedStatements) DeleteAll(ctx context.Context) {
	ps.closeAll(ctx, ps.session)
	ps.stmts = make(map[string]*PreparedStatement)
	ps.session.PreparedPortals.portals = make(map[string]*PreparedPortal)
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
// When no longer in use, the PrepatedPortal needs to be close()d.
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

// PreparedPortals is a mapping of PreparedPortal names to their corresponding
// PreparedPortals.
//
// TODO(andrei): The connExecutor doesn't use this. Delete it once the Executor
// is gone.
type PreparedPortals struct {
	session *Session
	portals map[string]*PreparedPortal
}

func makePreparedPortals(s *Session) PreparedPortals {
	return PreparedPortals{
		session: s,
		portals: make(map[string]*PreparedPortal),
	}
}

// Get is part of the preparedStatementsAccessor interface.
func (pp PreparedPortals) Get(name string) (*PreparedPortal, bool) {
	portal, ok := pp.portals[name]
	return portal, ok
}

// Exists returns whether a PreparedPortal with the provided name exists.
func (pp PreparedPortals) Exists(name string) bool {
	_, ok := pp.Get(name)
	return ok
}

// New creates a new PreparedPortal with the provided name and corresponding
// PreparedStatement, binding the statement using the given QueryArguments.
func (pp PreparedPortals) New(
	ctx context.Context, name string, stmt *PreparedStatement, qargs tree.QueryArguments,
) (*PreparedPortal, error) {
	portal := &PreparedPortal{
		Stmt:   stmt,
		Qargs:  qargs,
		memAcc: pp.session.mon.MakeBoundAccount(),
	}
	sz := int64(uintptr(len(name)) + unsafe.Sizeof(*portal))
	if err := portal.memAcc.Grow(ctx, sz); err != nil {
		return nil, err
	}

	stmt.portalNames[name] = struct{}{}

	if prevPortal, ok := pp.Get(name); ok {
		prevPortal.close(ctx)
	}

	pp.portals[name] = portal
	return portal, nil
}

// Delete removes the PreparedPortal with the provided name from the PreparedPortals.
// The method returns whether a portal with that name was found and removed.
func (pp PreparedPortals) Delete(ctx context.Context, name string) bool {
	if portal, ok := pp.Get(name); ok {
		delete(portal.Stmt.portalNames, name)
		portal.close(ctx)
		delete(pp.portals, name)
		return true
	}
	return false
}
