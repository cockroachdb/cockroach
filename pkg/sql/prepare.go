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
	"bytes"
	"strconv"
	"unsafe"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
)

// PreparedStatement is a SQL statement that has been parsed and the types
// of arguments and results have been determined.
type PreparedStatement struct {
	// Str is the statement string prior to parsing, used to generate
	// error messages. This may be used in
	// the future to present a contextual error message based on location
	// information.
	Str string
	// Statement is the parsed, prepared SQL statement. It may be nil if the
	// prepared statement is empty.
	Statement   parser.Statement
	SQLTypes    parser.PlaceholderTypes
	Columns     sqlbase.ResultColumns
	portalNames map[string]struct{}

	ProtocolMeta interface{} // a field for protocol implementations to hang metadata off of.

	memAcc WrappableMemoryAccount
	// constantAcc handles the allocation of various constant-folded values which
	// are generated while planning the statement.
	constantAcc mon.BoundAccount
}

func (p *PreparedStatement) close(ctx context.Context, s *Session) {
	p.memAcc.Wsession(s).Close(ctx)
	p.constantAcc.Close(ctx)
}

// Statement contains a statement with optional expected result columns and metadata.
type Statement struct {
	AST           parser.Statement
	ExpectedTypes sqlbase.ResultColumns
	queryID       uint128.Uint128
	queryMeta     *queryMeta
}

func (s Statement) String() string {
	return s.AST.String()
}

// StatementList is a list of statements.
type StatementList []Statement

// NewStatementList creates a StatementList from a parser.StatementList.
func NewStatementList(stmts parser.StatementList) StatementList {
	sl := make(StatementList, len(stmts))
	for i, s := range stmts {
		sl[i] = Statement{AST: s}
	}
	return sl
}

func (l StatementList) String() string { return parser.AsString(l) }

// Format implements the NodeFormatter interface.
func (l StatementList) Format(buf *bytes.Buffer, f parser.FmtFlags) {
	for i, s := range l {
		if i > 0 {
			buf.WriteString("; ")
		}
		parser.FormatNode(buf, f, s.AST)
	}
}

// PreparedStatements is a mapping of PreparedStatement names to their
// corresponding PreparedStatements.
type PreparedStatements struct {
	session *Session
	stmts   map[string]*PreparedStatement
}

func makePreparedStatements(s *Session) PreparedStatements {
	return PreparedStatements{
		session: s,
		stmts:   make(map[string]*PreparedStatement),
	}
}

// Get returns the PreparedStatement with the provided name.
func (ps PreparedStatements) Get(name string) (*PreparedStatement, bool) {
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
	e *Executor, name, query string, placeholderHints parser.PlaceholderTypes,
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
		return nil, errWrongNumberOfPreparedStatements(len(stmts))
	}

	return ps.New(e, name, st, query, placeholderHints)
}

// New creates a new PreparedStatement with the provided name and corresponding
// query statements, using the given PlaceholderTypes hints to assist in
// inferring placeholder types.
//
// ps.session.Ctx() is used as the logging context for the prepare operation.
func (ps PreparedStatements) New(
	e *Executor,
	name string,
	stmt Statement,
	stmtStr string,
	placeholderHints parser.PlaceholderTypes,
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
	if err := pStmt.memAcc.Wsession(ps.session).OpenAndInit(ps.session.Ctx(), sz); err != nil {
		return nil, err
	}

	if prevStmt, ok := ps.Get(name); ok {
		prevStmt.close(ps.session.Ctx(), ps.session)
	}

	pStmt.Str = stmtStr
	ps.stmts[name] = pStmt
	return pStmt, nil
}

// Delete removes the PreparedStatement with the provided name from the PreparedStatements.
// The method returns whether a statement with that name was found and removed.
func (ps PreparedStatements) Delete(ctx context.Context, name string) bool {
	if stmt, ok := ps.Get(name); ok {
		if ps.session.PreparedPortals.portals != nil {
			for portalName := range stmt.portalNames {
				if portal, ok := ps.session.PreparedPortals.Get(name); ok {
					delete(ps.session.PreparedPortals.portals, portalName)
					portal.memAcc.Wsession(ps.session).Close(ctx)
				}
			}
		}
		stmt.close(ctx, ps.session)
		delete(ps.stmts, name)
		return true
	}
	return false
}

// closeAll de-registers all statements and portals from the monitor.
func (ps PreparedStatements) closeAll(ctx context.Context, s *Session) {
	for _, stmt := range ps.stmts {
		stmt.close(ctx, s)
	}
	for _, portal := range s.PreparedPortals.portals {
		portal.memAcc.Wsession(s).Close(ctx)
	}
}

// ClearStatementsAndPortals de-registers all statements and
// portals. Afterwards none can be added any more.
func (s *Session) ClearStatementsAndPortals(ctx context.Context) {
	s.PreparedStatements.closeAll(ctx, s)
	s.PreparedStatements.stmts = nil
	s.PreparedPortals.portals = nil
}

// DeleteAll removes all PreparedStatements from the PreparedStatements. This will in turn
// remove all PreparedPortals from the session's PreparedPortals.
// This is used by the "delete" message in the pgwire protocol; after DeleteAll
// statements and portals can be added again.
func (ps *PreparedStatements) DeleteAll(ctx context.Context) {
	ps.closeAll(ctx, ps.session)
	ps.stmts = make(map[string]*PreparedStatement)
	ps.session.PreparedPortals.portals = make(map[string]*PreparedPortal)
}

// PreparedPortal is a PreparedStatement that has been bound with query arguments.
type PreparedPortal struct {
	Stmt  *PreparedStatement
	Qargs parser.QueryArguments

	ProtocolMeta interface{} // a field for protocol implementations to hang metadata off of.

	memAcc WrappableMemoryAccount
}

// PreparedPortals is a mapping of PreparedPortal names to their corresponding
// PreparedPortals.
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

// Get returns the PreparedPortal with the provided name.
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
	ctx context.Context, name string, stmt *PreparedStatement, qargs parser.QueryArguments,
) (*PreparedPortal, error) {
	portal := &PreparedPortal{
		Stmt:  stmt,
		Qargs: qargs,
	}
	sz := int64(uintptr(len(name)) + unsafe.Sizeof(*portal))
	if err := portal.memAcc.Wsession(pp.session).OpenAndInit(ctx, sz); err != nil {
		return nil, err
	}

	stmt.portalNames[name] = struct{}{}

	if prevPortal, ok := pp.Get(name); ok {
		prevPortal.memAcc.Wsession(pp.session).Close(ctx)
	}

	pp.portals[name] = portal
	return portal, nil
}

// Delete removes the PreparedPortal with the provided name from the PreparedPortals.
// The method returns whether a portal with that name was found and removed.
func (pp PreparedPortals) Delete(ctx context.Context, name string) bool {
	if portal, ok := pp.Get(name); ok {
		delete(portal.Stmt.portalNames, name)
		portal.memAcc.Wsession(pp.session).Close(ctx)
		delete(pp.portals, name)
		return true
	}
	return false
}

// PrepareStmt implements the PREPARE statement.
// See https://www.postgresql.org/docs/current/static/sql-prepare.html for details.
func (e *Executor) PrepareStmt(session *Session, s *parser.Prepare) error {
	name := s.Name.String()
	if session.PreparedStatements.Exists(name) {
		return pgerror.NewErrorf(pgerror.CodeDuplicateDatabaseError,
			"prepared statement %q already exists", name)
	}
	typeHints := make(parser.PlaceholderTypes, len(s.Types))
	for i, t := range s.Types {
		typeHints[strconv.Itoa(i+1)] = parser.CastTargetToDatumType(t)
	}
	_, err := session.PreparedStatements.New(
		e, name, Statement{AST: s.Statement}, s.Statement.String(), typeHints,
	)
	return err
}

// getPreparedStatementForExecute implements the EXECUTE foo(args) SQL
// statement, returning the referenced prepared statement and correctly updated
// placeholder info.
// See https://www.postgresql.org/docs/current/static/sql-execute.html for details.
func getPreparedStatementForExecute(
	session *Session, s *parser.Execute,
) (ps *PreparedStatement, pInfo *parser.PlaceholderInfo, err error) {
	name := s.Name.String()
	prepared, ok := session.PreparedStatements.Get(name)
	if !ok {
		return ps, pInfo, pgerror.NewErrorf(pgerror.CodeInvalidSQLStatementNameError,
			"prepared statement %q does not exist", name)
	}

	if len(prepared.SQLTypes) != len(s.Params) {
		return ps, pInfo, pgerror.NewErrorf(pgerror.CodeSyntaxError,
			"wrong number of parameters for prepared statement %q: expected %d, got %d",
			name, len(prepared.SQLTypes), len(s.Params))
	}

	qArgs := make(parser.QueryArguments, len(s.Params))
	var p parser.Parser
	for i, e := range s.Params {
		idx := strconv.Itoa(i + 1)
		typedExpr, err := sqlbase.SanitizeVarFreeExpr(e, prepared.SQLTypes[idx], "EXECUTE parameter", session.SearchPath)
		if err != nil {
			return ps, pInfo, pgerror.NewError(pgerror.CodeFeatureNotSupportedError, err.Error())
		}
		if err := p.AssertNoAggregationOrWindowing(typedExpr, "EXECUTE parameters", session.SearchPath); err != nil {
			return ps, pInfo, err
		}
		qArgs[idx] = typedExpr
	}
	return prepared, &parser.PlaceholderInfo{Values: qArgs, Types: prepared.SQLTypes}, nil
}

// Deallocate implements the DEALLOCATE statement.
// See https://www.postgresql.org/docs/current/static/sql-deallocate.html for details.
func (p *planner) Deallocate(ctx context.Context, s *parser.Deallocate) (planNode, error) {
	if s.Name == "" {
		p.session.PreparedStatements.DeleteAll(ctx)
	} else {
		if found := p.session.PreparedStatements.Delete(ctx, string(s.Name)); !found {
			return nil, pgerror.NewErrorf(pgerror.CodeInvalidSQLStatementNameError,
				"prepared statement %q does not exist", s.Name)
		}
	}
	return &emptyNode{}, nil
}

// Execute creates a plan for an execute statement by substituting the plan for
// the prepared statement. This is not called in normal circumstances by
// the executor - it merely exists to enable explains and traces for execute
// statements.
func (p *planner) Execute(ctx context.Context, n *parser.Execute) (planNode, error) {
	ps, newPInfo, err := getPreparedStatementForExecute(p.session, n)
	if err != nil {
		return nil, err
	}
	p.semaCtx.Placeholders.Assign(newPInfo)

	return p.newPlan(ctx, ps.Statement, nil)
}
