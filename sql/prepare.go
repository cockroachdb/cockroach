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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package sql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/sql/parser"
)

// PreparedStatement is a SQL statement that has been parsed and the types
// of arguments and results have been determined.
type PreparedStatement struct {
	Query       string
	SQLTypes    parser.PlaceholderTypes
	Columns     []ResultColumn
	portalNames map[string]struct{}

	ProtocolMeta interface{} // a field for protocol implementations to hang metadata off of.
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

// New creates a new PreparedStatement with the provided name and corresponding
// query string, using the given PlaceholderTypes hints to assist in inferring
// placeholder types.
func (ps PreparedStatements) New(
	ctx context.Context,
	e *Executor,
	name, query string,
	placeholderHints parser.PlaceholderTypes,
) (*PreparedStatement, error) {
	// Prepare the query. This completes the typing of placeholders.
	cols, err := e.Prepare(query, ps.session, placeholderHints)
	if err != nil {
		return nil, err
	}
	stmt := &PreparedStatement{
		Query:       query,
		SQLTypes:    placeholderHints,
		Columns:     cols,
		portalNames: make(map[string]struct{}),
	}
	ps.stmts[name] = stmt
	return stmt, nil
}

// Delete removes the PreparedStatement with the provided name from the PreparedStatements.
// The method returns whether a statement with that name was found and removed.
func (ps PreparedStatements) Delete(name string) bool {
	if stmt, ok := ps.Get(name); ok {
		for portalName := range stmt.portalNames {
			delete(ps.session.PreparedPortals.portals, portalName)
		}
		delete(ps.stmts, name)
		return true
	}
	return false
}

// DeleteAll removes all PreparedStatements from the PreparedStatements. This will in turn
// remove all PreparedPortals from the session's PreparedPortals.
func (ps PreparedStatements) DeleteAll() {
	ps.stmts = make(map[string]*PreparedStatement)
	ps.session.PreparedPortals.portals = make(map[string]*PreparedPortal)
}

// PreparedPortal is a PreparedStatement that has been bound with query arguments.
type PreparedPortal struct {
	Stmt  *PreparedStatement
	Qargs parser.QueryArguments

	ProtocolMeta interface{} // a field for protocol implementations to hang metadata off of.
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
func (pp PreparedPortals) New(name string, stmt *PreparedStatement, qargs parser.QueryArguments,
) *PreparedPortal {
	stmt.portalNames[name] = struct{}{}
	portal := &PreparedPortal{
		Stmt:  stmt,
		Qargs: qargs,
	}
	pp.portals[name] = portal
	return portal
}

// Delete removes the PreparedPortal with the provided name from the PreparedPortals.
// The method returns whether a portal with that name was found and removed.
func (pp PreparedPortals) Delete(name string) bool {
	if portal, ok := pp.Get(name); ok {
		delete(portal.Stmt.portalNames, name)
		delete(pp.portals, name)
		return true
	}
	return false
}
