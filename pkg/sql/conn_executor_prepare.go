// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"strconv"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
)

func (ex *connExecutor) execPrepare(
	ctx context.Context, parseCmd PrepareStmt,
) (fsm.Event, fsm.EventPayload) {

	retErr := func(err error) (fsm.Event, fsm.EventPayload) {
		return eventNonRetriableErr{IsCommit: fsm.False}, eventNonRetriableErrPayload{err: err}
	}

	// The anonymous statement can be overwritter.
	if parseCmd.Name != "" {
		if _, ok := ex.prepStmtsNamespace.prepStmts[parseCmd.Name]; ok {
			err := pgerror.NewErrorf(
				pgerror.CodeDuplicatePreparedStatementError,
				"prepared statement %q already exists", parseCmd.Name,
			)
			return retErr(err)
		}
	} else {
		// Deallocate the unnamed statement, if it exists.
		ex.deletePreparedStmt(ctx, "")
	}

	ps, err := ex.addPreparedStmt(
		ctx, parseCmd.Name, Statement{AST: parseCmd.Stmt}, parseCmd.TypeHints,
	)
	if err != nil {
		return retErr(err)
	}

	// Convert the inferred SQL types back to an array of pgwire Oids.
	inTypes := make([]oid.Oid, 0, len(ps.TypeHints))
	if len(ps.TypeHints) > pgwirebase.MaxPreparedStatementArgs {
		return retErr(
			pgwirebase.NewProtocolViolationErrorf(
				"more than %d arguments to prepared statement: %d",
				pgwirebase.MaxPreparedStatementArgs, len(ps.TypeHints)))
	}
	for k, t := range ps.TypeHints {
		i, err := strconv.Atoi(k)
		if err != nil || i < 1 {
			return retErr(pgerror.NewErrorf(
				pgerror.CodeUndefinedParameterError, "invalid placeholder name: $%s", k))
		}
		// Placeholder names are 1-indexed; the arrays in the protocol are
		// 0-indexed.
		i--
		// Grow inTypes to be at least as large as i. Prepopulate all
		// slots with the hints provided, if any.
		for j := len(inTypes); j <= i; j++ {
			inTypes = append(inTypes, 0)
			if j < len(parseCmd.RawTypeHints) {
				inTypes[j] = parseCmd.RawTypeHints[j]
			}
		}
		// OID to Datum is not a 1-1 mapping (for example, int4 and int8
		// both map to TypeInt), so we need to maintain the types sent by
		// the client.
		if inTypes[i] != 0 {
			continue
		}
		inTypes[i] = t.Oid()
	}
	for i, t := range inTypes {
		if t == 0 {
			return retErr(pgerror.NewErrorf(
				pgerror.CodeIndeterminateDatatypeError,
				"could not determine data type of placeholder $%d", i+1))
		}
	}
	// Remember the inferred placeholder types so they can be reported on
	// Describe.
	ps.InTypes = inTypes
	return nil, nil
}

// addPreparedStmt creates a new PreparedStatement with the provided name using
// the given query. The new prepared statement is added to the connExecutor and
// also returned. It is illegal to call this when a statement with that name
// already exists (even for anonymous prepared statements).
//
// placeholderHints are used to assist in inferring placeholder types.
func (ex *connExecutor) addPreparedStmt(
	ctx context.Context, name string, stmt Statement, placeholderHints tree.PlaceholderTypes,
) (*PreparedStatement, error) {
	if _, ok := ex.prepStmtsNamespace.prepStmts[name]; ok {
		panic(fmt.Sprintf("prepared statement already exists: %q", name))
	}

	// Prepare the query. This completes the typing of placeholders.
	prepared, err := ex.prepare(ctx, stmt, placeholderHints)
	if err != nil {
		return nil, err
	}

	if err := prepared.memAcc.Grow(ctx, int64(len(name))); err != nil {
		return nil, err
	}
	ex.prepStmtsNamespace.prepStmts[name] = prepStmtEntry{
		PreparedStatement: prepared,
		portals:           make(map[string]struct{}),
	}
	return prepared, nil
}

// prepare prepares the given statement.
//
// placeholderHints may contain partial type information for placeholders.
// prepare will populate the missing types.
//
// The PreparedStatement is returned (or nil if there are no results). The
// returned PreparedStatement needs to be close()d once its no longer in use.
func (ex *connExecutor) prepare(
	ctx context.Context, stmt Statement, placeholderHints tree.PlaceholderTypes,
) (*PreparedStatement, error) {
	prepared := &PreparedStatement{
		TypeHints: placeholderHints,
		memAcc:    ex.sessionMon.MakeBoundAccount(),
	}
	// NB: if we start caching the plan, we'll want to keep around the memory
	// account used for the plan, rather than clearing it.
	defer prepared.memAcc.Clear(ctx)

	if stmt.AST == nil {
		return prepared, nil
	}
	prepared.Str = stmt.String()

	prepared.Statement = stmt.AST
	prepared.AnonymizedStr = anonymizeStmt(stmt)

	if err := placeholderHints.ProcessPlaceholderAnnotations(stmt.AST); err != nil {
		return nil, err
	}
	// Preparing needs a transaction because it needs to retrieve db/table
	// descriptors for type checking.
	txn := client.NewTxn(ex.server.cfg.DB, ex.server.cfg.NodeID.Get(), client.RootTxn)

	// Create a plan for the statement to figure out the typing, then close the
	// plan.
	if err := func() error {
		p := &ex.planner
		ex.resetPlanner(ctx, p, txn, ex.server.cfg.Clock.PhysicalTime() /* stmtTimestamp */)
		p.semaCtx.Placeholders.SetTypeHints(placeholderHints)
		p.extendedEvalCtx.PrepareOnly = true
		p.extendedEvalCtx.ActiveMemAcc = &prepared.memAcc
		// constantMemAcc accounts for all constant folded values that are computed
		// prior to any rows being computed.
		constantMemAcc := p.extendedEvalCtx.Mon.MakeBoundAccount()
		p.extendedEvalCtx.ActiveMemAcc = &constantMemAcc
		defer constantMemAcc.Close(ctx)

		protoTS, err := isAsOf(stmt.AST, p.EvalContext(), ex.server.cfg.Clock.Now() /* max */)
		if err != nil {
			return err
		}
		if protoTS != nil {
			p.asOfSystemTime = true
			// We can't use cached descriptors anywhere in this query, because
			// we want the descriptors at the timestamp given, not the latest
			// known to the cache.
			p.avoidCachedDescriptors = true
			txn.SetFixedTimestamp(ctx, *protoTS)
		}

		if err := p.prepare(ctx, stmt.AST); err != nil {
			return err
		}
		if p.curPlan.plan == nil {
			// The statement cannot be prepared. Nothing to do.
			return nil
		}
		defer p.curPlan.close(ctx)

		prepared.Columns = p.curPlan.columns()
		for _, c := range prepared.Columns {
			if err := checkResultType(c.Typ); err != nil {
				return err
			}
		}
		prepared.Types = p.semaCtx.Placeholders.Types
		return nil
	}(); err != nil {
		return nil, err
	}

	// Account for the memory used by this prepared statement: for now we are just
	// counting the size of the query string (we'll account for the statement name
	// at a higher layer). When we start storing the prepared query plan during
	// prepare, this should be tallied up to the monitor as well.
	if err := prepared.memAcc.Grow(ctx,
		int64(len(prepared.Str)+int(unsafe.Sizeof(*prepared))),
	); err != nil {
		return nil, err
	}

	return prepared, nil
}

func (ex *connExecutor) execBind(
	ctx context.Context, bindCmd BindStmt,
) (fsm.Event, fsm.EventPayload) {

	retErr := func(err error) (fsm.Event, fsm.EventPayload) {
		return eventNonRetriableErr{IsCommit: fsm.False}, eventNonRetriableErrPayload{err: err}
	}

	portalName := bindCmd.PortalName
	// The unnamed portal can be freely overwritten.
	if portalName != "" {
		if _, ok := ex.prepStmtsNamespace.portals[portalName]; ok {
			return retErr(pgerror.NewErrorf(
				pgerror.CodeDuplicateCursorError, "portal %q already exists", portalName))
		}
	} else {
		// Deallocate the unnamed portal, if it exists.
		ex.deletePortal(ctx, "")
	}

	ps, ok := ex.prepStmtsNamespace.prepStmts[bindCmd.PreparedStatementName]
	if !ok {
		return retErr(pgerror.NewErrorf(
			pgerror.CodeInvalidSQLStatementNameError,
			"unknown prepared statement %q", bindCmd.PreparedStatementName))
	}

	numQArgs := uint16(len(ps.InTypes))
	qArgFormatCodes := bindCmd.ArgFormatCodes

	// If a single code is specified, it is applied to all arguments.
	if len(qArgFormatCodes) != 1 && len(qArgFormatCodes) != int(numQArgs) {
		return retErr(pgwirebase.NewProtocolViolationErrorf(
			"wrong number of format codes specified: %d for %d arguments",
			len(qArgFormatCodes), numQArgs))
	}
	// If a single format code was specified, it applies to all the arguments.
	if len(qArgFormatCodes) == 1 {
		fmtCode := qArgFormatCodes[0]
		qArgFormatCodes = make([]pgwirebase.FormatCode, numQArgs)
		for i := range qArgFormatCodes {
			qArgFormatCodes[i] = fmtCode
		}
	}

	if len(bindCmd.Args) != int(numQArgs) {
		return retErr(
			pgwirebase.NewProtocolViolationErrorf(
				"expected %d arguments, got %d", numQArgs, len(bindCmd.Args)))
	}
	qargs := tree.QueryArguments{}
	for i, arg := range bindCmd.Args {
		k := strconv.Itoa(i + 1)
		t := ps.InTypes[i]
		if arg == nil {
			// nil indicates a NULL argument value.
			qargs[k] = tree.DNull
		} else {
			d, err := pgwirebase.DecodeOidDatum(t, qArgFormatCodes[i], arg)
			if err != nil {
				if _, ok := err.(*pgerror.Error); ok {
					return retErr(err)
				}
				return retErr(pgwirebase.NewProtocolViolationErrorf(
					"error in argument for $%d: %s", i+1, err.Error()))

			}
			qargs[k] = d
		}
	}

	numCols := len(ps.Columns)
	if (len(bindCmd.OutFormats) > 1) && (len(bindCmd.OutFormats) != numCols) {
		return retErr(pgwirebase.NewProtocolViolationErrorf(
			"expected 1 or %d for number of format codes, got %d",
			numCols, len(bindCmd.OutFormats)))
	}

	columnFormatCodes := bindCmd.OutFormats
	if len(bindCmd.OutFormats) == 1 {
		// Apply the format code to every column.
		columnFormatCodes = make([]pgwirebase.FormatCode, numCols)
		for i := 0; i < numCols; i++ {
			columnFormatCodes[i] = bindCmd.OutFormats[0]
		}
	}

	// Create the new PreparedPortal.
	if err := ex.addPortal(
		ctx, portalName, bindCmd.PreparedStatementName, ps.PreparedStatement, qargs, columnFormatCodes,
	); err != nil {
		return retErr(err)
	}

	if log.V(2) {
		log.Infof(ctx, "portal: %q for %q, args %q, formats %q",
			portalName, ps.Statement, qargs, columnFormatCodes)
	}

	return nil, nil
}

// addPortal creates a new PreparedPortal on the connExecutor.
//
// It is illegal to call this when a portal with that name already exists (even
// for anonymous portals).
func (ex *connExecutor) addPortal(
	ctx context.Context,
	portalName string,
	psName string,
	stmt *PreparedStatement,
	qargs tree.QueryArguments,
	outFormats []pgwirebase.FormatCode,
) error {
	if _, ok := ex.prepStmtsNamespace.portals[portalName]; ok {
		panic(fmt.Sprintf("portal already exists: %q", portalName))
	}

	portal, err := ex.newPreparedPortal(ctx, portalName, stmt, qargs, outFormats)
	if err != nil {
		return err
	}

	ex.prepStmtsNamespace.portals[portalName] = portalEntry{
		PreparedPortal: portal,
		psName:         psName,
	}
	ex.prepStmtsNamespace.prepStmts[psName].portals[portalName] = struct{}{}
	return nil
}

func (ex *connExecutor) deletePreparedStmt(ctx context.Context, name string) {
	psEntry, ok := ex.prepStmtsNamespace.prepStmts[name]
	if !ok {
		return
	}
	// If the prepared statement only exists in prepStmtsNamespace, it's up to us
	// to close it.
	baseP, inBase := ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.prepStmts[name]
	if !inBase || (baseP.PreparedStatement != psEntry.PreparedStatement) {
		psEntry.close(ctx)
	}
	for portalName := range psEntry.portals {
		ex.deletePortal(ctx, portalName)
	}
	delete(ex.prepStmtsNamespace.prepStmts, name)
}

func (ex *connExecutor) deletePortal(ctx context.Context, name string) {
	portalEntry, ok := ex.prepStmtsNamespace.portals[name]
	if !ok {
		return
	}
	// If the portal only exists in prepStmtsNamespace, it's up to us to close it.
	baseP, inBase := ex.extraTxnState.prepStmtsNamespaceAtTxnRewindPos.portals[name]
	if !inBase || (baseP.PreparedPortal != portalEntry.PreparedPortal) {
		portalEntry.close(ctx)
	}
	delete(ex.prepStmtsNamespace.portals, name)
	delete(ex.prepStmtsNamespace.prepStmts[portalEntry.psName].portals, name)
}

func (ex *connExecutor) execDelPrepStmt(
	ctx context.Context, delCmd DeletePreparedStmt,
) (fsm.Event, fsm.EventPayload) {
	switch delCmd.Type {
	case pgwirebase.PrepareStatement:
		_, ok := ex.prepStmtsNamespace.prepStmts[delCmd.Name]
		if !ok {
			// The spec says "It is not an error to issue Close against a nonexistent
			// statement or portal name". See
			// https://www.postgresql.org/docs/current/static/protocol-flow.html.
			break
		}

		ex.deletePreparedStmt(ctx, delCmd.Name)
	case pgwirebase.PreparePortal:
		_, ok := ex.prepStmtsNamespace.portals[delCmd.Name]
		if !ok {
			break
		}
		ex.deletePortal(ctx, delCmd.Name)
	default:
		panic(fmt.Sprintf("unknown del type: %s", delCmd.Type))
	}
	return nil, nil
}

func (ex *connExecutor) execDescribe(
	ctx context.Context, descCmd DescribeStmt, res DescribeResult,
) (fsm.Event, fsm.EventPayload) {

	retErr := func(err error) (fsm.Event, fsm.EventPayload) {
		return eventNonRetriableErr{IsCommit: fsm.False}, eventNonRetriableErrPayload{err: err}
	}

	switch descCmd.Type {
	case pgwirebase.PrepareStatement:
		ps, ok := ex.prepStmtsNamespace.prepStmts[descCmd.Name]
		if !ok {
			return retErr(pgerror.NewErrorf(
				pgerror.CodeInvalidSQLStatementNameError,
				"unknown prepared statement %q", descCmd.Name))
		}

		res.SetInTypes(ps.InTypes)

		if stmtHasNoData(ps.Statement) {
			res.SetNoDataRowDescription()
		} else {
			res.SetPrepStmtOutput(ctx, ps.Columns)
		}
	case pgwirebase.PreparePortal:
		portal, ok := ex.prepStmtsNamespace.portals[descCmd.Name]
		if !ok {
			return retErr(pgerror.NewErrorf(
				pgerror.CodeInvalidCursorNameError, "unknown portal %q", descCmd.Name))
		}

		if stmtHasNoData(portal.Stmt.Statement) {
			res.SetNoDataRowDescription()
		} else {
			res.SetPortalOutput(ctx, portal.Stmt.Columns, portal.OutFormats)
		}
	default:
		return retErr(errors.Errorf("unknown describe type: %s", descCmd.Type))
	}
	return nil, nil
}
