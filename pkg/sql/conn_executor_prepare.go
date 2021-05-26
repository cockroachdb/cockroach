// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

func (ex *connExecutor) execPrepare(
	ctx context.Context, parseCmd PrepareStmt,
) (fsm.Event, fsm.EventPayload) {

	retErr := func(err error) (fsm.Event, fsm.EventPayload) {
		return ex.makeErrEvent(err, parseCmd.AST)
	}

	// The anonymous statement can be overwritten.
	if parseCmd.Name != "" {
		if _, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[parseCmd.Name]; ok {
			err := pgerror.Newf(
				pgcode.DuplicatePreparedStatement,
				"prepared statement %q already exists", parseCmd.Name,
			)
			return retErr(err)
		}
	} else {
		// Deallocate the unnamed statement, if it exists.
		ex.deletePreparedStmt(ctx, "")
	}

	stmt := makeStatement(parseCmd.Statement, ex.generateID())
	ps, err := ex.addPreparedStmt(
		ctx,
		parseCmd.Name,
		stmt,
		parseCmd.TypeHints,
		parseCmd.RawTypeHints,
		PreparedStatementOriginWire,
	)
	if err != nil {
		return retErr(err)
	}

	// Convert the inferred SQL types back to an array of pgwire Oids.
	if len(ps.TypeHints) > pgwirebase.MaxPreparedStatementArgs {
		return retErr(
			pgwirebase.NewProtocolViolationErrorf(
				"more than %d arguments to prepared statement: %d",
				pgwirebase.MaxPreparedStatementArgs, len(ps.TypeHints)))
	}
	inferredTypes := make([]oid.Oid, len(ps.Types))
	copy(inferredTypes, parseCmd.RawTypeHints)

	for i := range ps.Types {
		// OID to Datum is not a 1-1 mapping (for example, int4 and int8
		// both map to TypeInt), so we need to maintain the types sent by
		// the client.
		if inferredTypes[i] == 0 {
			t, _ := ps.ValueType(tree.PlaceholderIdx(i))
			inferredTypes[i] = t.Oid()
		}
	}
	// Remember the inferred placeholder types so they can be reported on
	// Describe.
	ps.InferredTypes = inferredTypes
	return nil, nil
}

// addPreparedStmt creates a new PreparedStatement with the provided name using
// the given query. The new prepared statement is added to the connExecutor and
// also returned. It is illegal to call this when a statement with that name
// already exists (even for anonymous prepared statements).
//
// placeholderHints are used to assist in inferring placeholder types. The
// rawTypeHints are optional and represent OIDs indicated for the placeholders
// coming from the client via the wire protocol.
func (ex *connExecutor) addPreparedStmt(
	ctx context.Context,
	name string,
	stmt Statement,
	placeholderHints tree.PlaceholderTypes,
	rawTypeHints []oid.Oid,
	origin PreparedStatementOrigin,
) (*PreparedStatement, error) {
	if _, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[name]; ok {
		panic(errors.AssertionFailedf("prepared statement already exists: %q", name))
	}

	// Prepare the query. This completes the typing of placeholders.
	prepared, err := ex.prepare(ctx, stmt, placeholderHints, rawTypeHints, origin)
	if err != nil {
		return nil, err
	}

	if err := prepared.memAcc.Grow(ctx, int64(len(name))); err != nil {
		return nil, err
	}
	ex.extraTxnState.prepStmtsNamespace.prepStmts[name] = prepared
	return prepared, nil
}

// prepare prepares the given statement.
//
// placeholderHints may contain partial type information for placeholders.
// prepare will populate the missing types. It can be nil.
//
// The PreparedStatement is returned (or nil if there are no results). The
// returned PreparedStatement needs to be close()d once its no longer in use.
func (ex *connExecutor) prepare(
	ctx context.Context,
	stmt Statement,
	placeholderHints tree.PlaceholderTypes,
	rawTypeHints []oid.Oid,
	origin PreparedStatementOrigin,
) (*PreparedStatement, error) {

	prepared := &PreparedStatement{
		memAcc:   ex.sessionMon.MakeBoundAccount(),
		refCount: 1,

		createdAt: timeutil.Now(),
		origin:    origin,
	}
	// NB: if we start caching the plan, we'll want to keep around the memory
	// account used for the plan, rather than clearing it.
	defer prepared.memAcc.Clear(ctx)

	if stmt.AST == nil {
		return prepared, nil
	}

	// Preparing needs a transaction because it needs to retrieve db/table
	// descriptors for type checking. If we already have an open transaction for
	// this planner, use it. Using the user's transaction here is critical for
	// proper deadlock detection. At the time of writing, it is the case that any
	// data read on behalf of this transaction is not cached for use in other
	// transactions. It's critical that this fact remain true but nothing really
	// enforces it. If we create a new transaction (newTxn is true), we'll need to
	// finish it before we return.

	var flags planFlags
	prepare := func(ctx context.Context, txn *kv.Txn) (err error) {
		ex.statsCollector.Reset(ex.statsWriter, ex.phaseTimes)
		p := &ex.planner
		if origin != PreparedStatementOriginSQL {
			// If the PREPARE command was issued as a SQL statement, then we
			// have already reset the planner at the very beginning of the
			// execution (in execStmtInOpenState). We might have also
			// instrumented the planner to collect execution statistics, and
			// resetting the planner here would break the assumptions of the
			// instrumentation.
			ex.resetPlanner(ctx, p, txn, ex.server.cfg.Clock.PhysicalTime() /* stmtTS */)
		}

		if placeholderHints == nil {
			placeholderHints = make(tree.PlaceholderTypes, stmt.NumPlaceholders)
		} else if rawTypeHints != nil {
			// If we were provided any type hints, attempt to resolve any user defined
			// type OIDs into types.T's.
			for i := range placeholderHints {
				if placeholderHints[i] == nil {
					if i >= len(rawTypeHints) {
						return pgwirebase.NewProtocolViolationErrorf(
							"expected %d arguments, got %d",
							len(placeholderHints),
							len(rawTypeHints),
						)
					}
					if types.IsOIDUserDefinedType(rawTypeHints[i]) {
						var err error
						placeholderHints[i], err = ex.planner.ResolveTypeByOID(ctx, rawTypeHints[i])
						if err != nil {
							return err
						}
					}
				}
			}
		}

		prepared.PrepareMetadata = querycache.PrepareMetadata{
			PlaceholderTypesInfo: tree.PlaceholderTypesInfo{
				TypeHints: placeholderHints,
			},
		}
		prepared.Statement = stmt.Statement
		prepared.AnonymizedStr = stmt.AnonymizedStr

		// Point to the prepared state, which can be further populated during query
		// preparation.
		stmt.Prepared = prepared

		if err := tree.ProcessPlaceholderAnnotations(&ex.planner.semaCtx, stmt.AST, placeholderHints); err != nil {
			return err
		}

		p.stmt = stmt
		p.semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
		flags, err = ex.populatePrepared(ctx, txn, placeholderHints, p)
		return err
	}

	if txn := ex.state.mu.txn; txn != nil && txn.IsOpen() {
		// Use the existing transaction.
		if err := prepare(ctx, txn); err != nil {
			return nil, err
		}
	} else {
		// Use a new transaction. This will handle retriable errors here rather
		// than bubbling them up to the connExecutor state machine.
		if err := ex.server.cfg.DB.Txn(ctx, prepare); err != nil {
			return nil, err
		}
		// Prepare with an implicit transaction will end up creating
		// a new transaction. Once this transaction is complete,
		// we can safely release the leases, otherwise we will
		// incorrectly hold leases for later operations.
		ex.extraTxnState.descCollection.ReleaseAll(ctx)
	}

	// Account for the memory used by this prepared statement.
	if err := prepared.memAcc.Grow(ctx, prepared.MemoryEstimate()); err != nil {
		return nil, err
	}
	ex.updateOptCounters(flags)
	return prepared, nil
}

// populatePrepared analyzes and type-checks the query and populates
// stmt.Prepared.
func (ex *connExecutor) populatePrepared(
	ctx context.Context, txn *kv.Txn, placeholderHints tree.PlaceholderTypes, p *planner,
) (planFlags, error) {
	if before := ex.server.cfg.TestingKnobs.BeforePrepare; before != nil {
		if err := before(ctx, ex.planner.stmt.String(), txn); err != nil {
			return 0, err
		}
	}
	stmt := &p.stmt
	if err := p.semaCtx.Placeholders.Init(stmt.NumPlaceholders, placeholderHints); err != nil {
		return 0, err
	}
	p.extendedEvalCtx.PrepareOnly = true

	protoTS, err := p.isAsOf(ctx, stmt.AST)
	if err != nil {
		return 0, err
	}
	if protoTS != nil {
		p.semaCtx.AsOfTimestamp = protoTS
		txn.SetFixedTimestamp(ctx, *protoTS)
	}

	// PREPARE has a limited subset of statements it can be run with. Postgres
	// only allows SELECT, INSERT, UPDATE, DELETE and VALUES statements to be
	// prepared.
	// See: https://www.postgresql.org/docs/current/static/sql-prepare.html
	// However, we allow a large number of additional statements.
	// As of right now, the optimizer only works on SELECT statements and will
	// fallback for all others, so this should be safe for the foreseeable
	// future.
	flags, err := p.prepareUsingOptimizer(ctx)
	if err != nil {
		log.VEventf(ctx, 1, "optimizer prepare failed: %v", err)
		return 0, err
	}
	log.VEvent(ctx, 2, "optimizer prepare succeeded")
	// stmt.Prepared fields have been populated.
	return flags, nil
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
		if _, ok := ex.extraTxnState.prepStmtsNamespace.portals[portalName]; ok {
			return retErr(pgerror.Newf(
				pgcode.DuplicateCursor, "portal %q already exists", portalName))
		}
	} else {
		// Deallocate the unnamed portal, if it exists.
		ex.deletePortal(ctx, "")
	}

	ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[bindCmd.PreparedStatementName]
	if !ok {
		return retErr(pgerror.Newf(
			pgcode.InvalidSQLStatementName,
			"unknown prepared statement %q", bindCmd.PreparedStatementName))
	}

	numQArgs := uint16(len(ps.InferredTypes))

	// Decode the arguments, except for internal queries for which we just verify
	// that the arguments match what's expected.
	qargs := make(tree.QueryArguments, numQArgs)
	if bindCmd.internalArgs != nil {
		if len(bindCmd.internalArgs) != int(numQArgs) {
			return retErr(
				pgwirebase.NewProtocolViolationErrorf(
					"expected %d arguments, got %d", numQArgs, len(bindCmd.internalArgs)))
		}
		for i, datum := range bindCmd.internalArgs {
			t := ps.InferredTypes[i]
			if oid := datum.ResolvedType().Oid(); datum != tree.DNull && oid != t {
				return retErr(
					pgwirebase.NewProtocolViolationErrorf(
						"for argument %d expected OID %d, got %d", i, t, oid))
			}
			qargs[i] = datum
		}
	} else {
		qArgFormatCodes := bindCmd.ArgFormatCodes

		// If there is only one format code, then that format code is used to decode all the
		// arguments. But if the number of format codes provided does not match the number of
		// arguments AND it's not a single format code then we cannot infer what format to use to
		// decode all of the arguments.
		if len(qArgFormatCodes) != 1 && len(qArgFormatCodes) != int(numQArgs) {
			return retErr(pgwirebase.NewProtocolViolationErrorf(
				"wrong number of format codes specified: %d for %d arguments",
				len(qArgFormatCodes), numQArgs))
		}

		// If a single format code is provided and there is more than one argument to be decoded,
		// then expand qArgFormatCodes to the number of arguments provided.
		// If the number of format codes matches the number of arguments then nothing needs to be
		// done.
		if len(qArgFormatCodes) == 1 && numQArgs > 1 {
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

		for i, arg := range bindCmd.Args {
			k := tree.PlaceholderIdx(i)
			t := ps.InferredTypes[i]
			if arg == nil {
				// nil indicates a NULL argument value.
				qargs[k] = tree.DNull
			} else {
				typ, ok := types.OidToType[t]
				if !ok {
					var err error
					typ, err = ex.planner.ResolveTypeByOID(ctx, t)
					if err != nil {
						return nil, err
					}
				}
				d, err := pgwirebase.DecodeDatum(
					ex.planner.EvalContext(),
					typ,
					qArgFormatCodes[i],
					arg,
				)
				if err != nil {
					return retErr(pgerror.Wrapf(err, pgcode.ProtocolViolation,
						"error in argument for %s", k))
				}
				qargs[k] = d
			}
		}
	}

	numCols := len(ps.Columns)
	if (len(bindCmd.OutFormats) > 1) && (len(bindCmd.OutFormats) != numCols) {
		return retErr(pgwirebase.NewProtocolViolationErrorf(
			"expected 1 or %d for number of format codes, got %d",
			numCols, len(bindCmd.OutFormats)))
	}

	columnFormatCodes := bindCmd.OutFormats
	if len(bindCmd.OutFormats) == 1 && numCols > 1 {
		// Apply the format code to every column.
		columnFormatCodes = make([]pgwirebase.FormatCode, numCols)
		for i := 0; i < numCols; i++ {
			columnFormatCodes[i] = bindCmd.OutFormats[0]
		}
	}

	// This is a huge kludge to deal with the fact that we're resolving types
	// using a planner with a committed transaction. This ends up being almost
	// okay because the execution is going to re-acquire leases on these types.
	// Regardless, holding this lease is worse than not holding it. Users might
	// expect to get type mismatch errors if a rename of the type occurred.
	if ex.getTransactionState() == NoTxnStateStr {
		ex.planner.Descriptors().ReleaseAll(ctx)
	}

	// Create the new PreparedPortal.
	if err := ex.addPortal(ctx, portalName, ps, qargs, columnFormatCodes); err != nil {
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
	stmt *PreparedStatement,
	qargs tree.QueryArguments,
	outFormats []pgwirebase.FormatCode,
) error {
	if _, ok := ex.extraTxnState.prepStmtsNamespace.portals[portalName]; ok {
		panic(errors.AssertionFailedf("portal already exists: %q", portalName))
	}

	portal, err := ex.makePreparedPortal(ctx, portalName, stmt, qargs, outFormats)
	if err != nil {
		return err
	}

	ex.extraTxnState.prepStmtsNamespace.portals[portalName] = portal
	return nil
}

// exhaustPortal marks a portal with the provided name as "exhausted" and
// panics if there is no portal with such name.
func (ex *connExecutor) exhaustPortal(portalName string) {
	portal, ok := ex.extraTxnState.prepStmtsNamespace.portals[portalName]
	if !ok {
		panic(errors.AssertionFailedf("portal %s doesn't exist", portalName))
	}
	portal.exhausted = true
	ex.extraTxnState.prepStmtsNamespace.portals[portalName] = portal
}

func (ex *connExecutor) deletePreparedStmt(ctx context.Context, name string) {
	ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[name]
	if !ok {
		return
	}
	ps.decRef(ctx)
	delete(ex.extraTxnState.prepStmtsNamespace.prepStmts, name)
}

func (ex *connExecutor) deletePortal(ctx context.Context, name string) {
	portal, ok := ex.extraTxnState.prepStmtsNamespace.portals[name]
	if !ok {
		return
	}
	portal.decRef(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc, name)
	delete(ex.extraTxnState.prepStmtsNamespace.portals, name)
}

func (ex *connExecutor) execDelPrepStmt(
	ctx context.Context, delCmd DeletePreparedStmt,
) (fsm.Event, fsm.EventPayload) {
	switch delCmd.Type {
	case pgwirebase.PrepareStatement:
		_, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[delCmd.Name]
		if !ok {
			// The spec says "It is not an error to issue Close against a nonexistent
			// statement or portal name". See
			// https://www.postgresql.org/docs/current/static/protocol-flow.html.
			break
		}

		ex.deletePreparedStmt(ctx, delCmd.Name)
	case pgwirebase.PreparePortal:
		_, ok := ex.extraTxnState.prepStmtsNamespace.portals[delCmd.Name]
		if !ok {
			break
		}
		ex.deletePortal(ctx, delCmd.Name)
	default:
		panic(errors.AssertionFailedf("unknown del type: %s", delCmd.Type))
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
		ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[descCmd.Name]
		if !ok {
			return retErr(pgerror.Newf(
				pgcode.InvalidSQLStatementName,
				"unknown prepared statement %q", descCmd.Name))
		}

		res.SetInferredTypes(ps.InferredTypes)

		if stmtHasNoData(ps.AST) {
			res.SetNoDataRowDescription()
		} else {
			res.SetPrepStmtOutput(ctx, ps.Columns)
		}
	case pgwirebase.PreparePortal:
		portal, ok := ex.extraTxnState.prepStmtsNamespace.portals[descCmd.Name]
		if !ok {
			return retErr(pgerror.Newf(
				pgcode.InvalidCursorName, "unknown portal %q", descCmd.Name))
		}

		if stmtHasNoData(portal.Stmt.AST) {
			res.SetNoDataRowDescription()
		} else {
			res.SetPortalOutput(ctx, portal.Stmt.Columns, portal.OutFormats)
		}
	default:
		return retErr(errors.AssertionFailedf(
			"unknown describe type: %s", errors.Safe(descCmd.Type)))
	}
	return nil, nil
}
