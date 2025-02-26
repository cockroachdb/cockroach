// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
)

func (ex *connExecutor) execPrepare(
	ctx context.Context, parseCmd PrepareStmt,
) (fsm.Event, fsm.EventPayload) {
	retErr := func(err error) (fsm.Event, fsm.EventPayload) {
		return ex.makeErrEvent(err, parseCmd.AST)
	}

	// Preparing needs a transaction because it needs to retrieve db/table
	// descriptors for type checking. This implicit txn will be open until
	// the Sync message is handled.
	if _, isNoTxn := ex.machine.CurState().(stateNoTxn); isNoTxn {
		// The one exception is that we must not open a new transaction when
		// preparing SHOW COMMIT TIMESTAMP. If we did, it would destroy the
		// information about the previous transaction. We expect to execute
		// this command in NoTxn.
		if _, ok := parseCmd.AST.(*tree.ShowCommitTimestamp); !ok {
			return ex.beginImplicitTxn(ctx, parseCmd.AST, ex.QualityOfService())
		}
	} else if _, isAbortedTxn := ex.machine.CurState().(stateAborted); isAbortedTxn {
		if !ex.isAllowedInAbortedTxn(parseCmd.AST) {
			return retErr(sqlerrors.NewTransactionAbortedError("" /* customMsg */))
		}
	}

	// Check if we need to auto-commit the transaction due to DDL.
	if ev, payload := ex.maybeAutoCommitBeforeDDL(ctx, parseCmd.AST); ev != nil {
		return ev, payload
	}

	ctx, sp := tracing.ChildSpan(ctx, "prepare stmt")
	defer sp.Finish()

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

	stmt := makeStatement(parseCmd.Statement, ex.server.cfg.GenerateID(),
		tree.FmtFlags(queryFormattingForFingerprintsMask.Get(ex.server.cfg.SV())))
	_, err := ex.addPreparedStmt(
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
		return nil, pgerror.Newf(
			pgcode.DuplicatePreparedStatement,
			"prepared statement %q already exists", name,
		)
	}

	// Prepare the query. This completes the typing of placeholders.
	prepared, err := ex.prepare(ctx, stmt, placeholderHints, rawTypeHints, origin)
	if err != nil {
		return nil, err
	}

	if len(prepared.TypeHints) > pgwirebase.MaxPreparedStatementArgs {
		prepared.memAcc.Close(ctx)
		return nil, pgwirebase.NewProtocolViolationErrorf(
			"more than %d arguments to prepared statement: %d",
			pgwirebase.MaxPreparedStatementArgs, len(prepared.TypeHints))
	}

	if err := prepared.memAcc.Grow(ctx, int64(len(name))); err != nil {
		prepared.memAcc.Close(ctx)
		return nil, err
	}
	ex.extraTxnState.prepStmtsNamespace.prepStmts[name] = prepared
	ex.extraTxnState.prepStmtsNamespace.addLRUEntry(name, prepared.memAcc.Allocated())

	// Check if we're over prepared_statements_cache_size.
	cacheSize := ex.sessionData().PreparedStatementsCacheSize
	if cacheSize != 0 {
		lru := ex.extraTxnState.prepStmtsNamespace.prepStmtsLRU
		// While we're over the cache size, deallocate the LRU prepared statement.
		for tail := lru[prepStmtsLRUTail]; tail.prev != prepStmtsLRUHead && tail.prev != name; tail = lru[prepStmtsLRUTail] {
			if ex.extraTxnState.prepStmtsNamespace.prepStmtsLRUAlloc <= cacheSize {
				break
			}
			log.VEventf(
				ctx, 1,
				"prepared statements are using more than prepared_statements_cache_size (%s), "+
					"automatically deallocating %s", string(humanizeutil.IBytes(cacheSize)), tail.prev,
			)
			ex.deletePreparedStmt(ctx, tail.prev)
		}
	}

	// Remember the inferred placeholder types so they can be reported on
	// Describe. First, try to preserve the hints sent by the client.
	prepared.InferredTypes = make([]oid.Oid, len(prepared.Types))
	copy(prepared.InferredTypes, rawTypeHints)
	for i, it := range prepared.InferredTypes {
		// If the client did not provide an OID type hint, then infer the OID.
		if it == 0 || it == oid.T_unknown {
			if t, ok := prepared.ValueType(tree.PlaceholderIdx(i)); ok {
				prepared.InferredTypes[i] = t.Oid()
			}
		}
	}

	return prepared, nil
}

// prepare prepares the given statement. This is used to create the plan in the
// "extended" pgwire protocol.
//
// placeholderHints may contain partial type information for placeholders.
// prepare will populate the missing types. It can be nil.
func (ex *connExecutor) prepare(
	ctx context.Context,
	stmt Statement,
	placeholderHints tree.PlaceholderTypes,
	rawTypeHints []oid.Oid,
	origin PreparedStatementOrigin,
) (_ *PreparedStatement, retErr error) {

	prepared := &PreparedStatement{
		memAcc:   ex.sessionPreparedMon.MakeBoundAccount(),
		refCount: 1,

		createdAt: timeutil.Now(),
		origin:    origin,
	}
	defer func() {
		// Make sure to close the memory account if an error is returned.
		if retErr != nil {
			prepared.memAcc.Close(ctx)
		}
	}()

	if stmt.AST == nil {
		return prepared, nil
	}

	origNumPlaceholders := stmt.NumPlaceholders
	switch stmt.AST.(type) {
	case *tree.Prepare, *tree.CopyTo:
		// Special cases:
		// - We're preparing a SQL-level PREPARE using the
		// wire protocol. There's an ambiguity from the perspective of this code:
		// any placeholders that are inside of the statement that we're preparing
		// shouldn't be treated as placeholders to the PREPARE statement. So, we
		// edit the NumPlaceholders field to be 0 here.
		// - We're preparing a COPY ... TO statement. We match the Postgres
		// behavior, which is to treat the statement as if it had no placeholders.
		stmt.NumPlaceholders = 0
	}

	var flags planFlags
	prepare := func(ctx context.Context, txn *kv.Txn) (err error) {
		p := &ex.planner
		if origin == PreparedStatementOriginWire {
			// If the PREPARE command was issued as a SQL statement or through
			// deserialize_session, then we have already reset the planner at the very
			// beginning of the execution (in execStmtInOpenState). We might have also
			// instrumented the planner to collect execution statistics, and resetting
			// the planner here would break the assumptions of the instrumentation.
			ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
			ex.resetPlanner(ctx, p, txn, ex.server.cfg.Clock.PhysicalTime())
		}

		if err := ex.maybeAdjustTxnForDDL(ctx, stmt); err != nil {
			return err
		}

		if placeholderHints == nil {
			placeholderHints = make(tree.PlaceholderTypes, stmt.NumPlaceholders)
		} else if rawTypeHints != nil {
			// If we were provided any type hints, attempt to resolve any user defined
			// type OIDs into types.T's.
			for i := range placeholderHints {
				if placeholderHints[i] == nil {
					if i >= len(rawTypeHints) {
						break
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
				Types:     placeholderHints,
			},
		}
		prepared.Statement = stmt.Statement
		// When we set our prepared statement, we need to make sure to propagate
		// the original NumPlaceholders if we're preparing a PREPARE.
		prepared.Statement.NumPlaceholders = origNumPlaceholders
		prepared.StatementNoConstants = stmt.StmtNoConstants
		prepared.StatementSummary = stmt.StmtSummary

		// Point to the prepared state, which can be further populated during query
		// preparation.
		stmt.Prepared = prepared

		if stmt.NumPlaceholders > 0 {
			if err := tree.ProcessPlaceholderAnnotations(&ex.planner.semaCtx, stmt.AST, placeholderHints); err != nil {
				return err
			}
		}

		p.stmt = stmt
		p.semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
		p.extendedEvalCtx.Annotations = &p.semaCtx.Annotations
		flags, err = ex.populatePrepared(ctx, txn, placeholderHints, p, origin)
		return err
	}

	// Use the existing transaction.
	if err := prepare(ctx, ex.state.mu.txn); err != nil {
		if origin != PreparedStatementOriginSessionMigration {
			return nil, err
		} else {
			f := tree.NewFmtCtx(tree.FmtMarkRedactionNode | tree.FmtSimple)
			f.FormatNode(stmt.AST)
			redactableStmt := redact.SafeString(f.CloseAndGetString())
			log.Warningf(ctx, "could not prepare statement during session migration (%s): %v", redactableStmt, err)
		}
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
	ctx context.Context,
	txn *kv.Txn,
	placeholderHints tree.PlaceholderTypes,
	p *planner,
	origin PreparedStatementOrigin,
) (planFlags, error) {
	if before := ex.server.cfg.TestingKnobs.BeforePrepare; before != nil {
		if err := before(ctx, ex.planner.stmt.String(), txn); err != nil {
			return 0, err
		}
	}
	stmt := &p.stmt

	p.semaCtx.Placeholders.Init(stmt.NumPlaceholders, placeholderHints)
	p.extendedEvalCtx.PrepareOnly = true
	// If the statement is being prepared by a session migration, then we should
	// not evaluate the AS OF SYSTEM TIME timestamp. During session migration,
	// there is no way for the statement being prepared to be executed in this
	// transaction, so there's no need to fix the timestamp, unlike how we must
	// for pgwire- or SQL-level prepared statements.
	if origin != PreparedStatementOriginSessionMigration {
		if err := ex.handleAOST(ctx, p.stmt.AST); err != nil {
			return 0, err
		}
	}

	// PREPARE has a limited subset of statements it can be run with. Postgres
	// only allows SELECT, INSERT, UPDATE, DELETE and VALUES statements to be
	// prepared.
	// See: https://www.postgresql.org/docs/current/static/sql-prepare.html
	// However, we must be able to handle every type of statement below because
	// the Postgres extended protocol requires running statements via the prepare
	// and execute paths.
	flags, err := p.prepareUsingOptimizer(ctx, origin)
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
	var ps *PreparedStatement
	retErr := func(err error) (fsm.Event, fsm.EventPayload) {
		if bindCmd.PreparedStatementName != "" {
			err = errors.WithDetailf(err, "statement name %q", bindCmd.PreparedStatementName)
		}
		if bindCmd.PortalName != "" {
			err = errors.WithDetailf(err, "portal name %q", bindCmd.PortalName)
		}
		if ps != nil && ps.StatementSummary != "" {
			err = errors.WithDetailf(err, "statement summary %q", ps.StatementSummary)
		}
		return eventNonRetriableErr{IsCommit: fsm.False}, eventNonRetriableErrPayload{err: err}
	}

	var ok bool
	ps, ok = ex.extraTxnState.prepStmtsNamespace.prepStmts[bindCmd.PreparedStatementName]
	if !ok {
		return retErr(newPreparedStmtDNEError(ex.sessionData(), bindCmd.PreparedStatementName))
	}

	ex.extraTxnState.prepStmtsNamespace.touchLRUEntry(bindCmd.PreparedStatementName)

	// We need to make sure type resolution happens within a transaction.
	// Otherwise, for user-defined types we won't take the correct leases and
	// will get back out of date type information.
	// This code path is only used by the wire-level Bind command. The
	// SQL EXECUTE command (which also needs to bind and resolve types) is
	// handled separately in conn_executor_exec.
	if _, isNoTxn := ex.machine.CurState().(stateNoTxn); isNoTxn {
		// The one critical exception is that we must not open a transaction when
		// executing SHOW COMMIT TIMESTAMP as it would destroy the information
		// about the previously committed transaction.
		if _, ok := ps.AST.(*tree.ShowCommitTimestamp); !ok {
			return ex.beginImplicitTxn(ctx, ps.AST, ex.QualityOfService())
		}
	} else if _, isAbortedTxn := ex.machine.CurState().(stateAborted); isAbortedTxn {
		if !ex.isAllowedInAbortedTxn(ps.AST) {
			return retErr(sqlerrors.NewTransactionAbortedError("" /* customMsg */))
		}
	}

	// Check if we need to auto-commit the transaction due to DDL.
	if ev, payload := ex.maybeAutoCommitBeforeDDL(ctx, ps.AST); ev != nil {
		return ev, payload
	}

	portalName := bindCmd.PortalName
	// The unnamed portal can be freely overwritten.
	if portalName != "" {
		if _, ok := ex.extraTxnState.prepStmtsNamespace.portals[portalName]; ok {
			return retErr(pgerror.Newf(
				pgcode.DuplicateCursor, "portal %q already exists", portalName))
		}
		if cursor := ex.getCursorAccessor().getCursor(tree.Name(portalName)); cursor != nil {
			return retErr(pgerror.Newf(
				pgcode.DuplicateCursor, "portal %q already exists as cursor", portalName))
		}
	} else {
		// Deallocate the unnamed portal, if it exists.
		ex.deletePortal(ctx, "")
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
					"bind message supplies %d parameters, but requires %d", len(bindCmd.Args), numQArgs))
		}

		resolve := func(ctx context.Context, txn *kv.Txn) (err error) {
			ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
			p := &ex.planner
			ex.resetPlanner(ctx, p, txn, ex.server.cfg.Clock.PhysicalTime() /* stmtTS */)
			if err := ex.handleAOST(ctx, ps.AST); err != nil {
				return err
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
						// These special cases for json, json[] is here so we can
						// support decoding parameters with oid=json/json[] without
						// adding full support for these type.
						// TODO(sql-exp): Remove this if we support JSON.
						if t == oid.T_json {
							typ = types.Json
						} else if t == oid.T__json {
							typ = types.JSONArrayForDecodingOnly
						} else {
							var err error
							typ, err = ex.planner.ResolveTypeByOID(ctx, t)
							if err != nil {
								return err
							}
						}
					}
					d, err := pgwirebase.DecodeDatum(
						ctx,
						ex.planner.EvalContext(),
						typ,
						qArgFormatCodes[i],
						arg,
						p.datumAlloc,
					)
					if err != nil {
						return pgerror.Wrapf(err, pgcode.ProtocolViolation, "error in argument for %s", k)
					}
					qargs[k] = d
				}
			}
			return nil
		}

		// Use the existing transaction.
		if err := resolve(ctx, ex.state.mu.txn); err != nil {
			return retErr(err)
		}
	}

	numCols := len(ps.Columns)
	if (len(bindCmd.OutFormats) > 1) && (len(bindCmd.OutFormats) != numCols) {
		err := pgwirebase.NewProtocolViolationErrorf(
			"expected 1 or %d for number of format codes, got %d",
			numCols, len(bindCmd.OutFormats))
		// A user is hitting this error unexpectedly and rarely, dump extra info,
		// should be okay since this should be a very rare error.
		log.Infof(ctx, "%s outformats: %v, AST: %T, prepared statements: %s", err.Error(),
			bindCmd.OutFormats, ps.AST, ex.extraTxnState.prepStmtsNamespace.String())
		return retErr(err)
	}

	columnFormatCodes := bindCmd.OutFormats
	if len(bindCmd.OutFormats) == 1 && numCols > 1 {
		// Apply the format code to every column.
		columnFormatCodes = make([]pgwirebase.FormatCode, numCols)
		for i := 0; i < numCols; i++ {
			columnFormatCodes[i] = bindCmd.OutFormats[0]
		}
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
	if cursor := ex.getCursorAccessor().getCursor(tree.Name(portalName)); cursor != nil {
		panic(errors.AssertionFailedf("portal already exists as cursor: %q", portalName))
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
	alloc := ps.memAcc.Allocated()
	ps.decRef(ctx)
	delete(ex.extraTxnState.prepStmtsNamespace.prepStmts, name)
	ex.extraTxnState.prepStmtsNamespace.delLRUEntry(name, alloc)
}

func (ex *connExecutor) deletePortal(ctx context.Context, name string) {
	portal, ok := ex.extraTxnState.prepStmtsNamespace.portals[name]
	if !ok {
		return
	}
	portal.close(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc, name)
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
	_, isAbortedTxn := ex.machine.CurState().(stateAborted)

	switch descCmd.Type {
	case pgwirebase.PrepareStatement:
		ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[string(descCmd.Name)]
		if !ok {
			return retErr(newPreparedStmtDNEError(ex.sessionData(), string(descCmd.Name)))
		}
		// Not currently counting this as an LRU touch on prepStmtsLRU for
		// prepared_statements_cache_size (but maybe we should?).

		ast := ps.AST
		if execute, ok := ast.(*tree.Execute); ok {
			// If we're describing an EXECUTE, we need to look up the statement type
			// of the prepared statement that the EXECUTE refers to, or else we'll
			// return the wrong information for describe.
			innerPs, found := ex.extraTxnState.prepStmtsNamespace.prepStmts[string(execute.Name)]
			if !found {
				return retErr(newPreparedStmtDNEError(ex.sessionData(), string(execute.Name)))
			}
			ast = innerPs.AST
		}
		if isAbortedTxn && !ex.isAllowedInAbortedTxn(ast) {
			return retErr(sqlerrors.NewTransactionAbortedError("" /* customMsg */))
		}
		res.SetInferredTypes(ps.InferredTypes)
		if stmtHasNoData(ast) {
			res.SetNoDataRowDescription()
		} else {
			res.SetPrepStmtOutput(ctx, ps.Columns)
		}
	case pgwirebase.PreparePortal:
		// TODO(rimadeodhar): prepStmtsNamespace should also be updated to use tree.Name instead of string
		// for indexing internal maps.
		portal, ok := ex.extraTxnState.prepStmtsNamespace.portals[string(descCmd.Name)]
		if !ok {
			// Check SQL-level cursors.
			cursor := ex.getCursorAccessor().getCursor(descCmd.Name)
			if cursor == nil {
				return retErr(pgerror.Newf(
					pgcode.InvalidCursorName, "unknown portal %q", descCmd.Name))
			}
			if isAbortedTxn {
				return retErr(sqlerrors.NewTransactionAbortedError("" /* customMsg */))
			}
			// Sending a nil formatCodes is equivalent to sending all text format
			// codes.
			res.SetPortalOutput(ctx, cursor.Rows.Types(), nil /* formatCodes */)
			return nil, nil
		}

		ast := portal.Stmt.AST
		if isAbortedTxn && !ex.isAllowedInAbortedTxn(ast) {
			return retErr(sqlerrors.NewTransactionAbortedError("" /* customMsg */))
		}
		if stmtHasNoData(ast) {
			res.SetNoDataRowDescription()
		} else {
			res.SetPortalOutput(ctx, portal.Stmt.Columns, portal.OutFormats)
		}
	default:
		return retErr(pgerror.Newf(
			pgcode.ProtocolViolation,
			"invalid DESCRIBE message subtype %d", errors.Safe(byte(descCmd.Type)),
		))
	}
	return nil, nil
}

// isAllowedInAbortedTxn returns true if the statement is allowed to be
// prepared and executed inside of an aborted transaction.
func (ex *connExecutor) isAllowedInAbortedTxn(ast tree.Statement) bool {
	switch s := ast.(type) {
	case *tree.CommitTransaction, *tree.PrepareTransaction,
		*tree.RollbackTransaction, *tree.RollbackToSavepoint:
		return true
	case *tree.Savepoint:
		if ex.isCommitOnReleaseSavepoint(s.Name) {
			return true
		}
		return false
	default:
		return false
	}
}

// newPreparedStmtDNEError creates an InvalidSQLStatementName error for when a
// prepared statement does not exist.
func newPreparedStmtDNEError(sd *sessiondata.SessionData, name string) error {
	err := pgerror.Newf(
		pgcode.InvalidSQLStatementName, "prepared statement %q does not exist", name,
	)
	cacheSize := sd.PreparedStatementsCacheSize
	if cacheSize != 0 {
		err = errors.WithHintf(
			err, "note that prepared_statements_cache_size is set to %s",
			string(humanizeutil.IBytes(cacheSize)),
		)
	}
	return err
}
