// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// plpgsqlBuilder translates a PLpgSQL AST into a series of SQL routines that
// can be optimized and executed just like a native SQL statement. This allows
// CRDB to support PLpgSQL syntax without having to implement a specialized
// interpreter, and takes advantage of existing SQL optimizations.
//
// The main difficulty of executing PLpgSQL with the SQL execution engine lies
// in modeling the control flow. PLpgSQL supports typical control-flow
// statements like IF and WHILE, and it allows for variables to be assigned
// within these control-flow statements. After the control-flow statement exits,
// any modifications made to variables are still visible.
//
// plpgsqlBuilder handles this by constructing a new "continuation" routine for
// each branch when it reaches a control-flow statement. The continuation
// returns the (single column) result of executing the rest of the PLpgSQL
// statements from that point on. Transfer of control at a branching point is
// then handled by explicitly calling the continuation routine for that branch.
//
// Variable declarations are handled by projecting a column; variable
// assignments are handled by projecting a new column with the same name. The
// up-to-date values for each variable are passed to each invocation of a
// continuation routine.
//
// Return statements are handled by simply projecting the returned expression.
//
// For example:
//
//	CREATE FUNCTION f(x INT) RETURNS INT AS $$
//	   DECLARE
//	      i INT := 0;
//	   BEGIN
//	      LOOP
//	         IF i >= x THEN
//	            EXIT;
//	         END IF;
//	         i := i + 1;
//	      END LOOP;
//	      RETURN i;
//	   END
//	$$ LANGUAGE PLpgSQL;
//
// This function will be (logically) broken into the following routines:
//
//	CREATE FUNCTION f(x INT) RETURNS INT AS $$
//	   -- Initialize "i", then enter the loop.
//	   SELECT loop(x, i) FROM (SELECT 0 AS i);
//	$$ LANGUAGE SQL;
//
//	CREATE FUNCTION loop(x INT, i INT) RETURNS INT AS $$
//	   -- Check the IF condition, then call the correct branch continuation.
//	   SELECT CASE WHEN i >= x
//	      THEN then_branch(x, i)
//	      ELSE else_branch(x, i) END;
//	$$ LANGUAGE SQL;
//
//	CREATE FUNCTION then_branch(x INT, i INT) RETURNS INT AS $$
//	   -- Call the continuation for the statements after the loop.
//	   SELECT exit(x, i);
//	$$ LANGUAGE SQL;
//
//	CREATE FUNCTION else_branch(x INT, i INT) RETURNS INT AS $$
//	   -- Increment "i" and enter the next loop iteration.
//	   SELECT loop(x, i) FROM (SELECT i + 1 AS i);
//	$$ LANGUAGE SQL;
//
//	CREATE FUNCTION exit(x INT, i INT) RETURNS INT AS $$
//	   -- Return "i".
//	   SELECT i;
//	$$ LANGUAGE SQL;
//
// Note that some of these routines may be inlined in practice (e.g. exit()).
//
// See the buildPLpgSQLStatements comments for details. For further reference,
// see citations: [9] - the logic here is based on the transformation outlined
// there from PLpgSQL to "administrative normal form" (mutually tail-recursive
// functions). Note that the paper details further steps beyond ANF that we do
// not follow here, although they may be good routes for optimization in the
// future.
type plpgsqlBuilder struct {
	ob *Builder

	// colRefs, if non-nil, tracks the set of columns referenced by scalar
	// expressions.
	colRefs *opt.ColSet

	// params tracks the names and types for the original function parameters.
	params []tree.ParamType

	// decls is the set of variable declarations for a PL/pgSQL function.
	decls []plpgsqltree.PLpgSQLDecl

	// varTypes maps from the name of each variable to its type.
	varTypes map[tree.Name]*types.T

	// returnType is the return type of the PL/pgSQL function.
	returnType *types.T

	// continuations is used to model the control flow of a PL/pgSQL function.
	// The head of the continuations stack is used upon reaching the end of a
	// statement block to call a function that models the statements that come
	// next after the block. In the context of a loop, this is used to recursively
	// call back into the loop body.
	continuations []continuation

	// exitContinuations is similar to continuations, but is used upon reaching an
	// EXIT statement within a loop. It is used to resume execution with the
	// statements that follow the loop.
	exitContinuations []continuation

	identCounter int
}

func (b *plpgsqlBuilder) init(
	ob *Builder,
	colRefs *opt.ColSet,
	params []tree.ParamType,
	block *plpgsqltree.PLpgSQLStmtBlock,
	returnType *types.T,
) {
	b.ob = ob
	b.colRefs = colRefs
	b.params = params
	b.decls = block.Decls
	b.returnType = returnType
	b.varTypes = make(map[tree.Name]*types.T)
	for _, dec := range b.decls {
		typ, err := tree.ResolveType(b.ob.ctx, dec.Typ, b.ob.semaCtx.TypeResolver)
		if err != nil {
			panic(err)
		}
		b.varTypes[dec.Var] = typ
		if dec.NotNull {
			panic(unimplemented.NewWithIssueDetail(105243,
				"not null variable",
				"not-null PL/pgSQL variables are not yet supported",
			))
		}
		if dec.Constant {
			panic(unimplemented.NewWithIssueDetail(105241,
				"constant variable",
				"constant PL/pgSQL variables are not yet supported",
			))
		}
		if dec.Collate != "" {
			panic(unimplemented.NewWithIssueDetail(105245,
				"variable collation",
				"collation for PL/pgSQL variables is not yet supported",
			))
		}
	}
}

// build constructs an expression that returns the result of executing a
// PL/pgSQL function. See buildPLpgSQLStatements for more details.
func (b *plpgsqlBuilder) build(block *plpgsqltree.PLpgSQLStmtBlock, s *scope) *scope {
	s = s.push()
	b.ensureScopeHasExpr(s)

	// Some variable declarations initialize the variable.
	for _, dec := range b.decls {
		if dec.Expr != nil {
			s = b.addPLpgSQLAssign(s, dec.Var, dec.Expr)
		} else {
			// Uninitialized variables are null.
			s = b.addPLpgSQLAssign(s, dec.Var, &tree.CastExpr{Expr: tree.DNull, Type: dec.Typ})
		}
	}
	if s = b.buildPLpgSQLStatements(block.Body, s); s != nil {
		return s
	}
	// At least one path in the control flow does not terminate with a RETURN
	// statement.
	//
	// Postgres throws this error at runtime, so it's possible to define a
	// function that runs correctly for some inputs, but returns this error for
	// others. We are compiling rather than interpreting, so it seems better to
	// eagerly return the error if there is an execution path with no RETURN.
	// TODO(drewk): consider using RAISE (when it is implemented) to throw the
	// error at runtime instead.
	panic(pgerror.New(
		pgcode.RoutineExceptionFunctionExecutedNoReturnStatement,
		"control reached end of function without RETURN",
	))
}

// buildPLpgSQLStatements performs the majority of the work building a PL/pgSQL
// function definition into a form that can be handled by the SQL execution
// engine. It models control flow statements by defining (possibly recursive)
// functions that model returning control after a statement block has finished
// executing. See the comments within for further detail.
//
// buildPLpgSQLStatements returns nil if one or more branches in the given
// statements do not eventually terminate with a RETURN statement.
func (b *plpgsqlBuilder) buildPLpgSQLStatements(
	stmts []plpgsqltree.PLpgSQLStatement, s *scope,
) *scope {
	b.ensureScopeHasExpr(s)
	for i, stmt := range stmts {
		switch t := stmt.(type) {
		case *plpgsqltree.PLpgSQLStmtReturn:
			// RETURN is handled by projecting a single column with the expression
			// that is being returned.
			returnScalar := b.buildPLpgSQLExpr(t.Expr, b.returnType, s)
			returnColName := scopeColName("").WithMetadataName(b.makeIdentifier("stmt_return"))
			returnScope := s.push()
			b.ob.synthesizeColumn(returnScope, returnColName, b.returnType, nil /* expr */, returnScalar)
			b.ob.constructProjectForScope(s, returnScope)
			return returnScope
		case *plpgsqltree.PLpgSQLStmtAssign:
			// Assignment (:=) is handled by projecting a new column with the same
			// name as the variable being assigned.
			s = b.addPLpgSQLAssign(s, t.Var, t.Value)
		case *plpgsqltree.PLpgSQLStmtIf:
			if len(t.ElseIfList) != 0 {
				panic(unimplemented.New(
					"ELSIF statements",
					"PL/pgSQL ELSIF branches are not yet supported",
				))
			}
			// IF statement control flow is handled by calling a "continuation"
			// function in each branch that executes all the statements that logically
			// follow the IF statement block.
			//
			// Create a function that models executing the statements that follow the
			// IF statement. If the IF statement is the last statement in its own
			// block, a statement from an ancestor block will be used.
			// Example:
			//   IF (...) THEN ... END IF;
			//   RETURN (...); <-- This is used to build the continuation function.
			con := b.makeContinuation("stmt_if")
			b.finishContinuation(stmts[i+1:], &con, false /* recursive */)
			b.pushContinuation(con)
			// Build each branch of the IF statement, calling the continuation
			// function at the end of construction in order to resume execution after
			// the IF block.
			thenScope := b.buildPLpgSQLStatements(t.ThenBody, s.push())
			// Note that if the ELSE body is empty, elseExpr will be equivalent to
			// executing the statements following the IF statement (it will be a call
			// to the continuation that was built above).
			elseScope := b.buildPLpgSQLStatements(t.ElseBody, s.push())
			b.popContinuation()

			if thenScope == nil || elseScope == nil {
				// One or both branches didn't terminate with a RETURN statement.
				return nil
			}
			thenExpr, elseExpr := thenScope.expr, elseScope.expr

			// Build a scalar CASE statement that conditionally executes either branch
			// of the IF statement as a subquery.
			cond := b.buildPLpgSQLExpr(t.Condition, types.Bool, s)
			thenScalar := b.ob.factory.ConstructSubquery(thenExpr, &memo.SubqueryPrivate{})
			elseScalar := b.ob.factory.ConstructSubquery(elseExpr, &memo.SubqueryPrivate{})
			whenExpr := memo.ScalarListExpr{b.ob.factory.ConstructWhen(cond, thenScalar)}
			scalar := b.ob.factory.ConstructCase(memo.TrueSingleton, whenExpr, elseScalar)

			// Return a single column that projects the result of the CASE statement.
			returnColName := scopeColName("").WithMetadataName(b.makeIdentifier("stmt_if"))
			returnScope := s.push()
			b.ob.synthesizeColumn(returnScope, returnColName, b.returnType, nil /* expr */, scalar)
			b.ob.constructProjectForScope(s, returnScope)
			return returnScope
		case *plpgsqltree.PLpgSQLStmtSimpleLoop:
			if t.Label != "" {
				panic(unimplemented.New(
					"LOOP label",
					"LOOP statement labels are not yet supported",
				))
			}
			// LOOP control flow is handled similarly to IF statements, but two
			// continuation functions are used - one that executes the loop body, and
			// one that executes the statements following the LOOP statement. These
			// are used while building the loop body, which means that its definition
			// is recursive.
			//
			// Upon reaching the end of the loop body statements or a CONTINUE
			// statement, the loop body function is called. Upon reaching an EXIT
			// statement, the exit continuation is called to model returning control
			// flow to the statements outside the loop.
			exitCon := b.makeContinuation("loop_exit")
			b.finishContinuation(stmts[i+1:], &exitCon, false /* recursive */)
			b.pushExitContinuation(exitCon)
			loopContinuation := b.makeContinuation("stmt_loop")
			loopContinuation.isLoopContinuation = true
			b.pushContinuation(loopContinuation)
			b.finishContinuation(t.Body, &loopContinuation, true /* recursive */)
			b.popContinuation()
			b.popExitContinuation()
			return b.callContinuation(&loopContinuation, s)
		case *plpgsqltree.PLpgSQLStmtExit:
			if t.Label != "" {
				panic(unimplemented.New(
					"EXIT label",
					"EXIT statement labels are not yet supported",
				))
			}
			// EXIT statements are handled by calling the function that executes the
			// statements after a loop. Errors if used outside a loop.
			if con := b.getExitContinuation(); con != nil {
				return b.callContinuation(con, s)
			} else {
				panic(pgerror.New(
					pgcode.Syntax,
					"EXIT cannot be used outside a loop, unless it has a label",
				))
			}
		case *plpgsqltree.PLpgSQLStmtContinue:
			if t.Label != "" {
				panic(unimplemented.New(
					"CONTINUE label",
					"CONTINUE statement labels are not yet supported",
				))
			}
			// CONTINUE statements are handled by calling the function that executes
			// the loop body. Errors if used outside a loop.
			if con := b.getLoopContinuation(); con != nil {
				return b.callContinuation(con, s)
			} else {
				panic(pgerror.New(pgcode.Syntax, "CONTINUE cannot be used outside a loop"))
			}
		case *plpgsqltree.PLpgSQLStmtRaise:
			// RAISE statements allow the PLpgSQL function to send an error or a
			// notice to the client. We handle these side effects by building them
			// into a separate body statement that is only executed for its side
			// effects. The remaining PLpgSQL statements then become the last body
			// statement, which returns the actual result of evaluation.
			//
			// The synchronous notice sending behavior is implemented in the
			// crdb_internal.plpgsql_raise builtin function. The side-effecting body
			// statement just makes a call into crdb_internal.plpgsql_raise using the
			// RAISE statement options as parameters.
			con := b.makeContinuation("_stmt_raise")
			const raiseFnName = "crdb_internal.plpgsql_raise"
			props, overloads := builtinsregistry.GetBuiltinProperties(raiseFnName)
			if len(overloads) != 1 {
				panic(errors.AssertionFailedf("expected one overload for %s", raiseFnName))
			}
			raiseCall := b.ob.factory.ConstructFunction(
				b.getRaiseArgs(con.s, t),
				&memo.FunctionPrivate{
					Name:       raiseFnName,
					Typ:        types.Int,
					Properties: props,
					Overload:   &overloads[0],
				},
			)
			raiseColName := scopeColName("").WithMetadataName(b.makeIdentifier("stmt_raise"))
			raiseScope := con.s.push()
			b.ob.synthesizeColumn(raiseScope, raiseColName, types.Int, nil /* expr */, raiseCall)
			b.ob.constructProjectForScope(con.s, raiseScope)
			con.def.Body = []memo.RelExpr{raiseScope.expr}
			con.def.BodyProps = []*physical.Required{raiseScope.makePhysicalProps()}
			b.finishContinuation(stmts[i+1:], &con, false /* recursive */)
			return b.callContinuation(&con, s)
		default:
			panic(unimplemented.New(
				"unimplemented PL/pgSQL statement",
				"attempted to use a PL/pgSQL statement that is not yet supported",
			))
		}
	}
	// Call the parent continuation to execute the rest of the function.
	return b.callContinuation(b.getContinuation(), s)
}

// addPLpgSQLAssign adds a PL/pgSQL assignment to the current scope as a
// new column with the variable name that projects the assigned expression.
// If there is a column with the same name in the previous scope, it will be
// replaced. This allows the plpgsqlBuilder to model variable mutations.
func (b *plpgsqlBuilder) addPLpgSQLAssign(
	inScope *scope, ident plpgsqltree.PLpgSQLVariable, val plpgsqltree.PLpgSQLExpr,
) *scope {
	typ, ok := b.varTypes[ident]
	if !ok {
		panic(errors.AssertionFailedf("failed to find type for variable %s", ident))
	}
	assignScope := inScope.push()
	for i := range inScope.cols {
		col := &inScope.cols[i]
		if col.name.ReferenceName() == ident {
			// Allow the assignment to shadow previous values for this column.
			continue
		}
		// If the column is not an outer column, add the column as a pass-through
		// column from the previous scope.
		assignScope.appendColumn(col)
	}
	// Project the assignment as a new column.
	colName := scopeColName(ident)
	scalar := b.buildPLpgSQLExpr(val, typ, inScope)
	b.ob.synthesizeColumn(assignScope, colName, typ, nil, scalar)
	b.ob.constructProjectForScope(inScope, assignScope)
	return assignScope
}

// getRaiseArgs validates the options attached to the given PLpgSQL RAISE
// statement and returns the arguments to be used for a call to the
// crdb_internal.plpgsql_raise builtin function.
func (b *plpgsqlBuilder) getRaiseArgs(
	s *scope, raise *plpgsqltree.PLpgSQLStmtRaise,
) memo.ScalarListExpr {
	var severity, message, detail, hint, code opt.ScalarExpr
	makeConstStr := func(str string) opt.ScalarExpr {
		return b.ob.factory.ConstructConstVal(tree.NewDString(str), types.String)
	}
	// Retrieve the error/notice severity.
	logLevel := strings.ToUpper(raise.LogLevel)
	if logLevel == "" {
		// EXCEPTION is the default log level.
		logLevel = "EXCEPTION"
	}
	switch logLevel {
	case "EXCEPTION":
		// ERROR is the equivalent severity to log-level EXCEPTION.
		severity = makeConstStr("ERROR")
	case "LOG", "INFO", "NOTICE", "WARNING":
		severity = makeConstStr(logLevel)
	case "DEBUG":
		// DEBUG log-level maps to severity DEBUG1.
		severity = makeConstStr("DEBUG1")
	default:
		panic(unimplemented.Newf(
			"unimplemented log level", "RAISE log level %s is not yet supported", raise.LogLevel,
		))
	}
	// Retrieve the message, if it was set with the format syntax.
	if raise.Message != "" {
		message = b.makeRaiseFormatMessage(s, raise.Message, raise.Params)
	}
	if raise.Code != "" {
		code = makeConstStr(raise.Code)
	} else if raise.CodeName != "" {
		code = makeConstStr(raise.CodeName)
	}
	// Retrieve the RAISE options, if any.
	buildOptionExpr := func(name string, expr plpgsqltree.PLpgSQLExpr, isDup bool) opt.ScalarExpr {
		if isDup {
			panic(pgerror.Newf(pgcode.Syntax, "RAISE option already specified: %s", name))
		}
		return b.buildPLpgSQLExpr(expr, types.String, s)
	}
	for _, option := range raise.Options {
		optName := strings.ToUpper(option.OptType)
		switch optName {
		case "MESSAGE":
			message = buildOptionExpr(optName, option.Expr, message != nil)
		case "DETAIL":
			detail = buildOptionExpr(optName, option.Expr, detail != nil)
		case "HINT":
			hint = buildOptionExpr(optName, option.Expr, hint != nil)
		case "ERRCODE":
			code = buildOptionExpr(optName, option.Expr, code != nil)
		case "COLUMN", "CONSTRAINT", "DATATYPE", "TABLE", "SCHEMA":
			panic(unimplemented.NewWithIssuef(106237, "RAISE option %s is not yet implemented", optName))
		default:
			panic(errors.AssertionFailedf("unrecognized RAISE option: %s", option.OptType))
		}
	}
	if code == nil {
		if logLevel == "EXCEPTION" {
			// The default error code for EXCEPTION is ERRCODE_RAISE_EXCEPTION.
			code = makeConstStr(pgcode.RaiseException.String())
		} else {
			code = makeConstStr(pgcode.SuccessfulCompletion.String())
		}
	}
	// If no message text is supplied, use the error code or condition name.
	if message == nil {
		message = code
	}
	args := memo.ScalarListExpr{severity, message, detail, hint, code}
	for i := range args {
		if args[i] == nil {
			args[i] = makeConstStr("")
		}
	}
	return args
}

// A PLpgSQL RAISE statement can specify a format string, where supplied
// expressions replace instances of '%' in the string. A literal '%' character
// is specified by doubling it: '%%'. The formatting arguments can be arbitrary
// SQL expressions.
func (b *plpgsqlBuilder) makeRaiseFormatMessage(
	s *scope, format string, args []plpgsqltree.PLpgSQLExpr,
) (result opt.ScalarExpr) {
	makeConstStr := func(str string) opt.ScalarExpr {
		return b.ob.factory.ConstructConstVal(tree.NewDString(str), types.String)
	}
	addToResult := func(expr opt.ScalarExpr) {
		if result == nil {
			result = expr
		} else {
			// Concatenate the previously built string with the current one.
			result = b.ob.factory.ConstructConcat(result, expr)
		}
	}
	// Split the format string on each pair of '%' characters; any '%' characters
	// in the substrings are formatting parameters.
	var argIdx int
	for i, literalSubstr := range strings.Split(format, "%%") {
		if i > 0 {
			// Add the literal '%' character in place of the matched '%%'.
			addToResult(makeConstStr("%"))
		}
		// Split on the parameter characters '%'.
		for j, paramSubstr := range strings.Split(literalSubstr, "%") {
			if j > 0 {
				// Add the next argument at the location of this parameter.
				if argIdx >= len(args) {
					panic(pgerror.Newf(pgcode.PLpgSQL, "too few parameters specified for RAISE"))
				}
				addToResult(b.buildPLpgSQLExpr(args[argIdx], types.String, s))
				argIdx++
			}
			addToResult(makeConstStr(paramSubstr))
		}
	}
	if argIdx < len(args) {
		panic(pgerror.Newf(pgcode.PLpgSQL, "too many parameters specified for RAISE"))
	}
	return result
}

// makeContinuation allocates a new continuation function with an uninitialized
// definition.
func (b *plpgsqlBuilder) makeContinuation(name string) continuation {
	s := b.ob.allocScope()
	b.ensureScopeHasExpr(s)
	params := make(opt.ColList, 0, len(b.decls)+len(b.params))
	addParam := func(name tree.Name, typ *types.T) {
		colName := scopeColName(name)
		col := b.ob.synthesizeColumn(s, colName, typ, nil /* expr */, nil /* scalar */)
		// TODO(mgartner): Lift the 100 parameter restriction for synthesized
		// continuation UDFs.
		col.setParamOrd(len(params))
		params = append(params, col.id)
	}
	for _, dec := range b.decls {
		addParam(dec.Var, b.varTypes[dec.Var])
	}
	for _, param := range b.params {
		addParam(tree.Name(param.Name), param.Typ)
	}
	return continuation{
		def: &memo.UDFDefinition{
			Params:            params,
			Name:              b.makeIdentifier(name),
			Typ:               b.returnType,
			CalledOnNullInput: true,
		},
		s: s,
	}
}

// finishContinuation adds the final body statement to the definition of a
// continuation function. This statement returns the result of executing the
// given PLpgSQL statements. There may be other statements that are executed
// before this final statement for their side effects (e.g. RAISE statement).
//
// finishContinuation is separate from makeContinuation to allow recursive
// function definitions, which need to push the continuation before it is
// finished.
func (b *plpgsqlBuilder) finishContinuation(
	stmts []plpgsqltree.PLpgSQLStatement, con *continuation, recursive bool,
) {
	// Make sure to push s before constructing the continuation scope to ensure
	// that the parameter columns are not projected.
	continuationScope := b.buildPLpgSQLStatements(stmts, con.s.push())
	if continuationScope == nil {
		// One or more branches did not terminate with a RETURN statement.
		con.reachedEndOfFunction = true
		return
	}
	// Append to the body statements because some PLpgSQL statements will make a
	// continuation routine with more than one body statement in order to handle
	// side effects (see the RAISE case in buildPLpgSQLStatements).
	con.def.Body = append(con.def.Body, continuationScope.expr)
	con.def.BodyProps = append(con.def.BodyProps, continuationScope.makePhysicalProps())
	con.def.IsRecursive = recursive
	// Set the volatility of the continuation routine to the least restrictive
	// volatility level in the expression's Relational properties.
	vol := continuationScope.expr.Relational().VolatilitySet
	if vol.HasVolatile() {
		con.def.Volatility = volatility.Volatile
	} else if vol.HasStable() {
		con.def.Volatility = volatility.Stable
	} else if vol.IsLeakproof() {
		con.def.Volatility = volatility.Leakproof
	} else {
		con.def.Volatility = volatility.Immutable
	}
}

// callContinuation adds a column that projects the result of calling the
// given continuation function.
func (b *plpgsqlBuilder) callContinuation(con *continuation, s *scope) *scope {
	if con == nil || con.reachedEndOfFunction {
		// Return nil to signify "control reached end of function without RETURN".
		return nil
	}
	args := make(memo.ScalarListExpr, 0, len(b.decls)+len(b.params))
	addArg := func(name tree.Name, typ *types.T) {
		_, source, _, _ := s.FindSourceProvidingColumn(b.ob.ctx, name)
		if source != nil {
			args = append(args, b.ob.factory.ConstructVariable(source.(*scopeColumn).id))
		} else {
			args = append(args, b.ob.factory.ConstructNull(typ))
		}
	}
	for _, dec := range b.decls {
		addArg(dec.Var, b.varTypes[dec.Var])
	}
	for _, param := range b.params {
		addArg(tree.Name(param.Name), param.Typ)
	}
	// PLpgSQL continuation routines are always in tail-call position.
	call := b.ob.factory.ConstructUDFCall(args, &memo.UDFCallPrivate{Def: con.def, TailCall: true})

	returnColName := scopeColName("").WithMetadataName(con.def.Name)
	returnScope := s.push()
	b.ob.synthesizeColumn(returnScope, returnColName, b.returnType, nil /* expr */, call)
	b.ob.constructProjectForScope(s, returnScope)
	return returnScope
}

// buildPLpgSQLExpr parses and builds the given SQL expression into a ScalarExpr
// within the given scope.
func (b *plpgsqlBuilder) buildPLpgSQLExpr(
	expr plpgsqltree.PLpgSQLExpr, typ *types.T, s *scope,
) opt.ScalarExpr {
	expr, _ = tree.WalkExpr(s, expr)
	typedExpr, err := expr.TypeCheck(b.ob.ctx, b.ob.semaCtx, typ)
	if err != nil {
		panic(err)
	}
	return b.ob.buildScalar(typedExpr, s, nil, nil, b.colRefs)
}

func (b *plpgsqlBuilder) ensureScopeHasExpr(s *scope) {
	if s.expr == nil {
		s.expr = b.ob.factory.ConstructValues(memo.ScalarListWithEmptyTuple, &memo.ValuesPrivate{
			Cols: opt.ColList{},
			ID:   b.ob.factory.Metadata().NextUniqueID(),
		})
	}
}

func (b *plpgsqlBuilder) makeIdentifier(id string) string {
	b.identCounter++
	return fmt.Sprintf("%s_%d", id, b.identCounter)
}

// continuation holds the information necessary to pick up execution from some
// branching point in the control flow.
type continuation struct {
	// def is used to construct a call into a routine that picks up execution
	// from a branch in the control flow.
	def *memo.UDFDefinition

	// s is a scope initialized with the parameters of the routine. It should be
	// used to construct the routine body statement.
	s *scope

	// isLoopContinuation indicates that this continuation was constructed for the
	// body statements of a loop.
	isLoopContinuation bool

	// reachedEndOfFunction indicates that the statements used to define this
	// continuation did not return from at least one path in the control flow.
	// If this continuation is reachable from the root, we return a
	// "control reached end of function without RETURN" error.
	reachedEndOfFunction bool
}

func (b *plpgsqlBuilder) pushContinuation(con continuation) {
	b.continuations = append(b.continuations, con)
}

func (b *plpgsqlBuilder) popContinuation() {
	if len(b.continuations) > 0 {
		b.continuations = b.continuations[:len(b.continuations)-1]
	}
}

func (b *plpgsqlBuilder) getContinuation() *continuation {
	if len(b.continuations) == 0 {
		return nil
	}
	return &b.continuations[len(b.continuations)-1]
}

func (b *plpgsqlBuilder) pushExitContinuation(con continuation) {
	b.exitContinuations = append(b.exitContinuations, con)
}

func (b *plpgsqlBuilder) popExitContinuation() {
	if len(b.exitContinuations) > 0 {
		b.exitContinuations = b.exitContinuations[:len(b.exitContinuations)-1]
	}
}

func (b *plpgsqlBuilder) getExitContinuation() *continuation {
	if len(b.exitContinuations) == 0 {
		return nil
	}
	return &b.exitContinuations[len(b.exitContinuations)-1]
}

func (b *plpgsqlBuilder) getLoopContinuation() *continuation {
	for i := len(b.continuations) - 1; i >= 0; i-- {
		if b.continuations[i].isLoopContinuation {
			return &b.continuations[i]
		}
	}
	return nil
}
