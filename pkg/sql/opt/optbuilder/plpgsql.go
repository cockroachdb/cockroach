// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	ast "github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
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
// +---------+
// | Outline |
// +---------+
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
// +--------------+
// | Side Effects |
// +--------------+
//
// Side-effecting expressions must be executed in order as dictated by the
// control flow of the PLpgSQL statements. This is necessary in order to provide
// the imperative interface of PLpgSQL (vs the declarative interface of SQL).
// This is guaranteed by taking care to avoid duplicating, eliminating, and
// reordering volatile expressions.
//
// When possible, these guarantees are provided by executing a volatile
// expression alone in a subroutine's body statement. Routine body statements
// are always executed in order, and serve as an optimization barrier.
//
// There are cases where a volatile expression cannot be executed as its own
// body statement, and must instead be projected from a previous scope. One
// example of this is assignment - the assigned value must be able to reference
// previous values for the PLpgSQL variables, and its result must be available
// to whichever statement comes next in the control flow. Such cases are handled
// by adding explicit optimization barriers before and after projecting the
// volatile expression. This prevents optimizations that would change side
// effects, such as pushing a volatile expression into a join or union.
// See addBarrierIfVolatile for more information.
//
// +-----------------+
// | Further Reading |
// +-----------------+
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
	decls []ast.Declaration

	// varTypes maps from the name of each variable to its type.
	varTypes map[tree.Name]*types.T

	// constants tracks the variables that were declared as constant.
	constants map[tree.Name]struct{}

	// cursors is the set of cursor declarations for a PL/pgSQL routine. It is set
	// for bound cursor declarations, which allow a query to be associated with a
	// cursor before it is opened.
	cursors map[tree.Name]ast.CursorDeclaration

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

	// blockState is shared state for all routines that make up a PLpgSQL block,
	// including the implicit block that surrounds the body statements.
	blockState *tree.BlockState

	hasExceptionBlock bool
	identCounter      int
}

func (b *plpgsqlBuilder) init(
	ob *Builder, colRefs *opt.ColSet, params []tree.ParamType, block *ast.Block, returnType *types.T,
) {
	b.ob = ob
	b.colRefs = colRefs
	b.params = params
	b.returnType = returnType
	b.varTypes = make(map[tree.Name]*types.T)
	b.cursors = make(map[tree.Name]ast.CursorDeclaration)
	for i := range block.Decls {
		switch dec := block.Decls[i].(type) {
		case *ast.Declaration:
			b.decls = append(b.decls, *dec)
		case *ast.CursorDeclaration:
			// Declaration of a bound cursor declares a variable of type refcursor.
			b.decls = append(b.decls, ast.Declaration{Var: dec.Name, Typ: types.RefCursor})
			b.cursors[dec.Name] = *dec
		}
	}
	for _, param := range b.params {
		b.addVariableType(tree.Name(param.Name), param.Typ)
	}
	for _, dec := range b.decls {
		typ, err := tree.ResolveType(b.ob.ctx, dec.Typ, b.ob.semaCtx.TypeResolver)
		if err != nil {
			panic(err)
		}
		b.addVariableType(dec.Var, typ)
		if dec.NotNull {
			panic(unimplemented.NewWithIssueDetail(105243,
				"not null variable",
				"not-null PL/pgSQL variables are not yet supported",
			))
		}
		if dec.Collate != "" {
			panic(unimplemented.NewWithIssueDetail(105245,
				"variable collation",
				"collation for PL/pgSQL variables is not yet supported",
			))
		}
		if types.IsRecordType(typ) {
			panic(unimplemented.NewWithIssueDetail(114874,
				"RECORD variable",
				"RECORD type for PL/pgSQL variables is not yet supported",
			))
		}
	}
}

// build constructs an expression that returns the result of executing a
// PL/pgSQL function. See buildPLpgSQLStatements for more details.
func (b *plpgsqlBuilder) build(block *ast.Block, s *scope) *scope {
	s = s.push()
	b.ensureScopeHasExpr(s)

	b.constants = make(map[tree.Name]struct{})
	for _, dec := range b.decls {
		if dec.Expr != nil {
			// Some variable declarations initialize the variable.
			s = b.addPLpgSQLAssign(s, dec.Var, dec.Expr)
		} else {
			// Uninitialized variables are null.
			s = b.addPLpgSQLAssign(s, dec.Var, &tree.CastExpr{Expr: tree.DNull, Type: dec.Typ})
		}
		if dec.Constant {
			// Add to the constants map after initializing the variable, since
			// constant variables only prevent assignment, not initialization.
			b.constants[dec.Var] = struct{}{}
		}
	}
	if types.IsRecordType(b.returnType) {
		// Infer the concrete type by examining the RETURN statements. This has to
		// happen after building the declaration block because RETURN statements can
		// reference declared variables.
		recordVisitor := newRecordTypeVisitor(b.ob.ctx, b.ob.semaCtx, s)
		ast.Walk(recordVisitor, block)
		b.returnType = recordVisitor.typ
	}
	if exceptions := b.buildExceptions(block); exceptions != nil {
		// There is an implicit block around the body statements, with an optional
		// exception handler. Note that the variable declarations are not in block
		// scope, and exceptions thrown during variable declaration are not caught.
		//
		// The routine is volatile to prevent inlining. Only the block and
		// variable-assignment routines need to be volatile; see the buildExceptions
		// comment for details.
		b.blockState = &tree.BlockState{}
		blockCon := b.makeContinuation("exception_block")
		blockCon.def.ExceptionBlock = exceptions
		blockCon.def.Volatility = volatility.Volatile
		b.appendPlpgSQLStmts(&blockCon, block.Body)
		return b.callContinuation(&blockCon, s)
	}
	return b.buildPLpgSQLStatements(block.Body, s)
}

// buildPLpgSQLStatements performs the majority of the work building a PL/pgSQL
// function definition into a form that can be handled by the SQL execution
// engine. It models control flow statements by defining (possibly recursive)
// functions that model returning control after a statement block has finished
// executing. See the comments within for further detail.
//
// buildPLpgSQLStatements returns nil if one or more branches in the given
// statements do not eventually terminate with a RETURN statement.
func (b *plpgsqlBuilder) buildPLpgSQLStatements(stmts []ast.Statement, s *scope) *scope {
	b.ensureScopeHasExpr(s)
	for i, stmt := range stmts {
		switch t := stmt.(type) {
		case *ast.Return:
			// RETURN is handled by projecting a single column with the expression
			// that is being returned.
			returnScalar := b.buildPLpgSQLExpr(t.Expr, b.returnType, s)
			b.addBarrierIfVolatile(s, returnScalar)
			returnColName := scopeColName("").WithMetadataName(b.makeIdentifier("stmt_return"))
			returnScope := s.push()
			b.ensureScopeHasExpr(returnScope)
			b.ob.synthesizeColumn(returnScope, returnColName, b.returnType, nil /* expr */, returnScalar)
			b.ob.constructProjectForScope(s, returnScope)
			return returnScope

		case *ast.Assignment:
			// Assignment (:=) is handled by projecting a new column with the same
			// name as the variable being assigned.
			s = b.addPLpgSQLAssign(s, t.Var, t.Value)
			if b.hasExceptionBlock {
				// If exception handling is required, we have to start a new
				// continuation after each variable assignment. This ensures that in the
				// event of an error, the arguments of the currently executing routine
				// will be the correct values for the variables, and can be passed to
				// the exception handler routines. Set the volatility to Volatile in
				// order to ensure that the routine is not inlined. See the
				// handleException comment for details on why this is necessary.
				catchCon := b.makeContinuation("assign_exception_block")
				catchCon.def.Volatility = volatility.Volatile
				b.appendPlpgSQLStmts(&catchCon, stmts[i+1:])
				return b.callContinuation(&catchCon, s)
			}

		case *ast.If:
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
			b.appendPlpgSQLStmts(&con, stmts[i+1:])
			b.pushContinuation(con)
			// Build each branch of the IF statement, calling the continuation
			// function at the end of construction in order to resume execution after
			// the IF block.
			thenScope := b.buildPLpgSQLStatements(t.ThenBody, s.push())
			elsifScopes := make([]*scope, len(t.ElseIfList))
			for j := range t.ElseIfList {
				elsifScopes[j] = b.buildPLpgSQLStatements(t.ElseIfList[j].Stmts, s.push())
			}
			// Note that if the ELSE body is empty, elseExpr will be equivalent to
			// executing the statements following the IF statement (it will be a call
			// to the continuation that was built above).
			elseScope := b.buildPLpgSQLStatements(t.ElseBody, s.push())
			b.popContinuation()

			// If one of the branches does not terminate, return nil to indicate a
			// non-terminal branch.
			if thenScope == nil || elseScope == nil {
				return nil
			}
			for j := range elsifScopes {
				if elsifScopes[j] == nil {
					return nil
				}
			}

			// Build a scalar CASE statement that conditionally executes each branch
			// of the IF statement as a subquery.
			cond := b.buildPLpgSQLExpr(t.Condition, types.Bool, s)
			thenScalar := b.ob.factory.ConstructSubquery(thenScope.expr, &memo.SubqueryPrivate{})
			whens := make(memo.ScalarListExpr, 0, len(t.ElseIfList)+1)
			whens = append(whens, b.ob.factory.ConstructWhen(cond, thenScalar))
			for j := range t.ElseIfList {
				elsifCond := b.buildPLpgSQLExpr(t.ElseIfList[j].Condition, types.Bool, s)
				elsifScalar := b.ob.factory.ConstructSubquery(elsifScopes[j].expr, &memo.SubqueryPrivate{})
				whens = append(whens, b.ob.factory.ConstructWhen(elsifCond, elsifScalar))
			}
			elseScalar := b.ob.factory.ConstructSubquery(elseScope.expr, &memo.SubqueryPrivate{})
			scalar := b.ob.factory.ConstructCase(memo.TrueSingleton, whens, elseScalar)

			// Return a single column that projects the result of the CASE statement.
			returnColName := scopeColName("").WithMetadataName(b.makeIdentifier("stmt_if"))
			returnScope := s.push()
			b.ensureScopeHasExpr(returnScope)
			scalar = b.coerceType(scalar, b.returnType)
			b.addBarrierIfVolatile(s, scalar)
			b.ob.synthesizeColumn(returnScope, returnColName, b.returnType, nil /* expr */, scalar)
			b.ob.constructProjectForScope(s, returnScope)
			return returnScope

		case *ast.Loop:
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
			b.appendPlpgSQLStmts(&exitCon, stmts[i+1:])
			b.pushExitContinuation(exitCon)
			loopContinuation := b.makeRecursiveContinuation("stmt_loop")
			b.pushContinuation(loopContinuation)
			b.appendPlpgSQLStmts(&loopContinuation, t.Body)
			b.popContinuation()
			b.popExitContinuation()
			return b.callContinuation(&loopContinuation, s)

		case *ast.While:
			// A WHILE LOOP is syntactic sugar for a LOOP with a conditional
			// EXIT, so it is handled by a simple rewrite:
			//
			//   WHILE [cond] LOOP
			//     [body];
			//   END LOOP;
			//   =>
			//   LOOP
			//     IF [cond] THEN
			//       [body];
			//     ELSE
			//       EXIT;
			//     END IF;
			//   END LOOP;
			//
			loop := &ast.Loop{
				Label: t.Label,
				Body: []ast.Statement{&ast.If{
					Condition: t.Condition,
					ThenBody:  t.Body,
					ElseBody:  []ast.Statement{&ast.Exit{}},
				}},
			}
			newStmts := make([]ast.Statement, 0, len(stmts))
			newStmts = append(newStmts, loop)
			newStmts = append(newStmts, stmts[i+1:]...)
			return b.buildPLpgSQLStatements(newStmts, s)

		case *ast.Exit:
			if t.Label != "" {
				panic(unimplemented.New(
					"EXIT label",
					"EXIT statement labels are not yet supported",
				))
			}
			if t.Condition != nil {
				panic(unimplemented.New(
					"EXIT WHEN",
					"conditional EXIT statements are not yet supported",
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

		case *ast.Continue:
			if t.Label != "" {
				panic(unimplemented.New(
					"CONTINUE label",
					"CONTINUE statement labels are not yet supported",
				))
			}
			if t.Condition != nil {
				panic(unimplemented.New(
					"CONTINUE WHEN",
					"conditional CONTINUE statements are not yet supported",
				))
			}
			// CONTINUE statements are handled by calling the function that executes
			// the loop body. Errors if used outside a loop.
			if con := b.getLoopContinuation(); con != nil {
				return b.callContinuation(con, s)
			} else {
				panic(pgerror.New(pgcode.Syntax, "CONTINUE cannot be used outside a loop"))
			}

		case *ast.Raise:
			// RAISE statements allow the PLpgSQL function to send an error or a
			// notice to the client. We handle these side effects by building them
			// into a separate body statement that is only executed for its side
			// effects. The remaining PLpgSQL statements then become the last body
			// statement, which returns the actual result of evaluation.
			//
			// The synchronous notice sending behavior is implemented in the
			// crdb_internal.plpgsql_raise builtin function.
			con := b.makeContinuation("_stmt_raise")
			con.def.Volatility = volatility.Volatile
			b.appendBodyStmt(&con, b.buildPLpgSQLRaise(con.s, b.getRaiseArgs(con.s, t)))
			b.appendPlpgSQLStmts(&con, stmts[i+1:])
			return b.callContinuation(&con, s)

		case *ast.Execute:
			if t.Strict {
				panic(unimplemented.NewWithIssuef(107854,
					"INTO STRICT statements are not yet implemented",
				))
			}
			if len(t.Target) > 1 {
				seenTargets := make(map[ast.Variable]struct{})
				for _, name := range t.Target {
					if _, ok := seenTargets[name]; ok {
						panic(unimplemented.New(
							"duplicate INTO target",
							"assigning to a variable more than once in the same INTO statement is not supported",
						))
					}
					seenTargets[name] = struct{}{}
				}
			}

			// Create a new continuation routine to handle executing a SQL statement.
			execCon := b.makeContinuation("_stmt_exec")
			stmtScope := b.ob.buildStmtAtRootWithScope(t.SqlStmt, nil /* desiredTypes */, execCon.s)
			if t.Target == nil {
				// When there is not INTO target, build the SQL statement into a body
				// statement that is only executed for its side effects.
				b.appendBodyStmt(&execCon, stmtScope)
				b.appendPlpgSQLStmts(&execCon, stmts[i+1:])
				return b.callContinuation(&execCon, s)
			}
			// This statement has an INTO target. Unlike the above case, we need the
			// result of executing the SQL statement, since its result is assigned to
			// the target variables. We handle this using the following steps:
			//   1. Build the PLpgSQL statements following this one into a
			//      continuation routine.
			//   2. Build the INTO statement into a continuation routine that calls
			//      the continuation from Step 1 using its output as parameters.
			//   3. Call the INTO continuation from the parent scope.
			//
			// Step 1: build a continuation for the remaining PLpgSQL statements.
			retCon := b.makeContinuation("_stmt_exec_ret")
			b.appendPlpgSQLStmts(&retCon, stmts[i+1:])

			// Ensure that the SQL statement returns at most one row.
			stmtScope.expr = b.ob.factory.ConstructLimit(
				stmtScope.expr,
				b.ob.factory.ConstructConst(tree.NewDInt(tree.DInt(1)), types.Int),
				stmtScope.makeOrderingChoice(),
			)

			// Ensure that the SQL statement returns at least one row. The RIGHT join
			// ensures that when the SQL statement returns no rows, it is extended
			// with a single row of NULL values.
			stmtScope.expr = b.ob.factory.ConstructRightJoin(
				stmtScope.expr,
				b.ob.factory.ConstructNoColsRow(),
				nil, /* on */
				memo.EmptyJoinPrivate,
			)

			// Add an optimization barrier in case the projected variables are never
			// referenced again, to prevent column-pruning rules from dropping the
			// side effects of executing the SELECT ... INTO statement.
			if stmtScope.expr.Relational().VolatilitySet.HasVolatile() {
				b.addBarrier(stmtScope)
			}

			// Step 2: build the INTO statement into a continuation routine that calls
			// the previously built continuation.
			intoScope := b.buildInto(stmtScope, t.Target)
			intoScope = b.callContinuation(&retCon, intoScope)

			// Step 3: call the INTO continuation from the parent scope.
			b.appendBodyStmt(&execCon, intoScope)
			return b.callContinuation(&execCon, s)

		case *ast.Open:
			// OPEN statements are used to create a CURSOR for the current session.
			// This is handled by calling the plpgsql_open_cursor internal builtin
			// function in a separate body statement that returns no results, similar
			// to the RAISE implementation.
			if t.Scroll == tree.Scroll {
				panic(unimplemented.NewWithIssue(77102, "DECLARE SCROLL CURSOR"))
			}
			openCon := b.makeContinuation("_stmt_open")
			openCon.def.Volatility = volatility.Volatile
			_, source, _, err := openCon.s.FindSourceProvidingColumn(b.ob.ctx, t.CurVar)
			if err != nil {
				if pgerror.GetPGCode(err) == pgcode.UndefinedColumn {
					panic(pgerror.Newf(pgcode.Syntax, "\"%s\" is not a known variable", t.CurVar))
				}
				panic(err)
			}
			if !source.(*scopeColumn).typ.Identical(types.RefCursor) {
				panic(pgerror.Newf(pgcode.DatatypeMismatch,
					"variable \"%s\" must be of type cursor or refcursor", t.CurVar,
				))
			}
			// Initialize the routine with the information needed to pipe the first
			// body statement into a cursor.
			query := b.resolveOpenQuery(t)
			fmtCtx := b.ob.evalCtx.FmtCtx(tree.FmtSimple)
			fmtCtx.FormatNode(query)
			openCon.def.CursorDeclaration = &tree.RoutineOpenCursor{
				NameArgIdx: source.(*scopeColumn).getParamOrd(),
				Scroll:     t.Scroll,
				CursorSQL:  fmtCtx.CloseAndGetString(),
			}
			openScope := b.ob.buildStmtAtRootWithScope(query, nil /* desiredTypes */, openCon.s)
			if openScope.expr.Relational().CanMutate {
				// Cursors with mutations are invalid.
				panic(pgerror.Newf(pgcode.FeatureNotSupported,
					"DECLARE CURSOR must not contain data-modifying statements in WITH",
				))
			}
			b.appendBodyStmt(&openCon, openScope)
			b.appendPlpgSQLStmts(&openCon, stmts[i+1:])

			// Build a statement to generate a unique name for the cursor if one
			// was not supplied. Add this to its own volatile routine to ensure that
			// the name generation isn't reordered with other operations. Use the
			// resulting projected column as input to the OPEN continuation.
			nameCon := b.makeContinuation("_gen_cursor_name")
			nameCon.def.Volatility = volatility.Volatile
			nameScope := b.buildCursorNameGen(&nameCon, t.CurVar)
			b.appendBodyStmt(&nameCon, b.callContinuation(&openCon, nameScope))
			return b.callContinuation(&nameCon, s)

		case *ast.Close:
			// CLOSE statements close the cursor with the name supplied by a PLpgSQL
			// variable. The crdb_internal.plpgsql_close builtin function handles
			// closing the cursor. Build a volatile (non-inlinable) continuation
			// that calls the builtin function.
			closeCon := b.makeContinuation("_stmt_close")
			closeCon.def.Volatility = volatility.Volatile
			const closeFnName = "crdb_internal.plpgsql_close"
			props, overloads := builtinsregistry.GetBuiltinProperties(closeFnName)
			if len(overloads) != 1 {
				panic(errors.AssertionFailedf("expected one overload for %s", closeFnName))
			}
			_, source, _, err := closeCon.s.FindSourceProvidingColumn(b.ob.ctx, t.CurVar)
			if err != nil {
				if pgerror.GetPGCode(err) == pgcode.UndefinedColumn {
					panic(pgerror.Newf(pgcode.Syntax, "\"%s\" is not a known variable", t.CurVar))
				}
				panic(err)
			}
			if !source.(*scopeColumn).typ.Identical(types.RefCursor) {
				panic(pgerror.Newf(pgcode.DatatypeMismatch,
					"variable \"%s\" must be of type cursor or refcursor", t.CurVar,
				))
			}
			closeCall := b.ob.factory.ConstructFunction(
				memo.ScalarListExpr{b.ob.factory.ConstructVariable(source.(*scopeColumn).id)},
				&memo.FunctionPrivate{
					Name:       closeFnName,
					Typ:        types.Int,
					Properties: props,
					Overload:   &overloads[0],
				},
			)
			closeColName := scopeColName("").WithMetadataName(b.makeIdentifier("stmt_close"))
			closeScope := closeCon.s.push()
			b.ensureScopeHasExpr(closeScope)
			b.ob.synthesizeColumn(closeScope, closeColName, types.Int, nil /* expr */, closeCall)
			b.ob.constructProjectForScope(closeCon.s, closeScope)
			b.appendBodyStmt(&closeCon, closeScope)
			b.appendPlpgSQLStmts(&closeCon, stmts[i+1:])
			return b.callContinuation(&closeCon, s)

		case *ast.Fetch:
			// FETCH and MOVE statements are used to shift the position of a SQL
			// cursor and (for FETCH statements) retrieve a row from the cursor and
			// assign it to one or more PLpgSQL variables. MOVE statements have no
			// result, only side effects, so they are built into a separate body
			// statement. FETCH statements can mutate PLpgSQL variables, so they are
			// handled similarly to SELECT ... INTO statements - see above.
			//
			// All cursor interactions are handled by the crdb_internal.plpgsql_fetch
			// builtin function.
			if !t.IsMove {
				if t.Cursor.FetchType == tree.FetchAll || t.Cursor.FetchType == tree.FetchBackwardAll {
					panic(pgerror.New(
						pgcode.FeatureNotSupported, "FETCH statement cannot return multiple rows",
					))
				}
			}
			fetchCon := b.makeContinuation("_stmt_fetch")
			fetchCon.def.Volatility = volatility.Volatile
			fetchScope := b.buildFetch(fetchCon.s, t)
			if t.IsMove {
				b.appendBodyStmt(&fetchCon, fetchScope)
				b.appendPlpgSQLStmts(&fetchCon, stmts[i+1:])
				return b.callContinuation(&fetchCon, s)
			}
			// crdb_internal.plpgsql_fetch will return a tuple with the results of the
			// FETCH call. Project each element as a PLpgSQL variable. The number of
			// elements returned is equal to the length of the target list
			// (padded with NULLs), so we can assume each target variable has a
			// corresponding element.
			fetchCol := fetchScope.cols[0].id
			intoScope := fetchScope.push()
			for j := range t.Target {
				typ := b.resolveVariableForAssign(t.Target[j])
				colName := scopeColName(t.Target[j])
				scalar := b.ob.factory.ConstructColumnAccess(
					b.ob.factory.ConstructVariable(fetchCol),
					memo.TupleOrdinal(j),
				)
				scalar = b.coerceType(scalar, typ)
				b.ob.synthesizeColumn(intoScope, colName, typ, nil /* expr */, scalar)
			}
			b.ob.constructProjectForScope(fetchScope, intoScope)

			// Add a barrier in case the projected variables are never referenced
			// again, to prevent column-pruning rules from removing the FETCH.
			b.addBarrier(intoScope)

			// Call a continuation for the remaining PLpgSQL statements from the newly
			// built statement that has updated variables. Then, call the fetch
			// continuation from the parent scope.
			retCon := b.makeContinuation("_stmt_exec_ret")
			b.appendPlpgSQLStmts(&retCon, stmts[i+1:])
			intoScope = b.callContinuation(&retCon, intoScope)
			b.appendBodyStmt(&fetchCon, intoScope)
			return b.callContinuation(&fetchCon, s)

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

// resolveOpenQuery finds and validates the query that is bound to cursor for
// the given OPEN statement.
func (b *plpgsqlBuilder) resolveOpenQuery(open *ast.Open) tree.Statement {
	var boundStmt tree.Statement
	for name := range b.cursors {
		if open.CurVar == name {
			boundStmt = b.cursors[name].Query
			break
		}
	}
	stmt := open.Query
	if stmt != nil && boundStmt != nil {
		// A bound cursor cannot be opened with "OPEN FOR" syntax.
		panic(errors.WithHintf(
			pgerror.New(pgcode.Syntax, "syntax error at or near \"FOR\""),
			"cannot specify a query during OPEN for bound cursor \"%s\"", open.CurVar,
		))
	}
	if stmt == nil && boundStmt == nil {
		// The query was not specified either during cursor declaration or in the
		// open statement.
		panic(errors.WithHintf(
			pgerror.New(pgcode.Syntax, "expected \"FOR\" at or near \"OPEN\""),
			"no query was specified for cursor \"%s\"", open.CurVar,
		))
	}
	if stmt == nil {
		// This is a bound cursor.
		stmt = boundStmt
	}
	if _, ok := stmt.(*tree.Select); !ok {
		panic(pgerror.Newf(
			pgcode.InvalidCursorDefinition, "cannot open %s query as cursor", stmt.StatementTag(),
		))
	}
	return stmt
}

// buildCursorNameGen builds a statement that generates a unique name for the
// cursor if the variable containing the name is unset. The unique name
// generation is implemented by the crdb_internal.plpgsql_gen_cursor_name
// builtin function.
func (b *plpgsqlBuilder) buildCursorNameGen(nameCon *continuation, nameVar ast.Variable) *scope {
	_, source, _, _ := nameCon.s.FindSourceProvidingColumn(b.ob.ctx, nameVar)
	const nameFnName = "crdb_internal.plpgsql_gen_cursor_name"
	props, overloads := builtinsregistry.GetBuiltinProperties(nameFnName)
	if len(overloads) != 1 {
		panic(errors.AssertionFailedf("expected one overload for %s", nameFnName))
	}
	nameCall := b.ob.factory.ConstructFunction(
		memo.ScalarListExpr{b.ob.factory.ConstructVariable(source.(*scopeColumn).id)},
		&memo.FunctionPrivate{
			Name:       nameFnName,
			Typ:        types.RefCursor,
			Properties: props,
			Overload:   &overloads[0],
		},
	)
	nameScope := nameCon.s.push()
	b.ob.synthesizeColumn(nameScope, scopeColName(nameVar), types.RefCursor, nil /* expr */, nameCall)
	b.ob.constructProjectForScope(nameCon.s, nameScope)
	return nameScope
}

// addPLpgSQLAssign adds a PL/pgSQL assignment to the current scope as a
// new column with the variable name that projects the assigned expression.
// If there is a column with the same name in the previous scope, it will be
// replaced. This allows the plpgsqlBuilder to model variable mutations.
func (b *plpgsqlBuilder) addPLpgSQLAssign(inScope *scope, ident ast.Variable, val ast.Expr) *scope {
	typ := b.resolveVariableForAssign(ident)
	assignScope := inScope.push()
	b.ensureScopeHasExpr(assignScope)
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
	// Project the assignment as a new column. If the projected expression is
	// volatile, add barriers before and after the projection to prevent optimizer
	// rules from reordering or removing its side effects.
	colName := scopeColName(ident)
	scalar := b.buildPLpgSQLExpr(val, typ, inScope)
	b.addBarrierIfVolatile(inScope, scalar)
	b.ob.synthesizeColumn(assignScope, colName, typ, nil, scalar)
	b.ob.constructProjectForScope(inScope, assignScope)
	b.addBarrierIfVolatile(assignScope, scalar)
	return assignScope
}

// buildInto handles the mapping from the columns of a SQL statement to the
// variables in an INTO target.
func (b *plpgsqlBuilder) buildInto(stmtScope *scope, target []ast.Variable) *scope {
	var targetTypes []*types.T
	var targetNames []ast.Variable
	if b.targetIsRecordVar(target) {
		// For a single record-type variable, the SQL statement columns are assigned
		// as elements of the variable, rather than the variable itself.
		targetTypes = b.resolveVariableForAssign(target[0]).TupleContents()
	} else {
		targetNames = target
		targetTypes = make([]*types.T, len(target))
		for j := range target {
			targetTypes[j] = b.resolveVariableForAssign(target[j])
		}
	}

	// For each target, project an output column that aliases the
	// corresponding column from the SQL statement. Previous values for the
	// variables will naturally be "overwritten" by the projection, since
	// input columns are always considered before outer columns when resolving
	// a column reference.
	intoScope := stmtScope.push()
	for j, typ := range targetTypes {
		var colName scopeColumnName
		if targetNames != nil {
			colName = scopeColName(targetNames[j])
		}
		var scalar opt.ScalarExpr
		if j < len(stmtScope.cols) {
			scalar = b.ob.factory.ConstructVariable(stmtScope.cols[j].id)
		} else {
			// If there are less output columns than target variables, NULL is
			// assigned to any remaining targets.
			scalar = b.ob.factory.ConstructConstVal(tree.DNull, typ)
		}
		scalar = b.coerceType(scalar, typ)
		b.ob.synthesizeColumn(intoScope, colName, typ, nil /* expr */, scalar)
	}
	b.ob.constructProjectForScope(stmtScope, intoScope)
	if b.targetIsRecordVar(target) {
		// Handle a single record-type variable (see projectRecordVar for details).
		intoScope = b.projectRecordVar(intoScope, target[0])
	}
	return intoScope
}

// buildPLpgSQLRaise builds a call to the crdb_internal.plpgsql_raise builtin
// function, which implements the notice-sending behavior of RAISE statements.
func (b *plpgsqlBuilder) buildPLpgSQLRaise(inScope *scope, args memo.ScalarListExpr) *scope {
	const raiseFnName = "crdb_internal.plpgsql_raise"
	props, overloads := builtinsregistry.GetBuiltinProperties(raiseFnName)
	if len(overloads) != 1 {
		panic(errors.AssertionFailedf("expected one overload for %s", raiseFnName))
	}
	raiseCall := b.ob.factory.ConstructFunction(
		args,
		&memo.FunctionPrivate{
			Name:       raiseFnName,
			Typ:        types.Int,
			Properties: props,
			Overload:   &overloads[0],
		},
	)
	raiseColName := scopeColName("").WithMetadataName(b.makeIdentifier("stmt_raise"))
	raiseScope := inScope.push()
	b.ensureScopeHasExpr(raiseScope)
	b.ob.synthesizeColumn(raiseScope, raiseColName, types.Int, nil /* expr */, raiseCall)
	b.ob.constructProjectForScope(inScope, raiseScope)
	return raiseScope
}

// getRaiseArgs validates the options attached to the given PLpgSQL RAISE
// statement and returns the arguments to be used for a call to the
// crdb_internal.plpgsql_raise builtin function.
func (b *plpgsqlBuilder) getRaiseArgs(s *scope, raise *ast.Raise) memo.ScalarListExpr {
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
		panic(errors.AssertionFailedf("unexpected log level %s", raise.LogLevel))
	}
	// Retrieve the message, if it was set with the format syntax.
	if raise.Message != "" {
		message = b.makeRaiseFormatMessage(s, raise.Message, raise.Params)
	}
	if raise.Code != "" {
		if !pgcode.IsValidPGCode(raise.Code) {
			panic(pgerror.Newf(pgcode.Syntax, "invalid SQLSTATE code '%s'", raise.Code))
		}
		code = makeConstStr(raise.Code)
	} else if raise.CodeName != "" {
		if _, ok := pgcode.PLpgSQLConditionNameToCode[raise.CodeName]; !ok {
			panic(pgerror.Newf(
				pgcode.UndefinedObject, "unrecognized exception condition \"%s\"", raise.CodeName,
			))
		}
		code = makeConstStr(raise.CodeName)
	}
	// Retrieve the RAISE options, if any.
	buildOptionExpr := func(name string, expr ast.Expr, isDup bool) opt.ScalarExpr {
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
	s *scope, format string, args []ast.Expr,
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
					panic(pgerror.Newf(pgcode.Syntax, "too few parameters specified for RAISE"))
				}
				// If the argument is NULL, postgres prints "<NULL>".
				expr := &tree.CastExpr{Expr: args[argIdx], Type: types.String}
				arg := b.buildPLpgSQLExpr(expr, types.String, s)
				arg = b.ob.factory.ConstructCoalesce(memo.ScalarListExpr{arg, makeConstStr("<NULL>")})
				addToResult(arg)
				argIdx++
			}
			addToResult(makeConstStr(paramSubstr))
		}
	}
	if argIdx < len(args) {
		panic(pgerror.Newf(pgcode.Syntax, "too many parameters specified for RAISE"))
	}
	return result
}

// buildExceptions builds the ExceptionBlock for a PLpgSQL routine as a list of
// matchable error codes and routine definitions that handle each matched error.
// The exception handler is set for the top-level block routine. All child
// sub-routines of the block routine will use the same exception handler through
// the shared BlockState.
//
// Note that the variable declarations are not within the body of the block
// routine; this is because the declaration block is not within the scope of the
// exception block.
//
// The exception handler must observe up-to-date values for the PLpgSQL
// variables, so a new continuation routine must be created for all body
// statements following an assignment statement. This works because if an error
// occurs before the assignment continuation is called, it must have happened
// logically during or before the assignment statement completed. Therefore, the
// assignment did not succeed and the previous values for the variables should
// be used. If the error occurs after the assignment continuation is called, the
// continuation will have access to the updated value from the assignment, and
// can supply it to the exception handler. Consider the following example:
//
//		 CREATE TABLE t (x INT PRIMARY KEY);
//
//		 CREATE FUNCTION f() RETURNS INT AS $$
//			  DECLARE
//			    i INT = 0;
//			  BEGIN
//			    INSERT INTO t VALUES (i); --Insert 1
//			    i := 1;
//			    INSERT INTO t VALUES (i); --Insert 2
//	        i := 2;
//			    RETURN -1;
//			  EXCEPTION WHEN unique_violation THEN
//			    RETURN i;
//		    END
//		 $$ LANGUAGE PLpgSQL;
//
// We'll build the following continuations to handle the assignment statements:
//
//		 Continuation 1 (called by initial scope):
//		   --Initial: i = 0
//			 INSERT INTO t VALUES (i); --Insert 1
//		   i := 1;
//		 Continuation 2 (called by continuation 1):
//		   --Initial: i = 1
//			 INSERT INTO t VALUES (i); --Insert 2
//	     i := 2;
//		 Continuation 3 (called by continuation 2):
//		   --Initial: i = 2
//			 RETURN -1;
//
// Consider what happens if Insert 1 fails with a uniqueness violation. The body
// of Continuation 1 will result in the error, and Continuation 1 will match
// that error against the exception handler. It will then invoke the handler
// *with its own arguments* - in this case, i=0. The handler will then return
// 0 as the result of the routine.
//
// If Insert 1 succeeds and Insert 2 fails, Continuation 1 will successfully
// evaluate and call into Continuation 2 with the updated value of i=1. When
// Continuation 2 calls the exception handler, once again it will use its own
// argument i=1.
//
// If Insert 2 succeeds as well, it will project the new value i=2 and pass it
// to Continuation 3. However, Continuation 3 does not use the variable i and
// cannot throw an exception, and so the "i := 2" assignment will never become
// visible.
//
// The block and assignment continuations must be volatile to prevent inlining.
// The presence of an exception handler does not impose restrictions on inlining
// for other continuations.
func (b *plpgsqlBuilder) buildExceptions(block *ast.Block) *memo.ExceptionBlock {
	if len(block.Exceptions) == 0 {
		return nil
	}
	codes := make([]pgcode.Code, 0, len(block.Exceptions))
	handlers := make([]*memo.UDFDefinition, 0, len(block.Exceptions))
	addHandler := func(codeStr string, handler *memo.UDFDefinition) {
		code := pgcode.MakeCode(strings.ToUpper(codeStr))
		switch code {
		case pgcode.TransactionRollback, pgcode.TransactionIntegrityConstraintViolation,
			pgcode.SerializationFailure, pgcode.StatementCompletionUnknown,
			pgcode.DeadlockDetected:
			panic(unimplemented.NewWithIssue(111446,
				"catching a Transaction Retry error in a PLpgSQL EXCEPTION block is not yet implemented",
			))
		}
		codes = append(codes, code)
		handlers = append(handlers, handler)
	}
	for _, e := range block.Exceptions {
		handlerCon := b.makeContinuation("exception_handler")
		b.appendPlpgSQLStmts(&handlerCon, e.Action)
		handlerCon.def.Volatility = volatility.Volatile
		for _, cond := range e.Conditions {
			if cond.SqlErrState != "" {
				if !pgcode.IsValidPGCode(cond.SqlErrState) {
					panic(pgerror.Newf(pgcode.Syntax, "invalid SQLSTATE code '%s'", cond.SqlErrState))
				}
				addHandler(cond.SqlErrState, handlerCon.def)
				continue
			}
			// The match condition was supplied by name instead of code.
			if strings.ToUpper(cond.SqlErrName) == "OTHERS" {
				// The special "OTHERS" condition matches (almost) any error code.
				addHandler("OTHERS" /* codeStr */, handlerCon.def)
				continue
			}
			branchCodes, ok := pgcode.PLpgSQLConditionNameToCode[cond.SqlErrName]
			if !ok {
				panic(pgerror.Newf(
					pgcode.UndefinedObject, "unrecognized exception condition \"%s\"", cond.SqlErrName,
				))
			}
			for i := range branchCodes {
				addHandler(branchCodes[i], handlerCon.def)
			}
		}
	}
	b.hasExceptionBlock = true
	return &memo.ExceptionBlock{
		Codes:   codes,
		Actions: handlers,
	}
}

// buildEndOfFunctionRaise builds a RAISE statement that throws an error when
// control reaches the end of a PLpgSQL routine without reaching a RETURN
// statement.
func (b *plpgsqlBuilder) buildEndOfFunctionRaise(inScope *scope) *scope {
	makeConstStr := func(str string) opt.ScalarExpr {
		return b.ob.factory.ConstructConstVal(tree.NewDString(str), types.String)
	}
	args := memo.ScalarListExpr{
		makeConstStr("ERROR"), /* severity */
		makeConstStr("control reached end of function without RETURN"), /* message */
		makeConstStr(""), /* detail */
		makeConstStr(""), /* hint */
		makeConstStr(pgcode.RoutineExceptionFunctionExecutedNoReturnStatement.String()), /* code */
	}
	con := b.makeContinuation("_end_of_function")
	con.def.Volatility = volatility.Volatile
	b.appendBodyStmt(&con, b.buildPLpgSQLRaise(con.s, args))
	// Build a dummy statement that returns NULL. It won't be executed, but
	// ensures that the continuation routine's return type is correct.
	eofColName := scopeColName("").WithMetadataName(b.makeIdentifier("end_of_function"))
	eofScope := con.s.push()
	typedNull := b.ob.factory.ConstructNull(b.returnType)
	b.ob.synthesizeColumn(eofScope, eofColName, b.returnType, nil /* expr */, typedNull)
	b.ob.constructProjectForScope(inScope, eofScope)
	b.appendBodyStmt(&con, eofScope)
	return b.callContinuation(&con, inScope)
}

// buildFetch projects a call to the crdb_internal.plpgsql_fetch builtin
// function, which handles cursors for the PLpgSQL FETCH and MOVE statements.
func (b *plpgsqlBuilder) buildFetch(s *scope, fetch *ast.Fetch) *scope {
	const fetchFnName = "crdb_internal.plpgsql_fetch"
	props, overloads := builtinsregistry.GetBuiltinProperties(fetchFnName)
	if len(overloads) != 1 {
		panic(errors.AssertionFailedf("expected one overload for %s", fetchFnName))
	}
	_, source, _, err := s.FindSourceProvidingColumn(b.ob.ctx, fetch.Cursor.Name)
	if err != nil {
		if pgerror.GetPGCode(err) == pgcode.UndefinedColumn {
			panic(pgerror.Newf(pgcode.Syntax, "\"%s\" is not a known variable", fetch.Cursor.Name))
		}
		panic(err)
	}
	if !source.(*scopeColumn).typ.Identical(types.RefCursor) {
		panic(pgerror.Newf(pgcode.DatatypeMismatch,
			"variable \"%s\" must be of type cursor or refcursor", fetch.Cursor.Name,
		))
	}
	makeConst := func(val tree.Datum, typ *types.T) opt.ScalarExpr {
		return b.ob.factory.ConstructConstVal(val, typ)
	}
	// For a FETCH statement, we have to pass the expected result types.
	var typs []*types.T
	if !fetch.IsMove {
		if b.targetIsRecordVar(fetch.Target) {
			// If the target is a single record-type variable, the columns of the
			// FETCH are assigned as its *elements*, rather than directly to the
			// variable.
			typs = b.resolveVariableForAssign(fetch.Target[0]).TupleContents()
		} else {
			typs = make([]*types.T, len(fetch.Target))
			for i := range fetch.Target {
				typ := b.resolveVariableForAssign(fetch.Target[i])
				typs[i] = typ
			}
		}
	}
	returnType := types.MakeTuple(typs)
	elems := make(memo.ScalarListExpr, len(typs))
	for i := range elems {
		elems[i] = b.ob.factory.ConstructConstVal(tree.DNull, typs[i])
	}

	// The arguments are:
	//   1. The name of the cursor (resolved at runtime).
	//   2. The direction of the cursor (FIRST, RELATIVE).
	//   3. The count of the cursor direction (FORWARD 1, RELATIVE 5).
	//   4. The types of the columns to return (can be empty).
	// The result of the fetch will be cast to strings and returned as an array.
	fetchCall := b.ob.factory.ConstructFunction(
		memo.ScalarListExpr{
			b.ob.factory.ConstructVariable(source.(*scopeColumn).id),
			makeConst(tree.NewDInt(tree.DInt(fetch.Cursor.FetchType)), types.Int),
			makeConst(tree.NewDInt(tree.DInt(fetch.Cursor.Count)), types.Int),
			b.ob.factory.ConstructTuple(elems, returnType),
		},
		&memo.FunctionPrivate{
			Name:       fetchFnName,
			Typ:        returnType,
			Properties: props,
			Overload:   &overloads[0],
		},
	)
	b.addBarrierIfVolatile(s, fetchCall)
	fetchColName := scopeColName("").WithMetadataName(b.makeIdentifier("stmt_fetch"))
	fetchScope := s.push()
	b.ob.synthesizeColumn(fetchScope, fetchColName, returnType, nil /* expr */, fetchCall)
	b.ob.constructProjectForScope(s, fetchScope)
	if !fetch.IsMove && b.targetIsRecordVar(fetch.Target) {
		// Handle a single record-type variable (see projectRecordVar for details).
		fetchScope = b.projectRecordVar(fetchScope, fetch.Target[0])
	}
	return fetchScope
}

// targetIsSingleCompositeVar returns true if the given INTO target is a single
// RECORD-type variable.
func (b *plpgsqlBuilder) targetIsRecordVar(target []ast.Variable) bool {
	return len(target) == 1 && b.resolveVariableForAssign(target[0]).Family() == types.TupleFamily
}

// projectRecordVar handles the special case when a single RECORD-type variable
// is the target of an INTO clause or FETCH statement. In this case, the columns
// from the SQL statement (or FETCH) should be wrapped into a tuple, which is
// assigned to the RECORD-type variable.
func (b *plpgsqlBuilder) projectRecordVar(s *scope, name ast.Variable) *scope {
	typ := b.resolveVariableForAssign(name)
	recordScope := s.push()
	elems := make(memo.ScalarListExpr, len(s.cols))
	for j := range elems {
		elems[j] = b.ob.factory.ConstructVariable(s.cols[j].id)
	}
	tuple := b.ob.factory.ConstructTuple(elems, typ)
	col := b.ob.synthesizeColumn(recordScope, scopeColName(name), typ, nil /* expr */, tuple)
	recordScope.expr = b.ob.constructProject(s.expr, []scopeColumn{*col})
	return recordScope
}

// makeContinuation allocates a new continuation routine with an uninitialized
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
			BlockState:        b.blockState,
			RoutineType:       tree.UDFRoutine,
		},
		s: s,
	}
}

// makeRecursiveContinuation allocates a new continuation routine that can
// recursively invoke itself.
func (b *plpgsqlBuilder) makeRecursiveContinuation(name string) continuation {
	con := b.makeContinuation(name)
	con.def.IsRecursive = true
	return con
}

// appendBodyStmt adds a body statement to the definition of a continuation
// function. Only the last body statement will return results; all others will
// only be executed for their side effects (e.g. RAISE statement).
//
// appendBodyStmt is separate from makeContinuation to allow recursive routine
// definitions, which need to push the continuation before it is finished. The
// separation also allows for appending multiple body statements.
func (b *plpgsqlBuilder) appendBodyStmt(con *continuation, bodyScope *scope) {
	// Set the volatility of the continuation routine to the least restrictive
	// volatility level in the Relational properties of the body statements.
	vol := bodyScope.expr.Relational().VolatilitySet.ToVolatility()
	if con.def.Volatility < vol {
		con.def.Volatility = vol
	}
	con.def.Body = append(con.def.Body, bodyScope.expr)
	con.def.BodyProps = append(con.def.BodyProps, bodyScope.makePhysicalProps())
}

// appendPlpgSQLStmts builds the given PLpgSQL statements into a relational
// expression and appends it to the given continuation routine's body statements
// list.
func (b *plpgsqlBuilder) appendPlpgSQLStmts(con *continuation, stmts []ast.Statement) {
	// Make sure to push s before constructing the continuation scope to ensure
	// that the parameter columns are not projected.
	continuationScope := b.buildPLpgSQLStatements(stmts, con.s.push())
	b.appendBodyStmt(con, continuationScope)
}

// callContinuation adds a column that projects the result of calling the
// given continuation function.
func (b *plpgsqlBuilder) callContinuation(con *continuation, s *scope) *scope {
	if con == nil {
		return b.buildEndOfFunctionRaise(s)
	}
	args := make(memo.ScalarListExpr, 0, len(b.decls)+len(b.params))
	addArg := func(name tree.Name, typ *types.T) {
		_, source, _, err := s.FindSourceProvidingColumn(b.ob.ctx, name)
		if err != nil {
			panic(err)
		}
		args = append(args, b.ob.factory.ConstructVariable(source.(*scopeColumn).id))
	}
	for _, dec := range b.decls {
		addArg(dec.Var, b.varTypes[dec.Var])
	}
	for _, param := range b.params {
		addArg(tree.Name(param.Name), param.Typ)
	}
	// PLpgSQL continuation routines are always in tail-call position.
	call := b.ob.factory.ConstructUDFCall(args, &memo.UDFCallPrivate{Def: con.def, TailCall: true})
	b.addBarrierIfVolatile(s, call)

	returnColName := scopeColName("").WithMetadataName(con.def.Name)
	returnScope := s.push()
	b.ensureScopeHasExpr(returnScope)
	b.ob.synthesizeColumn(returnScope, returnColName, b.returnType, nil /* expr */, call)
	b.ob.constructProjectForScope(s, returnScope)
	return returnScope
}

// addBarrierIfVolatile checks if the given expression is volatile, and adds an
// optimization barrier to the given scope if it is. This should be used before
// projecting an expression within an existing scope. It is used to prevent
// side effects from being duplicated, eliminated, or reordered.
func (b *plpgsqlBuilder) addBarrierIfVolatile(s *scope, expr opt.ScalarExpr) {
	if s.expr.Relational().OutputCols.Empty() && s.expr.Relational().Cardinality.IsOne() {
		// As an optimization, don't add a barrier for the common case when the
		// input is a dummy expression that returns no columns and exactly one row.
		return
	}
	var p props.Shared
	memo.BuildSharedProps(expr, &p, b.ob.evalCtx)
	if p.VolatilitySet.HasVolatile() {
		b.addBarrier(s)
	}
}

// addBarrier adds an optimization barrier to the given scope, in order to
// prevent side effects from being duplicated, eliminated, or reordered.
func (b *plpgsqlBuilder) addBarrier(s *scope) {
	s.expr = b.ob.factory.ConstructBarrier(s.expr)
}

// buildPLpgSQLExpr parses and builds the given SQL expression into a ScalarExpr
// within the given scope.
func (b *plpgsqlBuilder) buildPLpgSQLExpr(expr ast.Expr, typ *types.T, s *scope) opt.ScalarExpr {
	expr, _ = tree.WalkExpr(s, expr)
	typedExpr, err := expr.TypeCheck(b.ob.ctx, b.ob.semaCtx, typ)
	if err != nil {
		panic(err)
	}
	scalar := b.ob.buildScalar(typedExpr, s, nil, nil, b.colRefs)
	return b.coerceType(scalar, typ)
}

// coerceType implements PLpgSQL type-coercion behavior.
func (b *plpgsqlBuilder) coerceType(scalar opt.ScalarExpr, typ *types.T) opt.ScalarExpr {
	resolved := scalar.DataType()
	if !resolved.Identical(typ) {
		// Postgres will attempt to coerce the expression's type with an assignment
		// cast. If that fails, it will convert to a string and attempt to parse the
		// string as the desired type.
		//
		// Note that we intentionally use an explicit cast instead of an assignment
		// cast here. This is because postgres does not error for narrowing type
		// coercion, but instead performs the cast without truncation. Using an
		// explicit cast, we also allow narrowing type coercion, but with
		// truncation. This difference is tracked in #115385.
		if !cast.ValidCast(resolved, typ, cast.ContextAssignment) {
			if !cast.ValidCast(types.String, typ, cast.ContextExplicit) {
				panic(pgerror.Newf(pgcode.DatatypeMismatch,
					"unable to coerce type %s to %s", resolved.Name(), typ.Name(),
				))
			}
			scalar = b.ob.factory.ConstructCast(scalar, types.String)
		}
		scalar = b.ob.factory.ConstructCast(scalar, typ)
	}
	return scalar
}

// resolveVariableForAssign attempts to retrieve the type of the variable with
// the given name, throwing an error if no such variable exists.
func (b *plpgsqlBuilder) resolveVariableForAssign(name tree.Name) *types.T {
	typ, ok := b.varTypes[name]
	if !ok {
		panic(pgerror.Newf(pgcode.Syntax, "\"%s\" is not a known variable", name))
	}
	if b.constants != nil {
		if _, ok := b.constants[name]; ok {
			panic(pgerror.Newf(pgcode.ErrorInAssignment, "variable \"%s\" is declared CONSTANT", name))
		}
	}
	return typ
}

func (b *plpgsqlBuilder) ensureScopeHasExpr(s *scope) {
	if s.expr == nil {
		s.expr = b.ob.factory.ConstructNoColsRow()
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
		if b.continuations[i].def.IsRecursive {
			return &b.continuations[i]
		}
	}
	return nil
}

func (b *plpgsqlBuilder) addVariableType(name tree.Name, typ *types.T) {
	if _, ok := b.varTypes[name]; ok {
		panic(errors.WithHintf(
			unimplemented.NewWithIssue(117508, "variable shadowing is not yet implemented"),
			"variable \"%s\" shadows a previously defined variable", name,
		))
	}
	b.varTypes[name] = typ
}

// recordTypeVisitor is used to infer the concrete return type for a
// record-returning PLpgSQL routine. It visits each return statement and checks
// that the types of all returned expressions are either identical or UNKNOWN.
type recordTypeVisitor struct {
	ctx     context.Context
	semaCtx *tree.SemaContext
	s       *scope
	typ     *types.T
}

func newRecordTypeVisitor(
	ctx context.Context, semaCtx *tree.SemaContext, s *scope,
) *recordTypeVisitor {
	return &recordTypeVisitor{ctx: ctx, semaCtx: semaCtx, s: s, typ: types.Unknown}
}

var _ ast.StatementVisitor = &recordTypeVisitor{}

func (r *recordTypeVisitor) Visit(stmt ast.Statement) (newStmt ast.Statement, changed bool) {
	if retStmt, ok := stmt.(*ast.Return); ok {
		desired := types.Any
		if r.typ != types.Unknown {
			desired = r.typ
		}
		expr, _ := tree.WalkExpr(r.s, retStmt.Expr)
		typedExpr, err := expr.TypeCheck(r.ctx, r.semaCtx, desired)
		if err != nil {
			panic(err)
		}
		typ := typedExpr.ResolvedType()
		if typ == types.Unknown {
			return stmt, false
		}
		if typ.Family() != types.TupleFamily {
			panic(pgerror.New(pgcode.DatatypeMismatch,
				"cannot return non-composite value from function returning composite type",
			))
		}
		if r.typ == types.Unknown {
			r.typ = typ
			return stmt, false
		}
		if !typ.Identical(r.typ) {
			panic(errors.WithHint(
				unimplemented.NewWithIssue(115384,
					"returning different types from a RECORD-returning function is not yet supported",
				),
				"try casting all RETURN statements to the same type",
			))
		}
	}
	return stmt, false
}
