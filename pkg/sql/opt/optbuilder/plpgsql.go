// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	ast "github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
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
// +---------------------+
// | Lazy SQL Evaluation |
// +---------------------+
//
// Trigger functions are created before they are associated with a particular
// table by a CREATE TRIGGER statement. This means that column references within
// SQL statements and expressions cannot be resolved when the trigger function
// is created. However, it is still possible (and desirable) to validate the
// PL/pgSQL code at this time.
//
// In order to validate the PL/pgSQL during the creation of a trigger function
// without analyzing SQL statements, we replace:
//   - SQL expressions with typed NULL values, and
//   - SQL statements by a single-row VALUES operator with no columns.
//
// See also the buildSQLExpr and buildSQLStatement methods.
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

	// returnType is the return type of the PL/pgSQL routine.
	returnType *types.T

	// continuations is a stack of sub-routines that are called to resume
	// execution from a certain point within the PL/pgSQL routine. For example,
	// branches of an IF-statement will call a continuation to resume execution
	// with the statements following the IF-statement.
	//
	// Each continuation stores its context, and callers filter depending on this
	// context; see also continuationType.
	continuations []continuation

	// blocks is a stack containing every block in the path from the root block to
	// the current block. It is necessary to track the entire stack because
	// variables from a parent block can be referenced in a child block.
	blocks []plBlock

	// outParams is the set of OUT parameters for the routine.
	outParams []ast.Variable

	// outScope is the output scope for the routine. It is only used for
	// transaction control statements in procedures, which need the presentation
	// to construct a new procedure that will resume execution. Note that due to
	// OUT parameters, stored procedures resolve their return type before
	// building their body statements.
	outScope *scope

	routineName  string
	isProcedure  bool
	isDoBlock    bool
	buildSQL     bool
	identCounter int
}

// routineParam is similar to tree.RoutineParam but stores the resolved type.
type routineParam struct {
	name  ast.Variable
	typ   *types.T
	class tree.RoutineParamClass
}

func newPLpgSQLBuilder(
	ob *Builder,
	routineName, rootBlockLabel string,
	colRefs *opt.ColSet,
	routineParams []routineParam,
	returnType *types.T,
	isProcedure, isDoBlock, buildSQL bool,
	outScope *scope,
) *plpgsqlBuilder {
	const initialBlocksCap = 2
	b := &plpgsqlBuilder{
		ob:          ob,
		colRefs:     colRefs,
		returnType:  returnType,
		blocks:      make([]plBlock, 0, initialBlocksCap),
		routineName: routineName,
		isProcedure: isProcedure,
		isDoBlock:   isDoBlock,
		buildSQL:    buildSQL,
		outScope:    outScope,
	}
	// Build the initial block for the routine parameters, which are considered
	// PL/pgSQL variables.
	b.pushBlock(plBlock{
		label:    rootBlockLabel,
		vars:     make([]ast.Variable, 0, len(routineParams)),
		varTypes: make(map[ast.Variable]*types.T),
	})
	for _, param := range routineParams {
		if param.name != "" {
			// TODO(119502): unnamed parameters can only be accessed via $i
			// notation.
			b.addVariable(param.name, param.typ)
		}
		if tree.IsOutParamClass(param.class) {
			b.outParams = append(b.outParams, param.name)
		}
	}
	return b
}

// plBlock encapsulates the local state of a PL/pgSQL block, including cursor
// and variable declarations, as well the exception handler and label.
type plBlock struct {
	// label is the label provided for the block, if any. It can be used when
	// resolving a PL/pgSQL variable.
	label string

	// vars is an ordered list of variables declared in a PL/pgSQL block.
	//
	// INVARIANT: the variables of a parent (ancestor) block *always* form a
	// prefix of the variables of a child (descendant) block when creating or
	// calling a continuation.
	vars []ast.Variable

	// varTypes maps from the name of each variable in the scope to its type.
	varTypes map[ast.Variable]*types.T

	// constants tracks the variables that were declared as constant.
	constants map[ast.Variable]struct{}

	// cursors is the set of cursor declarations for a PL/pgSQL block. It is set
	// for bound cursor declarations, which allow a query to be associated with a
	// cursor before it is opened.
	cursors map[ast.Variable]ast.CursorDeclaration

	// hiddenVars is an ordered list of *hidden* variables that were not declared
	// by the user, but are used internally by the builder. Hidden variables are
	// not visible to the user, and are identified by their metadata name. They
	// can only be assigned to by directly calling assignToHiddenVariable().
	//
	// As an example, the internal counter variable for a FOR loop is a
	// hidden variable.
	//
	// INVARIANT: the hidden variables of a given block *always* follow the
	// variables when creating or calling a continuation.
	hiddenVars []string

	// hiddenVarTypes maps from each hidden variable in the scope to its type.
	hiddenVarTypes map[string]*types.T

	// hasExceptionHandler tracks whether this block has an exception handler.
	hasExceptionHandler bool

	// state is shared for all sub-routines that make up a PLpgSQL block,
	// including the implicit block that surrounds the body statements. It is used
	// for exception handling and cursor declarations. Note that the state is not
	// shared between parent and child or sibling blocks - it is unique within a
	// given block.
	state *tree.BlockState
}

// buildRootBlock builds a PL/pgSQL routine starting with the root block.
func (b *plpgsqlBuilder) buildRootBlock(
	astBlock *ast.Block, s *scope, routineParams []routineParam,
) *scope {
	// Push the scope so that the routine parameters live on a parent scope
	// instead of the current one. This indicates that the columns are "outer"
	// columns, which can be referenced but do not originate from an input
	// expression. If we don't do this, the result would be internal errors due
	// to Project expressions that try to "pass through" input columns that aren't
	// actually produced by the input expression.
	s = s.push()
	b.ensureScopeHasExpr(s)

	// Initialize OUT parameters to NULL. Note that the initial block for
	// parameters was already created in newPLpgSQLBuilder().
	for _, param := range routineParams {
		if param.class != tree.RoutineParamOut || param.name == "" {
			continue
		}
		s = b.addPLpgSQLAssign(
			s, param.name, &tree.CastExpr{Expr: tree.DNull, Type: param.typ}, noIndirection,
		)
	}
	if b.isProcedure {
		var tc transactionControlVisitor
		ast.Walk(&tc, astBlock)
		if tc.foundTxnControlStatement {
			if b.ob.insideNestedPLpgSQLCall {
				// Disallow transaction control statements in nested routines for now.
				// TODO(#122266): once we support this, make sure to validate that
				// transaction control statements are only allowed in a nested procedure
				// when all ancestors are procedures or DO blocks.
				panic(unimplemented.NewWithIssue(122266,
					"transaction control statements in nested routines",
				))
			}
			if b.isDoBlock {
				// Disallow transaction control statements in DO blocks for now.
				panic(unimplemented.NewWithIssue(138704,
					"transaction control statements in DO blocks",
				))
			}
			// Disable stable folding, since different parts of the routine can be run
			// in different transactions.
			b.ob.factory.FoldingControl().TemporarilyDisallowStableFolds(func() {
				s = b.buildBlock(astBlock, s)
			})
			return s
		}
	}
	return b.buildBlock(astBlock, s)
}

// pushNewBlock creates a new non-root block and adds it to the stack. The
// caller should use popBlock() to remove the block from the stack once it is
// out of scope.
func (b *plpgsqlBuilder) pushNewBlock(astBlock *ast.Block) *plBlock {
	if len(b.blocks) == 0 {
		// There should always be a root block for the routine parameters.
		panic(errors.AssertionFailedf("expected at least one PLpgSQL block"))
	}
	block := b.pushBlock(plBlock{
		label:     astBlock.Label,
		vars:      make([]ast.Variable, 0, len(astBlock.Decls)),
		varTypes:  make(map[ast.Variable]*types.T),
		constants: make(map[ast.Variable]struct{}),
		cursors:   make(map[ast.Variable]ast.CursorDeclaration),
	})
	if len(astBlock.Exceptions) > 0 || b.hasExceptionHandler() {
		// If the current block or some ancestor block has an exception handler, it
		// is necessary to maintain the BlockState with a reference to the parent
		// BlockState (if any).
		block.state = &tree.BlockState{}
		if parent := b.parentBlock(); parent != nil {
			block.state.Parent = parent.state
		}
	}
	return block
}

// addDeclarations adds the given variable declarations to the given block.
func (b *plpgsqlBuilder) addDeclarations(decls []ast.Statement, block *plBlock, s *scope) *scope {
	for i := range decls {
		switch dec := decls[i].(type) {
		case *ast.Declaration:
			if dec.NotNull {
				panic(notNullVarErr)
			}
			if dec.Collate != "" {
				panic(collatedVarErr)
			}
			typ, err := tree.ResolveType(b.ob.ctx, dec.Typ, b.ob.semaCtx.TypeResolver)
			if err != nil {
				panic(err)
			}
			if typ.Identical(types.AnyTuple) {
				panic(recordVarErr)
			} else if typ.IsPolymorphicType() {
				// NOTE: Postgres also returns an "unsupported" error.
				panic(pgerror.Newf(pgcode.FeatureNotSupported,
					"variable \"%s\" has pseudo-type %s", dec.Var, typ.Name(),
				))
			}
			b.addVariable(dec.Var, typ)
			if dec.Expr != nil {
				// Some variable declarations initialize the variable.
				s = b.addPLpgSQLAssign(s, dec.Var, dec.Expr, noIndirection)
			} else {
				// Uninitialized variables are null.
				s = b.addPLpgSQLAssign(
					s, dec.Var, &tree.CastExpr{Expr: tree.DNull, Type: typ}, noIndirection,
				)
			}
			if dec.Constant {
				// Add to the constants map after initializing the variable, since
				// constant variables only prevent assignment, not initialization.
				block.constants[dec.Var] = struct{}{}
			}
		case *ast.CursorDeclaration:
			// Declaration of a bound cursor declares a variable of type refcursor.
			b.addVariable(dec.Name, types.RefCursor)
			s = b.addPLpgSQLAssign(
				s, dec.Name, &tree.CastExpr{Expr: tree.DNull, Type: types.RefCursor}, noIndirection,
			)
			block.cursors[dec.Name] = *dec
		}
	}
	return s
}

// buildBlock constructs an expression that returns the result of executing a
// PL/pgSQL block, including variable declarations and exception handlers.
//
// buildBlock should only be used for non-root blocks.
func (b *plpgsqlBuilder) buildBlock(astBlock *ast.Block, s *scope) *scope {
	// Allocate a new block and add its declarations to the scope.
	b.ensureScopeHasExpr(s)
	block := b.pushNewBlock(astBlock)
	defer b.popBlock()
	s = b.addDeclarations(astBlock.Decls, block, s)

	// For a RECORD-returning routine, infer the concrete type by examining the
	// RETURN statements. This has to happen after building the declaration
	// block because RETURN statements can reference declared variables.
	if b.returnType.Identical(types.AnyTuple) {
		recordVisitor := newRecordTypeVisitor(b.ob.ctx, b.ob.semaCtx, s, astBlock)
		ast.Walk(recordVisitor, astBlock)
		if rtyp := recordVisitor.typ; rtyp == nil || rtyp.Identical(types.AnyTuple) {
			// rtyp is nil when there is no RETURN statement in this block. rtyp
			// can be AnyTuple when RETURN statement invokes a RECORD-returning
			// UDF. We currently don't support such cases.
			panic(wildcardReturnTypeErr)
		} else if rtyp.Family() != types.UnknownFamily {
			// Don't overwrite the wildcard type with Unknown one in case we
			// have other blocks that have concrete type.
			b.returnType = rtyp
		}
	}
	// Build the exception handler. This has to happen after building the variable
	// declarations, since the exception handler can reference the block's vars.
	if len(astBlock.Exceptions) > 0 {
		exceptionBlock := b.buildExceptions(astBlock)
		block.hasExceptionHandler = true

		// There is an implicit block around the body statements, with an optional
		// exception handler. Note that the variable declarations are not in block
		// scope, and exceptions thrown during variable declaration are not caught.
		//
		// The routine is volatile to prevent inlining; see the buildExceptions
		// comment for details.
		blockCon := b.makeContinuation("nested_block")
		blockCon.def.ExceptionBlock = exceptionBlock
		blockCon.def.Volatility = volatility.Volatile
		blockCon.def.BlockStart = true
		b.appendPlpgSQLStmts(&blockCon, astBlock.Body)
		return b.callContinuation(&blockCon, s)
	}
	// Finally, build the body statements for the block.
	return b.buildPLpgSQLStatements(astBlock.Body, s)
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
		case *ast.Block:
			// For a nested block, push a continuation with the remaining statements
			// before calling recursively into buildBlock. The continuation will be
			// called when the control flow within the nested block terminates.
			blockCon := b.makeContinuationWithTyp("post_nested_block", t.Label, continuationBlockExit)
			if len(t.Exceptions) > 0 {
				// If the block has an exception handler, mark the continuation as
				// volatile to prevent inlining. This is necessary to ensure that
				// transitions out of a PL/pgSQL block are correctly tracked during
				// execution. The transition *into* the block is marked volatile for the
				// same reason; see also buildBlock and buildExceptions.
				blockCon.def.Volatility = volatility.Volatile
			}
			b.appendPlpgSQLStmts(&blockCon, stmts[i+1:])
			b.pushContinuation(blockCon)
			scope := b.buildBlock(t, s)
			b.popContinuation()
			return scope

		case *ast.Return:
			// If the routine has OUT-parameters or a VOID return type, the RETURN
			// statement must have no expression. Otherwise, the RETURN statement must
			// have a non-empty expression.
			expr := t.Expr
			if b.hasOutParam() {
				if expr != nil {
					panic(returnWithOUTParameterErr)
				}
				expr = b.makeReturnForOutParams()
			} else if b.returnType.Family() == types.VoidFamily {
				if expr != nil {
					if b.isProcedure {
						panic(returnWithVoidParameterProcedureErr)
					} else {
						panic(returnWithVoidParameterErr)
					}
				}
				expr = tree.DNull
			}
			if expr == nil {
				panic(emptyReturnErr)
			}
			// RETURN is handled by projecting a single column with the expression
			// that is being returned.
			returnScalar := b.buildSQLExpr(expr, b.returnType, s)
			b.addBarrierIfVolatile(s, returnScalar)
			returnColName := scopeColName("").WithMetadataName(b.makeIdentifier("stmt_return"))
			returnScope := s.push()
			b.ob.synthesizeColumn(returnScope, returnColName, b.returnType, nil /* expr */, returnScalar)
			b.ob.constructProjectForScope(s, returnScope)
			return returnScope

		case *ast.Assignment:
			// Assignment (:=) is handled by projecting a new column with the same
			// name as the variable being assigned.
			s = b.addPLpgSQLAssign(s, t.Var, t.Value, t.Indirection)
			if b.hasExceptionHandler() {
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
			cond := b.buildSQLExpr(t.Condition, types.Bool, s)
			thenScalar := b.ob.factory.ConstructSubquery(thenScope.expr, &memo.SubqueryPrivate{})
			whens := make(memo.ScalarListExpr, 0, len(t.ElseIfList)+1)
			whens = append(whens, b.ob.factory.ConstructWhen(cond, thenScalar))
			for j := range t.ElseIfList {
				elsifCond := b.buildSQLExpr(t.ElseIfList[j].Condition, types.Bool, s)
				elsifScalar := b.ob.factory.ConstructSubquery(elsifScopes[j].expr, &memo.SubqueryPrivate{})
				whens = append(whens, b.ob.factory.ConstructWhen(elsifCond, elsifScalar))
			}
			elseScalar := b.ob.factory.ConstructSubquery(elseScope.expr, &memo.SubqueryPrivate{})
			scalar := b.ob.factory.ConstructCase(memo.TrueSingleton, whens, elseScalar)

			// Return a single column that projects the result of the CASE statement.
			returnColName := scopeColName("").WithMetadataName(b.makeIdentifier("stmt_if"))
			returnScope := s.push()
			scalar = b.coerceType(scalar, b.returnType)
			b.addBarrierIfVolatile(s, scalar)
			b.ob.synthesizeColumn(returnScope, returnColName, b.returnType, nil /* expr */, scalar)
			b.ob.constructProjectForScope(s, returnScope)
			return returnScope

		case *ast.Loop:
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
			exitCon := b.makeContinuationWithTyp("loop_exit", t.Label, continuationLoopExit)
			b.appendPlpgSQLStmts(&exitCon, stmts[i+1:])
			b.pushContinuation(exitCon)
			loopContinuation := b.makeContinuationWithTyp("stmt_loop", t.Label, continuationLoopContinue)
			loopContinuation.def.IsRecursive = true
			b.pushContinuation(loopContinuation)
			b.appendPlpgSQLStmts(&loopContinuation, t.Body)
			b.popContinuation()
			b.popContinuation()
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
			return b.buildPLpgSQLStatements(b.prependStmt(loop, stmts[i+1:]), s)

		case *ast.ForLoop:
			// Build a continuation that will resume execution after the loop.
			exitCon := b.makeContinuationWithTyp("loop_exit", t.Label, continuationLoopExit)
			b.appendPlpgSQLStmts(&exitCon, stmts[i+1:])
			switch c := t.Control.(type) {
			case *ast.IntForLoopControl:
				b.pushContinuation(exitCon)
				// FOR target IN [ REVERSE ] expr .. expr [ BY expr ] LOOP ...
				scope := b.handleIntForLoop(s, t, c)
				b.popContinuation()
				return scope
			default:
				panic(errors.WithDetail(unsupportedPLStmtErr,
					"query and cursor FOR loops are not yet supported",
				))
			}

		case *ast.Exit:
			if t.Condition != nil {
				// EXIT with a condition is syntactic sugar for EXIT inside an IF stmt.
				ifStmt := &ast.If{
					Condition: t.Condition,
					ThenBody:  []ast.Statement{&ast.Exit{Label: t.Label}},
				}
				return b.buildPLpgSQLStatements(b.prependStmt(ifStmt, stmts[i+1:]), s)
			}
			// EXIT statements are handled by calling the function that executes the
			// statements after a loop. Errors if used outside either a loop or a
			// block with a label.
			conTypes := continuationLoopExit
			if t.Label != unspecifiedLabel {
				conTypes |= continuationBlockExit
			}
			if con := b.getContinuation(conTypes, t.Label); con != nil {
				return b.callContinuation(con, s)
			}
			if t.Label == unspecifiedLabel {
				panic(exitOutsideLoopErr)
			}
			if t.Label == b.rootBlock().label {
				// An EXIT from the root block has the same handling as when the routine
				// ends with no RETURN statement.
				return b.handleEndOfFunction(s)
			}
			if t.Label == b.routineName {
				// An EXIT from the routine name itself results in an end-of-function
				// error, even for a VOID-returning routine or one with OUT-parameters.
				eofCon := b.makeContinuationWithTyp("root_exit", t.Label, continuationBlockExit)
				b.buildEndOfFunctionRaise(&eofCon)
				return b.callContinuation(&eofCon, s)
			}
			panic(pgerror.Newf(pgcode.Syntax,
				"there is no label \"%s\" attached to any block or loop enclosing this statement", t.Label,
			))

		case *ast.Continue:
			if t.Condition != nil {
				// CONTINUE with a condition is syntactic sugar for CONTINUE inside an
				// IF stmt.
				ifStmt := &ast.If{
					Condition: t.Condition,
					ThenBody:  []ast.Statement{&ast.Continue{Label: t.Label}},
				}
				return b.buildPLpgSQLStatements(b.prependStmt(ifStmt, stmts[i+1:]), s)
			}
			// CONTINUE statements are handled by calling the function that executes
			// the loop body. Errors if used outside a loop.
			conTypes := continuationLoopContinue
			if t.Label != unspecifiedLabel {
				conTypes |= continuationBlockExit
			}
			if con := b.getContinuation(conTypes, t.Label); con != nil {
				if con.typ == continuationBlockExit {
					panic(pgerror.Newf(pgcode.Syntax,
						"block label \"%s\" cannot be used in CONTINUE", t.Label,
					))
				}
				return b.callContinuation(con, s)
			}
			if t.Label == unspecifiedLabel {
				panic(continueOutsideLoopErr)
			}
			panic(pgerror.Newf(pgcode.Syntax,
				"there is no label \"%s\" attached to any block or loop enclosing this statement", t.Label,
			))

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
			b.appendBodyStmtFromScope(&con, b.buildPLpgSQLRaise(con.s, b.getRaiseArgs(con.s, t)))
			b.appendPlpgSQLStmts(&con, stmts[i+1:])
			return b.callContinuation(&con, s)

		case *ast.Execute:
			if _, ok := t.SqlStmt.(*tree.SetTransaction); ok {
				// SET TRANSACTION must happen immediately after a COMMIT or ROLLBACK
				// statement. When we handle COMMIT/ROLLBACK, we check for following
				// SET TRANSACTION statements, so encountering one here means it's in an
				// incorrect location.
				panic(setTxnNotAfterControlStmtErr)
			}
			b.checkDuplicateTargets(t.Target, "INTO")
			strict := t.Strict || b.ob.evalCtx.SessionData().PLpgSQLUseStrictInto

			// Create a new continuation routine to handle executing a SQL statement.
			execCon := b.makeContinuation("_stmt_exec")
			stmtScope := b.buildSQLStatement(t.SqlStmt, execCon.s)
			if len(t.Target) == 0 {
				// When there is no INTO target, build the SQL statement into a body
				// statement that is only executed for its side effects.
				b.appendBodyStmtFromScope(&execCon, stmtScope)
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
			limitVal := tree.DInt(1)
			if strict {
				// Increase the limit so that it's possible to check that there is
				// exactly one row.
				limitVal = tree.DInt(2)
			}
			stmtScope.expr = b.ob.factory.ConstructLimit(
				stmtScope.expr,
				b.ob.factory.ConstructConst(tree.NewDInt(limitVal), types.Int),
				stmtScope.makeOrderingChoice(),
			)

			// Add an optimization barrier in case the projected variables are never
			// referenced again, to prevent column-pruning rules from dropping the
			// side effects of executing the SELECT ... INTO statement.
			if stmtScope.expr.Relational().VolatilitySet.HasVolatile() {
				b.ob.addBarrier(stmtScope)
			}

			if strict {
				// Check that the expression produces exactly one row.
				b.addOneRowCheck(stmtScope)
			} else {
				// Ensure that the SQL statement returns at least one row. The RIGHT
				// join ensures that when the SQL statement returns no rows, it is
				// extended with a single row of NULL values.
				stmtScope.expr = b.ob.factory.ConstructRightJoin(
					stmtScope.expr,
					b.ob.factory.ConstructNoColsRow(),
					nil, /* on */
					memo.EmptyJoinPrivate,
				)
			}

			// Step 2: build the INTO statement into a continuation routine that calls
			// the previously built continuation.
			intoScope := b.buildInto(stmtScope, t.Target)
			intoScope = b.callContinuation(&retCon, intoScope)

			// Step 3: call the INTO continuation from the parent scope.
			b.appendBodyStmtFromScope(&execCon, intoScope)
			return b.callContinuation(&execCon, s)

		case *ast.Open:
			// OPEN statements are used to create a CURSOR for the current session.
			// This is handled by calling the plpgsql_open_cursor internal builtin
			// function in a separate body statement that returns no results, similar
			// to the RAISE implementation.
			if t.Scroll == tree.Scroll {
				panic(scrollableCursorErr)
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
			openScope := b.buildSQLStatement(query, openCon.s)
			if openScope.expr.Relational().CanMutate {
				// Cursors with mutations are invalid.
				panic(cursorMutationErr)
			}
			b.appendBodyStmtFromScope(&openCon, openScope)
			b.appendPlpgSQLStmts(&openCon, stmts[i+1:])

			// Build a statement to generate a unique name for the cursor if one
			// was not supplied. Add this to its own volatile routine to ensure that
			// the name generation isn't reordered with other operations. Use the
			// resulting projected column as input to the OPEN continuation.
			nameCon := b.makeContinuation("_gen_cursor_name")
			nameCon.def.Volatility = volatility.Volatile
			nameScope := b.buildCursorNameGen(&nameCon, t.CurVar)
			b.appendBodyStmtFromScope(&nameCon, b.callContinuation(&openCon, nameScope))
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
			b.ob.synthesizeColumn(closeScope, closeColName, types.Int, nil /* expr */, closeCall)
			b.ob.constructProjectForScope(closeCon.s, closeScope)
			b.appendBodyStmtFromScope(&closeCon, closeScope)
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
					panic(fetchRowsErr)
				}
			}
			b.checkDuplicateTargets(t.Target, "FETCH")
			fetchCon := b.makeContinuation("_stmt_fetch")
			fetchCon.def.Volatility = volatility.Volatile
			fetchScope := b.buildFetch(fetchCon.s, t)
			if t.IsMove {
				b.appendBodyStmtFromScope(&fetchCon, fetchScope)
				b.appendPlpgSQLStmts(&fetchCon, stmts[i+1:])
				return b.callContinuation(&fetchCon, s)
			}
			// crdb_internal.plpgsql_fetch will return a tuple with the results of the
			// FETCH call. Project each element as a PLpgSQL variable.
			//
			// Note: The number of tuple elements is equal to the length of the target
			// list (padded with NULLs), so we can assume each target variable has a
			// corresponding element.
			intoScope := b.projectTupleAsIntoTarget(fetchScope, t.Target)

			// Add a barrier in case the projected variables are never referenced
			// again, to prevent column-pruning rules from removing the FETCH.
			b.ob.addBarrier(intoScope)

			// Call a continuation for the remaining PLpgSQL statements from the newly
			// built statement that has updated variables.
			retCon := b.makeContinuation("_stmt_fetch_ret")
			b.appendPlpgSQLStmts(&retCon, stmts[i+1:])
			intoScope = b.callContinuation(&retCon, intoScope)

			// Add the built statement to the FETCH continuation.
			b.appendBodyStmtFromScope(&fetchCon, intoScope)
			return b.callContinuation(&fetchCon, s)

		case *ast.Null:
			// PL/pgSQL NULL statements are a no-op.
			continue

		case *ast.TransactionControl:
			// Transaction control statements are handled by a TxnControlExpr, which
			// wraps a continuation for the remaining statements in the routine.
			// During execution, a TxnControlExpr directs the session to commit or
			// rollback the transaction, and supplies a plan for the continuation to
			// run in the new transaction.
			if t.Chain {
				panic(txnControlWithChainErr)
			}
			// NOTE: postgres doesn't make the following checks until runtime (see
			// also #119750).
			// TODO(#88198): check the calling context, since transaction control
			// statements are only allowed through a stack of SPs and DO blocks.
			if b.hasExceptionHandler() {
				panic(txnControlWithExceptionErr)
			}
			if !b.isProcedure {
				panic(txnInUDFErr)
			}
			name := "_stmt_commit"
			txnOpType := tree.StoredProcTxnCommit
			if t.Rollback {
				name = "_stmt_rollback"
				txnOpType = tree.StoredProcTxnRollback
			}
			// Check the following statements for SET TRANSACTION statements, which
			// apply to the new transaction that begins after the COMMIT/ROLLBACK.
			//
			// Note: SET TRANSACTION statements are only allowed immediately following
			// COMMIT or ROLLBACK (or another SET TRANSACTION), in the same block.
			var txnModes tree.TransactionModes
			for stmts = stmts[i+1:]; len(stmts) > 0; stmts = stmts[1:] {
				execStmt, ok := stmts[0].(*ast.Execute)
				if !ok {
					break
				}
				setTxnStmt, ok := execStmt.SqlStmt.(*tree.SetTransaction)
				if !ok {
					break
				}
				if err := txnModes.Merge(setTxnStmt.Modes); err != nil {
					panic(err)
				}
			}
			con := b.makeContinuation(name)
			con.def.Volatility = volatility.Volatile
			b.appendPlpgSQLStmts(&con, stmts)
			return b.callContinuationWithTxnOp(&con, s, txnOpType, txnModes)

		case *ast.Call:
			// Build a continuation that will execute the procedure, and then the
			// following PL/pgSQL statements.
			callCon := b.makeContinuation("_stmt_call")
			callCon.def.Volatility = volatility.Volatile

			// Resolve the procedure definition and overload for the call. Project the
			// result of the procedure call as a single output column.
			callScope := callCon.s.push()
			proc, def := b.ob.resolveProcedureDefinition(callScope, t.Proc)
			overload := proc.ResolvedOverload()
			procTyp := proc.ResolvedType()
			colName := scopeColName("").WithMetadataName(b.makeIdentifier("stmt_call"))
			col := b.ob.synthesizeColumn(callScope, colName, procTyp, nil /* expr */, nil /* scalar */)
			b.ob.withinNestedPLpgSQLCall(func() {
				col.scalar = b.ob.buildRoutine(proc, def, callCon.s, callScope, b.colRefs)
			})
			b.ob.constructProjectForScope(callCon.s, callScope)

			// Collect any target variables in OUT-parameter position. The result of
			// the procedure will be assigned to these variables, if any.
			var target []ast.Variable
			for j := range overload.RoutineParams {
				if overload.RoutineParams[j].IsOutParam() {
					if j < len(proc.Exprs) {
						// j can be greater or equal to number of arguments when
						// the default argument is used.
						if arg, ok := proc.Exprs[j].(*scopeColumn); ok {
							target = append(target, arg.name.refName)
							continue
						}
					}
					panic(pgerror.Newf(pgcode.Syntax,
						"procedure parameter \"%s\" is an output parameter "+
							"but corresponding argument is not writable",
						string(overload.RoutineParams[j].Name),
					))
				}
			}
			b.checkDuplicateTargets(target, "CALL")
			if len(target) == 0 {
				// When there is no INTO target, build the nested procedure call into a
				// body statement that is only executed for its side effects.
				b.appendBodyStmtFromScope(&callCon, callScope)
				b.appendPlpgSQLStmts(&callCon, stmts[i+1:])
				return b.callContinuation(&callCon, s)
			}
			// The nested procedure will return a tuple with elements corresponding
			// to the target variables. Project each element as a PLpgSQL variable.
			intoScope := b.projectTupleAsIntoTarget(callScope, target)

			// Call a continuation for the remaining PLpgSQL statements from the newly
			// built statement that has updated variables.
			retCon := b.makeContinuation("_stmt_call_ret")
			b.appendPlpgSQLStmts(&retCon, stmts[i+1:])
			intoScope = b.callContinuation(&retCon, intoScope)

			// Add the built statement to the CALL continuation.
			b.appendBodyStmtFromScope(&callCon, intoScope)
			return b.callContinuation(&callCon, s)

		case *ast.DoBlock:
			if !b.ob.evalCtx.Settings.Version.ActiveVersion(b.ob.ctx).IsActive(clusterversion.V25_1) {
				panic(doBlockVersionErr)
			}
			// DO statements are used to execute an anonymous code block. They are
			// handled by building the statements in the block into a routine that is
			// executed immediately.
			//
			// Build a continuation that will execute the routine in the first body
			// statement, and then the following PL/pgSQL statements in the second.
			doCon := b.makeContinuation("_stmt_do")
			doCon.def.Volatility = volatility.Volatile
			body, bodyProps := b.ob.buildPLpgSQLDoBody(t)
			b.appendBodyStmt(&doCon, body, bodyProps)
			b.appendPlpgSQLStmts(&doCon, stmts[i+1:])
			return b.callContinuation(&doCon, s)

		default:
			panic(errors.WithDetailf(unsupportedPLStmtErr,
				"%s is not yet supported", stmt.PlpgSQLStatementTag(),
			))
		}
	}
	// Call the parent continuation to execute the rest of the function. Ignore
	// loop exit continuations, which are only invoked by EXIT statements.
	conTypes := continuationDefault | continuationLoopContinue | continuationBlockExit
	return b.callContinuation(b.getContinuation(conTypes, unspecifiedLabel), s)
}

// handleIntForLoop constructs the plan for an integer FOR loop, which
// increments a counter variable on each iteration. The loop body is executed
// until the counter exceeds the upper bound.
func (b *plpgsqlBuilder) handleIntForLoop(
	s *scope, forLoop *ast.ForLoop, control *ast.IntForLoopControl,
) *scope {
	if len(forLoop.Target) != 1 {
		panic(intForLoopTargetErr)
	}
	// Build an implicit block declaring:
	//  * The loop target variable.
	//  * Hidden variables for the lower and upper bounds, step size, an internal
	//    counter that is incremented on each iteration.
	//
	// The target variable is assigned the value of the counter variable on each
	// iteration. Both variables are necessary because modifications to the target
	// variable apply only to that iteration, and do not affect future iterations.
	// Example:
	//
	//   CREATE FUNCTION f() RETURNS INT LANGUAGE PLpgSQL AS $$
	//     BEGIN
	//       FOR i IN 1..3 LOOP
	//         i := i * 100;
	//         RAISE NOTICE 'i: %', i;
	//       END LOOP;
	//       RETURN 0;
	//     END
	//   $$;
	//
	// The above function would produce results 100, 200, and 300, rather
	// than exiting after the first iteration.
	b.pushNewBlock(&ast.Block{Label: forLoop.Label})
	defer b.popBlock()
	const (
		lowerName   = "_loop_lower"
		upperName   = "_loop_upper"
		stepName    = "_loop_step"
		counterName = "_loop_counter"
	)
	b.addHiddenVariable(lowerName, types.Int)
	b.addHiddenVariable(upperName, types.Int)
	b.addHiddenVariable(stepName, types.Int)
	b.addHiddenVariable(counterName, types.Int)
	b.addVariable(forLoop.Target[0], types.Int)

	// Initialize the constant bounds and step size.
	stepSize := control.Step
	if stepSize == nil {
		// The default step size is 1.
		stepSize = tree.NewDInt(1)
	}
	s = b.assignToHiddenVariable(s, lowerName, control.Lower)
	s = b.assignToHiddenVariable(s, upperName, control.Upper)
	s = b.assignToHiddenVariable(s, stepName, stepSize)

	// When referencing a hidden variable, make sure to check the correct scope,
	// as different columns can represent the variable depending on context.
	refHiddenVar := func(s *scope, name string) *scopeColumn {
		return s.findAnonymousColumnWithMetadataName(name)
	}

	// Add runtime checks for the bounds and step size.
	branches := make(memo.ScalarListExpr, 0, 4)
	raiseErrArgs := make([]memo.ScalarListExpr, 0, 4)
	const severity, detail, hint = "ERROR", "", ""
	addCheck := func(message, code string, checkCond opt.ScalarExpr) {
		raiseErrArgs = append(raiseErrArgs, b.ob.makeConstRaiseArgs(severity, message, detail, hint, code))
		branches = append(branches, checkCond)
	}
	addNullCheck := func(context string, varRef tree.Expr) {
		checkCond := b.buildSQLExpr(&tree.IsNullExpr{Expr: varRef}, types.Bool, s)
		message := fmt.Sprintf("%s of FOR loop cannot be null", context)
		addCheck(message, pgcode.NullValueNotAllowed.String(), checkCond)
	}
	addNullCheck("lower bound" /* context */, refHiddenVar(s, lowerName))
	addNullCheck("upper bound" /* context */, refHiddenVar(s, upperName))
	addNullCheck("BY value" /* context */, refHiddenVar(s, stepName))
	addCheck("BY value of FOR loop must be greater than zero", /* message */
		pgcode.InvalidParameterValue.String(),
		b.buildSQLExpr(&tree.ComparisonExpr{
			Operator: treecmp.MakeComparisonOperator(treecmp.LE),
			Left:     refHiddenVar(s, stepName),
			Right:    tree.DZero,
		}, types.Bool, s))
	b.addRuntimeCheck(s, branches, raiseErrArgs)

	// Initialize the loop counter target variables with the lower bound.
	s = b.assignToHiddenVariable(s, counterName, refHiddenVar(s, lowerName))
	s = b.addPLpgSQLAssign(s, forLoop.Target[0], refHiddenVar(s, lowerName), noIndirection)

	// The looping will be implemented by two continuations: one to execute the
	// loop body, and one to increment the counter variable. The loop body and
	// increment continuations will call each other recursively.
	loopCon := b.makeContinuation("stmt_loop")
	loopCon.def.IsRecursive = true
	incrementCon := b.makeContinuationWithTyp("stmt_loop_inc", forLoop.Label, continuationLoopContinue)
	incrementCon.def.IsRecursive = true

	// Push the increment continuation so that the loop body can call into it.
	b.pushContinuation(incrementCon)

	// Now, build the loop body continuation. Build an IF statement that checks
	// whether the counter variable has exceeded the upper bound, and executes the
	// loop body if not.
	cmpOp := treecmp.MakeComparisonOperator(treecmp.LE)
	if control.Reverse {
		cmpOp = treecmp.MakeComparisonOperator(treecmp.GE)
	}
	cond := &tree.ComparisonExpr{
		Operator: cmpOp,
		Left:     refHiddenVar(loopCon.s, counterName),
		Right:    refHiddenVar(loopCon.s, upperName),
	}
	ifStmt := &ast.If{Condition: cond, ThenBody: forLoop.Body, ElseBody: []ast.Statement{&ast.Exit{}}}
	b.appendPlpgSQLStmts(&loopCon, []ast.Statement{ifStmt})

	// Now that the loop body is built, pop the increment continuation.
	b.popContinuation()

	// Finally, build the increment continuation. Assign to the counter variable
	// using the value of the step variable, and then assign the result to the
	// target variable.
	incScope := incrementCon.s.push()
	b.ensureScopeHasExpr(incScope)
	binOp := treebin.MakeBinaryOperator(treebin.Plus)
	if control.Reverse {
		binOp = treebin.MakeBinaryOperator(treebin.Minus)
	}
	inc := &tree.BinaryExpr{
		Operator: binOp,
		Left:     refHiddenVar(incScope, counterName),
		Right:    refHiddenVar(incScope, stepName),
	}
	incScope = b.assignToHiddenVariable(incScope, counterName, inc)
	incScope = b.addPLpgSQLAssign(
		incScope, forLoop.Target[0], refHiddenVar(incScope, counterName), noIndirection,
	)
	// Call recursively into the loop body continuation.
	incScope = b.callContinuation(&loopCon, incScope)
	b.appendBodyStmtFromScope(&incrementCon, incScope)

	// Notably, we call the loop body continuation here, rather than the
	// increment continuation, because the counter should not be incremented
	// until after the first iteration.
	return b.callContinuation(&loopCon, s)
}

// resolveOpenQuery finds and validates the query that is bound to cursor for
// the given OPEN statement.
func (b *plpgsqlBuilder) resolveOpenQuery(open *ast.Open) tree.Statement {
	// Search the blocks in reverse order to ensure that more recent declarations
	// are encountered first.
	var boundStmt tree.Statement
	for i := len(b.blocks) - 1; i >= 0; i-- {
		block := &b.blocks[i]
		for name := range block.cursors {
			if open.CurVar == name {
				boundStmt = block.cursors[name].Query
				break
			}
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
//
// indirection is the optional name of a field from the (composite-typed)
// variable. If it is set, then it is the field that is being assigned to, not
// the variable itself.
func (b *plpgsqlBuilder) addPLpgSQLAssign(
	inScope *scope, ident ast.Variable, val ast.Expr, indirection tree.Name,
) *scope {
	typ := b.resolveVariableForAssign(ident)
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
	// Project the assignment as a new column. If the projected expression is
	// volatile, add barriers before and after the projection to prevent optimizer
	// rules from reordering or removing its side effects.
	colName := scopeColName(ident)
	var scalar opt.ScalarExpr
	if indirection != noIndirection {
		scalar = b.handleIndirectionForAssign(inScope, typ, ident, indirection, val)
	} else {
		scalar = b.buildSQLExpr(val, typ, inScope)
	}
	b.addBarrierIfVolatile(inScope, scalar)
	b.ob.synthesizeColumn(assignScope, colName, typ, nil, scalar)
	b.ob.constructProjectForScope(inScope, assignScope)
	b.addBarrierIfVolatile(assignScope, scalar)
	return assignScope
}

// assignToHiddenVariable is similar to addPLpgSQLAssign, but it assigns to a
// hidden variable that is not visible to the user.
func (b *plpgsqlBuilder) assignToHiddenVariable(inScope *scope, name string, val ast.Expr) *scope {
	typ := b.resolveHiddenVariableForAssign(name)
	assignScope := inScope.push()
	for i := range inScope.cols {
		col := &inScope.cols[i]
		if col.name.MetadataName() == name {
			// Allow the assignment to shadow previous values for this column.
			continue
		}
		// If the column is not an outer column, add the column as a pass-through
		// column from the previous scope.
		assignScope.appendColumn(col)
	}
	colName := scopeColName("").WithMetadataName(name)
	scalar := b.buildSQLExpr(val, typ, inScope)
	b.addBarrierIfVolatile(inScope, scalar)
	b.ob.synthesizeColumn(assignScope, colName, typ, nil, scalar)
	b.ob.constructProjectForScope(inScope, assignScope)
	b.addBarrierIfVolatile(assignScope, scalar)
	return assignScope
}

const noIndirection = ""

// handleIndirectionForAssign is used to handle an assignment like "a.b := 2",
// where "a" is a record variable and "b" is a field of that record. The
// function constructs a new tuple with the field "b" replaced by the new value,
// and all other fields copied from the original tuple.
func (b *plpgsqlBuilder) handleIndirectionForAssign(
	inScope *scope, typ *types.T, ident ast.Variable, indirection tree.Name, val tree.Expr,
) opt.ScalarExpr {
	elemName := indirection.Normalize()

	// We do not yet support qualifying a variable with a block label.
	b.checkBlockLabelReference(elemName)
	if !b.buildSQL {
		// For lazy SQL evaluation, replace all expressions with NULL.
		return memo.NullSingleton
	}
	if typ.Family() != types.TupleFamily {
		// Like Postgres, treat this as a failure to find a matching
		// block.variable pair.
		panic(pgerror.Newf(pgcode.Syntax, "\"%s.%s\" is not a known variable", ident, indirection))
	}
	var found bool
	var elemIdx int
	for i := range typ.TupleLabels() {
		if typ.TupleLabels()[i] == elemName {
			found = true
			elemIdx = i
			break
		}
	}
	if !found {
		panic(pgerror.Newf(pgcode.UndefinedColumn,
			"record \"%s\" has no field \"%s\"", ident, elemName,
		))
	}
	_, source, _, err := inScope.FindSourceProvidingColumn(b.ob.ctx, ident)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "failed to find variable %s", ident))
	}
	if !source.(*scopeColumn).typ.Identical(typ) {
		panic(errors.AssertionFailedf("unexpected type for variable %s", ident))
	}
	varCol := b.ob.factory.ConstructVariable(source.(*scopeColumn).id)
	scalar := b.buildSQLExpr(val, typ.TupleContents()[elemIdx], inScope)
	newElems := make([]opt.ScalarExpr, len(typ.TupleContents()))
	for i := range typ.TupleContents() {
		if i == elemIdx {
			newElems[i] = scalar
		} else {
			newElems[i] = b.ob.factory.ConstructColumnAccess(varCol, memo.TupleOrdinal(i))
		}
	}
	return b.ob.factory.ConstructTuple(newElems, typ)
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

// buildPLpgSQLRaise builds a Project expression which implements the
// notice-sending behavior of RAISE statements.
func (b *plpgsqlBuilder) buildPLpgSQLRaise(inScope *scope, args memo.ScalarListExpr) *scope {
	raiseColName := scopeColName("").WithMetadataName(b.makeIdentifier("stmt_raise"))
	raiseScope := inScope.push()
	fn := b.ob.makePLpgSQLRaiseFn(args)
	b.ob.synthesizeColumn(raiseScope, raiseColName, types.Int, nil /* expr */, fn)
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
		return b.buildSQLExpr(expr, types.String, s)
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
					panic(tooFewRaiseParamsErr)
				}
				// If the argument is NULL, postgres prints "<NULL>".
				expr := &tree.CastExpr{Expr: args[argIdx], Type: types.String}
				arg := b.buildSQLExpr(expr, types.String, s)
				arg = b.ob.factory.ConstructCoalesce(memo.ScalarListExpr{arg, makeConstStr("<NULL>")})
				addToResult(arg)
				argIdx++
			}
			addToResult(makeConstStr(paramSubstr))
		}
	}
	if argIdx < len(args) {
		panic(tooManyRaiseParamsErr)
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
// The block entry/exit and assignment continuations for a block with an
// exception handler must be volatile to prevent inlining. The presence of an
// exception handler does not impose restrictions on inlining for other types of
// continuations.
//
// Inlining is disabled for the block-exit continuation to ensure that the
// statements following the nested block are correctly handled as part of the
// parent block. Otherwise, an error thrown from the parent block could
// incorrectly be caught by the exception handler of the nested block.
func (b *plpgsqlBuilder) buildExceptions(block *ast.Block) *memo.ExceptionBlock {
	codes := make([]pgcode.Code, 0, len(block.Exceptions))
	handlers := make([]*memo.UDFDefinition, 0, len(block.Exceptions))
	addHandler := func(codeStr string, handler *memo.UDFDefinition) {
		code := pgcode.MakeCode(strings.ToUpper(codeStr))
		switch code {
		case pgcode.TransactionRollback, pgcode.TransactionIntegrityConstraintViolation,
			pgcode.SerializationFailure, pgcode.StatementCompletionUnknown,
			pgcode.DeadlockDetected:
			panic(retryableErrErr)
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
	return &memo.ExceptionBlock{
		Codes:   codes,
		Actions: handlers,
	}
}

// handleEndOfFunction handles the case when control flow reaches the end of a
// PL/pgSQL routine without reaching a RETURN statement.
func (b *plpgsqlBuilder) handleEndOfFunction(inScope *scope) *scope {
	if b.hasOutParam() || b.returnType.Family() == types.VoidFamily {
		// Routines with OUT-parameters and VOID return types need not explicitly
		// specify a RETURN statement.
		var returnExpr tree.Expr = tree.DNull
		if b.hasOutParam() {
			returnExpr = b.makeReturnForOutParams()
		}
		returnScope := inScope.push()
		colName := scopeColName("_implicit_return")
		returnScalar := b.buildSQLExpr(returnExpr, b.returnType, inScope)
		b.ob.synthesizeColumn(returnScope, colName, b.returnType, nil /* expr */, returnScalar)
		b.ob.constructProjectForScope(inScope, returnScope)
		return returnScope
	}
	// Build a RAISE statement which throws an end-of-function error if executed.
	con := b.makeContinuation("_end_of_function")
	b.buildEndOfFunctionRaise(&con)
	return b.callContinuation(&con, inScope)
}

// buildEndOfFunctionRaise adds to the given continuation a RAISE statement that
// throws an end-of-function error, as well as a typed RETURN NULL to ensure
// that type-checking works out.
func (b *plpgsqlBuilder) buildEndOfFunctionRaise(con *continuation) {
	args := b.ob.makeConstRaiseArgs(
		"ERROR", /* severity */
		"control reached end of function without RETURN", /* message */
		"", /* detail */
		"", /* hint */
		pgcode.RoutineExceptionFunctionExecutedNoReturnStatement.String(), /* code */
	)
	con.def.Volatility = volatility.Volatile
	b.appendBodyStmtFromScope(con, b.buildPLpgSQLRaise(con.s, args))

	// Build a dummy statement that returns NULL. It won't be executed, but
	// ensures that the continuation routine's return type is correct.
	eofColName := scopeColName("").WithMetadataName(b.makeIdentifier("end_of_function"))
	eofScope := con.s.push()
	typedNull := b.ob.factory.ConstructNull(b.returnType)
	b.ob.synthesizeColumn(eofScope, eofColName, b.returnType, nil /* expr */, typedNull)
	b.ob.constructProjectForScope(con.s, eofScope)
	b.appendBodyStmtFromScope(con, eofScope)
}

// addOneRowCheck handles INTO STRICT, where a SQL statement is required to
// return exactly one row, or an error occurs.
func (b *plpgsqlBuilder) addOneRowCheck(s *scope) {
	// Add a ScalarGroupBy which passes through input columns, and also computes a
	// row count.
	aggs := make(memo.AggregationsExpr, 0, len(s.cols)+1)
	for j := range s.cols {
		// Create a pass-through aggregation. AnyNotNull works here because if there
		// is more than one row, execution will halt with an error and the rows will
		// be discarded anyway.
		agg := b.ob.factory.ConstructAnyNotNullAgg(b.ob.factory.ConstructVariable(s.cols[j].id))
		aggs = append(aggs, b.ob.factory.ConstructAggregationsItem(agg, s.cols[j].id))
	}
	rowCountColName := b.makeIdentifier("_plpgsql_row_count")
	rowCountCol := b.ob.factory.Metadata().AddColumn(rowCountColName, types.Int)
	rowCountAgg := b.ob.factory.ConstructCountRows()
	aggs = append(aggs, b.ob.factory.ConstructAggregationsItem(rowCountAgg, rowCountCol))
	s.expr = b.ob.factory.ConstructScalarGroupBy(s.expr, aggs, &memo.GroupingPrivate{})

	// Add a runtime check for the row count.
	tooFewRowsArgs := b.ob.makeConstRaiseArgs(
		"ERROR",                     /* severity */
		"query returned no rows",    /* message */
		"",                          /* detail */
		"",                          /* hint */
		pgcode.NoDataFound.String(), /* code */
	)
	tooManyRowsArgs := b.ob.makeConstRaiseArgs(
		"ERROR",                            /* severity */
		"query returned more than one row", /* message */
		"",                                 /* detail */
		"Make sure the query returns a single row, or use LIMIT 1.", /* hint */
		pgcode.TooManyRows.String(),                                 /* code */
	)
	rowCountVar := b.ob.factory.ConstructVariable(rowCountCol)
	scalarOne := b.ob.factory.ConstructConstVal(tree.NewDInt(1), types.Int)
	branches := memo.ScalarListExpr{
		b.ob.factory.ConstructLt(rowCountVar, scalarOne),
		b.ob.factory.ConstructGt(rowCountVar, scalarOne),
	}
	b.addRuntimeCheck(s, branches, []memo.ScalarListExpr{tooFewRowsArgs, tooManyRowsArgs})
}

// addRuntimeCheck projects a column that implements a check that must happen at
// runtime. The supplied boolean branch expressions and RAISE arguments will be
// used to construct a CASE expression that raises an error if the corresponding
// condition is true.
func (b *plpgsqlBuilder) addRuntimeCheck(
	s *scope, branches memo.ScalarListExpr, raiseErrArgs []memo.ScalarListExpr,
) {
	if len(branches) != len(raiseErrArgs) {
		panic(errors.AssertionFailedf("mismatched branches and raiseErrArgs"))
	}
	originalCols := s.colSet()
	caseWhens := make(memo.ScalarListExpr, len(branches))
	for i := range branches {
		raiseErrFn := b.ob.makePLpgSQLRaiseFn(raiseErrArgs[i])
		caseWhens[i] = b.ob.factory.ConstructWhen(branches[i], raiseErrFn)
	}
	caseExpr := b.ob.factory.ConstructCase(
		memo.TrueSingleton, caseWhens, b.ob.factory.ConstructNull(types.Int),
	)
	runtimeCheckColName := b.makeIdentifier("_plpgsql_runtime_check")
	runtimeCheckCol := b.ob.factory.Metadata().AddColumn(runtimeCheckColName, types.Int)
	proj := memo.ProjectionsExpr{b.ob.factory.ConstructProjectionsItem(caseExpr, runtimeCheckCol)}
	s.expr = b.ob.factory.ConstructProject(s.expr, proj, originalCols)

	// Add an optimization barrier to ensure that the runtime checks are not
	// eliminated by column-pruning. Then, remove the temporary columns from the
	// output.
	b.ob.addBarrier(s)
	s.expr = b.ob.factory.ConstructProject(s.expr, memo.ProjectionsExpr{}, originalCols)
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
// definition. Note that the parameters of the continuation will be determined
// by the current block; if a child block declares new variables, its
// continuations will have more parameters than those of its parent.
func (b *plpgsqlBuilder) makeContinuation(conName string) continuation {
	s := b.ob.allocScope()
	params := make(opt.ColList, 0, b.variableCount()+b.hiddenVariableCount())
	addParam := func(name scopeColumnName, typ *types.T) {
		col := b.ob.synthesizeColumn(s, name, typ, nil /* expr */, nil /* scalar */)
		// TODO(mgartner): Lift the 100 parameter restriction for synthesized
		// continuation UDFs.
		col.setParamOrd(len(params))
		params = append(params, col.id)
	}
	// Invariant: the variables of a child block always follow those of a parent
	// block in a continuation's parameters. This ensures that a continuation
	// from a parent block can be called in a child block simply by truncating the
	// set of variables that are in scope for that block (see callContinuation).
	for i := range b.blocks {
		block := &b.blocks[i]
		for _, name := range block.vars {
			addParam(scopeColName(name), block.varTypes[name])
		}
		for _, name := range block.hiddenVars {
			// Do not give the column constructed for a hidden variable a reference
			// name, since hidden variables cannot be referenced by the user.
			addParam(scopeColName("").WithMetadataName(name), block.hiddenVarTypes[name])
		}
	}
	b.ensureScopeHasExpr(s)
	return continuation{
		def: &memo.UDFDefinition{
			Params:            params,
			Name:              b.makeIdentifier(conName),
			Typ:               b.returnType,
			CalledOnNullInput: true,
			BlockState:        b.block().state,
			RoutineType:       tree.UDFRoutine,
			RoutineLang:       tree.RoutineLangPLpgSQL,
		},
		typ: continuationDefault,
		s:   s,
	}
}

// makeContinuationWithTyp allocates a continuation for a loop or block, with
// the correct label and continuation type.
func (b *plpgsqlBuilder) makeContinuationWithTyp(
	name, label string, typ continuationType,
) continuation {
	con := b.makeContinuation(name)
	con.label = label
	con.typ = typ
	return con
}

// appendBodyStmt adds the given body statement and its required properties to
// the definition of a continuation function. Only the last body statement will
// return results; all others will only be executed for their side effects
// (e.g. RAISE statement).
//
// appendBodyStmt is separate from makeContinuation to allow recursive routine
// definitions, which need to push the continuation before it is finished. The
// separation also allows for appending multiple body statements.
func (b *plpgsqlBuilder) appendBodyStmt(
	con *continuation, body memo.RelExpr, bodyProps *physical.Required,
) {
	// Set the volatility of the continuation routine to the least restrictive
	// volatility level in the Relational properties of the body statements.
	vol := body.Relational().VolatilitySet.ToVolatility()
	if con.def.Volatility < vol {
		con.def.Volatility = vol
	}
	con.def.Body = append(con.def.Body, body)
	con.def.BodyProps = append(con.def.BodyProps, bodyProps)
}

// appendBodyStmtFromScope is similar to appendBodyStmt, but retrieves the body
// statement its required properties from the given scope for convenience.
func (b *plpgsqlBuilder) appendBodyStmtFromScope(con *continuation, bodyScope *scope) {
	b.appendBodyStmt(con, bodyScope.expr, bodyScope.makePhysicalProps())
}

// appendPlpgSQLStmts builds the given PLpgSQL statements into a relational
// expression and appends it to the given continuation routine's body statements
// list.
func (b *plpgsqlBuilder) appendPlpgSQLStmts(con *continuation, stmts []ast.Statement) {
	// Make sure to push s before constructing the continuation scope to ensure
	// that the parameter columns are not projected.
	continuationScope := b.buildPLpgSQLStatements(stmts, con.s.push())
	b.appendBodyStmtFromScope(con, continuationScope)
}

// callContinuation adds a column that projects the result of calling the
// given continuation function.
func (b *plpgsqlBuilder) callContinuation(con *continuation, s *scope) *scope {
	if con == nil {
		return b.handleEndOfFunction(s)
	}
	args := b.makeContinuationArgs(con, s)
	call := b.ob.factory.ConstructUDFCall(args, &memo.UDFCallPrivate{Def: con.def})
	b.addBarrierIfVolatile(s, call)

	returnColName := scopeColName("").WithMetadataName(con.def.Name)
	returnScope := s.push()
	b.ob.synthesizeColumn(returnScope, returnColName, b.returnType, nil /* expr */, call)
	b.ob.constructProjectForScope(s, returnScope)
	return returnScope
}

// callContinuationWithTxnOp is similar to callContinuation, but wraps the
// continuation in a TxnControlExpr that will commit or abort the current
// transaction before resuming execution with the continuation.
func (b *plpgsqlBuilder) callContinuationWithTxnOp(
	con *continuation, s *scope, txnOp tree.StoredProcTxnOp, txnModes tree.TransactionModes,
) *scope {
	if con == nil {
		panic(errors.AssertionFailedf("nil continuation with transaction control"))
	}
	if txnOp == tree.StoredProcTxnNoOp {
		panic(errors.AssertionFailedf("no-op transaction control statement"))
	}
	b.ob.addBarrier(s)
	returnScope := s.push()
	args := b.makeContinuationArgs(con, s)
	txnPrivate := &memo.TxnControlPrivate{TxnOp: txnOp, TxnModes: txnModes, Def: con.def}
	if b.outScope != nil {
		txnPrivate.Props = b.outScope.makePhysicalProps()
		txnPrivate.OutCols = b.outScope.colList()
	} else {
		// outScope may be nil if we're in the context of function creation.
		// It's fine to not fully initialize the TxnControl expression in this
		// case.
		txnPrivate.Props = &physical.Required{}
	}
	txnControlExpr := b.ob.factory.ConstructTxnControl(args, txnPrivate)
	returnColName := scopeColName("").WithMetadataName(con.def.Name)
	b.ob.synthesizeColumn(returnScope, returnColName, b.returnType, nil /* expr */, txnControlExpr)
	b.ob.constructProjectForScope(s, returnScope)
	return returnScope
}

func (b *plpgsqlBuilder) makeContinuationArgs(con *continuation, s *scope) memo.ScalarListExpr {
	args := make(memo.ScalarListExpr, 0, len(con.def.Params))
	for i := range b.blocks {
		if len(args) == len(con.def.Params) {
			// A continuation has parameters for every variable that is in scope for
			// the block in which it was created. If we reach the end of those
			// parameters early, the continuation must be from an ancestor block. Any
			// remaining variables are out of scope because control has passed back to
			// the ancestor block.
			//
			// NOTE: makeContinuation maintains an invariant that variables from a
			// parent block precede those of a child block in a continuation's
			// parameters.
			break
		}
		block := &b.blocks[i]
		for _, name := range block.vars {
			_, source, _, err := s.FindSourceProvidingColumn(b.ob.ctx, name)
			if err != nil {
				panic(err)
			}
			args = append(args, b.ob.factory.ConstructVariable(source.(*scopeColumn).id))
		}
		for _, name := range block.hiddenVars {
			col := s.findAnonymousColumnWithMetadataName(name)
			if col == nil {
				panic(errors.AssertionFailedf("hidden variable %s not found", name))
			}
			args = append(args, b.ob.factory.ConstructVariable(col.id))
		}
	}
	return args
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
		b.ob.addBarrier(s)
	}
}

// buildSQLExpr type-checks and builds the given SQL expression into a
// ScalarExpr within the given scope.
func (b *plpgsqlBuilder) buildSQLExpr(expr ast.Expr, typ *types.T, s *scope) opt.ScalarExpr {
	if !b.buildSQL {
		// For lazy SQL evaluation, replace all expressions with NULL.
		return memo.NullSingleton
	}
	// Save any outer CTEs before building the expression, which may have
	// subqueries with inner CTEs.
	prevCTEs := b.ob.ctes
	b.ob.ctes = nil
	defer func() {
		b.ob.ctes = prevCTEs
	}()
	expr, _ = tree.WalkExpr(s, expr)
	typedExpr, err := expr.TypeCheck(b.ob.ctx, b.ob.semaCtx, typ)
	if err != nil {
		panic(err)
	}
	scalar := b.ob.buildScalar(typedExpr, s, nil, nil, b.colRefs)
	scalar = b.coerceType(scalar, typ)
	if len(b.ob.ctes) == 0 {
		return scalar
	}
	// There was at least one CTE within the scalar expression. It is possible to
	// "hoist" them above this point, but building them eagerly here means that
	// callers don't have to worry about CTE handling.
	f := b.ob.factory
	valuesCol := f.Metadata().AddColumn("", scalar.DataType())
	valuesExpr := f.ConstructValues(
		memo.ScalarListExpr{f.ConstructTuple(memo.ScalarListExpr{scalar}, scalar.DataType())},
		&memo.ValuesPrivate{Cols: opt.ColList{valuesCol}, ID: f.Metadata().NextUniqueID()},
	)
	withExpr := b.ob.buildWiths(valuesExpr, b.ob.ctes)
	return f.ConstructSubquery(withExpr, &memo.SubqueryPrivate{})
}

// buildSQLStatement type-checks and builds the given SQL statement into a
// RelExpr within the given scope.
func (b *plpgsqlBuilder) buildSQLStatement(stmt tree.Statement, inScope *scope) (outScope *scope) {
	if !b.buildSQL {
		// For lazy SQL evaluation, replace all statements with a single row without
		// any columns.
		outScope = inScope.push()
		outScope.expr = b.ob.factory.ConstructNoColsRow()
		return outScope
	}
	return b.ob.buildStmtAtRootWithScope(stmt, nil /* desiredTypes */, inScope)
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
func (b *plpgsqlBuilder) resolveVariableForAssign(name ast.Variable) *types.T {
	// Search the blocks in reverse order to ensure that more recent declarations
	// are encountered first.
	for i := len(b.blocks) - 1; i >= 0; i-- {
		block := &b.blocks[i]
		typ, ok := block.varTypes[name]
		if !ok {
			continue
		}
		if block.constants != nil {
			if _, ok := block.constants[name]; ok {
				panic(pgerror.Newf(pgcode.ErrorInAssignment, "variable \"%s\" is declared CONSTANT", name))
			}
		}
		return typ
	}
	panic(pgerror.Newf(pgcode.Syntax, "\"%s\" is not a known variable", name))
}

// resolveHiddenVariableForAssign is similar to resolveVariableForAssign, but
// applies to hidden variables, which are identified only by their name in the
// query's metadata. It panics if the hidden variable is not found.
func (b *plpgsqlBuilder) resolveHiddenVariableForAssign(name string) *types.T {
	// Search the blocks in reverse order to ensure that more recent declarations
	// are encountered first.
	for i := len(b.blocks) - 1; i >= 0; i-- {
		block := &b.blocks[i]
		typ, ok := block.hiddenVarTypes[name]
		if !ok {
			continue
		}
		return typ
	}
	panic(errors.AssertionFailedf("hidden variable %s not found", name))
}

// projectTupleAsIntoTarget maps from the elements of a tuple column to the
// variables of an INTO target for a FETCH or CALL statement.
//
// projectTupleAsIntoTarget assumes that inScope contains exactly one column,
// that the column is a tuple, and that the number of elements is equal to the
// number of target variables.
func (b *plpgsqlBuilder) projectTupleAsIntoTarget(inScope *scope, target []ast.Variable) *scope {
	intoScope := inScope.push()
	tupleCol := inScope.cols[0].id
	for i := range target {
		typ := b.resolveVariableForAssign(target[i])
		colName := scopeColName(target[i])
		scalar := b.ob.factory.ConstructColumnAccess(
			b.ob.factory.ConstructVariable(tupleCol),
			memo.TupleOrdinal(i),
		)
		scalar = b.coerceType(scalar, typ)
		b.ob.synthesizeColumn(intoScope, colName, typ, nil /* expr */, scalar)
	}
	b.ob.constructProjectForScope(inScope, intoScope)
	return intoScope
}

// checkDuplicateTargets checks for duplicate variables that are targets for
// assignment, which currently isn't supported.
func (b *plpgsqlBuilder) checkDuplicateTargets(target []ast.Variable, stmtName string) {
	if len(target) > 1 {
		seenTargets := make(map[ast.Variable]struct{})
		for _, name := range target {
			if _, ok := seenTargets[name]; ok {
				panic(unimplemented.NewWithIssueDetailf(121605,
					"assigning to a variable more than once in the same statement is not supported",
					"duplicate %s target", stmtName,
				))
			}
			seenTargets[name] = struct{}{}
		}
	}
}

// checkBlockLabelReference checks that the given name does not reference a
// block label, since doing so is not yet supported. This is only necessary for
// assignment statements, since all other cases currently do not allow the
// "a.b" syntax.
func (b *plpgsqlBuilder) checkBlockLabelReference(name string) {
	for i := len(b.blocks) - 1; i >= 0; i-- {
		for _, blockVarName := range b.blocks[i].vars {
			if name == string(blockVarName) {
				// We found the variable. Even if there is a block with the same name,
				// it is shadowed by the variable.
				return
			}
		}
		if b.blocks[i].label == name {
			panic(unimplemented.NewWithIssuef(122322,
				"qualifying a variable with a block label is not yet supported: %s", name,
			))
		}
	}
}

func (b *plpgsqlBuilder) prependStmt(stmt ast.Statement, stmts []ast.Statement) []ast.Statement {
	newStmts := make([]ast.Statement, 0, len(stmts)+1)
	newStmts = append(newStmts, stmt)
	return append(newStmts, stmts...)
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

func (b *plpgsqlBuilder) hasOutParam() bool {
	return len(b.outParams) > 0
}

// makeReturnForOutParams builds the implicit RETURN expression for a routine
// with OUT-parameters.
func (b *plpgsqlBuilder) makeReturnForOutParams() tree.Expr {
	if len(b.outParams) == 0 {
		panic(errors.AssertionFailedf("expected at least one out param"))
	}
	exprs := make(tree.Exprs, len(b.outParams))
	for i, param := range b.outParams {
		if param != "" {
			exprs[i] = tree.NewUnresolvedName(string(param))
		} else {
			// TODO(121251): if the unnamed parameter of INOUT type, then we
			// should be using the argument expression here (assuming this
			// parameter hasn't been modified via $i notation which is tracked
			// by 119502).
			exprs[i] = tree.DNull
		}
	}
	if len(exprs) == 1 && !b.isProcedure {
		// For procedures, even a single column is wrapped in a tuple.
		return exprs[0]
	}
	return &tree.Tuple{Exprs: exprs}
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

	// label is the label of the block or loop that gave rise to this
	// continuation, if any.
	label string

	// typ defines the context of the continuation.
	typ continuationType
}

const unspecifiedLabel = ""

// continuationType defines the context of the continuation, e.g. loop exit vs
// continue. This is used to filter continuations when determining which one
// must be called next.
type continuationType uint8

const (
	// continuationDefault is used for continuations that don't interact with
	// EXIT and CONTINUE statements.
	continuationDefault continuationType = 1 << iota

	// continuationLoopContinue encapsulates the body of a loop. CONTINUE
	// statements jump execution to this type of continuation, as can terminal
	// statements within the loop body.
	continuationLoopContinue

	// continuationLoopExit encapsulates the statements following a loop. Only
	// EXIT statements can jump execution to this type of continuation.
	continuationLoopExit

	// continuationBlockExit encapsulates the statements following a PL/pgSQL
	// block. EXIT statements can jump execution to this type of continuation, as
	// can terminal statements within the block body.
	continuationBlockExit
)

func (b *plpgsqlBuilder) pushContinuation(con continuation) {
	b.continuations = append(b.continuations, con)
}

func (b *plpgsqlBuilder) popContinuation() {
	if len(b.continuations) > 0 {
		b.continuations = b.continuations[:len(b.continuations)-1]
	}
}

// getContinuation attempts to retrieve the most recent continuation from the
// stack with the given required type and label.
//
// - allowedTypes is a bitset with each of the allowed continuation types.
func (b *plpgsqlBuilder) getContinuation(
	allowedTypes continuationType, label string,
) *continuation {
	for i := len(b.continuations) - 1; i >= 0; i-- {
		if allowedTypes&b.continuations[i].typ == 0 {
			// The caller requested to skip continuations of this type.
			continue
		}
		if label != unspecifiedLabel && b.continuations[i].label != label {
			// The caller specified the label of the continuation to retrieve.
			continue
		}
		return &b.continuations[i]
	}
	return nil
}

// addVariable adds a variable with the given name and type to the current
// PL/pgSQL block scope.
func (b *plpgsqlBuilder) addVariable(name ast.Variable, typ *types.T) {
	curBlock := b.block()
	if _, ok := curBlock.varTypes[name]; ok {
		panic(pgerror.Newf(pgcode.Syntax, "duplicate declaration at or near \"%s\"", name))
	}
	for i := range b.blocks {
		block := &b.blocks[i]
		if _, ok := block.varTypes[name]; ok {
			panic(errors.WithHintf(
				unimplemented.NewWithIssue(117508, "variable shadowing is not yet implemented"),
				"variable \"%s\" shadows a previously defined variable", name,
			))
		}
	}
	curBlock.vars = append(curBlock.vars, name)
	curBlock.varTypes[name] = typ
}

// addHiddenVariable adds a hidden variable with the given (metadata) name and
// type to the current PL/pgSQL block scope.
func (b *plpgsqlBuilder) addHiddenVariable(metadataName string, typ *types.T) {
	curBlock := b.block()
	curBlock.hiddenVars = append(curBlock.hiddenVars, metadataName)
	if curBlock.hiddenVarTypes == nil {
		curBlock.hiddenVarTypes = make(map[string]*types.T)
	}
	curBlock.hiddenVarTypes[metadataName] = typ
}

// block returns the block for the current PL/pgSQL block.
func (b *plpgsqlBuilder) block() *plBlock {
	return &b.blocks[len(b.blocks)-1]
}

// parentBlock returns the parent block for the current PL/pgSQL block. It
// returns nil if the current block does not have a parent.
func (b *plpgsqlBuilder) parentBlock() *plBlock {
	if len(b.blocks) <= 1 {
		return nil
	}
	return &b.blocks[len(b.blocks)-2]
}

// rootBlock returns the root block that encapsulates the entire routine.
func (b *plpgsqlBuilder) rootBlock() *plBlock {
	return &b.blocks[0]
}

// pushBlock puts the given block on the stack. It is used when entering the
// scope of a PL/pgSQL block.
func (b *plpgsqlBuilder) pushBlock(bs plBlock) *plBlock {
	b.blocks = append(b.blocks, bs)
	return &b.blocks[len(b.blocks)-1]
}

// popBlock removes the current block from the stack. It is used when leaving
// the scope of a PL/pgSQL block.
func (b *plpgsqlBuilder) popBlock() {
	b.blocks = b.blocks[:len(b.blocks)-1]
}

// hasExceptionHandler returns true if any block from the root to the current
// block has an exception handler.
func (b *plpgsqlBuilder) hasExceptionHandler() bool {
	for i := range b.blocks {
		if b.blocks[i].hasExceptionHandler {
			return true
		}
	}
	return false
}

// variableCount returns the number of PL/pgSQL variables that are in scope for
// the current block. Note that this count does not include hidden variables.
func (b *plpgsqlBuilder) variableCount() int {
	var count int
	for i := range b.blocks {
		count += len(b.blocks[i].vars)
	}
	return count
}

// variableCountWithHidden returns the number of hidden variables that are in
// scope for the current block.
func (b *plpgsqlBuilder) hiddenVariableCount() int {
	var count int
	for i := range b.blocks {
		count += len(b.blocks[i].hiddenVars)
	}
	return count
}

// recordTypeVisitor is used to infer the concrete return type for a
// record-returning PLpgSQL routine. It visits each return statement and checks
// that the types of all returned expressions are either identical or UNKNOWN.
type recordTypeVisitor struct {
	ctx     context.Context
	semaCtx *tree.SemaContext
	s       *scope
	typ     *types.T
	block   *ast.Block
}

func newRecordTypeVisitor(
	ctx context.Context, semaCtx *tree.SemaContext, s *scope, block *ast.Block,
) *recordTypeVisitor {
	return &recordTypeVisitor{ctx: ctx, semaCtx: semaCtx, s: s, block: block}
}

var _ ast.StatementVisitor = &recordTypeVisitor{}

func (r *recordTypeVisitor) Visit(stmt ast.Statement) (newStmt ast.Statement, recurse bool) {
	switch t := stmt.(type) {
	case *ast.Block:
		if t != r.block {
			// This is a nested block. We can't visit it yet, since we haven't built
			// its variable declarations, and therefore can't type-check expressions
			// that reference those variables.
			return t, false
		}
	case *ast.Return:
		desired := types.AnyElement
		if r.typ != nil && r.typ.Family() != types.UnknownFamily {
			desired = r.typ
		}
		expr, _ := tree.WalkExpr(r.s, t.Expr)
		typedExpr, err := expr.TypeCheck(r.ctx, r.semaCtx, desired)
		if err != nil {
			panic(err)
		}
		typ := typedExpr.ResolvedType()
		switch typ.Family() {
		case types.UnknownFamily, types.TupleFamily:
		default:
			panic(nonCompositeErr)
		}
		if r.typ == nil || r.typ.Family() == types.UnknownFamily {
			r.typ = typ
			return stmt, false
		}
		if typ.Family() == types.UnknownFamily {
			return stmt, false
		}
		if !typ.Identical(r.typ) {
			panic(recordReturnErr)
		}
	}
	return stmt, true
}

// transactionControlVisitor is used to check for COMMIT or ROLLBACK statements
// for a PL/pgSQL stored procedure, so that stable folding can be disabled.
type transactionControlVisitor struct {
	foundTxnControlStatement bool
}

var _ ast.StatementVisitor = &transactionControlVisitor{}

func (tc *transactionControlVisitor) Visit(
	stmt ast.Statement,
) (newStmt ast.Statement, recurse bool) {
	if _, ok := stmt.(*ast.TransactionControl); ok {
		tc.foundTxnControlStatement = true
		return stmt, false
	}
	return stmt, !tc.foundTxnControlStatement
}

var (
	unsupportedPLStmtErr = unimplemented.New("unimplemented PL/pgSQL statement",
		"attempted to use a PL/pgSQL statement that is not yet supported",
	)
	notNullVarErr = unimplemented.NewWithIssueDetail(105243, "not null variable",
		"not-null PL/pgSQL variables are not yet supported",
	)
	collatedVarErr = unimplemented.NewWithIssueDetail(105245, "variable collation",
		"collation for PL/pgSQL variables is not yet supported",
	)
	recordVarErr = unimplemented.NewWithIssueDetail(114874, "RECORD variable",
		"RECORD type for PL/pgSQL variables is not yet supported",
	)
	scrollableCursorErr = unimplemented.NewWithIssue(77102,
		"DECLARE SCROLL CURSOR",
	)
	retryableErrErr = unimplemented.NewWithIssue(111446,
		"catching a Transaction Retry error in a PLpgSQL EXCEPTION block is not yet implemented",
	)
	recordReturnErr = errors.WithHint(
		unimplemented.NewWithIssue(115384,
			"returning different types from a RECORD-returning function is not yet supported",
		),
		"try casting all RETURN statements to the same type",
	)
	wildcardReturnTypeErr = unimplemented.NewWithIssue(122945,
		"wildcard return type is not yet supported in this context")
	exitOutsideLoopErr = pgerror.New(pgcode.Syntax,
		"EXIT cannot be used outside a loop, unless it has a label",
	)
	continueOutsideLoopErr = pgerror.New(pgcode.Syntax,
		"CONTINUE cannot be used outside a loop",
	)
	cursorMutationErr = pgerror.Newf(pgcode.FeatureNotSupported,
		"DECLARE CURSOR must not contain data-modifying statements in WITH",
	)
	fetchRowsErr = pgerror.New(pgcode.FeatureNotSupported,
		"FETCH statement cannot return multiple rows",
	)
	tooFewRaiseParamsErr = pgerror.Newf(pgcode.Syntax,
		"too few parameters specified for RAISE",
	)
	tooManyRaiseParamsErr = pgerror.Newf(pgcode.Syntax,
		"too many parameters specified for RAISE",
	)
	nonCompositeErr = pgerror.New(pgcode.DatatypeMismatch,
		"cannot return non-composite value from function returning composite type",
	)
	returnWithOUTParameterErr = pgerror.New(pgcode.DatatypeMismatch,
		"RETURN cannot have a parameter in function with OUT parameters",
	)
	returnWithVoidParameterErr = pgerror.New(pgcode.DatatypeMismatch,
		"RETURN cannot have a parameter in function returning void",
	)
	returnWithVoidParameterProcedureErr = pgerror.New(pgcode.Syntax,
		"RETURN cannot have a parameter in a procedure")
	emptyReturnErr = pgerror.New(pgcode.Syntax,
		"missing expression at or near \"RETURN;\"",
	)
	txnControlWithExceptionErr = errors.WithDetail(
		pgerror.Newf(pgcode.InvalidTransactionTermination, "invalid transaction termination"),
		"PL/pgSQL COMMIT/ROLLBACK is not allowed inside a block with exception handlers",
	)
	txnInUDFErr = errors.WithDetail(
		pgerror.Newf(pgcode.InvalidTransactionTermination, "invalid transaction termination"),
		"PL/pgSQL COMMIT/ROLLBACK is not allowed inside a user-defined function")
	txnControlWithChainErr = unimplemented.NewWithIssue(119646,
		"COMMIT or ROLLBACK with AND CHAIN syntax is not yet implemented",
	)
	setTxnNotAfterControlStmtErr = errors.WithHint(
		pgerror.New(pgcode.ActiveSQLTransaction, "SET TRANSACTION must be called before any query"),
		"PL/pgSQL SET TRANSACTION statements must immediately follow COMMIT or ROLLBACK",
	)
	intForLoopTargetErr = pgerror.New(pgcode.Syntax,
		"integer FOR loop must have only one target variable",
	)
	doBlockVersionErr = unimplemented.Newf("do blocks",
		"DO statement usage inside a routine definition is not supported until version 25.1",
	)
)
