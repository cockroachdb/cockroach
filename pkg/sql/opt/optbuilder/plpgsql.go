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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

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
	continuations []memo.UDFCallPrivate

	// exitContinuations is similar to continuations, but is used upon reaching an
	// EXIT statement within a loop. It is used to resume execution with the
	// statements that follow the loop.
	exitContinuations []memo.UDFCallPrivate

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
		// TODO(drewk): either add issues or implement these cases.
		if dec.NotNull {
			panic(unimplemented.New(
				"not null variable",
				"not-null PL/pgSQL variables are not yet supported",
			))
		}
		if dec.Constant {
			panic(unimplemented.New(
				"constant variable",
				"constant PL/pgSQL variables are not yet supported",
			))
		}
		if dec.Collate != "" {
			panic(unimplemented.New(
				"variable collation",
				"collation for PL/pgSQL variables is not yet supported",
			))
		}
	}
}

// build constructs an expression that returns the result of executing a
// PL/pgSQL function. See buildPLpgSQLStatements for more details.
func (b *plpgsqlBuilder) build(block *plpgsqltree.PLpgSQLStmtBlock, s *scope) *scope {
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
	return b.buildPLpgSQLStatements(block.Body, s)
}

// buildPLpgSQLStatements performs the majority of the work building a PL/pgSQL
// function definition into a form that can be handled by the SQL execution
// engine. It models control flow statements by defining (possibly recursive)
// functions that model returning control after a statement block has finished
// executing. See the comments within for further detail.
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
			b.pushContinuation(b.buildContinuation(stmts[i+1:], "stmt_if"))
			// Build each branch of the IF statement, calling the continuation
			// function at the end of construction in order to resume execution after
			// the IF block. Note that if the ELSE body is empty, elseExpr will be
			// equivalent to executing the statements following the IF statement.
			thenExpr := b.buildPLpgSQLStatements(t.ThenBody, s.push()).expr
			elseExpr := b.buildPLpgSQLStatements(t.ElseBody, s.push()).expr
			b.popContinuation()

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
			b.pushExitContinuation(b.buildContinuation(stmts[i+1:], "loop_exit"))
			loopContinuation := b.makeContinuation("stmt_loop")
			b.pushContinuation(loopContinuation)
			b.finishContinuation(t.Body, loopContinuation.Def, true /* recursive */)
			b.popExitContinuation()
			b.popContinuation()
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
			if continuation := b.getExitContinuation(); continuation != nil {
				return b.callContinuation(continuation, s)
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
			if continuation := b.getLoopContinuation(); continuation != nil {
				return b.callContinuation(continuation, s)
			} else {
				panic(pgerror.New(pgcode.Syntax, "CONTINUE cannot be used outside a loop"))
			}
		default:
			panic(unimplemented.New(
				"unimplemented PL/pgSQL statement",
				"attempted to use a PL/pgSQL statement that is not yet supported",
			))
		}
	}
	if continuation := b.getContinuation(); continuation != nil {
		// If we reached the end of a statement block and RETURN has not yet been
		// called, there must be an ancestor scope that has provided a continuation
		// function that resumes control with the remaining statements.
		return b.callContinuation(continuation, s)
	} else {
		panic(pgerror.New(pgcode.RoutineExceptionFunctionExecutedNoReturnStatement,
			"control reached end of function without RETURN",
		))
	}
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
		if inScope.expr.Relational().OutputCols.Contains(col.id) {
			// If the column is not an outer column, add the column as a pass-through
			// column from the previous scope.
			assignScope.appendColumn(col)
		}
	}
	// Project the assignment as a new column.
	colName := scopeColName(ident)
	scalar := b.buildPLpgSQLExpr(val, typ, inScope)
	b.ob.synthesizeColumn(assignScope, colName, typ, nil, scalar)
	b.ob.constructProjectForScope(inScope, assignScope)

	// Ensure that outer columns remain in scope.
	for i := range inScope.cols {
		col := &inScope.cols[i]
		if !inScope.expr.Relational().OutputCols.Contains(col.id) {
			assignScope.appendColumn(col)
		}
	}
	return assignScope
}

// buildContinuation returns a fully initialized continuation function that
// executes the given statements and resumes control with the parent scope.
func (b *plpgsqlBuilder) buildContinuation(
	stmts []plpgsqltree.PLpgSQLStatement, name string,
) memo.UDFCallPrivate {
	if len(stmts) == 0 {
		if continuation := b.getContinuation(); continuation != nil {
			return *continuation
		} else {
			panic(pgerror.New(pgcode.RoutineExceptionFunctionExecutedNoReturnStatement,
				"control reached end of function without RETURN",
			))
		}
	}
	continuation := b.makeContinuation(name)
	b.finishContinuation(stmts, continuation.Def, false /* recursive */)
	return continuation
}

// makeContinuation allocates a new continuation function with an uninitialized
// definition.
func (b *plpgsqlBuilder) makeContinuation(name string) memo.UDFCallPrivate {
	return memo.UDFCallPrivate{
		Name:              b.makeIdentifier(name),
		Def:               &memo.UDFDefinition{},
		Typ:               b.returnType,
		CalledOnNullInput: true,
	}
}

// finishContinuation initializes the definition of a continuation function with
// the function body. It is separate from makeContinuation to allow recursive
// function definitions.
func (b *plpgsqlBuilder) finishContinuation(
	stmts []plpgsqltree.PLpgSQLStatement, def *memo.UDFDefinition, recursive bool,
) {
	s := b.ob.allocScope()
	b.ensureScopeHasExpr(s)
	params := make(opt.ColList, 0, len(b.decls)+len(b.params))
	addParam := func(name tree.Name, typ *types.T) {
		colName := scopeColName(name)
		col := b.ob.synthesizeColumn(s, colName, typ, nil /* expr */, nil /* scalar */)
		col.setParamOrd(len(params))
		params = append(params, col.id)
	}
	for _, dec := range b.decls {
		addParam(dec.Var, b.varTypes[dec.Var])
	}
	for _, param := range b.params {
		addParam(tree.Name(param.Name), param.Typ)
	}
	// Make sure to push s before constructing the continuation scope to ensure
	// that the parameter columns are not projected.
	continuationScope := b.buildPLpgSQLStatements(stmts, s.push())
	*def = memo.UDFDefinition{
		Body:        []memo.RelExpr{continuationScope.expr},
		BodyProps:   []*physical.Required{continuationScope.makePhysicalProps()},
		Params:      params,
		IsRecursive: recursive,
	}
}

// callContinuation adds a column that projects the result of calling the
// given continuation function.
func (b *plpgsqlBuilder) callContinuation(continuation *memo.UDFCallPrivate, s *scope) *scope {
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
	call := b.ob.factory.ConstructUDFCall(args, continuation)

	returnColName := scopeColName("").WithMetadataName(continuation.Name)
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

func (b *plpgsqlBuilder) pushContinuation(continuation memo.UDFCallPrivate) {
	b.continuations = append(b.continuations, continuation)
}

func (b *plpgsqlBuilder) popContinuation() {
	if len(b.continuations) > 0 {
		b.continuations = b.continuations[:len(b.continuations)-1]
	}
}

func (b *plpgsqlBuilder) getContinuation() *memo.UDFCallPrivate {
	if len(b.continuations) == 0 {
		return nil
	}
	return &b.continuations[len(b.continuations)-1]
}

func (b *plpgsqlBuilder) pushExitContinuation(exitContinuation memo.UDFCallPrivate) {
	b.exitContinuations = append(b.exitContinuations, exitContinuation)
}

func (b *plpgsqlBuilder) popExitContinuation() {
	if len(b.exitContinuations) > 0 {
		b.exitContinuations = b.exitContinuations[:len(b.exitContinuations)-1]
	}
}

func (b *plpgsqlBuilder) getExitContinuation() *memo.UDFCallPrivate {
	if len(b.exitContinuations) == 0 {
		return nil
	}
	return &b.exitContinuations[len(b.exitContinuations)-1]
}

func (b *plpgsqlBuilder) getLoopContinuation() *memo.UDFCallPrivate {
	for i := len(b.continuations) - 1; i >= 0; i-- {
		if b.continuations[i].Def.Body == nil {
			// A loop continuation will have an unfinished definition.
			// TODO(drewk): once more control-flow is supported (e.g. labels), it will
			// probably be best to make a continuation struct.
			return &b.continuations[i]
		}
	}
	return nil
}
