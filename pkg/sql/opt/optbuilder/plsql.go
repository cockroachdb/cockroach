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
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type plsqlBuilder struct {
	ob *Builder

	// colRefs, if non-nil, tracks the set of columns referenced by scalar
	// expressions.
	colRefs *opt.ColSet

	// params tracks the names and types for the original function parameters.
	params []tree.ParamType

	// decs is the set of variable declarations for a PL/pgSQL function.
	decs []tree.PLDeclaration

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

func (b *plsqlBuilder) init(
	ob *Builder,
	colRefs *opt.ColSet,
	params []tree.ParamType,
	decs []tree.PLDeclaration,
	returnType *types.T,
) {
	b.ob = ob
	b.colRefs = colRefs
	b.params = params
	b.decs = decs
	b.returnType = returnType
	b.varTypes = make(map[tree.Name]*types.T)
	for _, dec := range b.decs {
		b.varTypes[dec.Ident] = dec.Typ
	}
}

// build constructs an expression that returns the result of executing a
// PL/pgSQL function. See buildPLpgSQLStatements for more details.
func (b *plsqlBuilder) build(stmts []tree.PLStatement, s *scope) *scope {
	b.ensureScopeHasExpr(s)

	// Some variable declarations initialize the variable.
	for _, dec := range b.decs {
		if dec.Assign != nil {
			s = b.addPLpgSQLAssign(s, dec.Ident, dec.Assign)
		}
	}
	return b.buildPLpgSQLStatements(stmts, s)
}

// buildPLpgSQLStatements performs the majority of the work building a PL/pgSQL
// function definition into a form that can be handled by the SQL execution
// engine. It models control flow statements by defining (possibly recursive)
// functions that model returning control after a statement block has finished
// executing. See the comments within for further detail.
func (b *plsqlBuilder) buildPLpgSQLStatements(stmts []tree.PLStatement, s *scope) *scope {
	b.ensureScopeHasExpr(s)
	for i, stmt := range stmts {
		switch t := stmt.(type) {
		case *tree.PLReturn:
			// RETURN is handled by projecting a single column with the expression
			// that is being returned.
			returnScalar := b.buildPLpgSQLExpr(t.Expr, b.returnType, s)
			returnColName := scopeColName("").WithMetadataName(b.makeIdentifier("return"))
			returnScope := s.push()
			b.ob.synthesizeColumn(returnScope, returnColName, b.returnType, nil /* expr */, returnScalar)
			b.ob.constructProjectForScope(s, returnScope)
			return returnScope
		case *tree.PLAssignment:
			// Assignment (:=) is handled by projecting a new column with the same
			// name as the variable being assigned.
			s = b.addPLpgSQLAssign(s, t.Ident, t.Assign)
		case *tree.PLIf:
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
			b.pushContinuation(b.buildContinuation(stmts[i+1:]))
			// Build each branch of the IF statement, calling the continuation
			// function at the end construction in order to resume execution after the
			// IF block.
			thenExpr := b.buildPLpgSQLStatements(t.Then, s.push()).expr
			elseExpr := b.buildPLpgSQLStatements(t.Else, s.push()).expr
			b.popContinuation()

			// Build a scalar CASE statement that conditionally executes either branch
			// of the IF statement as a subquery.
			cond := b.buildPLpgSQLExpr(t.Cond, types.Bool, s)
			thenScalar := b.ob.factory.ConstructSubquery(thenExpr, &memo.SubqueryPrivate{})
			elseScalar := b.ob.factory.ConstructSubquery(elseExpr, &memo.SubqueryPrivate{})
			whenExpr := memo.ScalarListExpr{b.ob.factory.ConstructWhen(cond, thenScalar)}
			scalar := b.ob.factory.ConstructCase(memo.TrueSingleton, whenExpr, elseScalar)

			// Return a single column that projects the result of the CASE statement.
			returnColName := scopeColName("").WithMetadataName(b.makeIdentifier("if"))
			returnScope := s.push()
			b.ob.synthesizeColumn(returnScope, returnColName, b.returnType, nil /* expr */, scalar)
			b.ob.constructProjectForScope(s, returnScope)
			return returnScope
		case *tree.PLLoop:
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
			if t.Cond != nil {
				panic(errors.AssertionFailedf("cannot yet handle loop with condition"))
			}
			b.pushExitContinuation(b.buildContinuation(stmts[i+1:]))
			loopContinuation := b.makeContinuation()
			b.pushContinuation(loopContinuation)
			b.finishContinuation(t.Body, loopContinuation.Def)
			return b.callContinuation(&loopContinuation, s)
		case *tree.PLExit:
			// EXIT statements are handled by calling the function that executes the
			// statements after a loop. Errors if used outside a loop.
			if continuation := b.getExitContinuation(); continuation != nil {
				return b.callContinuation(continuation, s)
			} else {
				panic(errors.AssertionFailedf("called exit outside of loop!"))
			}
		case *tree.PLContinue:
			// CONTINUE statements are handled by calling the function that executes
			// the loop body. Errors if used outside a loop.
			if continuation := b.getContinuation(); continuation != nil {
				return b.callContinuation(continuation, s)
			} else {
				panic(errors.AssertionFailedf("called continue outside of loop!"))
			}
		default:
			panic(errors.AssertionFailedf("unhandled plsql operator"))
		}
	}
	if continuation := b.getContinuation(); continuation != nil {
		// If we reached the end of a statement block and RETURN has not yet been
		// called, there must be an ancestor scope that has provided a continuation
		// function that resumes control with the remaining statements.
		return b.callContinuation(continuation, s)
	} else {
		panic(errors.AssertionFailedf("PL/pgSQL function failed to return"))
	}
}

// addPLpgSQLAssign adds a PL/pgSQL assignment to the current scope as a
// new column with the variable name that projects the assigned expression.
// If there is a column with the same name in the previous scope, it will be
// replaced. This allows the plsqlBuilder to model variable mutations.
func (b *plsqlBuilder) addPLpgSQLAssign(
	inScope *scope, ident tree.Name, val *tree.PLSQLExpr,
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
		// Add the column as a pass-through column from the previous scope.
		assignScope.appendColumn(col)
	}
	// Project the assignment as a new column.
	colName := scopeColName(ident)
	scalar := b.buildPLpgSQLExpr(val, typ, inScope)
	b.ob.synthesizeColumn(assignScope, colName, typ, nil, scalar)
	b.ob.constructProjectForScope(inScope, assignScope)
	return assignScope
}

// buildContinuation returns a fully initialized continuation function that
// executes the given statements and resumes control with the parent scope.
func (b *plsqlBuilder) buildContinuation(stmts []tree.PLStatement) memo.UDFCallPrivate {
	if len(stmts) == 0 {
    if continuation := b.getContinuation(); continuation != nil {
      return *continuation
    } else {
      panic(errors.AssertionFailedf("missing return statement!"))
    }
	}
	continuation := b.makeContinuation()
	b.finishContinuation(stmts, continuation.Def)
	return continuation
}

// makeContinuation allocates a new continuation function with an uninitialized
// definition.
func (b *plsqlBuilder) makeContinuation() memo.UDFCallPrivate {
	return memo.UDFCallPrivate{
		Name:              b.makeIdentifier("block"),
		Def:               &memo.UDFDefinition{},
		Typ:               b.returnType,
		CalledOnNullInput: true,
	}
}

// finishContinuation initializes the definition of a continuation function with
// the function body. It is separate from makeContinuation to allow recursive
// function definitions.
func (b *plsqlBuilder) finishContinuation(stmts []tree.PLStatement, def *memo.UDFDefinition) {
	s := b.ob.allocScope()
	b.ensureScopeHasExpr(s)
	params := make(opt.ColList, 0, len(b.decs)+len(b.params))
	addParam := func(name tree.Name, typ *types.T) {
		colName := scopeColName(name)
		col := b.ob.synthesizeColumn(s, colName, typ, nil /* expr */, nil /* scalar */)
		col.setParamOrd(len(params))
		params = append(params, col.id)
	}
	for _, dec := range b.decs {
		addParam(dec.Ident, dec.Typ)
	}
	for _, param := range b.params {
		addParam(tree.Name(param.Name), param.Typ)
	}
	// Make sure to push s before constructing the continuation scope to ensure
	// that the parameter columns are not projected.
	continuationScope := b.buildPLpgSQLStatements(stmts, s.push())
	*def = memo.UDFDefinition{
		Body:       []memo.RelExpr{continuationScope.expr},
		BodyProps:  []*physical.Required{continuationScope.makePhysicalProps()},
		Params:     params,
		ReturnType: b.returnType,
	}
}

// callContinuation adds a column that projects the result of calling the
// given continuation function.
func (b *plsqlBuilder) callContinuation(continuation *memo.UDFCallPrivate, s *scope) *scope {
	args := make(memo.ScalarListExpr, 0, len(b.decs)+len(b.params))
	addArg := func(name tree.Name, typ *types.T) {
		_, source, _, _ := s.FindSourceProvidingColumn(b.ob.ctx, name)
		if source != nil {
			args = append(args, b.ob.factory.ConstructVariable(source.(*scopeColumn).id))
		} else {
			args = append(args, b.ob.factory.ConstructNull(typ))
		}
	}
	for _, dec := range b.decs {
		addArg(dec.Ident, dec.Typ)
	}
	for _, param := range b.params {
		addArg(tree.Name(param.Name), param.Typ)
	}
	call := b.ob.factory.ConstructUDFCall(args, continuation)

	returnColName := scopeColName("").WithMetadataName(b.makeIdentifier("continuation"))
	returnScope := s.push()
	b.ob.synthesizeColumn(returnScope, returnColName, b.returnType, nil /* expr */, call)
	b.ob.constructProjectForScope(s, returnScope)
	return returnScope
}

// buildPLpgSQLExpr parses and builds the given SQL expression into a ScalarExpr
// within the given scope.
func (b *plsqlBuilder) buildPLpgSQLExpr(
	plExpr *tree.PLSQLExpr, typ *types.T, s *scope,
) opt.ScalarExpr {
	expr, err := parser.ParseExpr(plExpr.SQL)
	if err != nil {
		panic(err)
	}
	expr, _ = tree.WalkExpr(s, expr)
	typedExpr, err := expr.TypeCheck(b.ob.ctx, b.ob.semaCtx, typ)
	if err != nil {
		panic(err)
	}
	return b.ob.buildScalar(typedExpr, s, nil, nil, b.colRefs)
}

func (b *plsqlBuilder) ensureScopeHasExpr(s *scope) {
	if s.expr == nil {
		s.expr = b.ob.factory.ConstructValues(memo.ScalarListWithEmptyTuple, &memo.ValuesPrivate{
			Cols: opt.ColList{},
			ID:   b.ob.factory.Metadata().NextUniqueID(),
		})
	}
}

func (b *plsqlBuilder) makeIdentifier(id string) string {
	b.identCounter++
	return fmt.Sprintf("_crdb_internal_%s_%d", id, b.identCounter)
}

func (b *plsqlBuilder) pushContinuation(continuation memo.UDFCallPrivate) {
	b.continuations = append(b.continuations, continuation)
}

func (b *plsqlBuilder) popContinuation() {
	if len(b.continuations) > 0 {
		b.continuations = b.continuations[:len(b.continuations)-1]
	}
}

func (b *plsqlBuilder) getContinuation() *memo.UDFCallPrivate {
	if len(b.continuations) == 0 {
		return nil
	}
	return &b.continuations[len(b.continuations)-1]
}

func (b *plsqlBuilder) pushExitContinuation(exitContinuation memo.UDFCallPrivate) {
	b.exitContinuations = append(b.exitContinuations, exitContinuation)
}

func (b *plsqlBuilder) popExitContinuation() {
	if len(b.exitContinuations) > 0 {
		b.exitContinuations = b.exitContinuations[:len(b.exitContinuations)-1]
	}
}

func (b *plsqlBuilder) getExitContinuation() *memo.UDFCallPrivate {
	if len(b.exitContinuations) == 0 {
		return nil
	}
	return &b.exitContinuations[len(b.exitContinuations)-1]
}
