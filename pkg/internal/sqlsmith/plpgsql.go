// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlsmith

import (
	"maps"

	ast "github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func (s *Smither) makeRoutineBodyPLpgSQL(
	params tree.ParamTypes, rTyp *types.T, vol tree.RoutineVolatility,
) string {
	scope := makeBlockScope(len(params), rTyp, vol)
	for i := range params {
		scope.addVariable(params[i].Name, params[i].Typ, false /* constant */)
	}
	// Add a RETURN statement to the end of the block, to avoid end-of-function
	// errors.
	block := s.makePLpgSQLBlock(scope)
	block.Body = append(block.Body, s.makePLpgSQLReturn(scope))
	return "\n" + tree.AsStringWithFlags(s.makePLpgSQLBlock(scope), tree.FmtParsable)
}

func (s *Smither) makePLpgSQLBlock(scope plpgsqlBlockScope) *ast.Block {
	const maxStmts = 11
	decls, newScope := s.makePLpgSQLDeclarations(scope)
	body := s.makePLpgSQLStatements(newScope, maxStmts)
	// TODO(#106368): optionally add a label.
	return &ast.Block{
		Decls: decls,
		Body:  body,
	}
}

func (s *Smither) makePLpgSQLVarName(prefix string, scope plpgsqlBlockScope) tree.Name {
	varName := s.name(prefix)
	for scope.hasVariable(string(varName)) {
		varName = s.name(prefix)
	}
	return varName
}

func (s *Smither) makePLpgSQLDeclarations(
	scope plpgsqlBlockScope,
) ([]ast.Statement, plpgsqlBlockScope) {
	// Create a new scope with all the outer variables included.
	numDecls := s.rnd.Intn(11)
	newScope := scope.makeChild(numDecls)

	// Now add the declarations for this block.
	// TODO(#106368): add support for cursor declarations.
	decls := make([]ast.Statement, numDecls)
	for i := 0; i < numDecls; i++ {
		varName := s.makePLpgSQLVarName("decl", newScope)
		varTyp, varTypResolvable := s.randType()
		for varTyp.Identical(types.AnyTuple) || varTyp.Family() == types.CollatedStringFamily {
			// TODO(#114874): allow record types here when they are supported.
			// TODO(#105245): allow collated strings when they are supported.
			varTyp, varTypResolvable = s.randType()
		}
		constant := s.d6() == 1
		var expr ast.Expr
		if constant || s.coin() {
			// If the variable is constant, it must be assigned here.
			expr = s.makePLpgSQLExpr(scope, varTyp)
		}
		decls[i] = &ast.Declaration{
			Var:      varName,
			Constant: constant,
			Typ:      varTypResolvable,
			Expr:     expr,
		}
		newScope.addVariable(string(varName), varTyp, constant)
	}
	return decls, newScope
}

func (s *Smither) makePLpgSQLStatements(scope plpgsqlBlockScope, maxCount int) []ast.Statement {
	numStmts := s.rnd.Intn(maxCount + 1)
	stmts := make([]ast.Statement, 0, numStmts)
	for i := 0; i < numStmts; i++ {
		for {
			// No need for a retry counter, because NULL statement creation always
			// succeeds, and eventually we will sample one and end the loop.
			stmt, ok := s.plpgsqlStmtSampler.Next()(s, scope)
			if ok {
				stmts = append(stmts, stmt)
				break
			}
		}
	}
	return stmts
}

func (s *Smither) makePLpgSQLIf(scope plpgsqlBlockScope) *ast.If {
	const maxBranchStmts = 3
	ifStmt := &ast.If{
		Condition: s.makePLpgSQLCond(scope),
		ThenBody:  s.makePLpgSQLStatements(scope, maxBranchStmts),
	}
	if s.coin() {
		numElseIfs := s.rnd.Intn(3) + 1
		ifStmt.ElseIfList = make([]ast.ElseIf, numElseIfs)
		for i := 0; i < numElseIfs; i++ {
			ifStmt.ElseIfList[i] = ast.ElseIf{
				Condition: s.makePLpgSQLCond(scope),
				Stmts:     s.makePLpgSQLStatements(scope, maxBranchStmts),
			}
		}
	}
	if s.coin() {
		ifStmt.ElseBody = s.makePLpgSQLStatements(scope, maxBranchStmts)
	}
	return ifStmt
}

func (s *Smither) makePLpgSQLReturn(scope plpgsqlBlockScope) *ast.Return {
	return &ast.Return{Expr: s.makePLpgSQLExpr(scope, scope.rTyp)}
}

func (s *Smither) makePLpgSQLExpr(scope plpgsqlBlockScope, t *types.T) ast.Expr {
	return makeScalar(s, t, scope.refs)
}

func (s *Smither) makePLpgSQLCond(scope plpgsqlBlockScope) ast.Expr {
	return makeBoolExpr(s, scope.refs)
}

// TODO(#106368): implement generation for the remaining statements.
var (
	plpgsqlStmts = []plpgsqlStatementWeight{
		{1, makePLpgSQLBlock},
		{2, makePLpgSQLReturn},
		{2, makePLpgSQLIf},
		{2, makePLpgSQLWhile},
		{2, makePLpgSQLForLoop},
		{5, makePLpgSQLNull},
		{10, makePLpgSQLAssign},
		{10, makePLpgSQLExecSQL},
	}
)

type plpgsqlStatementWeight struct {
	weight int
	elem   plpgsqlStatement
}

func makePLpgSQLBlock(s *Smither, scope plpgsqlBlockScope) (stmt ast.Statement, ok bool) {
	return s.makePLpgSQLBlock(scope), true
}

func makePLpgSQLReturn(s *Smither, scope plpgsqlBlockScope) (stmt ast.Statement, ok bool) {
	return s.makePLpgSQLReturn(scope), true
}

func makePLpgSQLIf(s *Smither, scope plpgsqlBlockScope) (stmt ast.Statement, ok bool) {
	return s.makePLpgSQLIf(scope), true
}

func makePLpgSQLAssign(s *Smither, scope plpgsqlBlockScope) (stmt ast.Statement, ok bool) {
	if len(scope.vars) == 0 {
		// There must be a variable that can be assigned.
		return nil, false
	}
	varName := scope.vars[s.rnd.Intn(len(scope.vars))]
	if scope.variableIsConstant(varName) {
		// Cannot assign to a CONSTANT variable.
		return nil, false
	}
	expr := s.makePLpgSQLExpr(scope, scope.varTypes[varName])
	return &ast.Assignment{Var: ast.Variable(varName), Value: expr}, true
}

func makePLpgSQLExecSQL(s *Smither, scope plpgsqlBlockScope) (stmt ast.Statement, ok bool) {
	const maxRetries = 5
	var sqlStmt tree.Statement
	for i := 0; i < maxRetries; i++ {
		var desiredTypes []*types.T
		var targets []ast.Variable
		if s.coin() {
			// Support INTO syntax. Pick a subset of variables to assign into.
			usedVars := make(map[string]struct{})
			numNonConstVars := len(scope.vars) - len(scope.constants)
			for len(usedVars) < numNonConstVars {
				// Pick non-constant variable that hasn't been used yet.
				var varName string
				for {
					varName = scope.vars[s.rnd.Intn(len(scope.vars))]
					if scope.variableIsConstant(varName) {
						continue
					}
					if _, used := usedVars[varName]; used {
						continue
					}
					usedVars[varName] = struct{}{}
					desiredTypes = append(desiredTypes, scope.varTypes[varName])
					targets = append(targets, tree.Name(varName))
					break
				}
				if s.coin() {
					break
				}
			}
		}
		sqlStmt, ok = s.makeSQLStmtForRoutine(scope.vol, scope.refs, desiredTypes)
		if ok {
			return &ast.Execute{
				SqlStmt: sqlStmt,
				// Strict option won't matter if targets is empty.
				Strict: s.d6() == 1,
				Target: targets,
			}, true
		}
	}
	return nil, false
}

func makePLpgSQLNull(_ *Smither, _ plpgsqlBlockScope) (stmt ast.Statement, ok bool) {
	return &ast.Null{}, true
}

func makePLpgSQLForLoop(s *Smither, scope plpgsqlBlockScope) (stmt ast.Statement, ok bool) {
	// TODO(#105246): add support for other query and cursor FOR loops.
	control := ast.IntForLoopControl{
		Reverse: s.coin(),
		Lower:   s.makePLpgSQLExpr(scope, types.Int),
		Upper:   s.makePLpgSQLExpr(scope, types.Int),
	}
	if s.coin() {
		control.Step = s.makePLpgSQLExpr(scope, types.Int)
	}
	newScope := scope.makeChild(1 /* numNewVars */)
	loopVarName := s.makePLpgSQLVarName("loop", newScope)
	newScope.addVariable(string(loopVarName), types.Int, false /* constant */)
	const maxLoopStmts = 3
	return &ast.ForLoop{
		// TODO(#106368): optionally add a label.
		Target:  []ast.Variable{loopVarName},
		Control: &control,
		Body:    s.makePLpgSQLStatements(newScope, maxLoopStmts),
	}, true
}

func makePLpgSQLWhile(s *Smither, scope plpgsqlBlockScope) (stmt ast.Statement, ok bool) {
	const maxLoopStmts = 3
	return &ast.While{
		// TODO(#106368): optionally add a label.
		Condition: s.makePLpgSQLCond(scope),
		Body:      s.makePLpgSQLStatements(scope, maxLoopStmts),
	}, true
}

// plpgsqlBlockScope holds the information needed to ensure that generated
// statements obey PL/pgSQL syntax and scoping rules.
type plpgsqlBlockScope struct {
	// varTypes is a mapping from each variable name to its type.
	varTypes map[string]*types.T

	// constants tracks the variables that have been declared CONSTANT.
	constants map[string]struct{}

	// vars is a list of the names of every variable that is in scope for the
	// current block.
	vars []string

	// refs is the list of colRefs for every variable in the current scope. It
	// could be rebuilt from the vars and varTypes fields, but is kept up-to-date
	// here for convenience.
	refs colRefs

	// rTyp is the return type of the routine.
	rTyp *types.T

	// vol is the volatility of the routine.
	vol tree.RoutineVolatility
}

func makeBlockScope(numVars int, rTyp *types.T, vol tree.RoutineVolatility) plpgsqlBlockScope {
	return plpgsqlBlockScope{
		varTypes:  make(map[string]*types.T),
		constants: make(map[string]struct{}),
		vars:      make([]string, 0, numVars),
		refs:      make(colRefs, 0, numVars),
		rTyp:      rTyp,
		vol:       vol,
	}
}

func (s *plpgsqlBlockScope) makeChild(numNewVars int) plpgsqlBlockScope {
	totalNumVars := len(s.vars) + numNewVars
	newScope := plpgsqlBlockScope{
		varTypes:  maps.Clone(s.varTypes),
		constants: maps.Clone(s.constants),
		vars:      make([]string, 0, totalNumVars),
		refs:      make(colRefs, 0, totalNumVars),
		rTyp:      s.rTyp,
		vol:       s.vol,
	}
	newScope.vars = append(newScope.vars, s.vars...)
	newScope.refs = append(newScope.refs, s.refs...)
	return newScope
}

func (s *plpgsqlBlockScope) hasVariable(name string) bool {
	return s.varTypes[name] != nil
}

func (s *plpgsqlBlockScope) variableIsConstant(name string) bool {
	_, isConstant := s.constants[name]
	return isConstant
}

func (s *plpgsqlBlockScope) addVariable(name string, typ *types.T, constant bool) {
	if s.hasVariable(name) {
		// TODO(#117508): remove this check when variable shadowing is allowed.
		panic(errors.AssertionFailedf("cannot shadow variable %s", name))
	}
	s.varTypes[name] = typ
	s.vars = append(s.vars, name)
	s.refs = append(s.refs, &colRef{typ: typ, item: &tree.ColumnItem{ColumnName: tree.Name(name)}})
	if constant {
		s.constants[name] = struct{}{}
	}
}
