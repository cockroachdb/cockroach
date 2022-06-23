package simple

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"honnef.co/go/tools/analysis/code"
	"honnef.co/go/tools/analysis/edit"
	"honnef.co/go/tools/analysis/lint"
	"honnef.co/go/tools/analysis/report"
	"honnef.co/go/tools/go/ast/astutil"
	"honnef.co/go/tools/go/types/typeutil"
	"honnef.co/go/tools/internal/passes/buildir"
	"honnef.co/go/tools/internal/sharedcheck"
	"honnef.co/go/tools/knowledge"
	"honnef.co/go/tools/pattern"

	"golang.org/x/exp/typeparams"
	"golang.org/x/tools/go/analysis"
)

var (
	checkSingleCaseSelectQ1 = pattern.MustParse(`
		(ForStmt
			nil nil nil
			select@(SelectStmt
				(CommClause
					(Or
						(UnaryExpr "<-" _)
						(AssignStmt _ _ (UnaryExpr "<-" _)))
					_)))`)
	checkSingleCaseSelectQ2 = pattern.MustParse(`(SelectStmt (CommClause _ _))`)
)

func CheckSingleCaseSelect(pass *analysis.Pass) (interface{}, error) {
	seen := map[ast.Node]struct{}{}
	fn := func(node ast.Node) {
		if m, ok := code.Match(pass, checkSingleCaseSelectQ1, node); ok {
			seen[m.State["select"].(ast.Node)] = struct{}{}
			report.Report(pass, node, "should use for range instead of for { select {} }", report.FilterGenerated())
		} else if _, ok := code.Match(pass, checkSingleCaseSelectQ2, node); ok {
			if _, ok := seen[node]; !ok {
				report.Report(pass, node, "should use a simple channel send/receive instead of select with a single case",
					report.ShortRange(),
					report.FilterGenerated())
			}
		}
	}
	code.Preorder(pass, fn, (*ast.ForStmt)(nil), (*ast.SelectStmt)(nil))
	return nil, nil
}

var (
	checkLoopCopyQ = pattern.MustParse(`
		(Or
			(RangeStmt
				key@(Ident _) value@(Ident _) ":=" src
				[(AssignStmt (IndexExpr dst key) "=" value)])
			(RangeStmt
				key@(Ident _) nil ":=" src
				[(AssignStmt (IndexExpr dst key) "=" (IndexExpr src key))])
			(ForStmt
				(AssignStmt key@(Ident _) ":=" (IntegerLiteral "0"))
				(BinaryExpr key "<" (CallExpr (Symbol "len") [src]))
				(IncDecStmt key "++")
				[(AssignStmt (IndexExpr dst key) "=" (IndexExpr src key))]))`)
)

func CheckLoopCopy(pass *analysis.Pass) (interface{}, error) {
	// TODO revisit once range doesn't require a structural type

	isInvariant := func(k, v types.Object, node ast.Expr) bool {
		if code.MayHaveSideEffects(pass, node, nil) {
			return false
		}
		invariant := true
		ast.Inspect(node, func(node ast.Node) bool {
			if node, ok := node.(*ast.Ident); ok {
				obj := pass.TypesInfo.ObjectOf(node)
				if obj == k || obj == v {
					// don't allow loop bodies like 'a[i][i] = v'
					invariant = false
					return false
				}
			}
			return true
		})
		return invariant
	}

	var elType func(T types.Type) (el types.Type, isArray bool, isArrayPointer bool, ok bool)
	elType = func(T types.Type) (el types.Type, isArray bool, isArrayPointer bool, ok bool) {
		switch typ := T.Underlying().(type) {
		case *types.Slice:
			return typ.Elem(), false, false, true
		case *types.Array:
			return typ.Elem(), true, false, true
		case *types.Pointer:
			el, isArray, _, ok = elType(typ.Elem())
			return el, isArray, true, ok
		default:
			return nil, false, false, false
		}
	}

	fn := func(node ast.Node) {
		m, ok := code.Match(pass, checkLoopCopyQ, node)
		if !ok {
			return
		}

		src := m.State["src"].(ast.Expr)
		dst := m.State["dst"].(ast.Expr)

		k := pass.TypesInfo.ObjectOf(m.State["key"].(*ast.Ident))
		var v types.Object
		if value, ok := m.State["value"]; ok {
			v = pass.TypesInfo.ObjectOf(value.(*ast.Ident))
		}
		if !isInvariant(k, v, dst) {
			return
		}
		if !isInvariant(k, v, src) {
			// For example: 'for i := range foo()'
			return
		}

		Tsrc := pass.TypesInfo.TypeOf(src)
		Tdst := pass.TypesInfo.TypeOf(dst)
		TsrcElem, TsrcArray, TsrcPointer, ok := elType(Tsrc)
		if !ok {
			return
		}
		if TsrcPointer {
			Tsrc = Tsrc.Underlying().(*types.Pointer).Elem()
		}
		TdstElem, TdstArray, TdstPointer, ok := elType(Tdst)
		if !ok {
			return
		}
		if TdstPointer {
			Tdst = Tdst.Underlying().(*types.Pointer).Elem()
		}

		if !types.Identical(TsrcElem, TdstElem) {
			return
		}

		if TsrcArray && TdstArray && types.Identical(Tsrc, Tdst) {
			if TsrcPointer {
				src = &ast.StarExpr{
					X: src,
				}
			}
			if TdstPointer {
				dst = &ast.StarExpr{
					X: dst,
				}
			}
			r := &ast.AssignStmt{
				Lhs: []ast.Expr{dst},
				Rhs: []ast.Expr{src},
				Tok: token.ASSIGN,
			}

			report.Report(pass, node, "should copy arrays using assignment instead of using a loop",
				report.FilterGenerated(),
				report.ShortRange(),
				report.Fixes(edit.Fix("replace loop with assignment", edit.ReplaceWithNode(pass.Fset, node, r))))
		} else {
			opts := []report.Option{
				report.ShortRange(),
				report.FilterGenerated(),
			}
			tv, err := types.Eval(pass.Fset, pass.Pkg, node.Pos(), "copy")
			if err == nil && tv.IsBuiltin() {
				src := m.State["src"].(ast.Expr)
				if TsrcArray {
					src = &ast.SliceExpr{
						X: src,
					}
				}
				dst := m.State["dst"].(ast.Expr)
				if TdstArray {
					dst = &ast.SliceExpr{
						X: dst,
					}
				}

				r := &ast.CallExpr{
					Fun:  &ast.Ident{Name: "copy"},
					Args: []ast.Expr{dst, src},
				}
				opts = append(opts, report.Fixes(edit.Fix("replace loop with call to copy()", edit.ReplaceWithNode(pass.Fset, node, r))))
			}
			report.Report(pass, node, "should use copy() instead of a loop", opts...)
		}
	}
	code.Preorder(pass, fn, (*ast.ForStmt)(nil), (*ast.RangeStmt)(nil))
	return nil, nil
}

func CheckIfBoolCmp(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		if code.IsInTest(pass, node) {
			return
		}

		expr := node.(*ast.BinaryExpr)
		if expr.Op != token.EQL && expr.Op != token.NEQ {
			return
		}
		x := code.IsBoolConst(pass, expr.X)
		y := code.IsBoolConst(pass, expr.Y)
		if !x && !y {
			return
		}
		var other ast.Expr
		var val bool
		if x {
			val = code.BoolConst(pass, expr.X)
			other = expr.Y
		} else {
			val = code.BoolConst(pass, expr.Y)
			other = expr.X
		}

		ok := typeutil.All(pass.TypesInfo.TypeOf(other), func(term *typeparams.Term) bool {
			basic, ok := term.Type().Underlying().(*types.Basic)
			return ok && basic.Kind() == types.Bool
		})
		if !ok {
			return
		}
		op := ""
		if (expr.Op == token.EQL && !val) || (expr.Op == token.NEQ && val) {
			op = "!"
		}
		r := op + report.Render(pass, other)
		l1 := len(r)
		r = strings.TrimLeft(r, "!")
		if (l1-len(r))%2 == 1 {
			r = "!" + r
		}
		report.Report(pass, expr, fmt.Sprintf("should omit comparison to bool constant, can be simplified to %s", r),
			report.FilterGenerated(),
			report.Fixes(edit.Fix("simplify bool comparison", edit.ReplaceWithString(expr, r))))
	}
	code.Preorder(pass, fn, (*ast.BinaryExpr)(nil))
	return nil, nil
}

var (
	checkBytesBufferConversionsQ  = pattern.MustParse(`(CallExpr _ [(CallExpr sel@(SelectorExpr recv _) [])])`)
	checkBytesBufferConversionsRs = pattern.MustParse(`(CallExpr (SelectorExpr recv (Ident "String")) [])`)
	checkBytesBufferConversionsRb = pattern.MustParse(`(CallExpr (SelectorExpr recv (Ident "Bytes")) [])`)
)

func CheckBytesBufferConversions(pass *analysis.Pass) (interface{}, error) {
	if pass.Pkg.Path() == "bytes" || pass.Pkg.Path() == "bytes_test" {
		// The bytes package can use itself however it wants
		return nil, nil
	}
	fn := func(node ast.Node, stack []ast.Node) {
		m, ok := code.Match(pass, checkBytesBufferConversionsQ, node)
		if !ok {
			return
		}
		call := node.(*ast.CallExpr)
		sel := m.State["sel"].(*ast.SelectorExpr)

		typ := pass.TypesInfo.TypeOf(call.Fun)
		if typ == types.Universe.Lookup("string").Type() && code.IsCallTo(pass, call.Args[0], "(*bytes.Buffer).Bytes") {
			if _, ok := stack[len(stack)-2].(*ast.IndexExpr); ok {
				// Don't flag m[string(buf.Bytes())] – thanks to a
				// compiler optimization, this is actually faster than
				// m[buf.String()]
				return
			}

			report.Report(pass, call, fmt.Sprintf("should use %v.String() instead of %v", report.Render(pass, sel.X), report.Render(pass, call)),
				report.FilterGenerated(),
				report.Fixes(edit.Fix("simplify conversion", edit.ReplaceWithPattern(pass.Fset, node, checkBytesBufferConversionsRs, m.State))))
		} else if typ, ok := typ.(*types.Slice); ok && typ.Elem() == types.Universe.Lookup("byte").Type() && code.IsCallTo(pass, call.Args[0], "(*bytes.Buffer).String") {
			report.Report(pass, call, fmt.Sprintf("should use %v.Bytes() instead of %v", report.Render(pass, sel.X), report.Render(pass, call)),
				report.FilterGenerated(),
				report.Fixes(edit.Fix("simplify conversion", edit.ReplaceWithPattern(pass.Fset, node, checkBytesBufferConversionsRb, m.State))))
		}

	}
	code.PreorderStack(pass, fn, (*ast.CallExpr)(nil))
	return nil, nil
}

func CheckStringsContains(pass *analysis.Pass) (interface{}, error) {
	// map of value to token to bool value
	allowed := map[int64]map[token.Token]bool{
		-1: {token.GTR: true, token.NEQ: true, token.EQL: false},
		0:  {token.GEQ: true, token.LSS: false},
	}
	fn := func(node ast.Node) {
		expr := node.(*ast.BinaryExpr)
		switch expr.Op {
		case token.GEQ, token.GTR, token.NEQ, token.LSS, token.EQL:
		default:
			return
		}

		value, ok := code.ExprToInt(pass, expr.Y)
		if !ok {
			return
		}

		allowedOps, ok := allowed[value]
		if !ok {
			return
		}
		b, ok := allowedOps[expr.Op]
		if !ok {
			return
		}

		call, ok := expr.X.(*ast.CallExpr)
		if !ok {
			return
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return
		}
		pkgIdent, ok := sel.X.(*ast.Ident)
		if !ok {
			return
		}
		funIdent := sel.Sel
		if pkgIdent.Name != "strings" && pkgIdent.Name != "bytes" {
			return
		}

		var r ast.Expr
		switch funIdent.Name {
		case "IndexRune":
			r = &ast.SelectorExpr{
				X:   pkgIdent,
				Sel: &ast.Ident{Name: "ContainsRune"},
			}
		case "IndexAny":
			r = &ast.SelectorExpr{
				X:   pkgIdent,
				Sel: &ast.Ident{Name: "ContainsAny"},
			}
		case "Index":
			r = &ast.SelectorExpr{
				X:   pkgIdent,
				Sel: &ast.Ident{Name: "Contains"},
			}
		default:
			return
		}

		r = &ast.CallExpr{
			Fun:  r,
			Args: call.Args,
		}
		if !b {
			r = &ast.UnaryExpr{
				Op: token.NOT,
				X:  r,
			}
		}

		report.Report(pass, node, fmt.Sprintf("should use %s instead", report.Render(pass, r)),
			report.FilterGenerated(),
			report.Fixes(edit.Fix(fmt.Sprintf("simplify use of %s", report.Render(pass, call.Fun)), edit.ReplaceWithNode(pass.Fset, node, r))))
	}
	code.Preorder(pass, fn, (*ast.BinaryExpr)(nil))
	return nil, nil
}

var (
	checkBytesCompareQ  = pattern.MustParse(`(BinaryExpr (CallExpr (Symbol "bytes.Compare") args) op@(Or "==" "!=") (IntegerLiteral "0"))`)
	checkBytesCompareRe = pattern.MustParse(`(CallExpr (SelectorExpr (Ident "bytes") (Ident "Equal")) args)`)
	checkBytesCompareRn = pattern.MustParse(`(UnaryExpr "!" (CallExpr (SelectorExpr (Ident "bytes") (Ident "Equal")) args))`)
)

func CheckBytesCompare(pass *analysis.Pass) (interface{}, error) {
	if pass.Pkg.Path() == "bytes" || pass.Pkg.Path() == "bytes_test" {
		// the bytes package is free to use bytes.Compare as it sees fit
		return nil, nil
	}
	fn := func(node ast.Node) {
		m, ok := code.Match(pass, checkBytesCompareQ, node)
		if !ok {
			return
		}

		args := report.RenderArgs(pass, m.State["args"].([]ast.Expr))
		prefix := ""
		if m.State["op"].(token.Token) == token.NEQ {
			prefix = "!"
		}

		var fix analysis.SuggestedFix
		switch tok := m.State["op"].(token.Token); tok {
		case token.EQL:
			fix = edit.Fix("simplify use of bytes.Compare", edit.ReplaceWithPattern(pass.Fset, node, checkBytesCompareRe, m.State))
		case token.NEQ:
			fix = edit.Fix("simplify use of bytes.Compare", edit.ReplaceWithPattern(pass.Fset, node, checkBytesCompareRn, m.State))
		default:
			panic(fmt.Sprintf("unexpected token %v", tok))
		}
		report.Report(pass, node, fmt.Sprintf("should use %sbytes.Equal(%s) instead", prefix, args), report.FilterGenerated(), report.Fixes(fix))
	}
	code.Preorder(pass, fn, (*ast.BinaryExpr)(nil))
	return nil, nil
}

func CheckForTrue(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		loop := node.(*ast.ForStmt)
		if loop.Init != nil || loop.Post != nil {
			return
		}
		if !code.IsBoolConst(pass, loop.Cond) || !code.BoolConst(pass, loop.Cond) {
			return
		}
		report.Report(pass, loop, "should use for {} instead of for true {}",
			report.ShortRange(),
			report.FilterGenerated())
	}
	code.Preorder(pass, fn, (*ast.ForStmt)(nil))
	return nil, nil
}

func CheckRegexpRaw(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		call := node.(*ast.CallExpr)
		if !code.IsCallToAny(pass, call, "regexp.MustCompile", "regexp.Compile") {
			return
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return
		}
		lit, ok := call.Args[knowledge.Arg("regexp.Compile.expr")].(*ast.BasicLit)
		if !ok {
			// TODO(dominikh): support string concat, maybe support constants
			return
		}
		if lit.Kind != token.STRING {
			// invalid function call
			return
		}
		if lit.Value[0] != '"' {
			// already a raw string
			return
		}
		val := lit.Value
		if !strings.Contains(val, `\\`) {
			return
		}
		if strings.Contains(val, "`") {
			return
		}

		bs := false
		for _, c := range val {
			if !bs && c == '\\' {
				bs = true
				continue
			}
			if bs && c == '\\' {
				bs = false
				continue
			}
			if bs {
				// backslash followed by non-backslash -> escape sequence
				return
			}
		}

		report.Report(pass, call, fmt.Sprintf("should use raw string (`...`) with regexp.%s to avoid having to escape twice", sel.Sel.Name), report.FilterGenerated())
	}
	code.Preorder(pass, fn, (*ast.CallExpr)(nil))
	return nil, nil
}

var (
	checkIfReturnQIf  = pattern.MustParse(`(IfStmt nil cond [(ReturnStmt [ret@(Builtin (Or "true" "false"))])] nil)`)
	checkIfReturnQRet = pattern.MustParse(`(ReturnStmt [ret@(Builtin (Or "true" "false"))])`)
)

func CheckIfReturn(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		block := node.(*ast.BlockStmt)
		l := len(block.List)
		if l < 2 {
			return
		}
		n1, n2 := block.List[l-2], block.List[l-1]

		if len(block.List) >= 3 {
			if _, ok := block.List[l-3].(*ast.IfStmt); ok {
				// Do not flag a series of if statements
				return
			}
		}
		m1, ok := code.Match(pass, checkIfReturnQIf, n1)
		if !ok {
			return
		}
		m2, ok := code.Match(pass, checkIfReturnQRet, n2)
		if !ok {
			return
		}

		if op, ok := m1.State["cond"].(*ast.BinaryExpr); ok {
			switch op.Op {
			case token.EQL, token.LSS, token.GTR, token.NEQ, token.LEQ, token.GEQ:
			default:
				return
			}
		}

		ret1 := m1.State["ret"].(*ast.Ident)
		ret2 := m2.State["ret"].(*ast.Ident)

		if ret1.Name == ret2.Name {
			// we want the function to return true and false, not the
			// same value both times.
			return
		}

		cond := m1.State["cond"].(ast.Expr)
		origCond := cond
		if ret1.Name == "false" {
			cond = negate(cond)
		}
		report.Report(pass, n1,
			fmt.Sprintf("should use 'return %s' instead of 'if %s { return %s }; return %s'",
				report.Render(pass, cond),
				report.Render(pass, origCond), report.Render(pass, ret1), report.Render(pass, ret2)),
			report.FilterGenerated())
	}
	code.Preorder(pass, fn, (*ast.BlockStmt)(nil))
	return nil, nil
}

func negate(expr ast.Expr) ast.Expr {
	switch expr := expr.(type) {
	case *ast.BinaryExpr:
		out := *expr
		switch expr.Op {
		case token.EQL:
			out.Op = token.NEQ
		case token.LSS:
			out.Op = token.GEQ
		case token.GTR:
			out.Op = token.LEQ
		case token.NEQ:
			out.Op = token.EQL
		case token.LEQ:
			out.Op = token.GTR
		case token.GEQ:
			out.Op = token.LSS
		}
		return &out
	case *ast.Ident, *ast.CallExpr, *ast.IndexExpr, *ast.StarExpr:
		return &ast.UnaryExpr{
			Op: token.NOT,
			X:  expr,
		}
	case *ast.UnaryExpr:
		if expr.Op == token.NOT {
			return expr.X
		}
		return &ast.UnaryExpr{
			Op: token.NOT,
			X:  expr,
		}
	default:
		return &ast.UnaryExpr{
			Op: token.NOT,
			X: &ast.ParenExpr{
				X: expr,
			},
		}
	}
}

// CheckRedundantNilCheckWithLen checks for the following redundant nil-checks:
//
//   if x == nil || len(x) == 0 {}
//   if x != nil && len(x) != 0 {}
//   if x != nil && len(x) == N {} (where N != 0)
//   if x != nil && len(x) > N {}
//   if x != nil && len(x) >= N {} (where N != 0)
//
func CheckRedundantNilCheckWithLen(pass *analysis.Pass) (interface{}, error) {
	isConstZero := func(expr ast.Expr) (isConst bool, isZero bool) {
		_, ok := expr.(*ast.BasicLit)
		if ok {
			return true, code.IsIntegerLiteral(pass, expr, constant.MakeInt64(0))
		}
		id, ok := expr.(*ast.Ident)
		if !ok {
			return false, false
		}
		c, ok := pass.TypesInfo.ObjectOf(id).(*types.Const)
		if !ok {
			return false, false
		}
		return true, c.Val().Kind() == constant.Int && c.Val().String() == "0"
	}

	fn := func(node ast.Node) {
		// check that expr is "x || y" or "x && y"
		expr := node.(*ast.BinaryExpr)
		if expr.Op != token.LOR && expr.Op != token.LAND {
			return
		}
		eqNil := expr.Op == token.LOR

		// check that x is "xx == nil" or "xx != nil"
		x, ok := expr.X.(*ast.BinaryExpr)
		if !ok {
			return
		}
		if eqNil && x.Op != token.EQL {
			return
		}
		if !eqNil && x.Op != token.NEQ {
			return
		}
		xx, ok := x.X.(*ast.Ident)
		if !ok {
			return
		}
		if !code.IsNil(pass, x.Y) {
			return
		}

		// check that y is "len(xx) == 0" or "len(xx) ... "
		y, ok := expr.Y.(*ast.BinaryExpr)
		if !ok {
			return
		}
		if eqNil && y.Op != token.EQL { // must be len(xx) *==* 0
			return
		}
		yx, ok := y.X.(*ast.CallExpr)
		if !ok {
			return
		}
		if !code.IsCallTo(pass, yx, "len") {
			return
		}
		yxArg, ok := yx.Args[knowledge.Arg("len.v")].(*ast.Ident)
		if !ok {
			return
		}
		if yxArg.Name != xx.Name {
			return
		}

		if eqNil && !code.IsIntegerLiteral(pass, y.Y, constant.MakeInt64(0)) { // must be len(x) == *0*
			return
		}

		if !eqNil {
			isConst, isZero := isConstZero(y.Y)
			if !isConst {
				return
			}
			switch y.Op {
			case token.EQL:
				// avoid false positive for "xx != nil && len(xx) == 0"
				if isZero {
					return
				}
			case token.GEQ:
				// avoid false positive for "xx != nil && len(xx) >= 0"
				if isZero {
					return
				}
			case token.NEQ:
				// avoid false positive for "xx != nil && len(xx) != <non-zero>"
				if !isZero {
					return
				}
			case token.GTR:
				// ok
			default:
				return
			}
		}

		// finally check that xx type is one of array, slice, map or chan
		// this is to prevent false positive in case if xx is a pointer to an array
		typ := pass.TypesInfo.TypeOf(xx)
		ok = typeutil.All(typ, func(term *typeparams.Term) bool {
			switch term.Type().Underlying().(type) {
			case *types.Slice:
				return true
			case *types.Map:
				return true
			case *types.Chan:
				return true
			case *types.Pointer:
				return false
			case *typeparams.TypeParam:
				return false
			default:
				lint.ExhaustiveTypeSwitch(term.Type().Underlying())
				return false
			}
		})
		if !ok {
			return
		}

		report.Report(pass, expr, fmt.Sprintf("should omit nil check; len() for %s is defined as zero", typ), report.FilterGenerated())
	}
	code.Preorder(pass, fn, (*ast.BinaryExpr)(nil))
	return nil, nil
}

var checkSlicingQ = pattern.MustParse(`(SliceExpr x@(Object _) low (CallExpr (Builtin "len") [x]) nil)`)

func CheckSlicing(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		if _, ok := code.Match(pass, checkSlicingQ, node); ok {
			expr := node.(*ast.SliceExpr)
			report.Report(pass, expr.High,
				"should omit second index in slice, s[a:len(s)] is identical to s[a:]",
				report.FilterGenerated(),
				report.Fixes(edit.Fix("simplify slice expression", edit.Delete(expr.High))))
		}
	}
	code.Preorder(pass, fn, (*ast.SliceExpr)(nil))
	return nil, nil
}

func refersTo(pass *analysis.Pass, expr ast.Expr, ident types.Object) bool {
	found := false
	fn := func(node ast.Node) bool {
		ident2, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		if ident == pass.TypesInfo.ObjectOf(ident2) {
			found = true
			return false
		}
		return true
	}
	ast.Inspect(expr, fn)
	return found
}

var checkLoopAppendQ = pattern.MustParse(`
	(RangeStmt
		(Ident "_")
		val@(Object _)
		_
		x
		[(AssignStmt [lhs] "=" [(CallExpr (Builtin "append") [lhs val])])]) `)

func CheckLoopAppend(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		m, ok := code.Match(pass, checkLoopAppendQ, node)
		if !ok {
			return
		}

		val := m.State["val"].(types.Object)
		if refersTo(pass, m.State["lhs"].(ast.Expr), val) {
			return
		}

		src := pass.TypesInfo.TypeOf(m.State["x"].(ast.Expr))
		dst := pass.TypesInfo.TypeOf(m.State["lhs"].(ast.Expr))
		if !types.Identical(src, dst) {
			return
		}

		r := &ast.AssignStmt{
			Lhs: []ast.Expr{m.State["lhs"].(ast.Expr)},
			Tok: token.ASSIGN,
			Rhs: []ast.Expr{
				&ast.CallExpr{
					Fun: &ast.Ident{Name: "append"},
					Args: []ast.Expr{
						m.State["lhs"].(ast.Expr),
						m.State["x"].(ast.Expr),
					},
					Ellipsis: 1,
				},
			},
		}

		report.Report(pass, node, fmt.Sprintf("should replace loop with %s", report.Render(pass, r)),
			report.ShortRange(),
			report.FilterGenerated(),
			report.Fixes(edit.Fix("replace loop with call to append", edit.ReplaceWithNode(pass.Fset, node, r))))
	}
	code.Preorder(pass, fn, (*ast.RangeStmt)(nil))
	return nil, nil
}

var (
	checkTimeSinceQ = pattern.MustParse(`(CallExpr (SelectorExpr (CallExpr (Symbol "time.Now") []) (Symbol "(time.Time).Sub")) [arg])`)
	checkTimeSinceR = pattern.MustParse(`(CallExpr (SelectorExpr (Ident "time") (Ident "Since")) [arg])`)
)

func CheckTimeSince(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		if _, edits, ok := code.MatchAndEdit(pass, checkTimeSinceQ, checkTimeSinceR, node); ok {
			report.Report(pass, node, "should use time.Since instead of time.Now().Sub",
				report.FilterGenerated(),
				report.Fixes(edit.Fix("replace with call to time.Since", edits...)))
		}
	}
	code.Preorder(pass, fn, (*ast.CallExpr)(nil))
	return nil, nil
}

var (
	checkTimeUntilQ = pattern.MustParse(`(CallExpr (Symbol "(time.Time).Sub") [(CallExpr (Symbol "time.Now") [])])`)
	checkTimeUntilR = pattern.MustParse(`(CallExpr (SelectorExpr (Ident "time") (Ident "Until")) [arg])`)
)

func CheckTimeUntil(pass *analysis.Pass) (interface{}, error) {
	if !code.IsGoVersion(pass, 8) {
		return nil, nil
	}
	fn := func(node ast.Node) {
		if _, ok := code.Match(pass, checkTimeUntilQ, node); ok {
			if sel, ok := node.(*ast.CallExpr).Fun.(*ast.SelectorExpr); ok {
				r := pattern.NodeToAST(checkTimeUntilR.Root, map[string]interface{}{"arg": sel.X}).(ast.Node)
				report.Report(pass, node, "should use time.Until instead of t.Sub(time.Now())",
					report.FilterGenerated(),
					report.Fixes(edit.Fix("replace with call to time.Until", edit.ReplaceWithNode(pass.Fset, node, r))))
			} else {
				report.Report(pass, node, "should use time.Until instead of t.Sub(time.Now())", report.FilterGenerated())
			}
		}
	}
	code.Preorder(pass, fn, (*ast.CallExpr)(nil))
	return nil, nil
}

var (
	checkUnnecessaryBlankQ1 = pattern.MustParse(`
		(AssignStmt
			[_ (Ident "_")]
			_
			(Or
				(IndexExpr _ _)
				(UnaryExpr "<-" _))) `)
	checkUnnecessaryBlankQ2 = pattern.MustParse(`
		(AssignStmt
			(Ident "_") _ recv@(UnaryExpr "<-" _))`)
)

func CheckUnnecessaryBlank(pass *analysis.Pass) (interface{}, error) {
	fn1 := func(node ast.Node) {
		if _, ok := code.Match(pass, checkUnnecessaryBlankQ1, node); ok {
			r := *node.(*ast.AssignStmt)
			r.Lhs = r.Lhs[0:1]
			report.Report(pass, node, "unnecessary assignment to the blank identifier",
				report.FilterGenerated(),
				report.Fixes(edit.Fix("remove assignment to blank identifier", edit.ReplaceWithNode(pass.Fset, node, &r))))
		} else if m, ok := code.Match(pass, checkUnnecessaryBlankQ2, node); ok {
			report.Report(pass, node, "unnecessary assignment to the blank identifier",
				report.FilterGenerated(),
				report.Fixes(edit.Fix("simplify channel receive operation", edit.ReplaceWithNode(pass.Fset, node, m.State["recv"].(ast.Node)))))
		}
	}

	fn3 := func(node ast.Node) {
		rs := node.(*ast.RangeStmt)

		// for _
		if rs.Value == nil && astutil.IsBlank(rs.Key) {
			report.Report(pass, rs.Key, "unnecessary assignment to the blank identifier",
				report.FilterGenerated(),
				report.Fixes(edit.Fix("remove assignment to blank identifier", edit.Delete(edit.Range{rs.Key.Pos(), rs.TokPos + 1}))))
		}

		// for _, _
		if astutil.IsBlank(rs.Key) && astutil.IsBlank(rs.Value) {
			// FIXME we should mark both key and value
			report.Report(pass, rs.Key, "unnecessary assignment to the blank identifier",
				report.FilterGenerated(),
				report.Fixes(edit.Fix("remove assignment to blank identifier", edit.Delete(edit.Range{rs.Key.Pos(), rs.TokPos + 1}))))
		}

		// for x, _
		if !astutil.IsBlank(rs.Key) && astutil.IsBlank(rs.Value) {
			report.Report(pass, rs.Value, "unnecessary assignment to the blank identifier",
				report.FilterGenerated(),
				report.Fixes(edit.Fix("remove assignment to blank identifier", edit.Delete(edit.Range{rs.Key.End(), rs.Value.End()}))))
		}
	}

	code.Preorder(pass, fn1, (*ast.AssignStmt)(nil))
	if code.IsGoVersion(pass, 4) {
		code.Preorder(pass, fn3, (*ast.RangeStmt)(nil))
	}
	return nil, nil
}

func CheckSimplerStructConversion(pass *analysis.Pass) (interface{}, error) {
	// TODO(dh): support conversions between type parameters
	fn := func(node ast.Node, stack []ast.Node) {
		if unary, ok := stack[len(stack)-2].(*ast.UnaryExpr); ok && unary.Op == token.AND {
			// Do not suggest type conversion between pointers
			return
		}

		lit := node.(*ast.CompositeLit)
		typ1, _ := pass.TypesInfo.TypeOf(lit.Type).(*types.Named)
		if typ1 == nil {
			return
		}
		s1, ok := typ1.Underlying().(*types.Struct)
		if !ok {
			return
		}

		var typ2 *types.Named
		var ident *ast.Ident
		getSelType := func(expr ast.Expr) (types.Type, *ast.Ident, bool) {
			sel, ok := expr.(*ast.SelectorExpr)
			if !ok {
				return nil, nil, false
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok {
				return nil, nil, false
			}
			typ := pass.TypesInfo.TypeOf(sel.X)
			return typ, ident, typ != nil
		}
		if len(lit.Elts) == 0 {
			return
		}
		if s1.NumFields() != len(lit.Elts) {
			return
		}
		for i, elt := range lit.Elts {
			var t types.Type
			var id *ast.Ident
			var ok bool
			switch elt := elt.(type) {
			case *ast.SelectorExpr:
				t, id, ok = getSelType(elt)
				if !ok {
					return
				}
				if i >= s1.NumFields() || s1.Field(i).Name() != elt.Sel.Name {
					return
				}
			case *ast.KeyValueExpr:
				var sel *ast.SelectorExpr
				sel, ok = elt.Value.(*ast.SelectorExpr)
				if !ok {
					return
				}

				if elt.Key.(*ast.Ident).Name != sel.Sel.Name {
					return
				}
				t, id, ok = getSelType(elt.Value)
			}
			if !ok {
				return
			}
			// All fields must be initialized from the same object
			if ident != nil && ident.Obj != id.Obj {
				return
			}
			typ2, _ = t.(*types.Named)
			if typ2 == nil {
				return
			}
			ident = id
		}

		if typ2 == nil {
			return
		}

		if typ1.Obj().Pkg() != typ2.Obj().Pkg() {
			// Do not suggest type conversions between different
			// packages. Types in different packages might only match
			// by coincidence. Furthermore, if the dependency ever
			// adds more fields to its type, it could break the code
			// that relies on the type conversion to work.
			return
		}

		s2, ok := typ2.Underlying().(*types.Struct)
		if !ok {
			return
		}
		if typ1 == typ2 {
			return
		}
		if code.IsGoVersion(pass, 8) {
			if !types.IdenticalIgnoreTags(s1, s2) {
				return
			}
		} else {
			if !types.Identical(s1, s2) {
				return
			}
		}

		r := &ast.CallExpr{
			Fun:  lit.Type,
			Args: []ast.Expr{ident},
		}
		report.Report(pass, node,
			fmt.Sprintf("should convert %s (type %s) to %s instead of using struct literal", ident.Name, types.TypeString(typ2, types.RelativeTo(pass.Pkg)), types.TypeString(typ1, types.RelativeTo(pass.Pkg))),
			report.FilterGenerated(),
			report.Fixes(edit.Fix("use type conversion", edit.ReplaceWithNode(pass.Fset, node, r))))
	}
	code.PreorderStack(pass, fn, (*ast.CompositeLit)(nil))
	return nil, nil
}

func CheckTrim(pass *analysis.Pass) (interface{}, error) {
	sameNonDynamic := func(node1, node2 ast.Node) bool {
		if reflect.TypeOf(node1) != reflect.TypeOf(node2) {
			return false
		}

		switch node1 := node1.(type) {
		case *ast.Ident:
			return node1.Obj == node2.(*ast.Ident).Obj
		case *ast.SelectorExpr, *ast.IndexExpr:
			return astutil.Equal(node1, node2)
		case *ast.BasicLit:
			return astutil.Equal(node1, node2)
		}
		return false
	}

	isLenOnIdent := func(fn ast.Expr, ident ast.Expr) bool {
		call, ok := fn.(*ast.CallExpr)
		if !ok {
			return false
		}
		if !code.IsCallTo(pass, call, "len") {
			return false
		}
		if len(call.Args) != 1 {
			return false
		}
		return sameNonDynamic(call.Args[knowledge.Arg("len.v")], ident)
	}

	fn := func(node ast.Node) {
		var pkg string
		var fun string

		ifstmt := node.(*ast.IfStmt)
		if ifstmt.Init != nil {
			return
		}
		if ifstmt.Else != nil {
			return
		}
		if len(ifstmt.Body.List) != 1 {
			return
		}
		condCall, ok := ifstmt.Cond.(*ast.CallExpr)
		if !ok {
			return
		}

		condCallName := code.CallName(pass, condCall)
		switch condCallName {
		case "strings.HasPrefix":
			pkg = "strings"
			fun = "HasPrefix"
		case "strings.HasSuffix":
			pkg = "strings"
			fun = "HasSuffix"
		case "strings.Contains":
			pkg = "strings"
			fun = "Contains"
		case "bytes.HasPrefix":
			pkg = "bytes"
			fun = "HasPrefix"
		case "bytes.HasSuffix":
			pkg = "bytes"
			fun = "HasSuffix"
		case "bytes.Contains":
			pkg = "bytes"
			fun = "Contains"
		default:
			return
		}

		assign, ok := ifstmt.Body.List[0].(*ast.AssignStmt)
		if !ok {
			return
		}
		if assign.Tok != token.ASSIGN {
			return
		}
		if len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			return
		}
		if !sameNonDynamic(condCall.Args[0], assign.Lhs[0]) {
			return
		}

		switch rhs := assign.Rhs[0].(type) {
		case *ast.CallExpr:
			if len(rhs.Args) < 2 || !sameNonDynamic(condCall.Args[0], rhs.Args[0]) || !sameNonDynamic(condCall.Args[1], rhs.Args[1]) {
				return
			}

			rhsName := code.CallName(pass, rhs)
			if condCallName == "strings.HasPrefix" && rhsName == "strings.TrimPrefix" ||
				condCallName == "strings.HasSuffix" && rhsName == "strings.TrimSuffix" ||
				condCallName == "strings.Contains" && rhsName == "strings.Replace" ||
				condCallName == "bytes.HasPrefix" && rhsName == "bytes.TrimPrefix" ||
				condCallName == "bytes.HasSuffix" && rhsName == "bytes.TrimSuffix" ||
				condCallName == "bytes.Contains" && rhsName == "bytes.Replace" {
				report.Report(pass, ifstmt, fmt.Sprintf("should replace this if statement with an unconditional %s", rhsName), report.FilterGenerated())
			}
		case *ast.SliceExpr:
			slice := rhs
			if !ok {
				return
			}
			if slice.Slice3 {
				return
			}
			if !sameNonDynamic(slice.X, condCall.Args[0]) {
				return
			}

			validateOffset := func(off ast.Expr) bool {
				switch off := off.(type) {
				case *ast.CallExpr:
					return isLenOnIdent(off, condCall.Args[1])
				case *ast.BasicLit:
					if pkg != "strings" {
						return false
					}
					if _, ok := condCall.Args[1].(*ast.BasicLit); !ok {
						// Only allow manual slicing with an integer
						// literal if the second argument to HasPrefix
						// was a string literal.
						return false
					}
					s, ok1 := code.ExprToString(pass, condCall.Args[1])
					n, ok2 := code.ExprToInt(pass, off)
					if !ok1 || !ok2 || n != int64(len(s)) {
						return false
					}
					return true
				default:
					return false
				}
			}

			switch fun {
			case "HasPrefix":
				// TODO(dh) We could detect a High that is len(s), but another
				// rule will already flag that, anyway.
				if slice.High != nil {
					return
				}
				if !validateOffset(slice.Low) {
					return
				}
			case "HasSuffix":
				if slice.Low != nil {
					n, ok := code.ExprToInt(pass, slice.Low)
					if !ok || n != 0 {
						return
					}
				}
				switch index := slice.High.(type) {
				case *ast.BinaryExpr:
					if index.Op != token.SUB {
						return
					}
					if !isLenOnIdent(index.X, condCall.Args[0]) {
						return
					}
					if !validateOffset(index.Y) {
						return
					}
				default:
					return
				}
			default:
				return
			}

			var replacement string
			switch fun {
			case "HasPrefix":
				replacement = "TrimPrefix"
			case "HasSuffix":
				replacement = "TrimSuffix"
			}
			report.Report(pass, ifstmt, fmt.Sprintf("should replace this if statement with an unconditional %s.%s", pkg, replacement),
				report.ShortRange(),
				report.FilterGenerated())
		}
	}
	code.Preorder(pass, fn, (*ast.IfStmt)(nil))
	return nil, nil
}

var (
	checkLoopSlideQ = pattern.MustParse(`
		(ForStmt
			(AssignStmt initvar@(Ident _) _ (IntegerLiteral "0"))
			(BinaryExpr initvar "<" limit@(Ident _))
			(IncDecStmt initvar "++")
			[(AssignStmt
				(IndexExpr slice@(Ident _) initvar)
				"="
				(IndexExpr slice (BinaryExpr offset@(Ident _) "+" initvar)))])`)
	checkLoopSlideR = pattern.MustParse(`
		(CallExpr
			(Ident "copy")
			[(SliceExpr slice nil limit nil)
				(SliceExpr slice offset nil nil)])`)
)

func CheckLoopSlide(pass *analysis.Pass) (interface{}, error) {
	// TODO(dh): detect bs[i+offset] in addition to bs[offset+i]
	// TODO(dh): consider merging this function with LintLoopCopy
	// TODO(dh): detect length that is an expression, not a variable name
	// TODO(dh): support sliding to a different offset than the beginning of the slice

	fn := func(node ast.Node) {
		loop := node.(*ast.ForStmt)
		m, edits, ok := code.MatchAndEdit(pass, checkLoopSlideQ, checkLoopSlideR, loop)
		if !ok {
			return
		}
		typ := pass.TypesInfo.TypeOf(m.State["slice"].(*ast.Ident))
		// The pattern probably needs a core type, but All is fine, too. Either way we only accept slices.
		if !typeutil.All(typ, typeutil.IsSlice) {
			return
		}

		report.Report(pass, loop, "should use copy() instead of loop for sliding slice elements",
			report.ShortRange(),
			report.FilterGenerated(),
			report.Fixes(edit.Fix("use copy() instead of loop", edits...)))
	}
	code.Preorder(pass, fn, (*ast.ForStmt)(nil))
	return nil, nil
}

var (
	checkMakeLenCapQ1 = pattern.MustParse(`(CallExpr (Builtin "make") [typ size@(IntegerLiteral "0")])`)
	checkMakeLenCapQ2 = pattern.MustParse(`(CallExpr (Builtin "make") [typ size size])`)
)

func CheckMakeLenCap(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		if pass.Pkg.Path() == "runtime_test" && filepath.Base(pass.Fset.Position(node.Pos()).Filename) == "map_test.go" {
			// special case of runtime tests testing map creation
			return
		}
		if m, ok := code.Match(pass, checkMakeLenCapQ1, node); ok {
			T := m.State["typ"].(ast.Expr)
			size := m.State["size"].(ast.Node)

			if _, ok := typeutil.CoreType(pass.TypesInfo.TypeOf(T)).Underlying().(*types.Chan); ok {
				report.Report(pass, size, fmt.Sprintf("should use make(%s) instead", report.Render(pass, T)), report.FilterGenerated())
			}

		} else if m, ok := code.Match(pass, checkMakeLenCapQ2, node); ok {
			// TODO(dh): don't consider sizes identical if they're
			// dynamic. for example: make(T, <-ch, <-ch).
			T := m.State["typ"].(ast.Expr)
			size := m.State["size"].(ast.Node)
			report.Report(pass, size,
				fmt.Sprintf("should use make(%s, %s) instead", report.Render(pass, T), report.Render(pass, size)),
				report.FilterGenerated())
		}
	}
	code.Preorder(pass, fn, (*ast.CallExpr)(nil))
	return nil, nil
}

var (
	checkAssertNotNilFn1Q = pattern.MustParse(`
		(IfStmt
			(AssignStmt [(Ident "_") ok@(Object _)] _ [(TypeAssertExpr assert@(Object _) _)])
			(Or
				(BinaryExpr ok "&&" (BinaryExpr assert "!=" (Builtin "nil")))
				(BinaryExpr (BinaryExpr assert "!=" (Builtin "nil")) "&&" ok))
			_
			_)`)
	checkAssertNotNilFn2Q = pattern.MustParse(`
		(IfStmt
			nil
			(BinaryExpr lhs@(Object _) "!=" (Builtin "nil"))
			[
				ifstmt@(IfStmt
					(AssignStmt [(Ident "_") ok@(Object _)] _ [(TypeAssertExpr lhs _)])
					ok
					_
					nil)
			]
			nil)`)
)

func CheckAssertNotNil(pass *analysis.Pass) (interface{}, error) {
	fn1 := func(node ast.Node) {
		m, ok := code.Match(pass, checkAssertNotNilFn1Q, node)
		if !ok {
			return
		}
		assert := m.State["assert"].(types.Object)
		assign := m.State["ok"].(types.Object)
		report.Report(pass, node, fmt.Sprintf("when %s is true, %s can't be nil", assign.Name(), assert.Name()),
			report.ShortRange(),
			report.FilterGenerated())
	}
	fn2 := func(node ast.Node) {
		m, ok := code.Match(pass, checkAssertNotNilFn2Q, node)
		if !ok {
			return
		}
		ifstmt := m.State["ifstmt"].(*ast.IfStmt)
		lhs := m.State["lhs"].(types.Object)
		assignIdent := m.State["ok"].(types.Object)
		report.Report(pass, ifstmt, fmt.Sprintf("when %s is true, %s can't be nil", assignIdent.Name(), lhs.Name()),
			report.ShortRange(),
			report.FilterGenerated())
	}
	// OPT(dh): merge fn1 and fn2
	code.Preorder(pass, fn1, (*ast.IfStmt)(nil))
	code.Preorder(pass, fn2, (*ast.IfStmt)(nil))
	return nil, nil
}

func CheckDeclareAssign(pass *analysis.Pass) (interface{}, error) {
	hasMultipleAssignments := func(root ast.Node, ident *ast.Ident) bool {
		num := 0
		ast.Inspect(root, func(node ast.Node) bool {
			if num >= 2 {
				return false
			}
			assign, ok := node.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, lhs := range assign.Lhs {
				if oident, ok := lhs.(*ast.Ident); ok {
					if oident.Obj == ident.Obj {
						num++
					}
				}
			}

			return true
		})
		return num >= 2
	}
	fn := func(node ast.Node) {
		block := node.(*ast.BlockStmt)
		if len(block.List) < 2 {
			return
		}
		for i, stmt := range block.List[:len(block.List)-1] {
			_ = i
			decl, ok := stmt.(*ast.DeclStmt)
			if !ok {
				continue
			}
			gdecl, ok := decl.Decl.(*ast.GenDecl)
			if !ok || gdecl.Tok != token.VAR || len(gdecl.Specs) != 1 {
				continue
			}
			vspec, ok := gdecl.Specs[0].(*ast.ValueSpec)
			if !ok || len(vspec.Names) != 1 || len(vspec.Values) != 0 {
				continue
			}

			assign, ok := block.List[i+1].(*ast.AssignStmt)
			if !ok || assign.Tok != token.ASSIGN {
				continue
			}
			if len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
				continue
			}
			ident, ok := assign.Lhs[0].(*ast.Ident)
			if !ok {
				continue
			}
			if vspec.Names[0].Obj != ident.Obj {
				continue
			}

			if refersTo(pass, assign.Rhs[0], pass.TypesInfo.ObjectOf(ident)) {
				continue
			}
			if hasMultipleAssignments(block, ident) {
				continue
			}

			r := &ast.GenDecl{
				Specs: []ast.Spec{
					&ast.ValueSpec{
						Names:  vspec.Names,
						Values: []ast.Expr{assign.Rhs[0]},
						Type:   vspec.Type,
					},
				},
				Tok: gdecl.Tok,
			}
			report.Report(pass, decl, "should merge variable declaration with assignment on next line",
				report.FilterGenerated(),
				report.Fixes(edit.Fix("merge declaration with assignment", edit.ReplaceWithNode(pass.Fset, edit.Range{decl.Pos(), assign.End()}, r))))
		}
	}
	code.Preorder(pass, fn, (*ast.BlockStmt)(nil))
	return nil, nil
}

func CheckRedundantBreak(pass *analysis.Pass) (interface{}, error) {
	fn1 := func(node ast.Node) {
		clause := node.(*ast.CaseClause)
		if len(clause.Body) < 2 {
			return
		}
		branch, ok := clause.Body[len(clause.Body)-1].(*ast.BranchStmt)
		if !ok || branch.Tok != token.BREAK || branch.Label != nil {
			return
		}
		report.Report(pass, branch, "redundant break statement", report.FilterGenerated())
	}
	fn2 := func(node ast.Node) {
		var ret *ast.FieldList
		var body *ast.BlockStmt
		switch x := node.(type) {
		case *ast.FuncDecl:
			ret = x.Type.Results
			body = x.Body
		case *ast.FuncLit:
			ret = x.Type.Results
			body = x.Body
		default:
			lint.ExhaustiveTypeSwitch(node)
		}
		// if the func has results, a return can't be redundant.
		// similarly, if there are no statements, there can be
		// no return.
		if ret != nil || body == nil || len(body.List) < 1 {
			return
		}
		rst, ok := body.List[len(body.List)-1].(*ast.ReturnStmt)
		if !ok {
			return
		}
		// we don't need to check rst.Results as we already
		// checked x.Type.Results to be nil.
		report.Report(pass, rst, "redundant return statement", report.FilterGenerated())
	}
	code.Preorder(pass, fn1, (*ast.CaseClause)(nil))
	code.Preorder(pass, fn2, (*ast.FuncDecl)(nil), (*ast.FuncLit)(nil))
	return nil, nil
}

func isFormatter(T types.Type, msCache *typeutil.MethodSetCache) bool {
	// TODO(dh): this function also exists in staticcheck/lint.go – deduplicate.

	ms := msCache.MethodSet(T)
	sel := ms.Lookup(nil, "Format")
	if sel == nil {
		return false
	}
	fn, ok := sel.Obj().(*types.Func)
	if !ok {
		// should be unreachable
		return false
	}
	sig := fn.Type().(*types.Signature)
	if sig.Params().Len() != 2 {
		return false
	}
	// TODO(dh): check the types of the arguments for more
	// precision
	if sig.Results().Len() != 0 {
		return false
	}
	return true
}

var checkRedundantSprintfQ = pattern.MustParse(`(CallExpr (Symbol "fmt.Sprintf") [format arg])`)

func CheckRedundantSprintf(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		m, ok := code.Match(pass, checkRedundantSprintfQ, node)
		if !ok {
			return
		}

		format := m.State["format"].(ast.Expr)
		arg := m.State["arg"].(ast.Expr)
		// TODO(dh): should we really support named constants here?
		// shouldn't we only look for string literals? to avoid false
		// positives via build tags?
		if s, ok := code.ExprToString(pass, format); !ok || s != "%s" {
			return
		}
		typ := pass.TypesInfo.TypeOf(arg)
		if typeparams.IsTypeParam(typ) {
			return
		}
		irpkg := pass.ResultOf[buildir.Analyzer].(*buildir.IR).Pkg

		if types.TypeString(typ, nil) == "reflect.Value" {
			// printing with %s produces output different from using
			// the String method
			return
		}

		if isFormatter(typ, &irpkg.Prog.MethodSets) {
			// the type may choose to handle %s in arbitrary ways
			return
		}

		if types.Implements(typ, knowledge.Interfaces["fmt.Stringer"]) {
			replacement := &ast.CallExpr{
				Fun: &ast.SelectorExpr{
					X:   arg,
					Sel: &ast.Ident{Name: "String"},
				},
			}
			report.Report(pass, node, "should use String() instead of fmt.Sprintf",
				report.Fixes(edit.Fix("replace with call to String method", edit.ReplaceWithNode(pass.Fset, node, replacement))))
		} else if typ == types.Universe.Lookup("string").Type() {
			report.Report(pass, node, "the argument is already a string, there's no need to use fmt.Sprintf",
				report.FilterGenerated(),
				report.Fixes(edit.Fix("remove unnecessary call to fmt.Sprintf", edit.ReplaceWithNode(pass.Fset, node, arg))))
		} else if typ.Underlying() == types.Universe.Lookup("string").Type() {
			replacement := &ast.CallExpr{
				Fun:  &ast.Ident{Name: "string"},
				Args: []ast.Expr{arg},
			}
			report.Report(pass, node, "the argument's underlying type is a string, should use a simple conversion instead of fmt.Sprintf",
				report.FilterGenerated(),
				report.Fixes(edit.Fix("replace with conversion to string", edit.ReplaceWithNode(pass.Fset, node, replacement))))
		} else if slice, ok := typ.Underlying().(*types.Slice); ok && slice.Elem() == types.Universe.Lookup("byte").Type() {
			// Note that we check slice.Elem(), not slice.Elem().Underlying, because of https://github.com/golang/go/issues/23536
			replacement := &ast.CallExpr{
				Fun:  &ast.Ident{Name: "string"},
				Args: []ast.Expr{arg},
			}
			report.Report(pass, node, "the argument's underlying type is a slice of bytes, should use a simple conversion instead of fmt.Sprintf",
				report.FilterGenerated(),
				report.Fixes(edit.Fix("replace with conversion to string", edit.ReplaceWithNode(pass.Fset, node, replacement))))
		}

	}
	code.Preorder(pass, fn, (*ast.CallExpr)(nil))
	return nil, nil
}

var (
	checkErrorsNewSprintfQ = pattern.MustParse(`(CallExpr (Symbol "errors.New") [(CallExpr (Symbol "fmt.Sprintf") args)])`)
	checkErrorsNewSprintfR = pattern.MustParse(`(CallExpr (SelectorExpr (Ident "fmt") (Ident "Errorf")) args)`)
)

func CheckErrorsNewSprintf(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		if _, edits, ok := code.MatchAndEdit(pass, checkErrorsNewSprintfQ, checkErrorsNewSprintfR, node); ok {
			// TODO(dh): the suggested fix may leave an unused import behind
			report.Report(pass, node, "should use fmt.Errorf(...) instead of errors.New(fmt.Sprintf(...))",
				report.FilterGenerated(),
				report.Fixes(edit.Fix("use fmt.Errorf", edits...)))
		}
	}
	code.Preorder(pass, fn, (*ast.CallExpr)(nil))
	return nil, nil
}

func CheckRangeStringRunes(pass *analysis.Pass) (interface{}, error) {
	return sharedcheck.CheckRangeStringRunes(pass)
}

var checkNilCheckAroundRangeQ = pattern.MustParse(`
	(IfStmt
		nil
		(BinaryExpr x@(Object _) "!=" (Builtin "nil"))
		[(RangeStmt _ _ _ x _)]
		nil)`)

func CheckNilCheckAroundRange(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		m, ok := code.Match(pass, checkNilCheckAroundRangeQ, node)
		if !ok {
			return
		}
		ok = typeutil.All(m.State["x"].(types.Object).Type(), func(term *typeparams.Term) bool {
			switch term.Type().Underlying().(type) {
			case *types.Slice, *types.Map:
				return true
			case *typeparams.TypeParam, *types.Chan, *types.Pointer:
				return false
			default:
				lint.ExhaustiveTypeSwitch(term.Type().Underlying())
				return false
			}
		})
		if !ok {
			return
		}
		report.Report(pass, node, "unnecessary nil check around range", report.ShortRange(), report.FilterGenerated())
	}
	code.Preorder(pass, fn, (*ast.IfStmt)(nil))
	return nil, nil
}

func isPermissibleSort(pass *analysis.Pass, node ast.Node) bool {
	call := node.(*ast.CallExpr)
	typeconv, ok := call.Args[0].(*ast.CallExpr)
	if !ok {
		return true
	}

	sel, ok := typeconv.Fun.(*ast.SelectorExpr)
	if !ok {
		return true
	}
	name := code.SelectorName(pass, sel)
	switch name {
	case "sort.IntSlice", "sort.Float64Slice", "sort.StringSlice":
	default:
		return true
	}

	return false
}

func CheckSortHelpers(pass *analysis.Pass) (interface{}, error) {
	type Error struct {
		node ast.Node
		msg  string
	}
	var allErrors []Error
	fn := func(node ast.Node) {
		var body *ast.BlockStmt
		switch node := node.(type) {
		case *ast.FuncLit:
			body = node.Body
		case *ast.FuncDecl:
			body = node.Body
		default:
			lint.ExhaustiveTypeSwitch(node)
		}
		if body == nil {
			return
		}

		var errors []Error
		permissible := false
		fnSorts := func(node ast.Node) bool {
			if permissible {
				return false
			}
			if !code.IsCallTo(pass, node, "sort.Sort") {
				return true
			}
			if isPermissibleSort(pass, node) {
				permissible = true
				return false
			}
			call := node.(*ast.CallExpr)
			// isPermissibleSort guarantees that this type assertion will succeed
			typeconv := call.Args[knowledge.Arg("sort.Sort.data")].(*ast.CallExpr)
			sel := typeconv.Fun.(*ast.SelectorExpr)
			name := code.SelectorName(pass, sel)

			switch name {
			case "sort.IntSlice":
				errors = append(errors, Error{node, "should use sort.Ints(...) instead of sort.Sort(sort.IntSlice(...))"})
			case "sort.Float64Slice":
				errors = append(errors, Error{node, "should use sort.Float64s(...) instead of sort.Sort(sort.Float64Slice(...))"})
			case "sort.StringSlice":
				errors = append(errors, Error{node, "should use sort.Strings(...) instead of sort.Sort(sort.StringSlice(...))"})
			}
			return true
		}
		ast.Inspect(body, fnSorts)

		if permissible {
			return
		}
		allErrors = append(allErrors, errors...)
	}
	code.Preorder(pass, fn, (*ast.FuncLit)(nil), (*ast.FuncDecl)(nil))
	sort.Slice(allErrors, func(i, j int) bool {
		return allErrors[i].node.Pos() < allErrors[j].node.Pos()
	})
	var prev token.Pos
	for _, err := range allErrors {
		if err.node.Pos() == prev {
			continue
		}
		prev = err.node.Pos()
		report.Report(pass, err.node, err.msg, report.FilterGenerated())
	}
	return nil, nil
}

var checkGuardedDeleteQ = pattern.MustParse(`
	(IfStmt
		(AssignStmt
			[(Ident "_") ok@(Ident _)]
			":="
			(IndexExpr m key))
		ok
		[call@(CallExpr (Builtin "delete") [m key])]
		nil)`)

func CheckGuardedDelete(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		if m, ok := code.Match(pass, checkGuardedDeleteQ, node); ok {
			report.Report(pass, node, "unnecessary guard around call to delete",
				report.ShortRange(),
				report.FilterGenerated(),
				report.Fixes(edit.Fix("remove guard", edit.ReplaceWithNode(pass.Fset, node, m.State["call"].(ast.Node)))))
		}
	}

	code.Preorder(pass, fn, (*ast.IfStmt)(nil))
	return nil, nil
}

var (
	checkSimplifyTypeSwitchQ = pattern.MustParse(`
		(TypeSwitchStmt
			nil
			expr@(TypeAssertExpr ident@(Ident _) _)
			body)`)
	checkSimplifyTypeSwitchR = pattern.MustParse(`(AssignStmt ident ":=" expr)`)
)

func CheckSimplifyTypeSwitch(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		m, ok := code.Match(pass, checkSimplifyTypeSwitchQ, node)
		if !ok {
			return
		}
		stmt := node.(*ast.TypeSwitchStmt)
		expr := m.State["expr"].(ast.Node)
		ident := m.State["ident"].(*ast.Ident)

		x := pass.TypesInfo.ObjectOf(ident)
		var allOffenders []*ast.TypeAssertExpr
		canSuggestFix := true
		for _, clause := range stmt.Body.List {
			clause := clause.(*ast.CaseClause)
			if len(clause.List) != 1 {
				continue
			}
			hasUnrelatedAssertion := false
			var offenders []*ast.TypeAssertExpr
			ast.Inspect(clause, func(node ast.Node) bool {
				assert2, ok := node.(*ast.TypeAssertExpr)
				if !ok {
					return true
				}
				ident, ok := assert2.X.(*ast.Ident)
				if !ok {
					hasUnrelatedAssertion = true
					return false
				}
				if pass.TypesInfo.ObjectOf(ident) != x {
					hasUnrelatedAssertion = true
					return false
				}

				if !types.Identical(pass.TypesInfo.TypeOf(clause.List[0]), pass.TypesInfo.TypeOf(assert2.Type)) {
					hasUnrelatedAssertion = true
					return false
				}
				offenders = append(offenders, assert2)
				return true
			})
			if !hasUnrelatedAssertion {
				// don't flag cases that have other type assertions
				// unrelated to the one in the case clause. often
				// times, this is done for symmetry, when two
				// different values have to be asserted to the same
				// type.
				allOffenders = append(allOffenders, offenders...)
			}
			canSuggestFix = canSuggestFix && !hasUnrelatedAssertion
		}
		if len(allOffenders) != 0 {
			var opts []report.Option
			for _, offender := range allOffenders {
				opts = append(opts, report.Related(offender, "could eliminate this type assertion"))
			}
			opts = append(opts, report.FilterGenerated())

			msg := fmt.Sprintf("assigning the result of this type assertion to a variable (switch %s := %s.(type)) could eliminate type assertions in switch cases",
				report.Render(pass, ident), report.Render(pass, ident))
			if canSuggestFix {
				var edits []analysis.TextEdit
				edits = append(edits, edit.ReplaceWithPattern(pass.Fset, expr, checkSimplifyTypeSwitchR, m.State))
				for _, offender := range allOffenders {
					edits = append(edits, edit.ReplaceWithNode(pass.Fset, offender, offender.X))
				}
				opts = append(opts, report.Fixes(edit.Fix("simplify type switch", edits...)))
				report.Report(pass, expr, msg, opts...)
			} else {
				report.Report(pass, expr, msg, opts...)
			}
		}
	}
	code.Preorder(pass, fn, (*ast.TypeSwitchStmt)(nil))
	return nil, nil
}

func CheckRedundantCanonicalHeaderKey(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		call := node.(*ast.CallExpr)
		callName := code.CallName(pass, call)
		switch callName {
		case "(net/http.Header).Add", "(net/http.Header).Del", "(net/http.Header).Get", "(net/http.Header).Set":
		default:
			return
		}

		if !code.IsCallTo(pass, call.Args[0], "net/http.CanonicalHeaderKey") {
			return
		}

		report.Report(pass, call,
			fmt.Sprintf("calling net/http.CanonicalHeaderKey on the 'key' argument of %s is redundant", callName),
			report.FilterGenerated(),
			report.Fixes(edit.Fix("remove call to CanonicalHeaderKey", edit.ReplaceWithNode(pass.Fset, call.Args[0], call.Args[0].(*ast.CallExpr).Args[0]))))
	}
	code.Preorder(pass, fn, (*ast.CallExpr)(nil))
	return nil, nil
}

var checkUnnecessaryGuardQ = pattern.MustParse(`
	(Or
		(IfStmt
			(AssignStmt [(Ident "_") ok@(Ident _)] ":=" indexexpr@(IndexExpr _ _))
			ok
			set@(AssignStmt indexexpr "=" (CallExpr (Builtin "append") indexexpr:values))
			(AssignStmt indexexpr "=" (CompositeLit _ values)))
		(IfStmt
			(AssignStmt [(Ident "_") ok] ":=" indexexpr@(IndexExpr _ _))
			ok
			set@(AssignStmt indexexpr "+=" value)
			(AssignStmt indexexpr "=" value))
		(IfStmt
			(AssignStmt [(Ident "_") ok] ":=" indexexpr@(IndexExpr _ _))
			ok
			set@(IncDecStmt indexexpr "++")
			(AssignStmt indexexpr "=" (IntegerLiteral "1"))))`)

func CheckUnnecessaryGuard(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		if m, ok := code.Match(pass, checkUnnecessaryGuardQ, node); ok {
			if code.MayHaveSideEffects(pass, m.State["indexexpr"].(ast.Expr), nil) {
				return
			}
			report.Report(pass, node, "unnecessary guard around map access",
				report.ShortRange(),
				report.Fixes(edit.Fix("simplify map access", edit.ReplaceWithNode(pass.Fset, node, m.State["set"].(ast.Node)))))
		}
	}
	code.Preorder(pass, fn, (*ast.IfStmt)(nil))
	return nil, nil
}

var (
	checkElaborateSleepQ = pattern.MustParse(`(SelectStmt (CommClause (UnaryExpr "<-" (CallExpr (Symbol "time.After") [arg])) body))`)
	checkElaborateSleepR = pattern.MustParse(`(CallExpr (SelectorExpr (Ident "time") (Ident "Sleep")) [arg])`)
)

func CheckElaborateSleep(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		if m, ok := code.Match(pass, checkElaborateSleepQ, node); ok {
			if body, ok := m.State["body"].([]ast.Stmt); ok && len(body) == 0 {
				report.Report(pass, node, "should use time.Sleep instead of elaborate way of sleeping",
					report.ShortRange(),
					report.FilterGenerated(),
					report.Fixes(edit.Fix("Use time.Sleep", edit.ReplaceWithPattern(pass.Fset, node, checkElaborateSleepR, m.State))))
			} else {
				// TODO(dh): we could make a suggested fix if the body
				// doesn't declare or shadow any identifiers
				report.Report(pass, node, "should use time.Sleep instead of elaborate way of sleeping",
					report.ShortRange(),
					report.FilterGenerated())
			}
		}
	}
	code.Preorder(pass, fn, (*ast.SelectStmt)(nil))
	return nil, nil
}

var (
	checkPrintSprintQ = pattern.MustParse(`
		(Or
			(CallExpr
				fn@(Or
					(Symbol "fmt.Print")
					(Symbol "fmt.Sprint")
					(Symbol "fmt.Println")
					(Symbol "fmt.Sprintln"))
				[(CallExpr (Symbol "fmt.Sprintf") f:_)])
			(CallExpr
				fn@(Or
					(Symbol "fmt.Fprint")
					(Symbol "fmt.Fprintln"))
				[_ (CallExpr (Symbol "fmt.Sprintf") f:_)]))`)

	checkTestingErrorSprintfQ = pattern.MustParse(`
		(CallExpr
			sel@(SelectorExpr
				recv
				(Ident
					name@(Or
						"Error"
						"Fatal"
						"Fatalln"
						"Log"
						"Panic"
						"Panicln"
						"Print"
						"Println"
						"Skip")))
			[(CallExpr (Symbol "fmt.Sprintf") args)])`)

	checkLogSprintfQ = pattern.MustParse(`
		(CallExpr
			(Symbol
				(Or
					"log.Fatal"
					"log.Fatalln"
					"log.Panic"
					"log.Panicln"
					"log.Print"
					"log.Println"))
			[(CallExpr (Symbol "fmt.Sprintf") args)])`)

	checkSprintfMapping = map[string]struct {
		recv        string
		alternative string
	}{
		"(*testing.common).Error": {"(*testing.common)", "Errorf"},
		"(testing.TB).Error":      {"(testing.TB)", "Errorf"},
		"(*testing.common).Fatal": {"(*testing.common)", "Fatalf"},
		"(testing.TB).Fatal":      {"(testing.TB)", "Fatalf"},
		"(*testing.common).Log":   {"(*testing.common)", "Logf"},
		"(testing.TB).Log":        {"(testing.TB)", "Logf"},
		"(*testing.common).Skip":  {"(*testing.common)", "Skipf"},
		"(testing.TB).Skip":       {"(testing.TB)", "Skipf"},
		"(*log.Logger).Fatal":     {"(*log.Logger)", "Fatalf"},
		"(*log.Logger).Fatalln":   {"(*log.Logger)", "Fatalf"},
		"(*log.Logger).Panic":     {"(*log.Logger)", "Panicf"},
		"(*log.Logger).Panicln":   {"(*log.Logger)", "Panicf"},
		"(*log.Logger).Print":     {"(*log.Logger)", "Printf"},
		"(*log.Logger).Println":   {"(*log.Logger)", "Printf"},
		"log.Fatal":               {"", "log.Fatalf"},
		"log.Fatalln":             {"", "log.Fatalf"},
		"log.Panic":               {"", "log.Panicf"},
		"log.Panicln":             {"", "log.Panicf"},
		"log.Print":               {"", "log.Printf"},
		"log.Println":             {"", "log.Printf"},
	}
)

func CheckPrintSprintf(pass *analysis.Pass) (interface{}, error) {
	fmtPrintf := func(node ast.Node) {
		m, ok := code.Match(pass, checkPrintSprintQ, node)
		if !ok {
			return
		}

		name := m.State["fn"].(*types.Func).Name()
		var msg string
		switch name {
		case "Print", "Fprint", "Sprint":
			newname := name + "f"
			msg = fmt.Sprintf("should use fmt.%s instead of fmt.%s(fmt.Sprintf(...))", newname, name)
		case "Println", "Fprintln", "Sprintln":
			if _, ok := m.State["f"].(*ast.BasicLit); !ok {
				// This may be an instance of
				// fmt.Println(fmt.Sprintf(arg, ...)) where arg is an
				// externally provided format string and the caller
				// cannot guarantee that the format string ends with a
				// newline.
				return
			}
			newname := name[:len(name)-2] + "f"
			msg = fmt.Sprintf("should use fmt.%s instead of fmt.%s(fmt.Sprintf(...)) (but don't forget the newline)", newname, name)
		}
		report.Report(pass, node, msg,
			report.FilterGenerated())
	}

	methSprintf := func(node ast.Node) {
		m, ok := code.Match(pass, checkTestingErrorSprintfQ, node)
		if !ok {
			return
		}
		mapped, ok := checkSprintfMapping[code.CallName(pass, node.(*ast.CallExpr))]
		if !ok {
			return
		}

		// Ensure that Errorf/Fatalf refer to the right method
		recvTV, ok := pass.TypesInfo.Types[m.State["recv"].(ast.Expr)]
		if !ok {
			return
		}
		obj, _, _ := types.LookupFieldOrMethod(recvTV.Type, recvTV.Addressable(), nil, mapped.alternative)
		f, ok := obj.(*types.Func)
		if !ok {
			return
		}
		if typeutil.FuncName(f) != mapped.recv+"."+mapped.alternative {
			return
		}

		alt := &ast.SelectorExpr{
			X:   m.State["recv"].(ast.Expr),
			Sel: &ast.Ident{Name: mapped.alternative},
		}
		report.Report(pass, node, fmt.Sprintf("should use %s(...) instead of %s(fmt.Sprintf(...))", report.Render(pass, alt), report.Render(pass, m.State["sel"].(*ast.SelectorExpr))))
	}

	pkgSprintf := func(node ast.Node) {
		_, ok := code.Match(pass, checkLogSprintfQ, node)
		if !ok {
			return
		}
		callName := code.CallName(pass, node.(*ast.CallExpr))
		mapped, ok := checkSprintfMapping[callName]
		if !ok {
			return
		}
		report.Report(pass, node, fmt.Sprintf("should use %s(...) instead of %s(fmt.Sprintf(...))", mapped.alternative, callName))
	}

	fn := func(node ast.Node) {
		fmtPrintf(node)
		// TODO(dh): add suggested fixes
		methSprintf(node)
		pkgSprintf(node)
	}
	code.Preorder(pass, fn, (*ast.CallExpr)(nil))
	return nil, nil
}

var checkSprintLiteralQ = pattern.MustParse(`
	(CallExpr
		fn@(Or
			(Symbol "fmt.Sprint")
			(Symbol "fmt.Sprintf"))
		[lit@(BasicLit "STRING" _)])`)

func CheckSprintLiteral(pass *analysis.Pass) (interface{}, error) {
	// We only flag calls with string literals, not expressions of
	// type string, because some people use fmt.Sprint(s) as a pattern
	// for copying strings, which may be useful when extracting a small
	// substring from a large string.
	fn := func(node ast.Node) {
		m, ok := code.Match(pass, checkSprintLiteralQ, node)
		if !ok {
			return
		}
		callee := m.State["fn"].(*types.Func)
		lit := m.State["lit"].(*ast.BasicLit)
		if callee.Name() == "Sprintf" {
			if strings.ContainsRune(lit.Value, '%') {
				// This might be a format string
				return
			}
		}
		report.Report(pass, node, fmt.Sprintf("unnecessary use of fmt.%s", callee.Name()),
			report.FilterGenerated(),
			report.Fixes(edit.Fix("Replace with string literal", edit.ReplaceWithNode(pass.Fset, node, lit))))
	}
	code.Preorder(pass, fn, (*ast.CallExpr)(nil))
	return nil, nil
}

func CheckSameTypeTypeAssertion(pass *analysis.Pass) (interface{}, error) {
	fn := func(node ast.Node) {
		expr := node.(*ast.TypeAssertExpr)
		if expr.Type == nil {
			// skip type switches
			//
			// TODO(dh): we could flag type switches, too, when a case
			// statement has the same type as expr.X – however,
			// depending on the location of that case, it might behave
			// identically to a default branch. we need to think
			// carefully about the instances we want to flag. We also
			// have to take nil interface values into consideration.
			//
			// It might make more sense to extend SA4020 to handle
			// this.
			return
		}
		t1 := pass.TypesInfo.TypeOf(expr.Type)
		t2 := pass.TypesInfo.TypeOf(expr.X)
		if types.IsInterface(t1) && types.Identical(t1, t2) {
			report.Report(pass, expr,
				fmt.Sprintf("type assertion to the same type: %s already has type %s", report.Render(pass, expr.X), report.Render(pass, expr.Type)),
				report.FilterGenerated())
		}
	}

	// TODO(dh): add suggested fixes. we need different fixes depending on the context:
	// - assignment with 1 or 2 lhs
	// - assignment to blank identifiers (as the first, second or both lhs)
	// - initializers in if statements, with the same variations as above

	code.Preorder(pass, fn, (*ast.TypeAssertExpr)(nil))
	return nil, nil
}
