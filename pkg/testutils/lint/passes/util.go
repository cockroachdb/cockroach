package passes

import (
	"fmt"
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/ast/astutil"
)

// FindContainingFile find the file in the pass's Fset which contains the given
// ast node.
func FindContainingFile(pass *analysis.Pass, n ast.Node) *ast.File {
	fPos := pass.Fset.File(n.Pos())
	for _, f := range pass.Files {
		if pass.Fset.File(f.Pos()) == fPos {
			return f
		}
	}
	panic(fmt.Errorf("cannot file file for %v", n))
}

// FindContainingFunc returns the deepest parent function of the given ast node.
// If the ast node is not a child of a function, nil will be returned.
func FindContainingFunc(pass *analysis.Pass, n ast.Node) *types.Func {
	stack, _ := astutil.PathEnclosingInterval(FindContainingFile(pass, n), n.Pos(), n.End())
	for i := len(stack) - 1; i >= 0; i-- {
		// If we stumble upon a func decl or func lit then we're in an interesting spot
		funcDecl, ok := stack[i].(*ast.FuncDecl)
		if !ok {
			continue
		}
		return pass.TypesInfo.ObjectOf(funcDecl.Name).(*types.Func)
	}
	return nil
}

// FindContainingFuncSig returns the signature of the deepest parent closure or
// function of the given ast node. If the ast node is not a child of a function
// or closure, nil will be returned.
func FindContainingFuncSig(pass *analysis.Pass, n ast.Node) *types.Signature {
	stack, _ := astutil.PathEnclosingInterval(FindContainingFile(pass, n), n.Pos(), n.End())
	for _, n := range stack {
		if funcDecl, ok := n.(*ast.FuncDecl); ok {
			return pass.TypesInfo.ObjectOf(funcDecl.Name).(*types.Func).Type().(*types.Signature)
		}
		if funcLit, ok := n.(*ast.FuncLit); ok {
			return pass.TypesInfo.Types[funcLit].Type.(*types.Signature)
		}
	}
	return nil
}
