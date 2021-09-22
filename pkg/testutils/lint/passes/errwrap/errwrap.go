package errwrap

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const Doc = `check for unwrapped errors`

var errorType = types.Universe.Lookup("error").Type().String()

// Analyzer checks for usage of errors.Is instead of direct ==/!=
// comparisons.
var Analyzer = &analysis.Analyzer{
	Name:     "errwrap",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	nodeFilter := []ast.Node{
		(*ast.ReturnStmt)(nil),
	}

	inspect.Preorder(nodeFilter, func(n ast.Node) {
		// Catch-all for possible bugs in the linter code.
		defer func() {
			if r := recover(); r != nil {
				if err, ok := r.(error); ok {
					pass.Reportf(n.Pos(), "internal linter error: %v", err)
					return
				}
				panic(r)
			}
		}()

		ret, ok := n.(*ast.ReturnStmt)
		if !ok {
			return
		}

		for _, expr := range ret.Results {
			retFn, ok := expr.(*ast.CallExpr)
			if ok {
				if len(retFn.Args) > 1 {
					for i := 0; i < len(retFn.Args); i++ {
						val := retFn.Args[i]
						if val == nil {
							return
						}
						if pass.TypesInfo.TypeOf(val).String() == errorType {
							reportUnwrappedErr(pass, retFn)
							return
						}
					}
				}

				if pass.TypesInfo.TypeOf(retFn.Args[0]).String() == errorType {
					reportUnwrappedErr(pass, retFn)
					return
				}
				return
			}

			if pass.TypesInfo.TypeOf(expr).String() == errorType {
				reportUnwrappedErr(pass, expr)
				return
			}

		}
		return
	})

	return nil, nil
}

func reportUnwrappedErr(pass *analysis.Pass, expr ast.Expr) {
	//Not sure if we can ignore signatures
	pass.Reportf(expr.Pos(), escNl(`unwrapped err insert advice`))
}

func escNl(msg string) string {
	return strings.ReplaceAll(msg, "\n", "\\n++")
}
