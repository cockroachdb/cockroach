// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"go/ast"
	"html/template"
	"os"
	"regexp"

	"golang.org/x/tools/go/ast/inspector"
)

func generateExprEval(fileName string, files []*ast.File) (err error) {
	ins := inspector.New(files)
	exprs := findExprTypes(ins)
	var tmpl = template.Must(template.Must(template.New("name").
		Funcs(template.FuncMap{
			"isDatum": func(name string) bool {
				return isDatumRE.MatchString(name)
			},
			"isPtr": func(name string) bool {
				switch name {
				case "dNull", "UnqualifiedStar":
					return false
				default:
					return true
				}
			},
		}).Parse("{{ if isPtr . }}*{{ end }}{{ . }}")).
		New("t").
		Parse(visitorTemplate))

	return writeFile(fileName, func(f *os.File) error {
		return tmpl.Execute(f, exprs.ordered())
	})
}

var isDatumRE = regexp.MustCompile("^D[A-Z]|^dNull$")

const visitorTemplate = header +
	`
// ExprEvaluator is used to evaluate TypedExpr expressions.
type ExprEvaluator interface {
{{- range . }}{{ if isDatum . }}{{ else }}
	Eval{{.}}(context.Context, {{ template "name" . }}) (Datum, error)
{{- end}}{{ end }}
}

{{ range . }}
// Eval is part of the TypedExpr interface.
func (node {{ template "name" . }}) Eval(ctx context.Context, v ExprEvaluator) (Datum, error) {
	{{ if isDatum . }}return node, nil{{ else }}return v.Eval{{.}}(ctx, node){{ end }}
}
{{ end }}
`

// findExprTypes finds all types in the files which have both the Walk method
// and the ResolvedType method as we know TypedExprs to both have. It returns
// a sorted list.
func findExprTypes(ins *inspector.Inspector) (exprs stringSet) {
	hasWalk := findTypesWithMethod(ins, isWalkMethod)
	hasResolvedType := findTypesWithMethod(ins, isResolvedTypeMethod)
	hasResolvedType.addAll(
		findStructTypesWithEmbeddedType(ins, hasWalk.contains, "typeAnnotation"),
	)
	return hasWalk.intersection(hasResolvedType)
}

func isResolvedTypeMethod(fd *ast.FuncDecl) bool {
	return fd.Name.Name == "ResolvedType" &&
		fd.Recv != nil &&
		len(fd.Type.Params.List) == 0 &&
		len(fd.Type.Results.List) == 1 &&
		isStarTypesT(fd.Type.Results.List[0].Type)
}

func isStarTypesT(expr ast.Expr) bool {
	st, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := st.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	return isIdentWithName(sel.X, "types") && sel.Sel.Name == "T"
}

func isWalkMethod(fd *ast.FuncDecl) bool {
	return fd.Name.Name == "Walk" &&
		fd.Recv != nil &&
		len(fd.Type.Params.List) == 1 &&
		isIdentWithName(fd.Type.Params.List[0].Type, "Visitor") &&
		len(fd.Type.Results.List) == 1 &&
		isIdentWithName(fd.Type.Results.List[0].Type, "Expr")
}

func isIdentWithName(expr ast.Expr, name string) bool {
	id, ok := expr.(*ast.Ident)
	if !ok {
		return false
	}
	return id.Name == name
}
