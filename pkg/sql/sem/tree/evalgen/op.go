// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"go/ast"
	"html/template"
	"os"

	"golang.org/x/tools/go/ast/inspector"
)

const (
	binaryOpsDefinitionsFile = "eval_binary_ops.go"
	unaryOpsDefinitionsFile  = "eval_unary_ops.go"
)

func generateOpsFile(fileName string, files []*ast.File, byName map[string]*ast.File) (err error) {
	return writeFile(fileName, func(f *os.File) error {
		return opsTemplate.Execute(f, struct {
			UnaryOps, BinaryOps []string
		}{
			UnaryOps: findEvalOps(
				files, byName[unaryOpsDefinitionsFile], isUnaryEvalMethod,
			).ordered(),
			BinaryOps: findEvalOps(
				files, byName[binaryOpsDefinitionsFile], isBinaryEvalMethod,
			).ordered(),
		})
	})
}

func findEvalOps(
	files []*ast.File, definitionFile *ast.File, existingMethodFilter func(decl *ast.FuncDecl) bool,
) stringSet {
	types := stringSet{}
	inspector.New([]*ast.File{definitionFile}).
		Preorder([]ast.Node{(*ast.TypeSpec)(nil)}, func(node ast.Node) {
			n := node.(*ast.TypeSpec)
			types.add(n.Name.Name)
		})
	// Find the operations which already define their eval method.
	// These things are syntactic sugar and should not be added to the
	// visitor.
	withExistingMethod := findTypesWithMethod(inspector.New(files), existingMethodFilter)
	types.removeAll(withExistingMethod)
	return types
}

func isBinaryEvalMethod(fd *ast.FuncDecl) bool {
	return fd.Name.Name == "Eval" &&
		fd.Recv != nil &&
		len(fd.Type.Results.List) == 2 &&
		isIdentWithName(fd.Type.Results.List[0].Type, "Datum") &&
		isIdentWithName(fd.Type.Results.List[1].Type, "error") &&
		((len(fd.Type.Params.List) == 3 &&
			isIdentWithName(fd.Type.Params.List[2].Type, "Datum") &&
			len(fd.Type.Params.List[2].Names) == 2) ||
			(len(fd.Type.Params.List) == 4 &&
				isIdentWithName(fd.Type.Params.List[2].Type, "Datum") &&
				len(fd.Type.Params.List[2].Names) == 1 &&
				isIdentWithName(fd.Type.Params.List[3].Type, "Datum") &&
				len(fd.Type.Params.List[3].Names) == 1)) &&
		isIdentWithName(fd.Type.Params.List[1].Type, "OpEvaluator")
}

func isUnaryEvalMethod(fd *ast.FuncDecl) bool {
	return fd.Name.Name == "Eval" &&
		fd.Recv != nil &&
		len(fd.Type.Results.List) == 2 &&
		isIdentWithName(fd.Type.Results.List[0].Type, "Datum") &&
		isIdentWithName(fd.Type.Results.List[1].Type, "error") &&
		len(fd.Type.Params.List) == 3 &&
		isIdentWithName(fd.Type.Params.List[1].Type, "OpEvaluator") &&
		isIdentWithName(fd.Type.Params.List[2].Type, "Datum") &&
		len(fd.Type.Params.List[2].Names) == 1
}

var opsTemplate = template.Must(template.
	New("t").
	Parse(opsTemplateStr))

const opsTemplateStr = header + `

// UnaryEvalOp is a unary operation which can be evaluated.
type UnaryEvalOp interface {
	Eval(context.Context, OpEvaluator, Datum) (Datum, error)
}

// BinaryEvalOp is a binary operation which can be evaluated.
type BinaryEvalOp interface {
	Eval(context.Context, OpEvaluator, Datum, Datum) (Datum, error)
}

// OpEvaluator is an evaluator for UnaryEvalOp and BinaryEvalOp operations.
type OpEvaluator interface {
	UnaryOpEvaluator
	BinaryOpEvaluator
}

// UnaryOpEvaluator knows how to evaluate UnaryEvalOps.
type UnaryOpEvaluator interface {
{{- range .UnaryOps }}
	Eval{{.}}(context.Context, *{{ . }}, Datum) (Datum, error)
{{- end}}
}

// UnaryOpEvaluator knows how to evaluate BinaryEvalOps.
type BinaryOpEvaluator interface {
{{- range .BinaryOps }}
	Eval{{.}}(context.Context, *{{ . }}, Datum, Datum) (Datum, error)
{{- end}}
}

{{ range .UnaryOps }}
// Eval is part of the UnaryEvalOp interface.
func (op *{{.}}) Eval(ctx context.Context, e OpEvaluator, v Datum) (Datum, error) {
	return e.Eval{{.}}(ctx, op, v)
}
{{ end }}{{ range .BinaryOps }}
// Eval is part of the BinaryEvalOp interface.
func (op *{{.}}) Eval(ctx context.Context, e OpEvaluator, a, b Datum) (Datum, error) {
	return e.Eval{{.}}(ctx, op, a, b)
}
{{ end }}
`
