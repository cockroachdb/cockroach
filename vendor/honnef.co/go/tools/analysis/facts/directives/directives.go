package directives

import (
	"reflect"

	"golang.org/x/tools/go/analysis"
	"honnef.co/go/tools/analysis/lint"
)

func directives(pass *analysis.Pass) (interface{}, error) {
	return lint.ParseDirectives(pass.Files, pass.Fset), nil
}

var Analyzer = &analysis.Analyzer{
	Name:             "directives",
	Doc:              "extracts linter directives",
	Run:              directives,
	RunDespiteErrors: true,
	ResultType:       reflect.TypeOf([]lint.Directive{}),
}
