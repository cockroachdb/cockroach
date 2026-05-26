// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package benchdoc

import (
	"go/token"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/testutils/benchdoc"
	"golang.org/x/tools/go/analysis"
)

// Doc documents this pass.
const Doc = `validate args specified in benchmark doc`

// Analyzer defines this pass.
var Analyzer = &analysis.Analyzer{
	Name:     "benchdoc",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{},
	Run:      run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	noop := func() (string, error) {
		return "", nil
	}
	report := func(pos token.Pos, err error) {
		pass.Reportf(pos, "%v", err)
	}
	for _, file := range pass.Files {
		// Skip non-test files.
		if !strings.HasSuffix(pass.Fset.Position(file.Pos()).Filename, "_test.go") {
			continue
		}
		_, _ = benchdoc.AnalyzeBenchmarkDocs(file, noop, noop, false, report)
	}
	return nil, nil
}
