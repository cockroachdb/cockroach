package main

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/analyzer"
	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/parser"
)

func main() {
	defs, err := parser.Parse("", os.Stdin)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
	_, err = analyzer.Analyze(defs)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
}
