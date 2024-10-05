// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

// generateOps generates the Operator type and enumeration for the Optgen
// AST nodes.
func generateOps(compiled *lang.CompiledExpr, w io.Writer) {
	fmt.Fprintf(w, "type Operator int\n\n")

	fmt.Fprintf(w, "const (\n")
	fmt.Fprintf(w, "  UnknownOp Operator = iota\n\n")

	for _, define := range compiled.Defines {
		fmt.Fprintf(w, "  %sOp\n", define.Name)
	}

	fmt.Fprintf(w, ")\n\n")
}
