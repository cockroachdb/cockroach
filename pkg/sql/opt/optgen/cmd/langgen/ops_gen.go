// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
