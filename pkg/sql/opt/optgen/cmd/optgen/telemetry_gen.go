// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

// telemetryGen generates the telemetry counters for all operators.
// TODO(knz): extend this to also do telemetry for rules.
type telemetryGen struct {
	compiled *lang.CompiledExpr
	w        io.Writer
	sorted   lang.DefineSetExpr
}

func (g *telemetryGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = w
	g.sorted = sortDefines(compiled.Defines)

	fmt.Fprintf(g.w, "package opt\n\n")

	fmt.Fprintf(g.w, "import (\n")
	fmt.Fprintf(g.w, "   \"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry\"\n")
	fmt.Fprintf(g.w, "   \"github.com/cockroachdb/cockroach/pkg/server/telemetry\"\n")
	fmt.Fprintf(g.w, ")\n\n")

	fmt.Fprintf(g.w, "var OpTelemetryCounters = [...]telemetry.Counter {\n")

	for _, define := range g.sorted {
		fmt.Fprintf(g.w, "%sOp: sqltelemetry.OptNodeCounter(\"%s\"),\n",
			define.Name, strings.ToLower(string(define.Name)))
	}

	fmt.Fprintf(g.w, "}\n\n")
}
