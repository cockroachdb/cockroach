// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

// ifactoryGen generates the opt.Factory interface which will be implemented by
// the normalizer.
type ifactoryGen struct {
	compiled *lang.CompiledExpr
	md       *metadata
	w        *matchWriter
}

func (g *ifactoryGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.md = newMetadata(compiled, "optbuilder")
	g.w = &matchWriter{writer: w}

	g.w.writeIndent("package optbuilder\n\n")

	g.w.nestIndent("import (\n")
	g.w.writeIndent("\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/memo\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/sem/tree\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/types\"\n")
	g.w.unnest(")\n\n")

	g.genInterface()
}

// genInterface generates the factory interface, including a construction
// function for each expression type. The code is similar to this:
//
//   type Factory interface {
//     // ConstructSelect constructs an expression for the Select operator.
//     ConstructSelect(
//       input memo.RelExpr,
//       filters memo.FiltersExpr,
//     ) memo.RelExpr
//
//     ...
//   }
//
func (g *ifactoryGen) genInterface() {
	defines := g.compiled.Defines.
		WithoutTag("Enforcer").
		WithoutTag("List").
		WithoutTag("ListItem").
		WithoutTag("Private")

	g.w.write("type Factory interface {\n")

	g.w.writeIndent("// Memo returns the memo structure that the factory is operating upon.\n")
	g.w.writeIndent("Memo() *memo.Memo\n\n")

	g.w.writeIndent("// Metadata returns the query-specific metadata, which includes information\n")
	g.w.writeIndent("// about the columns and tables used in this particular query.\n")
	g.w.writeIndent("Metadata() *opt.Metadata\n\n")

	g.w.writeIndent("// ConstructConstVal is a helper function that constructs one of the constant\n")
	g.w.writeIndent("// value operators from the given datum value. While most constants are\n")
	g.w.writeIndent("// represented with Const, there are special-case operators for True, False, and\n")
	g.w.writeIndent("// Null, to make matching easier. Null operators require the static type to be\n")
	g.w.writeIndent("// specified, so that rewrites do not change it.\n")
	g.w.writeIndent("ConstructConstVal(d tree.Datum, t *types.T) opt.ScalarExpr\n\n")

	for _, define := range defines {
		// Skip ConstructConst, so that callers will be directed towards
		// ConstructConstVal instead.
		if define.Name == "Const" {
			continue
		}

		// Generate Construct method.
		format := "// Construct%s constructs an expression for the %s operator.\n"
		g.w.nest(format, define.Name, define.Name)
		generateComments(g.w.writer, define.Comments, string(define.Name), string(define.Name))

		g.w.write("Construct%s(\n", define.Name)

		for _, field := range define.Fields {
			fieldTyp := g.md.typeOf(field)
			fieldName := g.md.fieldName(field)
			g.w.writeIndent("%s %s,\n", unTitle(fieldName), fieldTyp.asParam())
		}

		if define.Tags.Contains("Relational") {
			g.w.unnest(") memo.RelExpr\n\n")
		} else {
			g.w.unnest(") opt.ScalarExpr\n\n")
		}
	}

	g.w.write("}\n")
}
