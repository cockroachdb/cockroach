// Copyright 2020 The Cockroach Authors.
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
	"bytes"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

type execExplainGen struct {
	compiled *lang.CompiledExpr
	w        *matchWriter
}

func (g *execExplainGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = &matchWriter{writer: w}

	g.w.write("package explain\n\n")

	g.w.nestIndent("import (\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/cat\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/exec\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/sem/tree\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/types\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/inverted\"\n")
	g.w.unnest(")\n")

	g.genExplainFactory()
	g.genEnum()
	g.genStructs()
}

func (g *execExplainGen) genExplainFactory() {
	for _, define := range g.compiled.Defines {
		g.w.write("\n")
		g.w.nest("func (f *Factory) Construct%s(\n", define.Name)
		for _, field := range define.Fields {
			generateComments(g.w.writer, field.Comments, string(field.Name), unTitle(string(field.Name)))
			g.w.writeIndent("%s %s,\n", unTitle(string(field.Name)), field.Type)
		}
		g.w.write(") (exec.Node, error) {\n")
		var inputNodes []string
		ordering := "nil /* ordering */"
		for _, field := range define.Fields {
			if field.Type == "exec.Node" {
				inputNodes = append(inputNodes, unTitle(string(field.Name)))
			}
			if field.Type == "exec.OutputOrdering" {
				ordering = unTitle(string(field.Name))
			}
		}
		for _, n := range inputNodes {
			g.w.writeIndent("%sNode := %s.(*Node)\n", n, n)
		}
		opName := unTitle(string(define.Name))
		g.w.nestIndent("args := &%sArgs{\n", opName)
		for _, field := range define.Fields {
			rhs := unTitle(string(field.Name))
			if field.Type == "exec.Node" {
				rhs = rhs + "Node"
			}
			g.w.writeIndent("%s: %s,\n", field.Name, rhs)
		}
		g.w.unnest("}\n")

		var nodesBuf bytes.Buffer
		// ScanBuffer is an exception here, the node it references is not a
		// "child".
		if define.Name != "ScanBuffer" {
			for _, n := range inputNodes {
				fmt.Fprintf(&nodesBuf, ", %sNode", n)
			}
		}
		g.w.writeIndent("_n, err := f.newNode(%sOp, args, %s%s)\n", opName, ordering, nodesBuf.String())
		g.w.nestIndent("if err != nil {\n")
		g.w.writeIndent("return nil, err\n")
		g.w.unnest("}\n")
		g.w.writeIndent("// Build the \"real\" node.\n")
		g.w.nestIndent("wrapped, err := f.wrappedFactory.Construct%s(\n", define.Name)
		for _, field := range define.Fields {
			switch field.Type {
			case "exec.Node":
				g.w.writeIndent("%sNode.WrappedNode(),\n", unTitle(string(field.Name)))
			case "exec.Plan":
				g.w.writeIndent("%s.(*Plan).WrappedPlan,\n", unTitle(string(field.Name)))
			default:
				g.w.writeIndent("%s,\n", unTitle(string(field.Name)))
			}
		}
		g.w.unnest(")\n")
		g.w.nestIndent("if err != nil {\n")
		g.w.writeIndent("return nil, err\n")
		g.w.unnest("}\n")
		g.w.writeIndent("_n.wrappedNode = wrapped\n")
		g.w.writeIndent("return _n, nil\n")
		g.w.unnest("}\n")
	}
}

func (g *execExplainGen) genEnum() {
	g.w.write("\ntype execOperator int\n")
	g.w.write("\n")
	g.w.nest("const (\n")
	g.w.writeIndent("unknownOp execOperator = iota\n")
	for _, define := range g.compiled.Defines {
		g.w.writeIndent("%sOp\n", unTitle(string(define.Name)))
	}
	g.w.unnest(")\n")
}

func (g *execExplainGen) genStructs() {
	for _, define := range g.compiled.Defines {
		g.w.write("\n")
		g.w.nest("type %sArgs struct{\n", unTitle(string(define.Name)))
		for _, field := range define.Fields {
			typ := field.Type
			if typ == "exec.Node" {
				typ = "*Node"
			}
			g.w.writeIndent("%s %s\n", field.Name, typ)
		}
		g.w.unnest("}\n")
	}
}
