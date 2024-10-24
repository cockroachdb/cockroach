// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

type execFactoryGen struct {
	compiled *lang.CompiledExpr
	w        *matchWriter
}

func (g *execFactoryGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = &matchWriter{writer: w}

	g.w.write("package exec\n\n")

	g.w.nestIndent("import (\n")
	g.w.writeIndent("\"context\"\n\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/cat\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/cat\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/sem/tree\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/types\"\n")
	g.w.writeIndent("\"github.com/cockroachdb/cockroach/pkg/sql/inverted\"\n")
	g.w.unnest(")\n\n")

	g.genExecFactory()
	g.genStubFactory()
}

func (g *execFactoryGen) genExecFactory() {
	g.w.write("// Factory defines the interface for building an execution plan, which consists\n")
	g.w.write("// of a tree of execution nodes (currently a sql.planNode tree).\n")
	g.w.write("//\n")
	g.w.write("// The tree is always built bottom-up. The Construct methods either construct\n")
	g.w.write("// leaf nodes, or they take other nodes previously constructed by this same\n")
	g.w.write("// factory as children.\n")
	g.w.write("//\n")
	g.w.write("// The PlanGistFactory further requires that the factory methods be called in\n")
	g.w.write("// natural tree order, that is, after an operator's children are constructed the\n")
	g.w.write("// next factory method to be called must be the parent of those nodes. The gist's\n")
	g.w.write("// flattened tree representation relies on this to enable the use of a stack to\n")
	g.w.write("// hold children, thus avoiding the complexity of dealing with other tree traversal\n")
	g.w.write("// methods.\n")
	g.w.write("//\n")
	g.w.write("// The TypedExprs passed to these functions refer to columns of the input node\n")
	g.w.write("// via IndexedVars.\n")
	g.w.nest("type Factory interface {\n")
	g.w.writeIndent("// ConstructPlan creates a plan enclosing the given plan and (optionally)\n")
	g.w.writeIndent("// subqueries, cascades, and checks.\n")
	g.w.writeIndent("//\n")
	g.w.writeIndent("// Subqueries are executed before the root tree, which can refer to subquery\n")
	g.w.writeIndent("// results using tree.Subquery nodes.\n")
	g.w.writeIndent("//\n")
	g.w.writeIndent("// Cascades are executed after the root tree. They can return more cascades\n")
	g.w.writeIndent("// and checks which should also be executed.\n")
	g.w.writeIndent("//\n")
	g.w.writeIndent("// Checks are executed after all cascades have been executed. They don't\n")
	g.w.writeIndent("// return results but can generate errors (e.g. foreign key check failures).\n")
	g.w.writeIndent("//\n")
	g.w.writeIndent("// RootRowCount is the number of rows returned by the root node, negative if\n")
	g.w.writeIndent("// the stats weren't available to make a good estimate.\n")
	g.w.nestIndent("ConstructPlan(\n")
	g.w.writeIndent("root Node,\n")
	g.w.writeIndent("subqueries []Subquery,\n")
	g.w.writeIndent("cascades, triggers []PostQuery,\n")
	g.w.writeIndent("checks []Node,\n")
	g.w.writeIndent("rootRowCount int64,\n")
	g.w.writeIndent("flags PlanFlags,\n")
	g.w.unnest(") (Plan, error)\n")

	g.w.write("\n")
	g.w.nest("// Ctx returns the ctx of this execution.\n")
	g.w.writeIndent("Ctx() context.Context\n")
	g.w.unnest("\n")

	for _, define := range g.compiled.Defines {
		g.w.write("\n")
		g.w.write("// Construct%s creates a node for a %s operation.\n", define.Name, define.Name)
		if len(define.Comments) > 0 {
			g.w.write("//\n")
			generateComments(g.w.writer, define.Comments, string(define.Name), string(define.Name))
		}
		g.w.nest("Construct%s(\n", define.Name)
		for _, field := range define.Fields {
			generateComments(g.w.writer, field.Comments, string(field.Name), unTitle(string(field.Name)))
			// Remove "exec." from types.
			typ := strings.Replace(string(field.Type), "exec.", "", 1)
			g.w.writeIndent("%s %s,\n", unTitle(string(field.Name)), typ)
		}
		g.w.unnest(") (Node, error)\n")
	}
	g.w.unnest("}\n\n")
}

func (g *execFactoryGen) genStubFactory() {
	g.w.write("// StubFactory is a do-nothing implementation of Factory, used for testing.\n")
	g.w.write("type StubFactory struct{}\n")
	g.w.write("\n")
	g.w.write("var _ Factory = StubFactory{}\n")
	g.w.write("\n")
	g.w.nestIndent("func (StubFactory) ConstructPlan(\n")
	g.w.writeIndent("root Node,\n")
	g.w.writeIndent("subqueries []Subquery,\n")
	g.w.writeIndent("cascades, triggers []PostQuery,\n")
	g.w.writeIndent("checks []Node,\n")
	g.w.writeIndent("rootRowCount int64,\n")
	g.w.writeIndent("flags PlanFlags,\n")
	g.w.unnest(") (Plan, error) {\n")
	g.w.nestIndent("return struct{}{}, nil\n")
	g.w.unnest("}\n")

	g.w.write("\n")
	g.w.nest("func (StubFactory) Ctx() context.Context {\n")
	g.w.writeIndent("return context.Background()\n")
	g.w.unnest("}\n")

	for _, define := range g.compiled.Defines {
		g.w.write("\n")
		g.w.nest("func (StubFactory) Construct%s(\n", define.Name)
		for _, field := range define.Fields {
			// Remove "exec." from types.
			typ := strings.Replace(string(field.Type), "exec.", "", 1)
			g.w.writeIndent("%s %s,\n", unTitle(string(field.Name)), typ)
		}
		g.w.unnest(") (Node, error) {\n")
		g.w.nestIndent("return struct{}{}, nil\n")
		g.w.unnest("}\n")
	}
}
