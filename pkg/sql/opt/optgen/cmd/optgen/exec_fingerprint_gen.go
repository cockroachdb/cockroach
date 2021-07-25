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
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

type execFingerprintGen struct {
	compiled *lang.CompiledExpr
	w        *matchWriter
}

func (g *execFingerprintGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
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

	g.genFingerprintFactory()
	g.genFingerprintDecoder()
}

func (g *execFingerprintGen) genFingerprintFactory() {
	for _, define := range g.compiled.Defines {
		g.w.write("\n")
		g.w.nest("func (f *FingerprintFactory) Construct%s(\n", define.Name)
		for _, field := range define.Fields {
			generateComments(g.w.writer, field.Comments, string(field.Name), unTitle(string(field.Name)))
			g.w.writeIndent("%s %s,\n", unTitle(string(field.Name)), field.Type)
		}
		g.w.write(") (exec.Node, error) {\n")

		op := fmt.Sprintf("%sOp", unTitle(string(define.Name)))
		g.w.write("f.encodeOperator(%s)\n", op)
		for _, field := range define.Fields {
			// grab ids
			var expr, encoder string
			name := unTitle(string(field.Name))
			switch unTitle(string(field.Type)) {
			case "cat.Index", "cat.Table", "cat.Schema":
				expr = fmt.Sprintf("%s.ID(),", name)
				encoder = "encodeId"
			case "[]exec.NodeColumnOrdinal":
				expr = name
				encoder = "encodeNodeColumnOrdinals"
			case "colinfo.ResultColumns":
				expr = name
				encoder = "encodeResultColumns"
			case "bool":
				expr = name
				encoder = "encodeBool"
			case "descpb.JoinType":
				expr = fmt.Sprintf("byte(%s)", name)
				encoder = "encodeByte"
			case "colinfo.ColumnOrdering":
				expr = name
				encoder = "encodeColumnOrdering"
			case "exec.ScanParams":
				expr = name
				encoder = "encodeScanParams"
			}
			if len(expr) > 0 {
				g.w.writeIndent("f.%s(%s)\n", encoder, expr)
			}
		}
		//
		//g.w.writeIndent("fmt.Printf(\"%%s\", nodeNames[%s])\n", op)
		//for _, field := range define.Fields {
		//	g.w.writeIndent("fmt.Printf(\"%%v\\n\", %s)\n", unTitle(string(field.Name)))
		//}
		//g.w.writeIndent("fmt.Println()\n")

		g.w.nestIndent("node, err := f.wrappedFactory.Construct%s(\n", define.Name)
		for _, field := range define.Fields {
			g.w.writeIndent("%s,\n", unTitle(string(field.Name)))
		}
		g.w.unnest(")\n")
		g.w.writeIndent("return node, err\n")
		g.w.unnest("}\n")
	}
}

func (g *execFingerprintGen) genFingerprintDecoder() {
	g.w.write("\n")
	g.w.nest("func (f *FingerprintFactory) decodeOperatorBody(op execOperator) (*Node, error) {\n")
	g.w.writeIndent("var _n *Node\n")
	g.w.writeIndent("var reqOrdering exec.OutputOrdering\n")
	g.w.writeIndent("var err error\n")
	g.w.nestIndent("switch op {\n")
	childrenNames := []string{}
	for _, define := range g.compiled.Defines {
		g.w.writeIndent("case %sOp:\n", unTitle(string(define.Name)))
		g.w.nestIndent("var args %sArgs\n", unTitle(string(define.Name)))
		for _, field := range define.Fields {
			// grab ids
			var argName, decoder string
			name := unTitle(string(field.Name))
			argName = title(name)
			switch unTitle(string(field.Type)) {
			case "cat.Table":
				decoder = "decodeTable"
			case "cat.Index":
				decoder = "decodeIndex"
			case "cat.Schema":
				decoder = "decodeSchema"
			case "[]exec.NodeColumnOrdinal":
				decoder = "decodeNodeColumnOrdinals"
			case "colinfo.ResultColumns":
				decoder = "decodeResultColumns"
			case "bool":
				if strings.Compare(string(define.Name), "AlterTableRelocate") == 0 {
					argName = unTitle(argName)
				}
				decoder = "decodeBool"
			case "descpb.JoinType":
				decoder = "decodeJoinType"
			case "colinfo.ColumnOrdering":
				decoder = "decodeColumnOrdering"
			case "exec.ScanParams":
				decoder = "decodeScanParams"
			}
			if len(decoder) > 0 {
				g.w.writeIndent("args.%s = f.%s()\n", argName, decoder)
			}
			// ScanBuffer is an exception here, the node it references is not a child.
			if field.Type == "exec.Node" && define.Name != "ScanBuffer" {
				childName := string(field.Name)
				childrenNames = append(childrenNames, childName)
			}
		}

		for i := len(childrenNames) - 1; i >= 0; i-- {
			childrenNames[i] = "args." + childrenNames[i]
			g.w.writeIndent("%s = f.popChild()\n", childrenNames[i])
		}

		g.w.writeIndent("_n, err = f.newNode(op, &args, reqOrdering, %s)\n", strings.Join(childrenNames, ","))
		childrenNames = []string{}
		g.w.unnest("")
	}
	g.w.unnest("}\n")
	g.w.writeIndent("return _n, err\n")
	g.w.unnest("}\n")
}
