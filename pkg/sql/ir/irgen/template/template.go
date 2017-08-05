// Copyright 2017 The Cockroach Authors.
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

package template

import (
	"bytes"
	"fmt"
	"io"
)

// Template is the type of a simple program whose input is hierarchical data
// (Node) and whose output is text.
type Template struct {
	typ statementType
	// If typ is echoStatement, arg is the text. If typ is forStatement or
	// ifStatement, arg is the label.
	arg      string
	children []*Template
}

type statementType int

const (
	blockStatement statementType = iota
	echoStatement
	forStatement
	ifStatement
)

// Dump prints the given template to w.
func (tmpl Template) Dump(w io.Writer) {
	tmpl.dump(w, 0)
}

func (tmpl *Template) dump(w io.Writer, indent int) {
	switch tmpl.typ {
	case blockStatement:
		tmpl.dumpChildren(w, indent)
	case echoStatement:
		printIndentedLine(w, indent, "echo", tmpl.arg)
	case forStatement:
		printIndentedLine(w, indent, "for", tmpl.arg)
		tmpl.dumpChildren(w, indent+1)
		printIndentedLine(w, indent, "done", tmpl.arg)
	case ifStatement:
		printIndentedLine(w, indent, "if", tmpl.arg)
		tmpl.dumpChildren(w, indent+1)
		printIndentedLine(w, indent, "fi", tmpl.arg)
	default:
		panic(fmt.Sprintf("case %d is not handled", tmpl.typ))
	}
}

func (tmpl *Template) dumpChildren(w io.Writer, indent int) {
	for _, child := range tmpl.children {
		child.dump(w, indent)
	}
}

func printIndentedLine(w io.Writer, indent int, directive string, arg string) {
	var b bytes.Buffer
	for j := 0; j < indent; j++ {
		b.WriteByte('\t')
	}
	b.WriteString(directive)
	if arg != "" {
		b.WriteByte('\t')
		b.WriteString(arg)
	}
	_, _ = fmt.Fprintln(w, b.String())
}
