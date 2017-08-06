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

package ir

import (
	"bytes"
	"fmt"
)

// ToString converts a syntax tree back to the input syntax,
// using Format()
func ToString(ast interface{}) string {
	var buf bytes.Buffer
	Format(&buf, 0, ast)
	return buf.String()
}

// Format converts a syntax tree back to the input syntax.
func Format(buf *bytes.Buffer, ident int, ast interface{}) {
	switch n := ast.(type) {
	case *NamedType:
		if n.Type == nil {
			fmt.Fprintf(buf, "(typedef :name %s)", n.Name)
		} else {
			fmt.Fprintf(buf, "(typedef\n%*s:name %s", ident+4, "", n.Name)
			if n.Type != nil {
				fmt.Fprintf(buf, "\n%*s:type", ident+4, "")
				Format(buf, ident+8, n.Type)
			}
			buf.WriteByte(')')
		}
	case *EnumItem:
		fmt.Fprintf(buf, "(const :name %s :tag %d)", n.Name, n.Tag)
	case *EnumType:
		if len(n.Items) == 0 {
			buf.WriteString(" (enum)")
		} else {
			fmt.Fprintf(buf, "\n%*s(enum\n", ident, "")
			for i := range n.Items {
				fmt.Fprintf(buf, "%*s", ident+4, "")
				Format(buf, ident+8, &n.Items[i])
				buf.WriteByte('\n')
			}
			fmt.Fprintf(buf, "%*s)", ident, "")
		}
	case *StructItem:
		fmt.Fprintf(buf, "(field :name %s :type %s :isslice %t :tag %d)",
			n.Name, n.Type.Name, n.IsSlice, n.Tag)
	case *StructType:
		if len(n.Items) == 0 {
			buf.WriteString(" (struct)")
		} else {
			fmt.Fprintf(buf, "\n%*s(struct\n", ident, "")
			for i := range n.Items {
				fmt.Fprintf(buf, "%*s", ident+4, "")
				Format(buf, ident+8, &n.Items[i])
				buf.WriteByte('\n')
			}
			fmt.Fprintf(buf, "%*s)", ident, "")
		}
	case *SumItem:
		fmt.Fprintf(buf, "(alt :type %s :tag %d)", n.Type.Name, n.Tag)
	case *SumType:
		if len(n.Items) == 0 {
			buf.WriteString("(sum)")
		} else {
			fmt.Fprintf(buf, "\n%*s(sum\n", ident, "")
			for i := range n.Items {
				fmt.Fprintf(buf, "%*s", ident+4, "")
				Format(buf, ident+8, &n.Items[i])
				buf.WriteByte('\n')
			}
			fmt.Fprintf(buf, "%*s)", ident, "")
		}
	case []NamedType:
		for i := range n {
			if i > 0 {
				fmt.Fprintf(buf, "%*s", ident, "")
			}
			Format(buf, ident, &n[i])
			buf.WriteByte('\n')
		}
	default:
		panic(fmt.Sprintf("unhandled type: %T - %+v", ast, ast))
	}
}
