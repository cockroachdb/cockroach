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
//

package codegen

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/ir"
	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/parser"
)

type Settings struct {
	CapEnums int
	CapRefs  int
	LenAlloc int
}

// GenerateCode writes code implementing the given types into b.
func GenerateCode(b *bytes.Buffer, s Settings, namedTypes []ir.NamedType) string {
	if s.LenAlloc <= 0 {
		panic("LenAlloc must be positive")
	}
	fmt.Fprintf(b, "package d\n")
	fmt.Fprintf(b, "\nimport \"fmt\"\n")
	fmt.Fprintf(b, "\ntype Allocator struct {\n")
	fmt.Fprintf(b, "\tnodes []arb\n")
	fmt.Fprintf(b, "}\n")
	fmt.Fprintf(b, "\nfunc (a *Allocator) New() *arb {\n")
	fmt.Fprintf(b, "\tif len(a.nodes) == 0 {\n")
	fmt.Fprintf(b, "\t\ta.nodes = make([]arb, %d)\n", s.LenAlloc)
	fmt.Fprintf(b, "\t}\n")
	fmt.Fprintf(b, "\tnode := &a.nodes[0]\n")
	fmt.Fprintf(b, "\ta.nodes = a.nodes[1:]\n")
	fmt.Fprintf(b, "\treturn node\n")
	fmt.Fprintf(b, "}\n")
	fmt.Fprintf(b, "\ntype arb struct {\n")
	fmt.Fprintf(b, "\tenums [%d]int32\n", s.CapEnums)
	fmt.Fprintf(b, "\trefs [%d]*arb\n", s.CapRefs)
	fmt.Fprintf(b, "\textra\n")
	fmt.Fprintf(b, "}\n")
	fmt.Fprintf(b, "\ntype extra interface {\n")
	fmt.Fprintf(b, "\textraRefs() []*arb\n")
	fmt.Fprintf(b, "}\n")
	for _, typ := range namedTypes {
		if typ.Type == nil {
			continue
		}
		fmt.Fprintf(b, "\n// ---- %s ---- //\n", typ.Name)
		switch t := typ.Type.(type) {
		case nil:

		case *ir.EnumType:
			generateEnum(b, typ.Name, typ.Name, *t)

		case *ir.StructType:
			fmt.Fprintf(b, "\ntype %s struct{ ref *arb }\n", typ.Name)
			fmt.Fprintf(b, "\ntype %sValue struct {\n", typ.Name)
			var bExtra, bR, bGet bytes.Buffer
			needExtra := false
			lenEnums := 0
			lenRefs := 0
			for _, i := range t.Items {
				if i.IsSlice {
					panic("slices not handled yet")
				}
				fmt.Fprintf(b, "\t%s %s // %d\n", i.Name, i.Type.Name, i.Tag)
				var getter string
				switch it := i.Type.Type.(type) {
				case nil:
					needExtra = true
					fmt.Fprintf(&bExtra, "\t%s %s\n", i.Name, i.Type.Name)
					fmt.Fprintf(&bR, "\textra.%s = x.%s\n", i.Name, i.Name)
					getter = fmt.Sprintf("x.ref.extra.(extra%s).%s", typ.Name, i.Name)
				case *ir.EnumType:
					if lenEnums < s.CapEnums {
						fmt.Fprintf(&bR, "\ty.enums[%d] = int32(x.%s)\n", lenEnums, i.Name)
						getter = fmt.Sprintf("%s(x.ref.enums[%d])", i.Type.Name, lenEnums)
						lenEnums++
					} else {
						needExtra = true
						fmt.Fprintf(&bExtra, "\t%s %s\n", i.Name, i.Type.Name)
						fmt.Fprintf(&bR, "\textra.%s = x.%s\n", i.Name, i.Name)
						getter = fmt.Sprintf("x.ref.extra.(extra%s).%s", typ.Name, i.Name)
					}
				case *ir.StructType:
					if lenRefs < s.CapRefs {
						fmt.Fprintf(&bR, "\ty.refs[%d] = x.%s.ref\n", lenRefs, i.Name)
						getter = fmt.Sprintf("%s{x.ref.refs[%d]}", i.Type.Name, lenRefs)
					} else {
						needExtra = true
						fmt.Fprintf(&bR, "\textra.refs[%d] = x.%s.ref\n", lenRefs-s.CapRefs, i.Name)
						getter = fmt.Sprintf("%s{x.ref.extra.(extra%s).refs[%d]}", i.Type.Name, typ.Name, lenRefs-s.CapRefs)
					}
					lenRefs++
				case *ir.SumType:
					var tagGetter string
					if lenEnums < s.CapEnums {
						tagGetter = fmt.Sprintf("%sTag(x.ref.enums[%d])", i.Type.Name, lenEnums)
						fmt.Fprintf(&bR, "\ty.enums[%d] = int32(x.%s.tag)\n", lenEnums, i.Name)
						lenEnums++
					} else {
						needExtra = true
						fmt.Fprintf(&bExtra, "\t%s %sTag\n", i.Name, i.Type.Name)
						tagGetter = fmt.Sprintf("x.ref.extra.(extra%s).%s", typ.Name, i.Name)
						fmt.Fprintf(&bR, "\textra.%s = x.%s.tag\n", i.Name, i.Name)
					}
					var refGetter string
					if lenRefs < s.CapRefs {
						fmt.Fprintf(&bR, "\ty.refs[%d] = x.%s.ref\n", lenRefs, i.Name)
						refGetter = fmt.Sprintf("x.ref.refs[%d]", lenRefs)
					} else {
						needExtra = true
						fmt.Fprintf(&bR, "\textra.refs[%d] = x.%s.ref\n", lenRefs-s.CapRefs, i.Name)
						refGetter = fmt.Sprintf("x.ref.extra.(extra%s).refs[%d]", typ.Name, lenRefs-s.CapRefs)
					}
					getter = fmt.Sprintf("%s{%s, %s}", i.Type.Name, tagGetter, refGetter)
					lenRefs++
				default:
					panic(fmt.Sprintf("case %T not handled", it))
				}
				fmt.Fprintf(&bGet, "\nfunc (x %s) %s() %s { return %s }\n", typ.Name, i.Name, i.Type.Name, getter)
			}
			fmt.Fprintf(b, "}\n")
			if needExtra {
				fmt.Fprintf(b, "\ntype extra%s struct {\n", typ.Name)
				bExtra.WriteTo(b)
				if lenRefs > s.CapRefs {
					fmt.Fprintf(b, "\trefs [%d]*arb\n", lenRefs-s.CapRefs)
				}
				fmt.Fprintf(b, "}\n")
				var extraRefs string
				if lenRefs <= s.CapRefs {
					extraRefs = "nil"
				} else {
					extraRefs = "x.refs[:]"
				}
				fmt.Fprintf(b, "\nfunc (x extra%s) extraRefs() []*arb { return %s }\n", typ.Name, extraRefs)
			}
			fmt.Fprintf(b, "\nfunc (x %sValue) R(a *Allocator) %s {\n", typ.Name, typ.Name)
			fmt.Fprintf(b, "\ty := a.New()\n")
			if needExtra {
				fmt.Fprintf(b, "\tvar extra extra%s\n", typ.Name)
			}
			bR.WriteTo(b)
			if needExtra {
				fmt.Fprintf(b, "\ty.extra = extra\n")
			}
			fmt.Fprintf(b, "\treturn %s{y}\n", typ.Name)
			fmt.Fprintf(b, "}\n")
			bGet.WriteTo(b)
			fmt.Fprintf(b, "\nfunc (x %s) V() %sValue {\n", typ.Name, typ.Name)
			fmt.Fprintf(b, "\treturn %sValue{", typ.Name)
			for j, i := range t.Items {
				if j > 0 {
					fmt.Fprintf(b, ", ")
				}
				if i.IsSlice {
					panic("slices not handled yet")
				}
				fmt.Fprintf(b, "x.%s()", i.Name)
			}
			fmt.Fprintf(b, "}\n")
			fmt.Fprintf(b, "}\n")
			for _, i := range t.Items {
				if i.IsSlice {
					panic("slices not handled yet")
				}
				fmt.Fprintf(b, "\nfunc (x %sValue) With%s(y %s) %sValue {\n", typ.Name, i.Name, i.Type.Name, typ.Name)
				fmt.Fprintf(b, "\tx.%s = y\n", i.Name)
				fmt.Fprintf(b, "\treturn x\n")
				fmt.Fprintf(b, "}\n")
			}

		case *ir.SumType:
			fmt.Fprintf(b, "\ntype %s struct {\n", typ.Name)
			fmt.Fprintf(b, "\ttag %sTag\n", typ.Name)
			fmt.Fprintf(b, "\tref *arb\n")
			fmt.Fprintf(b, "}\n")
			var tt ir.EnumType
			for _, i := range t.Items {
				tt.Items = append(tt.Items, ir.EnumItem{Name: parser.ItemName(i.Type.Name), Tag: i.Tag})
			}
			generateEnum(b, typ.Name+"Tag", typ.Name, tt)
			fmt.Fprintf(b, "\nfunc (x %s) Tag() %sTag { return x.tag }\n", typ.Name, typ.Name)
			for _, i := range t.Items {
				fmt.Fprintf(b, "\nfunc (x %s) %s() %s { return %s{%s%s, x.ref} }\n", i.Type.Name, typ.Name, typ.Name, typ.Name, typ.Name, i.Type.Name)
				fmt.Fprintf(b, "\nfunc (x %s) %s() (%s, bool) {\n", typ.Name, i.Type.Name, i.Type.Name)
				fmt.Fprintf(b, "\tif x.tag != %s%s {\n", typ.Name, i.Type.Name)
				fmt.Fprintf(b, "\t\treturn %s{}, false\n", i.Type.Name)
				fmt.Fprintf(b, "\t}\n")
				fmt.Fprintf(b, "\treturn %s{x.ref}, true\n", i.Type.Name)
				fmt.Fprintf(b, "}\n")
				fmt.Fprintf(b, "\nfunc (x %s) MustBe%s() %s {\n", typ.Name, i.Type.Name, i.Type.Name)
				fmt.Fprintf(b, "\tif x.tag != %s%s {\n", typ.Name, i.Type.Name)
				fmt.Fprintf(b, "\t\tpanic(fmt.Sprintf(%q, x.tag))\n", fmt.Sprintf("type assertion failed: expected %s but got %%s", i.Type.Name))
				fmt.Fprintf(b, "\t}\n")
				fmt.Fprintf(b, "\treturn %s{x.ref}\n", i.Type.Name)
				fmt.Fprintf(b, "}\n")
			}

		default:
			panic(fmt.Sprintf("case %T not handled", t))
		}
	}
	return b.String()
}

func generateEnum(b *bytes.Buffer, name parser.TypeName, prefix parser.TypeName, t ir.EnumType) {
	fmt.Fprintf(b, "\ntype %s int32\n\nconst (\n", name)
	for j, i := range t.Items {
		constName := fmt.Sprintf("%s%s", prefix, i.Name)
		if j == 0 {
			fmt.Fprintf(b, "\t%s %s = %d\n", constName, name, i.Tag)
		} else {
			fmt.Fprintf(b, "\t%s = %d\n", constName, i.Tag)
		}
	}
	fmt.Fprintf(b, ")\n")
	fmt.Fprintf(b, "\nfunc (x %s) String() string {\n", name)
	fmt.Fprintf(b, "\tswitch x {\n")
	for _, i := range t.Items {
		fmt.Fprintf(b, "\tcase %s%s:\n", prefix, i.Name)
		fmt.Fprintf(b, "\t\treturn %q\n", i.Name)
	}
	fmt.Fprintf(b, "\tdefault:\n")
	fmt.Fprintf(b, "\t\treturn fmt.Sprintf(%q, x)\n", fmt.Sprintf("<unknown %s %%d>", name))
	fmt.Fprintf(b, "\t}\n")
	fmt.Fprintf(b, "}\n")
}
