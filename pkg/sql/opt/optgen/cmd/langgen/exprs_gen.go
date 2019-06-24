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
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

// exprsGen generates the AST expression structs for the Optgen language, as
// well as the Expr interface functions. It generates code using the AST that a
// previous version of itself generated (compiler bootstrapping).
type exprsGen struct {
	compiled *lang.CompiledExpr
	w        io.Writer
}

func (g *exprsGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = w

	fmt.Fprintf(g.w, "import (\n")
	fmt.Fprintf(g.w, "  \"bytes\"\n")
	fmt.Fprintf(g.w, "  \"fmt\"\n")
	fmt.Fprintf(g.w, ")\n\n")

	for _, define := range g.compiled.Defines {
		g.genExprType(define)
		g.genOpFunc(define)
		g.genChildCountFunc(define)
		g.genChildFunc(define)
		g.genChildNameFunc(define)
		g.genValueFunc(define)
		g.genVisitFunc(define)
		g.genSourceFunc(define)
		g.genInferredType(define)
		g.genStringFunc(define)
		g.genFormatFunc(define)
	}
}

// type SomeExpr struct {
//   FieldName FieldType
// }
func (g *exprsGen) genExprType(define *lang.DefineExpr) {
	exprType := fmt.Sprintf("%sExpr", define.Name)

	// Generate the expression type.
	if isValueType(define) {
		fmt.Fprintf(g.w, "type %s %s\n", exprType, g.translateType(valueType(define)))
	} else if isSliceType(define) {
		fmt.Fprintf(g.w, "type %s []%s\n", exprType, g.translateType(sliceElementType(define)))
	} else {
		fmt.Fprintf(g.w, "type %s struct {\n", exprType)

		for _, field := range define.Fields {
			fmt.Fprintf(g.w, "  %s %s\n", field.Name, g.translateType(string(field.Type)))
		}

		if hasSourceField(define) {
			fmt.Fprintf(g.w, "  Src *SourceLoc\n")
		}
		if define.Tags.Contains("HasType") {
			fmt.Fprintf(g.w, "  Typ DataType")
		}
		fmt.Fprintf(g.w, "}\n\n")
	}
}

// func (e *SomeExpr) Op() Operator {
//   return SomeOp
// }
func (g *exprsGen) genOpFunc(define *lang.DefineExpr) {
	exprType := fmt.Sprintf("%sExpr", define.Name)
	opType := fmt.Sprintf("%sOp", define.Name)

	fmt.Fprintf(g.w, "func (e *%s) Op() Operator {\n", exprType)
	fmt.Fprintf(g.w, "  return %s\n", opType)
	fmt.Fprintf(g.w, "}\n\n")
}

// func (e *SomeExpr) ChildCount() int {
//   return 1
// }
func (g *exprsGen) genChildCountFunc(define *lang.DefineExpr) {
	exprType := fmt.Sprintf("%sExpr", define.Name)

	// ChildCount method.
	fmt.Fprintf(g.w, "func (e *%s) ChildCount() int {\n", exprType)
	if isSliceType(define) {
		fmt.Fprintf(g.w, "  return len(*e)\n")
	} else if isValueType(define) {
		fmt.Fprintf(g.w, "  return 0\n")
	} else {
		fmt.Fprintf(g.w, "  return %d\n", len(define.Fields))
	}
	fmt.Fprintf(g.w, "}\n\n")
}

// func (e *SomeExpr) Child(nth int) Expr {
//   switch nth {
//   case 0:
//     return e.FieldName
//   }
//   panic(fmt.Sprintf("child index %d is out of range", nth))
// }
func (g *exprsGen) genChildFunc(define *lang.DefineExpr) {
	exprType := fmt.Sprintf("%sExpr", define.Name)

	fmt.Fprintf(g.w, "func (e *%s) Child(nth int) Expr {\n", exprType)

	if isSliceType(define) {
		if g.isValueOrSliceType(sliceElementType(define)) {
			fmt.Fprintf(g.w, "  return &(*e)[nth]\n")
		} else {
			fmt.Fprintf(g.w, "  return (*e)[nth]\n")
		}
	} else if isValueType(define) {
		fmt.Fprintf(g.w, "  panic(fmt.Sprintf(\"child index %%d is out of range\", nth))\n")
	} else {
		if len(define.Fields) != 0 {
			fmt.Fprintf(g.w, "  switch nth {\n")
			for i, field := range define.Fields {
				fmt.Fprintf(g.w, "  case %d:\n", i)

				if g.isValueOrSliceType(string(field.Type)) {
					fmt.Fprintf(g.w, "    return &e.%s\n", field.Name)
				} else {
					fmt.Fprintf(g.w, "    return e.%s\n", field.Name)
				}
			}
			fmt.Fprintf(g.w, "  }\n")
		}

		fmt.Fprintf(g.w, "  panic(fmt.Sprintf(\"child index %%d is out of range\", nth))\n")
	}

	fmt.Fprintf(g.w, "}\n\n")
}

// func (e *SomeExpr) ChildName(nth int) string {
//   switch nth {
//   case 0:
//     return "FieldName"
//   }
//   panic(fmt.Sprintf("child index %d is out of range", nth))
// }
func (g *exprsGen) genChildNameFunc(define *lang.DefineExpr) {
	exprType := fmt.Sprintf("%sExpr", define.Name)

	fmt.Fprintf(g.w, "func (e *%s) ChildName(nth int) string {\n", exprType)

	if !isSliceType(define) && !isValueType(define) && len(define.Fields) != 0 {
		fmt.Fprintf(g.w, "  switch (nth) {\n")
		for i, field := range define.Fields {
			fmt.Fprintf(g.w, "  case %d:\n", i)

			fmt.Fprintf(g.w, "    return \"%s\"\n", field.Name)
		}
		fmt.Fprintf(g.w, "  }\n")
	}

	fmt.Fprintf(g.w, "  return \"\"\n")
	fmt.Fprintf(g.w, "}\n\n")
}

// func (e *SomeExpr) Value() interface{} {
//   return string(*e)
// }
func (g *exprsGen) genValueFunc(define *lang.DefineExpr) {
	exprType := fmt.Sprintf("%sExpr", define.Name)

	fmt.Fprintf(g.w, "func (e *%s) Value() interface{} {\n", exprType)
	if isValueType(define) {
		fmt.Fprintf(g.w, "  return %s(*e)\n", valueType(define))
	} else {
		fmt.Fprintf(g.w, "  return nil\n")
	}
	fmt.Fprintf(g.w, "}\n\n")
}

// func (e *SomeExpr) Visit(visit VisitFunc) Expr {
//   children := visitChildren(e, visit)
//   if children != nil {
//     return &SomeExpr{FieldName: children[0].(*FieldType)}
//   }
//   return e
// }
func (g *exprsGen) genVisitFunc(define *lang.DefineExpr) {
	exprType := fmt.Sprintf("%sExpr", define.Name)

	fmt.Fprintf(g.w, "func (e *%s) Visit(visit VisitFunc) Expr {\n", exprType)

	// Value type definition has no children.
	if !isValueType(define) && len(define.Fields) != 0 {
		fmt.Fprintf(g.w, "  children := visitChildren(e, visit)\n")
		fmt.Fprintf(g.w, "  if children != nil {\n")

		if isSliceType(define) {
			elemType := g.translateType(sliceElementType(define))
			if elemType != "Expr" {
				fmt.Fprintf(g.w, "    typedChildren := make(%s, len(children))\n", exprType)
				fmt.Fprintf(g.w, "    for i := 0; i < len(children); i++ {\n")
				if g.isValueOrSliceType(sliceElementType(define)) {
					fmt.Fprintf(g.w, "      typedChildren[i] = *children[i].(*%s)\n", elemType)
				} else {
					fmt.Fprintf(g.w, "      typedChildren[i] = children[i].(%s)\n", elemType)
				}
				fmt.Fprintf(g.w, "    }\n")
				fmt.Fprintf(g.w, "    return &typedChildren\n")
			} else {
				fmt.Fprintf(g.w, "    typedChildren := %s(children)\n", exprType)
				fmt.Fprintf(g.w, "    return &typedChildren\n")
			}
		} else {
			fmt.Fprintf(g.w, "    return &%s{", exprType)

			for i, field := range define.Fields {
				fieldType := g.translateType(string(field.Type))

				if i != 0 {
					fmt.Fprintf(g.w, ", ")
				}

				if g.isValueOrSliceType(string(field.Type)) {
					fmt.Fprintf(g.w, "%s: *children[%d].(*%s)", field.Name, i, fieldType)
				} else if field.Type == "Expr" {
					fmt.Fprintf(g.w, "%s: children[%d]", field.Name, i)
				} else {
					fmt.Fprintf(g.w, "%s: children[%d].(%s)", field.Name, i, fieldType)
				}
			}

			// Propagate source file, line, pos.
			if hasSourceField(define) {
				fmt.Fprintf(g.w, ", Src: e.Source()")
			}

			fmt.Fprintf(g.w, "}\n")
		}

		fmt.Fprintf(g.w, "  }\n")
	}

	fmt.Fprintf(g.w, "  return e\n")
	fmt.Fprintf(g.w, "}\n\n")
}

// func (e *SomeExpr) Source() *SourceLoc {
//   return e.Src
// }
func (g *exprsGen) genSourceFunc(define *lang.DefineExpr) {
	exprType := fmt.Sprintf("%sExpr", define.Name)

	fmt.Fprintf(g.w, "func (e *%s) Source() *SourceLoc {\n", exprType)
	if hasSourceField(define) {
		fmt.Fprintf(g.w, "  return e.Src\n")
	} else {
		fmt.Fprintf(g.w, "  return nil\n")
	}
	fmt.Fprintf(g.w, "}\n\n")
}

// func (e *SomeExpr) InferredType() DataType {
//   return e.Typ
// }
func (g *exprsGen) genInferredType(define *lang.DefineExpr) {
	exprType := fmt.Sprintf("%sExpr", define.Name)

	fmt.Fprintf(g.w, "func (e *%s) InferredType() DataType {\n", exprType)
	if define.Tags.Contains("HasType") {
		fmt.Fprintf(g.w, "  return e.Typ\n")
	} else if isValueType(define) {
		fmt.Fprintf(g.w, "  return %sDataType\n", title(g.translateType(valueType(define))))
	} else {
		fmt.Fprintf(g.w, "  return AnyDataType\n")
	}
	fmt.Fprintf(g.w, "}\n\n")
}

// func (e *SomeExpr) String() string {
//   var buf bytes.Buffer
//   e.Format(&buf, 0)
//   return buf.String()
// }
func (g *exprsGen) genStringFunc(define *lang.DefineExpr) {
	exprType := fmt.Sprintf("%sExpr", define.Name)

	fmt.Fprintf(g.w, "func (e *%s) String() string {\n", exprType)
	fmt.Fprintf(g.w, "  var buf bytes.Buffer\n")
	fmt.Fprintf(g.w, "  e.Format(&buf, 0)\n")
	fmt.Fprintf(g.w, "  return buf.String()\n")
	fmt.Fprintf(g.w, "}\n\n")
}

// func (e *SomeExpr) Format(buf *bytes.Buffer, level int) {
//   formatExpr(e, buf, level)
// }
func (g *exprsGen) genFormatFunc(define *lang.DefineExpr) {
	exprType := fmt.Sprintf("%sExpr", define.Name)

	fmt.Fprintf(g.w, "func (e *%s) Format(buf *bytes.Buffer, level int) {\n", exprType)
	fmt.Fprintf(g.w, "  formatExpr(e, buf, level)\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *exprsGen) translateType(typ string) string {
	switch typ {
	case "Expr":
		return typ

	case "string":
		return typ

	case "int64":
		return typ
	}

	if g.isValueOrSliceType(typ) {
		return fmt.Sprintf("%sExpr", typ)
	}

	return fmt.Sprintf("*%sExpr", typ)
}

func (g *exprsGen) isValueOrSliceType(typ string) bool {
	// Expr is built-in type, without explicit definition.
	if typ == "Expr" {
		return false
	}

	// Pass slices and value types by value.
	define := g.compiled.LookupDefine(typ)
	if define == nil {
		panic(fmt.Sprintf("could not find define for type %s", typ))
	}
	return isValueType(define) || isSliceType(define)
}

// isValueType returns true if the define statement is defining a value
// expression, which is a leaf expression that is equivalent to a primitive
// type like string or int. These types return non-nil for the Value function.
func isValueType(d *lang.DefineExpr) bool {
	return d.Tags.Contains("Value")
}

// isSliceType returns true if the define statement is defining a slice
// expression, which is an expression that stores a slice of expressions of
// some other type, like []*RuleExpr or []TagExpr.
func isSliceType(d *lang.DefineExpr) bool {
	return d.Tags.Contains("Slice")
}

// valueType returns the name of the primitive type which the defined type
// is equivalent to, like string or int.
func valueType(d *lang.DefineExpr) string {
	if d.Fields[0].Name != "Value" {
		panic(fmt.Sprintf("expected 'Value' field name, found %s", d.Fields[0].Name))
	}
	return string(d.Fields[0].Type)
}

// sliceElementType returns the type of elements in the slice expression.
func sliceElementType(d *lang.DefineExpr) string {
	if d.Fields[0].Name != "Element" {
		panic(fmt.Sprintf("expected 'Element' field name, found %s", d.Fields[0].Name))
	}
	return string(d.Fields[0].Type)
}

// hasSourceField returns true if the defined expression has a Src field that
// stores the original source information (file, line, pos).
func hasSourceField(d *lang.DefineExpr) bool {
	return !isValueType(d) && !isSliceType(d)
}

// title returns the given string with its first letter capitalized.
func title(name string) string {
	rune, size := utf8.DecodeRuneInString(name)
	return fmt.Sprintf("%c%s", unicode.ToUpper(rune), name[size:])
}
