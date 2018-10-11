// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

// exprsGen generates the memo expression structs used by the optimizer, as well
// as memoization and interning methods.
type exprsGen struct {
	compiled *lang.CompiledExpr
	md       *metadata
	w        io.Writer
}

func (g *exprsGen) generate(compiled *lang.CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.md = newMetadata(compiled, "memo")
	g.w = w

	fmt.Fprintf(g.w, "package memo\n\n")

	fmt.Fprintf(g.w, "import (\n")
	fmt.Fprintf(g.w, "  \"fmt\"\n")
	fmt.Fprintf(g.w, "  \"unsafe\"\n")
	fmt.Fprintf(g.w, "\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/coltypes\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/opt/props\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/sem/tree\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/sem/types\"\n")
	fmt.Fprintf(g.w, ")\n\n")

	for _, define := range g.compiled.Defines {
		g.genExprDef(define)
	}

	g.genMemoizeFuncs()
	g.genAddToGroupFuncs()
	g.genInternFuncs()
	g.genBuildPropsFunc()
}

// genExprDef generates an expression's type definition and its methods.
func (g *exprsGen) genExprDef(define *lang.DefineExpr) {
	opTyp := g.md.typeOf(define)

	// Generate comment for the expression struct.
	generateDefineComments(g.w, define, opTyp.name)

	// Generate the struct and methods.
	if define.Tags.Contains("List") {
		listItemTyp := opTyp.listItemType
		fmt.Fprintf(g.w, "type %s []%s\n\n", opTyp.name, listItemTyp.name)
		fmt.Fprintf(g.w, "var Empty%s = %s{}\n\n", opTyp.name, opTyp.name)
		g.genListExprFuncs(define)
	} else if define.Tags.Contains("Enforcer") {
		g.genExprStruct(define)
		g.genEnforcerFuncs(define)
	} else if define.Tags.Contains("Private") {
		g.genPrivateStruct(define)
	} else {
		g.genExprStruct(define)
		g.genExprFuncs(define)
	}

	// Generate the expression's group struct and methods
	g.genExprGroupDef(define)
}

// genExprGroupDef generates the group struct definition for a relational
// expression, plus its methods:
//
//   type selectGroup struct {
//     mem   *Memo
//     rel   props.Relational
//     phys  *props.Physical
//     cst   Cost
//     first SelectExpr
//   }
//
func (g *exprsGen) genExprGroupDef(define *lang.DefineExpr) {
	if !define.Tags.Contains("Relational") {
		return
	}

	structType := fmt.Sprintf("%sExpr", define.Name)
	groupStructType := fmt.Sprintf("%sGroup", unTitle(string(define.Name)))

	// Generate the type definition.
	fmt.Fprintf(g.w, "type %s struct {\n", groupStructType)
	fmt.Fprintf(g.w, "  mem *Memo\n")
	fmt.Fprintf(g.w, "  rel props.Relational\n")
	fmt.Fprintf(g.w, "  phys *props.Physical\n")
	fmt.Fprintf(g.w, "  cst Cost\n")
	fmt.Fprintf(g.w, "  first %s\n", structType)
	fmt.Fprintf(g.w, "}\n\n")
	fmt.Fprintf(g.w, "var _ exprGroup = &%s{}\n\n", groupStructType)

	// Generate the memo method.
	fmt.Fprintf(g.w, "func (g *%s) memo() *Memo {\n", groupStructType)
	fmt.Fprintf(g.w, "  return g.mem\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the relational method.
	fmt.Fprintf(g.w, "func (g *%s) relational() *props.Relational {\n", groupStructType)
	fmt.Fprintf(g.w, "  return &g.rel\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the physical method.
	fmt.Fprintf(g.w, "func (g *%s) physical() *props.Physical {\n", groupStructType)
	fmt.Fprintf(g.w, "  return g.phys\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the firstExpr method.
	fmt.Fprintf(g.w, "func (g *%s) firstExpr() RelExpr {\n", groupStructType)
	fmt.Fprintf(g.w, "  return &g.first\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the cost method.
	fmt.Fprintf(g.w, "func (g *%s) cost() Cost {\n", groupStructType)
	fmt.Fprintf(g.w, "  return g.cst\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the setBestProps method.
	fmt.Fprintf(g.w, "func (g *%s) setBestProps(physical *props.Physical, cost Cost) {\n", groupStructType)
	fmt.Fprintf(g.w, "  g.phys = physical\n")
	fmt.Fprintf(g.w, "  g.cst = cost\n")
	fmt.Fprintf(g.w, "}\n\n")
}

// genPrivateStruct generates the struct for a define tagged as Private:
//
//  type FunctionPrivate struct {
//    Name       string
//    Typ        types.T
//    Properties *tree.FunctionProperties
//    Overload   *tree.Overload
//  }
//
func (g *exprsGen) genPrivateStruct(define *lang.DefineExpr) {
	privTyp := g.md.typeOf(define)

	fmt.Fprintf(g.w, "type %s struct {\n", privTyp.name)
	for _, field := range define.Fields {
		fmt.Fprintf(g.w, "  %s %s\n", field.Name, g.md.typeOf(field).name)
	}
	fmt.Fprintf(g.w, "}\n\n")
}

// genExprStruct generates the struct type definition for an expression:
//
//   type SelectExpr struct {
//     Input   RelExpr
//     Filters FiltersExpr
//
//     grp  exprGroup
//     next RelExpr
//   }
//
func (g *exprsGen) genExprStruct(define *lang.DefineExpr) {
	opTyp := g.md.typeOf(define)

	fmt.Fprintf(g.w, "type %s struct {\n", opTyp.name)

	// Generate child fields.
	for _, field := range define.Fields {
		// If field's name is "_", then use Go embedding syntax.
		if isEmbeddedField(field) {
			fmt.Fprintf(g.w, "  %s\n", g.md.typeOf(field).name)
		} else {
			fieldName := g.md.fieldName(field)
			fmt.Fprintf(g.w, "  %s %s\n", fieldName, g.md.typeOf(field).name)
		}
	}

	if define.Tags.Contains("Scalar") {
		fmt.Fprintf(g.w, "\n")
		if g.needsDataTypeField(define) {
			fmt.Fprintf(g.w, "  Typ types.T\n")
		}
	} else if define.Tags.Contains("Enforcer") {
		fmt.Fprintf(g.w, "  Input RelExpr\n")
		fmt.Fprintf(g.w, "  phys  *props.Physical\n")
		fmt.Fprintf(g.w, "  cst   Cost\n")
	} else {
		fmt.Fprintf(g.w, "\n")
		fmt.Fprintf(g.w, "  grp  exprGroup\n")
		fmt.Fprintf(g.w, "  next RelExpr\n")
	}
	fmt.Fprintf(g.w, "}\n\n")
}

// genExprFuncs generates the methods for an expression, including those from
// the Expr, RelExpr, and ScalarExpr interfaces.
func (g *exprsGen) genExprFuncs(define *lang.DefineExpr) {
	opTyp := g.md.typeOf(define)
	childFields := g.md.childFields(define)
	privateField := g.md.privateField(define)

	if define.Tags.Contains("Scalar") {
		fmt.Fprintf(g.w, "var _ opt.ScalarExpr = &%s{}\n\n", opTyp.name)
	} else {
		fmt.Fprintf(g.w, "var _ RelExpr = &%s{}\n\n", opTyp.name)
	}

	// Generate the Op method.
	fmt.Fprintf(g.w, "func (e *%s) Op() opt.Operator {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return opt.%sOp\n", define.Name)
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the ChildCount method.
	fmt.Fprintf(g.w, "func (e *%s) ChildCount() int {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return %d\n", len(childFields))
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Child method.
	fmt.Fprintf(g.w, "func (e *%s) Child(nth int) opt.Expr {\n", opTyp.name)
	if len(childFields) > 0 {
		fmt.Fprintf(g.w, "  switch nth {\n")
		n := 0
		for _, field := range childFields {
			fieldName := g.md.fieldName(field)

			fmt.Fprintf(g.w, "  case %d:\n", n)
			if g.md.typeOf(field).isPointer {
				fmt.Fprintf(g.w, "    return e.%s\n", fieldName)
			} else {
				fmt.Fprintf(g.w, "    return &e.%s\n", fieldName)
			}
			n++
		}
		fmt.Fprintf(g.w, "  }\n")
	}
	fmt.Fprintf(g.w, "  panic(\"child index out of range\")\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Private method.
	fmt.Fprintf(g.w, "func (e *%s) Private() interface{} {\n", opTyp.name)
	if privateField != nil {
		fieldName := g.md.fieldName(privateField)

		if g.md.typeOf(privateField).isPointer {
			fmt.Fprintf(g.w, "  return e.%s\n", fieldName)
		} else {
			fmt.Fprintf(g.w, "  return &e.%s\n", fieldName)
		}
	} else {
		fmt.Fprintf(g.w, "  return nil\n")
	}
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the String method.
	fmt.Fprintf(g.w, "func (e *%s) String() string {\n", opTyp.name)
	if define.Tags.Contains("Scalar") {
		fmt.Fprintf(g.w, "  f := MakeExprFmtCtx(ExprFmtHideQualifications, nil)\n")
	} else {
		fmt.Fprintf(g.w, "  f := MakeExprFmtCtx(ExprFmtHideQualifications, e.Memo())\n")
	}
	fmt.Fprintf(g.w, "  f.FormatExpr(e)\n")
	fmt.Fprintf(g.w, "  return f.Buffer.String()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the SetChild method.
	fmt.Fprintf(g.w, "func (e *%s) SetChild(nth int, child opt.Expr) {\n", opTyp.name)
	if len(childFields) > 0 {
		fmt.Fprintf(g.w, "  switch nth {\n")
		n := 0
		for _, field := range childFields {
			fieldTyp := g.md.typeOf(field)
			fieldName := g.md.fieldName(field)

			fmt.Fprintf(g.w, "  case %d:\n", n)
			if fieldTyp.isPointer {
				fmt.Fprintf(g.w, "    e.%s = child.(%s)\n", fieldName, fieldTyp.name)
			} else {
				fmt.Fprintf(g.w, "    e.%s = *child.(*%s)\n", fieldName, fieldTyp.name)
			}
			fmt.Fprintf(g.w, "    return\n")
			n++
		}
		fmt.Fprintf(g.w, "  }\n")
	}
	fmt.Fprintf(g.w, "  panic(\"child index out of range\")\n")
	fmt.Fprintf(g.w, "}\n\n")

	if define.Tags.Contains("Scalar") {
		// Generate the DataType method.
		fmt.Fprintf(g.w, "func (e *%s) DataType() types.T {\n", opTyp.name)
		dataType := g.constDataType(define)
		if dataType != "" {
			fmt.Fprintf(g.w, "  return %s\n", dataType)
		} else {
			fmt.Fprintf(g.w, "  return e.Typ\n")
		}
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the ScalarProps method.
		if name := g.scalarPropsFieldName(define); name != "" {
			fmt.Fprintf(g.w, "func (e *%s) ScalarProps(mem *Memo) *props.Scalar {\n", opTyp.name)
			fmt.Fprintf(g.w, "  if !e.scalar.Populated {\n")
			fmt.Fprintf(g.w, "    mem.logPropsBuilder.build%sProps(e, &e.%s)\n", define.Name, name)
			fmt.Fprintf(g.w, "  }\n")
			fmt.Fprintf(g.w, "  return &e.%s\n", name)
			fmt.Fprintf(g.w, "}\n\n")
		}
	} else {
		// Generate the Memo method.
		fmt.Fprintf(g.w, "func (e *%s) Memo() *Memo {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return e.grp.memo()\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the Relational method.
		fmt.Fprintf(g.w, "func (e *%s) Relational() *props.Relational {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return e.grp.relational()\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the FirstExpr method.
		fmt.Fprintf(g.w, "func (e *%s) FirstExpr() RelExpr {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return e.grp.firstExpr()\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the NextExpr method.
		fmt.Fprintf(g.w, "func (e *%s) NextExpr() RelExpr {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return e.next\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the Physical method.
		fmt.Fprintf(g.w, "func (e *%s) Physical() *props.Physical {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return e.grp.physical()\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the Cost method.
		fmt.Fprintf(g.w, "func (e *%s) Cost() Cost {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return e.grp.cost()\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the group method.
		fmt.Fprintf(g.w, "func (e *%s) group() exprGroup {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return e.grp\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the setNext method.
		fmt.Fprintf(g.w, "func (e *%s) setNext(member RelExpr) {\n", opTyp.name)
		fmt.Fprintf(g.w, "  if e.next != nil {\n")
		fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"expression already has its next defined: %%s\", e))\n")
		fmt.Fprintf(g.w, "  }\n")
		fmt.Fprintf(g.w, "  e.next = member\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the setGroup method.
		fmt.Fprintf(g.w, "func (e *%s) setGroup(member RelExpr) {\n", opTyp.name)
		fmt.Fprintf(g.w, "  if e.grp != nil {\n")
		fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"expression is already in a group: %%s\", e))\n")
		fmt.Fprintf(g.w, "  }\n")
		fmt.Fprintf(g.w, "  e.grp = member.group()\n")
		fmt.Fprintf(g.w, "  LastGroupMember(member).setNext(e)\n")
		fmt.Fprintf(g.w, "}\n\n")
	}
}

// genEnforcerFuncs generates the methods for an enforcer operator, including
// those from the Expr and RelExpr interfaces.
func (g *exprsGen) genEnforcerFuncs(define *lang.DefineExpr) {
	opTyp := g.md.typeOf(define)

	// Generate the Op method.
	fmt.Fprintf(g.w, "func (e *%s) Op() opt.Operator {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return opt.%sOp\n", define.Name)
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the ChildCount method.
	fmt.Fprintf(g.w, "func (e *%s) ChildCount() int {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return 1\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Child method.
	fmt.Fprintf(g.w, "func (e *%s) Child(nth int) opt.Expr {\n", opTyp.name)
	fmt.Fprintf(g.w, "  if nth == 0 {\n")
	fmt.Fprintf(g.w, "    return e.Input\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "  panic(\"child index out of range\")\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Private method.
	fmt.Fprintf(g.w, "func (e *%s) Private() interface{} {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return nil\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the String method.
	fmt.Fprintf(g.w, "func (e *%s) String() string {\n", opTyp.name)
	fmt.Fprintf(g.w, "  f := MakeExprFmtCtx(ExprFmtHideQualifications, e.Memo())\n")
	fmt.Fprintf(g.w, "  f.FormatExpr(e)\n")
	fmt.Fprintf(g.w, "  return f.Buffer.String()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the SetChild method.
	fmt.Fprintf(g.w, "func (e *%s) SetChild(nth int, child opt.Expr) {\n", opTyp.name)
	fmt.Fprintf(g.w, "  if nth == 0 {\n")
	fmt.Fprintf(g.w, "    e.Input = child.(RelExpr)\n")
	fmt.Fprintf(g.w, "    return\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "  panic(\"child index out of range\")\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Memo method.
	fmt.Fprintf(g.w, "func (e *%s) Memo() *Memo {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return e.Input.Memo()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Relational method.
	fmt.Fprintf(g.w, "func (e *%s) Relational() *props.Relational {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return e.Input.Relational()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the FirstExpr method.
	fmt.Fprintf(g.w, "func (e *%s) FirstExpr() RelExpr {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return e.Input.FirstExpr()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the NextExpr method.
	fmt.Fprintf(g.w, "func (e *%s) NextExpr() RelExpr {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return nil\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Physical method.
	fmt.Fprintf(g.w, "func (e *%s) Physical() *props.Physical {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return e.phys\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Cost method.
	fmt.Fprintf(g.w, "func (e *%s) Cost() Cost {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return e.cst\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the group method.
	fmt.Fprintf(g.w, "func (e *%s) group() exprGroup {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return e.Input.group()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the setNext method.
	fmt.Fprintf(g.w, "func (e *%s) setNext(member RelExpr) {\n", opTyp.name)
	fmt.Fprintf(g.w, "  panic(\"setNext cannot be called on enforcers\")\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the setGroup method.
	fmt.Fprintf(g.w, "func (e *%s) setGroup(member exprGroup) {\n", opTyp.name)
	fmt.Fprintf(g.w, "  panic(\"setGroup cannot be called on enforcers\")\n")
	fmt.Fprintf(g.w, "}\n\n")
}

// genListExprFuncs generates the methods for a list expression, including those
// from the Expr and ScalarExpr interfaces.
func (g *exprsGen) genListExprFuncs(define *lang.DefineExpr) {
	if define.Tags.Contains("Relational") {
		panic("relational list operators are not supported; use scalar list child instead")
	}

	opTyp := g.md.typeOf(define)
	fmt.Fprintf(g.w, "var _ opt.ScalarExpr = &%s{}\n\n", opTyp.name)

	// Generate the Op method.
	fmt.Fprintf(g.w, "func (e *%s) Op() opt.Operator {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return opt.%sOp\n", define.Name)
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the ChildCount method.
	fmt.Fprintf(g.w, "func (e *%s) ChildCount() int {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return len(*e)\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Child method.
	fmt.Fprintf(g.w, "func (e *%s) Child(nth int) opt.Expr {\n", opTyp.name)
	if opTyp.listItemType.isPointer {
		fmt.Fprintf(g.w, "  return (*e)[nth]\n")
	} else {
		fmt.Fprintf(g.w, "  return &(*e)[nth]\n")
	}
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Private method.
	fmt.Fprintf(g.w, "func (e *%s) Private() interface{} {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return nil\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the String method.
	fmt.Fprintf(g.w, "func (e *%s) String() string {\n", opTyp.name)
	fmt.Fprintf(g.w, "  f := MakeExprFmtCtx(ExprFmtHideQualifications, nil)\n")
	fmt.Fprintf(g.w, "  f.FormatExpr(e)\n")
	fmt.Fprintf(g.w, "  return f.Buffer.String()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the SetChild method.
	fmt.Fprintf(g.w, "func (e *%s) SetChild(nth int, child opt.Expr) {\n", opTyp.name)
	if opTyp.listItemType.isPointer {
		fmt.Fprintf(g.w, "  (*e)[nth] = child.(%s)\n", opTyp.listItemType.name)
	} else {
		fmt.Fprintf(g.w, "  (*e)[nth] = *child.(*%s)\n", opTyp.listItemType.name)
	}
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the DataType method.
	fmt.Fprintf(g.w, "func (e *%s) DataType() types.T {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return types.Any\n")
	fmt.Fprintf(g.w, "}\n\n")
}

// genMemoizeFuncs generates Memoize methods on the memo. The Memoize methods
// check whether the expression is already part of the memo; if not, a new memo
// group is created for the expression.
func (g *exprsGen) genMemoizeFuncs() {
	defines := g.compiled.Defines.
		WithoutTag("Enforcer").
		WithoutTag("List").
		WithoutTag("ListItem").
		WithoutTag("Private")

	for _, define := range defines {
		opTyp := g.md.typeOf(define)

		fmt.Fprintf(g.w, "func (m *Memo) Memoize%s(\n", define.Name)
		for _, field := range define.Fields {
			fieldTyp := g.md.typeOf(field)
			fieldName := g.md.fieldName(field)
			fmt.Fprintf(g.w, "  %s %s,\n", unTitle(fieldName), fieldTyp.asParam())
		}
		fmt.Fprintf(g.w, ") *%s {\n", opTyp.name)

		if len(define.Fields) == 0 {
			fmt.Fprintf(g.w, "  return %sSingleton\n", define.Name)
			fmt.Fprintf(g.w, "}\n\n")
			continue
		}

		// Construct a new expression and add it to the interning map.
		if define.Tags.Contains("Scalar") {
			fmt.Fprintf(g.w, "  const size = int64(unsafe.Sizeof(%s{}))\n", opTyp.name)
			fmt.Fprintf(g.w, "  e := &%s{\n", opTyp.name)
		} else {
			groupName := fmt.Sprintf("%sGroup", unTitle(string(define.Name)))
			fmt.Fprintf(g.w, "  const size = int64(unsafe.Sizeof(%s{}))\n", groupName)
			fmt.Fprintf(g.w, "  grp := &%s{mem: m, first: %s{\n", groupName, opTyp.name)
		}

		for _, field := range define.Fields {
			fieldTyp := g.md.typeOf(field)
			fieldName := g.md.fieldName(field)

			if fieldTyp.passByVal {
				fmt.Fprintf(g.w, "   %s: %s,\n", fieldName, unTitle(fieldName))
			} else {
				fmt.Fprintf(g.w, "   %s: *%s,\n", fieldName, unTitle(fieldName))
			}
		}

		if define.Tags.Contains("Scalar") {
			fmt.Fprintf(g.w, "  }\n")
			if g.needsDataTypeField(define) {
				fmt.Fprintf(g.w, "  e.Typ = InferType(m, e)\n")
			}
			fmt.Fprintf(g.w, "  interned := m.interner.Intern%s(e)\n", define.Name)
		} else {
			fmt.Fprintf(g.w, "  }}\n")
			fmt.Fprintf(g.w, "  e := &grp.first\n")
			fmt.Fprintf(g.w, "  e.grp = grp\n")
			fmt.Fprintf(g.w, "  interned := m.interner.Intern%s(e)\n", define.Name)
		}

		// Build relational props, track memory usage, and check consistency if
		// expression was not already interned.
		fmt.Fprintf(g.w, "  if interned == e {\n")
		if !define.Tags.Contains("Scalar") {
			fmt.Fprintf(g.w, "  m.logPropsBuilder.build%sProps(e, &grp.rel)\n", define.Name)
		}
		fmt.Fprintf(g.w, "    m.memEstimate += size\n")
		fmt.Fprintf(g.w, "    m.checkExpr(e)\n")
		fmt.Fprintf(g.w, "  }\n")
		fmt.Fprintf(g.w, "  return interned\n")

		fmt.Fprintf(g.w, "}\n\n")
	}
}

// genAddToGroupFuncs generates AddToGroup methods on the memo.
func (g *exprsGen) genAddToGroupFuncs() {
	defines := g.compiled.Defines.WithTag("Relational").WithoutTag("List").WithoutTag("ListItem")
	for _, define := range defines {
		opTyp := g.md.typeOf(define)

		fmt.Fprintf(g.w, "func (m *Memo) Add%sToGroup(e *%s, grp RelExpr) *%s {\n",
			define.Name, opTyp.name, opTyp.name)
		fmt.Fprintf(g.w, "  const size = int64(unsafe.Sizeof(%s{}))\n", opTyp.name)
		fmt.Fprintf(g.w, "  interned := m.interner.Intern%s(e)\n", define.Name)
		fmt.Fprintf(g.w, "  if interned == e {\n")
		fmt.Fprintf(g.w, "    e.setGroup(grp)\n")
		fmt.Fprintf(g.w, "    m.memEstimate += size\n")
		fmt.Fprintf(g.w, "    m.checkExpr(e)\n")
		fmt.Fprintf(g.w, "  } else if interned.group() != grp.group() {\n")
		fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"%%s expression cannot be added to multiple groups: %%s\", e.Op(), e))\n")
		fmt.Fprintf(g.w, "  }\n")
		fmt.Fprintf(g.w, "  return interned\n")
		fmt.Fprintf(g.w, "}\n\n")
	}
}

// genInternFuncs generates methods on the interner.
func (g *exprsGen) genInternFuncs() {
	fmt.Fprintf(g.w, "func (in *interner) InternExpr(e opt.Expr) opt.Expr {\n")
	fmt.Fprintf(g.w, "  switch t := e.(type) {\n")
	for _, define := range g.compiled.Defines.WithoutTag("Enforcer").WithoutTag("Private") {
		opTyp := g.md.typeOf(define)
		fmt.Fprintf(g.w, "  case *%s:\n", opTyp.name)
		fmt.Fprintf(g.w, "    return in.Intern%s(t)\n", define.Name)
	}
	fmt.Fprintf(g.w, "  default:\n")
	fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"unhandled op: %%s\", e.Op()))\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "}\n\n")

	for _, define := range g.compiled.Defines.WithoutTag("Enforcer").WithoutTag("Private") {
		opTyp := g.md.typeOf(define)

		fmt.Fprintf(g.w, "func (in *interner) Intern%s(val *%s) *%s {\n",
			define.Name, opTyp.name, opTyp.name)

		fmt.Fprintf(g.w, "  in.hasher.Init()\n")

		// Generate code to compute hash.
		fmt.Fprintf(g.w, "  in.hasher.HashOperator(opt.%sOp)\n", define.Name)
		if opTyp.listItemType != nil {
			fmt.Fprintf(g.w, "  in.hasher.Hash%s(*val)\n", title(opTyp.friendlyName))
		} else {
			for _, field := range expandFields(g.compiled, define) {
				fieldName := g.md.fieldName(field)

				if !isExportedField(field) {
					continue
				}
				fieldTyp := g.md.typeOf(field)
				fmt.Fprintf(g.w, "  in.hasher.Hash%s(val.%s)\n", title(fieldTyp.friendlyName), fieldName)
			}
		}
		fmt.Fprintf(g.w, "\n")

		// Generate code to check for existing item with same hash.
		fmt.Fprintf(g.w, "  in.cache.Start(in.hasher.hash)\n")
		fmt.Fprintf(g.w, "  for in.cache.Next() {\n")
		fmt.Fprintf(g.w, "    if existing, ok := in.cache.Item().(*%s); ok {\n", opTyp.name)

		// Generate code to check expression equality when there's an existing item.
		first := true
		if opTyp.listItemType != nil {
			first = false
			fmt.Fprintf(g.w, "  if in.hasher.Is%sEqual(*val, *existing)", title(opTyp.friendlyName))
		} else {
			for _, field := range expandFields(g.compiled, define) {
				fieldName := g.md.fieldName(field)

				if !isExportedField(field) {
					continue
				}
				if !first {
					fmt.Fprintf(g.w, " && \n        ")
				} else {
					fmt.Fprintf(g.w, "      if ")
					first = false
				}

				fieldTyp := g.md.typeOf(field)
				fmt.Fprintf(g.w, "in.hasher.Is%sEqual(val.%s, existing.%s)",
					title(fieldTyp.friendlyName), fieldName, fieldName)
			}
		}

		if !first {
			fmt.Fprintf(g.w, " {\n")
			fmt.Fprintf(g.w, "        return existing\n")
			fmt.Fprintf(g.w, "      }\n")
		} else {
			// Handle expressions with no children.
			fmt.Fprintf(g.w, "      return existing\n")
		}
		fmt.Fprintf(g.w, "    }\n")
		fmt.Fprintf(g.w, "  }\n\n")

		// Generate code to add expression to the cache.
		fmt.Fprintf(g.w, "  in.cache.Add(val)\n")
		fmt.Fprintf(g.w, "  return val\n")
		fmt.Fprintf(g.w, "}\n\n")
	}
}

// genBuildPropsFunc generates a buildProps method for logicalPropsBuilder that
// dispatches to the strongly-typed buildXXXProps methods from a RelExpr.
func (g *exprsGen) genBuildPropsFunc() {
	fmt.Fprintf(g.w, "func (b *logicalPropsBuilder) buildProps(e RelExpr, rel *props.Relational) {\n")
	fmt.Fprintf(g.w, "  switch t := e.(type) {\n")

	for _, define := range g.compiled.Defines.WithTag("Relational") {
		opTyp := g.md.typeOf(define)
		fmt.Fprintf(g.w, "  case *%s:\n", opTyp.name)
		fmt.Fprintf(g.w, "    b.build%sProps(t, rel)\n", define.Name)
	}
	fmt.Fprintf(g.w, "  default:\n")
	fmt.Fprintf(g.w, "    panic(fmt.Sprintf(\"unhandled type: %%s\", t.Op()))\n")

	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *exprsGen) scalarPropsFieldName(define *lang.DefineExpr) string {
	for _, field := range expandFields(g.compiled, define) {
		if field.Type == "ScalarProps" {
			return string(field.Name)
		}
	}
	return ""
}

func (g *exprsGen) needsDataTypeField(define *lang.DefineExpr) bool {
	for _, field := range expandFields(g.compiled, define) {
		if field.Name == "Typ" && field.Type == "DatumType" {
			return false
		}
	}
	return g.constDataType(define) == ""
}

func (g *exprsGen) constDataType(define *lang.DefineExpr) string {
	switch define.Name {
	case "Exists", "Any", "AnyScalar":
		return "types.Bool"
	case "CountRows":
		return "types.Int"
	}
	if define.Tags.Contains("Comparison") || define.Tags.Contains("Boolean") {
		return "types.Bool"
	}
	return ""
}
