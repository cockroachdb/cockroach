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
	fmt.Fprintf(g.w, "  \"unsafe\"\n")
	fmt.Fprintf(g.w, "\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/opt\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/opt/cat\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/opt/props\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/sem/tree\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/types\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/cockroach/pkg/sql/inverted\"\n")
	fmt.Fprintf(g.w, "  \"github.com/cockroachdb/errors\"\n")
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
	generateComments(g.w, define.Comments, string(define.Name), opTyp.name)

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
//     first SelectExpr
//     best  bestProps
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
	fmt.Fprintf(g.w, "  first %s\n", structType)
	fmt.Fprintf(g.w, "  best bestProps\n")
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

	// Generate the firstExpr method.
	fmt.Fprintf(g.w, "func (g *%s) firstExpr() RelExpr {\n", groupStructType)
	fmt.Fprintf(g.w, "  return &g.first\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the bestProps method.
	fmt.Fprintf(g.w, "func (g *%s) bestProps() *bestProps {\n", groupStructType)
	fmt.Fprintf(g.w, "  return &g.best\n")
	fmt.Fprintf(g.w, "}\n\n")
}

// genPrivateStruct generates the struct for a define tagged as Private:
//
//   type FunctionPrivate struct {
//     Name       string
//     Typ        *types.T
//     Properties *tree.FunctionProperties
//     Overload   *tree.Overload
//   }
//
func (g *exprsGen) genPrivateStruct(define *lang.DefineExpr) {
	privTyp := g.md.typeOf(define)

	fmt.Fprintf(g.w, "type %s struct {\n", privTyp.name)
	for i, field := range define.Fields {
		// Generate comment for the struct field.
		if len(field.Comments) != 0 {
			if i != 0 {
				fmt.Fprintf(g.w, "\n")
			}
			generateComments(g.w, field.Comments, string(field.Name), string(field.Name))
		}

		// If field's name is "_", then use Go embedding syntax.
		if isEmbeddedField(field) {
			fmt.Fprintf(g.w, "  %s\n", g.md.typeOf(field).asField())
		} else {
			fmt.Fprintf(g.w, "  %s %s\n", field.Name, g.md.typeOf(field).asField())
		}
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
	for i, field := range define.Fields {
		// Generate comment for the struct field.
		if len(field.Comments) != 0 {
			if i != 0 {
				fmt.Fprintf(g.w, "\n")
			}
			generateComments(g.w, field.Comments, string(field.Name), string(field.Name))
		}

		// If field's name is "_", then use Go embedding syntax.
		if isEmbeddedField(field) {
			fmt.Fprintf(g.w, "  %s\n", g.md.typeOf(field).asField())
		} else {
			fieldName := g.md.fieldName(field)
			fmt.Fprintf(g.w, "  %s %s\n", fieldName, g.md.typeOf(field).asField())
		}
	}

	if define.Tags.Contains("Scalar") {
		fmt.Fprintf(g.w, "\n")
		if g.needsDataTypeField(define) {
			fmt.Fprintf(g.w, "  Typ *types.T\n")
		}
		if define.Tags.Contains("ListItem") {
			if define.Tags.Contains("ScalarProps") {
				fmt.Fprintf(g.w, "  scalar props.Scalar\n")
			}
		} else {
			fmt.Fprintf(g.w, "  id opt.ScalarID\n")
		}
	} else if define.Tags.Contains("Enforcer") {
		fmt.Fprintf(g.w, "  Input RelExpr\n")
		fmt.Fprintf(g.w, "  best  bestProps\n")
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

		// Generate the ID method.
		fmt.Fprintf(g.w, "func (e *%s) ID() opt.ScalarID {\n", opTyp.name)
		if define.Tags.Contains("ListItem") {
			fmt.Fprintf(g.w, "  return 0\n")
		} else {
			fmt.Fprintf(g.w, "  return e.id\n")
		}
		fmt.Fprintf(g.w, "}\n\n")
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
			fieldType := g.md.typeOf(field)

			// Use dynamicFieldLoadPrefix, since we're loading from field and
			// returning as dynamic opt.Expr type.
			fmt.Fprintf(g.w, "  case %d:\n", n)
			fmt.Fprintf(g.w, "    return %se.%s\n", dynamicFieldLoadPrefix(fieldType), fieldName)
			n++
		}
		fmt.Fprintf(g.w, "  }\n")
	}
	fmt.Fprintf(g.w, "  panic(errors.AssertionFailedf(\"child index out of range\"))\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Private method.
	fmt.Fprintf(g.w, "func (e *%s) Private() interface{} {\n", opTyp.name)
	if privateField != nil {
		fieldName := g.md.fieldName(privateField)
		fieldType := g.md.typeOf(privateField)

		// Use dynamicFieldLoadPrefix, since we're loading from field and returning
		// as dynamic interface{} type.
		fmt.Fprintf(g.w, "  return %se.%s\n", dynamicFieldLoadPrefix(fieldType), fieldName)
	} else {
		fmt.Fprintf(g.w, "  return nil\n")
	}
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the String method.
	fmt.Fprintf(g.w, "func (e *%s) String() string {\n", opTyp.name)
	if define.Tags.Contains("Scalar") {
		fmt.Fprintf(g.w, "  f := MakeExprFmtCtx(ExprFmtHideQualifications, nil, nil)\n")
	} else {
		fmt.Fprintf(g.w, "  f := MakeExprFmtCtx(ExprFmtHideQualifications, e.Memo(), nil)\n")
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

			// Use castFromDynamicParam and then fieldStorePrefix, in order to first
			// cast from the dynamic param type (opt.Expr) to the static param type,
			// and then to store that into the field.
			fmt.Fprintf(g.w, "  case %d:\n", n)
			fmt.Fprintf(g.w, "    %se.%s = %s\n", fieldStorePrefix(fieldTyp), fieldName,
				castFromDynamicParam("child", fieldTyp))
			fmt.Fprintf(g.w, "    return\n")
			n++
		}
		fmt.Fprintf(g.w, "  }\n")
	}
	fmt.Fprintf(g.w, "  panic(errors.AssertionFailedf(\"child index out of range\"))\n")
	fmt.Fprintf(g.w, "}\n\n")

	if define.Tags.Contains("Scalar") {
		// Generate the DataType method.
		fmt.Fprintf(g.w, "func (e *%s) DataType() *types.T {\n", opTyp.name)
		if dataType, ok := g.constDataType(define); ok {
			fmt.Fprintf(g.w, "  return %s\n", dataType)
		} else {
			fmt.Fprintf(g.w, "  return e.Typ\n")
		}
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the PopulateProps and ScalarProps methods.
		if define.Tags.Contains("ScalarProps") {
			fmt.Fprintf(g.w, "func (e *%s) PopulateProps(mem *Memo) {\n", opTyp.name)
			fmt.Fprintf(g.w, "  mem.logPropsBuilder.build%sProps(e, &e.scalar)\n", opTyp.name)
			fmt.Fprintf(g.w, "  e.scalar.Populated = true\n")
			fmt.Fprintf(g.w, "}\n\n")

			fmt.Fprintf(g.w, "func (e *%s) ScalarProps() *props.Scalar {\n", opTyp.name)
			fmt.Fprintf(g.w, "  return &e.scalar\n")
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

		// Generate the RequiredPhysical method.
		fmt.Fprintf(g.w, "func (e *%s) RequiredPhysical() *physical.Required {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return e.grp.bestProps().required\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the ProvidedPhysical method.
		fmt.Fprintf(g.w, "func (e *%s) ProvidedPhysical() *physical.Provided {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return &e.grp.bestProps().provided\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the Cost method.
		fmt.Fprintf(g.w, "func (e *%s) Cost() Cost {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return e.grp.bestProps().cost\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the group method.
		fmt.Fprintf(g.w, "func (e *%s) group() exprGroup {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return e.grp\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the bestProps method.
		fmt.Fprintf(g.w, "func (e *%s) bestProps() *bestProps {\n", opTyp.name)
		fmt.Fprintf(g.w, "  return e.grp.bestProps()\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the setNext method.
		fmt.Fprintf(g.w, "func (e *%s) setNext(member RelExpr) {\n", opTyp.name)
		fmt.Fprintf(g.w, "  if e.next != nil {\n")
		fmt.Fprintf(g.w, "    panic(errors.AssertionFailedf(\"expression already has its next defined: %%s\", e))\n")
		fmt.Fprintf(g.w, "  }\n")
		fmt.Fprintf(g.w, "  e.next = member\n")
		fmt.Fprintf(g.w, "}\n\n")

		// Generate the setGroup method.
		fmt.Fprintf(g.w, "func (e *%s) setGroup(member RelExpr) {\n", opTyp.name)
		fmt.Fprintf(g.w, "  if e.grp != nil {\n")
		fmt.Fprintf(g.w, "    panic(errors.AssertionFailedf(\"expression is already in a group: %%s\", e))\n")
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
	fmt.Fprintf(g.w, "  panic(errors.AssertionFailedf(\"child index out of range\"))\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Private method.
	fmt.Fprintf(g.w, "func (e *%s) Private() interface{} {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return nil\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the String method.
	fmt.Fprintf(g.w, "func (e *%s) String() string {\n", opTyp.name)
	fmt.Fprintf(g.w, "  f := MakeExprFmtCtx(ExprFmtHideQualifications, e.Memo(), nil)\n")
	fmt.Fprintf(g.w, "  f.FormatExpr(e)\n")
	fmt.Fprintf(g.w, "  return f.Buffer.String()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the SetChild method.
	fmt.Fprintf(g.w, "func (e *%s) SetChild(nth int, child opt.Expr) {\n", opTyp.name)
	fmt.Fprintf(g.w, "  if nth == 0 {\n")
	fmt.Fprintf(g.w, "    e.Input = child.(RelExpr)\n")
	fmt.Fprintf(g.w, "    return\n")
	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "  panic(errors.AssertionFailedf(\"child index out of range\"))\n")
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

	// Generate the RequiredPhysical method.
	fmt.Fprintf(g.w, "func (e *%s) RequiredPhysical() *physical.Required {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return e.best.required\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the ProvidedPhysical method.
	fmt.Fprintf(g.w, "func (e *%s) ProvidedPhysical() *physical.Provided {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return &e.best.provided\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Cost method.
	fmt.Fprintf(g.w, "func (e *%s) Cost() Cost {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return e.best.cost\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the bestProps method.
	fmt.Fprintf(g.w, "func (e *%s) bestProps() *bestProps {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return &e.best\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the group method.
	fmt.Fprintf(g.w, "func (e *%s) group() exprGroup {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return e.Input.group()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the setNext method.
	fmt.Fprintf(g.w, "func (e *%s) setNext(member RelExpr) {\n", opTyp.name)
	fmt.Fprintf(g.w, "  panic(errors.AssertionFailedf(\"setNext cannot be called on enforcers\"))\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the setGroup method.
	fmt.Fprintf(g.w, "func (e *%s) setGroup(member exprGroup) {\n", opTyp.name)
	fmt.Fprintf(g.w, "  panic(errors.AssertionFailedf(\"setGroup cannot be called on enforcers\"))\n")
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

	// Generate the ID method.
	fmt.Fprintf(g.w, "func (e *%s) ID() opt.ScalarID {\n", opTyp.name)
	fmt.Fprintf(g.w, "  panic(errors.AssertionFailedf(\"lists have no id\"))")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Op method.
	fmt.Fprintf(g.w, "func (e *%s) Op() opt.Operator {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return opt.%sOp\n", define.Name)
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the ChildCount method.
	fmt.Fprintf(g.w, "func (e *%s) ChildCount() int {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return len(*e)\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Child method.
	// Use dynamicFieldLoadPrefix, since the field is being passed as the dynamic
	// opt.Expr type.
	fmt.Fprintf(g.w, "func (e *%s) Child(nth int) opt.Expr {\n", opTyp.name)
	fmt.Fprintf(g.w, "    return %s(*e)[nth]\n", dynamicFieldLoadPrefix(opTyp.listItemType))
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Private method.
	fmt.Fprintf(g.w, "func (e *%s) Private() interface{} {\n", opTyp.name)
	fmt.Fprintf(g.w, "  return nil\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the String method.
	fmt.Fprintf(g.w, "func (e *%s) String() string {\n", opTyp.name)
	fmt.Fprintf(g.w, "  f := MakeExprFmtCtx(ExprFmtHideQualifications, nil, nil)\n")
	fmt.Fprintf(g.w, "  f.FormatExpr(e)\n")
	fmt.Fprintf(g.w, "  return f.Buffer.String()\n")
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the SetChild method.
	// Use castFromDynamicParam and then fieldStorePrefix, in order to first cast
	// from the dynamic param type (opt.Expr) to the static param type, and then
	// to store that into the field.
	fmt.Fprintf(g.w, "func (e *%s) SetChild(nth int, child opt.Expr) {\n", opTyp.name)
	fmt.Fprintf(g.w, "    (*e)[nth] = %s%s\n", fieldStorePrefix(opTyp.listItemType),
		castFromDynamicParam("child", opTyp.listItemType))
	fmt.Fprintf(g.w, "}\n\n")

	// Generate the DataType method.
	fmt.Fprintf(g.w, "func (e *%s) DataType() *types.T {\n", opTyp.name)
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
		fields := g.md.childAndPrivateFields(define)

		fmt.Fprintf(g.w, "func (m *Memo) Memoize%s(\n", define.Name)
		for _, field := range fields {
			fieldTyp := g.md.typeOf(field)
			fieldName := g.md.fieldName(field)
			fmt.Fprintf(g.w, "  %s %s,\n", unTitle(fieldName), fieldTyp.asParam())
		}
		if define.Tags.Contains("Scalar") {
			fmt.Fprintf(g.w, ") *%s {\n", opTyp.name)
		} else {
			fmt.Fprintf(g.w, ") RelExpr {\n")
		}

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

		for _, field := range fields {
			fieldTyp := g.md.typeOf(field)
			fieldName := g.md.fieldName(field)

			// Use fieldStorePrefix since a value with a static param type is being
			// stored as a field type.
			fmt.Fprintf(g.w, "   %s: %s%s,\n", fieldName, fieldStorePrefix(fieldTyp), unTitle(fieldName))
		}

		if define.Tags.Contains("Scalar") {
			fmt.Fprintf(g.w, "   id: m.NextID(),\n")
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
		fmt.Fprintf(g.w, "  if m.newGroupFn != nil {\n")
		fmt.Fprintf(g.w, "    m.newGroupFn(e)\n")
		fmt.Fprintf(g.w, "  }\n")

		if g.md.hasUnexportedFields(define) {
			fmt.Fprintf(g.w, "  e.initUnexportedFields(m)\n")
		}

		if !define.Tags.Contains("Scalar") {
			fmt.Fprintf(g.w, "  m.logPropsBuilder.build%sProps(e, &grp.rel)\n", define.Name)
			fmt.Fprintf(g.w, "  grp.rel.Populated = true\n")
		}
		fmt.Fprintf(g.w, "    m.memEstimate += size\n")
		fmt.Fprintf(g.w, "    m.CheckExpr(e)\n")
		fmt.Fprintf(g.w, "  }\n")
		if define.Tags.Contains("Scalar") {
			fmt.Fprintf(g.w, "  return interned\n")
		} else {
			// Return the normalized expression if this is a relational expr.
			fmt.Fprintf(g.w, "  return interned.FirstExpr()\n")
		}

		fmt.Fprintf(g.w, "}\n\n")
	}
}

// genAddToGroupFuncs generates AddToGroup methods on the memo.
func (g *exprsGen) genAddToGroupFuncs() {
	defines := g.compiled.Defines.WithTag("Relational").WithoutTag("List").WithoutTag("ListItem")
	for _, define := range defines {
		opTyp := g.md.typeOf(define)

		// The Add...ToGroup functions add a (possibly non-normalized) expression
		// to a memo group. They operate like this:
		//
		// Attempt to intern the expression. This will either give back the
		// original expression, meaning we had not previously interned it, or it
		// will give back a previously interned version.
		//
		// If we hadn't ever seen the expression before, then add it to the group
		// and move on.
		//
		// If we *had* seen it before, check if it is in the same group as the one
		// we're attempting to add it to. If it's in the same group, then this is
		// fine, move along. This happens, for example, if we try to apply
		// CommuteJoin twice.
		//
		// If it's in a different group, then we've learned something interesting:
		// two groups which we previously thought were distinct are actually
		// equivalent. One approach here would be to merge the two groups into a
		// single group, since we've proven that they're equivalent. We do
		// something simpler right now, which is to just bail on trying to add the
		// new expression, leaving the existing instance of it unchanged in its old
		// group. This can result in some expressions not getting fully explored,
		// but we do our best to make this outcome as much of an edge-case as
		// possible, so hopefully this is fine in almost every case.
		// TODO(justin): add telemetry for when group collisions happen. If this is
		// ever happening frequently than that is a bug.

		fmt.Fprintf(g.w, "func (m *Memo) Add%sToGroup(e *%s, grp RelExpr) *%s {\n",
			define.Name, opTyp.name, opTyp.name)
		fmt.Fprintf(g.w, "  const size = int64(unsafe.Sizeof(%s{}))\n", opTyp.name)
		fmt.Fprintf(g.w, "  interned := m.interner.Intern%s(e)\n", define.Name)
		fmt.Fprintf(g.w, "  if interned == e {\n")
		if g.md.hasUnexportedFields(define) {
			fmt.Fprintf(g.w, "    e.initUnexportedFields(m)\n")
		}
		fmt.Fprintf(g.w, "    e.setGroup(grp)\n")
		fmt.Fprintf(g.w, "    m.memEstimate += size\n")
		fmt.Fprintf(g.w, "    m.CheckExpr(e)\n")
		fmt.Fprintf(g.w, "  } else if interned.group() != grp.group() {\n")
		fmt.Fprintf(g.w, "    // This is a group collision, do nothing.\n")
		fmt.Fprintf(g.w, "    return nil\n")
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
	fmt.Fprintf(g.w, "    panic(errors.AssertionFailedf(\"unhandled op: %%s\", e.Op()))\n")
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
				if fieldTyp.usePointerIntern {
					fmt.Fprintf(g.w, "  in.hasher.HashPointer(unsafe.Pointer(val.%s))\n", fieldName)
				} else {
					fmt.Fprintf(g.w, "  in.hasher.Hash%s(val.%s)\n", title(fieldTyp.friendlyName), fieldName)
				}
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
				if fieldTyp.usePointerIntern {
					fmt.Fprintf(g.w, "in.hasher.IsPointerEqual(unsafe.Pointer(val.%s), unsafe.Pointer(existing.%s))",
						fieldName, fieldName)
				} else {
					fmt.Fprintf(g.w, "in.hasher.Is%sEqual(val.%s, existing.%s)",
						title(fieldTyp.friendlyName), fieldName, fieldName)
				}
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
	fmt.Fprintf(g.w, "    panic(errors.AssertionFailedf(\"unhandled type: %%s\", t.Op()))\n")

	fmt.Fprintf(g.w, "  }\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *exprsGen) needsDataTypeField(define *lang.DefineExpr) bool {
	if _, ok := g.constDataType(define); ok {
		return false
	}
	for _, field := range expandFields(g.compiled, define) {
		if field.Name == "Typ" && field.Type == "Type" {
			return false
		}
	}
	return true
}

func (g *exprsGen) constDataType(define *lang.DefineExpr) (_ string, ok bool) {
	for _, typ := range []string{"Bool", "Int", "Float"} {
		if define.Tags.Contains(typ) {
			return "types." + typ, true
		}
	}
	return "", false
}
