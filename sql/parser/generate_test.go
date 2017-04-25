// +build generate

/*
This file is used to generate the function and operator documentation. It
is implemented as a test because that is a convinent way to run arbitrary
code post-init. (We cannot simply use static code analysis because functions
are invoked that help populate the operator and function lists.) Running
this test will write operators.md and functions.md to docs/_includes/sql.

Run from this directory with: go test -tags generate -run Generate

Use -dir=/path/to/docs/_includes/sql to modify the output directory.
*/

package parser

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

var outDir = flag.String("dir", filepath.Join("..", "..", "..", "docs", "_includes", "sql"), "generated output directory")

type operation struct {
	left  string
	right string
	ret   string
	op    string
}

func (o operation) String() string {
	if o.right == "" {
		return fmt.Sprintf("%s%s", o.op, o.left)
	}
	return fmt.Sprintf("%s %s %s", o.left, o.op, o.right)
}

type operations []operation

func (p operations) Len() int      { return len(p) }
func (p operations) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p operations) Less(i, j int) bool {
	if p[i].right != "" && p[j].right == "" {
		return false
	}
	if p[i].right == "" && p[j].right != "" {
		return true
	}
	if p[i].left != p[j].left {
		return p[i].left < p[j].left
	}
	if p[i].right != p[j].right {
		return p[i].right < p[j].right
	}
	return p[i].ret < p[j].ret
}

func TestGenerateOperators(t *testing.T) {
	ops := make(map[string]operations)
	for k, v := range unaryOps {
		op := k.op.String()
		typ := typeName(k.argType)
		ops[op] = append(ops[op], operation{
			left: typ,
			ret:  v.returnType.Type(),
			op:   op,
		})
	}
	for k, v := range binOps {
		op := k.op.String()
		left := typeName(k.leftType)
		right := typeName(k.rightType)
		ops[op] = append(ops[op], operation{
			left:  left,
			right: right,
			ret:   v.returnType.Type(),
			op:    op,
		})
	}
	for k := range cmpOps {
		op := k.op.String()
		left := typeName(k.leftType)
		right := typeName(k.rightType)
		ops[op] = append(ops[op], operation{
			left:  left,
			right: right,
			ret:   "bool",
			op:    op,
		})
	}
	var opstrs []string
	for k, v := range ops {
		sort.Sort(v)
		opstrs = append(opstrs, k)
	}
	sort.Strings(opstrs)
	b := new(bytes.Buffer)
	b.WriteString("## Operators\n\n")
	for _, op := range opstrs {
		fmt.Fprintf(b, "`%s` | Return\n", op)
		fmt.Fprintf(b, "--- | ---\n")
		for _, v := range ops[op] {
			fmt.Fprintf(b, "`%s` | `%s`\n", v.String(), v.ret)
		}
		fmt.Fprintln(b)
	}
	if err := ioutil.WriteFile(filepath.Join(*outDir, "operators.md"), b.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestGenerateFunctions(t *testing.T) {
	typePtrs := make(map[uintptr]string)
	typeFns := map[string]interface{}{
		"bytes":     typeBytes,
		"date":      typeDate,
		"float":     typeFloat,
		"decimal":   typeDecimal,
		"int":       typeInt,
		"interval":  typeInterval,
		"string":    typeString,
		"timestamp": typeTimestamp,
		"T":         typeTuple,
	}
	for name, v := range typeFns {
		typePtrs[reflect.ValueOf(v).Pointer()] = name
	}
	functions := make(map[string][]string)
	for name, fns := range builtins {
		for _, fn := range fns {
			var args string
			switch ft := fn.types.(type) {
			case argTypes:
				var typs []string
				for _, typ := range ft {
					typs = append(typs, typeName(typ))
				}
				args = strings.Join(typs, ", ")
			case anyType:
				args = "T, ..."
			case variadicType:
				args = fmt.Sprintf("%s, ...", typeName(ft.typ))
			default:
				t.Fatalf("unknown type: %T", ft)
			}
			fp := reflect.ValueOf(fn.returnType).Pointer()
			ret, ok := typePtrs[fp]
			if !ok {
				// Aggregate function.
				ret = args
			}
			s := fmt.Sprintf("%s(%s) | %s", name, args, ret)
			functions[ret] = append(functions[ret], s)
		}
	}
	var rets []string
	for k, v := range functions {
		sort.Strings(v)
		rets = append(rets, k)
	}
	sort.Strings(rets)
	b := new(bytes.Buffer)
	b.WriteString("## Functions\n\n")
	for _, ret := range rets {
		fmt.Fprintf(b, "%s functions | Return\n", ret)
		b.WriteString("--- | ---\n")
		b.WriteString(strings.Join(functions[ret], "\n"))
		b.WriteString("\n\n")
	}
	if err := ioutil.WriteFile(filepath.Join(*outDir, "functions.md"), b.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
}

func typeName(t reflect.Type) string {
	return reflect.Zero(t).Interface().(Datum).Type()
}
