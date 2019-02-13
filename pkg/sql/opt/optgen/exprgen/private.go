// Copyright 2019 The Cockroach Authors.
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

package exprgen

import (
	"context"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// evalPrivate evaluates a list of the form
//   [ (FieldName <value>) ... ]
// into an operation private of the given type (e.g. ScanPrivate, etc).
//
// Various implicit conversions are supported. Examples:
//  - table ID: "table"
//  - index ordinal: "table@index"
//  - column lists or sets: "a,b,c"
//  - orderings and ordering choices: "+a,-b"
//  - operators: "inner-join"
//
func (eg *exprGen) evalPrivate(privType reflect.Type, expr lang.Expr) interface{} {
	if expr.Op() != lang.ListOp {
		panic(errorf("private must be a list of the form [ (FieldName Value) ... ]"))
	}

	items := expr.(*lang.ListExpr).Items

	result := reflect.New(privType)

	for _, item := range items {
		// Each item must be of the form (FieldName Value).
		fn, ok := item.(*lang.FuncExpr)
		if !ok || len(fn.Args) != 1 {
			panic(errorf("private list must contain items of the form (FieldName Value)"))
		}
		fieldName := fn.SingleName()
		field := result.Elem().FieldByName(fieldName)
		if !field.IsValid() {
			panic(errorf("invalid field %s for %s", fieldName, privType))
		}
		val := eg.convertPrivateFieldValue(privType, fieldName, field.Type(), eg.eval(fn.Args[0]))
		field.Set(reflect.ValueOf(val))
	}
	return result.Interface()
}

func (eg *exprGen) convertPrivateFieldValue(
	privType reflect.Type, fieldName string, fieldType reflect.Type, value interface{},
) interface{} {

	// This code handles the conversion of a user-friendly value and the value of
	// the field in the private structure.

	if str, ok := value.(string); ok {
		switch fieldType {
		case reflect.TypeOf(opt.TableID(0)):
			return eg.addTable(str)

		case reflect.TypeOf(0):
			if strings.HasSuffix(fieldName, "Index") {
				return eg.findIndex(str)
			}

		case reflect.TypeOf(opt.Operator(0)):
			return eg.opFromStr(str)
		}
	}

	if res := eg.castToDesiredType(value, fieldType); res != nil {
		return res
	}
	panic(errorf("invalid value for %s.%s: %v", privType, fieldName, value))
}

// addTable resolves the given table name and adds the table to the metadata.
func (eg *exprGen) addTable(name string) opt.TableID {
	tn := tree.MakeUnqualifiedTableName(tree.Name(name))
	ds, _, err := eg.cat.ResolveDataSource(context.Background(), &tn)
	if err != nil {
		panic(exprGenErr{err})
	}
	tab, ok := ds.(cat.Table)
	if !ok {
		panic(errorf("non-table datasource %s not supported", name))
	}
	return eg.mem.Metadata().AddTable(tab)
}

// findIndex looks for an index specified as "table@idx_name" among the tables
// already added to the metadata.
func (eg *exprGen) findIndex(str string) int {
	a := strings.Split(str, "@")
	if len(a) != 2 {
		panic(errorf("index must be specified as table@index"))
	}
	table, index := a[0], a[1]
	var tab cat.Table
	for _, meta := range eg.mem.Metadata().AllTables() {
		if meta.Alias.Table() == table {
			if tab != nil {
				panic(errorf("ambiguous table name %s", table))
			}
			tab = meta.Table
		}
	}
	if tab == nil {
		panic(errorf("unknown table %s", table))
	}
	for i := 0; i < tab.IndexCount(); i++ {
		if string(tab.Index(i).Name()) == index {
			return i
		}
	}
	panic(errorf("index %s not found for table %s", index, table))
}

// opFromStr converts an operator string like "inner-join" to the corresponding
// operator.
func (eg *exprGen) opFromStr(str string) opt.Operator {
	for i := opt.Operator(1); i < opt.NumOperators; i++ {
		if i.String() == str {
			return i
		}
	}
	panic(errorf("unknown operator %s", str))
}
