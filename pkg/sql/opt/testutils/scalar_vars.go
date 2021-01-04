// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// ScalarVars is a helper used to populate the metadata with specified columns,
// useful for tests involving scalar expressions.
type ScalarVars struct {
	cols         opt.ColSet
	notNullCols  opt.ColSet
	computedCols map[opt.ColumnID]tree.Expr
}

// Init parses variables definition strings, adds new columns to the metadata,
// and initializes the ScalarVars.
//
// Each definition string is of the form:
//   "<var-name> type1 [not null]
//
// The not-null columns can be retrieved via NotNullCols().
//
func (sv *ScalarVars) Init(md *opt.Metadata, vars []string) error {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*sv = ScalarVars{}
	// We use the same syntax that is used with CREATE TABLE, so reuse the parsing
	// logic.
	varDef := strings.Join(vars, ", ")
	sql := fmt.Sprintf("CREATE TABLE foo (%s)", varDef)
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		return errors.Wrapf(err, "invalid vars definition '%s'", varDef)
	}
	ct := stmt.AST.(*tree.CreateTable)
	for _, d := range ct.Defs {
		cd, ok := d.(*tree.ColumnTableDef)
		if !ok {
			return errors.Newf("invalid vars definition '%s'", varDef)
		}
		if cd.PrimaryKey.IsPrimaryKey || cd.Unique.IsUnique || cd.DefaultExpr.Expr != nil ||
			len(cd.CheckExprs) > 0 || cd.References.Table != nil || cd.Family.Name != "" {
			return errors.Newf("invalid vars definition '%s'", varDef)
		}
		typ := tree.MustBeStaticallyKnownType(cd.Type)
		id := md.AddColumn(string(cd.Name), typ)
		sv.cols.Add(id)
		if cd.Nullable.Nullability == tree.NotNull {
			sv.notNullCols.Add(id)
		}

		if cd.Computed.Computed {
			if sv.computedCols == nil {
				sv.computedCols = make(map[opt.ColumnID]tree.Expr)
			}
			sv.computedCols[id] = cd.Computed.Expr
		}
	}
	return nil
}

// Cols returns all the columns.
func (sv *ScalarVars) Cols() opt.ColSet {
	return sv.cols
}

// NotNullCols returns the columns that correspond to not null variables.
func (sv *ScalarVars) NotNullCols() opt.ColSet {
	return sv.notNullCols
}

// ComputedCols returns a map of computed column expressions.
func (sv *ScalarVars) ComputedCols() map[opt.ColumnID]tree.Expr {
	return sv.computedCols
}
