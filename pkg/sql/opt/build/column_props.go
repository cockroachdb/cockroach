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

package build

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// columnProps holds per-column information that is scoped to a particular
// relational expression. Note that columnProps implements the tree.TypedExpr
// interface. During name resolution, unresolved column names in the AST are
// replaced with a columnProps.
type columnProps struct {
	name  optbase.ColumnName
	table optbase.TableName
	typ   types.T

	// index is an identifier for this column, which is unique across all the
	// columns in the query.
	index  xform.ColumnIndex
	hidden bool
}

var _ tree.TypedExpr = &columnProps{}
var _ tree.VariableExpr = &columnProps{}

func (c columnProps) String() string {
	if c.table == "" {
		return tree.NameString(string(c.name))
	}
	return fmt.Sprintf("%s.%s",
		tree.NameString(string(c.table)), tree.NameString(string(c.name)))
}

func (c columnProps) matches(tblName optbase.TableName, colName optbase.ColumnName) bool {
	if colName != c.name {
		return false
	}
	if tblName == "" {
		return true
	}
	return c.table == tblName
}
