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

package memo

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

// ExprNameGenerator is used to generate a unique name for each relational
// expression in a query tree. See GenerateName for details.
type ExprNameGenerator struct {
	prefix    string
	exprCount int
}

// NewExprNameGenerator creates a new instance of ExprNameGenerator,
// initialized with the given prefix.
func NewExprNameGenerator(prefix string) *ExprNameGenerator {
	return &ExprNameGenerator{prefix: prefix}
}

// GenerateName generates a name for a relational expression with the given
// operator. It is used to generate names for each relational expression
// in a query tree, corresponding to the tables that will be created if the
// session variable `save_tables_prefix` is non-empty.
//
// Each invocation of GenerateName is guaranteed to produce a unique name for
// a given instance of ExprNameGenerator. This works because each name is
// appended with a unique, auto-incrementing number. For readability, the
// generated names also contain a common prefix and the name of the relational
// operator separated with underscores. For example: my_query_scan_2.
//
// Since the names are generated with an auto-incrementing number, the order
// of invocation is important. For a given query, the number assigned to each
// relational subexpression corresponds to the order in which the expression
// was encountered during tree traversal. Thus, in order to generate a
// consistent name, always call GenerateName in a pre-order traversal of the
// expression tree.
//
func (g *ExprNameGenerator) GenerateName(op opt.Operator) string {
	// Replace all instances of "-" in the operator name with "_" in order to
	// create a legal table name.
	operator := strings.Replace(op.String(), "-", "_", -1)
	g.exprCount++
	return fmt.Sprintf("%s_%s_%d", g.prefix, operator, g.exprCount)
}
