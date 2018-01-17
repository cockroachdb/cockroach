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

package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Show a session-local variable name.
func (p *planner) ShowVar(ctx context.Context, n *tree.ShowVar) (planNode, error) {
	origName := n.Name
	name := strings.ToLower(n.Name)

	if name == "all" {
		return p.delegateQuery(ctx, "SHOW SESSION ALL", "TABLE crdb_internal.session_variables",
			nil, nil)
	}

	if _, ok := varGen[name]; !ok {
		return nil, fmt.Errorf("unknown variable: %q", origName)
	}

	varName := lex.EscapeSQLString(name)
	nm := tree.Name(name)
	return p.delegateQuery(ctx, "SHOW "+varName,
		fmt.Sprintf(
			`SELECT value AS %[1]s FROM crdb_internal.session_variables `+
				`WHERE variable = %[2]s`,
			nm.String(), varName),
		nil, nil)
}
