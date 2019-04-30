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

package delegate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ValidVars contains the set of variable names; initialized from the SQL
// package.
var ValidVars = make(map[string]struct{})

// Show a session-local variable name.
func (d *delegator) delegateShowVar(n *tree.ShowVar) (tree.Statement, error) {
	origName := n.Name
	name := strings.ToLower(n.Name)

	if name == "all" {
		return parse(
			"SELECT variable, value FROM crdb_internal.session_variables WHERE hidden = FALSE",
		)
	}

	if _, ok := ValidVars[name]; !ok {
		return nil, pgerror.Newf(pgerror.CodeUndefinedObjectError,
			"unrecognized configuration parameter %q", origName)
	}

	varName := lex.EscapeSQLString(name)
	nm := tree.Name(name)
	return parse(fmt.Sprintf(
		`SELECT value AS %[1]s FROM crdb_internal.session_variables WHERE variable = %[2]s`,
		nm.String(), varName,
	))
}
