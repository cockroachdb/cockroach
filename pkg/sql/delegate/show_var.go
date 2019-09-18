// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// ValidVars contains the set of variable names; initialized from the SQL
// package.
var ValidVars = make(map[string]struct{})

// Show a session-local variable name.
func (d *delegator) delegateShowVar(n *tree.ShowVar) (tree.Statement, error) {
	origName := n.Name
	name := strings.ToLower(n.Name)

	if name == "locality" {
		sqltelemetry.IncrementShowCounter(sqltelemetry.Locality)
	}

	if name == "all" {
		return parse(
			"SELECT variable, value FROM crdb_internal.session_variables WHERE hidden = FALSE",
		)
	}

	if _, ok := ValidVars[name]; !ok {
		return nil, pgerror.Newf(pgcode.UndefinedObject,
			"unrecognized configuration parameter %q", origName)
	}

	varName := lex.EscapeSQLString(name)
	nm := tree.Name(name)
	return parse(fmt.Sprintf(
		`SELECT value AS %[1]s FROM crdb_internal.session_variables WHERE variable = %[2]s`,
		nm.String(), varName,
	))
}
