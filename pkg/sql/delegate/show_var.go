// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"strings"

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
	name := strings.ToLower(n.Name)

	if name == "locality" {
		sqltelemetry.IncrementShowCounter(sqltelemetry.Locality)
	}

	if name == "all" {
		return d.parse(
			"SELECT variable, value FROM crdb_internal.session_variables WHERE hidden = FALSE",
		)
	}

	// TODO(richardjcai): Remove this clause by making the `SetVar` for
	// the database session variable verify if the database exists or not.
	// Currently, on connection to a database, we rely on a query to
	// hit the database resolution path giving us a database is undefined
	// error. The below query allows us to keep this behaviour.
	if name == "database" {
		return d.parse(
			"SELECT value as database FROM crdb_internal.session_variables WHERE variable = 'database'")
	}

	if _, ok := ValidVars[name]; !ok {
		// Custom options go to planNode.
		if strings.Contains(name, ".") {
			return nil, nil
		}
		return nil, pgerror.Newf(pgcode.UndefinedObject,
			"unrecognized configuration parameter %q", n.Name)
	}

	return nil, nil
}
