// Copyright 2024 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// delegateShowVariablesForRole implements SHOW VARIABLES FOR ROLE <name> which returns all the default session values
// for the given role.
// Privileges: None.
func (d *delegator) delegateShowVariablesForRole(
	n *tree.ShowVariablesForRole,
) (tree.Statement, error) {

	getVariablesForRoleQuery := `SELECT split_part(setting, '=', 1) AS session_variables,
		substring(setting FROM position('=' in setting) + 1) AS default_values
		FROM system.database_role_settings,
	     unnest(settings) AS setting
		WHERE role_name = ''`

	if n.All {
		return d.parse(getVariablesForRoleQuery)
	}

	name := n.Name.Name
	getVariablesForRoleQuery += fmt.Sprintf(
		`
			UNION
			SELECT split_part(setting, '=', 1) AS session_variables,
			substring(setting FROM position('=' in setting) + 1) AS default_values   
			FROM system.database_role_settings,
				unnest(settings) AS setting
      WHERE role_name = '%[1]s';`,
		name)
	return d.parse(getVariablesForRoleQuery)
}
