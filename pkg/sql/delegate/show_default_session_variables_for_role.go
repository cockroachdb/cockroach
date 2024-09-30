// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
)

// delegateShowDefaultSessionVariablesForRole implements SHOW DEFAULT SESSION VARIABLES FOR ROLE <name> which returns all the default session variables for a
// user.
// for the given role.
// Privileges: None.
func (d *delegator) delegateShowDefaultSessionVariablesForRole(
	n *tree.ShowDefaultSessionVariablesForRole,
) (tree.Statement, error) {

	// Check if user has at least one of CREATEROLE, MODIFYCLUSTERSETTING, or MODIFYSQLCLUSTERSETTING privileges
	hasPrivilege := false
	cat := d.catalog
	globalPrivObj := syntheticprivilege.GlobalPrivilegeObject
	user := cat.GetCurrentUser()
	if err := cat.CheckPrivilege(d.ctx, globalPrivObj, user, privilege.CREATEROLE); err == nil {
		hasPrivilege = true
	} else if pgerror.GetPGCode(err) != pgcode.InsufficientPrivilege {
		return nil, err
	}
	if !hasPrivilege {
		if err := cat.CheckPrivilege(d.ctx, globalPrivObj, user, privilege.MODIFYCLUSTERSETTING); err == nil {
			hasPrivilege = true
		} else if pgerror.GetPGCode(err) != pgcode.InsufficientPrivilege {
			return nil, err
		}
	}
	if !hasPrivilege {
		if err := cat.CheckPrivilege(d.ctx, globalPrivObj, user, privilege.MODIFYSQLCLUSTERSETTING); err == nil {
			hasPrivilege = true
		} else if pgerror.GetPGCode(err) != pgcode.InsufficientPrivilege {
			return nil, err
		}
	}

	// If user is not admin and has neither privilege, return an error.
	if !hasPrivilege {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"only users with %s, %s or %s privileges are allowed to SHOW DEFAULT SESSION VARIABLES FOR ROLE",
			privilege.CREATEROLE, privilege.MODIFYCLUSTERSETTING, privilege.MODIFYSQLCLUSTERSETTING)
	}

	getVariablesForRoleQuery := `SELECT split_part(setting, '=', 1) AS session_variables,
			substring(setting FROM position('=' in setting) + 1) AS default_values,
			datname AS database
			FROM pg_catalog.pg_db_role_setting pg1
			LEFT JOIN pg_catalog.pg_roles pg2 ON pg1.setrole = pg2.oid
			LEFT JOIN pg_catalog.pg_database pg3 ON pg1.setdatabase = pg3.oid
			CROSS JOIN unnest(pg1.setconfig) AS setting
      WHERE pg2.rolname IS NULL`

	if n.All {
		return d.parse(getVariablesForRoleQuery)
	}

	name := n.Name.Name
	getVariablesForRoleQuery = fmt.Sprintf(
		`WITH
  global AS (
    SELECT
      split_part(setting, '=', 1) AS session_variables,
      substring(setting FROM position('=' in setting) + 1) AS default_values,
      datname AS database,
      CASE WHEN rolname IS NULL OR datname IS NULL THEN true ELSE false END AS inherited_globally
    FROM
      pg_catalog.pg_db_role_setting pg1
      LEFT JOIN pg_catalog.pg_roles pg2 ON pg1.setrole = pg2.oid
      LEFT JOIN pg_catalog.pg_database pg3 ON pg1.setdatabase = pg3.oid
      CROSS JOIN unnest(pg1.setconfig) AS setting
    WHERE
      pg2.rolname IS NULL
  ),
  specified AS (
    SELECT
      split_part(setting, '=', 1) AS session_variables,
      substring(setting FROM position('=' in setting) + 1) AS default_values,
      datname AS database,
      CASE WHEN rolname IS NULL OR datname IS NULL THEN true ELSE false END AS inherited_globally
    FROM
      pg_catalog.pg_db_role_setting pg1
      LEFT JOIN pg_catalog.pg_roles pg2 ON pg1.setrole = pg2.oid
      LEFT JOIN pg_catalog.pg_database pg3 ON pg1.setdatabase = pg3.oid
      CROSS JOIN unnest(pg1.setconfig) AS setting
    WHERE
      (pg2.rolname = '%[1]s')
  )
	SELECT * FROM global WHERE NOT EXISTS(SELECT 1 FROM specified WHERE specified.session_variables=global.session_variables)
		 UNION
	SELECT * FROM specified AS combined_result;`,
		name)

	return d.parse(getVariablesForRoleQuery)
}
