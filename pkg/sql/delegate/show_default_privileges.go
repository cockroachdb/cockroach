// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// delegateShowDefaultPrivileges implements SHOW DEFAULT PRIVILEGES
// which returns default privileges for a specified role.
func (d *delegator) delegateShowDefaultPrivileges(
	n *tree.ShowDefaultPrivileges,
) (tree.Statement, error) {
	currentDatabase, err := d.getSpecifiedOrCurrentDatabase("")
	if err != nil {
		return nil, err
	}

	schemaClause := " AND schema_name IS NULL"
	if n.Schema != "" {
		schemaClause = fmt.Sprintf(" AND schema_name = %s", lexbase.EscapeSQLString(n.Schema.String()))
	}

	query := fmt.Sprintf(
		"SELECT role, for_all_roles, object_type, grantee, privilege_type, is_grantable "+
			"FROM crdb_internal.default_privileges WHERE database_name = %s%s",
		lexbase.EscapeSQLString(currentDatabase.Normalize()),
		schemaClause,
	)

	if n.ForAllRoles {
		query += " AND for_all_roles=true"
	} else if len(n.Roles) > 0 {
		targetRoles, err := decodeusername.FromRoleSpecList(
			d.evalCtx.SessionData(), username.PurposeValidation, n.Roles,
		)
		if err != nil {
			return nil, err
		}

		query = fmt.Sprintf("%s AND for_all_roles=false AND role IN (", query)
		for i, role := range targetRoles {
			if i != 0 {
				query += fmt.Sprintf(", '%s'", role.Normalized())
			} else {
				query += fmt.Sprintf("'%s'", role.Normalized())
			}
		}

		query += ")"
	} else {
		query = fmt.Sprintf("%s AND for_all_roles=false AND role = '%s'",
			query, d.evalCtx.SessionData().User())
	}
	query += " ORDER BY 1,2,3,4,5"
	return parse(query)
}
