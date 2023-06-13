// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessioninit

import (
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// UsersTableName represents system.users.
var UsersTableName = tree.NewTableNameWithSchema("system", catconstants.PublicSchemaName, "users")

// RoleOptionsTableName represents system.role_options.
var RoleOptionsTableName = tree.NewTableNameWithSchema("system", catconstants.PublicSchemaName, "role_options")

// DatabaseRoleSettingsTableName represents system.database_role_settings.
var DatabaseRoleSettingsTableName = tree.NewTableNameWithSchema("system", catconstants.PublicSchemaName, "database_role_settings")

// defaultDatabaseID is used in the settingsCache for entries that should
// apply to all database.
const defaultDatabaseID = 0

// defaultUsername is used in the settingsCache for entries that should
// apply to all roles.
var defaultUsername = username.MakeSQLUsernameFromPreNormalizedString("")
