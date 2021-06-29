// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package authentication

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// UsersTableName represents system.users.
var UsersTableName = tree.NewTableNameWithSchema("system", tree.PublicSchemaName, "users")

// RoleOptionsTableName represents system.role_options.
var RoleOptionsTableName = tree.NewTableNameWithSchema("system", tree.PublicSchemaName, "role_options")
