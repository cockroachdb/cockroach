// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package syntheticprivilege

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// SystemPrivilegesTableName represents system.privileges.
var SystemPrivilegesTableName = tree.NewTableNameWithSchema("system", catconstants.PublicSchemaName, "privileges")
