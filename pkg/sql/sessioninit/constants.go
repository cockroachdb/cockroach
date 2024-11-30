// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessioninit

import "github.com/cockroachdb/cockroach/pkg/security/username"

// defaultDatabaseID is used in the settingsCache for entries that should
// apply to all database.
const defaultDatabaseID = 0

// defaultUsername is used in the settingsCache for entries that should
// apply to all roles.
var defaultUsername = username.MakeSQLUsernameFromPreNormalizedString("")
