// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testcat

import (
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// CreateRole handles the CREATE ROLE/USER statement.
func (tc *Catalog) CreateRole(n *tree.CreateRole) {
	usr, err := username.MakeSQLUsernameFromUserInput(n.Name.Name, username.PurposeCreation)
	if err != nil {
		panic(err)
	}
	_, found := tc.users[usr]
	if found {
		if n.IfNotExists {
			return
		}
		panic(errors.Newf(`user %q already exists`, n.Name.Name))
	}
	tc.users[usr] = roleMembership{isMemberOfAdminRole: false}
}
