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

// SetVar implements the 'SET ...' SQL statement. Currently, it supports only
// statements that set the current role (e.g., SET ROLE <user>).
func (tc *Catalog) SetVar(n *tree.SetVar) {
	if n.Name != "role" {
		panic(errors.Newf("SET only supports SET ROLE: %q", n.Name))
	}
	if len(n.Values) != 1 {
		panic(errors.Newf("Only support 1 value with SET"))
	}
	if n.ResetAll {
		panic(errors.Newf("RESET ALL is not supported with SET"))
	}
	newUser, err := username.MakeSQLUsernameFromUserInput(n.Values[0].String(), username.PurposeValidation)
	if err != nil {
		panic(err)
	}
	if _, found := tc.users[newUser]; !found {
		panic(errors.Newf("User %q does not exist", newUser.Normalized()))
	}
	tc.currentUser = newUser
}
