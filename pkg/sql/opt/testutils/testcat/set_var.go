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

// SetVar implements the 'SET ...' SQL statement.
func (tc *Catalog) SetVar(n *tree.SetVar) {
	if len(n.Values) != 1 {
		panic(errors.Newf("Only support 1 value with SET"))
	}
	if n.ResetAll {
		panic(errors.Newf("RESET ALL is not supported with SET"))
	}
	if n.Name == "role" {
		newUser, err := username.MakeSQLUsernameFromUserInput(n.Values[0].String(), username.PurposeValidation)
		if err != nil {
			panic(err)
		}
		if _, found := tc.users[newUser]; !found {
			panic(errors.Newf("User %q does not exist", newUser.Normalized()))
		}
		tc.currentUser = newUser
	} else if _, isReset := n.Values[0].(tree.DefaultVal); isReset {
		// "SET var = DEFAULT" means RESET.
		if tc.sessionVars != nil {
			delete(tc.sessionVars, n.Name)
		}
	} else {
		if tc.sessionVars == nil {
			tc.sessionVars = make(map[string]string)
		}
		tc.sessionVars[n.Name] = n.Values[0].String()
	}
}
