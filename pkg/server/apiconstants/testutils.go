// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiconstants

import "github.com/cockroachdb/cockroach/pkg/security/username"

const (
	// TestingUser is a username available in test servers,
	// that has been granted the admin role.
	TestingUser = "authentic_user"

	// TestingUserNoAdmin is a username available in test servers,
	// that has not been granted the admin role.
	TestingUserNoAdmin = "authentic_user_noadmin"
)

// TestingUserName returns the username of the authenticated
// user with an admin role.
func TestingUserName() username.SQLUsername {
	return username.MakeSQLUsernameFromPreNormalizedString(TestingUser)
}

// TestingUserNameNoAdmin returns the username of the
// authenticated user without an admin role.
func TestingUserNameNoAdmin() username.SQLUsername {
	return username.MakeSQLUsernameFromPreNormalizedString(TestingUserNoAdmin)
}
