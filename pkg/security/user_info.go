// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import "github.com/lib/pq/oid"

// SQLUserInfo s
type SQLUserInfo struct {
	Username SQLUsername
	UserID   oid.Oid
}

// MakeSQLUserInfoFromPreNormalizedString takes a string containing a
// canonical username and user id and converts it to a SQLUsername. The caller
// of this promises that the username argument is pre-normalized. This conversion
// is cheap.
// Note: avoid using this function when processing strings
// in requests from external APIs.
func MakeSQLUserInfoFromPreNormalizedString(username string, ID oid.Oid) SQLUserInfo {
	return SQLUserInfo{Username: MakeSQLUsernameFromPreNormalizedString(username), UserID: ID}
}
