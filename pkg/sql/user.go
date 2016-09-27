// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Alfonso Subiotto Marqués (alfonso@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/pkg/errors"
)

// GetUserHashedPassword returns the hashedPassword for the given username if
// found in system.users.
func GetUserHashedPassword(username string, session *Session, executor *Executor) ([]byte, error) {
	if session == nil || executor == nil {
		return nil, errors.Errorf("error looking up user %s", username)
	}
	// The root user is not in system.users.
	if username == security.RootUser {
		return nil, nil
	}

	placeholderInfo := parser.NewPlaceholderInfo()
	placeholderInfo.SetValue("1", parser.NewDString(username))

	// The session's user has to be changed to RootUser to ensure SELECT
	// privilege on the system.users table.
	// A circular dependency between the session's executor's planner and the
	// original session plus an inability to change the planner's pointer
	// (due to the field not being public) means that making a shallow copy of
	// session and modifying the copy's user has no effect since when
	// executing a query, a planner's session (in this case session) will be
	// used to determine the User and therefore the privileges.
	var originalSessionUser string
	originalSessionUser, session.User = session.User, security.RootUser
	r := executor.ExecuteStatements(
		session, "SELECT hashedPassword FROM system.users WHERE username=$1", placeholderInfo,
	)
	session.User = originalSessionUser
	defer r.Close()

	if len(r.ResultList) != 1 || r.ResultList[0].Err != nil {
		return nil, errors.Errorf("error looking up user %s", username)
	} else if r.ResultList[0].Rows.Len() != 1 {
		return nil, errors.Errorf("user %s does not exist", username)
	}

	return []byte(*r.ResultList[0].Rows.At(0)[0].(*parser.DBytes)), nil
}
