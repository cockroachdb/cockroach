// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package username contains utilities for interacting with usernames
// and their specs as defined in tree.
package decodeusername

import (
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// FromRoleSpec converts a tree.RoleSpec to a security.SQLUsername.
func FromRoleSpec(
	sessionData *sessiondata.SessionData, purpose security.UsernamePurpose, r tree.RoleSpec,
) (security.SQLUsername, error) {
	if r.RoleSpecType == tree.CurrentUser {
		return sessionData.User(), nil
	} else if r.RoleSpecType == tree.SessionUser {
		return sessionData.SessionUser(), nil
	}
	username, err := security.MakeSQLUsernameFromUserInput(r.Name, purpose)
	if err != nil {
		if errors.Is(err, security.ErrUsernameTooLong) {
			err = pgerror.WithCandidateCode(err, pgcode.NameTooLong)
		} else if errors.IsAny(err, security.ErrUsernameInvalid, security.ErrUsernameEmpty) {
			err = pgerror.WithCandidateCode(err, pgcode.InvalidName)
		}
		return username, errors.Wrapf(err, "%q", username)
	}
	return username, nil
}

// FromRoleSpecList converts a tree.RoleSpecList to a slice of security.SQLUsername.
func FromRoleSpecList(
	sessionData *sessiondata.SessionData, purpose security.UsernamePurpose, l tree.RoleSpecList,
) ([]security.SQLUsername, error) {
	targetRoles := make([]security.SQLUsername, len(l))
	for i, role := range l {
		user, err := FromRoleSpec(sessionData, purpose, role)
		if err != nil {
			return nil, err
		}
		targetRoles[i] = user
	}
	return targetRoles, nil
}

// FromNameList converts a tree.NameList containing SQL input of usernames,
// normalizes the names and returns them as a list of SQLUsernames.
func FromNameList(l tree.NameList) ([]security.SQLUsername, error) {
	targetRoles := make([]security.SQLUsername, len(l))
	for i, role := range l {
		user, err := security.MakeSQLUsernameFromUserInput(string(role), security.UsernameValidation)
		if err != nil {
			return nil, err
		}
		targetRoles[i] = user
	}
	return targetRoles, nil
}
