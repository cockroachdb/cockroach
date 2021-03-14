// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blathers

import (
	"context"

	"github.com/google/go-github/v32/github"
)

// isOrgMember returns whether a member is part of the given organization.
func isOrgMember(
	ctx context.Context, ghClient *github.Client, org string, login string,
) (bool, error) {
	isMember, _, err := ghClient.Organizations.IsMember(ctx, org, login)
	if err != nil {
		return false, wrapf(ctx, err, "failed getting organization member status")
	}
	return isMember, err
}

// TODO: cache this.
func getOrganizationLogins(
	ctx context.Context, ghClient *github.Client, org string,
) (map[string]*github.User, error) {
	logins := make(map[string]*github.User)
	opts := &github.ListMembersOptions{
		ListOptions: github.ListOptions{
			PerPage: 100,
		},
	}
	more := true
	for more {
		members, resp, err := ghClient.Organizations.ListMembers(
			ctx,
			org,
			opts,
		)
		if err != nil {
			return nil, wrapf(ctx, err, "error listing org members")
		}
		for _, member := range members {
			logins[member.GetLogin()] = member
		}
		more = resp.NextPage != 0
		if more {
			opts.Page = resp.NextPage
		}
	}
	return logins, nil
}
