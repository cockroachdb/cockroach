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

	"github.com/cockroachdb/errors"
	"github.com/google/go-github/v32/github"
)

func findProjectsForTeams(
	ctx context.Context, ghClient *github.Client, teams []string,
) (map[string]int64, error) {
	// TODO(blathers): use TEAMS.yaml.

	type key struct {
		owner  string
		repo   string
		column string
	}
	type val struct {
		name string
		team string
	}
	searchBy := map[key][]val{}
	for _, team := range teams {
		board, ok := teamToBoards[team]
		if !ok {
			continue
		}
		k := key{owner: board.owner, repo: board.repo, column: board.column}
		searchBy[k] = append(searchBy[k], val{name: board.name, team: team})
	}
	ret := map[string]int64{}
	for k, vals := range searchBy {
		opts := &github.ProjectListOptions{
			ListOptions: github.ListOptions{
				PerPage: 100,
			},
		}
		more := true
		for more {
			var r []*github.Project
			var resp *github.Response
			var err error
			if k.repo != "" {
				r, resp, err = ghClient.Repositories.ListProjects(ctx, k.owner, k.repo, opts)
			} else {
				r, resp, err = ghClient.Organizations.ListProjects(ctx, k.owner, opts)
			}
			if err != nil {
				return nil, wrapf(ctx, err, "error finding projects")
			}
			for _, proj := range r {
				// Purposefully n^2 (because I am lazy).
				for _, val := range vals {
					if proj.GetName() == val.name {
						id, err := findColumnForProject(ctx, ghClient, proj.GetID(), k.column)
						if err != nil {
							if e, ok := err.(notFoundError); ok {
								ret[val.team] = e.defaultID
								break
							}
						}
						ret[val.team] = id
						break
					}
				}
			}

			more = resp.NextPage != 0
			if more {
				opts.Page = resp.NextPage
			}
		}
	}
	return ret, nil
}

type notFoundError struct {
	error
	defaultID int64
}

func findColumnForProject(
	ctx context.Context, ghClient *github.Client, projectID int64, columnName string,
) (int64, error) {
	more := true
	opts := &github.ListOptions{
		PerPage: 100,
	}
	// default to first column it sees.
	// there is a test to ensure columns exist; fallback to nice behaviour.
	colID := int64(0)
	for more {
		cols, resp, err := ghClient.Projects.ListProjectColumns(ctx, projectID, opts)
		if err != nil {
			return 0, wrapf(ctx, err, "error adding project cols")
		}
		for _, col := range cols {
			if col.GetName() == columnName {
				return col.GetID(), nil
			}
			if colID == 0 {
				colID = col.GetID()
			}
		}
		more = resp.NextPage != 0
		if more {
			opts.Page = resp.NextPage
		}
	}
	return 0, notFoundError{errors.Newf("no column id found"), colID}
}
