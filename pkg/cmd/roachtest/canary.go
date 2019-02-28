// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// This file contains common elements for all 3rd party test suite roachtests.
// TODO(bram): There are more common elements between all the canary tests,
// factor more of them into here.

// blacklist is a lists of known test errors and failures.
type blacklist map[string]string

// blacklistForVersion contains both a blacklist of known test errors and
// failures but also an optional ignorelist for flaky tests.
// When the test suite is run, the results are compared to this list.
// Any passed test that is not on this blacklist is reported as PASS - expected
// Any passed test that is on this blacklist is reported as PASS - unexpected
// Any failed test that is on this blacklist is reported as FAIL - expected
// Any failed test that is not on blackthis list is reported as FAIL - unexpected
// Any test on this blacklist that is not run is reported as FAIL - not run
// Ant test in the ignorelist is reported as SKIP if it is run
type blacklistForVersion struct {
	versionPrefix  string
	blacklistname  string
	blacklist      blacklist
	ignorelistname string
	ignorelist     blacklist
}

type blacklistsForVersion []blacklistForVersion

// getLists returns the appropriate blacklist and ignorelist based on the
// cockroach version. This check only looks to ensure that the prefix that
// matches.
func (b blacklistsForVersion) getLists(version string) (string, blacklist, string, blacklist) {
	for _, info := range b {
		if strings.HasPrefix(version, info.versionPrefix) {
			return info.blacklistname, info.blacklist, info.ignorelistname, info.ignorelist
		}
	}
	return "", nil, "", nil
}

func fetchCockroachVersion(ctx context.Context, c *cluster, nodeIndex int) (string, error) {
	db, err := c.ConnE(ctx, nodeIndex)
	if err != nil {
		return "", err
	}
	defer db.Close()
	var version string
	if err := db.QueryRowContext(ctx,
		`SELECT value FROM crdb_internal.node_build_info where field = 'Version'`,
	).Scan(&version); err != nil {
		return "", err
	}
	return version, nil
}

// maybeAddGithubLink will take the issue and if it is just a number, then it
// will return a full github link.
func maybeAddGithubLink(issue string) string {
	if len(issue) == 0 {
		return ""
	}
	issueNum, err := strconv.Atoi(issue)
	if err != nil {
		return issue
	}
	return fmt.Sprintf("https://github.com/cockroachdb/cockroach/issues/%d", issueNum)
}
