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
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/retry"
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

// The following functions are augmented basic cluster functions but there tends
// to be common networking issues that cause test failures and require putting
// a retry block around them.

var canaryRetryOptions = retry.Options{
	InitialBackoff: 10 * time.Second,
	Multiplier:     2,
	MaxBackoff:     5 * time.Minute,
	MaxRetries:     10,
}

// repeatRunE is the same function as c.RunE but with an automatic retry loop.
func repeatRunE(
	ctx context.Context, c *cluster, node nodeListOption, operation string, args ...string,
) error {
	var lastError error
	for attempt, r := 0, retry.StartWithCtx(ctx, canaryRetryOptions); r.Next(); {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.t.Failed() {
			return fmt.Errorf("test has failed")
		}
		attempt++
		c.l.Printf("attempt %d - %s", attempt, operation)
		lastError = c.RunE(ctx, node, args...)
		if lastError != nil {
			c.l.Printf("error - retrying: %s", lastError)
			continue
		}
		return nil
	}
	return fmt.Errorf("all attempts failed for %s due to error: %s", operation, lastError)
}

// repeatRunWithBuffer is the same function as c.RunWithBuffer but with an
// automatic retry loop.
func repeatRunWithBuffer(
	ctx context.Context, c *cluster, l *logger, node nodeListOption, operation string, args ...string,
) ([]byte, error) {
	var lastError error
	for attempt, r := 0, retry.StartWithCtx(ctx, canaryRetryOptions); r.Next(); {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if c.t.Failed() {
			return nil, fmt.Errorf("test has failed")
		}
		attempt++
		c.l.Printf("attempt %d - %s", attempt, operation)
		var result []byte
		result, lastError = c.RunWithBuffer(ctx, l, node, args...)
		if lastError != nil {
			c.l.Printf("error - retrying: %s", lastError)
			continue
		}
		return result, nil
	}
	return nil, fmt.Errorf("all attempts failed for %s, with error: %s", operation, lastError)
}

// repeatGitCloneE is the same function as c.GitCloneE but with an automatic
// retry loop.
func repeatGitCloneE(
	ctx context.Context, c *cluster, src, dest, branch string, node nodeListOption,
) error {
	var lastError error
	for attempt, r := 0, retry.StartWithCtx(ctx, canaryRetryOptions); r.Next(); {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.t.Failed() {
			return fmt.Errorf("test has failed")
		}
		attempt++
		c.l.Printf("attempt %d - clone %s", attempt, src)
		lastError = c.GitCloneE(ctx, src, dest, branch, node)
		if lastError != nil {
			c.l.Printf("error - retrying: %s", lastError)
			continue
		}
		return nil
	}
	return fmt.Errorf("Could not clone %s due to error: %s", src, lastError)
}

var canaryTagRegex = regexp.MustCompile(`^[^a-zA-Z]*$`)

// repeatGetLatestTag fetches the latest (sorted) tag from a github repo.
// There is no equivalent function on the cluster as this is really only needed
// for the canary tests.
// NB: that this is kind of brittle, as it looks for the latest tag without any
// letters in it, as alphas, betas and rcs tend to have letters in them. It
// might make sense to instead pass in a regex specific to the repo instead
// of using the one here.
func repeatGetLatestTag(ctx context.Context, c *cluster, user string, repo string) (string, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/tags", user, repo)
	httpClient := &http.Client{Timeout: 10 * time.Second}
	type Tag struct {
		Name string
	}
	type Tags []Tag
	var lastError error
	for attempt, r := 0, retry.StartWithCtx(ctx, canaryRetryOptions); r.Next(); {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if c.t.Failed() {
			return "", fmt.Errorf("test has failed")
		}
		attempt++

		c.l.Printf("attempt %d - fetching %s", attempt, url)
		var resp *http.Response
		resp, lastError = httpClient.Get(url)
		if lastError != nil {
			c.l.Printf("error fetching - retrying: %s", lastError)
			continue
		}
		defer resp.Body.Close()

		var tags Tags
		lastError = json.NewDecoder(resp.Body).Decode(&tags)
		if lastError != nil {
			c.l.Printf("error decoding - retrying: %s", lastError)
			continue
		}
		if len(tags) == 0 {
			return "", fmt.Errorf("no tags found at %s", url)
		}

		var actualTags []string
		for _, t := range tags {
			if canaryTagRegex.MatchString(t.Name) {
				actualTags = append(actualTags, t.Name)
			}
		}
		sort.Strings(actualTags)
		return actualTags[len(actualTags)-1], nil
	}
	return "", fmt.Errorf("could not get tags from %s, due to error: %s", url, lastError)
}
