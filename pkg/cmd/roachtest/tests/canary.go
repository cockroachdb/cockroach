// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// This file contains common elements for all 3rd party test suite roachtests.
// TODO(bram): There are more common elements between all the canary tests,
// factor more of them into here.

// blocklist is a lists of known test errors and failures.
type blocklist map[string]string

// blocklistForVersion contains both a blocklist of known test errors and
// failures but also an optional ignorelist for flaky tests.
// When the test suite is run, the results are compared to this list.
// Any passed test that is not on this blocklist is reported as PASS - expected
// Any passed test that is on this blocklist is reported as PASS - unexpected
// Any failed test that is on this blocklist is reported as FAIL - expected
// Any failed test that is not on blocklist list is reported as FAIL - unexpected
// Any test on this blocklist that is not run is reported as FAIL - not run
// Ant test in the ignorelist is reported as SKIP if it is run
type blocklistForVersion struct {
	versionPrefix  string
	blocklistname  string
	blocklist      blocklist
	ignorelistname string
	ignorelist     blocklist
}

type blocklistsForVersion []blocklistForVersion

// SecureDBConnectionParams contains information used to create a secure
// connection to CRDB.
type SecureDBConnectionParams struct {
	username string
	certsDir string
	port     int
}

// NewSecureDBConnectionParams creates a SecureDBConnectionParams struct for creating
// a secure connection.
func NewSecureDBConnectionParams(
	username string, certsDir string, port int,
) *SecureDBConnectionParams {
	return &SecureDBConnectionParams{
		username: username,
		certsDir: certsDir,
		port:     port,
	}
}

// getLists returns the appropriate blocklist and ignorelist based on the
// cockroach version. This check only looks to ensure that the prefix that
// matches.
func (b blocklistsForVersion) getLists(version string) (string, blocklist, string, blocklist) {
	for _, info := range b {
		if strings.HasPrefix(version, info.versionPrefix) {
			return info.blocklistname, info.blocklist, info.ignorelistname, info.ignorelist
		}
	}
	return "", nil, "", nil
}

func fetchCockroachVersion(
	ctx context.Context,
	c cluster.Cluster,
	nodeIndex int,
	dbConnectionParams *SecureDBConnectionParams,
) (string, error) {
	var db *gosql.DB
	var err error
	if dbConnectionParams != nil {
		db, err = c.ConnSecure(
			ctx, nodeIndex, dbConnectionParams.username,
			dbConnectionParams.certsDir, dbConnectionParams.port,
		)
		if err != nil {
			return "", err
		}
	} else {
		db, err = c.ConnE(ctx, nodeIndex)
		if err != nil {
			return "", err
		}
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
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	node option.NodeListOption,
	operation string,
	args ...string,
) error {
	var lastError error
	for attempt, r := 0, retry.StartWithCtx(ctx, canaryRetryOptions); r.Next(); {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if t.Failed() {
			return fmt.Errorf("test has failed")
		}
		attempt++
		t.L().Printf("attempt %d - %s", attempt, operation)
		lastError = c.RunE(ctx, node, args...)
		if lastError != nil {
			t.L().Printf("error - retrying: %s", lastError)
			continue
		}
		return nil
	}
	return fmt.Errorf("all attempts failed for %s due to error: %s", operation, lastError)
}

// repeatRunWithBuffer is the same function as c.RunWithBuffer but with an
// automatic retry loop.
func repeatRunWithBuffer(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	node option.NodeListOption,
	operation string,
	args ...string,
) ([]byte, error) {
	var (
		lastResult []byte
		lastError  error
	)
	for attempt, r := 0, retry.StartWithCtx(ctx, canaryRetryOptions); r.Next(); {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if t.Failed() {
			return nil, fmt.Errorf("test has failed")
		}
		attempt++
		t.L().Printf("attempt %d - %s", attempt, operation)
		lastResult, lastError = c.RunWithBuffer(ctx, t.L(), node, args...)
		if lastError != nil {
			t.L().Printf("error - retrying: %s\n%s", lastError, string(lastResult))
			continue
		}
		return lastResult, nil
	}
	return nil, fmt.Errorf("all attempts failed for %s, with error: %s\n%s", operation, lastError, lastResult)
}

// repeatGitCloneE is the same function as c.GitCloneE but with an automatic
// retry loop.
func repeatGitCloneE(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	src, dest, branch string,
	node option.NodeListOption,
) error {
	var lastError error
	for attempt, r := 0, retry.StartWithCtx(ctx, canaryRetryOptions); r.Next(); {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if t.Failed() {
			return fmt.Errorf("test has failed")
		}
		attempt++
		t.L().Printf("attempt %d - clone %s", attempt, src)
		lastError = c.GitClone(ctx, t.L(), src, dest, branch, node)
		if lastError != nil {
			t.L().Printf("error - retrying: %s", lastError)
			continue
		}
		return nil
	}
	return fmt.Errorf("could not clone %s due to error: %s", src, lastError)
}

// repeatGetLatestTag fetches the latest (sorted) tag from a github repo.
// There is no equivalent function on the cluster as this is really only needed
// for the canary tests.
// The regex passed in must contain at least a single group named "major" and
// may contain "minor", "point" and "subpoint" in order of decreasing importance
// for sorting purposes.
func repeatGetLatestTag(
	ctx context.Context, t test.Test, user string, repo string, releaseRegex *regexp.Regexp,
) (string, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/tags", user, repo)
	httpClient := &http.Client{Timeout: 10 * time.Second}
	type Tag struct {
		Name string
	}
	type releaseTag struct {
		tag      string
		major    int
		minor    int
		point    int
		subpoint int
	}
	type Tags []Tag
	atoiOrZero := func(groups map[string]string, name string) int {
		value, ok := groups[name]
		if !ok {
			return 0
		}
		i, err := strconv.Atoi(value)
		if err != nil {
			return 0
		}
		return i
	}
	var lastError error
	for attempt, r := 0, retry.StartWithCtx(ctx, canaryRetryOptions); r.Next(); {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if t.Failed() {
			return "", fmt.Errorf("test has failed")
		}
		attempt++

		t.L().Printf("attempt %d - fetching %s", attempt, url)
		var resp *http.Response
		resp, lastError = httpClient.Get(url)
		if lastError != nil {
			t.L().Printf("error fetching - retrying: %s", lastError)
			continue
		}
		defer resp.Body.Close()

		var tags Tags
		lastError = json.NewDecoder(resp.Body).Decode(&tags)
		if lastError != nil {
			t.L().Printf("error decoding - retrying: %s", lastError)
			continue
		}
		if len(tags) == 0 {
			return "", fmt.Errorf("no tags found at %s", url)
		}
		var releaseTags []releaseTag
		for _, t := range tags {
			match := releaseRegex.FindStringSubmatch(t.Name)
			if match == nil {
				continue
			}
			groups := map[string]string{}
			for i, name := range match {
				groups[releaseRegex.SubexpNames()[i]] = name
			}
			if _, ok := groups["major"]; !ok {
				continue
			}
			releaseTags = append(releaseTags, releaseTag{
				tag:      t.Name,
				major:    atoiOrZero(groups, "major"),
				minor:    atoiOrZero(groups, "minor"),
				point:    atoiOrZero(groups, "point"),
				subpoint: atoiOrZero(groups, "subpoint"),
			})
		}
		if len(releaseTags) == 0 {
			return "", fmt.Errorf("no tags match the given regex")
		}
		sort.SliceStable(releaseTags, func(i, j int) bool {
			return releaseTags[i].major < releaseTags[j].major ||
				releaseTags[i].minor < releaseTags[j].minor ||
				releaseTags[i].point < releaseTags[j].point ||
				releaseTags[i].subpoint < releaseTags[j].subpoint
		})

		return releaseTags[len(releaseTags)-1].tag, nil
	}
	return "", fmt.Errorf("could not get tags from %s, due to error: %s", url, lastError)
}
