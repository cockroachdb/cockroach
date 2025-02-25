// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// This file contains common elements for all 3rd party test suite roachtests.
// TODO(bram): There are more common elements between all the canary tests,
// factor more of them into here.

// blocklist is a lists of known test errors and failures.
type blocklist map[string]string

// listWithName contains both a blocklist of known test errors and
// failures and also an optional ignorelist for flaky tests.
// When the test suite is run, the results are compared to this blocklist.
// Any passed test that is not on this blocklist is reported as PASS - expected
// Any passed test that is on this blocklist is reported as PASS - unexpected
// Any failed test that is on this blocklist is reported as FAIL - expected
// Any failed test that is not on blocklist blocklist is reported as FAIL - unexpected
// Any test on this blocklist that is not run is reported as FAIL - not run
// Any test in the ignorelist is reported as SKIP if it is run
type listWithName struct {
	blocklistName  string
	blocklist      blocklist
	ignorelistName string
	ignorelist     blocklist
}

func fetchCockroachVersion(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, nodeIndex int,
) (string, error) {
	db, err := c.ConnE(ctx, l, nodeIndex)
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
		lastError = c.RunE(ctx, option.WithNodes(node), args...)
		if lastError != nil {
			t.L().Printf("error - retrying: %s", lastError)
			continue
		}
		return nil
	}
	return errors.Wrapf(lastError, "all attempts failed for %s", operation)
}

// repeatRunWithDetailsSingleNode is the same function as c.RunWithDetailsSingleNode but with an
// automatic retry loop.
func repeatRunWithDetailsSingleNode(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	node option.NodeListOption,
	operation string,
	args ...string,
) (install.RunResultDetails, error) {
	var (
		lastResult install.RunResultDetails
		lastError  error
	)
	for attempt, r := 0, retry.StartWithCtx(ctx, canaryRetryOptions); r.Next(); {
		if ctx.Err() != nil {
			return lastResult, ctx.Err()
		}
		if t.Failed() {
			return lastResult, fmt.Errorf("test has failed")
		}
		attempt++
		t.L().Printf("attempt %d - %s", attempt, operation)
		lastResult, lastError = c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(node), args...)
		if lastError != nil {
			t.L().Printf("error - retrying: %s", lastError)
			continue
		}
		return lastResult, nil
	}
	return lastResult, errors.Wrapf(lastError, "all attempts failed for %s", operation)
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
	return errors.Wrapf(lastError, "could not clone %s", src)
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
		//nolint:deferloop TODO(#137605)
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
		for _, tag := range tags {
			match := releaseRegex.FindStringSubmatch(tag.Name)
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
				tag:      tag.Name,
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
	return "", errors.Wrapf(lastError, "could not get tags from %s", url)
}

// gitCloneWithRecurseSubmodules clones a git repo from src into dest and checks out origin's
// version of the given branch, but with a --recurse-submodules flag.
// The src, dest, and branch arguments must not contain shell special characters.
func gitCloneWithRecurseSubmodules(
	ctx context.Context,
	c cluster.Cluster,
	l *logger.Logger,
	src, dest, branch string,
	node option.NodeListOption,
) error {
	cmd := []string{"bash", "-e", "-c", fmt.Sprintf(`'
		if ! test -d %[1]s; then
	  		git clone --recurse-submodules -b %[2]s --depth 1 %[3]s %[1]s
		else
	  		cd %[1]s
	  		git fetch origin
	  		git checkout origin/%[2]s
		fi
	'`, dest, branch, src),
	}
	return errors.Wrap(c.RunE(ctx, option.WithNodes(node), cmd...), "gitCloneWithRecurseSubmodules")
}
