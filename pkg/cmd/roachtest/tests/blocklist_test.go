// Copyright 2019 The Cockroach Authors.
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
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/google/go-github/github"
	"golang.org/x/oauth2"
)

const githubAPITokenEnv = "GITHUB_API_TOKEN"
const runBlocklistEnv = "RUN_BLOCKLIST_TEST"

func TestBlocklists(t *testing.T) {
	if _, ok := os.LookupEnv(runBlocklistEnv); !ok {
		skip.IgnoreLintf(t, "Blocklist test is only run if %s is set", runBlocklistEnv)
	}

	blocklists := map[string]blocklist{
		"hibernate":    hibernateBlockList20_2,
		"pgjdbc":       pgjdbcBlockList20_2,
		"psycopg":      psycopgBlockList20_2,
		"django":       djangoBlocklist20_2,
		"sqlAlchemy":   sqlAlchemyBlocklist20_2,
		"libpq":        libPQBlocklist20_2,
		"gopg":         gopgBlockList20_2,
		"pgx":          pgxBlocklist20_2,
		"activerecord": activeRecordBlockList20_2,
	}
	type reasonCount struct {
		reason string
		count  int
		suites map[string]bool
	}

	var failureMap = make(map[string]*reasonCount, 200)
	for suite, bl := range blocklists {
		for _, reason := range bl {
			if _, ok := failureMap[reason]; !ok {
				failureMap[reason] = &reasonCount{
					reason: reason,
					suites: make(map[string]bool, 10),
				}
			}
			failureMap[reason].count++
			failureMap[reason].suites[suite] = true
		}
	}

	counts := make([]reasonCount, 0, len(failureMap))
	for _, count := range failureMap {
		counts = append(counts, *count)
	}
	sort.Slice(counts, func(i, j int) bool {
		return counts[i].count > counts[j].count
	})

	ctx := context.Background()
	// This test exceeds the rate limit for non-authed requests. To run
	// this test locally, set the environment variable GITHUB_API_TOKEN
	// to your personal access token.
	token, ok := os.LookupEnv(githubAPITokenEnv)
	if !ok {
		t.Fatalf("GitHub API token environment variable %s is not set", githubAPITokenEnv)
	}
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
	tc := oauth2.NewClient(ctx, ts)
	client := github.NewClient(tc)

	anyClosed := false
	for i := range counts {
		issueTitle := "unknown"
		reason := counts[i].reason
		var issueNum int
		var err error
		state := ""
		if issueNum, err = strconv.Atoi(counts[i].reason); err == nil {
			if issue, _, err := client.Issues.Get(ctx, "cockroachdb", "cockroach", issueNum); err == nil {
				issueTitle = strings.Replace(issue.GetTitle(), ",", " ", -1)
				state = issue.GetState()
				if state != "open" {
					anyClosed = true
				}
			}
			reason = fmt.Sprintf("https://github.com/cockroachdb/cockroach/issues/%d", issueNum)
		}
		suites := ""
		for suite := range counts[i].suites {
			suites += suite + " "
		}
		fmt.Printf("%4d,%6s,%-54s,%s,%s\n", counts[i].count, state, reason, issueTitle, suites)
	}

	if anyClosed {
		t.Fatal("Some closed issues appear in blocklists")
	}
}
