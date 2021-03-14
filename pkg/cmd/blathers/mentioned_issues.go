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
	"regexp"
	"strconv"
)

// mentionedIssues is an issue that has been mentioned from a
// body of text.
type mentionedIssue struct {
	owner  string
	repo   string
	number int
}

var (
	removeNestedQuotesRegex = regexp.MustCompile("```[^`]*```")
	// numberRegex captures #1234 and cockroachdb/cockroach#1234
	numberRegex = regexp.MustCompile(`(([a-zA-Z_-]+)/([a-zA-Z_-]+))?#(\d+)`)
	// fullIssueURLRegex captures URLs directing to issues.
	fullIssueURLRegex = regexp.MustCompile(`github.com/([a-zA-Z_-]+)/([a-zA-Z-_]+)/(pull|issues)/(\d+)`)
)

// findMentionedIssues returns issues that have been mentioned in
// a body of text that belongs to the given owner.
func findMentionedIssues(owner string, defaultRepo string, text string) []mentionedIssue {
	text = removeNestedQuotesRegex.ReplaceAllString(text, "")
	mentionedIssues := make(map[mentionedIssue]struct{})
	for _, group := range numberRegex.FindAllStringSubmatch(text, -1) {
		o := owner
		if group[2] != "" {
			o = group[2]
			if o != owner {
				continue
			}
		}
		r := defaultRepo
		if group[3] != "" {
			r = group[3]
		}
		number, err := strconv.ParseInt(group[4], 10, 64)
		if err != nil {
			continue
		}
		// Ignore `uname -a` output greedily.
		if number < 100 {
			continue
		}
		mentionedIssues[mentionedIssue{owner: o, repo: r, number: int(number)}] = struct{}{}
	}

	for _, group := range fullIssueURLRegex.FindAllStringSubmatch(text, -1) {
		if group[1] != owner {
			continue
		}
		number, err := strconv.ParseInt(group[4], 10, 64)
		if err != nil {
			continue
		}
		mentionedIssues[mentionedIssue{owner: owner, repo: group[2], number: int(number)}] = struct{}{}
	}

	ret := make([]mentionedIssue, 0, len(mentionedIssues))
	for mi := range mentionedIssues {
		ret = append(ret, mi)
	}
	return ret
}
