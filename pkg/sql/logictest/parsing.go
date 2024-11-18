// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logictest

import (
	"fmt"
	"strconv"
	"strings"
)

// extractGithubIssue parses directive fields of the form:
//
//	[#ISSUE] [args...]
//
// If the first field is a number prefixed with #, it will be interpreted as
// a github issue and the rest of the fiels are returned. Otherwise, returns -1
// and the given fields.
func extractGithubIssue(fields []string) (githubIssueID int, args []string) {
	if len(fields) < 1 {
		return -1, nil
	}
	if numStr, ok := strings.CutPrefix(fields[0], "#"); ok {
		if num, err := strconv.ParseUint(numStr, 10, 32); err == nil {
			return int(num), fields[1:]
		}
	}
	return -1, fields
}

func githubIssueStr(githubIssueID int) string {
	if githubIssueID <= 0 {
		return "no issue given"
	}
	return fmt.Sprintf("#%d", githubIssueID)
}
