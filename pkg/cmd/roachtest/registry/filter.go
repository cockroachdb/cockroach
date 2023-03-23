// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package registry

import (
	"regexp"
	"strings"
)

// TestFilter holds the name and tag filters for filtering tests.
// See NewTestFilter.
type TestFilter struct {
	Name *regexp.Regexp
	// Multiple `tag:` parameters can be passed for which only one needs to match, but the
	// value of a single `tag:` parameter can be a comma-separated list of tags which all need
	// to match.
	// e.g. `tag:foo,bar` matches tests with tags `foo` and `bar`, and `tag:foo tag:bar` matches
	// tests with either tag `foo` or tag `bar`.
	//
	// This set contains each tag, so the above examples would be represented as `["foo,bar"]` and
	// `["foo", "bar"]` respectively..
	Tags       map[string]struct{}
	RunSkipped bool
}

// NewTestFilter initializes a new filter. The strings are interpreted
// as regular expressions. As a special case, a `tag:` prefix implies
// that the remainder of the string filters tests by tag, and not by
// name.
func NewTestFilter(filter []string, runSkipped bool) *TestFilter {
	var name []string
	tags := make(map[string]struct{})
	for _, v := range filter {
		if strings.HasPrefix(v, "tag:") {
			tags[strings.TrimPrefix(v, "tag:")] = struct{}{}
		} else {
			name = append(name, v)
		}
	}

	makeRE := func(strs []string) *regexp.Regexp {
		switch len(strs) {
		case 0:
			return regexp.MustCompile(`.`)
		case 1:
			return regexp.MustCompile(strs[0])
		default:
			for i := range strs {
				strs[i] = "(" + strs[i] + ")"
			}
			return regexp.MustCompile(strings.Join(strs, "|"))
		}
	}

	return &TestFilter{
		Name:       makeRE(name),
		Tags:       tags,
		RunSkipped: runSkipped,
	}
}
