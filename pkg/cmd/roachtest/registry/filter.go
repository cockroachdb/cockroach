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
	Tag  *regexp.Regexp
	// RawTag is the string representation of the regexps in tag.
	RawTag []string
}

// NewTestFilter initializes a new filter. The strings are interpreted
// as regular expressions. As a special case, a `tag:` prefix implies
// that the remainder of the string filters tests by tag, and not by
// name.
func NewTestFilter(filter []string) *TestFilter {
	var name []string
	var tag []string
	var rawTag []string
	for _, v := range filter {
		if strings.HasPrefix(v, "tag:") {
			tag = append(tag, strings.TrimPrefix(v, "tag:"))
			rawTag = append(rawTag, v)
		} else {
			name = append(name, v)
		}
	}

	if len(tag) == 0 {
		tag = []string{DefaultTag}
		rawTag = []string{"tag:" + DefaultTag}
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
		Name:   makeRE(name),
		Tag:    makeRE(tag),
		RawTag: rawTag,
	}
}
