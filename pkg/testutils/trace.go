// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.

package testutils

import (
	"regexp"

	"github.com/pkg/errors"
)

// MatchInOrder matches interprets the given slice of strings as a slice of
// regular expressions and checks that they match, in order and without overlap,
// against the given string. For example, if s=abcdefg and res=ab,cd,fg no error
// is returned, whereas res=abc,cde would return a descriptive error about
// failing to match cde.
func MatchInOrder(s string, res ...string) error {
	sPos := 0
	for i := range res {
		reStr := "(?ms)" + res[i]
		re, err := regexp.Compile(reStr)
		if err != nil {
			return errors.Errorf("regexp %d (%q) does not compile: %s", i, reStr, err)
		}
		loc := re.FindStringIndex(s[sPos:])
		if loc == nil {
			// Not found.
			return errors.Errorf(
				"unable to find regexp %d (%q) in remaining string:\n\n%s\n\nafter having matched:\n\n%s",
				i, reStr, s[sPos:], s[:sPos],
			)
		}
		sPos += loc[1]
	}
	return nil
}
