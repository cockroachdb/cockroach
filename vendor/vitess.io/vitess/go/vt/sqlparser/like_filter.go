/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	re = regexp.MustCompile(`([^\\]?|[\\]{2})[%_]`)
)

func replacer(s string) string {
	if strings.HasPrefix(s, `\\`) {
		return s[2:]
	}

	result := strings.Replace(s, "%", ".*", -1)
	result = strings.Replace(result, "_", ".", -1)

	return result
}

// LikeToRegexp converts a like sql expression to regular expression
func LikeToRegexp(likeExpr string) *regexp.Regexp {
	if likeExpr == "" {
		return regexp.MustCompile("^.*$") // Can never fail
	}

	keyPattern := regexp.QuoteMeta(likeExpr)
	keyPattern = re.ReplaceAllStringFunc(keyPattern, replacer)
	keyPattern = fmt.Sprintf("^%s$", keyPattern)
	return regexp.MustCompile(keyPattern) // Can never fail
}
