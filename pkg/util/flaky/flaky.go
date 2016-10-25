// Copyright 2016 The Cockroach Authors.
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

package flaky

import (
	"strconv"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Skipper is a narrow subset of testing.TB required for Register().
type Skipper interface {
	Skip(...interface{})
}

const envVarName = "COCKROACH_SKIP_FLAKY_TESTS"

var skipFlakyTests = envutil.EnvOrDefaultBool(envVarName, false)

// Register should be called from tests which are deemed flaky, causing them to
// be skipped whenever COCKROACH_SKIP_FLAKY_TESTS is set to a truthy value. The
// parameters are typically the *testing.T, a cockroachdb/cockroach issue
// number, and optionally comments on the test's flakyness. A zero issue number
// can be passed when no issue is available.
func Register(t Skipper, issueNumber int, comments ...string) {
	var reason string
	if len(comments) > 0 {
		reason = strings.Join(comments, "\n") + "; "
	}
	var invoke func(...interface{})
	if skipFlakyTests {
		invoke = t.Skip
	} else {
		ctx := context.Background()
		invoke = func(args ...interface{}) {
			log.Warning(ctx, args...)
		}
	}
	issue := "no issue filed"
	if issueNumber > 0 {
		issue = "https://github.com/cockroachdb/cockroach/issues/" + strconv.Itoa(issueNumber)
	}
	var addendum string
	if !skipFlakyTests {
		addendum = " (consider " + envVarName + "=true)"
	}
	invoke("test is flaky: " + reason + issue + addendum)
}
