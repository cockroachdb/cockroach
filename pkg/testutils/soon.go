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

package testutils

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
)

// DefaultSucceedsSoonDuration is the maximum amount of time unittests
// will wait for a condition to become true. See SucceedsSoon().
const DefaultSucceedsSoonDuration = 45 * time.Second

// SucceedsSoon fails the test (with t.Fatal) unless the supplied
// function runs without error within a preset maximum duration. The
// function is invoked immediately at first and then successively with
// an exponential backoff starting at 1ns and ending at the maximum
// duration (currently 15s).
func SucceedsSoon(t testing.TB, fn func() error) {
	SucceedsSoonDepth(1, t, fn)
}

// SucceedsSoonDepth is like SucceedsSoon() but with an additional
// stack depth offset.
func SucceedsSoonDepth(depth int, t testing.TB, fn func() error) {
	if err := util.RetryForDuration(DefaultSucceedsSoonDuration, fn); err != nil {
		file, line, _ := caller.Lookup(depth + 1)
		t.Fatalf("%s:%d, condition failed to evaluate within %s: %s", file, line, DefaultSucceedsSoonDuration, err)
	}
}
