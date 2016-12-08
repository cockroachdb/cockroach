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

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

func TestSucceedsSoon(t *testing.T) {
	// Try a method which always succeeds.
	SucceedsSoon(t, func() error { return nil })

	// Try a method which succeeds after a known duration.
	start := timeutil.Now()
	duration := time.Millisecond * 10
	SucceedsSoon(t, func() error {
		elapsed := timeutil.Since(start)
		if elapsed > duration {
			return nil
		}
		return errors.Errorf("%s elapsed, waiting until %s elapses", elapsed, duration)
	})
}
