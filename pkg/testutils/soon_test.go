// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
