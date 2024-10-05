// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
