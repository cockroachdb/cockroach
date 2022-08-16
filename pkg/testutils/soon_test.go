// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSucceedsSoon(t *testing.T) {
	// Try a method which always succeeds.
	testutils.SucceedsSoon(t, func() error { return nil })

	// Try a method which succeeds after a known duration.
	start := timeutil.Now()
	duration := time.Millisecond * 10
	testutils.SucceedsSoon(t, func() error {
		elapsed := timeutil.Since(start)
		if elapsed > duration {
			return nil
		}
		return errors.Errorf("%s elapsed, waiting until %s elapses", elapsed, duration)
	})
}
func TestRequireSoon(t *testing.T) {
	// Try a method which always succeeds.
	testutils.RequireSoon(t, func(t testutils.TB) error { return nil })

	// Try a method which succeeds after a known duration.
	start := timeutil.Now()
	duration := time.Millisecond * 10
	testutils.RequireSoon(t, func(t testutils.TB) error {
		assert.Greater(t, timeutil.Since(start), duration)
		return nil
	})

	// Ensure that helpers that call FailNow halt execution immediately,
	// then retry, while helpers that just call Errorf don't halt execution.
	postAssertCount := 0
	postRequireCount := 0
	testutils.RequireSoon(t, func(t testutils.TB) error {
		assert.Greater(t, postAssertCount, 1)
		postAssertCount++
		require.Equal(t, postAssertCount, 3)
		postRequireCount++
		return nil
	})

	require.Equal(t, 3, postAssertCount)
	require.Equal(t, 1, postRequireCount)

}
