// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fail_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/fail"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestFailPoints(t *testing.T) {
	if fail.Enabled {
		testFailPointsEnabled(t)
	} else {
		testFailPointsDisabled(t)
	}
}

func testFailPointsEnabled(t *testing.T) {
	// Activate a fail point.
	exp := errors.New("err")
	defer fail.Activate(fail.EventRaftApplyEntry, func(args ...interface{}) error {
		require.Len(t, args, 2)
		require.Equal(t, 1, args[0])
		require.Equal(t, 2, args[1])
		return exp
	})()

	// Duplicate activation.
	require.Panics(t, func() { fail.Activate(fail.EventRaftApplyEntry, nil) })

	// Trigger activated fail point.
	err := fail.Point(fail.EventRaftApplyEntry, 1, 2)
	require.Equal(t, exp, err)

	// Trigger unactivated fail point.
	err = fail.Point(fail.EventRaftSnapshotBeforeIngestSST)
	require.Nil(t, err)

	// Active with the default callback and trigger again.
	de := fail.Activate(fail.EventRaftSnapshotBeforeIngestSST, nil)
	err = fail.Point(fail.EventRaftSnapshotBeforeIngestSST)
	require.NotNil(t, err)
	require.IsType(t, &fail.Error{}, err)
	require.Equal(t, fail.EventRaftSnapshotBeforeIngestSST, err.(*fail.Error).Event())

	// Deactivate and trigger again.
	de()
	err = fail.Point(fail.EventRaftSnapshotBeforeIngestSST)
	require.Nil(t, err)
}

func testFailPointsDisabled(t *testing.T) {
	require.Panics(t, func() { fail.Activate(fail.EventRaftApplyEntry, nil) })

	res := fail.Point(fail.EventRaftApplyEntry)
	require.Nil(t, res)
}
