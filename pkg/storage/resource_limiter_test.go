// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestResourceLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := timeutil.NewManualTime(timeutil.Now())
	l := NewResourceLimiter(ResourceLimiterOptions{MaxRunTime: time.Minute}, clock)

	require.Equal(t, ResourceLimitNotReached, l.IsExhausted(), "Exhausted when time didn't move")
	clock.Advance(time.Second * 59)
	require.Equal(t, ResourceLimitReachedSoft, l.IsExhausted(), "Soft limit not reached")
	clock.Advance(time.Minute)
	require.Equal(t, ResourceLimitReachedHard, l.IsExhausted(), "Hard limit not reached")
}
