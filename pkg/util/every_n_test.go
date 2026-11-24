// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/stretchr/testify/require"
)

func TestEveryN(t *testing.T) {
	testCases := []struct {
		t        time.Duration // time since start
		expected bool
	}{
		{0, true}, // the first attempt to log should always succeed
		{0, false},
		{time.Second, false},
		{time.Minute - 1, false},
		{time.Minute, true},
		{time.Minute, false},
		{time.Minute + 30*time.Second, false},
		{10 * time.Minute, true},
		{10 * time.Minute, false},
		{10*time.Minute + 59*time.Second, false},
		{11 * time.Minute, true},
	}
	t.Run("time.Time", func(t *testing.T) {
		start := timeutil.Now()
		en := Every(time.Minute)
		for _, tc := range testCases {
			require.Equal(t, tc.expected, en.ShouldProcess(start.Add(tc.t)))
		}
	})
	t.Run("crtime.NowMono", func(t *testing.T) {
		start := crtime.NowMono()
		en := EveryMono(time.Minute)
		for _, tc := range testCases {
			require.Equal(t, tc.expected, en.ShouldProcess(start.Add(tc.t)))
		}
	})
}
