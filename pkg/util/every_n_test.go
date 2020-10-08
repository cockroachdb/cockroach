// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestEveryN(t *testing.T) {
	start := timeutil.Now()
	en := EveryN{N: time.Minute}
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
	for _, tc := range testCases {
		if a, e := en.ShouldProcess(start.Add(tc.t)), tc.expected; a != e {
			t.Errorf("ShouldProcess(%v) got %v, want %v", tc.t, a, e)
		}
	}
}
