// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build linux
// +build linux

package ptp

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestClockNow sanity checks that Clock.Now() sourced from the CLOCK_REALTIME
// device returns time close to timeutil.Now(). This ensures that the conversion
// from the time returned by a clock device to Go's time.Time is correct.
func TestClockNow(t *testing.T) {
	if got, want := realtime().Now(), timeutil.Now(); want.Sub(got).Abs() > 10*time.Second {
		t.Errorf("clock mismatch: got %v; timeutil says %v", got, want)
	}
}

func TestClockNowWithMockCGetTime(t *testing.T) {
	defer testutils.TestingHook(&cGetTime, func(_ fdType, t *structTimespec) error {
		*t = structTimespec{tv_sec: 3, tv_nsec: 5}
		return nil
	})()
	require.EqualValues(t, 3*1e9+5, realtime().Now().UnixNano())
}
