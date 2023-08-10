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
	"context"
	"os"
	"testing"
	"time"

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

func TestInitClock(t *testing.T) {
	var file *os.File
	var err error
	var devOpenerCallCount int
	clock := &Clock{
		devOpener: func(name string) (*os.File, error) {
			require.Equal(t, "/dev/ptp-test", name)
			file, err = os.CreateTemp("", "")
			require.NoError(t, err)
			require.NotNil(t, file)
			devOpenerCallCount++
			return file, err
		},
		cGetTime: func(fd fdType, ts *structTimespec) error {
			require.Equal(t, 1, devOpenerCallCount)
			require.EqualValues(t, ^file.Fd()<<3|3, fd)
			*ts = structTimespec{tv_sec: 3, tv_nsec: 5}
			return nil
		},
	}
	ctx := context.Background()
	_, err = clock.initClock(ctx, "/dev/ptp-test")
	require.NoError(t, err)
	require.EqualValues(t, 3*1e9+5, clock.Now().UnixNano())
}
