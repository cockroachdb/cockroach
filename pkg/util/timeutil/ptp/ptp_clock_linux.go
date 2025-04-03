// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package ptp

/*
#include <time.h>
*/
import "C"

import (
	"context"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Clock contains the handle of the clock device as well as the
// clock id.
// The Clock implements the hlc.WallClock interface, so it can be used
// as the time source for an hlc.Clock:
// hlc.NewClock(MakeClock(...), ...).
type Clock struct {
	// clockDevice is not used after the device is open but is here to prevent the GC
	// from closing the device and invalidating the clockDeviceID.
	clockDevice   *os.File
	clockDeviceID uintptr
}

// MakeClock creates a new Clock for the given device path.
func MakeClock(ctx context.Context, clockDevicePath string) (Clock, error) {
	var result Clock
	var err error
	result.clockDevice, err = os.Open(clockDevicePath)
	if err != nil {
		return result, errors.Wrapf(err, "cannot open %s", clockDevicePath)
	}

	clockDeviceFD := result.clockDevice.Fd()
	// For clarification of how the clock id is computed:
	// https://lore.kernel.org/patchwork/patch/868609/
	// https://github.com/torvalds/linux/blob/7e63420847ae5f1036e4f7c42f0b3282e73efbc2/tools/testing/selftests/ptp/testptp.c#L87
	clockID := (^clockDeviceFD << 3) | 3
	log.Infof(
		ctx,
		"opened clock device %s with fd %d, mod_fd %x",
		clockDevicePath,
		clockDeviceFD,
		clockID,
	)
	var ts C.struct_timespec
	_, err = C.clock_gettime(C.clockid_t(clockID), &ts)
	if err != nil {
		return result, errors.Wrap(err, "UseClockDevice: error calling clock_gettime")
	}
	result.clockDeviceID = clockID

	return result, nil
}

// Now implements the hlc.WallClock interface.
func (p Clock) Now() time.Time {
	var ts C.struct_timespec
	_, err := C.clock_gettime(C.clockid_t(p.clockDeviceID), &ts)
	if err != nil {
		panic(err)
	}

	return timeutil.Unix(int64(ts.tv_sec), int64(ts.tv_nsec))
}

// realtime returns a clock using the system CLOCK_REALTIME device. For testing.
func realtime() Clock {
	return Clock{clockDeviceID: uintptr(C.CLOCK_REALTIME)}
}
