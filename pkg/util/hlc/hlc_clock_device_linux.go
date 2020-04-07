// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build linux

package hlc

/*
#include <time.h>
*/
import "C"

import (
	"context"
	"os"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// ClockSource contains the handle of the clock device as well as the
// clock id.
type ClockSource struct {
	// clockDevice is not used after the device is open but is here to prevent the GC
	// from closing the device and invalidating the clockDeviceID.
	clockDevice   *os.File
	clockDeviceID uintptr
}

// MakeClockSource creates a new ClockSource for the given device path.
func MakeClockSource(ctx context.Context, clockDevicePath string) (ClockSource, error) {
	var result ClockSource
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

// UnixNano returns the clock device's physical nanosecond
// unix epoch timestamp as a convenience to create a HLC via
// c := hlc.NewClock(dev.UnixNano, ...).
func (p ClockSource) UnixNano() int64 {
	var ts C.struct_timespec
	_, err := C.clock_gettime(C.clockid_t(p.clockDeviceID), &ts)
	if err != nil {
		panic(err)
	}

	return int64(ts.tv_sec)*1e9 + int64(ts.tv_nsec)
}
