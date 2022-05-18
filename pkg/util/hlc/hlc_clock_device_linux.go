// Copyright 2017 The Cockroach Authors.
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

package hlc

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

// PTPClock contains the handle of the clock device as well as the
// clock id.
// The PTPClock implements the hlc.WallClock interface, so it can be used
// as the time source for an hlc.Clock:
// hlc.NewClock(MakePTPClock(...), ...).
type PTPClock struct {
	// clockDevice is not used after the device is open but is here to prevent the GC
	// from closing the device and invalidating the clockDeviceID.
	clockDevice   *os.File
	clockDeviceID uintptr
}

var _ WallClock = PTPClock{}

// MakePTPClock creates a new PTPClock for the given device path.
func MakePTPClock(ctx context.Context, clockDevicePath string) (PTPClock, error) {
	var result PTPClock
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
func (p PTPClock) Now() time.Time {
	var ts C.struct_timespec
	_, err := C.clock_gettime(C.clockid_t(p.clockDeviceID), &ts)
	if err != nil {
		panic(err)
	}

	return timeutil.Unix(int64(ts.tv_sec)*1e9, int64(ts.tv_nsec))
}
