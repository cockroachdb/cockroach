// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !linux

package hlc

import (
	"os"

	"github.com/pkg/errors"
)

// ClockDevice contains the handle of the clock device as well as the
// clock id.
type ClockDevice struct {
	clockDevice   *os.File
	clockDeviceID uintptr
}

// UnixNano is not use on OSes other than Linux
func (p ClockDevice) UnixNano() int64 {
	panic(errors.New("clock device not supported on this OS"))
}

// MakeClockDevice us not used on OSes other than Linux
func MakeClockDevice(_ string) (ClockDevice, error) {
	return ClockDevice{}, errors.New("clock device not supported on this OS")
}
