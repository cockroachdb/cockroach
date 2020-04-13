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
	"context"

	"github.com/pkg/errors"
)

// ClockSource contains the handle of the clock device as well as the
// clock id.
type ClockSource struct {
}

// UnixNano is not used on platforms other than Linux
func (p ClockSource) UnixNano() int64 {
	panic(errors.New("clock device not supported on this platform"))
}

// MakeClockSource us not used on platforms other than Linux
func MakeClockSource(_ context.Context, _ string) (ClockSource, error) {
	return ClockSource{}, errors.New("clock device not supported on this platform")
}
