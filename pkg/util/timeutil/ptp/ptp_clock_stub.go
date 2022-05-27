// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !linux
// +build !linux

package ptp

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
)

// PTPClock reads the time from a ptp device. Only implemented on Linux.
type PTPClock struct{}

// MakePTPClock us not used on platforms other than Linux
func MakePTPClock(_ context.Context, _ string) (PTPClock, error) {
	return PTPClock{}, errors.New("clock device not supported on this platform")
}

// Now implements the hlc.WallClock interface.
func (p PTPClock) Now() time.Time {
	panic(errors.New("clock device not supported on this platform"))
}
