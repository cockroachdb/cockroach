// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timeutil

import (
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sys/windows"
)

func init() {
	if err := windows.LoadGetSystemTimePreciseAsFileTime(); err != nil {
		panic(errors.Wrap(err, "CockroachDB requires Windows 8 or higher"))
	}
}

// Now returns the current UTC time.
//
// This has a higher precision than time.Now in go1.8, but is much slower
// (~2000x) and requires Windows 8+.
//
// TODO(tamird): consider removing this in go1.9. go1.9 is expected to add
// monotonic clock support to values retured from time.Now, which this
// implementation will not support. The monotonic clock support may also
// obviate the need for this, since we only need the higher precision when
// subtracting `time.Time`s.
func Now() time.Time {
	var ft windows.Filetime
	windows.GetSystemTimePreciseAsFileTime(&ft)
	return time.Unix(0, ft.Nanoseconds()).UTC()
}
