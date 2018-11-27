// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
