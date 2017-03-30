// Copyright 2016 The Cockroach Authors.
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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package timeutil

import (
	"time"
	"unsafe"

	"github.com/leekchan/timeutil"
	"github.com/pkg/errors"
)

// This is a modified version of github.com/jeffjen/datefmt inlined here until
// https://github.com/jeffjen/datefmt/pull/3 is merged.

// #include <stdlib.h>
// #define __USE_XOPEN
// #include <time.h>
import "C"

// Strftime converts a time to a string using some C-style format.
func Strftime(t time.Time, layout string) (string, error) {
	return timeutil.Strftime(&t, layout), nil
}

// Strptime converts a string to a time using some C-style format.
func Strptime(layout, value string) (time.Time, error) {
	// TODO(knz) this uses C's `strptime` which doesn't
	// know about microseconds. We may want to change to an
	// implementation that does this better.
	cLayout := C.CString(layout)
	defer C.free(unsafe.Pointer(cLayout))
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))

	var cTime C.struct_tm
	if _, err := C.strptime(cValue, cLayout, &cTime); err != nil {
		return time.Time{}, errors.Wrapf(err, "could not parse %s as %s", value, layout)
	}
	return time.Date(
		int(cTime.tm_year)+1900,
		time.Month(cTime.tm_mon+1),
		int(cTime.tm_mday),
		int(cTime.tm_hour),
		int(cTime.tm_min),
		int(cTime.tm_sec),
		0,
		time.FixedZone(C.GoString(cTime.tm_zone), int(cTime.tm_gmtoff)),
	), nil
}
