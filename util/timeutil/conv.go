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

	"github.com/jeffjen/datefmt"
	"github.com/leekchan/timeutil"
)

// Strftime converts a time to a string using some C-style format.
func Strftime(time time.Time, fmt string) (string, error) {
	return timeutil.Strftime(&time, fmt), nil
}

// Strptime converts a string to a time using some C-style format.
func Strptime(time, fmt string) (time.Time, error) {
	// TODO(knz) The `datefmt` package uses C's `strptime` which doesn't
	// know about microseconds. We may want to change to an
	// implementation that does this better.
	return datefmt.Strptime(fmt, time)
}
