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

package timeutil

import (
	"errors"
	"strings"
	"time"
)

var errTZDataNotFound = errors.New("timezone data cannot be found")

// LoadLocation returns the time.Location with the given name.
// The name is taken to be a location name corresponding to a file
// in the IANA Time Zone database, such as "America/New_York".
//
// We do not use Go's time.LoadLocation() directly because:
// 1) it maps "Local" to the local time zone, whereas we want UTC.
// 2) when a tz is not found, it reports some garbage message
// related to zoneinfo.zip, which we don't ship, instead
// of a more useful message like "the tz file with such name
// is not present in one of the standard tz locations".
func LoadLocation(name string) (*time.Location, error) {
	switch strings.ToLower(name) {
	case "local", "default":
		name = "UTC"
	}
	l, err := time.LoadLocation(name)
	if err != nil && strings.Contains(err.Error(), "zoneinfo.zip") {
		err = errTZDataNotFound
	}
	return l, err
}
