// Copyright 2016 The Cockroach Authors.
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
	case "utc":
		// TODO(knz): See #36864. This code is a crutch, and should be
		// removed in favor of a cache of available locations with
		// case-insensitive lookup.
		name = "UTC"
	}
	l, err := time.LoadLocation(name)
	if err != nil && strings.Contains(err.Error(), "zoneinfo.zip") {
		err = errTZDataNotFound
	}
	return l, err
}
