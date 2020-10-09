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
	"strings"
	"time"

	"4d63.com/tz"
)

// LoadLocation returns the time.Location with the given name.
// The name is taken to be a location name corresponding to a file
// in the IANA Time Zone database, such as "America/New_York".
//
// We do not use Go's time.LoadLocation() directly because:
// 1) It maps "Local" to the local time zone, whereas we want UTC.
// 2) It depends on the operating system's installation of zoneinfo.zip.
//    Instead, we use 4d63.com/tz's embedded time.
//    In Go1.15+, we can re-use time.LoadLocation if we include
//    https://tip.golang.org/doc/go1.15#time/tzdata.
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
	return tz.LoadLocation(name)
}
