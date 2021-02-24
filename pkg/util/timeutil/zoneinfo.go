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
	// embed tzdata in case system tzdata is not available.
	_ "time/tzdata"
)

//go:generate go run gen/main.go

// LoadLocation returns the time.Location with the given name.
// The name is taken to be a location name corresponding to a file
// in the IANA Time Zone database, such as "America/New_York".
//
// We do not use Go's time.LoadLocation() directly because it maps
// "Local" to the local time zone, whereas we want UTC.
func LoadLocation(name string) (*time.Location, error) {
	loweredName := strings.ToLower(name)
	switch loweredName {
	case "local", "default":
		loweredName = "utc"
		name = "UTC"
	}
	// If we know this is a lowercase name in tzdata, use the uppercase form.
	if v, ok := lowercaseTimezones[loweredName]; ok {
		// If this location is not found, we may have a case where the tzdata names
		// have different values than the system tz names.
		// If this is the case, allback onto the default logic, where the name is read
		// off other sources before tzdata.
		if loc, err := time.LoadLocation(v); err == nil {
			return loc, nil
		}
	}
	return time.LoadLocation(name)
}
