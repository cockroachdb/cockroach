// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"sort"
	"strings"
	"sync"
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

var tzsOnce sync.Once
var tzs []string

// TimeZones lists all supported timezones.
func TimeZones() []string {
	tzsOnce.Do(func() {
		tzs = make([]string, 0, len(lowercaseTimezones))
		for _, tz := range lowercaseTimezones {
			tzs = append(tzs, tz)
		}
		sort.Strings(tzs)
	})
	return tzs
}
