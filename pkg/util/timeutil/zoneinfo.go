// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
