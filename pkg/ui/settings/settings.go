// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package uisettings

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	utc int64 = iota
	americaNewYork
)

// DefaultTimezone is the timezone used to format timestamps in the UI. Any valid
// timezone is supported. For the sake of backwards compatability, the default of
// this setting is empty, which ultimately allows for the frontend to choose the
// final default value if none is set in the cluster.
var DefaultTimezone = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"ui.default_timezone",
	"the default timezone used to format timestamps in the ui",
	"",
	settings.WithValidateString(func(sv *settings.Values, v string) error {
		_, err := timeutil.LoadLocation(v)
		if err != nil {
			return errors.Wrap(err, "invalid timezone")
		}
		return nil
	}),
	settings.WithPublic)

var DisplayTimezoneEnums = map[int64]string{
	utc:            "Etc/UTC",
	americaNewYork: "America/New_York",
}

// TODO (kyle.wong): This setting is deprecated and should be removed
// in a future version, probably v25.4.
var DisplayTimezone = settings.RegisterEnumSetting(
	settings.ApplicationLevel,
	"ui.display_timezone",
	"the timezone used to format timestamps in the ui. This setting is deprecated"+
		"and will be removed in a future version. Use the 'ui.default_timezone' setting "+
		"instead. 'ui.default_timezone' takes precedence over this setting.",
	"Etc/UTC",
	DisplayTimezoneEnums,
	settings.WithPublic)
