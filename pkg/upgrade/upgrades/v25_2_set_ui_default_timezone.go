// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// setUiDefaultTimezone sets the value of the 'ui.default_timezone' cluster
// setting to be the same as the 'ui.display_timezone' setting if it is set.
// 'ui.default_timezone' is the new cluster setting to replace
// 'ui.display_timezone' which doesn't have any restrictions on which timezones
// can be set.
func setUiDefaultTimezone(
	ctx context.Context, version clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Get the value of the 'ui.display_timezone' setting, if it has been
	// overridden.
	row, err := d.DB.Executor().QueryRow(
		ctx,
		"get-display-timezone",
		nil,
		"SELECT value FROM system.settings WHERE name = 'ui.display_timezone'")

	if err != nil {
		return err
	}

	if len(row) == 0 || row[0] == nil {
		return nil
	}

	// value is a string representation of the enum value for the display
	// timezone.
	value := string(tree.MustBeDString(row[0]))
	enumVal, err := strconv.Atoi(value)

	if err != nil {
		return err
	}

	if tz, ok := ui.DisplayTimezoneEnums[int64(enumVal)]; ok {
		if _, err := d.DB.Executor().Exec(
			ctx,
			"set-default-timezone",
			nil,
			"SET cluster setting ui.default_timezone=$1",
			tz); err != nil {
			return err
		}
	}

	return nil
}
