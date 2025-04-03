// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings"
	uisettings "github.com/cockroachdb/cockroach/pkg/ui/settings"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// setUiDefaultTimezone sets the value of the 'ui.default_timezone' cluster
// setting to be the same as the 'ui.display_timezone' setting if it is set.
// 'ui.default_timezone' is the new cluster setting to replace
// 'ui.display_timezone' which doesn't have any restrictions on which timezones
// can be set.
func setUiDefaultTimezone(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {

	// Set ui.default_timezone to the value of ui.display_timezone if it has been
	// explicitly set.
	if uisettings.DisplayTimezone.ValueOrigin(ctx, &d.Settings.SV) == settings.OriginExplicitlySet {
		tz := uisettings.DisplayTimezone.String(&d.Settings.SV)
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
