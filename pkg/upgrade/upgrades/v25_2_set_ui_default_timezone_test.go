// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	uisettings "github.com/cockroachdb/cockroach/pkg/ui/settings"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
)

func TestSetUiDefaultTimezone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V25_2)
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					ClusterVersionOverride:         clusterversion.MinSupported.Version(),
				},
			},
		},
	}

	testutils.RunValues(t, "display_timezone", maps.Values(uisettings.DisplayTimezoneEnums), func(t *testing.T, val string) {
		cluster := testcluster.StartTestCluster(t, 1, clusterArgs)
		defer cluster.Stopper().Stop(ctx)
		sqlDB := cluster.ServerConn(0)

		var displayTimezoneVal string
		var defaultTimezoneVal string
		runner := sqlutils.MakeSQLRunner(sqlDB)
		runner.Exec(t, fmt.Sprintf("SET cluster setting ui.display_timezone = '%s'", val))
		runner.QueryRow(t, "SHOW cluster setting ui.display_timezone").Scan(&displayTimezoneVal)
		// cast to lower case because enum setting type sets all setting values to lower case
		require.Equal(t, strings.ToLower(val), displayTimezoneVal)

		upgrades.Upgrade(t, sqlDB, clusterversion.V25_2_SetUiDefaultTimezoneSetting, nil, false)

		runner.QueryRow(t, "SHOW cluster setting ui.default_timezone").Scan(&defaultTimezoneVal)
		require.Equal(t, strings.ToLower(val), defaultTimezoneVal)
	})

	t.Run("display_timezone=default", func(t *testing.T) {
		cluster := testcluster.StartTestCluster(t, 1, clusterArgs)
		defer cluster.Stopper().Stop(ctx)
		sqlDB := cluster.ServerConn(0)
		runner := sqlutils.MakeSQLRunner(sqlDB)

		var displayTimezoneVal string
		var defaultTimezoneVal string

		runner.QueryRow(t, "SHOW cluster setting ui.display_timezone").Scan(&displayTimezoneVal)
		require.Equal(t, strings.ToLower(uisettings.DisplayTimezone.DefaultString()), displayTimezoneVal)

		upgrades.Upgrade(t, sqlDB, clusterversion.V25_2_SetUiDefaultTimezoneSetting, nil, false)
		runner.QueryRow(t, "SHOW cluster setting ui.default_timezone").Scan(&defaultTimezoneVal)
		// Since ui.display_timezone has not been set, `ui.default_timezone` should
		// still be the default value.
		require.Equal(t, uisettings.DefaultTimezone.Default(), defaultTimezoneVal)
	})
}
