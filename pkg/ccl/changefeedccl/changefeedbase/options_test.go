// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedbase

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestOptionsValidations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, MakeDefaultOptions().ValidateForCreateChangefeed(false),
		"Default options should be valid")
	require.NoError(t, MakeDefaultOptions().ValidateForCreateChangefeed(true),
		"Default options should be valid")

	tests := []struct {
		input     map[string]string
		isPred    bool
		expectErr string
	}{
		{map[string]string{"format": "txt"}, false, "unknown format"},
		{map[string]string{"initial_scan": "", "no_initial_scan": ""}, false, "cannot specify both"},
		{map[string]string{"format": "txt"}, true, "unknown format"},
		{map[string]string{"initial_scan": "", "no_initial_scan": ""}, true, "cannot specify both"},
		{map[string]string{"format": "parquet", "topic_in_value": ""}, false, "cannot specify both"},
		// Verify that the returned error uses the syntax initial_scan='yes' instead of initial_scan_only. See #97008.
		{map[string]string{"initial_scan_only": "", "resolved": ""}, true, "cannot specify both initial_scan='only'"},
		{map[string]string{"initial_scan_only": "", "resolved": ""}, true, "cannot specify both initial_scan='only'"},
		{map[string]string{"key_column": "b"}, false, "requires the unordered option"},
	}

	for _, test := range tests {
		o := MakeStatementOptions(test.input)
		err := o.ValidateForCreateChangefeed(test.isPred)
		if test.expectErr == "" {
			require.NoError(t, err)
		} else {
			require.Error(t, err, fmt.Sprintf("%v should not be valid", test.input))
			require.Contains(t, err.Error(), test.expectErr)
		}
	}
}

func TestLaggingRangesVersionGate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// The version does not matter if the default config is used.
	t.Run("default config", func(t *testing.T) {
		opts := MakeDefaultOptions()
		settings := cluster.MakeTestingClusterSettingsWithVersions(clusterversion.ByKey(clusterversion.V23_2_ChangefeedLaggingRangesOpts), clusterversion.ByKey(clusterversion.V23_1), true)
		_, _, err := opts.GetLaggingRangesConfig(ctx, settings)
		require.NoError(t, err)

		settings = cluster.MakeTestingClusterSettingsWithVersions(clusterversion.ByKey(clusterversion.V23_2_ChangefeedLaggingRangesOpts-1), clusterversion.ByKey(clusterversion.V23_1), true)
		_, _, err = opts.GetLaggingRangesConfig(ctx, settings)
		require.NoError(t, err)
	})

	t.Run("non-default options", func(t *testing.T) {
		opts := MakeDefaultOptions()

		opts.m[OptLaggingRangesThreshold] = "25ms"
		opts.m[OptLaggingRangesPollingInterval] = "250ms"

		settings := cluster.MakeTestingClusterSettingsWithVersions(clusterversion.ByKey(clusterversion.V23_2_ChangefeedLaggingRangesOpts), clusterversion.ByKey(clusterversion.V23_1), true)
		_, _, err := opts.GetLaggingRangesConfig(ctx, settings)
		require.NoError(t, err)

		settings = cluster.MakeTestingClusterSettingsWithVersions(clusterversion.ByKey(clusterversion.V23_2_ChangefeedLaggingRangesOpts-1), clusterversion.ByKey(clusterversion.V23_1), true)
		_, _, err = opts.GetLaggingRangesConfig(ctx, settings)
		require.Error(t, err, "cluster version must be 23.2 or greater")
	})
}
