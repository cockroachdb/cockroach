// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestIsTypeSupportedInVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		v clusterversion.VersionKey
		t *types.T

		ok bool
	}{
		{clusterversion.Version19_2, types.Time, true},
		{clusterversion.Version19_2, types.Timestamp, true},
		{clusterversion.Version19_2, types.Interval, true},

		{clusterversion.Version19_2, types.TimeTZ, false},
		{clusterversion.VersionTimeTZType, types.TimeTZ, true},

		{clusterversion.Version19_2, types.MakeTime(0), false},
		{clusterversion.Version19_2, types.MakeTimeTZ(0), false},
		{clusterversion.VersionTimeTZType, types.MakeTimeTZ(0), false},
		{clusterversion.Version19_2, types.MakeTimestamp(0), false},
		{clusterversion.Version19_2, types.MakeTimestampTZ(0), false},
		{
			clusterversion.Version19_2,
			types.MakeInterval(types.IntervalTypeMetadata{Precision: 3, PrecisionIsSet: true}),
			false,
		},
		{
			clusterversion.Version19_2,
			types.MakeInterval(
				types.IntervalTypeMetadata{
					DurationField: types.IntervalDurationField{DurationType: types.IntervalDurationType_SECOND},
				},
			),
			false,
		},
		{clusterversion.VersionTimePrecision, types.MakeTime(0), true},
		{clusterversion.VersionTimePrecision, types.MakeTimeTZ(0), true},
		{clusterversion.VersionTimePrecision, types.MakeTimestamp(0), true},
		{clusterversion.VersionTimePrecision, types.MakeTimestampTZ(0), true},
		{
			clusterversion.VersionTimePrecision,
			types.MakeInterval(types.IntervalTypeMetadata{Precision: 3, PrecisionIsSet: true}),
			true,
		},
		{
			clusterversion.VersionTimePrecision,
			types.MakeInterval(
				types.IntervalTypeMetadata{
					DurationField: types.IntervalDurationField{DurationType: types.IntervalDurationType_SECOND},
				},
			),
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s:%s", tc.v, tc.t.SQLString()), func(t *testing.T) {
			ok, err := isTypeSupportedInVersion(
				clusterversion.ClusterVersion{Version: clusterversion.VersionByKey(tc.v)},
				tc.t,
			)
			require.NoError(t, err)
			require.Equal(t, tc.ok, ok)
		})
	}
}
