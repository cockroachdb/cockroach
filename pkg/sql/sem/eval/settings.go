// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

const experimentalBox2DClusterSettingName = "sql.spatial.experimental_box2d_comparison_operators.enabled"

var experimentalBox2DClusterSetting = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	experimentalBox2DClusterSettingName,
	"enables the use of certain experimental box2d comparison operators",
	false,
	settings.WithPublic)

// allowEnumVersionSkew is an escape hatch for the assertion in DEnum.Compare
// that the two values being compared were produced with the same or adjacent
// descriptor versions of their enum type.
//
// Comparing values of versions further apart means that stale type metadata
// leaked into the query, which is a bug, and the assertion exists to surface
// such bugs (see cockroachlabs/cockroach#12711). However, the comparison itself
// is usually well-defined, so this setting allows unblocking a workload hit by
// such a bug until it is fixed.
var allowEnumVersionSkew = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.enum.allow_version_skew_in_comparisons.enabled",
	"if set, comparing two values of an enum type that were produced with "+
		"versions of the type more than one version apart is allowed when the "+
		"two versions are consistent with each other and with the values, "+
		"instead of failing with an internal error",
	false,
)

func checkExperimentalBox2DComparisonOperatorEnabled(s *cluster.Settings) error {
	if !experimentalBox2DClusterSetting.Get(&s.SV) {
		return errors.WithHintf(
			pgerror.Newf(
				pgcode.FeatureNotSupported,
				"this box2d comparison operator is experimental",
			),
			"To enable box2d comparators, use `SET CLUSTER SETTING %s = on`.",
			experimentalBox2DClusterSettingName,
		)
	}
	return nil
}
