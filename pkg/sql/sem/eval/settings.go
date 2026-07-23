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
