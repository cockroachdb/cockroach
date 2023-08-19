// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	settings.TenantWritable,
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
