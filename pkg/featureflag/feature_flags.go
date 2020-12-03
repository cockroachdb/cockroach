// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package featureflag

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// FeatureFlagEnabledDefault is used for the default value of all feature flag
// cluster settings.
const FeatureFlagEnabledDefault = true

// CheckEnabled checks whether a specific feature has been enabled or disabled
// via its cluster settings, and returns an error if it has been disabled. It
// increments a telemetry counter for any feature flag that is denied and logs
// the feature and category that was denied.
func CheckEnabled(
	ctx context.Context, s *settings.BoolSetting, sv *settings.Values, featureName string,
) error {
	if enabled := s.Get(sv); !enabled {
		telemetry.Inc(sqltelemetry.FeatureDeniedByFeatureFlagCounter)
		if log.V(2) {
			log.Warningf(
				ctx,
				"feature %s was attempted but disabled via cluster settings",
				featureName,
			)
		}

		return pgerror.Newf(
			pgcode.OperatorIntervention,
			"feature %s was disabled by the database administrator",
			featureName,
		)
	}
	return nil
}
