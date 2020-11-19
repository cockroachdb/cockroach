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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// FeatureFlagEnabledDefault is used for the default value of all feature flag
// cluster settings.
const FeatureFlagEnabledDefault = true

// CheckEnabled checks whether a specific feature has been enabled or disabled
// via its cluster settings, and returns an error if it has been disabled.
func CheckEnabled(s *settings.BoolSetting, sv *settings.Values, featureName string) error {
	if enabled := s.Get(sv); !enabled {
		return pgerror.Newf(pgcode.OperatorIntervention,
			"%s feature was disabled by the database administrator",
			featureName)
	}
	return nil
}
