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
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/settings"
)

// featureSchemaChangeEnabled is used to enable and disable any features that
// require schema changes. Documentation for which features are covered TBD.
// TODO(angelaw): should this live in a separate file here, or in one of the
// other files under the sql package? Or be a public variable in the featureflag
// package?
var featureSchemaChangeEnabled = settings.RegisterPublicBoolSetting(
	"feature.schemachange.enabled",
	"set to true to enable schema changes, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault)
