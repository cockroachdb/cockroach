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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/settings"
)

// featureSchemaChangeEnabled is the cluster setting used to enable and disable
// any features that require schema changes. Documentation for which features
// are covered TBD.
var featureSchemaChangeEnabled = settings.RegisterBoolSetting(
	"feature.schema_change.enabled",
	"set to true to enable schema changes, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
).WithPublic()

// checkSchemaChangeEnabled is a method that wraps the featureflag.CheckEnabled
// method specifically for all features that are categorized as schema changes.
func checkSchemaChangeEnabled(
	ctx context.Context, execCfg *ExecutorConfig, schemaFeatureName string,
) error {
	if err := featureflag.CheckEnabled(
		ctx,
		execCfg,
		featureSchemaChangeEnabled,
		fmt.Sprintf("%s is part of the schema change category, which", schemaFeatureName),
	); err != nil {
		return err
	}
	return nil
}
