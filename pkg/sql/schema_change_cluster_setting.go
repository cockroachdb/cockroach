// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// featureSchemaChangeEnabled is the cluster setting used to enable and disable
// any features that require schema changes. Documentation for which features
// are covered TBD.
var featureSchemaChangeEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"feature.schema_change.enabled",
	"set to true to enable schema changes, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
	settings.WithPublic)

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

// CheckFeature checks if a schema change feature is allowed.
func (p *planner) CheckFeature(ctx context.Context, featureName tree.SchemaFeatureName) error {
	return checkSchemaChangeEnabled(ctx, p.ExecCfg(), string(featureName))
}
