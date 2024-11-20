// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// addJobsTables adds the job_progress, job_progress_history, job_status and
// job_message tables.
func addJobsTables(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		if err := createSystemTable(
			ctx, d.DB, d.Settings, d.Codec,
			systemschema.SystemJobProgressTable,
			tree.LocalityLevelTable,
		); err != nil {
			return err
		}

		if err := createSystemTable(
			ctx, d.DB, d.Settings, d.Codec,
			systemschema.SystemJobProgressHistoryTable,
			tree.LocalityLevelTable,
		); err != nil {
			return err
		}

		if err := createSystemTable(
			ctx, d.DB, d.Settings, d.Codec,
			systemschema.SystemJobStatusTable,
			tree.LocalityLevelTable,
		); err != nil {
			return err
		}

		if err := createSystemTable(
			ctx, d.DB, d.Settings, d.Codec,
			systemschema.SystemJobMessageTable,
			tree.LocalityLevelTable,
		); err != nil {
			return err
		}

		return nil
	})
}
