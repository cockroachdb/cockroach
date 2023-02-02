// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	// Import keyvisjob so that it's init function will be called.
	_ "github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvisjob"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// keyVisualizerTablesMigration creates the system.span_stats_unique_keys, system.span_stats_buckets,
// system.span_stats_samples, and system.span_stats_tenant_boundaries tables.
func keyVisualizerTablesMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.SystemDeps,
) error {

	tables := []catalog.TableDescriptor{
		systemschema.SpanStatsUniqueKeysTable,
		systemschema.SpanStatsBucketsTable,
		systemschema.SpanStatsSamplesTable,
		systemschema.SpanStatsTenantBoundariesTable,
	}

	for _, table := range tables {
		err := createSystemTable(ctx, d.DB.KV(), d.Settings, keys.SystemSQLCodec,
			table)
		if err != nil {
			return err
		}
	}

	shouldBootstrapJob := true
	shouldConfigureTTL := true

	if knobs := d.KeyVisKnobs; knobs != nil {
		shouldBootstrapJob = !knobs.SkipJobBootstrap
		shouldConfigureTTL = !knobs.SkipZoneConfigBootstrap
	}

	// Set the TTL for the SpanStatsTenantBoundariesTable table.
	if shouldConfigureTTL {
		if _, err := d.DB.Executor().ExecEx(
			ctx,
			"set-SpanStatsTenantBoundariesTable-TTL",
			nil,
			sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			"ALTER TABLE system.span_stats_tenant_boundaries CONFIGURE ZONE USING gc.ttlseconds = $1",
			3600, /* one hour */
		); err != nil {
			return err
		}
	}

	if shouldBootstrapJob {
		record := jobs.Record{
			JobID:         jobs.KeyVisualizerJobID,
			Description:   "key visualizer job",
			Username:      username.NodeUserName(),
			Details:       jobspb.KeyVisualizerDetails{},
			Progress:      jobspb.KeyVisualizerProgress{},
			NonCancelable: true, // The job can't be canceled, but it can be paused.
		}

		// Make sure job with id doesn't already exist in system.jobs.
		row, err := d.DB.Executor().QueryRowEx(
			ctx,
			"check for existing key visualizer job",
			nil,
			sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			"SELECT * FROM system.jobs WHERE id = $1",
			record.JobID,
		)
		if err != nil {
			return err
		}

		// If there isn't a row for the key visualizer job, create the job.
		if row == nil {
			if _, err := d.JobRegistry.CreateAdoptableJobWithTxn(ctx, record, record.JobID, nil); err != nil {
				return err
			}
		}
	}

	return nil
}
