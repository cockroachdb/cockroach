// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfill

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// DistributedMergeConsumer identifies which backfill pipeline is attempting to
// opt into the distributed merge infrastructure.
type DistributedMergeConsumer int

const (
	// DistributedMergeConsumerLegacy corresponds to the legacy schema changer.
	DistributedMergeConsumerLegacy DistributedMergeConsumer = iota
	// DistributedMergeConsumerDeclarative corresponds to the declarative schema
	// changer (new schema change).
	DistributedMergeConsumerDeclarative
)

type distributedMergeIndexBackfillMode int64

const (
	distributedMergeModeDisabled distributedMergeIndexBackfillMode = iota
	distributedMergeModeEnabled
	distributedMergeModeLegacy
	distributedMergeModeDeclarative
	// aliases for synonyms.
	distributedMergeModeAliasFalse
	distributedMergeModeAliasTrue
	distributedMergeModeAliasOff
	distributedMergeModeAliasOn
)

// DistributedMergeIndexBackfillMode exposes the cluster setting used to control
// when index backfills run through the distributed merge pipeline.
var DistributedMergeIndexBackfillMode = settings.RegisterEnumSetting(
	settings.ApplicationLevel,
	"bulkio.index_backfill.distributed_merge.mode",
	"controls when the distributed merge pipeline powers index backfills: disabled/off/false, legacy, declarative, or enabled/on/true",
	"disabled",
	map[distributedMergeIndexBackfillMode]string{
		distributedMergeModeDisabled:    "disabled",
		distributedMergeModeEnabled:     "enabled",
		distributedMergeModeLegacy:      "legacy",
		distributedMergeModeDeclarative: "declarative",
		distributedMergeModeAliasFalse:  "false",
		distributedMergeModeAliasTrue:   "true",
		distributedMergeModeAliasOff:    "off",
		distributedMergeModeAliasOn:     "on",
	},
	settings.WithRetiredName("bulkio.index_backfill.distributed_merge.enabled"),
)

// DistributedMergeIterations controls the number of merge iterations to perform
// during index backfills using the distributed merge pipeline.
var DistributedMergeIterations = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.index_backfill.distributed_merge.iterations",
	"number of merge iterations to perform during index backfills",
	1,
	settings.IntWithMinimum(1),
)

// shouldEnableDistributedMergeIndexBackfill determines whether the specified
// backfill consumer should opt into the distributed merge pipeline based on the
// current cluster setting and version state.
func shouldEnableDistributedMergeIndexBackfill(
	ctx context.Context, st *cluster.Settings, consumer DistributedMergeConsumer,
) (bool, error) {
	mode := DistributedMergeIndexBackfillMode.Get(&st.SV)
	var enable bool
	switch mode {
	case distributedMergeModeDisabled, distributedMergeModeAliasFalse, distributedMergeModeAliasOff:
		enable = false
	case distributedMergeModeLegacy:
		enable = consumer == DistributedMergeConsumerLegacy
	case distributedMergeModeDeclarative:
		enable = consumer == DistributedMergeConsumerDeclarative
	case distributedMergeModeEnabled, distributedMergeModeAliasTrue, distributedMergeModeAliasOn:
		enable = true
	default:
		return false, errors.AssertionFailedf("unrecognized distributed merge index backfill mode %d", mode)
	}
	if enable && !st.Version.IsActive(ctx, clusterversion.V26_1) {
		return false, pgerror.New(pgcode.FeatureNotSupported, "distributed merge requires cluster version 26.1")
	}
	return enable, nil
}

// EnableDistributedMergeIndexBackfillSink updates the backfiller spec to use the
// distributed merge sink and file prefix for the provided storage prefix and job.
// The storagePrefix should be in the form "nodelocal://<nodeID>/" and is used as
// the base path for temporary SST files.
func EnableDistributedMergeIndexBackfillSink(
	storagePrefix string, jobID jobspb.JobID, spec *execinfrapb.BackfillerSpec,
) {
	spec.UseDistributedMergeSink = true
	spec.DistributedMergeFilePrefix = fmt.Sprintf("%sjob/%d/map", storagePrefix, jobID)
}

// DetermineDistributedMergeMode evaluates the cluster setting to decide
// whether backfills for the specified consumer should opt into the distributed
// merge pipeline.
func DetermineDistributedMergeMode(
	ctx context.Context, st *cluster.Settings, consumer DistributedMergeConsumer,
) (jobspb.IndexBackfillDistributedMergeMode, error) {
	if st == nil {
		return jobspb.IndexBackfillDistributedMergeMode_Disabled, nil
	}
	useDistributedMerge, err := shouldEnableDistributedMergeIndexBackfill(ctx, st, consumer)
	if err != nil {
		return jobspb.IndexBackfillDistributedMergeMode_Disabled, err
	}
	if useDistributedMerge {
		return jobspb.IndexBackfillDistributedMergeMode_Enabled, nil
	}
	return jobspb.IndexBackfillDistributedMergeMode_Disabled, nil
}

// StatementAllowsDistributedMerge reports whether this statement may use the
// distributed merge pipeline. This only checks statement-level eligibility. It
// does not decide whether the statement actually runs an index backfill.
//
// TODO(156934): everything is allowed except statements that create new unique
// indexes. Unique index creation is blocked until the distributed merge path
// supports enforcing uniqueness.
func StatementAllowsDistributedMerge(stmt tree.Statement) bool {
	switch s := stmt.(type) {
	case *tree.CreateIndex:
		if s.Unique {
			return false
		}
	case *tree.AlterTable:
		for _, cmd := range s.Cmds {
			if alterTableCmdIntroducesUniqueness(cmd) {
				return false
			}
		}
	}
	return true
}

// alterTableCmdIntroducesUniqueness reports whether this command adds a new
// uniqueness requirement.
func alterTableCmdIntroducesUniqueness(cmd tree.AlterTableCmd) bool {
	switch c := cmd.(type) {
	case *tree.AlterTableAddColumn:
		col := c.ColumnDef
		if col == nil {
			return false
		}
		if col.PrimaryKey.IsPrimaryKey {
			return true
		}
		if col.Unique.IsUnique && !col.Unique.WithoutIndex {
			return true
		}
	case *tree.AlterTableAddConstraint:
		if u, ok := c.ConstraintDef.(*tree.UniqueConstraintTableDef); ok {
			return !u.WithoutIndex
		}
	case *tree.AlterTableAlterPrimaryKey:
		// Primary keys are unique and always backed by an index.
		return true
	}
	return false
}
