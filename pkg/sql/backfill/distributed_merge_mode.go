// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfill

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkutil"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
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

// shouldEnableDistributedMergeIndexBackfill determines the distributed merge
// mode for the specified backfill consumer based on the current cluster setting
// and version state.
func shouldEnableDistributedMergeIndexBackfill(
	ctx context.Context, st *cluster.Settings, consumer DistributedMergeConsumer,
) (jobspb.IndexBackfillDistributedMergeMode, error) {
	mode := DistributedMergeIndexBackfillMode.Get(&st.SV)
	var result jobspb.IndexBackfillDistributedMergeMode
	switch mode {
	case distributedMergeModeDisabled, distributedMergeModeAliasFalse, distributedMergeModeAliasOff:
		return jobspb.IndexBackfillDistributedMergeMode_Disabled, nil
	case distributedMergeModeLegacy:
		if consumer != DistributedMergeConsumerLegacy {
			return jobspb.IndexBackfillDistributedMergeMode_Disabled, nil
		}
		result = jobspb.IndexBackfillDistributedMergeMode_Enabled
	case distributedMergeModeDeclarative:
		if consumer != DistributedMergeConsumerDeclarative {
			return jobspb.IndexBackfillDistributedMergeMode_Disabled, nil
		}
		// Explicit opt-in skips the sorted-data optimization.
		result = jobspb.IndexBackfillDistributedMergeMode_Force
	case distributedMergeModeEnabled, distributedMergeModeAliasTrue, distributedMergeModeAliasOn:
		result = jobspb.IndexBackfillDistributedMergeMode_Enabled
	default:
		return jobspb.IndexBackfillDistributedMergeMode_Disabled,
			errors.AssertionFailedf("unrecognized distributed merge index backfill mode %d", mode)
	}
	if !st.Version.IsActive(ctx, clusterversion.V26_1) {
		return jobspb.IndexBackfillDistributedMergeMode_Disabled,
			pgerror.New(pgcode.FeatureNotSupported, "distributed merge requires cluster version 26.1")
	}
	return result, nil
}

// EnableDistributedMergeIndexBackfillSink updates the backfiller spec to use the
// distributed merge sink. The file prefix stores just the path portion; each
// processor constructs the full nodelocal URI using its own node ID at runtime.
func EnableDistributedMergeIndexBackfillSink(jobID jobspb.JobID, spec *execinfrapb.BackfillerSpec) {
	spec.UseDistributedMergeSink = true
	spec.DistributedMergeFilePrefix = bulkutil.NewDistMergePaths(jobID).MapPath()
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
	return shouldEnableDistributedMergeIndexBackfill(ctx, st, consumer)
}

// IsBackfillDataSorted reports whether the data produced by scanning
// sourceIndex will be sorted in the key order of destIndex. When data
// is sorted, SSTs from different PK ranges are non-overlapping in the
// destination index key space, making distributed merge unnecessary.
//
// The check compares leading key columns of both indexes. If every
// overlapping column (up to the shorter index's key column count)
// shares the same column ID, the data is considered sorted. Direction
// is intentionally ignored: matching column IDs means the data is
// ordered in the destination key space regardless of sort direction.
//
// Non-forward indexes always return false because their key encoding
// expands each row into multiple entries.
func IsBackfillDataSorted(sourceIndex, destIndex catalog.Index) bool {
	if destIndex.GetType() != idxtype.FORWARD {
		return false
	}
	srcCols := sourceIndex.NumKeyColumns()
	dstCols := destIndex.NumKeyColumns()
	if srcCols == 0 || dstCols == 0 {
		return false
	}
	n := srcCols
	if dstCols < n {
		n = dstCols
	}
	for i := 0; i < n; i++ {
		if sourceIndex.GetKeyColumnID(i) != destIndex.GetKeyColumnID(i) {
			return false
		}
	}
	return true
}
