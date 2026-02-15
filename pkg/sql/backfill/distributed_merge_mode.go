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
	useDistributedMerge, err := shouldEnableDistributedMergeIndexBackfill(ctx, st, consumer)
	if err != nil {
		return jobspb.IndexBackfillDistributedMergeMode_Disabled, err
	}
	if !useDistributedMerge {
		return jobspb.IndexBackfillDistributedMergeMode_Disabled, nil
	}
	// When the setting specifically selects the declarative consumer,
	// return Force to skip the sorted-data optimization — the user
	// is explicitly opting in.
	mode := DistributedMergeIndexBackfillMode.Get(&st.SV)
	if mode == distributedMergeModeDeclarative {
		return jobspb.IndexBackfillDistributedMergeMode_Force, nil
	}
	return jobspb.IndexBackfillDistributedMergeMode_Enabled, nil
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
// Inverted indexes always return false because their key encoding
// expands each row into multiple entries.
func IsBackfillDataSorted(sourceIndex, destIndex catalog.Index) bool {
	if destIndex.GetType() == idxtype.INVERTED {
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
