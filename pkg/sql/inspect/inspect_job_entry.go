// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TriggerJob starts an inspect job for the snapshot. This always uses a fresh
// transaction to create and start the job.
func TriggerJob(
	ctx context.Context,
	jobRecordDescription string,
	execCfg *sql.ExecutorConfig,
	checks []*jobspb.InspectDetails_Check,
	asOf hlc.Timestamp,
) (*jobs.StartableJob, error) {
	record := jobs.Record{
		JobID:       execCfg.JobRegistry.MakeJobID(),
		Description: jobRecordDescription,
		Details: jobspb.InspectDetails{
			Checks: checks,
			AsOf:   asOf,
		},
		Progress:  jobspb.InspectProgress{},
		CreatedBy: nil,
		Username:  username.NodeUserName(),
		DescriptorIDs: func() descpb.IDs {
			ids := catalog.DescriptorIDSet{}
			for _, check := range checks {
				ids.Add(check.TableID)
			}
			return ids.Ordered()
		}(),
	}

	var job *jobs.StartableJob
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		return execCfg.JobRegistry.CreateStartableJobWithTxn(ctx, &job, record.JobID, txn, record)
	}); err != nil {
		if job != nil {
			if cleanupErr := job.CleanupOnRollback(ctx); cleanupErr != nil {
				log.Dev.Warningf(ctx, "failed to cleanup StartableJob: %v", cleanupErr)
			}
		}
		return nil, err
	}

	if err := job.Start(ctx); err != nil {
		return nil, err
	}
	log.Dev.Infof(ctx, "created and started inspect job %d", job.ID())

	return job, nil
}

// checksForDatabase generates checks on every supported index on every
// table in the given database.
func checksForDatabase(
	ctx context.Context, p sql.PlanHookState, db catalog.DatabaseDescriptor,
) ([]*jobspb.InspectDetails_Check, error) {
	avoidLeased := false
	if aost := p.ExtendedEvalContext().AsOfSystemTime; aost != nil {
		avoidLeased = true
	}
	byNameGetter := p.Descriptors().ByNameWithLeased(p.Txn())
	if avoidLeased {
		byNameGetter = p.Descriptors().ByName(p.Txn())
	}
	tables, err := byNameGetter.Get().GetAllTablesInDatabase(ctx, p.Txn(), db)
	if err != nil {
		return nil, err
	}

	checks := []*jobspb.InspectDetails_Check{}

	if err := tables.ForEachDescriptor(func(desc catalog.Descriptor) error {
		tableChecks, err := ChecksForTable(ctx, p.ExecCfg(), desc.(catalog.TableDescriptor), nil /* rowCount */)
		if err != nil {
			return err
		}
		checks = append(checks, tableChecks...)
		return nil
	}); err != nil {
		return nil, err
	}

	return checks, nil
}

// ChecksForTable generates checks on every supported index on the given table.
func ChecksForTable(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	table catalog.TableDescriptor,
	expectedRowCount *uint64,
) ([]*jobspb.InspectDetails_Check, error) {
	checks := []*jobspb.InspectDetails_Check{}

	// Skip non-physical tables that don't have physical storage to inspect.
	if !table.IsPhysicalTable() {
		return checks, nil
	}

	for _, idx := range table.ActiveIndexes() {
		checksOnIndex, err := checksForIndex(ctx, execCfg, index{TableDescriptor: table, Index: idx}, false /* errorOnNoSupport */)
		if err != nil {
			return nil, err
		}
		checks = append(checks, checksOnIndex...)
	}

	if expectedRowCount != nil {
		checks = append(checks, &jobspb.InspectDetails_Check{
			Type:         jobspb.InspectCheckRowCount,
			TableID:      table.GetID(),
			TableVersion: table.GetVersion(),
			RowCount:     *expectedRowCount,
		})
	}

	return checks, nil
}

// checksByIndexNames generates checks for the specified index names.
// If index names are not found or are not supported for inspection, an error is returned.
// Index names are deduplicated.
func checksByIndexNames(
	ctx context.Context, execCfg *sql.ExecutorConfig, p sql.PlanHookState, names tree.TableIndexNames,
) ([]*jobspb.InspectDetails_Check, error) {
	checks := []*jobspb.InspectDetails_Check{}

	// Collect the indexes and dedupe them.
	type indexKey struct {
		descpb.ID
		descpb.IndexID
	}

	var indexes []index
	var seenIndexes = make(map[indexKey]struct{})
	for _, indexName := range names {
		_, tableDesc, indexDesc, err := p.GetTableAndIndex(ctx, indexName, privilege.INSPECT, false /* skipCache */)
		if err != nil {
			return nil, err
		}

		key := indexKey{tableDesc.GetID(), indexDesc.GetID()}
		if _, ok := seenIndexes[key]; !ok {
			indexes = append(indexes, index{tableDesc, indexDesc})
		}
		seenIndexes[key] = struct{}{}
	}

	for _, idx := range indexes {
		if checksForIndex, err := checksForIndex(ctx, execCfg, idx, true /* errorOnNoSupport */); err != nil {
			return nil, err
		} else {
			checks = append(checks, checksForIndex...)
		}
	}

	return checks, nil
}

type index struct {
	catalog.TableDescriptor
	catalog.Index
}

// checkFromIndexFunc defines a function that returns a check for a given index, a
// string reason for why the index is unsupported, or an error.
type checkFromIndexFunc func(ctx context.Context, execCfg *sql.ExecutorConfig, idx index) (*jobspb.InspectDetails_Check, string, error)

var checkSpecFactories = []checkFromIndexFunc{
	func(ctx context.Context, execCfg *sql.ExecutorConfig, idx index) (*jobspb.InspectDetails_Check, string, error) {
		if reason := isSupportedIndexForIndexConsistencyCheck(idx.Index, idx.TableDescriptor); reason != "" {
			return nil, reason, nil
		}

		return &jobspb.InspectDetails_Check{
			Type:         jobspb.InspectCheckIndexConsistency,
			TableID:      idx.TableDescriptor.GetID(),
			IndexID:      idx.Index.GetID(),
			TableVersion: idx.TableDescriptor.GetVersion(),
		}, "", nil
	},
	func(ctx context.Context, execCfg *sql.ExecutorConfig, idx index) (*jobspb.InspectDetails_Check, string, error) {
		if reason, _, err := isSupportedIndexForUniquenessCheck(
			ctx,
			idx.Index,
			idx.TableDescriptor,
			execCfg.Settings.Version.IsActive(ctx, clusterversion.V26_2),
			uniquenessCheckComplexKeysEnabled.Get(execCfg.SV()),
		); err != nil {
			return nil, "", err
		} else if reason != "" {
			return nil, reason, nil
		}

		return &jobspb.InspectDetails_Check{
			Type:         jobspb.InspectCheckUniqueness,
			TableID:      idx.TableDescriptor.GetID(),
			IndexID:      idx.Index.GetID(),
			TableVersion: idx.TableDescriptor.GetVersion(),
		}, "", nil
	},
}

func checksForIndex(
	ctx context.Context, execCfg *sql.ExecutorConfig, idx index, errorOnNoSupport bool,
) ([]*jobspb.InspectDetails_Check, error) {
	var checksForIndex []*jobspb.InspectDetails_Check
	var reasons []string
	for _, specFactory := range checkSpecFactories {
		check, reason, err := specFactory(ctx, execCfg, idx)
		if err != nil {
			return nil, err
		}
		if reason != "" {
			reasons = append(reasons, reason)
		} else if check != nil {
			checksForIndex = append(checksForIndex, check)
		}
	}

	if len(checksForIndex) == 0 && errorOnNoSupport {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"no supported checks for index %q on table %q (%s)",
			idx.Index.GetName(), idx.TableDescriptor.GetName(), strings.Join(reasons, "; "))
	}

	return checksForIndex, nil
}

// isSupportedIndexForIndexConsistencyCheck returns an empty string if a given
// index is supported for index consistency checking or a reason if it is not.
func isSupportedIndexForIndexConsistencyCheck(
	index catalog.Index, table catalog.TableDescriptor,
) (reason string) {
	if !index.Public() {
		return "index is not public"
	}

	if index.Primary() {
		return "cannot check primary index consistency against itself"
	}

	// We can only check a secondary index that has a 1-to-1 mapping between
	// keys in the primary index. Unsupported indexes should be filtered out
	// when the job is created.
	// TODO(154862): support partial indexes
	if index.IsPartial() {
		return "partial index"
	}
	// TODO(154762): support hash sharded indexes
	if index.IsSharded() {
		return "hash-sharded index"
	}

	switch t := index.GetType(); t {
	// TODO(154860): support inverted indexes
	case idxtype.INVERTED, idxtype.VECTOR:
		return t.String()
	}

	// TODO(154772): support expression indexes
	if table.IsExpressionIndex(index) {
		return "expression index"
	}

	// Check if any of the index key columns are virtual columns.
	// TODO(155841): add support for indexes on virtual columns.
	for i := 0; i < index.NumKeyColumns(); i++ {
		colID := index.GetKeyColumnID(i)
		col := catalog.FindColumnByID(table, colID)
		if col != nil && col.IsVirtual() {
			return "index on virtual column"
		}
	}

	return ""
}

func isSupportedIndexForUniquenessCheck(
	ctx context.Context,
	index catalog.Index,
	table catalog.TableDescriptor,
	isActiveV26_2, isComplexKeysEnabled bool,
) (reason string, uniquePos int, err error) {
	if !isActiveV26_2 {
		return "uniqueness: check requires v26.2", 0, nil
	}

	if !index.Primary() {
		return "uniqueness: check only valid on primary index", 0, nil
	}

	if !isRegionalByRow(table) {
		return "uniqueness: check only supported on REGIONAL BY ROW tables", 0, nil
	}

	uniquePositions, err := findUniqueRowIDColPositions(table, index)
	if err != nil {
		return "", 0, err
	}

	if len(uniquePositions) == 0 {
		return "uniqueness: check only supported on tables with a unique column", 0, nil
	} else if len(uniquePositions) > 1 {
		return "uniqueness: check only supported on tables with a single unique column", 0, nil
	}

	if uniquePositions[0] == 1 || isComplexKeysEnabled {
		return "", uniquePositions[0], nil
	} else {
		return "uniqueness: complex key layout requires the cluster setting to opt in to (expensive) validation", 0, nil
	}
}
