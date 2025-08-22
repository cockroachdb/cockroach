// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/ingeststopped"
	"github.com/cockroachdb/cockroach/pkg/jobs/joberror"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

const importJobRecoveryEventType eventpb.RecoveryEventType = "import_job"

type importTestingKnobs struct {
	afterImport            func(summary roachpb.RowCount) error
	beforeRunDSP           func() error
	onSetupFinish          func(flowinfra.Flow)
	rowCountValidation     chan error
	alwaysFlushJobProgress bool
}

type importResumer struct {
	job      *jobs.Job
	settings *cluster.Settings
	res      roachpb.RowCount

	testingKnobs importTestingKnobs
}

func (r *importResumer) TestingSetAfterImportKnob(fn func(summary roachpb.RowCount) error) {
	r.testingKnobs.afterImport = fn
}

var _ jobs.TraceableJob = &importResumer{}

func (r *importResumer) ForceRealSpan() bool {
	return true
}

// DumpTraceAfterRun implements the TraceableJob interface.
func (r *importResumer) DumpTraceAfterRun() bool {
	return true
}

var _ jobs.Resumer = &importResumer{}

var processorsPerNode = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.import.processors_per_node",
	"number of input processors to run on each sql instance",
	1,
	settings.PositiveInt,
)

var initialSplitsPerProcessor = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.import.initial_splits_per_processor",
	"number of initial splits each import processor with enough data will create",
	3,
	settings.NonNegativeInt,
)

var performConstraintValidation = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"bulkio.import.constraint_validation.unsafe.enabled",
	"should import perform constraint validation after data load. "+
		"NOTE: this setting should not be used on production clusters, as it could result in "+
		"incorrect query results if the imported data set violates constraints (i.e. contains duplicates).",
	true,
	settings.WithUnsafe,
)

var importRowCountValidation = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"bulkio.import.row_count_validation.unsafe.enabled",
	"should import perform row count validation after data load. "+
		"NOTE: this setting should not be used on production clusters, as disabling it could result in "+
		"undetected data corruption if the import process fails to import the expected number of rows.",
	true,
	settings.WithUnsafe,
)

// Resume is part of the jobs.Resumer interface.
func (r *importResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)
	newHistoricalInternalExecByTime := func(now hlc.Timestamp) descs.HistoricalInternalExecTxnRunner {
		return descs.NewHistoricalInternalExecTxnRunner(now, func(ctx context.Context, fn descs.InternalExecFn) error {
			return p.ExecCfg().InternalDB.DescsTxn(ctx, func(
				ctx context.Context, txn descs.Txn,
			) error {
				if err := txn.KV().SetFixedTimestamp(ctx, now); err != nil {
					return err
				}
				return fn(ctx, txn)
			})
		})
	}

	details := r.job.Details().(jobspb.ImportDetails)
	if details.PreImportRowCount == nil {
		// TODO(janexing): is here even the right place to initialize this field?
		details.PreImportRowCount = make(map[uint64]int64)
	}
	files := details.URIs
	format := details.Format

	tables := make(map[string]*execinfrapb.ReadImportDataSpec_ImportTable, len(details.Tables))
	rowCountValidation := importRowCountValidation.Get(&p.ExecCfg().Settings.SV)

	grp := ctxgroup.WithContext(ctx)
	// expectedRowCountReadyByIndex maps from a BulkOpSummaryID defined by
	// the table id and the index id. syncutil.Map is used to avoid data race.
	expectedRowCountReadyByIndex := syncutil.Map[uint64, chan int64]{}
	// preCountWaitGroup tracks the number of indexes that have not completed
	// pre-import row counting. The table should only be taken offline after all
	// index row counts have finished.
	var preCountWaitGroup sync.WaitGroup
	// preImportRowCountMu protects concurrent access to details.PreImportRowCount.
	// We need this mutex because PreImportRowCount is hung on to details, and details
	// is defined in a proto file which only accepts generic types as fields, so we
	// can't use a thread-safe map type like syncutil.Map directly in the proto.
	var preImportRowCountMu syncutil.Mutex
	if rowCountValidation {
		// TODO(janexing): simplify this once the details.Tables is changed
		// to a single table rather than a list.
		for i := range details.Tables {
			tblDesc := tabledesc.NewBuilder(details.Tables[i].Desc).BuildImmutableTable()
			for _, idx := range tblDesc.AllIndexes() {
				idx := idx
				tableTag := kvpb.BulkOpSummaryID(uint64(tblDesc.GetID()), uint64(idx.GetID()))
				expectedCntChan := make(chan int64)
				expectedRowCountReadyByIndex.Store(tableTag, &expectedCntChan)
				preCountWaitGroup.Add(1)
				grp.GoCtx(func(ctx context.Context) error {
					var preImportIdxLen int64
					var err error
					// Skip pre-import row counting when resuming a paused job, as the counts
					// were already captured during the initial run. Additionally, attempting
					// to re-execute row count queries on an offline table descriptor would
					// fail during job resumption.
					preImportRowCountMu.Lock()
					cnt, ok := details.PreImportRowCount[tableTag]
					preImportRowCountMu.Unlock()
					if ok {
						preImportIdxLen = cnt
					} else {
						preImportIdxLen, err = sql.CountIndexRowsAndMaybeCheckUniqueness(ctx, tblDesc, idx, false /* withFirstMutationPublic */, newHistoricalInternalExecByTime(p.ExecCfg().Clock.Now()), sessiondata.NoSessionDataOverride)
						if err != nil {
							preCountWaitGroup.Done()
							return errors.Wrapf(err, "counting rows in index %q of table %q before import", idx.GetName(), tblDesc.GetName())
						}
						preImportRowCountMu.Lock()
						details.PreImportRowCount[tableTag] = preImportIdxLen
						preImportRowCountMu.Unlock()
					}

					// Pre-count is now complete, signal the wait group that the table
					// can now be brought offline.
					preCountWaitGroup.Done()
					expectedRowCountChan, _ := expectedRowCountReadyByIndex.Load(tableTag)
					select {
					case expectedCount := <-*expectedRowCountChan:
						// We reach here only after the table has been published, hence it has been
						// changed to PUBLIC from OFFLINE. We need to retrieve the freshest descriptor
						// for the post-import row count query to be run successfully.
						var newTblDesc catalog.TableDescriptor
						if err := sql.DescsTxn(ctx, p.ExecCfg(), func(
							ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
						) error {
							freshMutableTblDesc, err := descsCol.MutableByID(txn.KV()).Table(ctx, tblDesc.GetID())
							if err != nil {
								return err
							}
							newTblDesc = freshMutableTblDesc.ImmutableCopy().(catalog.TableDescriptor)
							return nil
						}); err != nil {
							return errors.Wrapf(err, "failed to get fresh table descriptor for table %s during post-import row count validation", tblDesc.GetName())
						}
						postImportIdxLen, err :=
							sql.CountIndexRowsAndMaybeCheckUniqueness(ctx, newTblDesc, idx,
								false, /* withFirstMutationPublic */
								newHistoricalInternalExecByTime(p.ExecCfg().Clock.Now()),
								sessiondata.NoSessionDataOverride)
						if err != nil {
							return errors.Wrapf(err, "counting rows in index %q of table %q after import", idx.GetName(), tblDesc.GetName())
						}
						if diff := postImportIdxLen - preImportIdxLen; diff != expectedCount {
							return errors.AssertionFailedf("row count validation failed for index %q of table %q: expected %d rows imported, got %d",
								idx.GetName(), newTblDesc.GetName(), expectedCount, diff)
						}
					case <-ctx.Done():
						return ctx.Err()
					}
					return nil
				})
			}
		}
	}

	if rowCountValidation {
		// Wait for all the pre-counts to finish before bringing the table offline,
		// but respect context cancellation.
		done := make(chan struct{})
		go func() {
			preCountWaitGroup.Wait()
			close(done)
		}()

		select {
		// All pre-counts completed successfully.
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if details.Tables != nil {
		// Skip prepare stage on job resumption, if it has already been completed.
		if !details.PrepareComplete {
			if err := sql.DescsTxn(ctx, p.ExecCfg(), func(
				ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
			) error {
				var preparedDetails jobspb.ImportDetails
				var err error
				curDetails := details

				preparedDetails, err = r.prepareTablesForIngestion(ctx, p, curDetails, txn.KV(), descsCol)
				if err != nil {
					return err
				}

				// Telemetry for multi-region.
				for _, table := range preparedDetails.Tables {
					dbDesc, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, table.Desc.GetParentID())
					if err != nil {
						return err
					}
					if dbDesc.IsMultiRegion() {
						telemetry.Inc(sqltelemetry.ImportIntoMultiRegionDatabaseCounter)
					}
				}

				// Update the job details now that the schemas and table descs have
				// been "prepared".
				return r.job.WithTxn(txn).Update(ctx, func(
					txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
				) error {
					pl := md.Payload
					*pl.GetImport() = preparedDetails

					// Update the set of descriptors for later observability.
					// TODO(ajwerner): Do we need this idempotence test?
					prev := md.Payload.DescriptorIDs
					if prev == nil {
						var descriptorIDs []descpb.ID
						for _, table := range preparedDetails.Tables {
							descriptorIDs = append(descriptorIDs, table.Desc.GetID())
						}
						pl.DescriptorIDs = descriptorIDs
					}
					ju.UpdatePayload(pl)
					return nil
				})
			}); err != nil {
				return err
			}

			// Re-initialize details after prepare step.
			details = r.job.Details().(jobspb.ImportDetails)
			emitImportJobEvent(ctx, p, jobs.StateRunning, r.job)
		}

		for _, i := range details.Tables {
			var tableName string
			if i.Name != "" {
				tableName = i.Name
			} else if i.Desc != nil {
				tableName = i.Desc.Name
			} else {
				return errors.New("invalid table specification")
			}

			tables[tableName] = &execinfrapb.ReadImportDataSpec_ImportTable{
				Desc:       i.Desc,
				TargetCols: i.TargetCols,
			}
		}
	}

	typeDescs := make([]*descpb.TypeDescriptor, len(details.Types))
	for i, t := range details.Types {
		typeDescs[i] = t.Desc
	}

	// If details.Walltime is still 0, then it was not set during
	// `prepareTablesForIngestion`. This indicates that we are in an IMPORT INTO,
	// and that the walltime was not set in a previous run of IMPORT.
	//
	// In the case of importing into existing tables we must wait for all nodes
	// to see the same version of the updated table descriptor, after which we
	// shall choose a ts to import from.
	if details.Walltime == 0 {
		// Now that we know all the tables are offline, pick a walltime at which we
		// will write.
		details.Walltime = p.ExecCfg().Clock.Now().WallTime

		// Check if the tables being imported into are starting empty, in which case
		// we can cheaply clear-range instead of revert-range to cleanup (or if the
		// cluster has finalized to 22.1, use DeleteRange without predicate
		// filtering).
		for i := range details.Tables {
			tblDesc := tabledesc.NewBuilder(details.Tables[i].Desc).BuildImmutableTable()
			tblSpan := tblDesc.TableSpan(p.ExecCfg().Codec)
			res, err := p.ExecCfg().DB.Scan(ctx, tblSpan.Key, tblSpan.EndKey, 1 /* maxRows */)
			if err != nil {
				return errors.Wrap(err, "checking if existing table is empty")
			}
			details.Tables[i].WasEmpty = len(res) == 0

			// Update the descriptor in the job record and in the database
			details.Tables[i].Desc.ImportStartWallTime = details.Walltime

			if err := bindTableDescImportProperties(ctx, p, tblDesc.GetID(), details.Walltime); err != nil {
				return err
			}
		}

		if err := r.job.NoTxn().SetDetails(ctx, details); err != nil {
			return err
		}
	}

	procsPerNode := int(processorsPerNode.Get(&p.ExecCfg().Settings.SV))
	initialSplitsPerProc := int(initialSplitsPerProcessor.Get(&p.ExecCfg().Settings.SV))

	res, err := ingestWithRetry(
		ctx, p, r.job, tables, typeDescs, files, format, details.Walltime,
		r.testingKnobs, procsPerNode, initialSplitsPerProc,
	)
	if err != nil {
		return err
	}

	pkIDs := make(map[uint64]struct{}, len(details.Tables))
	for _, t := range details.Tables {
		pkIDs[kvpb.BulkOpSummaryID(uint64(t.Desc.ID), uint64(t.Desc.PrimaryIndex.ID))] = struct{}{}
	}
	r.res.DataSize = res.DataSize
	// NOTE: The following loop to update r.res cannot be integrated with the
	// later loop that signals expectedRowCountReadyByIndex for two reasons:
	// 1. The expectedRowCountReadyByIndex signal must happen after the table has been
	//    brought online via publishTables, otherwise the post-import row count query will fail.
	// 2. The update of r.res must happen before r.testingKnobs.afterImport because
	//    TestImportJobEventLogging expects the changed number of rows to be recorded
	//    prior to executing testingKnobs.afterImport so that it can test resuming
	//    interrupted import processes (such as TestCSVImportCanBeResumed).
	for id, count := range res.EntryCounts {
		if _, ok := pkIDs[id]; ok {
			r.res.Rows += count
		} else {
			r.res.IndexEntries += count
		}
	}

	if r.testingKnobs.afterImport != nil {
		if err := r.testingKnobs.afterImport(r.res); err != nil {
			return err
		}
	}

	if err := p.ExecCfg().JobRegistry.CheckPausepoint("import.after_ingest"); err != nil {
		return err
	}

	if performConstraintValidation.Get(&p.ExecCfg().Settings.SV) {
		if err := r.checkVirtualConstraints(ctx, p.ExecCfg(), r.job, p.User()); err != nil {
			return err
		}
	}

	// If the table being imported into referenced UDTs, ensure that a concurrent
	// schema change on any of the typeDescs has not modified the type descriptor. If
	// it has, it is unsafe to import the data and we fail the import job.
	if err := r.checkForUDTModification(ctx, p.ExecCfg()); err != nil {
		return err
	}

	if err := r.publishTables(ctx, p.ExecCfg(), res); err != nil {
		return err
	}

	// Now that the table descriptor is marked as PUBLIC, signal the row count
	// validation goroutines to continue with post-import row count checks.
	for id, count := range res.EntryCounts {
		// Signal row count validation goroutines with the expected count
		// from import.
		if expected, ok := expectedRowCountReadyByIndex.Load(id); ok {
			select {
			case *expected <- count:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// The special case where it is an empty import file, or resume from fully
	// processed files.
	if len(res.EntryCounts) == 0 {
		expectedRowCountReadyByIndex.Range(func(key uint64, value *chan int64) bool {
			select {
			case *value <- 0:
			case <-ctx.Done():
				return false
			}
			return true
		})
	}

	if rowCountValidation {
		rowCountValidationTestingKnob := r.testingKnobs.rowCountValidation
		// Wait for all row count validation goroutines to complete.
		// Since grp was created with ctxgroup.WithContext(ctx), grp.Wait() will
		// automatically return ctx.Err() immediately if the context is canceled,
		// without blocking. This eliminates the need for a separate select statement
		// to handle context cancellation - the ctxgroup package handles this for us.
		if err := grp.Wait(); err != nil {
			if rowCountValidationTestingKnob != nil {
				rowCountValidationTestingKnob <- err
				close(rowCountValidationTestingKnob)
			}
			return errors.Wrapf(err, "row count validation failed")
		}
		log.Event(ctx, "row count validation completed successfully")
		if rowCountValidationTestingKnob != nil {
			close(rowCountValidationTestingKnob)
		}
	}

	emitImportJobEvent(ctx, p, jobs.StateSucceeded, r.job)

	addToFileFormatTelemetry(details.Format.Format.String(), "succeeded")
	telemetry.CountBucketed("import.rows", r.res.Rows)
	const mb = 1 << 20
	sizeMb := r.res.DataSize / mb
	telemetry.CountBucketed("import.size-mb", sizeMb)

	sec := int64(timeutil.Since(timeutil.FromUnixMicros(r.job.Payload().StartedMicros)).Seconds())
	var mbps int64
	if sec > 0 {
		mbps = mb / sec
	}
	telemetry.CountBucketed("import.duration-sec.succeeded", sec)
	telemetry.CountBucketed("import.speed-mbps", mbps)
	// Tiny imports may skew throughput numbers due to overhead.
	if sizeMb > 10 {
		telemetry.CountBucketed("import.speed-mbps.over10mb", mbps)
	}

	logutil.LogJobCompletion(ctx, importJobRecoveryEventType, r.job.ID(), true, nil, r.res.Rows)

	return nil
}

// prepareTablesForIngestion prepares table descriptors for the ingestion
// step of import. The descriptors are in an IMPORTING state (offline) on
// successful completion of this method.
func (r *importResumer) prepareTablesForIngestion(
	ctx context.Context,
	p sql.JobExecContext,
	details jobspb.ImportDetails,
	txn *kv.Txn,
	descsCol *descs.Collection,
) (jobspb.ImportDetails, error) {
	importDetails := details
	importDetails.Tables = make([]jobspb.ImportDetails_Table, len(details.Tables))

	var err error
	var desc *descpb.TableDescriptor

	useImportEpochs := importEpochs.Get(&p.ExecCfg().Settings.SV)
	for i, table := range details.Tables {
		desc, err = prepareExistingTablesForIngestion(ctx, txn, descsCol, table.Desc, useImportEpochs)
		if err != nil {
			return importDetails, err
		}
		importDetails.Tables[i] = jobspb.ImportDetails_Table{
			Desc:       desc,
			Name:       table.Name,
			SeqVal:     table.SeqVal,
			TargetCols: table.TargetCols,
		}
	}

	importDetails.PrepareComplete = true

	// We have to wait for all nodes to see the same descriptor version before
	// choosing our Walltime.
	importDetails.Walltime = 0
	return importDetails, nil
}

// prepareExistingTablesForIngestion prepares descriptors for existing tables
// being imported into.
func prepareExistingTablesForIngestion(
	ctx context.Context,
	txn *kv.Txn,
	descsCol *descs.Collection,
	desc *descpb.TableDescriptor,
	useImportEpochs bool,
) (*descpb.TableDescriptor, error) {
	if len(desc.Mutations) > 0 {
		return nil, errors.Errorf("cannot IMPORT INTO a table with schema changes in progress -- try again later (pending mutation %s)", desc.Mutations[0].String())
	}

	// Note that desc is just used to verify that the version matches.
	importing, err := descsCol.MutableByID(txn).Table(ctx, desc.ID)
	if err != nil {
		return nil, err
	}
	// Ensure that the version of the table has not been modified since this
	// job was created.
	if got, exp := importing.Version, desc.Version; got != exp {
		return nil, errors.Errorf("another operation is currently operating on the table")
	}

	// Take the table offline for import.
	// TODO(dt): audit everywhere we get table descs (leases or otherwise) to
	// ensure that filtering by state handles IMPORTING correctly.

	// We only use the new OfflineForImport on 24.1, which bumps
	// the ImportEpoch, if we are completely on 24.1.
	if useImportEpochs {
		importing.OfflineForImport()
	} else {
		importing.SetOffline(tabledesc.OfflineReasonImporting)
	}

	// TODO(dt): de-validate all the FKs.
	if err := descsCol.WriteDesc(
		ctx, false /* kvTrace */, importing, txn,
	); err != nil {
		return nil, err
	}

	return importing.TableDesc(), nil
}

// bindTableDescImportProperties updates the table descriptor at the start of an
// import for a table that existed before the import.
func bindTableDescImportProperties(
	ctx context.Context, p sql.JobExecContext, id catid.DescID, startWallTime int64,
) error {
	if err := sql.DescsTxn(ctx, p.ExecCfg(), func(
		ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
	) error {
		mutableDesc, err := descsCol.MutableByID(txn.KV()).Table(ctx, id)
		if err != nil {
			return err
		}
		if err := mutableDesc.InitializeImport(startWallTime); err != nil {
			return err
		}
		if err := descsCol.WriteDesc(
			ctx, false /* kvTrace */, mutableDesc, txn.KV(),
		); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// publishTables updates the status of imported tables from OFFLINE to PUBLIC.
func (r *importResumer) publishTables(
	ctx context.Context, execCfg *sql.ExecutorConfig, res kvpb.BulkOpSummary,
) error {
	details := r.job.Details().(jobspb.ImportDetails)
	// Tables should only be published once.
	if details.TablesPublished {
		return nil
	}

	log.Event(ctx, "making tables live")

	err := sql.DescsTxn(ctx, execCfg, func(
		ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
	) error {
		b := txn.KV().NewBatch()
		for _, tbl := range details.Tables {
			newTableDesc, err := descsCol.MutableByID(txn.KV()).Table(ctx, tbl.Desc.ID)
			if err != nil {
				return err
			}
			newTableDesc.SetPublic()
			// NB: This is not using AllNonDropIndexes or directly mutating the
			// constraints returned by the other usual helpers because we need to
			// replace the `OutboundFKs` and `Checks` slices of newTableDesc with copies
			// that we can mutate. We need to do that because newTableDesc is a shallow
			// copy of tbl.Desc that we'll be asserting is the current version when we
			// CPut below.
			//
			// Set FK constraints to unvalidated before publishing the table imported
			// into.
			newTableDesc.OutboundFKs = make([]descpb.ForeignKeyConstraint, len(newTableDesc.OutboundFKs))
			copy(newTableDesc.OutboundFKs, tbl.Desc.OutboundFKs)
			for i := range newTableDesc.OutboundFKs {
				newTableDesc.OutboundFKs[i].Validity = descpb.ConstraintValidity_Unvalidated
			}

			// Set CHECK constraints to unvalidated before publishing the table imported into.
			for _, c := range newTableDesc.CheckConstraints() {
				// We only "unvalidate" constraints that are not hash-sharded column
				// check constraints.
				if !c.IsHashShardingConstraint() {
					c.CheckDesc().Validity = descpb.ConstraintValidity_Unvalidated
				}
			}
			newTableDesc.FinalizeImport()
			// TODO(dt): re-validate any FKs?
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, newTableDesc, b,
			); err != nil {
				return errors.Wrapf(err, "publishing table %d", newTableDesc.ID)
			}

			// This is needed for the post import validation of the table. Otherwise
			// the table will be in the OFFLINE state and the internal query via
			// table index will fail.
			// tbl.Desc.State = descpb.DescriptorState_PUBLIC
		}
		if err := txn.KV().Run(ctx, b); err != nil {
			return errors.Wrap(err, "publishing tables")
		}

		// Update job record to mark tables published state as complete.
		details.TablesPublished = true
		err := r.job.WithTxn(txn).SetDetails(ctx, details)
		if err != nil {
			return errors.Wrap(err, "updating job details after publishing tables")
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Initiate a run of CREATE STATISTICS. We don't know the actual number of
	// rows affected per table, so we use a large number because we want to make
	// sure that stats always get created/refreshed here.
	for i := range details.Tables {
		desc := tabledesc.NewBuilder(details.Tables[i].Desc).BuildImmutableTable()
		execCfg.StatsRefresher.NotifyMutation(desc, math.MaxInt32 /* rowsAffected */)
	}

	return nil
}

// checkVirtualConstraints checks constraints that are enforced via runtime
// checks, such as uniqueness checks that are not directly backed by an index.
func (r *importResumer) checkVirtualConstraints(
	ctx context.Context, execCfg *sql.ExecutorConfig, job *jobs.Job, user username.SQLUsername,
) error {
	for _, tbl := range job.Details().(jobspb.ImportDetails).Tables {
		desc := tabledesc.NewBuilder(tbl.Desc).BuildExistingMutableTable()
		desc.SetPublic()

		if sql.HasVirtualUniqueConstraints(desc) {
			status := jobs.StatusMessage(fmt.Sprintf("re-validating %s", desc.GetName()))
			if err := job.NoTxn().UpdateStatusMessage(ctx, status); err != nil {
				return errors.Wrapf(err, "failed to update running status of job %d", errors.Safe(job.ID()))
			}
		}

		if err := execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			txn.Descriptors().AddSyntheticDescriptor(desc)
			return sql.RevalidateUniqueConstraintsInTable(ctx, txn, user, desc)
		}); err != nil {
			return err
		}
	}
	return nil
}

// checkForUDTModification checks whether any of the types referenced by the
// table being imported into have been modified incompatibly since they were
// read during import planning. If they have, it may be unsafe to continue
// with the import since we could be ingesting data that is no longer valid
// for the type.
//
// Egs: Renaming an enum value mid import could result in the import ingesting a
// value that is no longer valid.
//
// TODO(SQL Schema): This method might be unnecessarily aggressive in failing
// the import. The semantics of what concurrent type changes are/are not safe
// during an IMPORT still need to be ironed out. Once they are, we can make this
// method more conservative in what it uses to deem a type change dangerous. At
// the time of writing, changes to privileges and back-references are supported.
// Additions of new values could be supported but are not. Renaming of logical
// enum values or removal of enum values will need to forever remain
// incompatible.
func (r *importResumer) checkForUDTModification(
	ctx context.Context, execCfg *sql.ExecutorConfig,
) error {
	details := r.job.Details().(jobspb.ImportDetails)
	if details.Types == nil {
		return nil
	}
	// typeDescsAreEquivalent returns true if a and b are the same types save
	// for the version, modification time, privileges, or the set of referencing
	// descriptors.
	typeDescsAreEquivalent := func(a, b *descpb.TypeDescriptor) (bool, error) {
		clearIgnoredFields := func(d *descpb.TypeDescriptor) *descpb.TypeDescriptor {
			d = protoutil.Clone(d).(*descpb.TypeDescriptor)
			d.ModificationTime = hlc.Timestamp{}
			d.Privileges = nil
			d.Version = 0
			d.ReferencingDescriptorIDs = nil
			return d
		}
		aData, err := protoutil.Marshal(clearIgnoredFields(a))
		if err != nil {
			return false, err
		}
		bData, err := protoutil.Marshal(clearIgnoredFields(b))
		if err != nil {
			return false, err
		}
		return bytes.Equal(aData, bData), nil
	}
	// checkTypeIsEquivalent checks that the current version of the type as
	// retrieved from the collection is equivalent to the previously saved
	// type descriptor used by the import.
	checkTypeIsEquivalent := func(
		ctx context.Context, txn *kv.Txn, col *descs.Collection,
		savedTypeDesc *descpb.TypeDescriptor,
	) error {
		typeDesc, err := col.ByIDWithoutLeased(txn).Get().Type(ctx, savedTypeDesc.GetID())
		if err != nil {
			return errors.Wrap(err, "resolving type descriptor when checking version mismatch")
		}
		if typeDesc.GetModificationTime() == savedTypeDesc.GetModificationTime() {
			return nil
		}
		equivalent, err := typeDescsAreEquivalent(typeDesc.TypeDesc(), savedTypeDesc)
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(
				err, "failed to check for type descriptor equivalence for type %q (%d)",
				typeDesc.GetName(), typeDesc.GetID())
		}
		if equivalent {
			return nil
		}
		return errors.WithHint(
			errors.Newf(
				"type descriptor %q (%d) has been modified, potentially incompatibly,"+
					" since import planning; aborting to avoid possible corruption",
				typeDesc.GetName(), typeDesc.GetID(),
			),
			"retrying the IMPORT operation may succeed if the operation concurrently"+
				" modifying the descriptor does not reoccur during the retry attempt",
		)
	}
	checkTypesAreEquivalent := func(
		ctx context.Context, txn isql.Txn, col *descs.Collection,
	) error {
		for _, savedTypeDesc := range details.Types {
			if err := checkTypeIsEquivalent(
				ctx, txn.KV(), col, savedTypeDesc.Desc,
			); err != nil {
				return err
			}
		}
		return nil
	}
	return sql.DescsTxn(ctx, execCfg, checkTypesAreEquivalent)
}

var retryDuration = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"bulkio.import.retry_duration",
	"duration during which the IMPORT can be retried in face of non-permanent errors",
	time.Minute*2,
	settings.PositiveDuration,
)

var importEpochs = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"bulkio.import.write_import_epoch.enabled",
	"controls whether IMPORT will write ImportEpoch's to descriptors",
	true,
)

func getFractionCompleted(job *jobs.Job) float64 {
	p := job.Progress()
	return float64(p.GetFractionCompleted())
}

func ingestWithRetry(
	ctx context.Context,
	execCtx sql.JobExecContext,
	job *jobs.Job,
	tables map[string]*execinfrapb.ReadImportDataSpec_ImportTable,
	typeDescs []*descpb.TypeDescriptor,
	from []string,
	format roachpb.IOFileFormat,
	walltime int64,
	testingKnobs importTestingKnobs,
	procsPerNode int,
	initialSplitsPerProc int,
) (kvpb.BulkOpSummary, error) {
	ctx, sp := tracing.ChildSpan(ctx, "importer.ingestWithRetry")
	defer sp.Finish()

	// Note that we don't specify MaxRetries since we use time-based limit
	// in the loop below.
	retryOpts := retry.Options{
		// Don't retry too aggressively.
		MaxBackoff: 15 * time.Second,
	}

	// We want to retry an import if there are transient failures (i.e. worker
	// nodes dying), so if we receive a retryable error, re-plan and retry the
	// import.
	var res kvpb.BulkOpSummary
	var err error
	// State to decide when to exit the retry loop.
	lastProgressChange, lastProgress := timeutil.Now(), getFractionCompleted(job)
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		for {
			res, err = distImport(
				ctx, execCtx, job, tables, typeDescs, from, format, walltime, testingKnobs, procsPerNode, initialSplitsPerProc,
			)
			// If we got a re-planning error, then do at least one more attempt
			// regardless of the retry duration.
			if err == nil || !errors.Is(err, sql.ErrPlanChanged) {
				break
			}
		}
		if err == nil {
			break
		}

		if errors.HasType(err, &kvpb.InsufficientSpaceError{}) {
			return res, jobs.MarkPauseRequestError(errors.UnwrapAll(err))
		}

		if joberror.IsPermanentBulkJobError(err) {
			return res, err
		}

		// If we are draining, it is unlikely we can start a new DistSQL
		// flow. Exit with a retryable error so that another node can
		// pick up the job.
		if execCtx.ExecCfg().JobRegistry.IsDraining() {
			return res, jobs.MarkAsRetryJobError(errors.Wrapf(err, "job encountered retryable error on draining node"))
		}

		// Re-load the job in order to update our progress object, which
		// may have been updated since the flow started.
		reloadedJob, reloadErr := execCtx.ExecCfg().JobRegistry.LoadClaimedJob(ctx, job.ID())
		if reloadErr != nil {
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			log.Dev.Warningf(ctx, "IMPORT job %d could not reload job progress when retrying: %+v",
				job.ID(), reloadErr)
		} else {
			job = reloadedJob
			// If we made decent progress with the IMPORT, reset the last
			// progress state.
			curProgress := getFractionCompleted(job)
			if madeProgress := curProgress - lastProgress; madeProgress >= 0.01 {
				log.Dev.Infof(ctx, "import made %d%% progress, resetting retry duration", int(math.Round(100*madeProgress)))
				lastProgress = curProgress
				lastProgressChange = timeutil.Now()
				r.Reset()
			}
		}

		maxRetryDuration := retryDuration.Get(&execCtx.ExecCfg().Settings.SV)
		if timeutil.Since(lastProgressChange) > maxRetryDuration {
			log.Dev.Warningf(ctx, "encountered retryable error but exceeded retry duration, stopping: %+v", err)
			break
		} else {
			log.Dev.Warningf(ctx, "encountered retryable error: %+v", err)
		}
	}

	// We have exhausted retries, but we have not seen a "PermanentBulkJobError" so
	// it is possible that this is a transient error that is taking longer than
	// our configured retry to go away.
	//
	// Let's pause the job instead of failing it so that the user can decide
	// whether to resume it or cancel it.
	if err != nil {
		return res, jobs.MarkPauseRequestError(errors.Wrap(err, "exhausted retries"))
	}
	return res, nil
}

// emitImportJobEvent emits an import job event to the event log.
func emitImportJobEvent(
	ctx context.Context, p sql.JobExecContext, status jobs.State, job *jobs.Job,
) {
	var importEvent eventpb.Import
	if err := p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return sql.LogEventForJobs(ctx, p.ExecCfg(), txn, &importEvent, int64(job.ID()),
			job.Payload(), p.User(), status)
	}); err != nil {
		log.Dev.Warningf(ctx, "failed to log event: %v", err)
	}
}

func writeNonDropDatabaseChange(
	ctx context.Context,
	desc *dbdesc.Mutable,
	txn isql.Txn,
	descsCol *descs.Collection,
	p sql.JobExecContext,
	jobDesc string,
) ([]jobspb.JobID, error) {
	var job *jobs.Job
	var err error
	if job, err = createNonDropDatabaseChangeJob(ctx, p.User(), desc.ID, jobDesc, p, txn); err != nil {
		return nil, err
	}

	queuedJob := []jobspb.JobID{job.ID()}
	b := txn.KV().NewBatch()
	err = descsCol.WriteDescToBatch(
		ctx,
		p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		desc,
		b,
	)
	if err != nil {
		return nil, err
	}
	return queuedJob, txn.KV().Run(ctx, b)
}

func createNonDropDatabaseChangeJob(
	ctx context.Context,
	user username.SQLUsername,
	databaseID descpb.ID,
	jobDesc string,
	p sql.JobExecContext,
	txn isql.Txn,
) (*jobs.Job, error) {
	jobRecord := jobs.Record{
		Description: jobDesc,
		Username:    user,
		Details: jobspb.SchemaChangeDetails{
			DescID:        databaseID,
			FormatVersion: jobspb.DatabaseJobFormatVersion,
		},
		Progress:      jobspb.SchemaChangeProgress{},
		NonCancelable: true,
	}

	jobID := p.ExecCfg().JobRegistry.MakeJobID()
	return p.ExecCfg().JobRegistry.CreateJobWithTxn(
		ctx,
		jobRecord,
		jobID,
		txn,
	)
}

// OnFailOrCancel is part of the jobs.Resumer interface. Removes data that has
// been committed from a import that has failed or been canceled. It does this
// by adding the table descriptors in DROP state, which causes the schema change
// stuff to delete the keys in the background.
func (r *importResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	p := execCtx.(sql.JobExecContext)

	// Emit to the event log that the job has started reverting.
	emitImportJobEvent(ctx, p, jobs.StateReverting, r.job)

	// TODO(sql-exp): increase telemetry count for import.total.failed and
	// import.duration-sec.failed.
	details := r.job.Details().(jobspb.ImportDetails)
	logutil.LogJobCompletion(ctx, importJobRecoveryEventType, r.job.ID(), false, jobErr, r.res.Rows)

	addToFileFormatTelemetry(details.Format.Format.String(), "failed")

	// If the import completed preparation and started writing, verify it has
	// stopped writing before proceeding to revert it.
	if details.PrepareComplete {
		log.Dev.Infof(ctx, "need to verify that no nodes are still importing since job had started writing...")
		const maxWait = time.Minute * 5
		if err := ingeststopped.WaitForNoIngestingNodes(ctx, p, r.job, maxWait); err != nil {
			log.Dev.Errorf(ctx, "unable to verify that attempted IMPORT job %d had stopped writing before reverting after %s: %v", r.job.ID(), maxWait, err)
		} else {
			log.Dev.Infof(ctx, "verified no nodes still ingesting on behalf of job %d", r.job.ID())
		}

	}

	cfg := execCtx.(sql.JobExecContext).ExecCfg()
	var jobsToRunAfterTxnCommit []jobspb.JobID
	if err := sql.DescsTxn(ctx, cfg, func(
		ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
	) error {
		if err := r.dropTables(ctx, txn, descsCol, cfg); err != nil {
			log.Dev.Errorf(ctx, "drop tables failed: %s", err.Error())
			return err
		}

		// Drop all the schemas which may have been created during a bundle import.
		// These schemas should now be empty as all the tables in them would be new
		// tables created during the import, and therefore dropped by the above
		// dropTables method. This allows us to avoid "collecting" objects in the
		// schema before dropping the descriptor.
		var err error
		jobsToRunAfterTxnCommit, err = r.dropSchemas(ctx, txn, descsCol, cfg, p)
		return err
	}); err != nil {
		return err
	}

	// Run any jobs which might have been queued when dropping the schemas.
	// This would be a job to drop all the schemas, and a job to update the parent
	// database descriptor.
	if len(jobsToRunAfterTxnCommit) != 0 {
		if err := p.ExecCfg().JobRegistry.Run(ctx, jobsToRunAfterTxnCommit); err != nil {
			return errors.Wrap(err, "failed to run jobs that drop the imported schemas")
		}
	}

	// Emit to the event log that the job has completed reverting.
	emitImportJobEvent(ctx, p, jobs.StateFailed, r.job)

	return nil
}

// CollectProfile is a part of the Resumer interface.
func (r *importResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

// dropTables implements the OnFailOrCancel logic.
func (r *importResumer) dropTables(
	ctx context.Context, txn isql.Txn, descsCol *descs.Collection, execCfg *sql.ExecutorConfig,
) error {
	details := r.job.Details().(jobspb.ImportDetails)

	// If the prepare step of the import job was not completed then the
	// descriptors do not need to be rolled back as the txn updating them never
	// completed.
	if !details.PrepareComplete {
		return nil
	}

	var tableWasEmpty bool
	var intoTable catalog.TableDescriptor
	for _, tbl := range details.Tables {
		desc, err := descsCol.MutableByID(txn.KV()).Table(ctx, tbl.Desc.ID)
		if err != nil {
			return err
		}
		intoTable = desc.ImmutableCopy().(catalog.TableDescriptor)
		tableWasEmpty = tbl.WasEmpty
	}
	// Clear table data from a rolling back IMPORT INTO cmd
	//
	// The walltime can be 0 if there is a failure between publishing the tables
	// as OFFLINE and then choosing a ingestion timestamp. This might happen
	// while waiting for the descriptor version to propagate across the cluster
	// for example.
	//
	// In this case, we don't want to rollback the data since data ingestion has
	// not yet begun (since we have not chosen a timestamp at which to ingest.)
	if details.Walltime != 0 && !tableWasEmpty {
		// NB: if a revert fails it will abort the rest of this failure txn, which is
		// also what brings tables back online. We _could_ change the error handling
		// or just move the revert into Resume()'s error return path, however it isn't
		// clear that just bringing a table back online with partially imported data
		// that may or may not be partially reverted is actually a good idea. It seems
		// better to do the revert here so that the table comes back if and only if,
		// it was rolled back to its pre-IMPORT state, and instead provide a manual
		// admin knob (e.g. ALTER TABLE REVERT TO SYSTEM TIME) if anything goes wrong.
		ts := hlc.Timestamp{WallTime: details.Walltime}.Prev()
		predicates := kvpb.DeleteRangePredicates{StartTime: ts}
		if err := sql.DeleteTableWithPredicate(
			ctx,
			execCfg.DB,
			execCfg.Codec,
			&execCfg.Settings.SV,
			execCfg.DistSender,
			intoTable.GetID(),
			predicates, sql.RevertTableDefaultBatchSize); err != nil {
			return errors.Wrap(err, "rolling back IMPORT INTO in non empty table via DeleteRange")
		}
	} else if tableWasEmpty {
		if err := gcjob.DeleteAllTableData(
			ctx, execCfg.DB, execCfg.DistSender, execCfg.Codec, intoTable,
		); err != nil {
			return errors.Wrap(err, "rolling back IMPORT INTO in empty table via DeleteRange")
		}
	}

	// Bring the IMPORT INTO table back online
	b := txn.KV().NewBatch()
	intoDesc, err := descsCol.MutableByID(txn.KV()).Table(ctx, intoTable.GetID())
	if err != nil {
		return err
	}
	intoDesc.SetPublic()
	intoDesc.FinalizeImport()
	const kvTrace = false
	if err := descsCol.WriteDescToBatch(ctx, kvTrace, intoDesc, b); err != nil {
		return err
	}
	return errors.Wrap(txn.KV().Run(ctx, b), "putting IMPORT INTO table back online")
}

func (r *importResumer) dropSchemas(
	ctx context.Context,
	txn isql.Txn,
	descsCol *descs.Collection,
	execCfg *sql.ExecutorConfig,
	p sql.JobExecContext,
) ([]jobspb.JobID, error) {
	details := r.job.Details().(jobspb.ImportDetails)

	// If the prepare step of the import job was not completed then the
	// descriptors do not need to be rolled back as the txn updating them never
	// completed.
	if !details.PrepareComplete {
		return nil, nil
	}

	// Resolve the database descriptor.
	desc, err := descsCol.MutableByID(txn.KV()).Desc(ctx, details.ParentID)
	if err != nil {
		return nil, err
	}

	dbDesc, ok := desc.(*dbdesc.Mutable)
	if !ok {
		return nil, errors.Newf("expected ID %d to refer to the database being imported into",
			details.ParentID)
	}

	// Write out the change to the database. This only creates a job record to be
	// run after the txn commits.
	queuedJob, err := writeNonDropDatabaseChange(ctx, dbDesc, txn, descsCol, p, "")
	if err != nil {
		return nil, err
	}

	return queuedJob, nil
}

// ReportResults implements JobResultsReporter interface.
func (r *importResumer) ReportResults(ctx context.Context, resultsCh chan<- tree.Datums) error {
	select {
	case resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(r.job.ID())),
		tree.NewDString(string(jobs.StateSucceeded)),
		tree.NewDFloat(tree.DFloat(1.0)),
		tree.NewDInt(tree.DInt(r.res.Rows)),
		tree.NewDInt(tree.DInt(r.res.IndexEntries)),
		tree.NewDInt(tree.DInt(r.res.DataSize)),
	}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeImport,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &importResumer{
				job:      job,
				settings: settings,
			}
		},
		jobs.UsesTenantCostControl,
	)
}
