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
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
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
	"github.com/cockroachdb/cockroach/pkg/sql/bulkutil"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/inspect"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/besteffort"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logutil"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const importJobRecoveryEventType eventpb.RecoveryEventType = "import_job"

type importTestingKnobs struct {
	afterImport            func(summary roachpb.RowCount) error
	beforeRunDSP           func() error
	onSetupFinish          func(flowinfra.Flow)
	alwaysFlushJobProgress bool
	beforeInitialRowCount  func() error
	// expectedRowCountOffset is added to the expected row count passed to the
	// inspect row count check.
	expectedRowCountOffset int64
}

type importResumer struct {
	job      *jobs.Job
	settings *cluster.Settings
	res      roachpb.RowCount

	// inspectJobID is the ID of the background INSPECT job triggered for row
	// count validation, if any. Zero means no INSPECT job was triggered.
	inspectJobID jobspb.JobID

	testingKnobs importTestingKnobs
}

func (r *importResumer) TestingSetAfterImportKnob(fn func(summary roachpb.RowCount) error) {
	r.testingKnobs.afterImport = fn
}

func (r *importResumer) TestingSetBeforeInitialRowCountKnob(fn func() error) {
	r.testingKnobs.beforeInitialRowCount = fn
}

func (r *importResumer) TestingSetExpectedRowCountOffset(offset int64) {
	r.testingKnobs.expectedRowCountOffset = offset
}

func (r *importResumer) TestingSetAlwaysFlushJobProgress() {
	r.testingKnobs.alwaysFlushJobProgress = true
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

// ImportRowCountValidationMode represents the mode for row count validation after import.
type ImportRowCountValidationMode int64

const (
	// ImportRowCountValidationOff disables row count validation after import.
	ImportRowCountValidationOff ImportRowCountValidationMode = iota
	// ImportRowCountValidationAsync enables asynchronous row count validation after import.
	ImportRowCountValidationAsync
	// ImportRowCountValidationSync enables synchronous (blocking) row count validation after import.
	ImportRowCountValidationSync
)

// importRowCountValidationMetamorphicValue determines the default value for
// importRowCountValidation in metamorphic test builds. It randomly selects
// between "async" and "sync" modes to increase test coverage.
var importRowCountValidationMetamorphicValue = metamorphic.ConstantWithTestChoice(
	"import-row-count-validation",
	"async", // background validation (default)
	"sync",  // blocking validation for tests
)

// Note: the internal key retains "unsafe" for backward compatibility, but the
// setting is no longer unsafe. The public name is set via WithName below.
var importRowCountValidation = settings.RegisterEnumSetting(
	settings.ApplicationLevel,
	"bulkio.import.row_count_validation.unsafe.mode",
	"controls validation of imported data via INSPECT jobs. "+
		"Options: 'off' (no validation), 'async' (background validation), "+
		"'sync' (blocking validation). "+
		"If disabled, IMPORT will not perform a post-import row count check.",
	importRowCountValidationMetamorphicValue,
	map[ImportRowCountValidationMode]string{
		ImportRowCountValidationOff:   "off",
		ImportRowCountValidationAsync: "async",
		ImportRowCountValidationSync:  "sync",
	},
	settings.WithName("bulkio.import.row_count_validation.mode"),
	settings.WithPublic,
	settings.WithRetiredName("bulkio.import.row_count_validation.unsafe.enabled"),
)

func getTable(details jobspb.ImportDetails) (jobspb.ImportDetails_Table, error) {
	if len(details.Tables) > 0 {
		if len(details.Tables) != 1 {
			return jobspb.ImportDetails_Table{}, errors.AssertionFailedf("expected exactly one table, got %d", len(details.Tables))
		}
		return details.Tables[0], nil
	}
	return details.Table, nil
}

// Resume is part of the jobs.Resumer interface.
func (r *importResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)

	details := r.job.Details().(jobspb.ImportDetails)
	files := details.URIs
	format := details.Format

	updateDetails := func(txn isql.Txn, details jobspb.ImportDetails) error {
		return r.job.WithTxn(txn).Update(ctx, func(
			txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
		) error {
			pl := md.Payload
			*pl.GetImport() = details

			// Update the set of descriptors for later observability.
			// TODO(ajwerner): Do we need this idempotence test?
			prev := md.Payload.DescriptorIDs
			if prev == nil {
				pl.DescriptorIDs = []descpb.ID{details.Table.Desc.ID}
			}
			ju.UpdatePayload(pl)
			return nil
		})
	}

	// Skip prepare stage on job resumption, if it has already been completed.
	if !details.PrepareComplete {
		if err := sql.DescsTxn(ctx, p.ExecCfg(), func(
			ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
		) error {
			preparedDetails, err := r.prepareTableForIngestion(ctx, p, details, txn.KV(), descsCol)
			if err != nil {
				return err
			}

			// Telemetry for multi-region.
			table, err := getTable(details)
			if err != nil {
				return err
			}
			dbDesc, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, table.Desc.GetParentID())
			if err != nil {
				return err
			}
			if dbDesc.IsMultiRegion() {
				telemetry.Inc(sqltelemetry.ImportIntoMultiRegionDatabaseCounter)
			}

			// Update the job details now that the schemas and table descs have
			// been "prepared".
			return updateDetails(txn, preparedDetails)
		}); err != nil {
			return err
		}

		// Re-initialize details after prepare step.
		details = r.job.Details().(jobspb.ImportDetails)
	}

	if r.testingKnobs.beforeInitialRowCount != nil {
		if err := r.testingKnobs.beforeInitialRowCount(); err != nil {
			return err
		}
	}

	if details.Table.InitialRowCount == 0 && details.Walltime == 0 {
		// Check if the table being imported into is starting empty, in which case
		// we can cheaply clear-range instead of DeleteRange to cleanup.
		//
		// Run the count after the table has been taken offline.
		//
		// The count is low-cost for empty tables and otherwise the expensive
		// full scan is only run the one time.
		//
		// We also check that the walltime has not been set yet. The walltime is
		// set later in this function, after the count. If the walltime is
		// already set, the count has already been performed in a previous
		// attempt. Without this check, resuming an import into an empty table
		// would re-count and include the already-ingested import data in the
		// initial row count.
		if err := p.ExecCfg().InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			rowCountDetails, err := r.detailsWithInitialRowCount(ctx, txn, details)
			if err != nil {
				return err
			}

			return updateDetails(txn, rowCountDetails)
		}); err != nil {
			return err
		}

		// Re-initialize details after the row count update.
		details = r.job.Details().(jobspb.ImportDetails)
		besteffort.Warning(ctx, "import-event", func(ctx context.Context) error {
			return emitImportJobEvent(ctx, p, jobs.StateRunning, r.job)
		})
	}

	table, err := getTable(details)
	if err != nil {
		return err
	}
	importTable := &execinfrapb.ReadImportDataSpec_ImportTable{
		Desc:       table.Desc,
		TargetCols: table.TargetCols,
	}

	typeDescs := make([]*descpb.TypeDescriptor, len(details.Types))
	for i, t := range details.Types {
		typeDescs[i] = t.Desc
	}

	// If details.Walltime is still 0, then it was not set during
	// `prepareTableForIngestion`.
	//
	// Since we're importing into an existing table, we must wait for all nodes
	// to see the same version of the updated table descriptor, after which we
	// shall choose a ts to import from.
	if details.Walltime == 0 {
		// Now that we know that the table is offline, pick a walltime at which we
		// will write.
		details.Walltime = p.ExecCfg().Clock.Now().WallTime

		// Update the descriptor in the job record and in the database
		details.Table.Desc.ImportStartWallTime = details.Walltime
		details.Tables[0].Desc.ImportStartWallTime = details.Walltime

		if err := bindTableDescImportProperties(ctx, p, table.Desc.ID, details.Walltime); err != nil {
			return err
		}

		if err := r.job.NoTxn().SetDetails(ctx, details); err != nil {
			return err
		}
	}

	procsPerNode := int(processorsPerNode.Get(&p.ExecCfg().Settings.SV))
	initialSplitsPerProc := int(initialSplitsPerProcessor.Get(&p.ExecCfg().Settings.SV))

	// Capture the cumulative imported row count from previous runs before
	// starting the current ingest. The progress summary is overwritten during
	// ingest, so we must read it beforehand. This is needed because r.res.Rows
	// only reflects the current run's ingested rows.
	pkID := kvpb.BulkOpSummaryID(uint64(table.Desc.ID), uint64(table.Desc.PrimaryIndex.ID))
	var previouslyImportedRows int64
	{
		prog := r.job.Progress()
		if importProgress := prog.GetImport(); importProgress != nil {
			previouslyImportedRows = importProgress.Summary.EntryCounts[pkID]
		}
	}

	res, err := ingestWithRetry(
		ctx, p, r.job, importTable, typeDescs, files, format, details.Walltime,
		r.testingKnobs, procsPerNode, initialSplitsPerProc,
	)
	if err != nil {
		return err
	}

	r.res.DataSize = res.DataSize
	for id, count := range res.EntryCounts {
		if id == pkID {
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

	setPublicTimestamp, err := r.publishTable(ctx, p.ExecCfg(), res)
	if err != nil {
		return err
	}

	validationMode := importRowCountValidation.Get(&p.ExecCfg().Settings.SV)
	switch validationMode {
	case ImportRowCountValidationOff:
	// No validation required.
	case ImportRowCountValidationAsync, ImportRowCountValidationSync:
		table, err := getTable(details)
		if err != nil {
			return err
		}

		var checks []*jobspb.InspectDetails_Check
		var tableName string
		if err := p.ExecCfg().InternalDB.DescsTxn(ctx, func(
			ctx context.Context, txn descs.Txn,
		) error {
			// INSPECT requires the latest descriptor. The one cached in the job is
			// out of date as it has an old table version.
			tblDesc, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, table.Desc.ID)
			if err != nil {
				return err
			}
			tableName = tblDesc.GetName()

			if creationVersion := r.job.Payload().CreationClusterVersion; !details.Table.WasEmpty && creationVersion.Less(clusterversion.V26_2.Version()) {
				log.Eventf(ctx, "skipping row count on table %q: the table was not empty and the job was started in an unsupported version", tableName)

				checks, err = inspect.ChecksForTable(ctx, nil /* p */, tblDesc, nil /* expectedRowCount */)
				return err
			}

			expectedRowCount := uint64(r.res.Rows + previouslyImportedRows + int64(table.InitialRowCount) + r.testingKnobs.expectedRowCountOffset)
			checks, err = inspect.ChecksForTable(ctx, nil /* p */, tblDesc, &expectedRowCount)
			return err
		}); err != nil {
			return err
		}

		if len(checks) > 0 {
			inspectJob, err := inspect.TriggerJob(
				ctx,
				fmt.Sprintf("import-validation-%s", tableName),
				p.ExecCfg(),
				checks,
				setPublicTimestamp,
			)
			if err != nil {
				return errors.Wrapf(err, "failed to trigger inspect for import validation for table %s", tableName)
			}
			r.inspectJobID = inspectJob.ID()
			log.Eventf(ctx, "triggered inspect job %d for import validation for table %s with AOST %s", inspectJob.ID(), tableName, setPublicTimestamp)

			// For sync mode, wait for the inspect job to complete.
			if validationMode == ImportRowCountValidationSync {
				if err := p.ExecCfg().JobRegistry.WaitForJobs(ctx, []jobspb.JobID{inspectJob.ID()}); err != nil {
					return errors.Wrapf(err, "failed to wait for inspect job %d for table %s", inspectJob.ID(), tableName)
				}
				log.Eventf(ctx, "inspect job %d completed for table %s", inspectJob.ID(), tableName)
			}
		}
	}

	besteffort.Warning(ctx, "import-event", func(ctx context.Context) error {
		return emitImportJobEvent(ctx, p, jobs.StateSucceeded, r.job)
	})

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

	// Cleanup temp storage on success
	r.cleanupTempStorage(ctx, p.ExecCfg())

	return nil
}

// detailsWithInitialRowCount checks if the table being imported into is empty
// and return an updated details with the initial row count.
func (r *importResumer) detailsWithInitialRowCount(
	ctx context.Context, txn descs.Txn, details jobspb.ImportDetails,
) (jobspb.ImportDetails, error) {
	rowCountDetails := details

	// Create an untracked copy for synthetic use.
	mut, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, details.Table.Desc.ID)
	if err != nil {
		return jobspb.ImportDetails{}, err
	}
	synthMut := tabledesc.NewBuilder(mut.TableDesc()).BuildExistingMutableTable()
	synthMut.SetPublic()

	query := fmt.Sprintf(`
SELECT
  count(*) AS row_count
FROM [%d AS t]`,
		mut.GetID(),
	)

	rowCount := uint64(0)
	if err := txn.WithSyntheticDescriptors([]catalog.Descriptor{synthMut}, func() error {
		row, err := txn.QueryRowEx(
			ctx,
			"import-initial-row-count",
			txn.KV(),
			sessiondata.InternalExecutorOverride{
				User: username.NodeUserName(),
			},
			query,
		)
		if err != nil {
			return errors.Wrap(err, "counting rows via synthetic descriptor")
		}
		if row == nil {
			return errors.AssertionFailedf("row count query returned no rows")
		}
		if len(row) != 1 {
			return errors.AssertionFailedf("row count query returned unexpected column count: %d", len(row))
		}
		rowCount = uint64(tree.MustBeDInt(row[0]))

		return nil
	}); err != nil {
		return jobspb.ImportDetails{}, err
	}

	rowCountDetails.Table.WasEmpty = rowCount == 0
	rowCountDetails.Tables[0].WasEmpty = rowCount == 0
	rowCountDetails.Table.InitialRowCount = rowCount
	rowCountDetails.Tables[0].InitialRowCount = rowCount

	return rowCountDetails, nil
}

// prepareTableForIngestion prepare the table descriptor for the ingestion
// step of import. The descriptor is in an IMPORTING state (offline) on
// successful completion of this method.
func (r *importResumer) prepareTableForIngestion(
	ctx context.Context,
	p sql.JobExecContext,
	details jobspb.ImportDetails,
	txn *kv.Txn,
	descsCol *descs.Collection,
) (jobspb.ImportDetails, error) {
	importDetails := details
	table, err := getTable(details)
	if err != nil {
		return jobspb.ImportDetails{}, err
	}

	desc := table.Desc
	if len(desc.Mutations) > 0 {
		return jobspb.ImportDetails{}, errors.Errorf("cannot IMPORT INTO a table with schema changes in progress -- try again later (pending mutation %s)", desc.Mutations[0].String())
	}

	// Note that desc is just used to verify that the version matches.
	importing, err := descsCol.MutableByID(txn).Table(ctx, desc.ID)
	if err != nil {
		return jobspb.ImportDetails{}, err
	}
	// Ensure that the version of the table has not been modified since this
	// job was created.
	if got, exp := importing.Version, desc.Version; got != exp {
		return jobspb.ImportDetails{}, errors.Errorf("another operation is currently operating on the table")
	}

	// Take the table offline for import.
	// TODO(dt): audit everywhere we get table descs (leases or otherwise) to
	// ensure that filtering by state handles IMPORTING correctly.

	// Use OfflineForImport which bumps the ImportEpoch.
	importing.OfflineForImport()

	// TODO(dt): de-validate all the FKs.
	if err := descsCol.WriteDesc(
		ctx, false /* kvTrace */, importing, txn,
	); err != nil {
		return jobspb.ImportDetails{}, err
	}

	importDetails.Table = jobspb.ImportDetails_Table{
		Desc:            importing.TableDesc(),
		Name:            table.Name,
		SeqVal:          table.SeqVal,
		WasEmpty:        table.WasEmpty,
		InitialRowCount: table.InitialRowCount,
		TargetCols:      table.TargetCols,
	}
	importDetails.Tables = []jobspb.ImportDetails_Table{importDetails.Table}

	importDetails.PrepareComplete = true

	// We have to wait for all nodes to see the same descriptor version before
	// choosing our Walltime.
	importDetails.Walltime = 0
	return importDetails, nil
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

// publishTable updates the status of the imported table from OFFLINE to PUBLIC.
func (r *importResumer) publishTable(
	ctx context.Context, execCfg *sql.ExecutorConfig, res kvpb.BulkOpSummary,
) (hlc.Timestamp, error) {
	var setPublicTimestamp hlc.Timestamp
	details := r.job.Details().(jobspb.ImportDetails)
	// The table should only be published once.
	if details.TablePublished {
		return setPublicTimestamp, nil
	}
	tbl, err := getTable(details)
	if err != nil {
		return setPublicTimestamp, err
	}

	log.Event(ctx, "making the table imported into live")

	var kvTxn *kv.Txn
	if err := sql.DescsTxn(ctx, execCfg, func(
		ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
	) error {
		kvTxn = txn.KV()
		b := kvTxn.NewBatch()
		newTableDesc, err := descsCol.MutableByID(kvTxn).Table(ctx, tbl.Desc.ID)
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
		if err := kvTxn.Run(ctx, b); err != nil {
			return errors.Wrapf(err, "publishing table %d", newTableDesc.ID)
		}

		// Update job record to mark table published state as complete.
		details.TablePublished = true
		err = r.job.WithTxn(txn).SetDetails(ctx, details)
		if err != nil {
			return errors.Wrap(err, "updating job details after publishing the table")
		}
		return nil
	}); err != nil {
		return setPublicTimestamp, err
	}

	// Try to get the commit timestamp for the transaction that set
	// the table to public. This timestamp will be used to run the
	// INSPECT job for this table, to ensure the INSPECT looks at the
	// snapshot that IMPORT just finished.
	setPublicTimestamp, err = kvTxn.CommitTimestamp()
	if err != nil {
		return setPublicTimestamp, err
	}

	// Initiate a run of CREATE STATISTICS. We don't know the actual number of
	// rows affected per table, so we use a large number because we want to make
	// sure that stats always get created/refreshed here.
	desc := tabledesc.NewBuilder(tbl.Desc).BuildImmutableTable()
	execCfg.StatsRefresher.NotifyMutation(ctx, desc, math.MaxInt32 /* rowsAffected */)

	return setPublicTimestamp, nil
}

// checkVirtualConstraints checks constraints that are enforced via runtime
// checks, such as uniqueness checks that are not directly backed by an index.
func (r *importResumer) checkVirtualConstraints(
	ctx context.Context, execCfg *sql.ExecutorConfig, job *jobs.Job, user username.SQLUsername,
) error {
	tbl, err := getTable(job.Details().(jobspb.ImportDetails))
	if err != nil {
		return err
	}
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

func getFractionCompleted(job *jobs.Job) float64 {
	p := job.Progress()
	return float64(p.GetFractionCompleted())
}

func ingestWithRetry(
	ctx context.Context,
	execCtx sql.JobExecContext,
	job *jobs.Job,
	table *execinfrapb.ReadImportDataSpec_ImportTable,
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
				ctx, execCtx, job, table, typeDescs, from, format, walltime, testingKnobs, procsPerNode, initialSplitsPerProc,
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
) error {
	var importEvent eventpb.Import
	return p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return sql.LogEventForJobs(ctx, p.ExecCfg(), txn, &importEvent, int64(job.ID()),
			job.Payload(), p.User(), status)
	})
}

// OnFailOrCancel is part of the jobs.Resumer interface. Removes data that has
// been committed from a import that has failed or been canceled. It does this
// by adding the table descriptors in DROP state, which causes the schema change
// stuff to delete the keys in the background.
func (r *importResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	p := execCtx.(sql.JobExecContext)
	details := r.job.Details().(jobspb.ImportDetails)

	if details.TablePublished {
		// If the table was published, there is nothing for us to clean up, the
		// descriptor is already online.
		log.Dev.Warningf(ctx, "import job %d failed or canceled after publishing the table, no revert necessary", r.job.ID())
		return nil
	}
	if !details.PrepareComplete {
		besteffort.Warning(ctx, "import-event", func(ctx context.Context) error {
			return emitImportJobEvent(ctx, p, jobs.StateFailed, r.job)
		})
		return nil
	}

	// Emit to the event log that the job has started reverting.
	besteffort.Warning(ctx, "import-event", func(ctx context.Context) error {
		return emitImportJobEvent(ctx, p, jobs.StateReverting, r.job)
	})

	// TODO(sql-exp): increase telemetry count for import.total.failed and
	// import.duration-sec.failed.
	logutil.LogJobCompletion(ctx, importJobRecoveryEventType, r.job.ID(), false, jobErr, r.res.Rows)

	addToFileFormatTelemetry(details.Format.Format.String(), "failed")

	// If the import completed preparation and started writing, verify it has
	// stopped writing before proceeding to revert it.
	besteffort.Error(ctx, "import-wait-for-no-ingest", func(ctx context.Context) error {
		const maxWait = time.Minute * 5
		err := ingeststopped.WaitForNoIngestingNodes(ctx, p, r.job, maxWait)
		return errors.Wrapf(err, "waiting for no nodes ingesting on behalf of job %d", r.job.ID())
	})

	cfg := execCtx.(sql.JobExecContext).ExecCfg()
	tbl, err := getTable(details)
	if err != nil {
		return err
	}

	switch {
	case tbl.WasEmpty:
		err := truncateTable(ctx, cfg, tbl.Desc.ID)
		if err != nil {
			return errors.Wrap(err, "truncating empty table to roll back import")
		}
	case details.Walltime == 0:
		// The walltime can be 0 if there is a failure between publishing the table
		// as OFFLINE and then choosing a ingestion timestamp. This might happen
		// while waiting for the descriptor version to propagate across the cluster
		// for example.
	default:
		err := revertTable(ctx, cfg, tbl.Desc.ID, details.Walltime)
		if err != nil {
			return errors.Wrap(err, "rolling back import to non-empty table")
		}
	}

	err = r.markOnline(ctx, cfg, tbl.Desc.ID)
	if err != nil {
		return errors.Wrap(err, "bringing table back online after import revert")
	}

	besteffort.Warning(ctx, "import-event", func(ctx context.Context) error {
		return emitImportJobEvent(ctx, p, jobs.StateFailed, r.job)
	})

	// Cleanup temp storage on failure/cancellation
	r.cleanupTempStorage(ctx, cfg)

	return nil
}

// cleanupTempStorage best-effort deletes job-scoped temporary files produced by
// distributed merge import. The storage prefixes should omit the "/job/<job-id>/"
// suffix; this helper will scope cleanup to the current job ID automatically.
//
// This should only be called once the job is finishing (successfully or after
// cancellation), to avoid deleting files needed for retry.
func (r *importResumer) cleanupTempStorage(ctx context.Context, execCfg *sql.ExecutorConfig) {
	if execCfg == nil || execCfg.DistSQLSrv == nil || execCfg.DistSQLSrv.ExternalStorageFromURI == nil {
		return
	}
	details := r.job.Details().(jobspb.ImportDetails)
	if !details.UseDistributedMerge {
		return
	}
	progress := r.job.Progress()
	importProgress := progress.GetImport()
	if importProgress == nil {
		return
	}

	prefixes := importProgress.SSTStoragePrefixes
	if len(prefixes) == 0 {
		return
	}

	cleaner := bulkutil.NewBulkJobCleaner(execCfg.DistSQLSrv.ExternalStorageFromURI, username.NodeUserName())
	defer func() {
		if err := cleaner.Close(); err != nil {
			log.Dev.Warningf(ctx, "job %d: closing bulk job cleaner: %v", r.job.ID(), err)
		}
	}()
	if err := cleaner.CleanupJobDirectories(ctx, r.job.ID(), prefixes); err != nil {
		log.Dev.Warningf(ctx, "job %d: cleaning up temporary SST files: %v", r.job.ID(), err)
	}
}

// CollectProfile is a part of the Resumer interface.
func (r *importResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

// truncateTable truncates a table by using the pre-parsed statement API of the
// internal executor. This is used to clean up an empty table during import
// rollback instead of using the GC job.
func truncateTable(ctx context.Context, execCfg *sql.ExecutorConfig, id catid.DescID) error {
	var tableName tree.TableName
	err := sql.DescsTxn(ctx, execCfg, func(ctx context.Context, txn isql.Txn, descsCol *descs.Collection) error {
		table, err := descsCol.ByIDWithLeased(txn.KV()).Get().Table(ctx, id)
		if err != nil {
			return errors.Wrap(err, "looking up table descriptor for truncate")
		}

		objName, err := descs.GetObjectName(ctx, txn.KV(), descsCol, table)
		if err != nil {
			return errors.Wrap(err, "getting fully qualified table name for truncate")
		}
		tableName = *objName.(*tree.TableName)
		return nil
	})
	if err != nil {
		return err
	}

	override := sessiondata.NodeUserSessionDataOverride
	override.MultiOverride = "use_declarative_schema_changer=unsafe_always"
	_, err = execCfg.InternalDB.Executor().ExecParsed(
		ctx,
		redact.RedactableString("import-truncate-table"),
		nil,
		override,
		statements.Statement[tree.Statement]{
			AST: &tree.Truncate{
				Tables:         tree.TableNames{tableName},
				DropBehavior:   tree.DropDefault,
				ImportRollback: true,
			},
			SQL: fmt.Sprintf("TRUNCATE TABLE %s", tableName.String()),
		},
	)

	return err
}

// revertTable implements the OnFailOrCancel logic.
func revertTable(
	ctx context.Context, execCfg *sql.ExecutorConfig, id catid.DescID, writeTime int64,
) error {
	ts := hlc.Timestamp{WallTime: writeTime}.Prev()
	predicates := kvpb.DeleteRangePredicates{StartTime: ts}
	err := sql.DeleteTableWithPredicate(
		ctx,
		execCfg.DB,
		execCfg.Codec,
		execCfg.Settings,
		execCfg.DistSender,
		id,
		predicates, sql.RevertTableDefaultBatchSize)
	return err
}

func (r *importResumer) markOnline(
	ctx context.Context, cfg *sql.ExecutorConfig, id catid.DescID,
) error {
	return sql.DescsTxn(ctx, cfg, func(ctx context.Context, txn isql.Txn, descsCol *descs.Collection) error {
		// Bring the IMPORT INTO table back online
		b := txn.KV().NewBatch()
		intoDesc, err := descsCol.MutableByID(txn.KV()).Table(ctx, id)
		if err != nil {
			return err
		}
		intoDesc.SetPublic()
		intoDesc.FinalizeImport()
		const kvTrace = false
		if err := descsCol.WriteDescToBatch(ctx, kvTrace, intoDesc, b); err != nil {
			return err
		}

		err = txn.KV().Run(ctx, b)
		if err != nil {
			return errors.Wrap(err, "bringing IMPORT INTO table back online")
		}

		err = r.job.WithTxn(txn).Update(ctx, func(
			txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
		) error {
			// Mark the table as published to avoid running cleanup again.
			details := md.Payload.GetImport()
			details.TablePublished = true
			ju.UpdatePayload(md.Payload)
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "updating job to mark table as published during cleanup")
		}

		return nil
	})
}

// ReportResults implements JobResultsReporter interface.
func (r *importResumer) ReportResults(ctx context.Context, resultsCh chan<- tree.Datums) error {
	var inspectJobIDDatum tree.Datum = tree.DNull
	if r.inspectJobID != 0 {
		inspectJobIDDatum = tree.NewDInt(tree.DInt(r.inspectJobID))
	}
	select {
	case resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(r.job.ID())),
		tree.NewDString(string(jobs.StateSucceeded)),
		tree.NewDFloat(tree.DFloat(1.0)),
		tree.NewDInt(tree.DInt(r.res.Rows)),
		tree.NewDInt(tree.DInt(r.res.IndexEntries)),
		tree.NewDInt(tree.DInt(r.res.DataSize)),
		inspectJobIDDatum,
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
