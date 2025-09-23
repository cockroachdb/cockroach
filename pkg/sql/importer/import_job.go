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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
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

// TODO(janexing): tune the default to True when INSPECT is merged in stable release.
var importRowCountValidation = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"bulkio.import.row_count_validation.unsafe.enabled",
	"enables asynchronous validation of imported data via INSPECT "+
		"jobs. When enabled, an INSPECT job runs after import completion to "+
		"detect potential data corruption. Disabling this setting may result "+
		"in undetected data corruption if the import process fails.",
	false,
	settings.WithUnsafe,
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
			return r.job.WithTxn(txn).Update(ctx, func(
				txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
			) error {
				pl := md.Payload
				*pl.GetImport() = preparedDetails

				// Update the set of descriptors for later observability.
				// TODO(ajwerner): Do we need this idempotence test?
				prev := md.Payload.DescriptorIDs
				if prev == nil {
					pl.DescriptorIDs = []descpb.ID{table.Desc.GetID()}
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

		// Check if the table being imported into is starting empty, in which case
		// we can cheaply clear-range instead of revert-range to cleanup (or if the
		// cluster has finalized to 22.1, use DeleteRange without predicate
		// filtering).
		tblDesc := tabledesc.NewBuilder(table.Desc).BuildImmutableTable()
		tblSpan := tblDesc.TableSpan(p.ExecCfg().Codec)
		res, err := p.ExecCfg().DB.Scan(ctx, tblSpan.Key, tblSpan.EndKey, 1 /* maxRows */)
		if err != nil {
			return errors.Wrap(err, "checking if existing table is empty")
		}
		details.Table.WasEmpty = len(res) == 0
		details.Tables[0].WasEmpty = len(res) == 0

		// Update the descriptor in the job record and in the database
		details.Table.Desc.ImportStartWallTime = details.Walltime
		details.Tables[0].Desc.ImportStartWallTime = details.Walltime

		if err := bindTableDescImportProperties(ctx, p, tblDesc.GetID(), details.Walltime); err != nil {
			return err
		}

		if err := r.job.NoTxn().SetDetails(ctx, details); err != nil {
			return err
		}
	}

	procsPerNode := int(processorsPerNode.Get(&p.ExecCfg().Settings.SV))
	initialSplitsPerProc := int(initialSplitsPerProcessor.Get(&p.ExecCfg().Settings.SV))

	res, err := ingestWithRetry(
		ctx, p, r.job, importTable, typeDescs, files, format, details.Walltime,
		r.testingKnobs, procsPerNode, initialSplitsPerProc,
	)
	if err != nil {
		return err
	}

	pkID := kvpb.BulkOpSummaryID(uint64(table.Desc.ID), uint64(table.Desc.PrimaryIndex.ID))
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

	if importRowCountValidation.Get(&p.ExecCfg().Settings.SV) {
		table, err := getTable(details)
		if err != nil {
			return err
		}
		tblDesc := tabledesc.NewBuilder(table.Desc).BuildImmutableTable()
		if len(tblDesc.PublicNonPrimaryIndexes()) > 0 {
			_, err := sql.TriggerInspectJob(
				ctx,
				fmt.Sprintf("import-validation-%s", tblDesc.GetName()),
				p.ExecCfg(),
				tblDesc,
				setPublicTimestamp,
			)
			if err != nil {
				return errors.Wrapf(err, "failed to trigger inspect for import validation for table %s", tblDesc.GetName())
			}
			log.Eventf(ctx, "triggered inspect job for import validation for table %s with AOST %s", tblDesc.GetName(), setPublicTimestamp)
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

	// We only use the new OfflineForImport on 24.1, which bumps
	// the ImportEpoch, if we are completely on 24.1.
	if importEpochs.Get(&p.ExecCfg().Settings.SV) {
		importing.OfflineForImport()
	} else {
		importing.SetOffline(tabledesc.OfflineReasonImporting)
	}

	// TODO(dt): de-validate all the FKs.
	if err := descsCol.WriteDesc(
		ctx, false /* kvTrace */, importing, txn,
	); err != nil {
		return jobspb.ImportDetails{}, err
	}

	importDetails.Table = jobspb.ImportDetails_Table{
		Desc:       importing.TableDesc(),
		Name:       table.Name,
		SeqVal:     table.SeqVal,
		TargetCols: table.TargetCols,
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
	execCfg.StatsRefresher.NotifyMutation(desc, math.MaxInt32 /* rowsAffected */)

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
) {
	var importEvent eventpb.Import
	if err := p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return sql.LogEventForJobs(ctx, p.ExecCfg(), txn, &importEvent, int64(job.ID()),
			job.Payload(), p.User(), status)
	}); err != nil {
		log.Dev.Warningf(ctx, "failed to log event: %v", err)
	}
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
	if err := sql.DescsTxn(ctx, cfg, func(
		ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
	) error {
		return r.dropTable(ctx, txn, descsCol, cfg)
	}); err != nil {
		log.Dev.Errorf(ctx, "drop table failed: %s", err.Error())
		return err
	}

	// Emit to the event log that the job has completed reverting.
	emitImportJobEvent(ctx, p, jobs.StateFailed, r.job)

	return nil
}

// CollectProfile is a part of the Resumer interface.
func (r *importResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

// dropTable implements the OnFailOrCancel logic.
func (r *importResumer) dropTable(
	ctx context.Context, txn isql.Txn, descsCol *descs.Collection, execCfg *sql.ExecutorConfig,
) error {
	details := r.job.Details().(jobspb.ImportDetails)

	// If the prepare step of the import job was not completed then the
	// descriptors do not need to be rolled back as the txn updating them never
	// completed.
	if !details.PrepareComplete {
		return nil
	}

	tbl, err := getTable(details)
	if err != nil {
		return err
	}
	desc, err := descsCol.MutableByID(txn.KV()).Table(ctx, tbl.Desc.ID)
	if err != nil {
		return err
	}
	intoTable := desc.ImmutableCopy().(catalog.TableDescriptor)
	// Clear table data from a rolling back IMPORT INTO cmd
	//
	// The walltime can be 0 if there is a failure between publishing the table
	// as OFFLINE and then choosing a ingestion timestamp. This might happen
	// while waiting for the descriptor version to propagate across the cluster
	// for example.
	//
	// In this case, we don't want to rollback the data since data ingestion has
	// not yet begun (since we have not chosen a timestamp at which to ingest.)
	if details.Walltime != 0 && !tbl.WasEmpty {
		// NB: if a revert fails it will abort the rest of this failure txn, which is
		// also what brings the table back online. We _could_ change the error handling
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
	} else if tbl.WasEmpty {
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
