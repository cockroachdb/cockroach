// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/joberror"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/ingesting"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/rewrite"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type importResumer struct {
	job      *jobs.Job
	settings *cluster.Settings
	res      roachpb.RowCount

	testingKnobs struct {
		afterImport            func(summary roachpb.RowCount) error
		alwaysFlushJobProgress bool
	}
}

func (r *importResumer) TestingSetAfterImportKnob(fn func(summary roachpb.RowCount) error) {
	r.testingKnobs.afterImport = fn
}

var _ jobs.TraceableJob = &importResumer{}

func (r *importResumer) ForceRealSpan() bool {
	return true
}

var _ jobs.Resumer = &importResumer{}

type preparedSchemaMetadata struct {
	schemaPreparedDetails jobspb.ImportDetails
	schemaRewrites        jobspb.DescRewriteMap
	newSchemaIDToName     map[descpb.ID]string
	oldSchemaIDToName     map[descpb.ID]string
	queuedSchemaJobs      []jobspb.JobID
}

// Resume is part of the jobs.Resumer interface.
func (r *importResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)
	if err := r.parseBundleSchemaIfNeeded(ctx, p); err != nil {
		return err
	}

	details := r.job.Details().(jobspb.ImportDetails)
	files := details.URIs
	format := details.Format

	tables := make(map[string]*execinfrapb.ReadImportDataSpec_ImportTable, len(details.Tables))
	if details.Tables != nil {
		// Skip prepare stage on job resumption, if it has already been completed.
		if !details.PrepareComplete {
			var schemaMetadata *preparedSchemaMetadata
			if err := sql.DescsTxn(ctx, p.ExecCfg(), func(
				ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
			) error {
				var preparedDetails jobspb.ImportDetails
				schemaMetadata = &preparedSchemaMetadata{
					newSchemaIDToName: make(map[descpb.ID]string),
					oldSchemaIDToName: make(map[descpb.ID]string),
				}
				var err error
				curDetails := details
				if len(details.Schemas) != 0 {
					schemaMetadata, err = r.prepareSchemasForIngestion(ctx, p, curDetails, txn, descsCol)
					if err != nil {
						return err
					}
					curDetails = schemaMetadata.schemaPreparedDetails
				}

				if r.settings.Version.IsActive(ctx, clusterversion.PublicSchemasWithDescriptors) {
					// In 22.1, the Public schema should always be present in the database.
					// Make sure it is part of schemaMetadata, it is not guaranteed to
					// be added in prepareSchemasForIngestion if we're not importing any
					// schemas.
					// The Public schema will not change in the database so both the
					// oldSchemaIDToName and newSchemaIDToName entries will be the
					// same for the Public schema.
					_, dbDesc, err := descsCol.GetImmutableDatabaseByID(ctx, txn, details.ParentID, tree.DatabaseLookupFlags{Required: true})
					if err != nil {
						return err
					}
					schemaMetadata.oldSchemaIDToName[dbDesc.GetSchemaID(tree.PublicSchema)] = tree.PublicSchema
					schemaMetadata.newSchemaIDToName[dbDesc.GetSchemaID(tree.PublicSchema)] = tree.PublicSchema
				}

				preparedDetails, err = r.prepareTablesForIngestion(ctx, p, curDetails, txn, descsCol,
					schemaMetadata)
				if err != nil {
					return err
				}

				// Telemetry for multi-region.
				for _, table := range preparedDetails.Tables {
					_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
						ctx, txn, table.Desc.GetParentID(), tree.DatabaseLookupFlags{Required: true})
					if err != nil {
						return err
					}
					if dbDesc.IsMultiRegion() {
						telemetry.Inc(sqltelemetry.ImportIntoMultiRegionDatabaseCounter)
					}
				}

				// Update the job details now that the schemas and table descs have
				// been "prepared".
				return r.job.Update(ctx, txn, func(
					txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
				) error {
					pl := md.Payload
					*pl.GetImport() = preparedDetails

					// Update the set of descriptors for later observability.
					// TODO(ajwerner): Do we need this idempotence test?
					prev := md.Payload.DescriptorIDs
					if prev == nil {
						var descriptorIDs []descpb.ID
						for _, schema := range preparedDetails.Schemas {
							descriptorIDs = append(descriptorIDs, schema.Desc.GetID())
						}
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

			// Run the queued job which updates the database descriptor to contain the
			// newly created schemas.
			// NB: Seems like the registry eventually adopts the job anyways but this
			// is in keeping with the semantics we use when creating a schema during
			// sql execution. Namely, queue job in the txn which creates the schema
			// desc and run once the txn has committed.
			if err := p.ExecCfg().JobRegistry.Run(ctx, p.ExecCfg().InternalExecutor,
				schemaMetadata.queuedSchemaJobs); err != nil {
				return err
			}

			// Re-initialize details after prepare step.
			details = r.job.Details().(jobspb.ImportDetails)
			emitImportJobEvent(ctx, p, jobs.StatusRunning, r.job)
		}

		// Create a mapping from schemaID to schemaName.
		schemaIDToName := make(map[descpb.ID]string)
		for _, i := range details.Schemas {
			schemaIDToName[i.Desc.GetID()] = i.Desc.GetName()
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

			// If we are importing from PGDUMP, qualify the table name with the schema
			// name since we support non-public schemas.
			if details.Format.Format == roachpb.IOFileFormat_PgDump {
				schemaName := tree.PublicSchema
				if schema, ok := schemaIDToName[i.Desc.GetUnexposedParentSchemaID()]; ok {
					schemaName = schema
				}
				tableName = fmt.Sprintf("%s.%s", schemaName, tableName)
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
	// shall chose a ts to import from.
	if details.Walltime == 0 {
		// Now that we know all the tables are offline, pick a walltime at which we
		// will write.
		details.Walltime = p.ExecCfg().Clock.Now().WallTime

		// Check if the tables being imported into are starting empty, in which
		// case we can cheaply clear-range instead of revert-range to cleanup.
		for i := range details.Tables {
			if !details.Tables[i].IsNew {
				tblDesc := tabledesc.NewBuilder(details.Tables[i].Desc).BuildImmutableTable()
				tblSpan := tblDesc.TableSpan(p.ExecCfg().Codec)
				res, err := p.ExecCfg().DB.Scan(ctx, tblSpan.Key, tblSpan.EndKey, 1 /* maxRows */)
				if err != nil {
					return errors.Wrap(err, "checking if existing table is empty")
				}
				details.Tables[i].WasEmpty = len(res) == 0
			}
		}

		if err := r.job.SetDetails(ctx, nil /* txn */, details); err != nil {
			return err
		}
	}

	res, err := ingestWithRetry(ctx, p, r.job, tables, typeDescs, files, format, details.Walltime,
		r.testingKnobs.alwaysFlushJobProgress)
	if err != nil {
		return err
	}

	pkIDs := make(map[uint64]struct{}, len(details.Tables))
	for _, t := range details.Tables {
		pkIDs[roachpb.BulkOpSummaryID(uint64(t.Desc.ID), uint64(t.Desc.PrimaryIndex.ID))] = struct{}{}
	}
	r.res.DataSize = res.DataSize
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

	// If the table being imported into referenced UDTs, ensure that a concurrent
	// schema change on any of the typeDescs has not modified the type descriptor. If
	// it has, it is unsafe to import the data and we fail the import job.
	if err := r.checkForUDTModification(ctx, p.ExecCfg()); err != nil {
		return err
	}

	if err := r.publishSchemas(ctx, p.ExecCfg()); err != nil {
		return err
	}

	if err := r.publishTables(ctx, p.ExecCfg(), res); err != nil {
		return err
	}

	// As of 21.2 we do not write a protected timestamp record during IMPORT INTO.
	// In case of a mixed version cluster with 21.1 and 21.2 nodes, it is possible
	// that the job was planned on an older node and then resumed on a 21.2 node.
	// Thus, we still need to clear the timestamp record that was written when the
	// IMPORT INTO was planned on the older node.
	//
	// TODO(adityamaru): Remove in 22.1.
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return r.releaseProtectedTimestamp(ctx, txn, p.ExecCfg().ProtectedTimestampProvider)
	}); err != nil {
		log.Errorf(ctx, "failed to release protected timestamp: %v", err)
	}

	emitImportJobEvent(ctx, p, jobs.StatusSucceeded, r.job)

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
	schemaMetadata *preparedSchemaMetadata,
) (jobspb.ImportDetails, error) {
	importDetails := details
	importDetails.Tables = make([]jobspb.ImportDetails_Table, len(details.Tables))

	newSchemaAndTableNameToIdx := make(map[string]int, len(importDetails.Tables))
	var hasExistingTables bool
	var err error
	var newTableDescs []jobspb.ImportDetails_Table
	var desc *descpb.TableDescriptor
	for i, table := range details.Tables {
		if !table.IsNew {
			desc, err = prepareExistingTablesForIngestion(ctx, txn, descsCol, table.Desc)
			if err != nil {
				return importDetails, err
			}
			importDetails.Tables[i] = jobspb.ImportDetails_Table{
				Desc: desc, Name: table.Name,
				SeqVal:     table.SeqVal,
				IsNew:      table.IsNew,
				TargetCols: table.TargetCols,
			}

			hasExistingTables = true
		} else {
			// PGDUMP imports support non-public schemas.
			// For the purpose of disambiguation we must take the schema into
			// account when constructing the newTablenameToIdx map.
			// At this point the table descriptor's parent schema ID has not being
			// remapped to the newly generated schema ID.
			key, err := constructSchemaAndTableKey(ctx, table.Desc, schemaMetadata.oldSchemaIDToName, p.ExecCfg().Settings.Version)
			if err != nil {
				return importDetails, err
			}
			newSchemaAndTableNameToIdx[key.String()] = i
			// Make a deep copy of the table descriptor so that rewrites do not
			// partially clobber the descriptor stored in details.
			newTableDescs = append(newTableDescs,
				*protoutil.Clone(&table).(*jobspb.ImportDetails_Table))
		}
	}

	// Prepare the table descriptors for newly created tables being imported
	// into.
	//
	// TODO(adityamaru): This is still unnecessarily complicated. If we can get
	// the new table desc preparation to work on a per desc basis, rather than
	// requiring all the newly created descriptors, then this can look like the
	// call to prepareExistingTablesForIngestion. Currently, FK references
	// misbehave when I tried to write the desc one at a time.
	if len(newTableDescs) != 0 {
		res, err := prepareNewTablesForIngestion(
			ctx, txn, descsCol, p, newTableDescs, importDetails.ParentID, schemaMetadata.schemaRewrites)
		if err != nil {
			return importDetails, err
		}

		for _, desc := range res {
			key, err := constructSchemaAndTableKey(ctx, desc, schemaMetadata.newSchemaIDToName, p.ExecCfg().Settings.Version)
			if err != nil {
				return importDetails, err
			}
			i := newSchemaAndTableNameToIdx[key.String()]
			table := details.Tables[i]
			importDetails.Tables[i] = jobspb.ImportDetails_Table{
				Desc:       desc,
				Name:       table.Name,
				SeqVal:     table.SeqVal,
				IsNew:      table.IsNew,
				TargetCols: table.TargetCols,
			}
		}
	}

	importDetails.PrepareComplete = true

	// If we do not have pending schema changes on existing descriptors we can
	// choose our Walltime (to IMPORT from) immediately. Otherwise, we have to
	// wait for all nodes to see the same descriptor version before doing so.
	if !hasExistingTables {
		importDetails.Walltime = p.ExecCfg().Clock.Now().WallTime
	} else {
		importDetails.Walltime = 0
	}

	return importDetails, nil
}

// prepareExistingTablesForIngestion prepares descriptors for existing tables
// being imported into.
func prepareExistingTablesForIngestion(
	ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, desc *descpb.TableDescriptor,
) (*descpb.TableDescriptor, error) {
	if len(desc.Mutations) > 0 {
		return nil, errors.Errorf("cannot IMPORT INTO a table with schema changes in progress -- try again later (pending mutation %s)", desc.Mutations[0].String())
	}

	// Note that desc is just used to verify that the version matches.
	importing, err := descsCol.GetMutableTableVersionByID(ctx, desc.ID, txn)
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
	importing.SetOffline("importing")

	// TODO(dt): de-validate all the FKs.
	if err := descsCol.WriteDesc(
		ctx, false /* kvTrace */, importing, txn,
	); err != nil {
		return nil, err
	}

	return importing.TableDesc(), nil
}

// prepareNewTablesForIngestion prepares descriptors for newly created
// tables being imported into.
func prepareNewTablesForIngestion(
	ctx context.Context,
	txn *kv.Txn,
	descsCol *descs.Collection,
	p sql.JobExecContext,
	importTables []jobspb.ImportDetails_Table,
	parentID descpb.ID,
	schemaRewrites jobspb.DescRewriteMap,
) ([]*descpb.TableDescriptor, error) {
	newMutableTableDescriptors := make([]*tabledesc.Mutable, len(importTables))
	for i := range importTables {
		newMutableTableDescriptors[i] = tabledesc.NewBuilder(importTables[i].Desc).BuildCreatedMutableTable()
	}

	// Verification steps have passed, generate a new table ID if we're
	// restoring. We do this last because we want to avoid calling
	// GenerateUniqueDescID if there's any kind of error above.
	// Reserving a table ID now means we can avoid the rekey work during restore.
	//
	// schemaRewrites may contain information which is used in rewrite.TableDescs
	// to rewrite the parent schema ID in the table desc to point to the correct
	// schema ID.
	tableRewrites := schemaRewrites
	if tableRewrites == nil {
		tableRewrites = make(jobspb.DescRewriteMap)
	}
	seqVals := make(map[descpb.ID]int64, len(importTables))
	for _, tableDesc := range importTables {
		id, err := descidgen.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
		if err != nil {
			return nil, err
		}
		oldParentSchemaID := tableDesc.Desc.GetUnexposedParentSchemaID()
		parentSchemaID := oldParentSchemaID
		if rw, ok := schemaRewrites[oldParentSchemaID]; ok {
			parentSchemaID = rw.ID
		}
		tableRewrites[tableDesc.Desc.ID] = &jobspb.DescriptorRewrite{
			ID:             id,
			ParentSchemaID: parentSchemaID,
			ParentID:       parentID,
		}
		seqVals[id] = tableDesc.SeqVal
	}
	if err := rewrite.TableDescs(
		newMutableTableDescriptors, tableRewrites, "",
	); err != nil {
		return nil, err
	}

	// After all of the ID's have been remapped, ensure that there aren't any name
	// collisions with any importing tables.
	for i := range newMutableTableDescriptors {
		tbl := newMutableTableDescriptors[i]
		err := descsCol.Direct().CheckObjectCollision(
			ctx,
			txn,
			tbl.GetParentID(),
			tbl.GetParentSchemaID(),
			tree.NewUnqualifiedTableName(tree.Name(tbl.GetName())),
		)
		if err != nil {
			return nil, err
		}
	}

	// tableDescs contains the same slice as newMutableTableDescriptors but
	// as tabledesc.TableDescriptor.
	tableDescs := make([]catalog.TableDescriptor, len(newMutableTableDescriptors))
	for i := range tableDescs {
		newMutableTableDescriptors[i].SetOffline("importing")
		tableDescs[i] = newMutableTableDescriptors[i]
	}

	var seqValKVs []roachpb.KeyValue
	for _, desc := range newMutableTableDescriptors {
		if v, ok := seqVals[desc.GetID()]; ok && v != 0 {
			key, val, err := sql.MakeSequenceKeyVal(p.ExecCfg().Codec, desc, v, false)
			if err != nil {
				return nil, err
			}
			kv := roachpb.KeyValue{Key: key}
			kv.Value.SetInt(val)
			seqValKVs = append(seqValKVs, kv)
		}
	}

	// Write the new TableDescriptors and flip the namespace entries over to
	// them. After this call, any queries on a table will be served by the newly
	// imported data.
	if err := ingesting.WriteDescriptors(ctx, p.ExecCfg().Codec, txn, p.User(), descsCol,
		nil /* databases */, nil, /* schemas */
		tableDescs, nil, tree.RequestedDescriptors, seqValKVs, "" /* inheritParentName */); err != nil {
		return nil, errors.Wrapf(err, "creating importTables")
	}

	newPreparedTableDescs := make([]*descpb.TableDescriptor, len(newMutableTableDescriptors))
	for i := range newMutableTableDescriptors {
		newPreparedTableDescs[i] = newMutableTableDescriptors[i].TableDesc()
	}

	return newPreparedTableDescs, nil
}

// prepareSchemasForIngestion is responsible for assigning the created schema
// descriptors actual IDs, updating the parent DB with references to the new
// schemas and writing the schema descriptors to disk.
func (r *importResumer) prepareSchemasForIngestion(
	ctx context.Context,
	p sql.JobExecContext,
	details jobspb.ImportDetails,
	txn *kv.Txn,
	descsCol *descs.Collection,
) (*preparedSchemaMetadata, error) {
	schemaMetadata := &preparedSchemaMetadata{
		schemaPreparedDetails: details,
		newSchemaIDToName:     make(map[descpb.ID]string),
		oldSchemaIDToName:     make(map[descpb.ID]string),
	}

	schemaMetadata.schemaPreparedDetails.Schemas = make([]jobspb.ImportDetails_Schema,
		len(details.Schemas))

	desc, err := descsCol.GetMutableDescriptorByID(ctx, txn, details.ParentID)
	if err != nil {
		return nil, err
	}

	dbDesc, ok := desc.(*dbdesc.Mutable)
	if !ok {
		return nil, errors.Newf("expected ID %d to refer to the database being imported into",
			details.ParentID)
	}

	schemaMetadata.schemaRewrites = make(jobspb.DescRewriteMap)
	mutableSchemaDescs := make([]*schemadesc.Mutable, 0)
	for _, desc := range details.Schemas {
		schemaMetadata.oldSchemaIDToName[desc.Desc.GetID()] = desc.Desc.GetName()
		newMutableSchemaDescriptor := schemadesc.NewBuilder(desc.Desc).BuildCreatedMutable().(*schemadesc.Mutable)

		// Verification steps have passed, generate a new schema ID. We do this
		// last because we want to avoid calling GenerateUniqueDescID if there's
		// any kind of error in the prior stages of import.
		id, err := descidgen.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
		if err != nil {
			return nil, err
		}
		newMutableSchemaDescriptor.Version = 1
		newMutableSchemaDescriptor.ID = id
		mutableSchemaDescs = append(mutableSchemaDescs, newMutableSchemaDescriptor)

		schemaMetadata.newSchemaIDToName[id] = newMutableSchemaDescriptor.GetName()

		// Update the parent database with this schema information.
		dbDesc.AddSchemaToDatabase(newMutableSchemaDescriptor.Name,
			descpb.DatabaseDescriptor_SchemaInfo{ID: newMutableSchemaDescriptor.ID})

		schemaMetadata.schemaRewrites[desc.Desc.ID] = &jobspb.DescriptorRewrite{
			ID: id,
		}
	}

	// Queue a job to write the updated database descriptor.
	schemaMetadata.queuedSchemaJobs, err = writeNonDropDatabaseChange(ctx, dbDesc, txn, descsCol, p,
		fmt.Sprintf("updating parent database %s when importing new schemas", dbDesc.GetName()))
	if err != nil {
		return nil, err
	}

	// Finally create the schemas on disk.
	for i, mutDesc := range mutableSchemaDescs {
		nameKey := catalogkeys.MakeSchemaNameKey(p.ExecCfg().Codec, dbDesc.ID, mutDesc.GetName())
		err = createSchemaDescriptorWithID(ctx, nameKey, mutDesc.ID, mutDesc, p, descsCol, txn)
		if err != nil {
			return nil, err
		}
		schemaMetadata.schemaPreparedDetails.Schemas[i] = jobspb.ImportDetails_Schema{
			Desc: mutDesc.SchemaDesc(),
		}
	}

	return schemaMetadata, err
}

// createSchemaDescriptorWithID writes a schema descriptor with `id` to disk.
func createSchemaDescriptorWithID(
	ctx context.Context,
	idKey roachpb.Key,
	id descpb.ID,
	descriptor catalog.Descriptor,
	p sql.JobExecContext,
	descsCol *descs.Collection,
	txn *kv.Txn,
) error {
	if descriptor.GetID() == descpb.InvalidID {
		return errors.AssertionFailedf("cannot create descriptor with an empty ID: %v", descriptor)
	}
	if descriptor.GetID() != id {
		return errors.AssertionFailedf("cannot create descriptor with an ID %v; expected ID %v; descriptor %v",
			id, descriptor.GetID(), descriptor)
	}
	b := &kv.Batch{}
	descID := descriptor.GetID()
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", idKey, descID)
	}
	b.CPut(idKey, descID, nil)
	if err := descsCol.Direct().WriteNewDescToBatch(
		ctx,
		p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		b,
		descriptor,
	); err != nil {
		return err
	}

	mutDesc, ok := descriptor.(catalog.MutableDescriptor)
	if !ok {
		return errors.Newf("unexpected type %T when creating descriptor", descriptor)
	}
	switch mutDesc.(type) {
	case *schemadesc.Mutable:
		if err := descsCol.AddUncommittedDescriptor(mutDesc); err != nil {
			return err
		}
	default:
		return errors.Newf("unexpected type %T when creating descriptor", mutDesc)
	}

	return txn.Run(ctx, b)
}

// parseBundleSchemaIfNeeded parses dump files (PGDUMP, MYSQLDUMP) for DDL
// statements and creates the relevant database, schema, table and type
// descriptors. Data from the dump files is ingested into these descriptors in
// the next phase of the import.
func (r *importResumer) parseBundleSchemaIfNeeded(ctx context.Context, phs interface{}) error {
	p := phs.(sql.JobExecContext)
	seqVals := make(map[descpb.ID]int64)
	details := r.job.Details().(jobspb.ImportDetails)
	skipFKs := details.SkipFKs
	parentID := details.ParentID
	files := details.URIs
	format := details.Format

	owner := r.job.Payload().UsernameProto.Decode()

	p.SessionDataMutatorIterator().SetSessionDefaultIntSize(details.DefaultIntSize)

	if details.ParseBundleSchema {
		var span *tracing.Span
		ctx, span = tracing.ChildSpan(ctx, "import-parsing-bundle-schema")
		defer span.Finish()

		if err := r.job.RunningStatus(ctx, nil /* txn */, func(_ context.Context, _ jobspb.Details) (jobs.RunningStatus, error) {
			return runningStatusImportBundleParseSchema, nil
		}); err != nil {
			return errors.Wrapf(err, "failed to update running status of job %d", errors.Safe(r.job.ID()))
		}

		var dbDesc catalog.DatabaseDescriptor
		{
			if err := sql.DescsTxn(ctx, p.ExecCfg(), func(
				ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
			) (err error) {
				_, dbDesc, err = descriptors.GetImmutableDatabaseByID(ctx, txn, parentID, tree.DatabaseLookupFlags{
					Required:    true,
					AvoidLeased: true,
				})
				if err != nil {
					return err
				}
				return err
			}); err != nil {
				return err
			}
		}

		var schemaDescs []*schemadesc.Mutable
		var tableDescs []*tabledesc.Mutable
		var err error
		walltime := p.ExecCfg().Clock.Now().WallTime

		if tableDescs, schemaDescs, err = parseAndCreateBundleTableDescs(
			ctx, p, details, seqVals, skipFKs, dbDesc, files, format, walltime, owner,
			r.job.ID()); err != nil {
			return err
		}

		schemaDetails := make([]jobspb.ImportDetails_Schema, len(schemaDescs))
		for i, schemaDesc := range schemaDescs {
			schemaDetails[i] = jobspb.ImportDetails_Schema{Desc: schemaDesc.SchemaDesc()}
		}
		details.Schemas = schemaDetails

		tableDetails := make([]jobspb.ImportDetails_Table, len(tableDescs))
		for i, tableDesc := range tableDescs {
			tableDetails[i] = jobspb.ImportDetails_Table{
				Name:   tableDesc.GetName(),
				Desc:   tableDesc.TableDesc(),
				SeqVal: seqVals[tableDescs[i].ID],
				IsNew:  true,
			}
		}
		details.Tables = tableDetails

		for _, tbl := range tableDescs {
			// For reasons relating to #37691, we disallow user defined types in
			// the standard IMPORT case.
			for _, col := range tbl.Columns {
				if col.Type.UserDefined() {
					return errors.Newf("IMPORT cannot be used with user defined types; use IMPORT INTO instead")
				}
			}
		}
		// Prevent job from redoing schema parsing and table desc creation
		// on subsequent resumptions.
		details.ParseBundleSchema = false
		if err := r.job.SetDetails(ctx, nil /* txn */, details); err != nil {
			return err
		}
	}
	return nil
}

func getPublicSchemaDescForDatabase(
	ctx context.Context, execCfg *sql.ExecutorConfig, db catalog.DatabaseDescriptor,
) (scDesc catalog.SchemaDescriptor, err error) {
	if !execCfg.Settings.Version.IsActive(ctx, clusterversion.PublicSchemasWithDescriptors) {
		return schemadesc.GetPublicSchema(), err
	}
	if err := sql.DescsTxn(ctx, execCfg, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		publicSchemaID := db.GetSchemaID(tree.PublicSchema)
		scDesc, err = descriptors.GetImmutableSchemaByID(ctx, txn, publicSchemaID, tree.SchemaLookupFlags{Required: true})
		return err
	}); err != nil {
		return nil, err
	}

	return scDesc, nil
}

// parseAndCreateBundleTableDescs parses and creates the table
// descriptors for bundle formats.
func parseAndCreateBundleTableDescs(
	ctx context.Context,
	p sql.JobExecContext,
	details jobspb.ImportDetails,
	seqVals map[descpb.ID]int64,
	skipFKs bool,
	parentDB catalog.DatabaseDescriptor,
	files []string,
	format roachpb.IOFileFormat,
	walltime int64,
	owner security.SQLUsername,
	jobID jobspb.JobID,
) ([]*tabledesc.Mutable, []*schemadesc.Mutable, error) {

	var schemaDescs []*schemadesc.Mutable
	var tableDescs []*tabledesc.Mutable
	var tableName string

	// A single table entry in the import job details when importing a bundle format
	// indicates that we are performing a single table import.
	// This info is populated during the planning phase.
	if len(details.Tables) > 0 {
		tableName = details.Tables[0].Name
	}

	store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, files[0], p.User())
	if err != nil {
		return tableDescs, schemaDescs, err
	}
	defer store.Close()

	raw, err := store.ReadFile(ctx, "")
	if err != nil {
		return tableDescs, schemaDescs, err
	}
	defer raw.Close(ctx)
	reader, err := decompressingReader(ioctx.ReaderCtxAdapter(ctx, raw), files[0], format.Compression)
	if err != nil {
		return tableDescs, schemaDescs, err
	}
	defer reader.Close()

	fks := fkHandler{skip: skipFKs, allowed: true, resolver: fkResolver{
		tableNameToDesc: make(map[string]*tabledesc.Mutable),
	}}
	switch format.Format {
	case roachpb.IOFileFormat_Mysqldump:
		id, err := descidgen.PeekNextUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
		if err != nil {
			return tableDescs, schemaDescs, err
		}
		fks.resolver.format.Format = roachpb.IOFileFormat_Mysqldump
		evalCtx := &p.ExtendedEvalContext().EvalContext
		tableDescs, err = readMysqlCreateTable(
			ctx, reader, evalCtx, p, id, parentDB, tableName, fks,
			seqVals, owner, walltime,
		)
		if err != nil {
			return tableDescs, schemaDescs, err
		}
	case roachpb.IOFileFormat_PgDump:
		fks.resolver.format.Format = roachpb.IOFileFormat_PgDump
		evalCtx := &p.ExtendedEvalContext().EvalContext

		// Setup a logger to handle unsupported DDL statements in the PGDUMP file.
		unsupportedStmtLogger := makeUnsupportedStmtLogger(ctx, p.User(), int64(jobID),
			format.PgDump.IgnoreUnsupported, format.PgDump.IgnoreUnsupportedLog, schemaParsing,
			p.ExecCfg().DistSQLSrv.ExternalStorage)

		tableDescs, schemaDescs, err = readPostgresCreateTable(ctx, reader, evalCtx, p, tableName,
			parentDB, walltime, fks, int(format.PgDump.MaxRowSize), owner, unsupportedStmtLogger)

		logErr := unsupportedStmtLogger.flush()
		if logErr != nil {
			return nil, nil, logErr
		}

	default:
		return tableDescs, schemaDescs, errors.Errorf(
			"non-bundle format %q does not support reading schemas", format.Format.String())
	}

	if err != nil {
		return tableDescs, schemaDescs, err
	}

	if tableDescs == nil && len(details.Tables) > 0 {
		return tableDescs, schemaDescs, errors.Errorf("table definition not found for %q", tableName)
	}

	return tableDescs, schemaDescs, err
}

// publishTables updates the status of imported tables from OFFLINE to PUBLIC.
func (r *importResumer) publishTables(
	ctx context.Context, execCfg *sql.ExecutorConfig, res roachpb.BulkOpSummary,
) error {
	details := r.job.Details().(jobspb.ImportDetails)
	// Tables should only be published once.
	if details.TablesPublished {
		return nil
	}

	// Write stub statistics for new tables created during the import. This should
	// be sufficient until the CREATE STATISTICS run finishes.
	r.writeStubStatisticsForImportedTables(ctx, execCfg, res)

	log.Event(ctx, "making tables live")

	err := sql.DescsTxn(ctx, execCfg, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		b := txn.NewBatch()
		for _, tbl := range details.Tables {
			newTableDesc, err := descsCol.GetMutableTableVersionByID(ctx, tbl.Desc.ID, txn)
			if err != nil {
				return err
			}
			newTableDesc.SetPublic()

			if !tbl.IsNew {
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
				for _, c := range newTableDesc.AllActiveAndInactiveChecks() {
					c.Validity = descpb.ConstraintValidity_Unvalidated
				}
			}

			// TODO(dt): re-validate any FKs?
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, newTableDesc, b,
			); err != nil {
				return errors.Wrapf(err, "publishing table %d", newTableDesc.ID)
			}
		}
		if err := txn.Run(ctx, b); err != nil {
			return errors.Wrap(err, "publishing tables")
		}

		// Update job record to mark tables published state as complete.
		details.TablesPublished = true
		err := r.job.SetDetails(ctx, txn, details)
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

// writeStubStatisticsForImportedTables writes "stub" statistics for new tables
// created during an import.
func (r *importResumer) writeStubStatisticsForImportedTables(
	ctx context.Context, execCfg *sql.ExecutorConfig, res roachpb.BulkOpSummary,
) {
	details := r.job.Details().(jobspb.ImportDetails)
	for _, tbl := range details.Tables {
		if tbl.IsNew {
			desc := tabledesc.NewBuilder(tbl.Desc).BuildImmutableTable()
			id := roachpb.BulkOpSummaryID(uint64(desc.GetID()), uint64(desc.GetPrimaryIndexID()))
			rowCount := uint64(res.EntryCounts[id])
			// TODO(michae2): collect distinct and null counts during import.
			distinctCount := uint64(float64(rowCount) * memo.UnknownDistinctCountRatio)
			nullCount := uint64(float64(rowCount) * memo.UnknownNullCountRatio)
			avgRowSize := uint64(memo.UnknownAvgRowSize)
			// Because we don't yet have real distinct and null counts, only produce
			// single-column stats to avoid the appearance of perfectly correlated
			// columns.
			multiColEnabled := false
			statistics, err := sql.StubTableStats(desc, jobspb.ImportStatsName, multiColEnabled)
			if err == nil {
				for _, statistic := range statistics {
					statistic.RowCount = rowCount
					statistic.DistinctCount = distinctCount
					statistic.NullCount = nullCount
					statistic.AvgSize = avgRowSize
				}
				// TODO(michae2): parallelize insertion of statistics.
				err = stats.InsertNewStats(ctx, execCfg.Settings, execCfg.InternalExecutor, nil /* txn */, statistics)
			}
			if err != nil {
				// Failure to create statistics should not fail the entire import.
				log.Warningf(
					ctx, "error while creating statistics during import of %q: %v",
					desc.GetName(), err,
				)
			}
		}
	}
}

// publishSchemas updates the status of imported schemas from OFFLINE to PUBLIC.
func (r *importResumer) publishSchemas(ctx context.Context, execCfg *sql.ExecutorConfig) error {
	details := r.job.Details().(jobspb.ImportDetails)
	// Schemas should only be published once.
	if details.SchemasPublished {
		return nil
	}
	log.Event(ctx, "making schemas live")

	return sql.DescsTxn(ctx, execCfg, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		b := txn.NewBatch()
		for _, schema := range details.Schemas {
			newDesc, err := descsCol.GetMutableDescriptorByID(ctx, txn, schema.Desc.GetID())
			if err != nil {
				return err
			}
			newSchemaDesc, ok := newDesc.(*schemadesc.Mutable)
			if !ok {
				return errors.Newf("expected schema descriptor with ID %v, got %v",
					schema.Desc.GetID(), newDesc)
			}
			newSchemaDesc.SetPublic()
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, newSchemaDesc, b,
			); err != nil {
				return errors.Wrapf(err, "publishing schema %d", newSchemaDesc.ID)
			}
		}
		if err := txn.Run(ctx, b); err != nil {
			return errors.Wrap(err, "publishing schemas")
		}

		// Update job record to mark tables published state as complete.
		details.SchemasPublished = true
		err := r.job.SetDetails(ctx, txn, details)
		if err != nil {
			return errors.Wrap(err, "updating job details after publishing schemas")
		}
		return nil
	})
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
		typeDesc, err := col.Direct().MustGetTypeDescByID(ctx, txn, savedTypeDesc.GetID())
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
		ctx context.Context, txn *kv.Txn, col *descs.Collection,
	) error {
		for _, savedTypeDesc := range details.Types {
			if err := checkTypeIsEquivalent(
				ctx, txn, col, savedTypeDesc.Desc,
			); err != nil {
				return err
			}
		}
		return nil
	}
	return sql.DescsTxn(ctx, execCfg, checkTypesAreEquivalent)
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
	alwaysFlushProgress bool,
) (roachpb.BulkOpSummary, error) {
	resumerSpan := tracing.SpanFromContext(ctx)

	// We retry on pretty generic failures -- any rpc error. If a worker node were
	// to restart, it would produce this kind of error, but there may be other
	// errors that are also rpc errors. Don't retry to aggressively.
	retryOpts := retry.Options{
		MaxBackoff: 1 * time.Second,
		MaxRetries: 5,
	}

	// We want to retry an import if there are transient failures (i.e. worker
	// nodes dying), so if we receive a retryable error, re-plan and retry the
	// import.
	var res roachpb.BulkOpSummary
	var err error
	var retryCount int32
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		retryCount++
		resumerSpan.RecordStructured(&roachpb.RetryTracingEvent{
			Operation:     "importResumer.ingestWithRetry",
			AttemptNumber: retryCount,
			RetryError:    tracing.RedactAndTruncateError(err),
		})
		res, err = distImport(ctx, execCtx, job, tables, typeDescs, from, format, walltime,
			alwaysFlushProgress)
		if err == nil {
			break
		}

		if joberror.IsPermanentBulkJobError(err) {
			return res, err
		}

		// Re-load the job in order to update our progress object, which may have
		// been updated by the changeFrontier processor since the flow started.
		reloadedJob, reloadErr := execCtx.ExecCfg().JobRegistry.LoadClaimedJob(ctx, job.ID())
		if reloadErr != nil {
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			log.Warningf(ctx, `IMPORT job %d could not reload job progress when retrying: %+v`,
				int64(job.ID()), reloadErr)
		} else {
			job = reloadedJob
		}
		log.Warningf(ctx, `encountered retryable error: %+v`, err)
	}

	if err != nil {
		return roachpb.BulkOpSummary{}, errors.Wrap(err, "exhausted retries")
	}
	return res, nil
}

// emitImportJobEvent emits an import job event to the event log.
func emitImportJobEvent(
	ctx context.Context, p sql.JobExecContext, status jobs.Status, job *jobs.Job,
) {
	var importEvent eventpb.Import
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return sql.LogEventForJobs(ctx, p.ExecCfg(), txn, &importEvent, int64(job.ID()),
			job.Payload(), p.User(), status)
	}); err != nil {
		log.Warningf(ctx, "failed to log event: %v", err)
	}
}

func constructSchemaAndTableKey(
	ctx context.Context,
	tableDesc *descpb.TableDescriptor,
	schemaIDToName map[descpb.ID]string,
	version clusterversion.Handle,
) (schemaAndTableName, error) {
	if !version.IsActive(ctx, clusterversion.PublicSchemasWithDescriptors) {
		if tableDesc.UnexposedParentSchemaID == keys.PublicSchemaIDForBackup {
			return schemaAndTableName{schema: "", table: tableDesc.GetName()}, nil
		}
	}
	schemaName, ok := schemaIDToName[tableDesc.GetUnexposedParentSchemaID()]
	if !ok && schemaName != tree.PublicSchema {
		return schemaAndTableName{}, errors.Newf("invalid parent schema %s with ID %d for table %s",
			schemaName, tableDesc.UnexposedParentSchemaID, tableDesc.GetName())
	}

	return schemaAndTableName{schema: schemaName, table: tableDesc.GetName()}, nil
}

func writeNonDropDatabaseChange(
	ctx context.Context,
	desc *dbdesc.Mutable,
	txn *kv.Txn,
	descsCol *descs.Collection,
	p sql.JobExecContext,
	jobDesc string,
) ([]jobspb.JobID, error) {
	var job *jobs.Job
	var err error
	if job, err = createNonDropDatabaseChangeJob(p.User(), desc.ID, jobDesc, p, txn); err != nil {
		return nil, err
	}

	queuedJob := []jobspb.JobID{job.ID()}
	b := txn.NewBatch()
	err = descsCol.WriteDescToBatch(
		ctx,
		p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		desc,
		b,
	)
	if err != nil {
		return nil, err
	}
	return queuedJob, txn.Run(ctx, b)
}

func createNonDropDatabaseChangeJob(
	user security.SQLUsername,
	databaseID descpb.ID,
	jobDesc string,
	p sql.JobExecContext,
	txn *kv.Txn,
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
		p.ExtendedEvalContext().Context,
		jobRecord,
		jobID,
		txn,
	)
}

// OnFailOrCancel is part of the jobs.Resumer interface. Removes data that has
// been committed from a import that has failed or been canceled. It does this
// by adding the table descriptors in DROP state, which causes the schema change
// stuff to delete the keys in the background.
func (r *importResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)

	// Emit to the event log that the job has started reverting.
	emitImportJobEvent(ctx, p, jobs.StatusReverting, r.job)

	details := r.job.Details().(jobspb.ImportDetails)
	addToFileFormatTelemetry(details.Format.Format.String(), "failed")
	cfg := execCtx.(sql.JobExecContext).ExecCfg()
	var jobsToRunAfterTxnCommit []jobspb.JobID
	if err := sql.DescsTxn(ctx, cfg, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		if err := r.dropTables(ctx, txn, descsCol, cfg); err != nil {
			return err
		}

		// Drop all the schemas which may have been created during a bundle import.
		// These schemas should now be empty as all the tables in them would be new
		// tables created during the import, and therefore dropped by the above
		// dropTables method. This allows us to avoid "collecting" objects in the
		// schema before dropping the descriptor.
		var err error
		jobsToRunAfterTxnCommit, err = r.dropSchemas(ctx, txn, descsCol, cfg, p)
		if err != nil {
			return err
		}
		// TODO(adityamaru): Remove in 22.1 since we do not write PTS records during
		// IMPORT INTO from 21.2+.
		return r.releaseProtectedTimestamp(ctx, txn, cfg.ProtectedTimestampProvider)
	}); err != nil {
		return err
	}

	// Run any jobs which might have been queued when dropping the schemas.
	// This would be a job to drop all the schemas, and a job to update the parent
	// database descriptor.
	if len(jobsToRunAfterTxnCommit) != 0 {
		if err := p.ExecCfg().JobRegistry.Run(ctx, p.ExecCfg().InternalExecutor,
			jobsToRunAfterTxnCommit); err != nil {
			return errors.Wrap(err, "failed to run jobs that drop the imported schemas")
		}
	}

	// Emit to the event log that the job has completed reverting.
	emitImportJobEvent(ctx, p, jobs.StatusFailed, r.job)

	return nil
}

// dropTables implements the OnFailOrCancel logic.
func (r *importResumer) dropTables(
	ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, execCfg *sql.ExecutorConfig,
) error {
	details := r.job.Details().(jobspb.ImportDetails)
	dropTime := int64(1)

	// If the prepare step of the import job was not completed then the
	// descriptors do not need to be rolled back as the txn updating them never
	// completed.
	if !details.PrepareComplete {
		return nil
	}

	var revert []catalog.TableDescriptor
	var empty []catalog.TableDescriptor
	for _, tbl := range details.Tables {
		if !tbl.IsNew {
			desc, err := descsCol.GetMutableTableVersionByID(ctx, tbl.Desc.ID, txn)
			if err != nil {
				return err
			}
			imm := desc.ImmutableCopy().(catalog.TableDescriptor)
			if tbl.WasEmpty {
				empty = append(empty, imm)
			} else {
				revert = append(revert, imm)
			}
		}
	}

	// The walltime can be 0 if there is a failure between publishing the tables
	// as OFFLINE and then choosing a ingestion timestamp. This might happen
	// while waiting for the descriptor version to propagate across the cluster
	// for example.
	//
	// In this case, we don't want to rollback the data since data ingestion has
	// not yet begun (since we have not chosen a timestamp at which to ingest.)
	if details.Walltime != 0 && len(revert) > 0 {
		// NB: if a revert fails it will abort the rest of this failure txn, which is
		// also what brings tables back online. We _could_ change the error handling
		// or just move the revert into Resume()'s error return path, however it isn't
		// clear that just bringing a table back online with partially imported data
		// that may or may not be partially reverted is actually a good idea. It seems
		// better to do the revert here so that the table comes back if and only if,
		// it was rolled back to its pre-IMPORT state, and instead provide a manual
		// admin knob (e.g. ALTER TABLE REVERT TO SYSTEM TIME) if anything goes wrong.
		ts := hlc.Timestamp{WallTime: details.Walltime}.Prev()

		// disallowShadowingBelow=writeTS used to write means no existing keys could
		// have been covered by a key imported and the table was offline to other
		// writes, so even if GC has run it would not have GC'ed any keys to which
		// we need to revert, so we can safely ignore the target-time GC check.
		const ignoreGC = true
		if err := sql.RevertTables(ctx, txn.DB(), execCfg, revert, ts, ignoreGC, sql.RevertTableDefaultBatchSize); err != nil {
			return errors.Wrap(err, "rolling back partially completed IMPORT")
		}
	}

	for i := range empty {
		// Set a DropTime on the table descriptor to differentiate it from an
		// older-format (v1.1) descriptor. This enables ClearTableData to use a
		// RangeClear for faster data removal, rather than removing by chunks.
		empty[i].TableDesc().DropTime = dropTime
		if err := gcjob.ClearTableData(
			ctx, execCfg.DB, execCfg.DistSender, execCfg.Codec, &execCfg.Settings.SV, empty[i],
		); err != nil {
			return errors.Wrapf(err, "clearing data for table %d", empty[i].GetID())
		}
	}

	b := txn.NewBatch()
	tablesToGC := make([]descpb.ID, 0, len(details.Tables))
	for _, tbl := range details.Tables {
		newTableDesc, err := descsCol.GetMutableTableVersionByID(ctx, tbl.Desc.ID, txn)
		if err != nil {
			return err
		}
		if tbl.IsNew {
			newTableDesc.SetDropped()
			// If the DropTime if set, a table uses RangeClear for fast data removal. This
			// operation starts at DropTime + the GC TTL. If we used now() here, it would
			// not clean up data until the TTL from the time of the error. Instead, use 1
			// (that is, 1ns past the epoch) to allow this to be cleaned up as soon as
			// possible. This is safe since the table data was never visible to users,
			// and so we don't need to preserve MVCC semantics.
			newTableDesc.DropTime = dropTime
			b.Del(catalogkeys.EncodeNameKey(execCfg.Codec, newTableDesc))
			tablesToGC = append(tablesToGC, newTableDesc.ID)
			descsCol.AddDeletedDescriptor(newTableDesc.GetID())
		} else {
			// IMPORT did not create this table, so we should not drop it.
			newTableDesc.SetPublic()
		}
		if err := descsCol.WriteDescToBatch(
			ctx, false /* kvTrace */, newTableDesc, b,
		); err != nil {
			return err
		}
	}

	// Queue a GC job.
	gcDetails := jobspb.SchemaChangeGCDetails{}
	for _, tableID := range tablesToGC {
		gcDetails.Tables = append(gcDetails.Tables, jobspb.SchemaChangeGCDetails_DroppedID{
			ID:       tableID,
			DropTime: dropTime,
		})
	}
	gcJobRecord := jobs.Record{
		Description:   fmt.Sprintf("GC for %s", r.job.Payload().Description),
		Username:      r.job.Payload().UsernameProto.Decode(),
		DescriptorIDs: tablesToGC,
		Details:       gcDetails,
		Progress:      jobspb.SchemaChangeGCProgress{},
		NonCancelable: true,
	}
	if _, err := execCfg.JobRegistry.CreateJobWithTxn(
		ctx, gcJobRecord, execCfg.JobRegistry.MakeJobID(), txn); err != nil {
		return err
	}

	return errors.Wrap(txn.Run(ctx, b), "rolling back tables")
}

func (r *importResumer) dropSchemas(
	ctx context.Context,
	txn *kv.Txn,
	descsCol *descs.Collection,
	execCfg *sql.ExecutorConfig,
	p sql.JobExecContext,
) ([]jobspb.JobID, error) {
	details := r.job.Details().(jobspb.ImportDetails)

	// If the prepare step of the import job was not completed then the
	// descriptors do not need to be rolled back as the txn updating them never
	// completed.
	if !details.PrepareComplete || len(details.Schemas) == 0 {
		return nil, nil
	}

	// Resolve the database descriptor.
	desc, err := descsCol.GetMutableDescriptorByID(ctx, txn, details.ParentID)
	if err != nil {
		return nil, err
	}

	dbDesc, ok := desc.(*dbdesc.Mutable)
	if !ok {
		return nil, errors.Newf("expected ID %d to refer to the database being imported into",
			details.ParentID)
	}

	droppedSchemaIDs := make([]descpb.ID, 0)
	for _, schema := range details.Schemas {
		desc, err := descsCol.GetMutableDescriptorByID(ctx, txn, schema.Desc.ID)
		if err != nil {
			return nil, err
		}
		var schemaDesc *schemadesc.Mutable
		var ok bool
		if schemaDesc, ok = desc.(*schemadesc.Mutable); !ok {
			return nil, errors.Newf("unable to resolve schema desc with ID %d", schema.Desc.ID)
		}

		// Mark the descriptor as dropped and write it to the batch.
		// Delete namespace entry or update draining names depending on version.

		schemaDesc.SetDropped()
		droppedSchemaIDs = append(droppedSchemaIDs, schemaDesc.GetID())

		b := txn.NewBatch()
		// TODO(postamar): remove version gate and else-block in 22.2
		if execCfg.Settings.Version.IsActive(ctx, clusterversion.AvoidDrainingNames) {
			if dbDesc.Schemas != nil {
				delete(dbDesc.Schemas, schemaDesc.GetName())
			}
			b.Del(catalogkeys.EncodeNameKey(p.ExecCfg().Codec, schemaDesc))
		} else {
			//lint:ignore SA1019 removal of deprecated method call scheduled for 22.2
			schemaDesc.AddDrainingName(descpb.NameInfo{
				ParentID:       details.ParentID,
				ParentSchemaID: keys.RootNamespaceID,
				Name:           schemaDesc.Name,
			})
			// Update the parent database with information about the dropped schema.
			dbDesc.AddSchemaToDatabase(schema.Desc.Name, descpb.DatabaseDescriptor_SchemaInfo{ID: dbDesc.ID, Dropped: true})
		}

		if err := descsCol.WriteDescToBatch(ctx, p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
			schemaDesc, b); err != nil {
			return nil, err
		}
		err = txn.Run(ctx, b)
		if err != nil {
			return nil, err
		}
	}

	// Write out the change to the database. This only creates a job record to be
	// run after the txn commits.
	queuedJob, err := writeNonDropDatabaseChange(ctx, dbDesc, txn, descsCol, p, "")
	if err != nil {
		return nil, err
	}

	// Create the job to drop the schema.
	dropSchemaJobRecord := jobs.Record{
		Description:   "dropping schemas as part of an import job rollback",
		Username:      p.User(),
		DescriptorIDs: droppedSchemaIDs,
		Details: jobspb.SchemaChangeDetails{
			DroppedSchemas:    droppedSchemaIDs,
			DroppedDatabaseID: descpb.InvalidID,
			FormatVersion:     jobspb.DatabaseJobFormatVersion,
		},
		Progress:      jobspb.SchemaChangeProgress{},
		NonCancelable: true,
	}
	jobID := p.ExecCfg().JobRegistry.MakeJobID()
	job, err := execCfg.JobRegistry.CreateJobWithTxn(ctx, dropSchemaJobRecord, jobID, txn)
	if err != nil {
		return nil, err
	}
	queuedJob = append(queuedJob, job.ID())

	return queuedJob, nil
}

func (r *importResumer) releaseProtectedTimestamp(
	ctx context.Context, txn *kv.Txn, pts protectedts.Storage,
) error {
	details := r.job.Details().(jobspb.ImportDetails)
	ptsID := details.ProtectedTimestampRecord
	// If the job doesn't have a protected timestamp then there's nothing to do.
	if ptsID == nil {
		return nil
	}
	err := pts.Release(ctx, txn, *ptsID)
	if errors.Is(err, protectedts.ErrNotExists) {
		// No reason to return an error which might cause problems if it doesn't
		// seem to exist.
		log.Warningf(ctx, "failed to release protected which seems not to exist: %v", err)
		err = nil
	}
	return err
}

// ReportResults implements JobResultsReporter interface.
func (r *importResumer) ReportResults(ctx context.Context, resultsCh chan<- tree.Datums) error {
	select {
	case resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(r.job.ID())),
		tree.NewDString(string(jobs.StatusSucceeded)),
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
	)
}
