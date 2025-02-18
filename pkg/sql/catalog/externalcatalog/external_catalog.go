// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package externalcatalog

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/externalcatalog/externalpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/ingesting"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// ExtractExternalCatalog extracts the table descriptors via the schema
// resolver.
func ExtractExternalCatalog(
	ctx context.Context,
	schemaResolver resolver.SchemaResolver,
	txn isql.Txn,
	descCol *descs.Collection,
	includeOffline bool,
	tableNames ...string,
) (externalpb.ExternalCatalog, error) {
	externalCatalog := externalpb.ExternalCatalog{}
	foundTypeDescriptors := make(map[descpb.ID]struct{})

	for _, name := range tableNames {
		uon, err := parser.ParseTableName(name)
		if err != nil {
			return externalpb.ExternalCatalog{}, err
		}
		lookupFlags := tree.ObjectLookupFlags{
			Required:             true,
			DesiredObjectKind:    tree.TableObject,
			IncludeOffline:       includeOffline,
			DesiredTableDescKind: tree.ResolveRequireTableDesc,
			RequireMutable:       includeOffline,
		}
		d, _, err := resolver.ResolveExistingObject(ctx, schemaResolver, uon, lookupFlags)
		if err != nil {
			return externalpb.ExternalCatalog{}, err
		}
		td, ok := d.(catalog.TableDescriptor)
		if !ok {
			return externalpb.ExternalCatalog{}, errors.New("expected table descriptor")
		}
		tableDesc := td.TableDesc()
		externalCatalog.Types, foundTypeDescriptors, err = getUDTsForTable(ctx, txn, descCol, externalCatalog.Types, foundTypeDescriptors, td, tableDesc.ParentID)
		if err != nil {
			return externalpb.ExternalCatalog{}, err
		}
		externalCatalog.Tables = append(externalCatalog.Tables, *tableDesc)
	}
	return externalCatalog, nil
}

func getUDTsForTable(
	ctx context.Context,
	txn isql.Txn,
	descsCol *descs.Collection,
	typeDescriptors []descpb.TypeDescriptor,
	foundTypeDescriptors map[descpb.ID]struct{},
	td catalog.TableDescriptor,
	parentID descpb.ID,
) ([]descpb.TypeDescriptor, map[descpb.ID]struct{}, error) {
	dbDesc, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, parentID)
	if err != nil {
		return nil, nil, err
	}
	typeIDs, _, err := td.GetAllReferencedTypeIDs(dbDesc,
		func(id descpb.ID) (catalog.TypeDescriptor, error) {
			return descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Type(ctx, id)
		})
	if err != nil {
		return nil, nil, errors.Wrap(err, "resolving type descriptors")
	}
	for _, typeID := range typeIDs {
		if _, ok := foundTypeDescriptors[typeID]; ok {
			continue
		}
		foundTypeDescriptors[typeID] = struct{}{}

		typeDesc, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Type(ctx, typeID)
		if err != nil {
			return nil, nil, err
		}
		typeDescriptors = append(typeDescriptors, *typeDesc.TypeDesc())
	}
	return typeDescriptors, foundTypeDescriptors, nil
}

// IngestExternalCatalog ingests the tables in the external catalog into into
// the database and schema.
//
// TODO: provide a list of databaseID/schemaID pairs to ingest into.
func IngestExternalCatalog(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	externalCatalog externalpb.ExternalCatalog,
	txn isql.Txn,
	descsCol *descs.Collection,
	databaseID descpb.ID,
	schemaID descpb.ID,
	setOffline bool,
) (externalpb.ExternalCatalog, error) {

	ingestedCatalog := externalpb.ExternalCatalog{}
	ingestedCatalog.Tables = make([]descpb.TableDescriptor, 0, len(externalCatalog.Tables))

	dbDesc, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, databaseID)
	if err != nil {
		return ingestedCatalog, err
	}
	tablesToWrite := make([]catalog.TableDescriptor, 0, len(externalCatalog.Tables))
	var originalParentID descpb.ID
	for _, table := range externalCatalog.Tables {
		if originalParentID == 0 {
			originalParentID = table.ParentID
		} else if originalParentID != table.ParentID {
			return ingestedCatalog, errors.New("all tables must belong to the same parent")
		}
		newID, err := execCfg.DescIDGenerator.GenerateUniqueDescID(ctx)
		if err != nil {
			return ingestedCatalog, err
		}
		// TODO: rewrite the tables to fresh ids.
		mutTable := tabledesc.NewBuilder(&table).BuildCreatedMutableTable()
		if setOffline {
			// TODO: Add some functional ops so client can set offline msg, among
			// other things.
			mutTable.SetOffline("")
		}
		mutTable.UnexposedParentSchemaID = schemaID
		mutTable.ParentID = dbDesc.GetID()
		mutTable.Version = 1
		mutTable.ID = newID
		tablesToWrite = append(tablesToWrite, mutTable)
		ingestedCatalog.Tables = append(ingestedCatalog.Tables, mutTable.TableDescriptor)
	}
	return ingestedCatalog, ingesting.WriteDescriptors(
		ctx, txn.KV(), user, descsCol, nil, nil, tablesToWrite, nil, nil,
		tree.RequestedDescriptors, nil /* extra */, "", true,
		false, /*allowCrossDatabaseRefs*/
	)
}

func DropIngestedExternalCatalog(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	ingested externalpb.ExternalCatalog,
	txn isql.Txn,
	jr *jobs.Registry,
	descsCol *descs.Collection,
	gcJobDescription string,
) error {
	b := txn.KV().NewBatch()
	const kvTrace = false
	// Collect the tables into mutable versions.
	mutableTables := make([]*tabledesc.Mutable, len(ingested.Tables))
	for i := range ingested.Tables {
		var err error
		mutableTables[i], err = descsCol.MutableByID(txn.KV()).Table(ctx, ingested.Tables[i].GetID())
		if err != nil {
			return err
		}
	}

	// TODO: Add more logic similar to restore rollback.

	// Drop the table descriptors that were created at the start of the restore.
	tablesToGC := make([]descpb.ID, 0, len(ingested.Tables))
	// Set the drop time as 1 (ns in Unix time), so that the table gets GC'd
	// immediately.
	dropTime := int64(1)
	scheduledJobs := jobs.ScheduledJobTxn(txn)
	env := sql.JobSchedulerEnv(execCfg.JobsKnobs())
	for i := range mutableTables {
		tableToDrop := mutableTables[i]
		tablesToGC = append(tablesToGC, tableToDrop.ID)
		tableToDrop.SetDropped()

		// Drop any schedules we may have implicitly created.
		if tableToDrop.HasRowLevelTTL() {
			scheduleID := tableToDrop.RowLevelTTL.ScheduleID
			if scheduleID != 0 {
				if err := scheduledJobs.DeleteByID(ctx, env, scheduleID); err != nil {
					return err
				}
			}
		}

		// Arrange for fast GC of table data.
		//
		// The new (09-2022) GC job uses range deletion tombstones to clear data and
		// then waits for the MVCC GC process to clear the data before removing any
		// descriptors. To ensure that this happens quickly, we install a zone
		// configuration for every table that we are going to drop with a small GC TTL.
		canSetGCTTL := execCfg.Codec.ForSystemTenant() ||
			(sqlclustersettings.SecondaryTenantZoneConfigsEnabled.Get(&execCfg.Settings.SV) &&
				sqlclustersettings.SecondaryTenantsAllZoneConfigsEnabled.Get(&execCfg.Settings.SV))
		canSetGCTTL = canSetGCTTL && tableToDrop.IsPhysicalTable()

		if canSetGCTTL {
			if err := SetGCTTLForDroppingTable(
				ctx, txn, descsCol, tableToDrop,
			); err != nil {
				log.Warningf(ctx, "setting low GC TTL for table %q failed: %s", tableToDrop.GetName(), err.Error())
			}
		} else {
			log.Infof(ctx, "cannot lower GC TTL for table %q", tableToDrop.GetName())
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
		Description:   gcJobDescription,
		Username:      user,
		DescriptorIDs: tablesToGC,
		Details:       gcDetails,
		Progress:      jobspb.SchemaChangeGCProgress{},
		NonCancelable: true,
	}
	if _, err := jr.CreateJobWithTxn(ctx, gcJobRecord, jr.MakeJobID(), txn); err != nil {
		return err
	}

	for _, t := range mutableTables {
		if err := descsCol.WriteDescToBatch(ctx, kvTrace, t, b); err != nil {
			return errors.Wrap(err, "writing dropping table to batch")
		}
		if err := descsCol.DeleteNamespaceEntryToBatch(ctx, kvTrace, t, b); err != nil {
			return errors.Wrap(err, "writing namespace delete to batch")
		}
	}
	return txn.KV().Run(ctx, b)
}

func SetGCTTLForDroppingTable(
	ctx context.Context, txn isql.Txn, descsCol *descs.Collection, tableToDrop *tabledesc.Mutable,
) error {
	log.VInfof(ctx, 2, "lowering TTL for table %q (%d)", tableToDrop.GetName(), tableToDrop.GetID())
	// We get a mutable descriptor here because we are going to construct a
	// synthetic descriptor collection in which they are online.
	dbDesc, err := descsCol.ByIDWithoutLeased(txn.KV()).Get().Database(ctx, tableToDrop.GetParentID())
	if err != nil {
		return err
	}

	schemaDesc, err := descsCol.ByIDWithoutLeased(txn.KV()).Get().Schema(ctx, tableToDrop.GetParentSchemaID())
	if err != nil {
		return err
	}
	tableName := tree.NewTableNameWithSchema(
		tree.Name(dbDesc.GetName()),
		tree.Name(schemaDesc.GetName()),
		tree.Name(tableToDrop.GetName()))

	// Set the db and table to public so that we can use ALTER TABLE below.  At
	// this point,they may be offline.
	mutDBDesc := dbdesc.NewBuilder(dbDesc.DatabaseDesc()).BuildCreatedMutable()
	mutTableDesc := tabledesc.NewBuilder(tableToDrop.TableDesc()).BuildCreatedMutable()
	mutDBDesc.SetPublic()
	mutTableDesc.SetPublic()

	syntheticDescriptors := []catalog.Descriptor{
		mutTableDesc,
		mutDBDesc,
	}
	if schemaDesc.SchemaKind() == catalog.SchemaUserDefined {
		mutSchemaDesc := schemadesc.NewBuilder(schemaDesc.SchemaDesc()).BuildCreatedMutable()
		mutSchemaDesc.SetPublic()
		syntheticDescriptors = append(syntheticDescriptors, mutSchemaDesc)
	}

	alterStmt := fmt.Sprintf("ALTER TABLE %s CONFIGURE ZONE USING gc.ttlseconds = 1", tableName.FQString())
	return txn.WithSyntheticDescriptors(syntheticDescriptors, func() error {
		_, err := txn.Exec(ctx, "set-low-gcttl", txn.KV(), alterStmt)
		return err
	})
}
