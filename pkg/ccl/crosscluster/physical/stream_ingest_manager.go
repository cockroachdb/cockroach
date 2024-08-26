// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package physical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/revertccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/repstream"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catsessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

type streamIngestManagerImpl struct {
	evalCtx     *eval.Context
	jobRegistry *jobs.Registry
	txn         isql.Txn
	sessionID   clusterunique.ID
}

// GetReplicationStatsAndStatus implements streaming.StreamIngestManager interface.
func (r *streamIngestManagerImpl) GetReplicationStatsAndStatus(
	ctx context.Context, ingestionJobID jobspb.JobID,
) (*streampb.StreamIngestionStats, string, error) {
	return getReplicationStatsAndStatus(ctx, r.jobRegistry, r.txn, ingestionJobID)
}

// RevertTenantToTimestamp  implements streaming.StreamIngestManager interface.
func (r *streamIngestManagerImpl) RevertTenantToTimestamp(
	ctx context.Context, tenantName roachpb.TenantName, revertTo hlc.Timestamp,
) error {
	return revertccl.RevertTenantToTimestamp(ctx, r.evalCtx, tenantName, revertTo, r.sessionID)
}

func newStreamIngestManagerWithPrivilegesCheck(
	ctx context.Context, evalCtx *eval.Context, txn isql.Txn, sessionID clusterunique.ID,
) (eval.StreamIngestManager, error) {
	execCfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	enterpriseCheckErr := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings, "REPLICATION")
	if enterpriseCheckErr != nil {
		return nil, pgerror.Wrap(enterpriseCheckErr,
			pgcode.CCLValidLicenseRequired, "physical replication requires an enterprise license on the secondary (and primary) cluster")
	}

	if err := evalCtx.SessionAccessor.CheckPrivilege(ctx,
		syntheticprivilege.GlobalPrivilegeObject,
		privilege.MANAGEVIRTUALCLUSTER); err != nil {
		return nil, err
	}

	return &streamIngestManagerImpl{
		evalCtx:     evalCtx,
		txn:         txn,
		jobRegistry: execCfg.JobRegistry,
		sessionID:   sessionID,
	}, nil
}

func getReplicationStatsAndStatus(
	ctx context.Context, jobRegistry *jobs.Registry, txn isql.Txn, ingestionJobID jobspb.JobID,
) (*streampb.StreamIngestionStats, string, error) {
	job, err := jobRegistry.LoadJobWithTxn(ctx, ingestionJobID, txn)
	if err != nil {
		return nil, jobspb.ReplicationError.String(), err
	}
	details, ok := job.Details().(jobspb.StreamIngestionDetails)
	if !ok {
		return nil, jobspb.ReplicationError.String(),
			errors.Newf("job with id %d is not a stream ingestion job", job.ID())
	}

	details.StreamAddress, err = streamclient.RedactSourceURI(details.StreamAddress)
	if err != nil {
		return nil, jobspb.ReplicationError.String(), err
	}

	stats, err := replicationutils.GetStreamIngestionStats(ctx, details, job.Progress())
	if err != nil {
		return nil, jobspb.ReplicationError.String(), err
	}
	if job.Status() == jobs.StatusPaused {
		return stats, jobspb.ReplicationPaused.String(), nil
	}
	return stats, stats.IngestionProgress.ReplicationStatus.String(), nil
}

func init() {
	repstream.GetStreamIngestManagerHook = newStreamIngestManagerWithPrivilegesCheck
}

// SetupReaderCatalog implements streaming.StreamIngestManager interface.
func (r *streamIngestManagerImpl) SetupReaderCatalog(
	ctx context.Context, from, to roachpb.TenantName, asOf hlc.Timestamp,
) error {
	execCfg := r.evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	var fromID, toID roachpb.TenantID
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		fromTenant, err := sql.GetTenantRecordByName(ctx, r.evalCtx.Settings, txn, from)
		if err != nil {
			return err
		}
		fromID, err = roachpb.MakeTenantID(fromTenant.ID)
		if err != nil {
			return err
		}
		toTenant, err := sql.GetTenantRecordByName(ctx, r.evalCtx.Settings, txn, to)
		if err != nil {
			return err
		}
		toID, err = roachpb.MakeTenantID(toTenant.ID)
		if err != nil {
			return err
		}
		if fromTenant.DataState != mtinfopb.DataStateReady {
			if fromTenant.PhysicalReplicationConsumerJobID == 0 {
				return errors.Newf("cannot copy catalog from tenant %s in state %s", from, fromTenant.DataState)
			}
			job, err := r.jobRegistry.LoadJobWithTxn(ctx, fromTenant.PhysicalReplicationConsumerJobID, txn)
			if err != nil {
				return errors.Wrap(err, "loading tenant replication job")
			}
			progress := job.Progress()
			replicatedTime := replicationutils.ReplicatedTimeFromProgress(&progress)
			if asOf.IsEmpty() {
				asOf = replicatedTime
			} else if replicatedTime.Less(asOf) {
				return errors.Newf("timestamp is not replicated yet")
			}
		} else if asOf.IsEmpty() {
			asOf = execCfg.Clock.Now()
		}
		if toTenant.ServiceMode != mtinfopb.ServiceModeNone && false {
			return errors.Newf("tenant %s must have service stopped to enable reader mode",
				toTenant.Name)
		}

		return nil
	}); err != nil {
		return err
	}

	if fromID.Equal(roachpb.SystemTenantID) || toID.Equal(roachpb.SystemTenantID) {
		return errors.New("cannot revert the system tenant")
	}

	extracted, err := getCatalogForTenantAsOf(ctx, execCfg, fromID, asOf)
	if err != nil {
		return err
	}

	m := mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name:     "tenant_reader",
		Res:      mon.MemoryResource,
		Settings: execCfg.Settings,
	})
	// Inherit session data, so that we can run validation.
	dsdp := catsessiondata.NewDescriptorSessionDataStackProvider(r.evalCtx.SessionDataStack)
	writeDescs := descs.NewBareBonesCollectionFactory(execCfg.Settings, keys.MakeSQLCodec(toID)).
		NewCollection(ctx, descs.WithMonitor(m), descs.WithDescriptorSessionDataProvider(dsdp))
	return execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Reset any state between txn retries.
		defer writeDescs.ReleaseAll(ctx)
		// Resolve any existing descriptors within the tenant, which
		// will be use to compute old values for writing.
		existingDescriptors, err := writeDescs.GetAllFromStorageUnvalidated(ctx, txn)
		if err != nil {
			return err
		}
		b := txn.NewBatch()
		if err := extracted.ForEachDescriptor(func(desc catalog.Descriptor) error {
			if !shouldSetupForReader(desc.GetID(), desc.GetParentID()) {
				return nil
			}
			// If there is an existing descriptor with the same ID, we should
			// determine the old bytes in storage for the upsert.
			var existingRawBytes []byte
			if existingDesc := existingDescriptors.LookupDescriptor(desc.GetID()); existingDesc != nil {
				existingRawBytes = existingDesc.GetRawBytesInStorage()
			}
			var mut catalog.MutableDescriptor
			switch t := desc.DescriptorProto().GetUnion().(type) {
			case *descpb.Descriptor_Table:
				t.Table.ReplicatedVersion = t.Table.Version
				t.Table.Version = 1
				mutBuilder := tabledesc.NewBuilder(t.Table)
				if existingRawBytes != nil {
					mutBuilder.SetRawBytesInStorage(existingRawBytes)
				}
				mutTbl := mutBuilder.BuildCreatedMutable().(*tabledesc.Mutable)
				mut = mutTbl
				// Convert any physical tables into external row tables.
				// Note: Materialized views will be converted, but their
				// view definition will be wiped.
				if mutTbl.IsPhysicalTable() {
					mutTbl.ViewQuery = ""
					mutTbl.SetExternalRowData(&descpb.ExternalRowData{TenantID: fromID, TableID: desc.GetID(), AsOf: asOf})
				}
			case *descpb.Descriptor_Database:
				t.Database.ReplicatedVersion = t.Database.Version
				t.Database.Version = 1
				mutBuilder := dbdesc.NewBuilder(t.Database)
				if existingRawBytes != nil {
					mutBuilder.SetRawBytesInStorage(existingRawBytes)
				}
				mut = mutBuilder.BuildCreatedMutable()
			case *descpb.Descriptor_Schema:
				t.Schema.ReplicatedVersion = t.Schema.Version
				t.Schema.Version = 1
				mutBuilder := schemadesc.NewBuilder(t.Schema)
				if existingRawBytes != nil {
					mutBuilder.SetRawBytesInStorage(existingRawBytes)
				}
				mut = mutBuilder.BuildCreatedMutable()
			case *descpb.Descriptor_Function:
				t.Function.ReplicatedVersion = t.Function.Version
				t.Function.Version = 1
				mutBuilder := funcdesc.NewBuilder(t.Function)
				if existingRawBytes != nil {
					mutBuilder.SetRawBytesInStorage(existingRawBytes)
				}
				mut = mutBuilder.BuildCreatedMutable()
			case *descpb.Descriptor_Type:
				t.Type.ReplicatedVersion = t.Type.Version
				t.Type.Version = 1
				mutBuilder := typedesc.NewBuilder(t.Type)
				if existingRawBytes != nil {
					mutBuilder.SetRawBytesInStorage(existingRawBytes)
				}
				mut = mutBuilder.BuildCreatedMutable()
			}
			return errors.Wrapf(writeDescs.WriteDescToBatch(ctx, true, mut, b),
				"unable to create replicated descriptor: %d %T", mut.GetID(), mut)
		}); err != nil {
			return err
		}
		if err := extracted.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
			if !shouldSetupForReader(e.GetID(), e.GetParentID()) {
				return nil
			}
			return errors.Wrapf(writeDescs.UpsertNamespaceEntryToBatch(ctx, true, e, b), "namespace entry %v", e)
		}); err != nil {
			return err
		}
		return errors.Wrap(txn.Run(ctx, b), "running batch")
	})
}

// shouldSetupForReader determines if a descriptor should be setup
// access via external row data.
func shouldSetupForReader(id descpb.ID, parentSchemaID descpb.ID) bool {
	switch id {
	case keys.UsersTableID, keys.RoleMembersTableID, keys.RoleOptionsTableID,
		keys.DatabaseRoleSettingsTableID, keys.TableStatisticsTableID:
		return true
	default:
		return parentSchemaID != keys.SystemDatabaseID &&
			id != keys.SystemDatabaseID
	}
}

// getCatalogForTenantAsOf reads the descriptors from a given tenant
// at the given timestamp.
func getCatalogForTenantAsOf(
	ctx context.Context, execCfg *sql.ExecutorConfig, tenantID roachpb.TenantID, asOf hlc.Timestamp,
) (all nstree.Catalog, _ error) {
	cf := descs.NewBareBonesCollectionFactory(execCfg.Settings, keys.MakeSQLCodec(tenantID))
	err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := txn.SetFixedTimestamp(ctx, asOf)
		if err != nil {
			return err
		}
		descs := cf.NewCollection(ctx)
		defer descs.ReleaseAll(ctx)
		all, err = descs.GetAllFromStorageUnvalidated(ctx, txn)
		if err != nil {
			return err
		}

		return nil
	})
	return all, err
}
