// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqltranslator

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

// protectedTimestampStateReader provides a target specific view of the
// protected timestamp records stored in the system table.
type protectedTimestampStateReader struct {
	schemaObjectProtections map[descpb.ID][]roachpb.ProtectionPolicy
	tenantProtections       []tenantProtectedTimestamps
	clusterProtections      []roachpb.ProtectionPolicy
}

// newProtectedTimestampStateReader returns an instance of a
// protectedTimestampStateReader that can be used to fetch target specific
// protected timestamp records given the supplied ptpb.State. The ptpb.State is
// the transactional state of the `system.protected_ts_records` table.
func newProtectedTimestampStateReader(
	ctx context.Context, jr *jobs.Registry, txn *kv.Txn, ptsState ptpb.State,
) (*protectedTimestampStateReader, error) {
	reader := &protectedTimestampStateReader{
		schemaObjectProtections: make(map[descpb.ID][]roachpb.ProtectionPolicy),
		tenantProtections:       make([]tenantProtectedTimestamps, 0),
		clusterProtections:      make([]roachpb.ProtectionPolicy, 0),
	}
	err := reader.loadProtectedTimestampRecords(ctx, jr, txn, ptsState)
	return reader, err
}

// getProtectionPoliciesForCluster returns all the protected timestamps that
// apply to the entire cluster's keyspace.
func (p *protectedTimestampStateReader) getProtectionPoliciesForCluster() []roachpb.ProtectionPolicy {
	return p.clusterProtections
}

// tenantProtectedTimestamps represents all the protections that apply to a
// tenant's keyspace.
type tenantProtectedTimestamps struct {
	protections []roachpb.ProtectionPolicy
	tenantID    roachpb.TenantID
}

// getProtectionPoliciesForTenants returns all the protected timestamps that
// apply to a particular tenant's keyspace. It returns this for all tenants that
// have protected timestamp records.
func (p *protectedTimestampStateReader) getProtectionPoliciesForTenants() []tenantProtectedTimestamps {
	return p.tenantProtections
}

// getProtectionPoliciesForSchemaObject returns all the protected timestamps
// that apply to the descID's keyspan.
func (p *protectedTimestampStateReader) getProtectionPoliciesForSchemaObject(
	descID descpb.ID,
) []roachpb.ProtectionPolicy {
	return p.schemaObjectProtections[descID]
}

func (p *protectedTimestampStateReader) loadProtectedTimestampRecords(
	ctx context.Context, jr *jobs.Registry, txn *kv.Txn, ptsState ptpb.State,
) error {
	tenantProtections := make(map[roachpb.TenantID][]roachpb.ProtectionPolicy)
	for _, record := range ptsState.Records {
		// TODO(adityamaru): When schedules start writing protected timestamp
		// records extend this logic to also include those records.
		isBackupProtectedTimestampRecord, err := jobsprotectedts.IsRecordWrittenByJobType(ctx, jr, txn,
			record.Meta, jobsprotectedts.Jobs, jobspb.TypeBackup)
		if err != nil {
			return errors.Wrap(err, "failed to check if record was written by a BACKUP")
		}
		protectionPolicy := roachpb.ProtectionPolicy{
			ProtectedTimestamp:         record.Timestamp,
			IgnoreIfExcludedFromBackup: isBackupProtectedTimestampRecord,
		}
		switch t := record.Target.GetUnion().(type) {
		case *ptpb.Target_Cluster:
			p.clusterProtections = append(p.clusterProtections, protectionPolicy)
		case *ptpb.Target_Tenants:
			for _, tenID := range t.Tenants.IDs {
				tenantProtections[tenID] = append(tenantProtections[tenID], protectionPolicy)
			}
		case *ptpb.Target_SchemaObjects:
			for _, descID := range t.SchemaObjects.IDs {
				p.schemaObjectProtections[descID] = append(p.schemaObjectProtections[descID], protectionPolicy)
			}
		}
	}

	for tenID, tenantProtections := range tenantProtections {
		p.tenantProtections = append(p.tenantProtections,
			tenantProtectedTimestamps{tenantID: tenID, protections: tenantProtections})
	}
	return nil
}
