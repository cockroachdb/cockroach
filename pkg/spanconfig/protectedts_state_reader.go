// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// ProtectedTimestampStateReader provides a target specific view of the
// protected timestamp records stored in the system table.
type ProtectedTimestampStateReader struct {
	schemaObjectProtections map[descpb.ID][]roachpb.ProtectionPolicy
	tenantProtections       []TenantProtectedTimestamps
	clusterProtections      []roachpb.ProtectionPolicy
}

// NewProtectedTimestampStateReader returns an instance of a
// ProtectedTimestampStateReader that can be used to fetch target specific
// protected timestamp records given the supplied ptpb.State. The ptpb.State is
// the transactional state of the `system.protected_ts_records` table.
func NewProtectedTimestampStateReader(
	_ context.Context, ptsState ptpb.State,
) *ProtectedTimestampStateReader {
	reader := &ProtectedTimestampStateReader{
		schemaObjectProtections: make(map[descpb.ID][]roachpb.ProtectionPolicy),
		tenantProtections:       make([]TenantProtectedTimestamps, 0),
		clusterProtections:      make([]roachpb.ProtectionPolicy, 0),
	}
	reader.loadProtectedTimestampRecords(ptsState)
	return reader
}

// GetProtectionPoliciesForCluster returns all the protected timestamps that
// apply to the entire cluster's keyspace.
func (p *ProtectedTimestampStateReader) GetProtectionPoliciesForCluster() []roachpb.ProtectionPolicy {
	return p.clusterProtections
}

// TenantProtectedTimestamps represents all the protections that apply to a
// tenant's keyspace.
type TenantProtectedTimestamps struct {
	Protections []roachpb.ProtectionPolicy
	TenantID    roachpb.TenantID
}

// GetTenantProtections returns the ProtectionPolicies that apply to this tenant.
func (t *TenantProtectedTimestamps) GetTenantProtections() []roachpb.ProtectionPolicy {
	return t.Protections
}

// GetTenantID returns the tenant ID of the tenant that the protected timestamp
// records target.
func (t *TenantProtectedTimestamps) GetTenantID() roachpb.TenantID {
	return t.TenantID
}

// GetProtectionPoliciesForTenants returns all the protected timestamps that
// apply to a particular tenant's keyspace. It returns this for all tenants that
// have protected timestamp records.
func (p *ProtectedTimestampStateReader) GetProtectionPoliciesForTenants() []TenantProtectedTimestamps {
	return p.tenantProtections
}

// ProtectionExistsForTenant returns all the protected timestamps that
// apply to a particular tenant's keyspace.
func (p *ProtectedTimestampStateReader) ProtectionExistsForTenant(tenantID roachpb.TenantID) bool {
	for _, tp := range p.tenantProtections {
		if tp.TenantID.Equal(tenantID) {
			return true
		}
	}
	return false
}

// GetProtectionPoliciesForSchemaObject returns all the protected timestamps
// that apply to the descID's keyspan.
func (p *ProtectedTimestampStateReader) GetProtectionPoliciesForSchemaObject(
	descID descpb.ID,
) []roachpb.ProtectionPolicy {
	return p.schemaObjectProtections[descID]
}

func (p *ProtectedTimestampStateReader) loadProtectedTimestampRecords(ptsState ptpb.State) {
	tenantProtections := make(map[roachpb.TenantID][]roachpb.ProtectionPolicy)
	for _, record := range ptsState.Records {
		// TODO(adityamaru): We should never see this post 22.1 since all records
		// will be written with a target.
		if record.Target == nil {
			continue
		}
		protectionPolicy := roachpb.ProtectionPolicy{
			ProtectedTimestamp:         record.Timestamp,
			IgnoreIfExcludedFromBackup: record.Target.IgnoreIfExcludedFromBackup,
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
			TenantProtectedTimestamps{TenantID: tenID, Protections: tenantProtections})
	}
}
