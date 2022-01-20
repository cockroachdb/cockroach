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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// protectedTimestampStateReader provides a target specific view of the
// protected timestamp records stored in the system table.
type protectedTimestampStateReader struct {
	schemaObjectProtections map[descpb.ID][]hlc.Timestamp
	tenantProtections       []tenantProtectedTimestamps
	clusterProtections      []hlc.Timestamp
}

// newProtectedTimestampStateReader returns an instance of a
// protectedTimestampStateReader that can be used to fetch target specific
// protected timestamp records given the supplied ptpb.State. The ptpb.State is
// the transactional state of the `system.protected_ts_records` table.
func newProtectedTimestampStateReader(
	_ context.Context, ptsState ptpb.State,
) *protectedTimestampStateReader {
	reader := &protectedTimestampStateReader{
		schemaObjectProtections: make(map[descpb.ID][]hlc.Timestamp),
		tenantProtections:       make([]tenantProtectedTimestamps, 0),
		clusterProtections:      make([]hlc.Timestamp, 0),
	}
	reader.loadProtectedTimestampRecords(ptsState)
	return reader
}

// GetProtectedTimestampsForCluster returns all the protected timestamps that
// apply to the entire cluster's keyspace.
func (p *protectedTimestampStateReader) GetProtectedTimestampsForCluster() []hlc.Timestamp {
	return p.clusterProtections
}

// tenantProtectedTimestamps represents all the protections that apply to a
// tenant's keyspace.
type tenantProtectedTimestamps struct {
	protections []hlc.Timestamp
	tenantID    roachpb.TenantID
}

// GetProtectedTimestampsForTenants returns all the protected timestamps that
// apply to a particular tenant's keyspace. It returns this for all tenants that
// have protected timestamp records.
func (p *protectedTimestampStateReader) GetProtectedTimestampsForTenants() []tenantProtectedTimestamps {
	return p.tenantProtections
}

// GetProtectedTimestampsForSchemaObject returns all the protected timestamps
// that apply to the descID's keyspan.
func (p *protectedTimestampStateReader) GetProtectedTimestampsForSchemaObject(
	descID descpb.ID,
) []hlc.Timestamp {
	return p.schemaObjectProtections[descID]
}

func (p *protectedTimestampStateReader) loadProtectedTimestampRecords(ptsState ptpb.State) {
	tenantProtections := make(map[roachpb.TenantID][]hlc.Timestamp)
	for _, record := range ptsState.Records {
		switch t := record.Target.GetUnion().(type) {
		case *ptpb.Target_Cluster:
			p.clusterProtections = append(p.clusterProtections, record.Timestamp)
		case *ptpb.Target_Tenants:
			for _, tenID := range t.Tenants.IDs {
				tenantProtections[tenID] = append(tenantProtections[tenID], record.Timestamp)
			}
		case *ptpb.Target_SchemaObjects:
			for _, descID := range t.SchemaObjects.IDs {
				p.schemaObjectProtections[descID] = append(p.schemaObjectProtections[descID], record.Timestamp)
			}
		}
	}

	for tenID, tenantProtections := range tenantProtections {
		p.tenantProtections = append(p.tenantProtections,
			tenantProtectedTimestamps{tenantID: tenID, protections: tenantProtections})
	}
}
