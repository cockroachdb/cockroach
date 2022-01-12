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

// ProtectedTimestampStateReader provides a target specific view of the
// protected timestamp records stored in the system table.
type ProtectedTimestampStateReader struct {
	schemaObjectProtections map[descpb.ID][]hlc.Timestamp
	tenantProtections       map[roachpb.TenantID][]hlc.Timestamp
	clusterProtections      []hlc.Timestamp
}

// newProtectedTimestampStateReader returns an instance of a
// ProtectedTimestampStateReader that can be used to fetch target specific
// protected timestamp records given the supplied ptpb.State. The ptpb.State is
// the transactional state of the `system.protected_ts_records` table.
func newProtectedTimestampStateReader(
	_ context.Context, ptsState ptpb.State,
) (*ProtectedTimestampStateReader, error) {
	reader := &ProtectedTimestampStateReader{
		schemaObjectProtections: make(map[descpb.ID][]hlc.Timestamp),
		tenantProtections:       make(map[roachpb.TenantID][]hlc.Timestamp),
		clusterProtections:      make([]hlc.Timestamp, 0),
	}
	if err := reader.loadProtectedTimestampRecords(ptsState); err != nil {
		return nil, err
	}
	return reader, nil
}

// GetProtectedTimestampsForCluster returns all the protected timestamps that
// apply to the entire cluster's keyspace.
func (p *ProtectedTimestampStateReader) GetProtectedTimestampsForCluster() []hlc.Timestamp {
	return p.clusterProtections
}

// TenantProtectedTimestamps represents all the Protections that apply to a
// tenant's keyspace.
type TenantProtectedTimestamps struct {
	Protections []hlc.Timestamp
	TenantID    roachpb.TenantID
}

// GetProtectedTimestampsForTenants returns all the protected timestamps that
// apply to a particular tenant's keyspace. It returns this for all tenants that
// have protected timestamp records.
func (p *ProtectedTimestampStateReader) GetProtectedTimestampsForTenants() []TenantProtectedTimestamps {
	var res []TenantProtectedTimestamps
	for k, v := range p.tenantProtections {
		res = append(res, TenantProtectedTimestamps{Protections: v, TenantID: k})
	}
	return res
}

// GetProtectedTimestampsForSchemaObject returns all the protected timestamps
// that apply to the descID's keyspan.
func (p *ProtectedTimestampStateReader) GetProtectedTimestampsForSchemaObject(
	descID descpb.ID,
) []hlc.Timestamp {
	return p.schemaObjectProtections[descID]
}

func (p *ProtectedTimestampStateReader) loadProtectedTimestampRecords(ptsState ptpb.State) error {
	for _, record := range ptsState.Records {
		switch t := record.Target.GetUnion().(type) {
		case *ptpb.Target_Cluster:
			p.clusterProtections = append(p.clusterProtections, record.Timestamp)
		case *ptpb.Target_Tenants:
			for _, tenID := range t.Tenants.IDs {
				p.tenantProtections[tenID] = append(p.tenantProtections[tenID], record.Timestamp)
			}
		case *ptpb.Target_SchemaObjects:
			for _, descID := range t.SchemaObjects.IDs {
				p.schemaObjectProtections[descID] = append(p.schemaObjectProtections[descID], record.Timestamp)
			}
		}
	}
	return nil
}

// TestingNewProtectedTimestampStateReader is a testing handle that can be used
// to construct a ProtectedTimestampStateReader.
func TestingNewProtectedTimestampStateReader(
	ctx context.Context, ptsState ptpb.State,
) (*ProtectedTimestampStateReader, error) {
	return newProtectedTimestampStateReader(ctx, ptsState)
}
