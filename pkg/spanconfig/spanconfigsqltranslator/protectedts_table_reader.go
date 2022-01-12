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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// ProtectedTimestampTableReader provides an in-memory, transaction scoped view
// of the system table that stores protected timestamp records.
type ProtectedTimestampTableReader struct {
	schemaObjectProtections map[descpb.ID][]hlc.Timestamp
	tenantProtections       map[roachpb.TenantID][]hlc.Timestamp
	clusterProtections      []hlc.Timestamp
}

// newProtectedTimestampTableReader returns an instance of a
// ProtectedTimestampTableReader, after populating the in-memory state
// representing the transaction scoped view of the `system.protected_ts_records`
// table.
func newProtectedTimestampTableReader(
	ctx context.Context, txn *kv.Txn, ptsProvider protectedts.Provider,
) (*ProtectedTimestampTableReader, error) {
	reader := &ProtectedTimestampTableReader{
		schemaObjectProtections: make(map[descpb.ID][]hlc.Timestamp),
		tenantProtections:       make(map[roachpb.TenantID][]hlc.Timestamp),
		clusterProtections:      make([]hlc.Timestamp, 0),
	}
	if err := reader.loadProtectedTimestampRecords(ctx, ptsProvider, txn); err != nil {
		return nil, err
	}
	return reader, nil
}

// GetProtectedTimestampsForCluster returns all the protected timestamps that
// apply to the entire cluster's keyspace.
func (p *ProtectedTimestampTableReader) GetProtectedTimestampsForCluster() []hlc.Timestamp {
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
func (p *ProtectedTimestampTableReader) GetProtectedTimestampsForTenants() []TenantProtectedTimestamps {
	var res []TenantProtectedTimestamps
	for k, v := range p.tenantProtections {
		res = append(res, TenantProtectedTimestamps{Protections: v, TenantID: k})
	}
	return res
}

// GetProtectedTimestampsForSchemaObject returns all the protected timestamps
// that apply to the descID's keyspan.
func (p *ProtectedTimestampTableReader) GetProtectedTimestampsForSchemaObject(
	descID descpb.ID,
) []hlc.Timestamp {
	return p.schemaObjectProtections[descID]
}

func (p *ProtectedTimestampTableReader) loadProtectedTimestampRecords(
	ctx context.Context, ptsProvider protectedts.Provider, txn *kv.Txn,
) error {
	state, err := ptsProvider.GetState(ctx, txn)
	if err != nil {
		return err
	}

	for _, record := range state.Records {
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

// TestingNewProtectedTimestampTableReader is a testing handle that can be used
// to construct a ProtectedTimestampTableReader.
func TestingNewProtectedTimestampTableReader(
	ctx context.Context, txn *kv.Txn, ptp protectedts.Provider,
) (*ProtectedTimestampTableReader, error) {
	return newProtectedTimestampTableReader(ctx, txn, ptp)
}
