// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigprotectedts

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	// ProdSystemProtectedTimestampTable is the system table where we store
	// protected timestamp records.
	ProdSystemProtectedTimestampTable = "system.protected_ts_records"
	tenantKeyPrefix                   = "tenant"
)

type key string

func makeSchemaObjectKey(id descpb.ID) key {
	return key(strconv.FormatUint(uint64(id), 10))
}

func makeTenantKey(tenantID roachpb.TenantID) key {
	return key(fmt.Sprintf("%s%d", tenantKeyPrefix, tenantID.ToUint64()))
}

func isTenantKey(k key) bool {
	return strings.HasPrefix(string(k), tenantKeyPrefix)
}

func getTenantIDFromKey(k key) roachpb.TenantID {
	tenantIDStr := strings.TrimPrefix(string(k), tenantKeyPrefix)
	tenantID, err := strconv.ParseUint(tenantIDStr, 10, 64)
	if err != nil {
		// this should never fail.
		return roachpb.TenantID{}
	}
	return roachpb.MakeTenantID(tenantID)
}

func makeClusterKey() key {
	return "cluster"
}

// ProtectedTimestampTableReader provides an in-memory, transaction scoped view
// of the system table that stores protected timestamp records.
//
// ProtectedTimestampTableReader implements the
// spanconfig.ProtectedTimestampTableReader interface with which the user can
// read from the in-memory store.
type ProtectedTimestampTableReader struct {
	protections map[key][]hlc.Timestamp
}

var _ spanconfig.ProtectedTimestampTableReader = &ProtectedTimestampTableReader{}

// New returns an instance of a ProtectedTimestampTableReader, with the
// in-memory state representing the transaction scoped view of the
// `ptsRecordSystemTable`.
func New(
	ctx context.Context, ptsRecordSystemTable string, ie sqlutil.InternalExecutor, txn *kv.Txn,
) (*ProtectedTimestampTableReader, error) {
	reader := &ProtectedTimestampTableReader{protections: make(map[key][]hlc.Timestamp)}
	if err := reader.loadProtectedTimestampRecords(ctx, ptsRecordSystemTable, ie, txn); err != nil {
		return nil, err
	}
	return reader, nil
}

// GetProtectedTimestampsForCluster implements the ProtectedTimestampTableReader
// interface.
func (p *ProtectedTimestampTableReader) GetProtectedTimestampsForCluster() []hlc.Timestamp {
	return p.protections[makeClusterKey()]
}

// GetProtectedTimestampsForTenants implements the ProtectedTimestampTableReader
// interface.
func (p *ProtectedTimestampTableReader) GetProtectedTimestampsForTenants() []spanconfig.TenantProtectedTimestamps {
	var res []spanconfig.TenantProtectedTimestamps
	for k, v := range p.protections {
		if isTenantKey(k) {
			res = append(res, spanconfig.TenantProtectedTimestamps{Protections: v, TenantID: getTenantIDFromKey(k)})
		}
	}
	return res
}

// GetProtectedTimestampsForSchemaObject implements the ProtectedTimestampTableReader interface.
func (p *ProtectedTimestampTableReader) GetProtectedTimestampsForSchemaObject(
	descID descpb.ID,
) []hlc.Timestamp {
	return p.protections[makeSchemaObjectKey(descID)]
}

type protectedTimestampRow struct {
	ts     hlc.Timestamp
	target *ptpb.Target
}

func (p *ProtectedTimestampTableReader) unmarshalProtectedTimestampRecord(
	row []tree.Datum, cols []colinfo.ResultColumn,
) (protectedTimestampRow, error) {
	var res protectedTimestampRow
	if len(row) != len(cols) {
		return res, errors.AssertionFailedf("expected only %d columns but found %d", len(cols), len(row))
	}

	for i := range cols {
		datum := tree.UnwrapDatum(nil, row[i])
		if datum == tree.DNull {
			// We should never see a null value.
			return res, errors.AssertionFailedf("unexpected NULL in column: %d of row: %+v", i, row)
		}

		switch d := datum.(type) {
		case *tree.DDecimal:
			ts, err := tree.DecimalToHLC(&d.Decimal)
			if err != nil {
				return res, err
			}
			res.ts = ts
		case *tree.DBytes:
			targetBytes := []byte(*d)
			target := &ptpb.Target{}
			if err := protoutil.Unmarshal(targetBytes, target); err != nil {
				return res, errors.Wrapf(err, "failed to unmarshal target column for row: %v", row)
			}
			res.target = target
		default:
			return res, errors.Newf("cannot handle type %T", datum)
		}
	}
	return res, nil
}

func (p *ProtectedTimestampTableReader) loadProtectedTimestampRecords(
	ctx context.Context, ptsRecordsSystemTable string, ie sqlutil.InternalExecutor, txn *kv.Txn,
) (retErr error) {
	getProtectedTimestampRecordStmt := fmt.Sprintf(`SELECT ts, target FROM %s`, ptsRecordsSystemTable)
	it, err := ie.QueryIteratorEx(ctx, "load-pts-records", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		getProtectedTimestampRecordStmt)
	if err != nil {
		return err
	}

	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		ptsRecord, err := p.unmarshalProtectedTimestampRecord(row, it.Types())
		if err != nil {
			return err
		}

		// Add an entry to the in-memory mapping.
		switch t := ptsRecord.target.Union.(type) {
		case *ptpb.Target_Cluster:
			clusterKey := makeClusterKey()
			p.protections[clusterKey] = append(p.protections[clusterKey], ptsRecord.ts)
		case *ptpb.Target_Tenants:
			for _, tenID := range t.Tenants.IDs {
				tenantKey := makeTenantKey(tenID)
				p.protections[tenantKey] = append(p.protections[tenantKey], ptsRecord.ts)
			}
		case *ptpb.Target_SchemaObjects:
			for _, descID := range t.SchemaObjects.IDs {
				descKey := makeSchemaObjectKey(descID)
				p.protections[descKey] = append(p.protections[descKey], ptsRecord.ts)
			}
		}
	}
	return err
}
