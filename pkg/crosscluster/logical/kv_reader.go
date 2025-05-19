// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// kvRowReader allows reading the primary key value given a set of datums. This
// is used in cases where a cput fails on an index other than the primary key.
//
// TODO(jeffswenson): delete this after fixing dist sender so that it returns
// deterministic errors.
type kvRowReader struct {
	tableDesc   catalog.TableDescriptor
	indexPrefix roachpb.Key
	colMap      catalog.TableColMap
	colFamilyID descpb.FamilyID

	scratch []byte
}

func newKVRowReader(
	ctx context.Context, codec keys.SQLCodec, tableDesc lease.LeasedDescriptor,
) (*kvRowReader, error) {
	desc := tableDesc.Underlying().(catalog.TableDescriptor)

	columns, err := writeableColumns(ctx, desc)
	if err != nil {
		return nil, err
	}

	var colMap catalog.TableColMap
	for i, col := range columns {
		colMap.Set(col.GetID(), i)
	}

	prefix := rowenc.MakeIndexKeyPrefix(
		codec, tableDesc.GetID(), desc.GetPrimaryIndexID(),
	)

	colFamilies := desc.GetFamilies()
	if len(colFamilies) != 1 {
		return nil, errors.AssertionFailedf("ldr only supports single column family tables")
	}

	return &kvRowReader{
		tableDesc:   desc,
		colMap:      colMap,
		indexPrefix: prefix,
		colFamilyID: colFamilies[0].ID,
	}, nil
}

// GetValue uses the txn to load the value of the row.
func (r *kvRowReader) GetValue(
	ctx context.Context, txn *kv.Txn, row tree.Datums,
) (kv.KeyValue, error) {
	r.scratch = r.scratch[:0]
	r.scratch = append(r.scratch, r.indexPrefix...)

	var containsNull bool
	var err error
	r.scratch, containsNull, err = rowenc.EncodeIndexKey(
		r.tableDesc, r.tableDesc.GetPrimaryIndex(), r.colMap, row, r.scratch,
	)
	if err != nil {
		return kv.KeyValue{}, err
	}
	if containsNull {
		return kv.KeyValue{}, errors.AssertionFailedf("row contains NULL values")
	}

	r.scratch = keys.MakeFamilyKey(r.scratch, uint32(r.colFamilyID))

	return txn.Get(ctx, r.scratch)
}
