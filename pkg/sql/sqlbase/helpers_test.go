// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func testingGetDescriptor(
	ctx context.Context, kvDB *kv.DB, codec keys.SQLCodec, database string, object string,
) (hlc.Timestamp, *descpb.Descriptor) {
	dKey := NewDatabaseKey(database)
	gr, err := kvDB.Get(ctx, dKey.Key(codec))
	if err != nil {
		panic(err)
	}
	if !gr.Exists() {
		panic("database missing")
	}
	dbDescID := descpb.ID(gr.ValueInt())

	tKey := NewPublicTableKey(dbDescID, object)
	gr, err = kvDB.Get(ctx, tKey.Key(codec))
	if err != nil {
		panic(err)
	}
	if !gr.Exists() {
		panic("object missing")
	}

	descKey := MakeDescMetadataKey(codec, descpb.ID(gr.ValueInt()))
	desc := &descpb.Descriptor{}
	ts, err := kvDB.GetProtoTs(ctx, descKey, desc)
	if err != nil || desc.Equal(descpb.Descriptor{}) {
		log.Fatalf(ctx, "proto with id %d missing. err: %v", gr.ValueInt(), err)
	}
	return ts, desc
}

// TestingGetTableDescriptor retrieves a table descriptor directly from the KV
// layer.
func TestingGetTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *descpb.TableDescriptor {
	ctx := context.Background()
	ts, desc := testingGetDescriptor(ctx, kvDB, codec, database, table)
	tableDesc := TableFromDescriptor(desc, ts)
	if tableDesc == nil {
		return nil
	}
	_, err := maybeFillInDescriptor(ctx, kvDB, codec, tableDesc, false)
	if err != nil {
		log.Fatalf(ctx, "failure to fill in descriptor. err: %v", err)
	}
	return tableDesc
}

// TestingGetImmutableTableDescriptor retrieves an immutable table descriptor
// directly from the KV layer.
func TestingGetImmutableTableDescriptor(
	kvDB *kv.DB, codec keys.SQLCodec, database string, table string,
) *ImmutableTableDescriptor {
	return NewImmutableTableDescriptor(*TestingGetTableDescriptor(kvDB, codec, database, table))
}

// ExtractIndexKey constructs the index (primary) key for a row from any index
// key/value entry, including secondary indexes.
//
// Don't use this function in the scan "hot path".
func ExtractIndexKey(
	a *DatumAlloc, codec keys.SQLCodec, tableDesc *ImmutableTableDescriptor, entry kv.KeyValue,
) (roachpb.Key, error) {
	indexID, key, err := DecodeIndexKeyPrefix(codec, tableDesc, entry.Key)
	if err != nil {
		return nil, err
	}
	if indexID == tableDesc.PrimaryIndex.ID {
		return entry.Key, nil
	}

	index, err := tableDesc.FindIndexByID(indexID)
	if err != nil {
		return nil, err
	}

	// Extract the values for index.ColumnIDs.
	indexTypes, err := GetColumnTypes(tableDesc, index.ColumnIDs)
	if err != nil {
		return nil, err
	}
	values := make([]EncDatum, len(index.ColumnIDs))
	dirs := index.ColumnDirections
	if len(index.Interleave.Ancestors) > 0 {
		// TODO(dan): In the interleaved index case, we parse the key twice; once to
		// find the index id so we can look up the descriptor, and once to extract
		// the values. Only parse once.
		var ok bool
		_, ok, _, err = DecodeIndexKey(codec, tableDesc, index, indexTypes, values, dirs, entry.Key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.Errorf("descriptor did not match key")
		}
	} else {
		key, _, err = DecodeKeyVals(indexTypes, values, dirs, key)
		if err != nil {
			return nil, err
		}
	}

	// Extract the values for index.ExtraColumnIDs
	extraTypes, err := GetColumnTypes(tableDesc, index.ExtraColumnIDs)
	if err != nil {
		return nil, err
	}
	extraValues := make([]EncDatum, len(index.ExtraColumnIDs))
	dirs = make([]descpb.IndexDescriptor_Direction, len(index.ExtraColumnIDs))
	for i := range index.ExtraColumnIDs {
		// Implicit columns are always encoded Ascending.
		dirs[i] = descpb.IndexDescriptor_ASC
	}
	extraKey := key
	if index.Unique {
		extraKey, err = entry.Value.GetBytes()
		if err != nil {
			return nil, err
		}
	}
	_, _, err = DecodeKeyVals(extraTypes, extraValues, dirs, extraKey)
	if err != nil {
		return nil, err
	}

	// Encode the index key from its components.
	colMap := make(map[descpb.ColumnID]int)
	for i, columnID := range index.ColumnIDs {
		colMap[columnID] = i
	}
	for i, columnID := range index.ExtraColumnIDs {
		colMap[columnID] = i + len(index.ColumnIDs)
	}
	indexKeyPrefix := MakeIndexKeyPrefix(codec, tableDesc, tableDesc.PrimaryIndex.ID)

	decodedValues := make([]tree.Datum, len(values)+len(extraValues))
	for i, value := range values {
		err := value.EnsureDecoded(indexTypes[i], a)
		if err != nil {
			return nil, err
		}
		decodedValues[i] = value.Datum
	}
	for i, value := range extraValues {
		err := value.EnsureDecoded(extraTypes[i], a)
		if err != nil {
			return nil, err
		}
		decodedValues[len(values)+i] = value.Datum
	}
	indexKey, _, err := EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, colMap, decodedValues, indexKeyPrefix)
	return indexKey, err
}
