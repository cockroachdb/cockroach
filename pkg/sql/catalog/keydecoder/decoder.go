// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package keydecoder provides utilities for decoding encoded SQL keys
// into human-readable schema information.
package keydecoder

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// DecodedKeyInfo contains decoded key information. When full decoding is not
// possible (e.g., table not found, permission denied, index not found), the
// struct is populated with whatever information was available and the Error
// field describes what went wrong.
type DecodedKeyInfo struct {
	TableID      uint32
	IndexID      uint32
	DatabaseName string
	SchemaName   string
	TableName    string
	IndexName    string
	KeyColumns   []DecodedKeyColumn
	// Error is set when the key could not be fully decoded. The struct will
	// contain whatever partial information was available up to the point of
	// failure.
	Error string
}

// DecodedKeyColumn contains information about a single decoded key column.
type DecodedKeyColumn struct {
	Name  string
	Type  string
	Value tree.Datum
}

// AuthzAccessor is an interface for checking authorization.
// If nil is passed to DecodeKey, authorization checks are skipped.
type AuthzAccessor interface {
	HasAnyPrivilege(ctx context.Context, obj privilege.Object) (bool, error)
}

// DecodeKey decodes an encoded key into a DecodedKeyInfo struct.
//
// Parameters:
//   - codec: SQL codec for decoding the key prefix
//   - dc: Descriptor collection for looking up table/index descriptors
//   - txn: KV transaction for reading descriptors
//   - authzAccessor: Optional authorization checker (pass nil to skip auth checks)
//   - key: The encoded key bytes to decode
func DecodeKey(
	ctx context.Context,
	codec keys.SQLCodec,
	dc *descs.Collection,
	txn *kv.Txn,
	authzAccessor AuthzAccessor,
	key []byte,
) (*DecodedKeyInfo, error) {
	// Decode the index prefix to extract table ID and index ID.
	remaining, tableID, indexID, err := codec.DecodeIndexPrefix(key)
	if err != nil {
		return nil, err
	}

	info := &DecodedKeyInfo{
		TableID: tableID,
		IndexID: indexID,
	}

	// Try to look up the table descriptor. Use ByIDWithoutLeased to avoid
	// acquiring descriptor leases that could interfere with concurrent schema changes.
	tableDesc, err := dc.ByIDWithoutLeased(txn).WithoutNonPublic().MaybeGet().Table(ctx, descpb.ID(tableID))
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		info.Error = "table not found"
		return info, nil
	}

	// Check that the user has any privilege on the table.
	if authzAccessor != nil {
		ok, err := authzAccessor.HasAnyPrivilege(ctx, tableDesc)
		if err != nil {
			return nil, err
		}
		if !ok {
			info.Error = "permission denied"
			return info, nil
		}
	}

	info.TableName = tableDesc.GetName()

	// Look up parent database and schema names.
	dbName, schemaName, err := lookupParentNames(ctx, dc, txn, tableDesc)
	if err != nil {
		return nil, err
	}
	info.DatabaseName = dbName
	info.SchemaName = schemaName

	// Look up the index.
	index := catalog.FindIndexByID(tableDesc, descpb.IndexID(indexID))
	if index == nil {
		info.Error = "index not found"
		return info, nil
	}

	info.IndexName = index.GetName()

	// Decode key column values.
	keyColumns, err := decodeKeyColumnInfos(tableDesc, index, remaining)
	if err != nil {
		return nil, err
	}
	info.KeyColumns = keyColumns

	return info, nil
}

// lookupParentNames looks up the database and schema names for a table descriptor.
func lookupParentNames(
	ctx context.Context, dc *descs.Collection, txn *kv.Txn, tableDesc catalog.TableDescriptor,
) (dbName string, schemaName string, err error) {
	parentID := tableDesc.GetParentID()
	if parentID != descpb.InvalidID {
		dbDesc, err := dc.ByIDWithoutLeased(txn).WithoutNonPublic().MaybeGet().Database(ctx, parentID)
		if err != nil {
			return "", "", err
		}
		if dbDesc != nil {
			dbName = dbDesc.GetName()
		}
	}

	parentSchemaID := tableDesc.GetParentSchemaID()
	if parentSchemaID != descpb.InvalidID {
		schemaDesc, err := dc.ByIDWithoutLeased(txn).WithoutNonPublic().MaybeGet().Schema(ctx, parentSchemaID)
		if err != nil {
			return "", "", err
		}
		if schemaDesc != nil {
			schemaName = schemaDesc.GetName()
		}
	}

	return dbName, schemaName, nil
}

// decodeKeyColumnInfos decodes the column values from the key bytes.
func decodeKeyColumnInfos(
	tableDesc catalog.TableDescriptor, index catalog.Index, keyBytes []byte,
) ([]DecodedKeyColumn, error) {
	var da tree.DatumAlloc

	numKeyCols := index.NumKeyColumns()
	numSuffixCols := 0
	if index.GetID() != tableDesc.GetPrimaryIndexID() && !index.IsUnique() {
		numSuffixCols = index.NumKeySuffixColumns()
	}
	totalCols := numKeyCols + numSuffixCols

	result := make([]DecodedKeyColumn, 0, totalCols)

	// Decode key columns.
	for i := 0; i < numKeyCols; i++ {
		colID := index.GetKeyColumnID(i)
		col, err := catalog.MustFindColumnByID(tableDesc, colID)
		if err != nil {
			return nil, err
		}

		dir, err := catalogkeys.IndexColumnEncodingDirection(index.GetKeyColumnDirection(i))
		if err != nil {
			return nil, err
		}

		if len(keyBytes) == 0 {
			break
		}

		datum, remaining, err := keyside.Decode(&da, col.GetType(), keyBytes, dir)
		if err != nil {
			return nil, err
		}
		keyBytes = remaining

		result = append(result, DecodedKeyColumn{
			Name:  col.GetName(),
			Type:  col.GetType().SQLString(),
			Value: datum,
		})
	}

	// Decode key suffix columns for non-unique secondary indexes.
	for i := 0; i < numSuffixCols; i++ {
		colID := index.GetKeySuffixColumnID(i)
		col, err := catalog.MustFindColumnByID(tableDesc, colID)
		if err != nil {
			return nil, err
		}

		if len(keyBytes) == 0 {
			break
		}

		datum, remaining, err := keyside.Decode(&da, col.GetType(), keyBytes, encoding.Ascending)
		if err != nil {
			return nil, err
		}
		keyBytes = remaining

		result = append(result, DecodedKeyColumn{
			Name:  col.GetName(),
			Type:  col.GetType().SQLString(),
			Value: datum,
		})
	}

	return result, nil
}
