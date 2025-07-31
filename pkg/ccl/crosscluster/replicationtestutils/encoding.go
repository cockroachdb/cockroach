// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationtestutils

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

// EncodeKV encodes primary key with the specified "values".  Values must be
// specified in the same order as the columns in the primary family.
func EncodeKV(
	t testing.TB, codec keys.SQLCodec, descr catalog.TableDescriptor, pkeyVals ...interface{},
) roachpb.KeyValue {
	require.Equal(t, 1, descr.NumFamilies(), "there can be only one")
	indexEntries := encodeKVImpl(t, codec, descr, pkeyVals...)
	require.Equal(t, 1, len(indexEntries))
	return roachpb.KeyValue{Key: indexEntries[0].Key, Value: indexEntries[0].Value}
}

// EncodeKVs is similar to EncodeKV, but can be used for a table with multiple
// column families, in which case up to one KV is returned per family.
func EncodeKVs(
	t testing.TB, codec keys.SQLCodec, descr catalog.TableDescriptor, pkeyVals ...interface{},
) []roachpb.KeyValue {
	indexEntries := encodeKVImpl(t, codec, descr, pkeyVals...)
	require.GreaterOrEqual(t, len(indexEntries), 1)
	kvs := make([]roachpb.KeyValue, len(indexEntries))
	for i := range indexEntries {
		kvs[i] = roachpb.KeyValue{Key: indexEntries[i].Key, Value: indexEntries[i].Value}
	}
	return kvs
}

func encodeKVImpl(
	t testing.TB, codec keys.SQLCodec, descr catalog.TableDescriptor, pkeyVals ...interface{},
) []rowenc.IndexEntry {
	primary := descr.GetPrimaryIndex()
	var datums tree.Datums
	var colMap catalog.TableColMap
	for i, val := range pkeyVals {
		datums = append(datums, nativeToDatum(t, val))
		col, err := catalog.MustFindColumnByID(descr, descpb.ColumnID(i+1))
		require.NoError(t, err)
		colMap.Set(col.GetID(), col.Ordinal())
	}

	const includeEmpty = true
	indexEntries, err := rowenc.EncodePrimaryIndex(codec, descr, primary,
		colMap, datums, includeEmpty)
	require.NoError(t, err)
	for i := range indexEntries {
		indexEntries[i].Value.InitChecksum(indexEntries[i].Key)
	}
	return indexEntries
}

func nativeToDatum(t testing.TB, native interface{}) tree.Datum {
	t.Helper()
	switch v := native.(type) {
	case bool:
		return tree.MakeDBool(tree.DBool(v))
	case int:
		return tree.NewDInt(tree.DInt(v))
	case string:
		return tree.NewDString(v)
	case nil:
		return tree.DNull
	case tree.Datum:
		return v
	default:
		t.Fatalf("unexpected value type %T", v)
		return nil
	}
}
