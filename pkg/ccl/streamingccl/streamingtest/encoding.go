// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingtest

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
	t *testing.T, codec keys.SQLCodec, descr catalog.TableDescriptor, pkeyVals ...interface{},
) roachpb.KeyValue {
	require.Equal(t, 1, descr.NumFamilies(), "there can be only one")
	primary := descr.GetPrimaryIndex()
	require.LessOrEqual(t, primary.NumKeyColumns(), len(pkeyVals))

	var datums tree.Datums
	var colMap catalog.TableColMap
	for i, val := range pkeyVals {
		datums = append(datums, nativeToDatum(t, val))
		col, err := descr.FindColumnWithID(descpb.ColumnID(i + 1))
		require.NoError(t, err)
		colMap.Set(col.GetID(), col.Ordinal())
	}

	const includeEmpty = true
	indexEntries, err := rowenc.EncodePrimaryIndex(codec, descr, primary,
		colMap, datums, includeEmpty)
	require.NoError(t, err)
	require.Equal(t, 1, len(indexEntries))
	indexEntries[0].Value.InitChecksum(indexEntries[0].Key)
	return roachpb.KeyValue{Key: indexEntries[0].Key, Value: indexEntries[0].Value}
}

func nativeToDatum(t *testing.T, native interface{}) tree.Datum {
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
