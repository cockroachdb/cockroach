// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowenc_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	. "github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type indexKeyTest struct {
	tableID              descpb.ID
	primaryInterleaves   []descpb.ID
	secondaryInterleaves []descpb.ID
	primaryValues        []tree.Datum // len must be at least primaryInterleaveComponents+1
	secondaryValues      []tree.Datum // len must be at least secondaryInterleaveComponents+1
}

func makeTableDescForTest(test indexKeyTest) (*tabledesc.Immutable, catalog.TableColMap) {
	primaryColumnIDs := make([]descpb.ColumnID, len(test.primaryValues))
	secondaryColumnIDs := make([]descpb.ColumnID, len(test.secondaryValues))
	columns := make([]descpb.ColumnDescriptor, len(test.primaryValues)+len(test.secondaryValues))
	var colMap catalog.TableColMap
	secondaryType := descpb.IndexDescriptor_FORWARD
	for i := range columns {
		columns[i] = descpb.ColumnDescriptor{
			ID: descpb.ColumnID(i + 1),
		}
		colMap.Set(columns[i].ID, i)
		if i < len(test.primaryValues) {
			columns[i].Type = test.primaryValues[i].ResolvedType()
			primaryColumnIDs[i] = columns[i].ID
		} else {
			columns[i].Type = test.secondaryValues[i-len(test.primaryValues)].ResolvedType()
			if colinfo.ColumnTypeIsInvertedIndexable(columns[i].Type) {
				secondaryType = descpb.IndexDescriptor_INVERTED
			}
			secondaryColumnIDs[i-len(test.primaryValues)] = columns[i].ID
		}
	}

	makeInterleave := func(indexID descpb.IndexID, ancestorTableIDs []descpb.ID) descpb.InterleaveDescriptor {
		var interleave descpb.InterleaveDescriptor
		interleave.Ancestors = make([]descpb.InterleaveDescriptor_Ancestor, len(ancestorTableIDs))
		for j, ancestorTableID := range ancestorTableIDs {
			interleave.Ancestors[j] = descpb.InterleaveDescriptor_Ancestor{
				TableID:         ancestorTableID,
				IndexID:         1,
				SharedPrefixLen: 1,
			}
		}
		return interleave
	}

	tableDesc := descpb.TableDescriptor{
		ID:      test.tableID,
		Columns: columns,
		PrimaryIndex: descpb.IndexDescriptor{
			ID:               1,
			ColumnIDs:        primaryColumnIDs,
			ColumnDirections: make([]descpb.IndexDescriptor_Direction, len(primaryColumnIDs)),
			Interleave:       makeInterleave(1, test.primaryInterleaves),
		},
		Indexes: []descpb.IndexDescriptor{{
			ID:               2,
			ColumnIDs:        secondaryColumnIDs,
			ExtraColumnIDs:   primaryColumnIDs,
			Unique:           true,
			ColumnDirections: make([]descpb.IndexDescriptor_Direction, len(secondaryColumnIDs)),
			Interleave:       makeInterleave(2, test.secondaryInterleaves),
			Type:             secondaryType,
		}},
	}
	return tabledesc.NewImmutable(tableDesc), colMap
}

func decodeIndex(
	codec keys.SQLCodec, tableDesc *tabledesc.Immutable, index *descpb.IndexDescriptor, key []byte,
) ([]tree.Datum, error) {
	types, err := colinfo.GetColumnTypes(tableDesc, index.ColumnIDs, nil)
	if err != nil {
		return nil, err
	}
	values := make([]EncDatum, len(index.ColumnIDs))
	colDirs := index.ColumnDirections
	_, ok, _, err := DecodeIndexKey(codec, tableDesc, index, types, values, colDirs, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Errorf("key did not match descriptor")
	}

	decodedValues := make([]tree.Datum, len(values))
	var da DatumAlloc
	for i, value := range values {
		err := value.EnsureDecoded(types[i], &da)
		if err != nil {
			return nil, err
		}
		decodedValues[i] = value.Datum
	}

	return decodedValues, nil
}

func TestIndexKey(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	var a DatumAlloc

	tests := []indexKeyTest{
		{50, nil, nil,
			[]tree.Datum{tree.NewDInt(10)},
			[]tree.Datum{tree.NewDInt(20)},
		},
		{50, []descpb.ID{100}, nil,
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11)},
			[]tree.Datum{tree.NewDInt(20)},
		},
		{50, []descpb.ID{100, 200}, nil,
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11), tree.NewDInt(12)},
			[]tree.Datum{tree.NewDInt(20)},
		},
		{50, nil, []descpb.ID{100},
			[]tree.Datum{tree.NewDInt(10)},
			[]tree.Datum{tree.NewDInt(20), tree.NewDInt(21)},
		},
		{50, []descpb.ID{100}, []descpb.ID{100},
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11)},
			[]tree.Datum{tree.NewDInt(20), tree.NewDInt(21)},
		},
		{50, []descpb.ID{100}, []descpb.ID{200},
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11)},
			[]tree.Datum{tree.NewDInt(20), tree.NewDInt(21)},
		},
		{50, []descpb.ID{100, 200}, []descpb.ID{100, 300},
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11), tree.NewDInt(12)},
			[]tree.Datum{tree.NewDInt(20), tree.NewDInt(21), tree.NewDInt(22)},
		},
	}

	for i := 0; i < 1000; i++ {
		var t indexKeyTest

		t.primaryInterleaves = make([]descpb.ID, rng.Intn(10))
		for j := range t.primaryInterleaves {
			t.primaryInterleaves[j] = descpb.ID(1 + rng.Intn(10))
		}
		valuesLen := randutil.RandIntInRange(rng, len(t.primaryInterleaves)+1, len(t.primaryInterleaves)+10)
		t.primaryValues = make([]tree.Datum, valuesLen)
		for j := range t.primaryValues {
			t.primaryValues[j] = RandDatum(rng, types.Int, true)
		}

		t.secondaryInterleaves = make([]descpb.ID, rng.Intn(10))
		for j := range t.secondaryInterleaves {
			t.secondaryInterleaves[j] = descpb.ID(1 + rng.Intn(10))
		}
		valuesLen = randutil.RandIntInRange(rng, len(t.secondaryInterleaves)+1, len(t.secondaryInterleaves)+10)
		t.secondaryValues = make([]tree.Datum, valuesLen)
		for j := range t.secondaryValues {
			t.secondaryValues[j] = RandDatum(rng, types.Int, true)
		}

		tests = append(tests, t)
	}

	for i, test := range tests {
		evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		defer evalCtx.Stop(context.Background())
		tableDesc, colMap := makeTableDescForTest(test)
		// Add the default family to each test, since secondary indexes support column families.
		var (
			colNames []string
			colIDs   descpb.ColumnIDs
		)
		for _, c := range tableDesc.Columns {
			colNames = append(colNames, c.Name)
			colIDs = append(colIDs, c.ID)
		}
		tableDesc.Families = []descpb.ColumnFamilyDescriptor{{
			Name:            "defaultFamily",
			ID:              0,
			ColumnNames:     colNames,
			ColumnIDs:       colIDs,
			DefaultColumnID: colIDs[0],
		}}

		testValues := append(test.primaryValues, test.secondaryValues...)

		codec := keys.SystemSQLCodec
		primaryKeyPrefix := MakeIndexKeyPrefix(codec, tableDesc, tableDesc.GetPrimaryIndexID())
		primaryKey, _, err := EncodeIndexKey(tableDesc, tableDesc.GetPrimaryIndex().IndexDesc(), colMap, testValues, primaryKeyPrefix)
		if err != nil {
			t.Fatal(err)
		}
		primaryValue := roachpb.MakeValueFromBytes(nil)
		primaryIndexKV := kv.KeyValue{Key: primaryKey, Value: &primaryValue}

		secondaryIndexEntry, err := EncodeSecondaryIndex(
			codec, tableDesc, tableDesc.PublicNonPrimaryIndexes()[0].IndexDesc(), colMap, testValues, true /* includeEmpty */)
		if len(secondaryIndexEntry) != 1 {
			t.Fatalf("expected 1 index entry, got %d. got %#v", len(secondaryIndexEntry), secondaryIndexEntry)
		}
		if err != nil {
			t.Fatal(err)
		}
		secondaryIndexKV := kv.KeyValue{
			Key:   secondaryIndexEntry[0].Key,
			Value: &secondaryIndexEntry[0].Value,
		}

		checkEntry := func(index *descpb.IndexDescriptor, entry kv.KeyValue) {
			values, err := decodeIndex(codec, tableDesc, index, entry.Key)
			if err != nil {
				t.Fatal(err)
			}

			for j, value := range values {
				testValue := testValues[colMap.GetDefault(index.ColumnIDs[j])]
				if value.Compare(evalCtx, testValue) != 0 {
					t.Fatalf("%d: value %d got %q but expected %q", i, j, value, testValue)
				}
			}

			indexID, _, err := DecodeIndexKeyPrefix(codec, tableDesc, entry.Key)
			if err != nil {
				t.Fatal(err)
			}
			if indexID != index.ID {
				t.Errorf("%d", i)
			}

			extracted, err := ExtractIndexKey(&a, codec, tableDesc, entry)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(extracted, primaryKey) {
				t.Errorf("%d got %s <%x>, but expected %s <%x>", i, extracted, []byte(extracted), roachpb.Key(primaryKey), primaryKey)
			}
		}

		checkEntry(tableDesc.GetPrimaryIndex().IndexDesc(), primaryIndexKV)
		checkEntry(tableDesc.PublicNonPrimaryIndexes()[0].IndexDesc(), secondaryIndexKV)
	}
}

func TestInvertedIndexKey(t *testing.T) {
	parseJSON := func(s string) *tree.DJSON {
		j, err := json.ParseJSON(s)
		if err != nil {
			t.Fatalf("Failed to parse %s: %v", s, err)
		}
		return tree.NewDJSON(j)
	}

	tests := []struct {
		value                           tree.Datum
		expectedKeys                    int
		expectedKeysExcludingEmptyArray int
	}{
		{
			value: &tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{},
			},
			expectedKeys:                    1,
			expectedKeysExcludingEmptyArray: 0,
		},
		{
			value: &tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{tree.NewDInt(1)},
			},
			expectedKeys:                    1,
			expectedKeysExcludingEmptyArray: 1,
		},
		{
			value: &tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{tree.NewDString("foo")},
			},
			expectedKeys:                    1,
			expectedKeysExcludingEmptyArray: 1,
		},
		{
			value: &tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{tree.NewDInt(1), tree.NewDInt(2), tree.NewDInt(1)},
			},
			// The keys should be deduplicated.
			expectedKeys:                    2,
			expectedKeysExcludingEmptyArray: 2,
		},
		{
			value:                           parseJSON(`{}`),
			expectedKeys:                    1,
			expectedKeysExcludingEmptyArray: 1,
		},
		{
			value:                           parseJSON(`[]`),
			expectedKeys:                    1,
			expectedKeysExcludingEmptyArray: 1,
		},
		{
			value: parseJSON(`[1, 2, 2]`),
			// The keys should be deduplicated.
			expectedKeys:                    2,
			expectedKeysExcludingEmptyArray: 2,
		},
		{
			value:                           parseJSON(`true`),
			expectedKeys:                    1,
			expectedKeysExcludingEmptyArray: 1,
		},
		{
			value:                           parseJSON(`null`),
			expectedKeys:                    1,
			expectedKeysExcludingEmptyArray: 1,
		},
		{
			value:                           parseJSON(`1`),
			expectedKeys:                    1,
			expectedKeysExcludingEmptyArray: 1,
		},
		{
			value:                           parseJSON(`{"a": "b"}`),
			expectedKeys:                    1,
			expectedKeysExcludingEmptyArray: 1,
		},
		{
			value:                           parseJSON(`{"a": "b", "c": {"d": "e", "f": "g"}}`),
			expectedKeys:                    3,
			expectedKeysExcludingEmptyArray: 3,
		},
	}

	runTest := func(value tree.Datum, expectedKeys int, version descpb.IndexDescriptorVersion) {
		primaryValues := []tree.Datum{tree.NewDInt(10)}
		secondaryValues := []tree.Datum{value}
		tableDesc, colMap := makeTableDescForTest(
			indexKeyTest{50, nil, nil,
				primaryValues, secondaryValues,
			})
		for _, idx := range tableDesc.PublicNonPrimaryIndexes() {
			idx.IndexDesc().Version = version
		}

		testValues := append(primaryValues, secondaryValues...)

		codec := keys.SystemSQLCodec

		secondaryIndexEntries, err := EncodeSecondaryIndex(
			codec, tableDesc, tableDesc.PublicNonPrimaryIndexes()[0].IndexDesc(), colMap, testValues, true /* includeEmpty */)
		if err != nil {
			t.Fatal(err)
		}
		if len(secondaryIndexEntries) != expectedKeys {
			t.Fatalf("For %s expected %d index entries, got %d. got %#v",
				value, expectedKeys, len(secondaryIndexEntries), secondaryIndexEntries,
			)
		}
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("TestInvertedIndexKey %d", i), func(t *testing.T) {
			runTest(test.value, test.expectedKeys, descpb.EmptyArraysInInvertedIndexesVersion)
			runTest(test.value, test.expectedKeysExcludingEmptyArray, descpb.SecondaryIndexFamilyFormatVersion)
		})
	}
}

func TestEncodeContainingArrayInvertedIndexSpans(t *testing.T) {
	testCases := []struct {
		value    string
		contains string
		expected bool
		unique   bool
	}{
		// This test uses EncodeInvertedIndexTableKeys and EncodeContainingInvertedIndexSpans
		// to determine whether the first Array value contains the second. If the first
		// value contains the second, expected is true. Otherwise it is false.
		//
		// If EncodeContainingInvertedIndexSpans produces spans that are guaranteed not to
		// contain duplicate primary keys, unique is true. Otherwise it is false.
		{`{}`, `{}`, true, false},
		{`{}`, `{1}`, false, true},
		{`{1}`, `{}`, true, false},
		{`{1}`, `{1}`, true, true},
		{`{1}`, `{1, 2}`, false, false},
		{`{1, 2}`, `{1}`, true, true},
		{`{1, 2}`, `{2}`, true, true},
		{`{1, 2}`, `{1, 2}`, true, false},
		{`{1, 2}`, `{1, 2, 1}`, true, false},
		{`{1, 2}`, `{1, 1}`, true, true},
		{`{1, 2, 3}`, `{1, 2, 4}`, false, false},
		{`{1, 2, 3}`, `{}`, true, false},
		{`{}`, `{NULL}`, false, true},
		{`{NULL}`, `{}`, true, false},
		{`{NULL}`, `{NULL}`, false, true},
		// unique is true in the case below since any array containing a NULL must
		// return empty spans, which are trivially unique.
		{`{2, NULL}`, `{2, NULL}`, false, true},
		{`{2, NULL}`, `{2}`, true, true},
		{`{2, NULL}`, `{NULL}`, false, true},
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	parseArray := func(s string) tree.Datum {
		arr, _, err := tree.ParseDArrayFromString(&evalCtx, s, types.Int)
		if err != nil {
			t.Fatalf("Failed to parse array %s: %v", s, err)
		}
		return arr
	}

	version := descpb.EmptyArraysInInvertedIndexesVersion
	runTest := func(left, right tree.Datum, expected, expectUnique bool) {
		keys, err := EncodeInvertedIndexTableKeys(left, nil, version)
		require.NoError(t, err)

		spansSlice, tight, unique, err := EncodeContainingInvertedIndexSpans(&evalCtx, right, nil, version)
		require.NoError(t, err)

		// Array spans are always tight.
		if tight != true {
			t.Errorf("For %s, expected tight=%v, but got %v", right, true, tight)
		}

		if unique != expectUnique {
			t.Errorf("For %s, expected unique=%v, but got %v", right, expectUnique, unique)
		}

		// The spans returned by EncodeContainingInvertedIndexSpans represent the
		// intersection of unions. So the below logic is performing a union on the
		// inner loop (any span in the slice can contain any of the keys), and an
		// intersection on the outer loop (all of the span slices must contain at
		// least one key).
		actual := len(spansSlice) > 0
		for _, spans := range spansSlice {
			found := false
			for _, span := range spans {
				for _, key := range keys {
					if span.ContainsKey(key) {
						found = true
						break
					}
				}
				if found == true {
					break
				}
			}
			actual = actual && found
		}

		if actual != expected {
			if expected {
				t.Errorf("expected %s to contain %s but it did not", left, right)
			} else {
				t.Errorf("expected %s not to contain %s but it did", left, right)
			}
		}
	}

	// Run pre-defined test cases from above.
	for _, c := range testCases {
		value, contains := parseArray(c.value), parseArray(c.contains)

		// First check that evaluating `value @> contains` matches the expected
		// result.
		res, err := tree.ArrayContains(&evalCtx, value.(*tree.DArray), contains.(*tree.DArray))
		require.NoError(t, err)
		if bool(*res) != c.expected {
			t.Fatalf(
				"expected value of %s @> %s did not match actual value. Expected: %v. Got: %s",
				c.value, c.contains, c.expected, res.String(),
			)
		}

		// Now check that we get the same result with the inverted index spans.
		runTest(value, contains, c.expected, c.unique)
	}

	// Run a set of randomly generated test cases.
	rng, _ := randutil.NewPseudoRand()
	for i := 0; i < 100; i++ {
		typ := RandArrayType(rng)

		// Generate two random arrays and evaluate the result of `left @> right`.
		left := RandArray(rng, typ, 0 /* nullChance */)
		right := RandArray(rng, typ, 0 /* nullChance */)

		res, err := tree.ArrayContains(&evalCtx, left.(*tree.DArray), right.(*tree.DArray))
		require.NoError(t, err)

		// The spans should not have duplicate values if there is exactly one
		// element after de-duplication.
		arr := right.(*tree.DArray).Array
		expectUnique := len(arr) > 0
		for i := range arr {
			if i > 0 && arr[i].Compare(&evalCtx, arr[0]) != 0 {
				expectUnique = false
				break
			}
		}

		// Now check that we get the same result with the inverted index spans.
		runTest(left, right, bool(*res), expectUnique)
	}
}

type arrayEncodingTest struct {
	name     string
	datum    tree.DArray
	encoding []byte
}

func TestArrayEncoding(t *testing.T) {
	tests := []arrayEncodingTest{
		{
			"empty int array",
			tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{},
			},
			[]byte{1, 3, 0},
		}, {
			"single int array",
			tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{tree.NewDInt(1)},
			},
			[]byte{1, 3, 1, 2},
		}, {
			"multiple int array",
			tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{tree.NewDInt(1), tree.NewDInt(2), tree.NewDInt(3)},
			},
			[]byte{1, 3, 3, 2, 4, 6},
		}, {
			"string array",
			tree.DArray{
				ParamTyp: types.String,
				Array:    tree.Datums{tree.NewDString("foo"), tree.NewDString("bar"), tree.NewDString("baz")},
			},
			[]byte{1, 6, 3, 3, 102, 111, 111, 3, 98, 97, 114, 3, 98, 97, 122},
		}, {
			"name array",
			tree.DArray{
				ParamTyp: types.Name,
				Array:    tree.Datums{tree.NewDName("foo"), tree.NewDName("bar"), tree.NewDName("baz")},
			},
			[]byte{1, 6, 3, 3, 102, 111, 111, 3, 98, 97, 114, 3, 98, 97, 122},
		},
		{
			"bool array",
			tree.DArray{
				ParamTyp: types.Bool,
				Array:    tree.Datums{tree.MakeDBool(true), tree.MakeDBool(false)},
			},
			[]byte{1, 10, 2, 10, 11},
		}, {
			"array containing a single null",
			tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{tree.DNull},
				HasNulls: true,
			},
			[]byte{17, 3, 1, 1},
		}, {
			"array containing multiple nulls",
			tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{tree.NewDInt(1), tree.DNull, tree.DNull},
				HasNulls: true,
			},
			[]byte{17, 3, 3, 6, 2},
		}, {
			"array whose NULL bitmap spans exactly one byte",
			tree.DArray{
				ParamTyp: types.Int,
				Array: tree.Datums{
					tree.NewDInt(1), tree.DNull, tree.DNull, tree.NewDInt(2), tree.NewDInt(3),
					tree.NewDInt(4), tree.NewDInt(5), tree.NewDInt(6),
				},
				HasNulls: true,
			},
			[]byte{17, 3, 8, 6, 2, 4, 6, 8, 10, 12},
		}, {
			"array whose NULL bitmap spans more than one byte",
			tree.DArray{
				ParamTyp: types.Int,
				Array: tree.Datums{
					tree.NewDInt(1), tree.DNull, tree.DNull, tree.NewDInt(2), tree.NewDInt(3),
					tree.NewDInt(4), tree.NewDInt(5), tree.NewDInt(6), tree.DNull,
				},
				HasNulls: true,
			},
			[]byte{17, 3, 9, 6, 1, 2, 4, 6, 8, 10, 12},
		},
	}

	for _, test := range tests {
		t.Run("encode "+test.name, func(t *testing.T) {
			enc, err := EncodeArray(&test.datum, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(enc, test.encoding) {
				t.Fatalf("expected %s to encode to %v, got %v", test.datum.String(), test.encoding, enc)
			}
		})

		t.Run("decode "+test.name, func(t *testing.T) {
			enc := make([]byte, 0)
			enc = append(enc, byte(len(test.encoding)))
			enc = append(enc, test.encoding...)
			d, _, err := DecodeArray(&DatumAlloc{}, test.datum.ParamTyp, enc)
			hasNulls := d.(*tree.DArray).HasNulls
			if test.datum.HasNulls != hasNulls {
				t.Fatalf("expected %v to have HasNulls=%t, got %t", enc, test.datum.HasNulls, hasNulls)
			}
			if err != nil {
				t.Fatal(err)
			}
			evalContext := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			if d.Compare(evalContext, &test.datum) != 0 {
				t.Fatalf("expected %v to decode to %s, got %s", enc, test.datum.String(), d.String())
			}
		})
	}
}

func BenchmarkArrayEncoding(b *testing.B) {
	ary := tree.DArray{ParamTyp: types.Int, Array: tree.Datums{}}
	for i := 0; i < 10000; i++ {
		_ = ary.Append(tree.NewDInt(1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeArray(&ary, nil)
	}
}

func TestMarshalColumnValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		typ   *types.T
		datum tree.Datum
		exp   roachpb.Value
	}{
		{
			typ:   types.Bool,
			datum: tree.MakeDBool(true),
			exp:   func() (v roachpb.Value) { v.SetBool(true); return }(),
		},
		{
			typ:   types.Bool,
			datum: tree.MakeDBool(false),
			exp:   func() (v roachpb.Value) { v.SetBool(false); return }(),
		},
		{
			typ:   types.Int,
			datum: tree.NewDInt(314159),
			exp:   func() (v roachpb.Value) { v.SetInt(314159); return }(),
		},
		{
			typ:   types.Float,
			datum: tree.NewDFloat(3.14159),
			exp:   func() (v roachpb.Value) { v.SetFloat(3.14159); return }(),
		},
		{
			typ: types.Decimal,
			datum: func() (v tree.Datum) {
				v, err := tree.ParseDDecimal("1234567890.123456890")
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				return
			}(),
			exp: func() (v roachpb.Value) {
				dDecimal, err := tree.ParseDDecimal("1234567890.123456890")
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				err = v.SetDecimal(&dDecimal.Decimal)
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				return
			}(),
		},
		{
			typ:   types.Date,
			datum: tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(314159)),
			exp:   func() (v roachpb.Value) { v.SetInt(314159); return }(),
		},
		{
			typ:   types.Date,
			datum: tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(math.MinInt64)),
			exp:   func() (v roachpb.Value) { v.SetInt(math.MinInt64); return }(),
		},
		{
			typ:   types.Date,
			datum: tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(math.MaxInt64)),
			exp:   func() (v roachpb.Value) { v.SetInt(math.MaxInt64); return }(),
		},
		{
			typ:   types.Time,
			datum: tree.MakeDTime(timeofday.FromInt(314159)),
			exp:   func() (v roachpb.Value) { v.SetInt(314159); return }(),
		},
		{
			typ:   types.Timestamp,
			datum: tree.MustMakeDTimestamp(timeutil.Unix(314159, 1000), time.Microsecond),
			exp:   func() (v roachpb.Value) { v.SetTime(timeutil.Unix(314159, 1000)); return }(),
		},
		{
			typ:   types.TimestampTZ,
			datum: tree.MustMakeDTimestampTZ(timeutil.Unix(314159, 1000), time.Microsecond),
			exp:   func() (v roachpb.Value) { v.SetTime(timeutil.Unix(314159, 1000)); return }(),
		},
		{
			typ:   types.String,
			datum: tree.NewDString("testing123"),
			exp:   func() (v roachpb.Value) { v.SetString("testing123"); return }(),
		},
		{
			typ:   types.Name,
			datum: tree.NewDName("testingname123"),
			exp:   func() (v roachpb.Value) { v.SetString("testingname123"); return }(),
		},
		{
			typ:   types.Bytes,
			datum: tree.NewDBytes(tree.DBytes([]byte{0x31, 0x41, 0x59})),
			exp:   func() (v roachpb.Value) { v.SetBytes([]byte{0x31, 0x41, 0x59}); return }(),
		},
		{
			typ: types.Uuid,
			datum: func() (v tree.Datum) {
				v, err := tree.ParseDUuidFromString("63616665-6630-3064-6465-616462656562")
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				return
			}(),
			exp: func() (v roachpb.Value) {
				dUUID, err := tree.ParseDUuidFromString("63616665-6630-3064-6465-616462656562")
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				v.SetBytes(dUUID.GetBytes())
				return
			}(),
		},
		{
			typ: types.INet,
			datum: func() (v tree.Datum) {
				v, err := tree.ParseDIPAddrFromINetString("192.168.0.1")
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				return
			}(),
			exp: func() (v roachpb.Value) {
				ipAddr, err := tree.ParseDIPAddrFromINetString("192.168.0.1")
				if err != nil {
					t.Fatalf("Unexpected error while creating expected value: %s", err)
				}
				data := ipAddr.ToBuffer(nil)
				v.SetBytes(data)
				return
			}(),
		},
	}

	for i, testCase := range tests {
		typ := testCase.typ
		col := descpb.ColumnDescriptor{ID: descpb.ColumnID(typ.Family() + 1), Type: typ}

		if actual, err := MarshalColumnValue(&col, testCase.datum); err != nil {
			t.Errorf("%d: unexpected error with column type %v: %v", i, typ, err)
		} else if !reflect.DeepEqual(actual, testCase.exp) {
			t.Errorf("%d: MarshalColumnValue() got %v, expected %v", i, actual, testCase.exp)
		}
	}
}

type interleaveTableArgs struct {
	indexKeyArgs indexKeyTest
	values       []tree.Datum
}

type interleaveInfo struct {
	tableID  uint64
	values   []tree.Datum
	equivSig []byte
	children map[string]*interleaveInfo
}

func createHierarchy() map[string]*interleaveInfo {
	return map[string]*interleaveInfo{
		"t1": {
			tableID: 50,
			values:  []tree.Datum{tree.NewDInt(10)},
			children: map[string]*interleaveInfo{
				"t2": {
					tableID: 100,
					values:  []tree.Datum{tree.NewDInt(10), tree.NewDInt(15)},
				},
				"t3": {
					tableID: 150,
					values:  []tree.Datum{tree.NewDInt(10), tree.NewDInt(20)},
					children: map[string]*interleaveInfo{
						"t4": {
							tableID: 20,
							values:  []tree.Datum{tree.NewDInt(10), tree.NewDInt(30)},
						},
					},
				},
			},
		},
	}
}

type equivSigTestCases struct {
	name     string
	table    interleaveTableArgs
	expected [][]byte
}

func createEquivTCs(hierarchy map[string]*interleaveInfo) []equivSigTestCases {
	return []equivSigTestCases{
		{
			name: "NoAncestors",
			table: interleaveTableArgs{
				indexKeyArgs: indexKeyTest{tableID: 50},
				values:       []tree.Datum{tree.NewDInt(10)},
			},
			expected: [][]byte{hierarchy["t1"].equivSig},
		},

		{
			name: "OneAncestor",
			table: interleaveTableArgs{
				indexKeyArgs: indexKeyTest{tableID: 100, primaryInterleaves: []descpb.ID{50}},
				values:       []tree.Datum{tree.NewDInt(10), tree.NewDInt(20)},
			},
			expected: [][]byte{hierarchy["t1"].equivSig, hierarchy["t1"].children["t2"].equivSig},
		},

		{
			name: "TwoAncestors",
			table: interleaveTableArgs{
				indexKeyArgs: indexKeyTest{tableID: 20, primaryInterleaves: []descpb.ID{50, 150}},
				values:       []tree.Datum{tree.NewDInt(10), tree.NewDInt(20), tree.NewDInt(30)},
			},
			expected: [][]byte{hierarchy["t1"].equivSig, hierarchy["t1"].children["t3"].equivSig, hierarchy["t1"].children["t3"].children["t4"].equivSig},
		},
	}
}

// equivSignatures annotates the hierarchy with the equivalence signatures
// for each table and returns an in-order depth-first traversal of the
// equivalence signatures.
func equivSignatures(
	hierarchy map[string]*interleaveInfo, parent []byte, signatures [][]byte,
) [][]byte {
	for _, info := range hierarchy {
		// Reset the reference to the parent for every child.
		curParent := parent
		curParent = encoding.EncodeUvarintAscending(curParent, info.tableID)
		// Primary ID is always 1
		curParent = encoding.EncodeUvarintAscending(curParent, 1)
		info.equivSig = make([]byte, len(curParent))
		copy(info.equivSig, curParent)
		signatures = append(signatures, info.equivSig)
		if len(info.children) > 0 {
			curParent = encoding.EncodeInterleavedSentinel(curParent)
			signatures = equivSignatures(info.children, curParent, signatures)
		}
	}
	return signatures
}

func TestIndexKeyEquivSignature(t *testing.T) {
	hierarchy := createHierarchy()
	hierarchySigs := equivSignatures(hierarchy, nil /*parent*/, nil /*signatures*/)
	// validEquivSigs is necessary for IndexKeyEquivSignatures.
	validEquivSigs := make(map[string]int)
	for i, sig := range hierarchySigs {
		validEquivSigs[string(sig)] = i
	}

	// Required buffers when extracting the index key's equivalence signature.
	var keySigBuf, keyRestBuf []byte

	for _, tc := range createEquivTCs(hierarchy) {
		t.Run(tc.name, func(t *testing.T) {
			// We need to initialize this for makeTableDescForTest.
			tc.table.indexKeyArgs.primaryValues = tc.table.values
			// Setup descriptors and form an index key.
			desc, colMap := makeTableDescForTest(tc.table.indexKeyArgs)
			primaryKeyPrefix := MakeIndexKeyPrefix(keys.SystemSQLCodec, desc, desc.GetPrimaryIndexID())
			primaryKey, _, err := EncodeIndexKey(
				desc, desc.GetPrimaryIndex().IndexDesc(), colMap, tc.table.values, primaryKeyPrefix)
			if err != nil {
				t.Fatal(err)
			}

			tableIdx, restKey, match, err := IndexKeyEquivSignature(primaryKey, validEquivSigs, keySigBuf, keyRestBuf)
			if err != nil {
				t.Fatal(err)
			}
			if !match {
				t.Fatalf("expected to extract equivalence signature from index key, instead false returned")
			}

			tableSig := tc.expected[len(tc.expected)-1]
			expectedTableIdx := validEquivSigs[string(tableSig)]
			if expectedTableIdx != tableIdx {
				t.Fatalf("table index returned does not match table index from validEquivSigs.\nexpected %d\nactual %d", expectedTableIdx, tableIdx)
			}

			// Column values should be at the beginning of the
			// remaining bytes of the key.
			pkIndexDesc := desc.GetPrimaryIndex().IndexDesc()
			colVals, null, err := EncodeColumns(pkIndexDesc.ColumnIDs, pkIndexDesc.ColumnDirections, colMap, tc.table.values, nil /*key*/)
			if err != nil {
				t.Fatal(err)
			}
			if null {
				t.Fatalf("unexpected null values when encoding expected column values")
			}

			if !bytes.Equal(colVals, restKey[:len(colVals)]) {
				t.Fatalf("missing column values from rest of key.\nexpected %v\nactual %v", colVals, restKey[:len(colVals)])
			}

			// The remaining bytes of the key should be the same
			// length as the primary key minus the equivalence
			// signature bytes.
			if len(primaryKey)-len(tableSig) != len(restKey) {
				t.Fatalf("unexpected rest of key length, expected %d, actual %d", len(primaryKey)-len(tableSig), len(restKey))
			}
		})
	}
}

// TestTableEquivSignatures verifies that TableEquivSignatures returns a slice
// of slice references to a table's interleave ancestors' equivalence
// signatures.
func TestTableEquivSignatures(t *testing.T) {
	hierarchy := createHierarchy()
	equivSignatures(hierarchy, nil /*parent*/, nil /*signatures*/)

	for _, tc := range createEquivTCs(hierarchy) {
		t.Run(tc.name, func(t *testing.T) {
			// We need to initialize this for makeTableDescForTest.
			tc.table.indexKeyArgs.primaryValues = tc.table.values
			// Setup descriptors and form an index key.
			desc, _ := makeTableDescForTest(tc.table.indexKeyArgs)
			equivSigs, err := TableEquivSignatures(&desc.TableDescriptor, desc.GetPrimaryIndex().IndexDesc())
			if err != nil {
				t.Fatal(err)
			}

			if len(equivSigs) != len(tc.expected) {
				t.Fatalf("expected %d equivalence signatures from TableEquivSignatures, actual %d", len(tc.expected), len(equivSigs))
			}
			for i, sig := range equivSigs {
				if !bytes.Equal(sig, tc.expected[i]) {
					t.Fatalf("equivalence signatures at index %d do not match.\nexpected\t%v\nactual\t%v", i, tc.expected[i], sig)
				}
			}
		})
	}
}

// TestEquivSignature verifies that invoking IndexKeyEquivSignature for an encoded index key
// for a given table-index pair returns the equivalent equivalence signature as
// that of the table-index from invoking TableEquivSignatures.
// It also checks that the equivalence signature is not equivalent to any other
// tables' equivalence signatures.
func TestEquivSignature(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tables []interleaveTableArgs
	}{
		{
			name: "Simple",
			tables: []interleaveTableArgs{
				{
					indexKeyArgs: indexKeyTest{tableID: 50},
					values:       []tree.Datum{tree.NewDInt(10)},
				},
				{
					indexKeyArgs: indexKeyTest{tableID: 51},
					values:       []tree.Datum{tree.NewDInt(20)},
				},
			},
		},

		{
			name: "ParentAndChild",
			tables: []interleaveTableArgs{
				{
					indexKeyArgs: indexKeyTest{tableID: 50},
					values:       []tree.Datum{tree.NewDInt(10)},
				},
				{
					indexKeyArgs: indexKeyTest{tableID: 51, primaryInterleaves: []descpb.ID{50}},
					values:       []tree.Datum{tree.NewDInt(10), tree.NewDInt(20)},
				},
			},
		},

		{
			name: "Siblings",
			tables: []interleaveTableArgs{
				{
					indexKeyArgs: indexKeyTest{tableID: 50},
					values:       []tree.Datum{tree.NewDInt(10)},
				},
				{
					indexKeyArgs: indexKeyTest{tableID: 51, primaryInterleaves: []descpb.ID{50}},
					values:       []tree.Datum{tree.NewDInt(10), tree.NewDInt(20)},
				},
				{
					indexKeyArgs: indexKeyTest{tableID: 52, primaryInterleaves: []descpb.ID{50}},
					values:       []tree.Datum{tree.NewDInt(30), tree.NewDInt(40)},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keyEquivSigs := make([][]byte, len(tc.tables))
			tableEquivSigs := make([][]byte, len(tc.tables))

			for i, table := range tc.tables {
				// We need to initialize this for makeTableDescForTest.
				table.indexKeyArgs.primaryValues = table.values

				// Setup descriptors and form an index key.
				desc, colMap := makeTableDescForTest(table.indexKeyArgs)
				primaryKeyPrefix := MakeIndexKeyPrefix(keys.SystemSQLCodec, desc, desc.GetPrimaryIndexID())
				primaryKey, _, err := EncodeIndexKey(
					desc, desc.GetPrimaryIndex().IndexDesc(), colMap, table.values, primaryKeyPrefix)
				if err != nil {
					t.Fatal(err)
				}

				// Extract out the table's equivalence signature.
				tempEquivSigs, err := TableEquivSignatures(&desc.TableDescriptor, desc.GetPrimaryIndex().IndexDesc())
				if err != nil {
					t.Fatal(err)
				}
				// The last signature is this table's.
				tableEquivSigs[i] = tempEquivSigs[len(tempEquivSigs)-1]

				validEquivSigs := make(map[string]int)
				for i, sig := range tempEquivSigs {
					validEquivSigs[string(sig)] = i
				}
				// Extract out the corresponding table index
				// of the index key's signature.
				tableIdx, _, _, err := IndexKeyEquivSignature(primaryKey, validEquivSigs, nil /*keySigBuf*/, nil /*keyRestBuf*/)
				if err != nil {
					t.Fatal(err)
				}
				// Map the table index back to the signature.
				keyEquivSigs[i] = tempEquivSigs[tableIdx]
			}

			for i, keySig := range keyEquivSigs {
				for j, tableSig := range tableEquivSigs {
					if i == j {
						// The corresponding table should have the same
						// equivalence signature as the one derived from the key.
						if !bytes.Equal(keySig, tableSig) {
							t.Fatalf("IndexKeyEquivSignature differs from equivalence signature for its table.\nKeySignature: %v\nTableSignature: %v", keySig, tableSig)
						}
					} else {
						// A different table should not have
						// the same equivalence signature.
						if bytes.Equal(keySig, tableSig) {
							t.Fatalf("IndexKeyEquivSignature produces equivalent signature for a different table.\nKeySignature: %v\nTableSignature: %v", keySig, tableSig)
						}
					}
				}
			}

		})
	}
}

func TestDecodeTableValue(t *testing.T) {
	a := &DatumAlloc{}
	for _, tc := range []struct {
		in  tree.Datum
		typ *types.T
		err string
	}{
		// These test cases are not intended to be exhaustive, but rather exercise
		// the special casing and error handling of DecodeTableValue.
		{tree.DNull, types.Bool, ""},
		{tree.DBoolTrue, types.Bool, ""},
		{tree.NewDInt(tree.DInt(4)), types.Bool, "value type is not True or False: Int"},
		{tree.DNull, types.Int, ""},
		{tree.NewDInt(tree.DInt(4)), types.Int, ""},
		{tree.DBoolTrue, types.Int, "decoding failed"},
	} {
		t.Run("", func(t *testing.T) {
			var prefix, scratch []byte
			buf, err := EncodeTableValue(prefix, 0 /* colID */, tc.in, scratch)
			if err != nil {
				t.Fatal(err)
			}
			d, _, err := DecodeTableValue(a, tc.typ, buf)
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("expected error %q, but got %v", tc.err, err)
			} else if err != nil {
				return
			}
			if tc.in.Compare(tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings()), d) != 0 {
				t.Fatalf("decoded datum %[1]v (%[1]T) does not match encoded datum %[2]v (%[2]T)", d, tc.in)
			}
		})
	}
}

// See CreateTestInterleavedHierarchy for the longest chain used for the short
// format.
var shortFormTables = [3]string{"parent1", "child1", "grandchild1"}

// ShortToLongKeyFmt converts the short key format preferred in test cases
//    /1/#/3/4
// to its long form required by parseTestkey
//    parent1/1/1/#/child1/1/3/4
// The short key format can end in an interleave sentinel '#' (i.e. after
// TightenEndKey).
// The short key format can also be "/" or end in "#/" which will append
// the parent's table/index info. without a trailing index column value.
func ShortToLongKeyFmt(short string) string {
	tableOrder := shortFormTables
	curTableIdx := 0

	var long []byte
	tokens := strings.Split(short, "/")
	// Verify short format starts with '/'.
	if tokens[0] != "" {
		panic("missing '/' token at the beginning of short format")
	}
	// Skip the first element since short format has starting '/'.
	tokens = tokens[1:]

	// Always append parent1.
	long = append(long, []byte(fmt.Sprintf("/%s/1/", tableOrder[curTableIdx]))...)
	curTableIdx++

	for i, tok := range tokens {
		// Permits ".../#/" to append table name without a value
		if tok == "" {
			continue
		}

		if tok == "#" {
			long = append(long, []byte("#/")...)
			// It's possible for the short-format to end with a #.
			if i == len(tokens)-1 {
				break
			}

			// New interleaved table and primary keys follow.
			if curTableIdx >= len(tableOrder) {
				panic("too many '#' tokens specified in short format (max 2 for child1 and 3 for grandchild1)")
			}

			long = append(long, []byte(fmt.Sprintf("%s/1/", tableOrder[curTableIdx]))...)
			curTableIdx++

			continue
		}

		long = append(long, []byte(fmt.Sprintf("%s/", tok))...)
	}

	// Remove the last '/'.
	return string(long[:len(long)-1])
}

// ExtractIndexKey constructs the index (primary) key for a row from any index
// key/value entry, including secondary indexes.
//
// Don't use this function in the scan "hot path".
func ExtractIndexKey(
	a *DatumAlloc, codec keys.SQLCodec, tableDesc catalog.TableDescriptor, entry kv.KeyValue,
) (roachpb.Key, error) {
	indexID, key, err := DecodeIndexKeyPrefix(codec, tableDesc, entry.Key)
	if err != nil {
		return nil, err
	}
	if indexID == tableDesc.GetPrimaryIndexID() {
		return entry.Key, nil
	}

	indexI, err := tableDesc.FindIndexWithID(indexID)
	if err != nil {
		return nil, err
	}
	index := indexI.IndexDesc()

	// Extract the values for index.ColumnIDs.
	indexTypes, err := colinfo.GetColumnTypes(tableDesc, index.ColumnIDs, nil)
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
	extraTypes, err := colinfo.GetColumnTypes(tableDesc, index.ExtraColumnIDs, nil)
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
	var colMap catalog.TableColMap
	for i, columnID := range index.ColumnIDs {
		colMap.Set(columnID, i)
	}
	for i, columnID := range index.ExtraColumnIDs {
		colMap.Set(columnID, i+len(index.ColumnIDs))
	}
	indexKeyPrefix := MakeIndexKeyPrefix(codec, tableDesc, tableDesc.GetPrimaryIndexID())

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
		tableDesc, tableDesc.GetPrimaryIndex().IndexDesc(), colMap, decodedValues, indexKeyPrefix)
	return indexKey, err
}
