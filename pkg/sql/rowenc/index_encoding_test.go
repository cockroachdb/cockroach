// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowenc_test

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	. "github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/trigram"
	"github.com/stretchr/testify/require"
)

type indexKeyTest struct {
	tableID         descpb.ID
	primaryValues   []tree.Datum
	secondaryValues []tree.Datum
}

func makeTableDescForTest(
	test indexKeyTest, isSecondaryIndexForward bool,
) (catalog.TableDescriptor, catalog.TableColMap) {
	primaryColumnIDs := make([]descpb.ColumnID, len(test.primaryValues))
	secondaryColumnIDs := make([]descpb.ColumnID, len(test.secondaryValues))
	columns := make([]descpb.ColumnDescriptor, len(test.primaryValues)+len(test.secondaryValues))
	var colMap catalog.TableColMap
	secondaryType := idxtype.FORWARD
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
				secondaryType = idxtype.INVERTED
			}
			if isSecondaryIndexForward && columns[i].Type.Family() == types.JsonFamily {
				secondaryType = idxtype.FORWARD
			}
			secondaryColumnIDs[i-len(test.primaryValues)] = columns[i].ID
		}
	}

	tableDesc := descpb.TableDescriptor{
		ID:      test.tableID,
		Columns: columns,
		PrimaryIndex: descpb.IndexDescriptor{
			ID:                  1,
			KeyColumnIDs:        primaryColumnIDs,
			KeyColumnDirections: make([]catenumpb.IndexColumn_Direction, len(primaryColumnIDs)),
		},
		Indexes: []descpb.IndexDescriptor{{
			ID:                  2,
			KeyColumnIDs:        secondaryColumnIDs,
			KeySuffixColumnIDs:  primaryColumnIDs,
			Unique:              true,
			KeyColumnDirections: make([]catenumpb.IndexColumn_Direction, len(secondaryColumnIDs)),
			Type:                secondaryType,
		}},
	}
	return tabledesc.NewBuilder(&tableDesc).BuildImmutableTable(), colMap
}

func decodeIndex(
	codec keys.SQLCodec, tableDesc catalog.TableDescriptor, index catalog.Index, key []byte,
) ([]tree.Datum, error) {
	types, err := getColumnTypes(tableDesc.IndexKeyColumns(index))
	if err != nil {
		return nil, err
	}
	values := make([]EncDatum, index.NumKeyColumns())
	colDirs := index.IndexDesc().KeyColumnDirections
	if _, err := DecodeIndexKey(codec, values, colDirs, key); err != nil {
		return nil, err
	}

	decodedValues := make([]tree.Datum, len(values))
	var da tree.DatumAlloc
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
	parseJSON := func(s string) *tree.DJSON {
		j, err := json.ParseJSON(s)
		if err != nil {
			t.Fatalf("Failed to parse %s: %v", s, err)
		}
		return tree.NewDJSON(j)
	}

	rng, _ := randutil.NewTestRand()
	var a tree.DatumAlloc

	tests := []indexKeyTest{
		{
			50,
			[]tree.Datum{tree.NewDInt(10)},
			[]tree.Datum{tree.NewDInt(20)},
		},
		{
			50,
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11)},
			[]tree.Datum{tree.NewDInt(20)},
		},
		{
			50,
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11), tree.NewDInt(12)},
			[]tree.Datum{tree.NewDInt(20)},
		},
		{
			50,
			[]tree.Datum{tree.NewDInt(10)},
			[]tree.Datum{tree.NewDInt(20), tree.NewDInt(21)},
		},
		{
			50,
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11)},
			[]tree.Datum{tree.NewDInt(20), tree.NewDInt(21)},
		},
		{
			50,
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11)},
			[]tree.Datum{tree.NewDInt(20), tree.NewDInt(21)},
		},
		{
			50,
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11), tree.NewDInt(12)},
			[]tree.Datum{tree.NewDInt(20), tree.NewDInt(21), tree.NewDInt(22)},
		},
		// Testing JSON in primary indexes.
		{
			tableID:         50,
			primaryValues:   []tree.Datum{parseJSON(`"a"`)},
			secondaryValues: []tree.Datum{tree.NewDInt(20)},
		},
		{
			tableID:         50,
			primaryValues:   []tree.Datum{parseJSON(`1`)},
			secondaryValues: []tree.Datum{tree.NewDInt(20)},
		},
		{
			tableID:         50,
			primaryValues:   []tree.Datum{parseJSON(`"a"`), parseJSON(`[1, 2, 3]`)},
			secondaryValues: []tree.Datum{tree.NewDInt(20)},
		},
		{
			tableID:         50,
			primaryValues:   []tree.Datum{parseJSON(`{"a": "b"}`)},
			secondaryValues: []tree.Datum{tree.NewDInt(20)},
		},
		{
			tableID:         50,
			primaryValues:   []tree.Datum{parseJSON(`{"a": "b", "c": "d"}`)},
			secondaryValues: []tree.Datum{tree.NewDInt(20)},
		},
		{
			tableID:         50,
			primaryValues:   []tree.Datum{parseJSON(`[1, "a", {"a": "b"}]`)},
			secondaryValues: []tree.Datum{tree.NewDInt(20)},
		},
		{
			tableID: 50,
			primaryValues: []tree.Datum{parseJSON(`null`), parseJSON(`[]`), parseJSON(`{}`),
				parseJSON(`""`)},
			secondaryValues: []tree.Datum{tree.NewDInt(20)},
		},
		// Testing JSON in secondary indexes.
		{
			tableID:         50,
			primaryValues:   []tree.Datum{tree.NewDInt(20)},
			secondaryValues: []tree.Datum{parseJSON(`{"a": "b"}`)},
		},
		{
			tableID:         50,
			primaryValues:   []tree.Datum{tree.NewDInt(20), tree.NewDInt(50)},
			secondaryValues: []tree.Datum{parseJSON(`{"a": "b"}`), parseJSON(`[1, "a", {"a": "b"}]`)},
		},
		{
			tableID:         50,
			primaryValues:   []tree.Datum{tree.NewDInt(20), tree.NewDInt(50)},
			secondaryValues: []tree.Datum{parseJSON(`1`)},
		},
		{
			tableID:         50,
			primaryValues:   []tree.Datum{tree.NewDInt(20), tree.NewDInt(50)},
			secondaryValues: []tree.Datum{parseJSON(`"b"`)},
		},
		{
			tableID:       50,
			primaryValues: []tree.Datum{tree.NewDInt(20), tree.NewDInt(50)},
			secondaryValues: []tree.Datum{parseJSON(`null`), parseJSON(`[]`), parseJSON(`{}`),
				parseJSON(`""`)},
		},
		// Testing JSON in both primary and secondary indexes.
		{
			tableID:         50,
			primaryValues:   []tree.Datum{parseJSON(`"a"`)},
			secondaryValues: []tree.Datum{parseJSON(`"b"`)},
		},
		{
			tableID:       50,
			primaryValues: []tree.Datum{parseJSON(`{"a": "b"}`), parseJSON(`[1, "a", {"a": "b"}]`)},
			secondaryValues: []tree.Datum{parseJSON(`null`), parseJSON(`[]`), parseJSON(`{}`),
				parseJSON(`""`)},
		},
	}

	for i := 0; i < 1000; i++ {
		var t indexKeyTest

		valuesLen := randutil.RandIntInRange(rng, 1, 10)
		t.primaryValues = make([]tree.Datum, valuesLen)
		for j := range t.primaryValues {
			t.primaryValues[j] = randgen.RandDatum(rng, types.Int, false /* nullOk */)
		}

		valuesLen = randutil.RandIntInRange(rng, 1, 10)
		t.secondaryValues = make([]tree.Datum, valuesLen)
		for j := range t.secondaryValues {
			t.secondaryValues[j] = randgen.RandDatum(rng, types.Int, true /* nullOk */)
		}

		tests = append(tests, t)
	}

	for i, test := range tests {
		ctx := context.Background()
		evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		defer evalCtx.Stop(ctx)
		tableDesc, colMap := makeTableDescForTest(test, true /* isSecondaryIndexForward */)
		// Add the default family to each test, since secondary indexes support column families.
		var (
			colNames []string
			colIDs   descpb.ColumnIDs
		)
		for _, c := range tableDesc.PublicColumns() {
			colNames = append(colNames, c.GetName())
			colIDs = append(colIDs, c.GetID())
		}
		tableDesc.TableDesc().Families = []descpb.ColumnFamilyDescriptor{{
			Name:            "defaultFamily",
			ID:              0,
			ColumnNames:     colNames,
			ColumnIDs:       colIDs,
			DefaultColumnID: colIDs[0],
		}}

		testValues := append(test.primaryValues, test.secondaryValues...)

		codec := keys.SystemSQLCodec
		primaryKeyPrefix := MakeIndexKeyPrefix(codec, tableDesc.GetID(), tableDesc.GetPrimaryIndexID())
		primaryKey, _, err := EncodeIndexKey(tableDesc, tableDesc.GetPrimaryIndex(), colMap, testValues, primaryKeyPrefix)
		if err != nil {
			t.Fatal(err)
		}
		primaryValue := roachpb.MakeValueFromBytes(nil)
		primaryIndexKV := kv.KeyValue{Key: primaryKey, Value: &primaryValue}

		secondaryIndexEntry, err := EncodeSecondaryIndex(
			ctx, codec, tableDesc, tableDesc.PublicNonPrimaryIndexes()[0],
			colMap, testValues, true, /* includeEmpty */
		)
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

		checkEntry := func(index catalog.Index, entry kv.KeyValue) {
			values, err := decodeIndex(codec, tableDesc, index, entry.Key)
			if err != nil {
				t.Fatal(err)
			}

			for j, value := range values {
				testValue := testValues[colMap.GetDefault(index.GetKeyColumnID(j))]
				if cmp, err := value.Compare(ctx, evalCtx, testValue); err != nil {
					t.Fatal(err)
				} else if cmp != 0 {
					t.Fatalf("%d: value %d got %q but expected %q", i, j, value, testValue)
				}
			}

			indexID, _, err := DecodeIndexKeyPrefix(codec, tableDesc.GetID(), entry.Key)
			if err != nil {
				t.Fatal(err)
			}
			if indexID != index.GetID() {
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

		checkEntry(tableDesc.GetPrimaryIndex(), primaryIndexKV)
		checkEntry(tableDesc.PublicNonPrimaryIndexes()[0], secondaryIndexKV)
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
		tableDesc, colMap := makeTableDescForTest(indexKeyTest{50, primaryValues, secondaryValues},
			false /* isSecondaryIndexForward */)
		for _, idx := range tableDesc.PublicNonPrimaryIndexes() {
			idx.IndexDesc().Version = version
		}

		testValues := append(primaryValues, secondaryValues...)

		codec := keys.SystemSQLCodec

		secondaryIndexEntries, err := EncodeSecondaryIndex(
			context.Background(), codec, tableDesc, tableDesc.PublicNonPrimaryIndexes()[0],
			colMap, testValues, true, /* includeEmpty */
		)
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
		indexedValue string
		value        string
		expected     bool
		unique       bool
	}{
		// This test uses EncodeInvertedIndexTableKeys and EncodeContainingInvertedIndexSpans
		// to determine whether the first Array value contains the second. If
		// indexedValue @> value, expected is true. Otherwise it is false.
		//
		// If EncodeContainingInvertedIndexSpans produces spans that are guaranteed not to
		// contain duplicate primary keys, unique is true. Otherwise it is false.
		{`{}`, `{}`, true, false},
		{`{}`, `{1}`, false, true},
		{`{1}`, `{}`, true, false},
		{`{1}`, `{1}`, true, true},
		{`{1}`, `{1, 2}`, false, true},
		{`{1, 2}`, `{1}`, true, true},
		{`{1, 2}`, `{2}`, true, true},
		{`{1, 2}`, `{1, 2}`, true, true},
		{`{1, 2}`, `{1, 2, 1}`, true, true},
		{`{1, 2}`, `{1, 1}`, true, true},
		{`{1, 2, 3}`, `{1, 2, 4}`, false, true},
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

	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	parseArray := func(s string) tree.Datum {
		arr, _, err := tree.ParseDArrayFromString(&evalCtx, s, types.Int)
		if err != nil {
			t.Fatalf("Failed to parse array %s: %v", s, err)
		}
		return arr
	}

	runTest := func(left, right tree.Datum, expected, expectUnique bool) {
		keys, err := EncodeInvertedIndexTableKeys(left, nil, descpb.LatestIndexDescriptorVersion)
		require.NoError(t, err)

		invertedExpr, err := EncodeContainingInvertedIndexSpans(context.Background(), &evalCtx, right)
		require.NoError(t, err)

		spanExpr, ok := invertedExpr.(*inverted.SpanExpression)
		if !ok {
			t.Fatalf("invertedExpr %v is not a SpanExpression", invertedExpr)
		}

		// Array spans are always tight.
		if spanExpr.Tight != true {
			t.Errorf("For %s, expected tight=%v, but got %v", right, true, spanExpr.Tight)
		}

		if spanExpr.Unique != expectUnique {
			t.Errorf("For %s, expected unique=%v, but got %v", right, expectUnique, spanExpr.Unique)
		}

		actual, err := spanExpr.ContainsKeys(keys)
		require.NoError(t, err)

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
		indexedValue, value := parseArray(c.indexedValue), parseArray(c.value)

		// First check that evaluating `indexedValue @> value` matches the expected
		// result.
		res, err := tree.ArrayContains(context.Background(), &evalCtx, indexedValue.(*tree.DArray), value.(*tree.DArray))
		require.NoError(t, err)
		if bool(*res) != c.expected {
			t.Fatalf(
				"expected value of %s @> %s did not match actual value. Expected: %v. Got: %s",
				c.indexedValue, c.value, c.expected, res.String(),
			)
		}

		// Now check that we get the same result with the inverted index spans.
		runTest(indexedValue, value, c.expected, c.unique)
	}

	// Run a set of randomly generated test cases.
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 100; i++ {
		typ := randgen.RandArrayType(rng)

		// We don't allow jsonpath indices.
		if typ.ArrayContents().Family() == types.JsonpathFamily {
			continue
		}

		// Generate two random arrays and evaluate the result of `left @> right`.
		left := randgen.RandArray(rng, typ, 0 /* nullChance */)
		right := randgen.RandArray(rng, typ, 0 /* nullChance */)

		res, err := tree.ArrayContains(context.Background(), &evalCtx, left.(*tree.DArray), right.(*tree.DArray))
		require.NoError(t, err)

		// The spans should not have duplicate values if there is at least one
		// element.
		arr := right.(*tree.DArray).Array
		expectUnique := len(arr) > 0

		// Now check that we get the same result with the inverted index spans.
		runTest(left, right, bool(*res), expectUnique)
	}
}

func TestEncodeContainedArrayInvertedIndexSpans(t *testing.T) {
	testCases := []struct {
		indexedValue string
		value        string
		containsKeys bool
		expected     bool
		unique       bool
	}{

		// This test uses EncodeInvertedIndexTableKeys and EncodeContainedInvertedIndexSpans
		// to determine if the spans produced from the second Array value will
		// correctly include or exclude the first value, indicated by
		// containsKeys. Then, if indexedValue <@ value, expected is true.

		// Not all indexedValues included in the spans are contained by the value,
		// so the expression is never tight. Also, the expression is a union of
		// spans, so unique should never be true unless the value produces a single
		// empty array span.

		// First we test that the spans will include expected values, even if
		// they are not necessarily contained by the value.
		{`{}`, `{}`, true, true, true},
		{`{1}`, `{1}`, true, true, false},
		{`{}`, `{1}`, true, true, false},
		{`{1, 2}`, `{1}`, true, false, false},
		{`{}`, `{1, 2}`, true, true, false},
		{`{2}`, `{1, 2}`, true, true, false},
		{`{2, NULL}`, `{1, 2}`, true, false, false},
		{`{1, 2}`, `{1, 2}`, true, true, false},
		{`{1, 3}`, `{1, 2}`, true, false, false},
		{`{2}`, `{2, 2}`, true, true, false},
		{`{1, 2}`, `{1, 2, 1}`, true, true, false},
		{`{1, 1, 2, 3}`, `{1, 2, 1}`, true, false, false},
		{`{1, 2, 4}`, `{1, 2, 3}`, true, false, false},
		{`{}`, `{NULL}`, true, true, true},
		{`{}`, `{NULL, NULL}`, true, true, true},
		{`{2}`, `{2, NULL}`, true, true, false},
		{`{2, 3}`, `{2, NULL}`, true, false, false},
		{`{1, NULL}`, `{1, 2, NULL}`, true, false, false},

		// Then we test that the spans exclude results that should be excluded.
		{`{1}`, `{}`, false, false, true},
		{`{NULL}`, `{}`, false, false, true},
		{`{2}`, `{1}`, false, false, false},
		{`{4, 3}`, `{2, 1}`, false, false, false},
		{`{5}`, `{1, 2, 1}`, false, false, false},
		{`{NULL, 3}`, `{1, 2, 1}`, false, false, false},
		{`{NULL}`, `{NULL}`, false, false, true},
		{`{NULL}`, `{1, NULL}`, false, false, false},
		{`{2, NULL}`, `{1, NULL}`, false, false, false},
	}

	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	parseArray := func(s string) tree.Datum {
		arr, _, err := tree.ParseDArrayFromString(&evalCtx, s, types.Int)
		if err != nil {
			t.Fatalf("Failed to parse array %s: %v", s, err)
		}
		return arr
	}

	runTest := func(indexedValue, value tree.Datum, expectContainsKeys, expected, expectUnique bool) {
		keys, err := EncodeInvertedIndexTableKeys(indexedValue, nil, descpb.LatestIndexDescriptorVersion)
		require.NoError(t, err)

		invertedExpr, err := EncodeContainedInvertedIndexSpans(context.Background(), &evalCtx, value)
		require.NoError(t, err)

		spanExpr, ok := invertedExpr.(*inverted.SpanExpression)
		if !ok {
			t.Fatalf("invertedExpr %v is not a SpanExpression", invertedExpr)
		}

		// Array spans for <@ are never tight.
		if spanExpr.Tight == true {
			t.Errorf("For %s, expected tight=false, but got true", value)
		}

		// Array spans for <@ are never unique unless the value produces a single
		// empty array span.
		if spanExpr.Unique != expectUnique {
			if expectUnique {
				t.Errorf("For %s, expected unique=true, but got false", value)
			} else {
				t.Errorf("For %s, expected unique=false, but got true", value)
			}
		}

		// Check if the indexedValue is included by the spans.
		containsKeys, err := spanExpr.ContainsKeys(keys)
		require.NoError(t, err)

		if containsKeys != expectContainsKeys {
			if expectContainsKeys {
				t.Errorf("expected spans of %s to include %s but they did not", value, indexedValue)
			} else {
				t.Errorf("expected spans of %s not to include %s but they did", value, indexedValue)
			}
		}

		// Since the spans are never tight, apply an additional filter to determine
		// if the result is contained.
		actual, err := tree.ArrayContains(context.Background(), &evalCtx, value.(*tree.DArray), indexedValue.(*tree.DArray))
		require.NoError(t, err)
		if bool(*actual) != expected {
			if expected {
				t.Errorf("expected %s to be contained by %s but it was not", indexedValue, value)
			} else {
				t.Errorf("expected %s not to be contained by %s but it was", indexedValue, value)
			}
		}
	}

	// Run pre-defined test cases from above.
	for _, c := range testCases {
		indexedValue, value := parseArray(c.indexedValue), parseArray(c.value)
		runTest(indexedValue, value, c.containsKeys, c.expected, c.unique)
	}

	// Run a set of randomly generated test cases.
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 100; i++ {
		typ := randgen.RandArrayType(rng)

		// Generate two random arrays and evaluate the result of `left <@ right`.
		left := randgen.RandArray(rng, typ, 0 /* nullChance */)
		right := randgen.RandArray(rng, typ, 0 /* nullChance */)

		// We cannot check for false positives with these tests (due to the fact that
		// the spans are not tight), so we will only test for false negatives.
		isContained, err := tree.ArrayContains(context.Background(), &evalCtx, right.(*tree.DArray), left.(*tree.DArray))
		require.NoError(t, err)
		if !*isContained {
			continue
		}

		// Check for uniqueness. We do not have to worry about cases containing
		// NULL since nullChance is set to 0.
		unique := false
		if len(right.(*tree.DArray).Array) == 0 {
			unique = true
		}

		// Now check that we get the same result with the inverted index spans.
		runTest(left, right, true, true, unique)
	}
}

// ExtractIndexKey constructs the index (primary) key for a row from any index
// key/value entry, including secondary indexes.
//
// Don't use this function in the scan "hot path".
func ExtractIndexKey(
	a *tree.DatumAlloc, codec keys.SQLCodec, tableDesc catalog.TableDescriptor, entry kv.KeyValue,
) (roachpb.Key, error) {
	indexID, key, err := DecodeIndexKeyPrefix(codec, tableDesc.GetID(), entry.Key)
	if err != nil {
		return nil, err
	}
	if indexID == tableDesc.GetPrimaryIndexID() {
		return entry.Key, nil
	}

	index, err := catalog.MustFindIndexByID(tableDesc, indexID)
	if err != nil {
		return nil, err
	}

	// Extract the values for index.KeyColumnIDs.
	indexTypes, err := getColumnTypes(tableDesc.IndexKeyColumns(index))
	if err != nil {
		return nil, err
	}
	values := make([]EncDatum, index.NumKeyColumns())
	dirs := index.IndexDesc().KeyColumnDirections
	key, _, err = DecodeKeyVals(values, dirs, key)
	if err != nil {
		return nil, err
	}

	// Extract the values for index.KeySuffixColumnIDs
	extraTypes, err := getColumnTypes(tableDesc.IndexKeySuffixColumns(index))
	if err != nil {
		return nil, err
	}
	extraValues := make([]EncDatum, index.NumKeySuffixColumns())
	dirs = make([]catenumpb.IndexColumn_Direction, index.NumKeySuffixColumns())
	for i := 0; i < index.NumKeySuffixColumns(); i++ {
		// Implicit columns are always encoded Ascending.
		dirs[i] = catenumpb.IndexColumn_ASC
	}
	extraKey := key
	if index.IsUnique() {
		extraKey, err = entry.Value.GetBytes()
		if err != nil {
			return nil, err
		}
	}
	_, _, err = DecodeKeyVals(extraValues, dirs, extraKey)
	if err != nil {
		return nil, err
	}

	// Encode the index key from its components.
	var colMap catalog.TableColMap
	for i := 0; i < index.NumKeyColumns(); i++ {
		columnID := index.GetKeyColumnID(i)
		colMap.Set(columnID, i)
	}
	for i := 0; i < index.NumKeySuffixColumns(); i++ {
		columnID := index.GetKeySuffixColumnID(i)
		colMap.Set(columnID, i+index.NumKeyColumns())
	}
	indexKeyPrefix := MakeIndexKeyPrefix(codec, tableDesc.GetID(), tableDesc.GetPrimaryIndexID())

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
		tableDesc, tableDesc.GetPrimaryIndex(), colMap, decodedValues, indexKeyPrefix)
	return indexKey, err
}

func getColumnTypes(columns []catalog.Column) ([]*types.T, error) {
	outTypes := make([]*types.T, len(columns))
	for i, col := range columns {
		if !col.Public() {
			return nil, fmt.Errorf("column-id \"%d\" does not exist", col.GetID())
		}
		outTypes[i] = col.GetType()
	}
	return outTypes, nil
}

func TestEncodeOverlapsArrayInvertedIndexSpans(t *testing.T) {
	testCases := []struct {
		indexedValue string
		value        string
		ok           bool
		expected     bool
		unique       bool
	}{

		// This test uses EncodeInvertedIndexTableKeys and EncodeOverlapsInvertedIndexSpans
		// to determine if the spans produced from the second Array value will
		// correctly overlap or be distinct from the first value.

		// The expression is a union of spans, so unique will be true IFF the value array
		// only contains one or more entries of the same non-null element (e.g. A && [1]).

		// First we test that the spans will include expected value.
		{`{1}`, `{1}`, true, true, true},
		{`{1, 2}`, `{1}`, true, true, true},
		{`{2}`, `{1, 2}`, true, true, false},
		{`{2,3}`, `{2,2,2}`, true, true, true},
		{`{2, NULL}`, `{1, 2}`, true, true, false},
		{`{1, 2}`, `{1, 2}`, true, true, false},
		{`{1, 3}`, `{1, 2}`, true, true, false},
		{`{2}`, `{2, 2}`, true, true, true},
		{`{1, 2}`, `{1, 2, 1}`, true, true, false},
		{`{1, 1, 2, 3}`, `{1, 2, 1}`, true, true, false},
		{`{1, 2, 4}`, `{1, 2, 3}`, true, true, false},
		{`{2}`, `{2, NULL}`, true, true, true},
		{`{2, 3}`, `{2, NULL}`, true, true, true},
		{`{1, NULL}`, `{1, 2, NULL}`, true, true, false},

		// Then we test that the spans exclude results that should be excluded.
		{`{}`, `{}`, false, false, false},
		{`NULL`, `NULL`, false, false, false},
		{`NULL`, `{1, 2}`, true, false, false},
		{`{1, 2}`, `NULL`, false, false, false},
		{`{}`, `{1}`, true, false, true},
		{`{1}`, `{}`, false, false, false},
		{`{}`, `{1, 2}`, true, false, false},
		{`{NULL}`, `{}`, false, false, false},
		{`{}`, `{NULL}`, false, false, false},
		{`{}`, `{NULL, NULL}`, false, false, false},
		{`{2}`, `{1}`, true, false, true},
		{`{4, 3}`, `{2, 1}`, true, false, false},
		{`{5}`, `{1, 2, 1}`, true, false, false},
		{`{NULL, 3}`, `{1, 2, 1}`, true, false, false},
		{`{NULL}`, `{NULL}`, false, false, false},
		{`{NULL}`, `{1, NULL}`, true, false, true},
		{`{1,NULL}`, `{NULL}`, false, false, false},
		{`{2, NULL}`, `{1, NULL}`, true, false, true},
	}

	ctx := context.Background()
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	parseArray := func(s string) tree.Datum {
		if s == "NULL" {
			return tree.DNull
		}
		arr, _, err := tree.ParseDArrayFromString(&evalCtx, s, types.Int)
		if err != nil {
			t.Fatalf("Failed to parse array %s: %v", s, err)
		}
		return arr
	}

	runTest := func(indexedValue, value tree.Datum, expected, ok, unique bool) {
		keys, err := EncodeInvertedIndexTableKeys(indexedValue, nil, descpb.PrimaryIndexWithStoredColumnsVersion)
		require.NoError(t, err)

		invertedExpr, err := EncodeOverlapsInvertedIndexSpans(context.Background(), &evalCtx, value)
		require.NoError(t, err)

		spanExpr, conversionOk := invertedExpr.(*inverted.SpanExpression)
		if ok && !conversionOk {
			t.Fatalf("For (%s, %s), Expr %v is not an InvertedExpression contrary to expectation", indexedValue, value, invertedExpr)
		} else if !ok && conversionOk {
			t.Fatalf("For (%s, %s), Expr %v is an InvertedExpression contrary to expectation", indexedValue, value, invertedExpr)
		} else if !ok && !conversionOk {
			return
		}

		// Array spans for && are always tight.
		if spanExpr.Tight != true {
			t.Errorf("For (%s, %s), expected tight=true, but got false", indexedValue, value)
		}

		// Array spans for && are unique only when the value
		// array contains one or more entries of the same non-null element.
		// e.g. A && [1, 1].
		if spanExpr.Unique != unique {
			t.Errorf("For (%s, %s), expected unique=%t, but got %t", indexedValue, value, unique, spanExpr.Unique)
		}

		// Check if the indexedValue is included by the spans (i.e. Overlaps).
		overlaps, err := spanExpr.ContainsKeys(keys)
		require.NoError(t, err)

		if overlaps != expected {
			if expected {
				t.Errorf("Expected spans of %s to overlap with %s but they did not", value, indexedValue)
			} else {
				t.Errorf("Expected spans of %s to not overlap with %s but they did", value, indexedValue)
			}
		}
	}

	// Run pre-defined test cases from above.
	for _, c := range testCases {
		indexedValue, value := parseArray(c.indexedValue), parseArray(c.value)
		runTest(indexedValue, value, c.expected, c.ok, c.unique)
	}

	// Run a set of randomly generated test cases.
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 100; i++ {
		typ := randgen.RandArrayType(rng)

		// We don't allow jsonpath indices.
		if typ.ArrayContents().Family() == types.JsonpathFamily {
			continue
		}

		// Generate two random arrays and evaluate the result of `left && right`.
		// Using 1/9th as the Null Chance to generate arrays with a small
		// number of NULLs added in between.
		left := randgen.RandArray(rng, typ, 9 /* nullChance */)
		right := randgen.RandArray(rng, typ, 9 /* nullChance */)

		overlaps, err := tree.ArrayOverlaps(ctx, &evalCtx, right.(*tree.DArray), left.(*tree.DArray))
		require.NoError(t, err)

		rightArr, _ := right.(*tree.DArray)
		// An inverted expression can only be generated if the value array is
		// non-empty or contains atleast one non-NULL element.
		ok := rightArr.Len() > 0 && rightArr.HasNonNulls
		// A unique span expression can be guaranteed when the input is of
		// the form:
		// Array A && Array containing one or more entries of same non-null
		// element e.g. A && [1, 1].
		unique, err := containsNonNullUniqueElement(ctx, &evalCtx, rightArr)
		require.NoError(t, err)

		// Now check that we get the same result with the inverted index spans.
		runTest(left, right, bool(*overlaps), ok, unique)
	}
}

// Determines if the input array contains only one or more entries of the
// same non-null element. NULL entries are not considered.
func containsNonNullUniqueElement(
	ctx context.Context, evalCtx *eval.Context, valArr *tree.DArray,
) (bool, error) {
	var lastVal tree.Datum = tree.DNull
	for _, val := range valArr.Array {
		if val != tree.DNull {
			if lastVal == tree.DNull {
				lastVal = val
				continue
			}
			if cmp, err := lastVal.Compare(ctx, evalCtx, val); err != nil {
				return false, err
			} else if cmp != 0 {
				return false, nil
			}
		}
	}
	return lastVal != tree.DNull, nil
}

type trigramSearchType int

const (
	like trigramSearchType = iota
	similar
	eq
)

func TestEncodeTrigramInvertedIndexSpans(t *testing.T) {
	testCases := []struct {
		// The value that's being indexed in the trigram index.
		indexedValue string
		// The value that's being turned into spans to search with.
		value string
		// Whether we're using LIKE or % operator for the search.
		searchType trigramSearchType
		// Whether we expect that the spans should contain the keys produced by
		// indexing the indexedValue. If the searchType is similar, then the
		// spans should contain at least one of the indexed keys, otherwise the
		// spans should contain all the indexed keys.
		containsKeys bool
		// Whether we expect that the indexed value should evaluate as matching
		// the LIKE or % expression that we're testing.
		expected bool
		unique   bool
	}{

		// This test uses EncodeInvertedIndexTableKeys and EncodeTrigramSpans
		// to determine if the spans produced from the second string value will
		// correctly include or exclude the first value.

		{`foobarbaz`, `%oob%baz`, like, true, true, false},
		{`foobarbaz`, `%oob%`, like, true, true, true},
		// Test that the order of the trigrams doesn't matter for containment, but
		// does matter for evaluation.
		{`staticcheck`, `%check%static%`, like, true, false, false},
		// Make sure that we can satisfy a query that includes a chunk that is too
		// short to produce any trigrams at all.
		{`test`, `%a%bar`, like, false, false, true},

		// "Reverse order" trigrams shouldn't match.
		{`test`, `tse`, like, false, false, true},

		// Similarity (%) queries.
		{`staticcheck`, `staricheck`, similar, true, true, false},
		{`staticcheck`, `blevicchlrk`, similar, true, false, false},
		{`staticcheck`, `che`, similar, true, false, false},
		{`staticcheck`, `xxx`, similar, false, false, false},
		{`staticcheck`, `xxxyyy`, similar, false, false, false},
		{`aaaaaa`, `aab`, similar, true, true, false},

		// Equality queries.
		{`staticcheck`, `staticcheck`, eq, true, true, false},
		{`staticcheck`, `staticcheckz`, eq, false, false, false},
		{`staticcheck`, `zstaticcheck`, eq, false, false, false},
		{`baba`, `abab`, eq, true, false, false},
		{`foo`, `foo`, eq, true, true, true},
		{`foo`, `bar`, eq, false, false, true},

		{`eabc`, `eabd`, eq, false, false, false},
	}

	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	evalCtx.SessionData().TrigramSimilarityThreshold = .3

	runTest := func(indexedValue, value string, searchType trigramSearchType,
		expectContainsKeys, expected, expectUnique bool) {
		t.Logf("test case: %s %s %v %t %t %t", indexedValue, value, searchType, expectContainsKeys, expected, expectUnique)
		keys, err := EncodeInvertedIndexTableKeys(tree.NewDString(indexedValue), nil, descpb.LatestIndexDescriptorVersion)
		require.NoError(t, err)

		typedExpr := makeTrigramBinOp(t, indexedValue, value, searchType)
		invertedExpr, err := EncodeTrigramSpans(value, searchType != similar)
		require.NoError(t, err)

		spanExpr, ok := invertedExpr.(*inverted.SpanExpression)
		if !ok {
			t.Fatalf("invertedExpr %v is not a SpanExpression", invertedExpr)
		}

		if spanExpr.Tight {
			// We never expect the inverted expressions for trigrams to be tight.
			t.Fatalf("unexpectedly found a tight expression")
		}
		require.Equal(t, expectUnique, spanExpr.Unique, "%s, %s: unexpected unique attribute", indexedValue, value)

		// Check if the indexedValue is included by the spans. If the search is
		// a similarity search, the spans should contain at least one key.
		// Otherwise, the spans should contain all the keys.
		var containsKeys bool
		if searchType == similar {
			for i := range keys {
				containsKey, err := spanExpr.ContainsKeys([][]byte{keys[i]})
				require.NoError(t, err)
				if containsKey {
					containsKeys = true
					break
				}
			}
		} else {
			containsKeys, err = spanExpr.ContainsKeys(keys)
			require.NoError(t, err)
		}
		require.Equal(t, expectContainsKeys, containsKeys, "%s, %s: expected containsKeys", indexedValue, value)

		// Since the spans are never tight, apply an additional filter to determine
		// if the result is contained.
		datum, err := eval.Expr(context.Background(), &evalCtx, typedExpr)
		require.NoError(t, err)
		actual := bool(*datum.(*tree.DBool))
		require.Equal(t, expected, actual, "%s, %s: expected evaluation result to match", indexedValue, value)
	}

	// Run pre-defined test cases from above.
	for _, c := range testCases {
		runTest(c.indexedValue, c.value, c.searchType, c.containsKeys, c.expected, c.unique)
	}

	// Run some random test cases.

	rng, _ := randutil.NewTestRand()
	for i := 0; i < 100; i++ {
		const alphabet = "abcdefg"

		// Generate two random strings and evaluate left % right, left LIKE right,
		// and left = right both via eval and via the span comparisons.
		left := util.RandString(rng, 15, alphabet)
		length := 3 + rng.Intn(5)
		right := util.RandString(rng, length, alphabet+"%")

		for _, searchType := range []trigramSearchType{like, eq, similar} {
			expr := makeTrigramBinOp(t, left, right, searchType)
			lTrigrams := trigram.MakeTrigrams(left, searchType == similar /* pad */)
			// Check for intersection. We're looking for a non-zero intersection
			// for similar, and complete containment of the right trigrams in the left
			// for eq and like.
			any := false
			all := true
			rTrigrams := trigram.MakeTrigrams(right, searchType == similar /* pad */)
			for _, trigram := range rTrigrams {
				idx := sort.Search(len(lTrigrams), func(i int) bool {
					return lTrigrams[i] >= trigram
				})
				if idx < len(lTrigrams) && lTrigrams[idx] == trigram {
					any = true
				} else {
					all = false
				}
			}
			var expectedContainsKeys bool
			if searchType == similar {
				expectedContainsKeys = any
			} else {
				expectedContainsKeys = all
			}

			d, err := eval.Expr(context.Background(), &evalCtx, expr)
			require.NoError(t, err)
			expected := bool(*d.(*tree.DBool))
			trigrams := trigram.MakeTrigrams(right, searchType == similar /* pad */)
			nTrigrams := len(trigrams)
			valid := nTrigrams > 0
			unique := nTrigrams == 1
			if !valid {
				_, err := EncodeTrigramSpans(right, searchType != similar /* allMustMatch */)
				require.Error(t, err)
				continue
			}
			runTest(left, right, searchType, expectedContainsKeys, expected, unique)
		}
	}
}

func makeTrigramBinOp(
	t *testing.T, indexedValue string, value string, searchType trigramSearchType,
) (typedExpr tree.TypedExpr) {
	var opstr string
	switch searchType {
	case like:
		opstr = "LIKE"
	case eq:
		opstr = "="
	case similar:
		opstr = "%"
	default:
		panic("no such searchtype")
	}
	expr, err := parser.ParseExpr(fmt.Sprintf("'%s' %s '%s'", indexedValue, opstr, value))
	require.NoError(t, err)

	semaContext := tree.MakeSemaContext(nil /* resolver */)
	typedExpr, err = tree.TypeCheck(context.Background(), expr, &semaContext, types.Bool)
	require.NoError(t, err)
	return typedExpr
}

func TestEncodeTrigramInvertedIndexSpansError(t *testing.T) {
	// Make sure that any input with a chunk with fewer than 3 characters returns
	// an error, since we can't produce trigrams from strings that don't meet a
	// minimum of 3 characters.
	testCases := []struct {
		input           string
		allMustMatchErr bool
		anyMustMatchErr bool
	}{
		{"fo", true, false},
		{"a", true, false},
		{"", true, true},
		// Non-alpha characters don't count against the limit.
		{"fo ", true, false},
		{"%fo%", true, false},
		{"#$(*)", true, true},
	}
	for _, tc := range testCases {
		_, err := EncodeTrigramSpans(tc.input, true /* allMustMatch */)
		if tc.allMustMatchErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		_, err = EncodeTrigramSpans(tc.input, false /* allMustMatch */)
		if tc.anyMustMatchErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestDecodeKeyVals(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		desc                 string
		key                  []byte
		vals                 []EncDatum
		expectedRemainingKey []byte
		expectedNumVals      int
	}{
		{
			desc:                 "vals_eq_bytes",
			key:                  []byte{1},
			vals:                 make([]EncDatum, 1),
			expectedRemainingKey: []byte{},
			expectedNumVals:      1,
		},
		{
			desc:                 "vals_lt_bytes",
			key:                  []byte{1, 1},
			vals:                 make([]EncDatum, 1),
			expectedRemainingKey: []byte{1},
			expectedNumVals:      1,
		},
		{
			desc:                 "vals_gt_bytes",
			key:                  []byte{1},
			vals:                 make([]EncDatum, 2),
			expectedRemainingKey: []byte{},
			expectedNumVals:      1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			actualRemainingKey, actualNumVals, err := DecodeKeyVals(tc.vals, nil, tc.key)
			require.NoError(t, err)
			require.Equal(t, tc.expectedRemainingKey, actualRemainingKey)
			require.Equal(t, tc.expectedNumVals, actualNumVals)
		})
	}
}
