// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlbase

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type indexKeyTest struct {
	tableID              ID
	primaryInterleaves   []ID
	secondaryInterleaves []ID
	primaryValues        []tree.Datum // len must be at least primaryInterleaveComponents+1
	secondaryValues      []tree.Datum // len must be at least secondaryInterleaveComponents+1
}

func makeTableDescForTest(test indexKeyTest) (TableDescriptor, map[ColumnID]int) {
	primaryColumnIDs := make([]ColumnID, len(test.primaryValues))
	secondaryColumnIDs := make([]ColumnID, len(test.secondaryValues))
	columns := make([]ColumnDescriptor, len(test.primaryValues)+len(test.secondaryValues))
	colMap := make(map[ColumnID]int, len(test.secondaryValues))
	for i := range columns {
		columns[i] = ColumnDescriptor{ID: ColumnID(i + 1), Type: ColumnType{SemanticType: ColumnType_INT}}
		colMap[columns[i].ID] = i
		if i < len(test.primaryValues) {
			primaryColumnIDs[i] = columns[i].ID
		} else {
			secondaryColumnIDs[i-len(test.primaryValues)] = columns[i].ID

		}
	}

	makeInterleave := func(indexID IndexID, ancestorTableIDs []ID) InterleaveDescriptor {
		var interleave InterleaveDescriptor
		interleave.Ancestors = make([]InterleaveDescriptor_Ancestor, len(ancestorTableIDs))
		for j, ancestorTableID := range ancestorTableIDs {
			interleave.Ancestors[j] = InterleaveDescriptor_Ancestor{
				TableID:         ancestorTableID,
				IndexID:         1,
				SharedPrefixLen: 1,
			}
		}
		return interleave
	}

	tableDesc := TableDescriptor{
		ID:      test.tableID,
		Columns: columns,
		PrimaryIndex: IndexDescriptor{
			ID:               1,
			ColumnIDs:        primaryColumnIDs,
			ColumnDirections: make([]IndexDescriptor_Direction, len(primaryColumnIDs)),
			Interleave:       makeInterleave(1, test.primaryInterleaves),
		},
		Indexes: []IndexDescriptor{{
			ID:               2,
			ColumnIDs:        secondaryColumnIDs,
			ExtraColumnIDs:   primaryColumnIDs,
			Unique:           true,
			ColumnDirections: make([]IndexDescriptor_Direction, len(secondaryColumnIDs)),
			Interleave:       makeInterleave(2, test.secondaryInterleaves),
		}},
	}

	return tableDesc, colMap
}

func decodeIndex(
	tableDesc *TableDescriptor, index *IndexDescriptor, key []byte,
) ([]tree.Datum, error) {
	types, err := GetColumnTypes(tableDesc, index.ColumnIDs)
	if err != nil {
		return nil, err
	}
	values := make([]EncDatum, len(index.ColumnIDs))
	colDirs := make([]encoding.Direction, len(index.ColumnDirections))
	for i, dir := range index.ColumnDirections {
		colDirs[i], err = dir.ToEncodingDirection()
		if err != nil {
			return nil, err
		}
	}
	_, ok, err := DecodeIndexKey(tableDesc, index, types, values, colDirs, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Errorf("key did not match descriptor")
	}

	decodedValues := make([]tree.Datum, len(values))
	var da DatumAlloc
	for i, value := range values {
		err := value.EnsureDecoded(&types[i], &da)
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
		{50, []ID{100}, nil,
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11)},
			[]tree.Datum{tree.NewDInt(20)},
		},
		{50, []ID{100, 200}, nil,
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11), tree.NewDInt(12)},
			[]tree.Datum{tree.NewDInt(20)},
		},
		{50, nil, []ID{100},
			[]tree.Datum{tree.NewDInt(10)},
			[]tree.Datum{tree.NewDInt(20), tree.NewDInt(21)},
		},
		{50, []ID{100}, []ID{100},
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11)},
			[]tree.Datum{tree.NewDInt(20), tree.NewDInt(21)},
		},
		{50, []ID{100}, []ID{200},
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11)},
			[]tree.Datum{tree.NewDInt(20), tree.NewDInt(21)},
		},
		{50, []ID{100, 200}, []ID{100, 300},
			[]tree.Datum{tree.NewDInt(10), tree.NewDInt(11), tree.NewDInt(12)},
			[]tree.Datum{tree.NewDInt(20), tree.NewDInt(21), tree.NewDInt(22)},
		},
	}

	for i := 0; i < 1000; i++ {
		var t indexKeyTest

		t.primaryInterleaves = make([]ID, rng.Intn(10))
		for j := range t.primaryInterleaves {
			t.primaryInterleaves[j] = ID(1 + rng.Intn(10))
		}
		valuesLen := randutil.RandIntInRange(rng, len(t.primaryInterleaves)+1, len(t.primaryInterleaves)+10)
		t.primaryValues = make([]tree.Datum, valuesLen)
		for j := range t.primaryValues {
			t.primaryValues[j] = RandDatum(rng, ColumnType{SemanticType: ColumnType_INT}, true)
		}

		t.secondaryInterleaves = make([]ID, rng.Intn(10))
		for j := range t.secondaryInterleaves {
			t.secondaryInterleaves[j] = ID(1 + rng.Intn(10))
		}
		valuesLen = randutil.RandIntInRange(rng, len(t.secondaryInterleaves)+1, len(t.secondaryInterleaves)+10)
		t.secondaryValues = make([]tree.Datum, valuesLen)
		for j := range t.secondaryValues {
			t.secondaryValues[j] = RandDatum(rng, ColumnType{SemanticType: ColumnType_INT}, true)
		}

		tests = append(tests, t)
	}

	for i, test := range tests {
		evalCtx := tree.NewTestingEvalContext()
		defer evalCtx.Stop(context.Background())
		tableDesc, colMap := makeTableDescForTest(test)
		testValues := append(test.primaryValues, test.secondaryValues...)

		primaryKeyPrefix := MakeIndexKeyPrefix(&tableDesc, tableDesc.PrimaryIndex.ID)
		primaryKey, _, err := EncodeIndexKey(
			&tableDesc, &tableDesc.PrimaryIndex, colMap, testValues, primaryKeyPrefix)
		if err != nil {
			t.Fatal(err)
		}
		primaryValue := roachpb.MakeValueFromBytes(nil)
		primaryIndexKV := client.KeyValue{Key: primaryKey, Value: &primaryValue}

		secondaryIndexEntry, err := EncodeSecondaryIndex(
			&tableDesc, &tableDesc.Indexes[0], colMap, testValues)
		if err != nil {
			t.Fatal(err)
		}
		secondaryIndexKV := client.KeyValue{
			Key:   secondaryIndexEntry.Key,
			Value: &secondaryIndexEntry.Value,
		}

		checkEntry := func(index *IndexDescriptor, entry client.KeyValue) {
			values, err := decodeIndex(&tableDesc, index, entry.Key)
			if err != nil {
				t.Fatal(err)
			}

			for j, value := range values {
				testValue := testValues[colMap[index.ColumnIDs[j]]]
				if value.Compare(evalCtx, testValue) != 0 {
					t.Fatalf("%d: value %d got %q but expected %q", i, j, value, testValue)
				}
			}

			indexID, _, err := DecodeIndexKeyPrefix(&tableDesc, entry.Key)
			if err != nil {
				t.Fatal(err)
			}
			if indexID != index.ID {
				t.Errorf("%d", i)
			}

			extracted, err := ExtractIndexKey(&a, &tableDesc, entry)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(extracted, primaryKey) {
				t.Errorf("%d got %s <%x>, but expected %s <%x>", i, extracted, []byte(extracted), roachpb.Key(primaryKey), primaryKey)
			}
		}

		checkEntry(&tableDesc.PrimaryIndex, primaryIndexKV)
		checkEntry(&tableDesc.Indexes[0], secondaryIndexKV)
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
			enc, err := encodeArray(&test.datum, nil)
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
			d, _, err := decodeArray(&DatumAlloc{}, test.datum.ParamTyp, enc)
			if err != nil {
				t.Fatal(err)
			}
			if d.Compare(tree.NewTestingEvalContext(), &test.datum) != 0 {
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
		_, _ = encodeArray(&ary, nil)
	}
}

func TestMarshalColumnValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		kind  ColumnType_SemanticType
		datum tree.Datum
		exp   roachpb.Value
	}{
		{
			kind:  ColumnType_BOOL,
			datum: tree.MakeDBool(true),
			exp:   func() (v roachpb.Value) { v.SetBool(true); return }(),
		},
		{
			kind:  ColumnType_BOOL,
			datum: tree.MakeDBool(false),
			exp:   func() (v roachpb.Value) { v.SetBool(false); return }(),
		},
		{
			kind:  ColumnType_INT,
			datum: tree.NewDInt(314159),
			exp:   func() (v roachpb.Value) { v.SetInt(314159); return }(),
		},
		{
			kind:  ColumnType_FLOAT,
			datum: tree.NewDFloat(3.14159),
			exp:   func() (v roachpb.Value) { v.SetFloat(3.14159); return }(),
		},
		{
			kind: ColumnType_DECIMAL,
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
			kind:  ColumnType_DATE,
			datum: tree.NewDDate(314159),
			exp:   func() (v roachpb.Value) { v.SetInt(314159); return }(),
		},
		{
			kind:  ColumnType_TIME,
			datum: tree.MakeDTime(timeofday.FromInt(314159)),
			exp:   func() (v roachpb.Value) { v.SetInt(314159); return }(),
		},
		{
			kind:  ColumnType_TIMESTAMP,
			datum: tree.MakeDTimestamp(timeutil.Unix(314159, 1000), time.Microsecond),
			exp:   func() (v roachpb.Value) { v.SetTime(timeutil.Unix(314159, 1000)); return }(),
		},
		{
			kind:  ColumnType_TIMESTAMPTZ,
			datum: tree.MakeDTimestampTZ(timeutil.Unix(314159, 1000), time.Microsecond),
			exp:   func() (v roachpb.Value) { v.SetTime(timeutil.Unix(314159, 1000)); return }(),
		},
		{
			kind:  ColumnType_STRING,
			datum: tree.NewDString("testing123"),
			exp:   func() (v roachpb.Value) { v.SetString("testing123"); return }(),
		},
		{
			kind:  ColumnType_NAME,
			datum: tree.NewDName("testingname123"),
			exp:   func() (v roachpb.Value) { v.SetString("testingname123"); return }(),
		},
		{
			kind:  ColumnType_BYTES,
			datum: tree.NewDBytes(tree.DBytes([]byte{0x31, 0x41, 0x59})),
			exp:   func() (v roachpb.Value) { v.SetBytes([]byte{0x31, 0x41, 0x59}); return }(),
		},
		{
			kind: ColumnType_UUID,
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
			kind: ColumnType_INET,
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
		typ := ColumnType{SemanticType: testCase.kind}
		col := ColumnDescriptor{ID: ColumnID(testCase.kind + 1), Type: typ}

		if actual, err := MarshalColumnValue(col, testCase.datum); err != nil {
			t.Errorf("%d: unexpected error with column type %v: %v", i, typ, err)
		} else if !reflect.DeepEqual(actual, testCase.exp) {
			t.Errorf("%d: MarshalColumnValue() got %s, expected %v", i, actual, testCase.exp)
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
				indexKeyArgs: indexKeyTest{tableID: 100, primaryInterleaves: []ID{50}},
				values:       []tree.Datum{tree.NewDInt(10), tree.NewDInt(20)},
			},
			expected: [][]byte{hierarchy["t1"].equivSig, hierarchy["t1"].children["t2"].equivSig},
		},

		{
			name: "TwoAncestors",
			table: interleaveTableArgs{
				indexKeyArgs: indexKeyTest{tableID: 20, primaryInterleaves: []ID{50, 150}},
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
			primaryKeyPrefix := MakeIndexKeyPrefix(&desc, desc.PrimaryIndex.ID)
			primaryKey, _, err := EncodeIndexKey(
				&desc, &desc.PrimaryIndex, colMap, tc.table.values, primaryKeyPrefix)
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
			colVals, null, err := EncodeColumns(desc.PrimaryIndex.ColumnIDs, desc.PrimaryIndex.ColumnDirections, colMap, tc.table.values, nil /*key*/)
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
			equivSigs, err := TableEquivSignatures(&desc, &desc.PrimaryIndex)
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
					indexKeyArgs: indexKeyTest{tableID: 51, primaryInterleaves: []ID{50}},
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
					indexKeyArgs: indexKeyTest{tableID: 51, primaryInterleaves: []ID{50}},
					values:       []tree.Datum{tree.NewDInt(10), tree.NewDInt(20)},
				},
				{
					indexKeyArgs: indexKeyTest{tableID: 52, primaryInterleaves: []ID{50}},
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
				primaryKeyPrefix := MakeIndexKeyPrefix(&desc, desc.PrimaryIndex.ID)
				primaryKey, _, err := EncodeIndexKey(
					&desc, &desc.PrimaryIndex, colMap, table.values, primaryKeyPrefix)
				if err != nil {
					t.Fatal(err)
				}

				// Extract out the table's equivalence signature.
				tempEquivSigs, err := TableEquivSignatures(&desc, &desc.PrimaryIndex)
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

func TestTightenStartKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlutils.CreateTestInterleavedHierarchy(t, sqlDB)

	// The interleaved hierarchy is as follows:
	//    parent		(pid1)
	//	child		(pid1, cid1, cid2)
	//	  grandchild	(pid1, cid1, cid2, gcid1)
	parent := GetTableDescriptor(kvDB, sqlutils.TestDB, "parent1")
	child := GetTableDescriptor(kvDB, sqlutils.TestDB, "child1")
	grandchild := GetTableDescriptor(kvDB, sqlutils.TestDB, "grandchild1")

	// Create DESC indexes for testing.
	r := sqlutils.MakeSQLRunner(sqlDB)
	r.Exec(t, fmt.Sprintf(`CREATE INDEX pid1_desc ON %s.parent1 (pid1 DESC)`, sqlutils.TestDB))
	parentDescIdx := GetTableDescriptor(kvDB, sqlutils.TestDB, "parent1").Indexes[0]
	r.Exec(t, fmt.Sprintf(`CREATE INDEX child_desc ON %s.child1 (pid1, cid1, cid2 DESC) INTERLEAVE IN PARENT %s.parent1 (pid1)`, sqlutils.TestDB, sqlutils.TestDB))
	childDescIdx := GetTableDescriptor(kvDB, sqlutils.TestDB, "child1").Indexes[0]
	r.Exec(t, fmt.Sprintf(`CREATE INDEX grandchild_desc ON %s.grandchild1 (pid1, cid1, cid2, gcid1 DESC) INTERLEAVE IN PARENT %s.child1(pid1, cid1, cid2)`, sqlutils.TestDB, sqlutils.TestDB))
	grandchildDescIdx := GetTableDescriptor(kvDB, sqlutils.TestDB, "grandchild1").Indexes[0]

	testCases := []struct {
		index    *IndexDescriptor
		input    roachpb.Key
		expected roachpb.Key
	}{
		// encodedNotNull(Asc) can appear at the end of a start key for
		// constraint IS NOT NULL on an ASC index (NULLs sorted first,
		// span starts (start key) on the first non-NULL).
		// See encodeStartConstraintAscending.
		// Expect: no change.
		{
			index:    &parent.PrimaryIndex,
			input:    encoding.EncodeNotNullAscending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/"))),
			expected: encoding.EncodeNotNullAscending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/"))),
		},

		// Same as previous test but with a child table.
		{
			index:    &child.PrimaryIndex,
			input:    encoding.EncodeNotNullAscending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2"))),
			expected: encoding.EncodeNotNullAscending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2"))),
		},

		// Same as previous test but with the IS NOT NULL on cid1.
		{
			index:    &child.PrimaryIndex,
			input:    encoding.EncodeNotNullAscending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/"))),
			expected: encoding.EncodeNotNullAscending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/"))),
		},

		// Same as previous test but with a grandchild table.
		{
			index:    &grandchild.PrimaryIndex,
			input:    encoding.EncodeNotNullAscending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2/3/#/"))),
			expected: encoding.EncodeNotNullAscending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2/3/#/"))),
		},

		// Same as previous test (with grandchild table but with
		// a child start key with IS NULL on cid2).
		{
			index:    &grandchild.PrimaryIndex,
			input:    encoding.EncodeNotNullAscending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2"))),
			expected: encoding.EncodeNotNullAscending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2"))),
		},

		// encodedNullDesc can appear at the end of a start key for
		// constraint IS NULL on a DESC index (NULLs sorted last,
		// span starts (start key) on the first NULLs).
		// See encodeStartConstraintDescending.
		// Expect: no change.
		{
			index:    &parentDescIdx,
			input:    encoding.EncodeNullDescending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/"))),
			expected: encoding.EncodeNullDescending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/"))),
		},

		// Same as previous test but with child table.
		{
			index:    &childDescIdx,
			input:    encoding.EncodeNullDescending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2"))),
			expected: encoding.EncodeNullDescending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2"))),
		},

		// Same as previous test but with IS NULL on cid1.
		{
			index:    &childDescIdx,
			input:    encoding.EncodeNullDescending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/"))),
			expected: encoding.EncodeNullDescending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/"))),
		},

		// Same as previous test but with grandchild table.
		{
			index:    &grandchildDescIdx,
			input:    encoding.EncodeNullDescending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2/3/#/"))),
			expected: encoding.EncodeNullDescending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2/3/#/"))),
		},

		// Same as previous test (with grandchild table but with
		// a child start key with IS NOT NULL on cid2).
		{
			index:    &grandchildDescIdx,
			input:    encoding.EncodeNullDescending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2"))),
			expected: encoding.EncodeNullDescending(EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2"))),
		},

		// Parent start key wrt parent index does not change.
		{
			index:    &parent.PrimaryIndex,
			input:    EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1")),
			expected: EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1")),
		},

		// Child start key wrt parent index should be tightened to the
		// next parent key.
		{
			index:    &parent.PrimaryIndex,
			input:    EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2/3")),
			expected: EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/2")),
		},

		// Parent start key wrt child index does not change.
		{
			index:    &child.PrimaryIndex,
			input:    EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1")),
			expected: EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1")),
		},

		// Child start key wrt child index does not change.
		{
			index:    &child.PrimaryIndex,
			input:    EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2/3")),
			expected: EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2/3")),
		},

		// Grandchild start key wrt child index is tightened to the
		// next child row.
		{
			index:    &child.PrimaryIndex,
			input:    EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2/3/#/4")),
			expected: EncodeTestKey(t, kvDB, ShortToLongKeyFmt("/1/#/2/4")),
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			actual, err := TightenStartKey(tc.index, tc.input)
			if err != nil {
				t.Fatal(err)
			}

			if !tc.expected.Equal(actual) {
				t.Errorf("expected tightened start key %s, got %s", tc.expected, actual)
			}
		})
	}
}

func TestTightenEndKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlutils.CreateTestInterleavedHierarchy(t, sqlDB)

	// Create DESC indexes for testing.
	r := sqlutils.MakeSQLRunner(sqlDB)
	r.Exec(t, fmt.Sprintf(`CREATE INDEX pid1_desc ON %s.parent1 (pid1 DESC)`, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE INDEX child_desc ON %s.child1 (pid1, cid1, cid2 DESC) INTERLEAVE IN PARENT %s.parent1 (pid1)`, sqlutils.TestDB, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE INDEX grandchild_desc ON %s.grandchild1 (pid1, cid1, cid2, gcid1 DESC) INTERLEAVE IN PARENT %s.child1(pid1, cid1, cid2)`, sqlutils.TestDB, sqlutils.TestDB))
	sqlutils.CreateTable(
		t,
		sqlDB,
		"nullabledesc",
		"a INT PRIMARY KEY, b INT, INDEX ab (a, b DESC)",
		1,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowIdxFn),
	)

	// The interleaved hierarchy is as follows:
	//    parent		(pid1)
	//	child		(pid1, cid1, cid2)
	//	  grandchild	(pid1, cid1, cid2, gcid1)
	parent := GetTableDescriptor(kvDB, sqlutils.TestDB, "parent1")
	child := GetTableDescriptor(kvDB, sqlutils.TestDB, "child1")
	grandchild := GetTableDescriptor(kvDB, sqlutils.TestDB, "grandchild1")
	nullableDescTable := GetTableDescriptor(kvDB, sqlutils.TestDB, "nullabledesc")

	parentDescIdx := parent.Indexes[0]
	childDescIdx := child.Indexes[0]
	grandchildDescIdx := grandchild.Indexes[0]
	nullableDescIdx := nullableDescTable.Indexes[0]

	testCases := []struct {
		table *TableDescriptor
		index *IndexDescriptor
		// See ShortToLongKeyFmt for how to represent a key.
		input string
		// If the end key is assumed to be inclusive when passed to
		// to TightenEndKey.
		inclusive bool
		expected  string
	}{
		// NOTNULLASC can appear at the end of an end key for
		// constraint IS NULL on an ASC index (NULLs sorted first,
		// span ends (end key) right before the first non-NULL).
		// See encodeEndConstraintAscending.

		// Parent end key wrt parent index and NOTNULLASC suffix.
		{
			table:    parent,
			index:    &parent.PrimaryIndex,
			input:    "/NOTNULLASC",
			expected: "/NULLASC/#",
		},

		// Same as previous test but with a child table.
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/NOTNULLASC",
			expected: "/1/#/2/NULLASC/#",
		},

		// Partial child end key with the IS NULL on cid1 (no change).
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/NOTNULLASC",
			expected: "/1/#/NULLASC/#",
		},

		// Same as previous-previous test but with a grandchild table.
		{
			table:    grandchild,
			index:    &grandchild.PrimaryIndex,
			input:    "/1/#/2/3/#/NOTNULLASC",
			expected: "/1/#/2/3/#/NULLASC/#",
		},

		// Child key with trailing NOTNULLASC wrt grandchild index does
		// not chnage.
		{
			table:    grandchild,
			index:    &grandchild.PrimaryIndex,
			input:    "/1/#/2/NOTNULLASC",
			expected: "/1/#/2/NOTNULLASC",
		},

		// NOTNULLDESC can appear at the end of a start key for
		// constraint IS NOT NULL on a DESC index (NULLs sorted last,
		// span ends (end key) right after the last non-NULL).
		// See encodeEndConstraintDescending.

		// Parent end key wrt parent index and NOTNULLDESC suffix.
		{
			table:    parent,
			index:    &parentDescIdx,
			input:    "/NOTNULLDESC",
			expected: "/NOTNULLDESC",
		},

		// Same as previous test but with child table.
		{
			table:    child,
			index:    &childDescIdx,
			input:    "/1/#/2/NOTNULLDESC",
			expected: "/1/#/2/NOTNULLDESC",
		},

		// Same as previous test but with IS NOT NULL on cid1.
		{
			table: child,
			index: &childDescIdx,
			input: "/1/#/NOTNULLDESC",
			// Contrary to the ASC case, we CANNOT convert this to
			//    /1/#/<IntMax>/#
			// Since NULLDESC sort after '#' and this would exclude
			//    /1/#/<IntMax>/NULLDESC
			// where cid2 = NULL.
			expected: "/1/#/NOTNULLDESC",
		},

		// Same as previous-previous test but with grandchild table.
		{
			table:    grandchild,
			index:    &grandchildDescIdx,
			input:    "/1/#/2/3/#/NOTNULLDESC",
			expected: "/1/#/2/3/#/NOTNULLDESC",
		},

		// Same as previous test (with grandchild table but with
		// a child start key with IS NOT NULL on cid2).
		// Expect no change.
		{
			table:    grandchild,
			index:    &grandchildDescIdx,
			input:    "/1/#/2/NOTNULLDESC",
			expected: "/1/#/2/NOTNULLDESC",
		},

		// Parent end key wrt parent index should be tightened to
		// the last parent key for the last parent row (pid1 = 0).
		{
			table:    parent,
			index:    &parent.PrimaryIndex,
			input:    "/1",
			expected: "/0/#",
		},

		{
			table: parent,
			index: &parent.PrimaryIndex,
			// This could be from a previous TightenEndKey.
			input: "/1/#",
			// TightenEndKey is idempotent.
			expected: "/1/#",
		},

		// Child end key wrt parent index should be tightened to read
		// up to the last parent key for the last parent row.
		{
			table:    parent,
			index:    &parent.PrimaryIndex,
			input:    "/1/#/2/3",
			expected: "/1/#",
		},

		// Parent end key wrt child index does not change.
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1",
			expected: "/1",
		},

		{
			index: &child.PrimaryIndex,
			// This could be from a previous TightenEndKey.
			input: "/1/#",
			// TightenEndKey is idempotent.
			expected: "/1/#",
		},

		// Child end key wrt child index should be tightened to read
		// up to the last child key for the last child row (cid2 = 2).
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/3",
			expected: "/1/#/2/2/#",
		},

		{
			table: child,
			index: &child.PrimaryIndex,
			// This could be from a previous TightenEndKey.
			input: "/1/#/2/2/#",
			// TightenEndKey is idempotent.
			expected: "/1/#/2/2/#",
		},

		// Grandchild end key wrt child index should be tightened to
		// read up to the last child key for the last child row (cid =
		// 3).
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/3/#/4",
			expected: "/1/#/2/3/#",
		},

		// Partial child key can be tightened.
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/2",
			expected: "/1/#/1/#",
		},

		// No DESC columns after the last column, tighten.
		{
			table:    child,
			index:    &childDescIdx,
			input:    "/1/#/2/3",
			expected: "/1/#/2/2/#",
		},

		// Even though cid2 is DESC, it is NON-NULLABLE since it is
		// part of the primary key.
		// Can indeed tighten.
		{
			table:    child,
			index:    &childDescIdx,
			input:    "/1/#/2",
			expected: "/1/#/1/#",
		},

		// This can be tightened since there is no third index column.
		{
			table:    nullableDescTable,
			index:    &nullableDescIdx,
			input:    "/1/2",
			expected: "/1/1/#",
		},

		// Since the key is inclusive, we need only append the
		// interleaved prefix.
		{
			table:     nullableDescTable,
			index:     &nullableDescIdx,
			input:     "/1/2",
			inclusive: true,
			expected:  "/1/2/#",
		},

		{
			table:    nullableDescTable,
			index:    &nullableDescIdx,
			input:    "/1/NULLDESC",
			expected: "/1/NOTNULLDESC/#",
		},

		// This cannot be tightened the second index column is
		// NULLABLE and has DESC direction.
		{
			table:    nullableDescTable,
			index:    &nullableDescIdx,
			input:    "/1",
			expected: "/1",
		},

		// If this end key is inclusive, we need to make it exclusive
		// by applying a PrefixEnd.
		{
			table:     nullableDescTable,
			index:     &nullableDescIdx,
			input:     "/1",
			inclusive: true,
			expected:  "/2",
		},

		// Inclusive parent end key wrt parent index should only see
		// interleave sentinel appended.
		{
			table:     parent,
			index:     &parent.PrimaryIndex,
			input:     "/1",
			inclusive: true,
			expected:  "/1/#",
		},

		// Inclusive parent end key wrt parent index should only see
		// interleave sentinel appended.
		{
			table:     parent,
			index:     &parent.PrimaryIndex,
			input:     "/1",
			inclusive: true,
			expected:  "/1/#",
		},

		{
			table:     parent,
			index:     &parent.PrimaryIndex,
			input:     "/NULLASC",
			inclusive: true,
			expected:  "/NULLASC/#",
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			actual, err := TightenEndKey(tc.table, tc.index, EncodeTestKey(t, kvDB, ShortToLongKeyFmt(tc.input)), tc.inclusive)
			if err != nil {
				t.Fatal(err)
			}

			expected := EncodeTestKey(t, kvDB, ShortToLongKeyFmt(tc.expected))
			if !expected.Equal(actual) {
				t.Errorf("expected tightened end key %s, got %s", expected, actual)
			}
		})
	}
}
