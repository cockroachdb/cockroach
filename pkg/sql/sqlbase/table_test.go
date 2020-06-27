// Copyright 2015 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
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
		columns[i] = ColumnDescriptor{
			ID:   ColumnID(i + 1),
			Type: types.Int,
		}
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
	codec keys.SQLCodec, tableDesc *TableDescriptor, index *IndexDescriptor, key []byte,
) ([]tree.Datum, error) {
	types, err := GetColumnTypes(tableDesc, index.ColumnIDs)
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
			t.primaryValues[j] = RandDatum(rng, types.Int, true)
		}

		t.secondaryInterleaves = make([]ID, rng.Intn(10))
		for j := range t.secondaryInterleaves {
			t.secondaryInterleaves[j] = ID(1 + rng.Intn(10))
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
			colIDs   ColumnIDs
		)
		for _, c := range tableDesc.Columns {
			colNames = append(colNames, c.Name)
			colIDs = append(colIDs, c.ID)
		}
		tableDesc.Families = []ColumnFamilyDescriptor{{
			Name:            "defaultFamily",
			ID:              0,
			ColumnNames:     colNames,
			ColumnIDs:       colIDs,
			DefaultColumnID: colIDs[0],
		}}

		testValues := append(test.primaryValues, test.secondaryValues...)

		codec := keys.SystemSQLCodec
		primaryKeyPrefix := MakeIndexKeyPrefix(codec, &tableDesc, tableDesc.PrimaryIndex.ID)
		primaryKey, _, err := EncodeIndexKey(
			&tableDesc, &tableDesc.PrimaryIndex, colMap, testValues, primaryKeyPrefix)
		if err != nil {
			t.Fatal(err)
		}
		primaryValue := roachpb.MakeValueFromBytes(nil)
		primaryIndexKV := kv.KeyValue{Key: primaryKey, Value: &primaryValue}

		secondaryIndexEntry, err := EncodeSecondaryIndex(
			codec, &tableDesc, &tableDesc.Indexes[0], colMap, testValues, true /* includeEmpty */)
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

		checkEntry := func(index *IndexDescriptor, entry kv.KeyValue) {
			values, err := decodeIndex(codec, &tableDesc, index, entry.Key)
			if err != nil {
				t.Fatal(err)
			}

			for j, value := range values {
				testValue := testValues[colMap[index.ColumnIDs[j]]]
				if value.Compare(evalCtx, testValue) != 0 {
					t.Fatalf("%d: value %d got %q but expected %q", i, j, value, testValue)
				}
			}

			indexID, _, err := DecodeIndexKeyPrefix(codec, &tableDesc, entry.Key)
			if err != nil {
				t.Fatal(err)
			}
			if indexID != index.ID {
				t.Errorf("%d", i)
			}

			extracted, err := ExtractIndexKey(&a, codec, &tableDesc, entry)
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
		_, _ = encodeArray(&ary, nil)
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
		col := ColumnDescriptor{ID: ColumnID(typ.Family() + 1), Type: typ}

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
			primaryKeyPrefix := MakeIndexKeyPrefix(keys.SystemSQLCodec, &desc, desc.PrimaryIndex.ID)
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
				primaryKeyPrefix := MakeIndexKeyPrefix(keys.SystemSQLCodec, &desc, desc.PrimaryIndex.ID)
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

func TestAdjustStartKeyForInterleave(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlutils.CreateTestInterleavedHierarchy(t, sqlDB)

	// Secondary indexes with DESC direction in the last column.
	r := sqlutils.MakeSQLRunner(sqlDB)
	r.Exec(t, fmt.Sprintf(`CREATE INDEX pid1_desc ON %s.parent1 (pid1 DESC)`, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE INDEX child_desc ON %s.child1 (pid1, cid1, cid2 DESC) INTERLEAVE IN PARENT %s.parent1 (pid1)`, sqlutils.TestDB, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE INDEX grandchild_desc ON %s.grandchild1 (pid1, cid1, cid2, gcid1 DESC) INTERLEAVE IN PARENT %s.child1(pid1, cid1, cid2)`, sqlutils.TestDB, sqlutils.TestDB))
	// Index with implicit primary columns (pid1, cid2).
	r.Exec(t, fmt.Sprintf(`CREATE INDEX child_non_unique ON %s.child1 (v, cid1)`, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE UNIQUE INDEX child_unique ON %s.child1 (v, cid1)`, sqlutils.TestDB))

	// The interleaved hierarchy is as follows:
	//    parent		(pid1)
	//	child		(pid1, cid1, cid2)
	//	  grandchild	(pid1, cid1, cid2, gcid1)
	parent := TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "parent1")
	child := TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "child1")
	grandchild := TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "grandchild1")

	parentDescIdx := parent.Indexes[0]
	childDescIdx := child.Indexes[0]
	childNonUniqueIdx := child.Indexes[1]
	childUniqueIdx := child.Indexes[2]
	grandchildDescIdx := grandchild.Indexes[0]

	testCases := []struct {
		index *IndexDescriptor
		// See ShortToLongKeyFmt for how to represent a key.
		input    string
		expected string
	}{
		// NOTNULLASC can appear at the end of a start key for
		// constraint IS NOT NULL on an ASC index (NULLs sorted first,
		// span starts (start key) on the first non-NULL).
		// See encodeStartConstraintAscending.

		{
			index:    &parent.PrimaryIndex,
			input:    "/NOTNULLASC",
			expected: "/NOTNULLASC",
		},
		{
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/NOTNULLASC",
			expected: "/1/#/2/NOTNULLASC",
		},
		{
			index:    &grandchild.PrimaryIndex,
			input:    "/1/#/2/3/#/NOTNULLASC",
			expected: "/1/#/2/3/#/NOTNULLASC",
		},

		{
			index:    &child.PrimaryIndex,
			input:    "/1/#/NOTNULLASC",
			expected: "/1/#/NOTNULLASC",
		},

		{
			index:    &grandchild.PrimaryIndex,
			input:    "/1/#/2/NOTNULLASC",
			expected: "/1/#/2/NOTNULLASC",
		},

		// NULLDESC can appear at the end of a start key for constraint
		// IS NULL on a DESC index (NULLs sorted last, span starts
		// (start key) on the first NULLs).
		// See encodeStartConstraintDescending.

		{
			index:    &parentDescIdx,
			input:    "/NULLDESC",
			expected: "/NULLDESC",
		},
		{
			index:    &childDescIdx,
			input:    "/1/#/2/NULLDESC",
			expected: "/1/#/2/NULLDESC",
		},
		{
			index:    &grandchildDescIdx,
			input:    "/1/#/2/3/#/NULLDESC",
			expected: "/1/#/2/3/#/NULLDESC",
		},

		{
			index:    &childDescIdx,
			input:    "/1/#/NULLDESC",
			expected: "/1/#/NULLDESC",
		},

		// Keys that belong to the given index (neither parent nor
		// children keys) do not need to be tightened.
		{
			index:    &parent.PrimaryIndex,
			input:    "/1",
			expected: "/1",
		},
		{
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/3",
			expected: "/1/#/2/3",
		},

		// Parent keys wrt child index is not tightened.
		{
			index:    &child.PrimaryIndex,
			input:    "/1",
			expected: "/1",
		},

		// Children keys wrt to parent index is tightened (pushed
		// forwards) to the next parent key.
		{
			index:    &parent.PrimaryIndex,
			input:    "/1/#/2/3",
			expected: "/2",
		},
		{
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/3/#/4",
			expected: "/1/#/2/4",
		},

		// Key with len > 1 tokens.
		{
			index:    &child.PrimaryIndex,
			input:    "/12345678901234/#/1234/1234567890/#/123/1234567",
			expected: "/12345678901234/#/1234/1234567891",
		},
		{
			index:    &child.PrimaryIndex,
			input:    "/12345678901234/#/d1403.2594/shelloworld/#/123/1234567",
			expected: "/12345678901234/#/d1403.2594/shelloworld/PrefixEnd",
		},

		// Index key with extra columns (implicit primary key columns).
		// We should expect two extra columns (in addition to the
		// two index columns).
		{
			index:    &childNonUniqueIdx,
			input:    "/2/3",
			expected: "/2/3",
		},
		{
			index:    &childNonUniqueIdx,
			input:    "/2/3/4",
			expected: "/2/3/4",
		},
		{
			index:    &childNonUniqueIdx,
			input:    "/2/3/4/5",
			expected: "/2/3/4/5",
		},
		{
			index:    &childNonUniqueIdx,
			input:    "/2/3/4/5/#/10",
			expected: "/2/3/4/6",
		},

		// Unique indexes only include implicit columns if they have
		// a NULL value.
		{
			index:    &childUniqueIdx,
			input:    "/2/3",
			expected: "/2/3",
		},
		{
			index:    &childUniqueIdx,
			input:    "/2/3/4",
			expected: "/2/4",
		},
		{
			index:    &childUniqueIdx,
			input:    "/2/NULLASC/4",
			expected: "/2/NULLASC/4",
		},
		{
			index:    &childUniqueIdx,
			input:    "/2/NULLASC/4/5",
			expected: "/2/NULLASC/4/5",
		},
		{
			index:    &childUniqueIdx,
			input:    "/2/NULLASC/4/5/#/6",
			expected: "/2/NULLASC/4/6",
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			codec := keys.SystemSQLCodec
			actual := EncodeTestKey(t, kvDB, codec, ShortToLongKeyFmt(tc.input))
			actual, err := AdjustStartKeyForInterleave(codec, tc.index, actual)
			if err != nil {
				t.Fatal(err)
			}

			expected := EncodeTestKey(t, kvDB, codec, ShortToLongKeyFmt(tc.expected))
			if !expected.Equal(actual) {
				t.Errorf("expected tightened start key %s, got %s", expected, actual)
			}
		})
	}
}

func TestAdjustEndKeyForInterleave(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlutils.CreateTestInterleavedHierarchy(t, sqlDB)

	// Secondary indexes with DESC direction in the last column.
	r := sqlutils.MakeSQLRunner(sqlDB)
	r.Exec(t, fmt.Sprintf(`CREATE INDEX pid1_desc ON %s.parent1 (pid1 DESC)`, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE INDEX child_desc ON %s.child1 (pid1, cid1, cid2 DESC) INTERLEAVE IN PARENT %s.parent1 (pid1)`, sqlutils.TestDB, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE INDEX grandchild_desc ON %s.grandchild1 (pid1, cid1, cid2, gcid1 DESC) INTERLEAVE IN PARENT %s.child1(pid1, cid1, cid2)`, sqlutils.TestDB, sqlutils.TestDB))
	// Index with implicit primary columns (pid1, cid2).
	r.Exec(t, fmt.Sprintf(`CREATE INDEX child_non_unique ON %s.child1 (v, cid1)`, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE UNIQUE INDEX child_unique ON %s.child1 (v, cid1)`, sqlutils.TestDB))

	// The interleaved hierarchy is as follows:
	//    parent		(pid1)
	//	child		(pid1, cid1, cid2)
	//	  grandchild	(pid1, cid1, cid2, gcid1)
	parent := TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "parent1")
	child := TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "child1")
	grandchild := TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "grandchild1")

	parentDescIdx := parent.Indexes[0]
	childDescIdx := child.Indexes[0]
	childNonUniqueIdx := child.Indexes[1]
	childUniqueIdx := child.Indexes[2]
	grandchildDescIdx := grandchild.Indexes[0]

	testCases := []struct {
		table *TableDescriptor
		index *IndexDescriptor
		// See ShortToLongKeyFmt for how to represent a key.
		input string
		// If the end key is assumed to be inclusive when passed to
		// to AdjustEndKeyForInterleave.
		inclusive bool
		expected  string
	}{
		// NOTNULLASC can appear at the end of an end key for
		// constraint IS NULL on an ASC index (NULLs sorted first,
		// span ends (end key) right before the first non-NULL).
		// See encodeEndConstraintAscending.

		{
			table:    parent,
			index:    &parent.PrimaryIndex,
			input:    "/NOTNULLASC",
			expected: "/NULLASC/#",
		},

		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/NOTNULLASC",
			expected: "/1/#/2/NULLASC/#",
		},

		{
			table:    grandchild,
			index:    &grandchild.PrimaryIndex,
			input:    "/1/#/2/3/#/NOTNULLASC",
			expected: "/1/#/2/3/#/NULLASC/#",
		},

		// No change since interleaved rows cannot occur between
		// partial primary key columns.
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/NOTNULLASC",
			expected: "/1/#/NOTNULLASC",
		},

		// No change since key belongs to an ancestor.
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

		// No change since descending indexes are always secondary and
		// secondary indexes are never tightened since they cannot
		// have interleaved rows.

		{
			table:    parent,
			index:    &parentDescIdx,
			input:    "/NOTNULLDESC",
			expected: "/NOTNULLDESC",
		},
		{
			table:    child,
			index:    &childDescIdx,
			input:    "/1/#/2/NOTNULLDESC",
			expected: "/1/#/2/NOTNULLDESC",
		},
		{
			table:    grandchild,
			index:    &grandchildDescIdx,
			input:    "/1/#/2/3/#/NOTNULLDESC",
			expected: "/1/#/2/3/#/NOTNULLDESC",
		},
		{
			table:    grandchild,
			index:    &grandchildDescIdx,
			input:    "/1/#/2/NOTNULLDESC",
			expected: "/1/#/2/NOTNULLDESC",
		},

		// NULLASC with inclusive=true is possible with IS NULL for
		// ascending indexes.
		// See encodeEndConstraintAscending.

		{
			table:     parent,
			index:     &parent.PrimaryIndex,
			input:     "/NULLASC",
			inclusive: true,
			expected:  "/NULLASC/#",
		},

		{
			table:     child,
			index:     &child.PrimaryIndex,
			input:     "/1/#/2/NULLASC",
			inclusive: true,
			expected:  "/1/#/2/NULLASC/#",
		},

		// Keys with all the column values of the primary key should be
		// tightened wrt to primary indexes since they can have
		// interleaved rows.

		{
			table:    parent,
			index:    &parent.PrimaryIndex,
			input:    "/1",
			expected: "/0/#",
		},
		{
			table:     parent,
			index:     &parent.PrimaryIndex,
			input:     "/1",
			inclusive: true,
			expected:  "/1/#",
		},

		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/3",
			expected: "/1/#/2/2/#",
		},
		{
			table:     child,
			index:     &child.PrimaryIndex,
			input:     "/1/#/2/3",
			inclusive: true,
			expected:  "/1/#/2/3/#",
		},

		// Idempotency.

		{
			table:    parent,
			index:    &parent.PrimaryIndex,
			input:    "/1/#",
			expected: "/1/#",
		},
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#",
			expected: "/1/#",
		},
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/2/#",
			expected: "/1/#/2/2/#",
		},

		// Children end keys wrt a "parent" index should be tightened
		// to read up to the last parent key.

		{
			table:    parent,
			index:    &parent.PrimaryIndex,
			input:    "/1/#/2/3",
			expected: "/1/#",
		},
		{
			table:     parent,
			index:     &parent.PrimaryIndex,
			input:     "/1/#/2/3",
			inclusive: true,
			expected:  "/1/#",
		},

		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/3/#/4",
			expected: "/1/#/2/3/#",
		},
		{
			table:     child,
			index:     &child.PrimaryIndex,
			input:     "/1/#/2/3/#/4",
			inclusive: true,
			expected:  "/1/#/2/3/#",
		},

		// Parent keys wrt child keys need not be tightened.

		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1",
			expected: "/1",
		},
		{
			table:     child,
			index:     &child.PrimaryIndex,
			input:     "/1",
			inclusive: true,
			expected:  "/2",
		},

		// Keys with a partial prefix of the primary key columns
		// need not be tightened since no interleaving can occur after.

		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/2",
			expected: "/1/#/2",
		},
		{
			table:     child,
			index:     &child.PrimaryIndex,
			input:     "/1/#/2",
			inclusive: true,
			expected:  "/1/#/3",
		},

		// Secondary indexes' end keys need not be tightened since
		// they cannot have interleaves.

		{
			table:    child,
			index:    &childDescIdx,
			input:    "/1/#/2/3",
			expected: "/1/#/2/3",
		},
		{
			table:     child,
			index:     &childDescIdx,
			input:     "/1/#/2/3",
			inclusive: true,
			expected:  "/1/#/2/4",
		},
		{
			table:    child,
			index:    &childDescIdx,
			input:    "/1/#/2",
			expected: "/1/#/2",
		},
		{
			table:     child,
			index:     &childDescIdx,
			input:     "/1/#/2",
			inclusive: true,
			expected:  "/1/#/3",
		},

		// Key with len > 1 tokens.
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/12345678901234/#/12345/12345678901234/#/123/1234567",
			expected: "/12345678901234/#/12345/12345678901234/#",
		},

		// Index key with extra columns (implicit primary key columns).
		// We should expect two extra columns (in addition to the
		// two index columns).
		{
			table:    child,
			index:    &childNonUniqueIdx,
			input:    "/2/3",
			expected: "/2/3",
		},
		{
			table:    child,
			index:    &childNonUniqueIdx,
			input:    "/2/3/4",
			expected: "/2/3/4",
		},
		{
			table:    child,
			index:    &childNonUniqueIdx,
			input:    "/2/3/4/5",
			expected: "/2/3/4/5",
		},
		// End key not adjusted since secondary indexes can't have
		// interleaved rows.
		{
			table:    child,
			index:    &childNonUniqueIdx,
			input:    "/2/3/4/5/#/10",
			expected: "/2/3/4/5/#/10",
		},

		{
			table:    child,
			index:    &childUniqueIdx,
			input:    "/2/3",
			expected: "/2/3",
		},
		// End key not adjusted since secondary indexes can't have
		// interleaved rows.
		{
			table:    child,
			index:    &childUniqueIdx,
			input:    "/2/3/4",
			expected: "/2/3/4",
		},
		{
			table:    child,
			index:    &childUniqueIdx,
			input:    "/2/NULLASC/4",
			expected: "/2/NULLASC/4",
		},
		{
			table:    child,
			index:    &childUniqueIdx,
			input:    "/2/NULLASC/4/5",
			expected: "/2/NULLASC/4/5",
		},
		// End key not adjusted since secondary indexes can't have
		// interleaved rows.
		{
			table:    child,
			index:    &childUniqueIdx,
			input:    "/2/NULLASC/4/5/#/6",
			expected: "/2/NULLASC/4/5/#/6",
		},

		// Keys with decimal values.
		// Not tightened since it's difficult to "go back" one logical
		// decimal value.
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/d3.4567",
			expected: "/1/#/2/d3.4567",
		},
		{
			table:     child,
			index:     &child.PrimaryIndex,
			input:     "/1/#/2/d3.4567",
			inclusive: true,
			expected:  "/1/#/2/d3.4567/#",
		},
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/d3.4567/#/8",
			expected: "/1/#/2/d3.4567/#",
		},

		// Keys with bytes values.
		// Not tightened since it's difficult to "go back" one logical
		// bytes value.
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/shelloworld",
			expected: "/1/#/2/shelloworld",
		},
		{
			table:     child,
			index:     &child.PrimaryIndex,
			input:     "/1/#/2/shelloworld",
			inclusive: true,
			expected:  "/1/#/2/shelloworld/#",
		},
		{
			table:    child,
			index:    &child.PrimaryIndex,
			input:    "/1/#/2/shelloworld/#/3",
			expected: "/1/#/2/shelloworld/#",
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			codec := keys.SystemSQLCodec
			actual := EncodeTestKey(t, kvDB, codec, ShortToLongKeyFmt(tc.input))
			actual, err := AdjustEndKeyForInterleave(codec, tc.table, tc.index, actual, tc.inclusive)
			if err != nil {
				t.Fatal(err)
			}

			expected := EncodeTestKey(t, kvDB, codec, ShortToLongKeyFmt(tc.expected))
			if !expected.Equal(actual) {
				t.Errorf("expected tightened end key %s, got %s", expected, actual)
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
