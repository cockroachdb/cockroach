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
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type indexKeyTest struct {
	primaryInterleaves   []ID
	secondaryInterleaves []ID
	primaryValues        []parser.Datum // len must be at least primaryInterleaveComponents+1
	secondaryValues      []parser.Datum // len must be at least secondaryInterleaveComponents+1
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
				IndexID:         IndexID(ancestorTableID + 1),
				SharedPrefixLen: 1,
			}
		}
		return interleave
	}

	tableDesc := TableDescriptor{
		ID:      50,
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
	a *DatumAlloc, tableDesc *TableDescriptor, index *IndexDescriptor, key []byte,
) ([]parser.Datum, error) {
	values, err := MakeEncodedKeyVals(tableDesc, index.ColumnIDs)
	if err != nil {
		return nil, err
	}
	colDirs := make([]encoding.Direction, len(index.ColumnDirections))
	for i, dir := range index.ColumnDirections {
		colDirs[i], err = dir.ToEncodingDirection()
		if err != nil {
			return nil, err
		}
	}
	_, ok, err := DecodeIndexKey(a, tableDesc, index, values, colDirs, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Errorf("key did not match descriptor")
	}

	decodedValues := make([]parser.Datum, len(values))
	var da DatumAlloc
	for i, value := range values {
		err := value.EnsureDecoded(&da)
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
		{nil, nil,
			[]parser.Datum{parser.NewDInt(10)},
			[]parser.Datum{parser.NewDInt(20)},
		},
		{[]ID{100}, nil,
			[]parser.Datum{parser.NewDInt(10), parser.NewDInt(11)},
			[]parser.Datum{parser.NewDInt(20)},
		},
		{[]ID{100, 200}, nil,
			[]parser.Datum{parser.NewDInt(10), parser.NewDInt(11), parser.NewDInt(12)},
			[]parser.Datum{parser.NewDInt(20)},
		},
		{nil, []ID{100},
			[]parser.Datum{parser.NewDInt(10)},
			[]parser.Datum{parser.NewDInt(20), parser.NewDInt(21)},
		},
		{[]ID{100}, []ID{100},
			[]parser.Datum{parser.NewDInt(10), parser.NewDInt(11)},
			[]parser.Datum{parser.NewDInt(20), parser.NewDInt(21)},
		},
		{[]ID{100}, []ID{200},
			[]parser.Datum{parser.NewDInt(10), parser.NewDInt(11)},
			[]parser.Datum{parser.NewDInt(20), parser.NewDInt(21)},
		},
		{[]ID{100, 200}, []ID{100, 300},
			[]parser.Datum{parser.NewDInt(10), parser.NewDInt(11), parser.NewDInt(12)},
			[]parser.Datum{parser.NewDInt(20), parser.NewDInt(21), parser.NewDInt(22)},
		},
	}

	for i := 0; i < 1000; i++ {
		var t indexKeyTest

		t.primaryInterleaves = make([]ID, rng.Intn(10))
		for j := range t.primaryInterleaves {
			t.primaryInterleaves[j] = ID(1 + rng.Intn(10))
		}
		valuesLen := randutil.RandIntInRange(rng, len(t.primaryInterleaves)+1, len(t.primaryInterleaves)+10)
		t.primaryValues = make([]parser.Datum, valuesLen)
		for j := range t.primaryValues {
			t.primaryValues[j] = RandDatum(rng, ColumnType{SemanticType: ColumnType_INT}, true)
		}

		t.secondaryInterleaves = make([]ID, rng.Intn(10))
		for j := range t.secondaryInterleaves {
			t.secondaryInterleaves[j] = ID(1 + rng.Intn(10))
		}
		valuesLen = randutil.RandIntInRange(rng, len(t.secondaryInterleaves)+1, len(t.secondaryInterleaves)+10)
		t.secondaryValues = make([]parser.Datum, valuesLen)
		for j := range t.secondaryValues {
			t.secondaryValues[j] = RandDatum(rng, ColumnType{SemanticType: ColumnType_INT}, true)
		}

		tests = append(tests, t)
	}

	for i, test := range tests {
		evalCtx := parser.NewTestingEvalContext()
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
			values, err := decodeIndex(&a, &tableDesc, index, entry.Key)
			if err != nil {
				t.Fatal(err)
			}

			for j, value := range values {
				testValue := testValues[colMap[index.ColumnIDs[j]]]
				if value.Compare(evalCtx, testValue) != 0 {
					t.Fatalf("%d: value %d got %q but expected %q", i, j, value, testValue)
				}
			}

			indexID, _, err := DecodeIndexKeyPrefix(&a, &tableDesc, entry.Key)
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
	datum    parser.DArray
	encoding []byte
}

func TestArrayEncoding(t *testing.T) {
	tests := []arrayEncodingTest{
		{
			"empty int array",
			parser.DArray{
				ParamTyp: parser.TypeInt,
				Array:    parser.Datums{},
			},
			[]byte{1, 3, 0},
		}, {
			"single int array",
			parser.DArray{
				ParamTyp: parser.TypeInt,
				Array:    parser.Datums{parser.NewDInt(1)},
			},
			[]byte{1, 3, 1, 2},
		}, {
			"multiple int array",
			parser.DArray{
				ParamTyp: parser.TypeInt,
				Array:    parser.Datums{parser.NewDInt(1), parser.NewDInt(2), parser.NewDInt(3)},
			},
			[]byte{1, 3, 3, 2, 4, 6},
		}, {
			"string array",
			parser.DArray{
				ParamTyp: parser.TypeString,
				Array:    parser.Datums{parser.NewDString("foo"), parser.NewDString("bar"), parser.NewDString("baz")},
			},
			[]byte{1, 6, 3, 3, 102, 111, 111, 3, 98, 97, 114, 3, 98, 97, 122},
		}, {
			"bool array",
			parser.DArray{
				ParamTyp: parser.TypeBool,
				Array:    parser.Datums{parser.MakeDBool(true), parser.MakeDBool(false)},
			},
			[]byte{1, 10, 2, 10, 11},
		}, {
			"array containing a single null",
			parser.DArray{
				ParamTyp: parser.TypeInt,
				Array:    parser.Datums{parser.DNull},
				HasNulls: true,
			},
			[]byte{17, 3, 1, 1},
		}, {
			"array containing multiple nulls",
			parser.DArray{
				ParamTyp: parser.TypeInt,
				Array:    parser.Datums{parser.NewDInt(1), parser.DNull, parser.DNull},
				HasNulls: true,
			},
			[]byte{17, 3, 3, 6, 2},
		}, {
			"array whose NULL bitmap spans exactly one byte",
			parser.DArray{
				ParamTyp: parser.TypeInt,
				Array: parser.Datums{
					parser.NewDInt(1), parser.DNull, parser.DNull, parser.NewDInt(2), parser.NewDInt(3),
					parser.NewDInt(4), parser.NewDInt(5), parser.NewDInt(6),
				},
				HasNulls: true,
			},
			[]byte{17, 3, 8, 6, 2, 4, 6, 8, 10, 12},
		}, {
			"array whose NULL bitmap spans more than one byte",
			parser.DArray{
				ParamTyp: parser.TypeInt,
				Array: parser.Datums{
					parser.NewDInt(1), parser.DNull, parser.DNull, parser.NewDInt(2), parser.NewDInt(3),
					parser.NewDInt(4), parser.NewDInt(5), parser.NewDInt(6), parser.DNull,
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
			if d.Compare(parser.NewTestingEvalContext(), &test.datum) != 0 {
				t.Fatalf("expected %v to decode to %s, got %s", enc, test.datum.String(), d.String())
			}
		})
	}
}

func BenchmarkArrayEncoding(b *testing.B) {
	ary := parser.DArray{ParamTyp: parser.TypeInt, Array: parser.Datums{}}
	for i := 0; i < 10000; i++ {
		_ = ary.Append(parser.NewDInt(1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = encodeArray(&ary, nil)
	}
}
