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
//
// Author: Daniel Harrison (daniel.harrison@gmail.com)

package sqlbase

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/randutil"
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
		columns[i] = ColumnDescriptor{ID: ColumnID(i + 1), Type: ColumnType{Kind: ColumnType_INT}}
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
			ID:                2,
			ColumnIDs:         secondaryColumnIDs,
			ImplicitColumnIDs: primaryColumnIDs,
			Unique:            true,
			ColumnDirections:  make([]IndexDescriptor_Direction, len(secondaryColumnIDs)),
			Interleave:        makeInterleave(2, test.secondaryInterleaves),
		}},
	}

	return tableDesc, colMap
}

func decodeIndex(
	a *DatumAlloc, tableDesc *TableDescriptor, index *IndexDescriptor, key []byte,
) ([]parser.Datum, error) {
	values := make([]parser.Datum, len(index.ColumnIDs))
	valTypes, err := MakeKeyVals(tableDesc, index.ColumnIDs)
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
	_, ok, err := DecodeIndexKey(a, tableDesc, index.ID, valTypes, values, colDirs, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Errorf("key did not match descriptor")
	}
	return values, nil
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
			t.primaryValues[j] = RandDatum(rng, ColumnType_INT, true)
		}

		t.secondaryInterleaves = make([]ID, rng.Intn(10))
		for j := range t.secondaryInterleaves {
			t.secondaryInterleaves[j] = ID(1 + rng.Intn(10))
		}
		valuesLen = randutil.RandIntInRange(rng, len(t.secondaryInterleaves)+1, len(t.secondaryInterleaves)+10)
		t.secondaryValues = make([]parser.Datum, valuesLen)
		for j := range t.secondaryValues {
			t.secondaryValues[j] = RandDatum(rng, ColumnType_INT, true)
		}

		tests = append(tests, t)
	}

	for i, test := range tests {
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
				if value.Compare(testValue) != 0 {
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
