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
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/encoding"
)

type errUniquenessConstraintViolation struct {
	index *IndexDescriptor
	vals  []parser.Datum
}

func (e errUniquenessConstraintViolation) Error() string {
	valStrs := make([]string, 0, len(e.vals))
	for _, val := range e.vals {
		valStrs = append(valStrs, val.String())
	}

	return fmt.Sprintf("duplicate key value (%s)=(%s) violates unique constraint %q",
		strings.Join(e.index.ColumnNames, ","),
		strings.Join(valStrs, ","),
		e.index.Name)
}

func convertBatchError(tableDesc *TableDescriptor, b client.Batch, origPErr *roachpb.Error) *roachpb.Error {
	if origPErr.Index == nil {
		return origPErr
	}
	index := origPErr.Index.Index
	if index >= int32(len(b.Results)) {
		panic(fmt.Sprintf("index %d outside of results: %+v", index, b.Results))
	}
	result := b.Results[index]
	if _, ok := origPErr.GoError().(*roachpb.ConditionFailedError); ok {
		for _, row := range result.Rows {
			indexID, key, pErr := decodeIndexKeyPrefix(tableDesc, row.Key)
			if pErr != nil {
				return pErr
			}
			index, pErr := tableDesc.FindIndexByID(indexID)
			if pErr != nil {
				return pErr
			}
			valTypes, pErr := makeKeyVals(tableDesc, index.ColumnIDs)
			if pErr != nil {
				return pErr
			}
			dirs := make([]encoding.Direction, 0, len(index.ColumnIDs))
			for _, dir := range index.ColumnDirections {
				convertedDir, err := dir.toEncodingDirection()
				if err != nil {
					return roachpb.NewError(err)
				}
				dirs = append(dirs, convertedDir)
			}
			vals := make([]parser.Datum, len(valTypes))
			if _, pErr := decodeKeyVals(valTypes, vals, dirs, key); pErr != nil {
				return pErr
			}

			return roachpb.NewError(errUniquenessConstraintViolation{index: index, vals: vals})
		}
	}
	return origPErr
}
