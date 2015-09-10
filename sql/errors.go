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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
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

func runBatchWithErrorConversion(tableDesc *TableDescriptor, runBatch func(*client.Batch) error, b client.Batch) error {
	if err := runBatch(&b); err != nil {
		for _, result := range b.Results {
			if _, ok := result.Err.(*proto.ConditionFailedError); ok {
				for _, row := range result.Rows {
					indexID, key, err := decodeIndexKeyPrefix(tableDesc, row.Key)
					if err != nil {
						return err
					}
					index, err := tableDesc.FindIndexByID(indexID)
					if err != nil {
						return err
					}
					valTypes, err := makeKeyVals(tableDesc, index.ColumnIDs)
					if err != nil {
						return err
					}
					vals := make([]parser.Datum, len(valTypes))
					if _, err := decodeKeyVals(valTypes, vals, key); err != nil {
						return err
					}

					return errUniquenessConstraintViolation{index: index, vals: vals}
				}
			}
		}
		return err
	}

	return nil
}
