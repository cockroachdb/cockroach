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
// Author: Andrei Matei (andreimatei1@gmail.com)

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/encoding"
)

func convertBatchError(tableDesc *sqlbase.TableDescriptor, b *client.Batch) error {
	origPErr := b.MustPErr()
	if origPErr.Index == nil {
		return origPErr.GoError()
	}
	index := origPErr.Index.Index
	if index >= int32(len(b.Results)) {
		panic(fmt.Sprintf("index %d outside of results: %+v", index, b.Results))
	}
	result := b.Results[index]
	var alloc sqlbase.DatumAlloc
	if _, ok := origPErr.GetDetail().(*roachpb.ConditionFailedError); ok {
		for _, row := range result.Rows {
			// TODO(dan): There's too much internal knowledge of the sql table
			// encoding here (and this callsite is the only reason
			// DecodeIndexKeyPrefix is exported). Refactor this bit out.
			indexID, key, err := sqlbase.DecodeIndexKeyPrefix(&alloc, tableDesc, row.Key)
			if err != nil {
				return err
			}
			index, err := tableDesc.FindIndexByID(indexID)
			if err != nil {
				return err
			}
			valTypes, err := sqlbase.MakeKeyVals(tableDesc, index.ColumnIDs)
			if err != nil {
				return err
			}
			dirs := make([]encoding.Direction, 0, len(index.ColumnIDs))
			for _, dir := range index.ColumnDirections {
				convertedDir, err := dir.ToEncodingDirection()
				if err != nil {
					return err
				}
				dirs = append(dirs, convertedDir)
			}
			vals := make([]parser.Datum, len(valTypes))
			if _, err := sqlbase.DecodeKeyVals(&alloc, valTypes, vals, dirs, key); err != nil {
				return err
			}

			return sqlbase.NewUniquenessConstraintViolationError(index, vals)
		}
	}
	return origPErr.GoError()
}

// convertToErrWithPGCode recognizes errs that should have SQL error codes to be
// reported to the client and converts err to them. If this doesn't apply, err
// is returned.
// Note that this returns a new proto, and no fields from the original one are
// copied. So only use it in contexts where the only thing that matters in the
// response is the error detail.
// TODO(andrei): convertBatchError() above seems to serve similar purposes, but
// it's called from more specialized contexts. Consider unifying the two.
func convertToErrWithPGCode(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*roachpb.RetryableTxnError); ok {
		return sqlbase.NewRetryError(err)
	}
	return err
}
