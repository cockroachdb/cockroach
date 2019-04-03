// Copyright 2019 The Cockroach Authors.
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

package row

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// queueFkExistenceChecksForRow initiates FK existence checks for a
// given mutated row.
func queueFkExistenceChecksForRow(
	ctx context.Context,
	checkRunner *fkExistenceBatchChecker,
	fkInfo map[sqlbase.IndexID][]fkExistenceCheckBaseHelper,
	mutatedIdx sqlbase.IndexID,
	mutatedRow tree.Datums,
	traceKV bool,
) error {
outer:
	for i, fk := range fkInfo[mutatedIdx] {
		// See https://github.com/cockroachdb/cockroach/issues/20305 or
		// https://www.postgresql.org/docs/11/sql-createtable.html for details on the
		// different composite foreign key matching methods.
		//
		// TODO(knz): it is efficient to do this dynamic dispatch based on
		// the match type and column layout again for every row. Consider
		// hoisting some of these checks to once per logical plan.
		switch fk.ref.Match {
		case sqlbase.ForeignKeyReference_SIMPLE:
			for _, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
				found, ok := fk.ids[colID]
				if !ok {
					return pgerror.AssertionFailedf("fk ids (%v) missing column id %d", fk.ids, colID)
				}
				if mutatedRow[found] == tree.DNull {
					continue outer
				}
			}
			if err := checkRunner.addCheck(ctx, mutatedRow, &fkInfo[mutatedIdx][i], traceKV); err != nil {
				return err
			}

		case sqlbase.ForeignKeyReference_FULL:
			var nulls, notNulls bool
			for _, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
				found, ok := fk.ids[colID]
				if !ok {
					return pgerror.AssertionFailedf("fk ids (%v) missing column id %d", fk.ids, colID)
				}
				if mutatedRow[found] == tree.DNull {
					nulls = true
				} else {
					notNulls = true
				}
			}
			if nulls && notNulls {
				// TODO(bram): expand this error to show more details.
				return pgerror.Newf(pgerror.CodeForeignKeyViolationError,
					"foreign key violation: MATCH FULL does not allow mixing of null and nonnull values %s for %s",
					mutatedRow, fk.ref.Name,
				)
			}
			// Never check references for MATCH FULL that are all nulls.
			if nulls {
				continue
			}
			if err := checkRunner.addCheck(ctx, mutatedRow, &fkInfo[mutatedIdx][i], traceKV); err != nil {
				return err
			}

		case sqlbase.ForeignKeyReference_PARTIAL:
			return pgerror.UnimplementedWithIssue(20305, "MATCH PARTIAL not supported")

		default:
			return pgerror.AssertionFailedf("unknown composite key match type: %v", fk.ref.Match)
		}
	}
	return nil
}
