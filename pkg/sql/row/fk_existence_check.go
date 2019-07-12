// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// queueFkExistenceChecksForRow initiates FK existence checks for a
// given mutated row.
func queueFkExistenceChecksForRow(
	ctx context.Context,
	checkRunner *fkExistenceBatchChecker,
	mutatedIdxHelpers []fkExistenceCheckBaseHelper,
	mutatedRow tree.Datums,
	traceKV bool,
) error {
outer:
	for i, fk := range mutatedIdxHelpers {
		// See https://github.com/cockroachdb/cockroach/issues/20305 or
		// https://www.postgresql.org/docs/11/sql-createtable.html for details on the
		// different composite foreign key matching methods.
		//
		// TODO(knz): it is inefficient to do this dynamic dispatch based on
		// the match type and column layout again for every row. Consider
		// hoisting some of these checks to once per logical plan.
		switch fk.ref.Match {
		case sqlbase.ForeignKeyReference_SIMPLE:
			for _, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
				found, ok := fk.ids[colID]
				if !ok {
					return errors.AssertionFailedf("fk ids (%v) missing column id %d", fk.ids, colID)
				}
				if mutatedRow[found] == tree.DNull {
					continue outer
				}
			}
			if err := checkRunner.addCheck(ctx, mutatedRow, &mutatedIdxHelpers[i], traceKV); err != nil {
				return err
			}

		case sqlbase.ForeignKeyReference_FULL:
			var nulls, notNulls bool
			for _, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
				found, ok := fk.ids[colID]
				if !ok {
					return errors.AssertionFailedf("fk ids (%v) missing column id %d", fk.ids, colID)
				}
				if mutatedRow[found] == tree.DNull {
					nulls = true
				} else {
					notNulls = true
				}
			}
			if nulls && notNulls {
				// TODO(bram): expand this error to show more details.
				return pgerror.Newf(pgcode.ForeignKeyViolation,
					"foreign key violation: MATCH FULL does not allow mixing of null and nonnull values %s for %s",
					mutatedRow, fk.ref.Name,
				)
			}
			// Never check references for MATCH FULL that are all nulls.
			if nulls {
				continue
			}
			if err := checkRunner.addCheck(ctx, mutatedRow, &mutatedIdxHelpers[i], traceKV); err != nil {
				return err
			}

		case sqlbase.ForeignKeyReference_PARTIAL:
			return unimplemented.NewWithIssue(20305, "MATCH PARTIAL not supported")

		default:
			return errors.AssertionFailedf("unknown composite key match type: %v", fk.ref.Match)
		}
	}
	return nil
}
