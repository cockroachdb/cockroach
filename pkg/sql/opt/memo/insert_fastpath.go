// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This module holds functions related to building insert fast path
// foreign key and uniqueness checks.

package memo

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/mutations"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// ValuesLegalForInsertFastPath tests if `values` is a Values expression that
// has no subqueries or UDFs and has less rows than the max number of entries in
// a KV batch for a mutation operation.
func ValuesLegalForInsertFastPath(values *ValuesExpr) bool {
	//  - The input is Values with at most mutations.MaxBatchSize, and there are no
	//    subqueries;
	//    (note that mutations.MaxBatchSize() is a quantity of keys in the batch
	//     that we send, not a number of rows. We use this as a guideline only,
	//     and there is no guarantee that we won't produce a bigger batch.)
	if values.ChildCount() > mutations.MaxBatchSize(false /* forceProductionMaxBatchSize */) ||
		values.Relational().HasSubquery ||
		values.Relational().HasUDF {
		return false
	}
	return true
}

// MkUniqueCheckErr generates a user-friendly error describing a uniqueness
// violation. The keyVals are the values that correspond to the
// cat.UniqueConstraint columns.
func MkUniqueCheckErr(md *opt.Metadata, c *UniqueChecksItem, keyVals tree.Datums) error {
	tabMeta := md.TableMeta(c.Table)
	uc := tabMeta.Table.Unique(c.CheckOrdinal)
	constraintName := uc.Name()
	var msg, details bytes.Buffer

	// Generate an error of the form:
	//   ERROR:  duplicate key value violates unique constraint "foo"
	//   DETAIL: Key (k)=(2) already exists.
	msg.WriteString("duplicate key value violates unique constraint ")
	lexbase.EncodeEscapedSQLIdent(&msg, constraintName)

	details.WriteString("Key (")
	for i := 0; i < uc.ColumnCount(); i++ {
		if i > 0 {
			details.WriteString(", ")
		}
		col := tabMeta.Table.Column(uc.ColumnOrdinal(tabMeta.Table, i))
		details.WriteString(string(col.ColName()))
	}
	details.WriteString(")=(")
	for i, d := range keyVals {
		if i > 0 {
			details.WriteString(", ")
		}
		details.WriteString(d.String())
	}

	details.WriteString(") already exists.")

	return errors.WithDetail(
		pgerror.WithConstraintName(
			pgerror.Newf(pgcode.UniqueViolation, "%s", msg.String()),
			constraintName,
		),
		details.String(),
	)
}
