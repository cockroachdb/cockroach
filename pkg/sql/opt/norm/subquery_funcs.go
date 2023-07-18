// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/errors"
)

// SingleConstValue returns the single constant value in rows. If rows is not a
// single-row, single-column, constant value, ok=false is returned.
func (c *CustomFuncs) SingleConstValue(rows memo.ScalarListExpr) (_ opt.ScalarExpr, ok bool) {
	if len(rows) != 1 {
		return nil, false
	}
	t, ok := rows[0].(*memo.TupleExpr)
	if !ok {
		panic(errors.AssertionFailedf("expected row to be a tuple"))
	}
	if len(t.Elems) != 1 {
		return nil, false
	}
	constant, ok := t.Elems[0].(*memo.ConstExpr)
	return constant, ok
}
