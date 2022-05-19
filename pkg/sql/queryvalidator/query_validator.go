// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package queryvalidator

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type MemoFactory func(stmt tree.Statement) (*memo.Memo, error)

type QueryValidator struct {
	memoFactory MemoFactory
}

func New(optBuilderFactor MemoFactory) *QueryValidator {
	return &QueryValidator{
		memoFactory: optBuilderFactor,
	}
}

func (v *QueryValidator) Validate(stmt tree.Statement, expectedTypes []*types.T) error {
	stmtMemo, err := v.memoFactory(stmt)
	if stmtMemo != nil && stmtMemo.RootExpr() != nil {
		rootExpr := stmtMemo.RootExpr()
		if _, ok := rootExpr.(memo.RelExpr); ok {
			outputCols := stmtMemo.RootProps().Presentation
			for i, col := range outputCols {
				meta := stmtMemo.Metadata().ColumnMeta(col.ID)
				colType := meta.Type
				expectedType := expectedTypes[i]
				if !expectedType.Equivalent(colType) && cast.ValidCast(colType, expectedTypes[0], cast.ContextImplicit) {
					return pgerror.New(pgcode.WrongObjectType, "types do not match")
				}
			}
		} else if scalarExpr, ok := stmtMemo.RootExpr().(opt.ScalarExpr); ok {
			colType := scalarExpr.DataType()
			if len(expectedTypes) != 1 || (!expectedTypes[0].Equivalent(colType) && cast.ValidCast(colType, expectedTypes[0], cast.ContextImplicit)) {
				return pgerror.New(pgcode.WrongObjectType, "type do not match")
			}
		} else {
			panic("unexpected memo expression")
		}
	}

	if err != nil {
		return err
	}
	return nil
}
