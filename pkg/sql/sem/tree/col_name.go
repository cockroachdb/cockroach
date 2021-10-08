// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// GetRenderColName computes a name for a result column.
// A name specified with AS takes priority, otherwise a name
// is derived from the expression.
//
// This function is meant to be used on untransformed syntax trees.
//
// The algorithm is borrowed from FigureColName() in PostgreSQL 10, to be
// found in src/backend/parser/parse_target.c. We reuse this algorithm
// to provide names more compatible with PostgreSQL.
func GetRenderColName(searchPath sessiondata.SearchPath, target SelectExpr) (string, error) {
	if target.As != "" {
		return string(target.As), nil
	}

	_, s, err := ComputeColNameInternal(searchPath, target.Expr)
	if err != nil {
		return s, err
	}
	if len(s) == 0 {
		s = "?column?"
	}
	return s, nil
}

// ComputeColNameInternal is the workhorse for GetRenderColName.
// The return value indicates the strength of the confidence in the result:
// 0 - no information
// 1 - second-best name choice
// 2 - good name choice
//
// The algorithm is borrowed from FigureColnameInternal in PostgreSQL 10,
// to be found in src/backend/parser/parse_target.c.
func ComputeColNameInternal(sp sessiondata.SearchPath, target Expr) (int, string, error) {
	// The order of the type cases below mirrors that of PostgreSQL's
	// own code, so that code reviews can more easily compare the two
	// implementations.
	switch e := target.(type) {
	case *UnresolvedName:
		if e.Star {
			return 0, "", nil
		}
		return 2, e.Parts[0], nil

	case *ColumnItem:
		return 2, e.Column(), nil

	case *IndirectionExpr:
		return ComputeColNameInternal(sp, e.Expr)

	case *FuncExpr:
		fd, err := e.Func.Resolve(sp)
		if err != nil {
			return 0, "", err
		}
		return 2, fd.Name, nil

	case *NullIfExpr:
		return 2, "nullif", nil

	case *IfExpr:
		return 2, "if", nil

	case *ParenExpr:
		return ComputeColNameInternal(sp, e.Expr)

	case *CastExpr:
		strength, s, err := ComputeColNameInternal(sp, e.Expr)
		if err != nil {
			return 0, "", err
		}
		if strength <= 1 {
			if typ, ok := GetStaticallyKnownType(e.Type); ok {
				return 0, computeCastName(typ), nil
			}
			return 1, e.Type.SQLString(), nil
		}
		return strength, s, nil

	case *AnnotateTypeExpr:
		// Ditto CastExpr.
		strength, s, err := ComputeColNameInternal(sp, e.Expr)
		if err != nil {
			return 0, "", err
		}
		if strength <= 1 {
			if typ, ok := GetStaticallyKnownType(e.Type); ok {
				return 0, computeCastName(typ), nil
			}
			return 1, e.Type.SQLString(), nil
		}
		return strength, s, nil

	case *CollateExpr:
		return ComputeColNameInternal(sp, e.Expr)

	case *ArrayFlatten:
		return 2, "array", nil

	case *Subquery:
		if e.Exists {
			return 2, "exists", nil
		}
		return computeColNameInternalSubquery(sp, e.Select)

	case *CaseExpr:
		strength, s, err := 0, "", error(nil)
		if e.Else != nil {
			strength, s, err = ComputeColNameInternal(sp, e.Else)
		}
		if strength <= 1 {
			s = "case"
			strength = 1
		}
		return strength, s, err

	case *Array:
		return 2, "array", nil

	case *Tuple:
		if e.Row {
			return 2, "row", nil
		}
		if len(e.Exprs) == 1 {
			if len(e.Labels) > 0 {
				return 2, e.Labels[0], nil
			}
			return ComputeColNameInternal(sp, e.Exprs[0])
		}

	case *CoalesceExpr:
		return 2, "coalesce", nil

		// CockroachDB-specific nodes follow.
	case *IfErrExpr:
		if e.Else == nil {
			return 2, "iserror", nil
		}
		return 2, "iferror", nil

	case *ColumnAccessExpr:
		return 2, string(e.ColName), nil

	case *DBool:
		// PostgreSQL implements the "true" and "false" literals
		// by generating the expressions 't'::BOOL and 'f'::BOOL, so
		// the derived column name is just "bool". Do the same.
		return 1, "bool", nil
	}

	return 0, "", nil
}

// computeColNameInternalSubquery handles the cases of subqueries that
// cannot be handled by the function above due to the Go typing
// differences.
func computeColNameInternalSubquery(
	sp sessiondata.SearchPath, s SelectStatement,
) (int, string, error) {
	switch e := s.(type) {
	case *ParenSelect:
		return computeColNameInternalSubquery(sp, e.Select.Select)
	case *ValuesClause:
		if len(e.Rows) > 0 && len(e.Rows[0]) == 1 {
			return 2, "column1", nil
		}
	case *SelectClause:
		if len(e.Exprs) == 1 {
			if len(e.Exprs[0].As) > 0 {
				return 2, string(e.Exprs[0].As), nil
			}
			return ComputeColNameInternal(sp, e.Exprs[0].Expr)
		}
	}
	return 0, "", nil
}

// computeCastName returns the name manufactured by Postgres for a computed (or
// annotated, in case of CRDB) column.
func computeCastName(typ *types.T) string {
	// Postgres uses the array element type name in case of array casts.
	if typ.Family() == types.ArrayFamily {
		typ = typ.ArrayContents()
	}
	return typ.PGName()

}
