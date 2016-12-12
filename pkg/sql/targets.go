// Copyright 2016 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/pkg/errors"
)

func (s *selectNode) addRender(target parser.SelectExpr, desiredType parser.Type) error {
	// Pre-normalize any VarName so the work is not done twice below.
	if err := target.NormalizeTopLevelVarName(); err != nil {
		return err
	}

	if isStar, cols, typedExprs, err := checkRenderStar(
		target, s.source.info, s.ivarHelper, true); err != nil {
		return err
	} else if isStar {
		s.columns = append(s.columns, cols...)
		s.render = append(s.render, typedExprs...)
		s.isStar = true
		return nil
	}

	// When generating an output column name it should exactly match the original
	// expression, so determine the output column name before we perform any
	// manipulations to the expression.
	outputName := getRenderColName(target)

	normalized, err := s.planner.analyzeExpr(target.Expr, s.sourceInfo, s.ivarHelper, desiredType, false, "")
	if err != nil {
		return err
	}

	s.addRenderColumn(normalized, ResultColumn{Name: outputName, Typ: normalized.ResolvedType()})

	return nil
}

// checkRenderStar checks if the SelectExpr is either an
// UnqualifiedStar or an AllColumnsSelector. If so, we match the
// prefix of the name to one of the tables in the query and then
// expand the "*" into a list of columns. A ResultColumns and Expr
// pair is returned for each column.
func checkRenderStar(
	target parser.SelectExpr,
	src *dataSourceInfo,
	ivarHelper parser.IndexedVarHelper,
	allowStars bool,
) (isStar bool, columns ResultColumns, exprs []parser.TypedExpr, err error) {
	v, ok := target.Expr.(parser.VarName)
	if !ok {
		return false, nil, nil, nil
	}
	switch v.(type) {
	case parser.UnqualifiedStar, *parser.AllColumnsSelector:
		if !allowStars {
			return false, nil, nil, errors.Errorf("\"%s\" is not allowed in this position", v)
		}
	default:
		return false, nil, nil, nil
	}

	if target.As != "" {
		return false, nil, nil, errors.Errorf("\"%s\" cannot be aliased", v)
	}

	columns, exprs, err = src.expandStar(v, ivarHelper)
	return true, columns, exprs, err
}
