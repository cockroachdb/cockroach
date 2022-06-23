/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"fmt"
	"sort"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// QueryMatchesTemplates sees if the given query has the same fingerprint as one of the given templates
// (one is enough)
func QueryMatchesTemplates(query string, queryTemplates []string) (match bool, err error) {
	if len(queryTemplates) == 0 {
		return false, fmt.Errorf("No templates found")
	}
	bv := make(map[string]*querypb.BindVariable)

	normalize := func(q string) (string, error) {
		q, err := NormalizeAlphabetically(q)
		if err != nil {
			return "", err
		}
		stmt, err := Parse(q)
		if err != nil {
			return "", err
		}
		Normalize(stmt, bv, "")
		normalized := String(stmt)
		return normalized, nil
	}

	normalizedQuery, err := normalize(query)
	if err != nil {
		return false, err
	}

	for _, template := range queryTemplates {
		normalizedTemplate, err := normalize(template)
		if err != nil {
			return false, err
		}

		// compare!
		if normalizedTemplate == normalizedQuery {
			return true, nil
		}
	}
	return false, nil
}

// NormalizeAlphabetically rewrites given query such that:
// - WHERE 'AND' expressions are reordered alphabetically
func NormalizeAlphabetically(query string) (normalized string, err error) {
	stmt, err := Parse(query)
	if err != nil {
		return normalized, err
	}
	var where *Where
	switch stmt := stmt.(type) {
	case *Update:
		where = stmt.Where
	case *Delete:
		where = stmt.Where
	case *Select:
		where = stmt.Where
	}
	if where != nil {
		andExprs := SplitAndExpression(nil, where.Expr)
		sort.SliceStable(andExprs, func(i, j int) bool {
			return String(andExprs[i]) < String(andExprs[j])
		})
		var newWhere *Where
		for _, expr := range andExprs {
			if newWhere == nil {
				newWhere = &Where{
					Type: WhereClause,
					Expr: expr,
				}
			} else {
				newWhere.Expr = &AndExpr{
					Left:  newWhere.Expr,
					Right: expr,
				}
			}
		}
		switch stmt := stmt.(type) {
		case *Update:
			stmt.Where = newWhere
		case *Delete:
			stmt.Where = newWhere
		case *Select:
			stmt.Where = newWhere
		}
	}
	return String(stmt), nil
}
