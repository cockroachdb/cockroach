// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/errors"
	"github.com/iancoleman/strcase"
)

// QueryBuilder helps build SQL WHERE clauses from FilterSets.
type QueryBuilder struct {
	whereClause strings.Builder
	args        []interface{}
	argIndex    int
}

// NewQueryBuilder creates a new SQL query builder.
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		args:     make([]interface{}, 0),
		argIndex: 1,
	}
}

// BuildWhere builds a SQL WHERE clause from a FilterSet.
func (qb *QueryBuilder) BuildWhere(fs *types.FilterSet) (string, []interface{}, error) {
	if fs == nil || fs.IsEmpty() {
		return "", nil, nil
	}

	if err := fs.Validate(); err != nil {
		return "", nil, errors.Wrap(err, "invalid filter set")
	}

	qb.whereClause.WriteString("WHERE ")

	for i, filter := range fs.Filters {
		if i > 0 {
			if fs.Logic == types.LogicAnd {
				qb.whereClause.WriteString(" AND ")
			} else {
				qb.whereClause.WriteString(" OR ")
			}
		}

		if err := qb.buildFilterCondition(filter); err != nil {
			return "", nil, errors.Wrapf(err, "failed to build condition for filter %d", i)
		}
	}

	return qb.whereClause.String(), qb.args, nil
}

// buildFilterCondition builds a single filter condition for SQL.
func (qb *QueryBuilder) buildFilterCondition(filter types.FieldFilter) error {

	// In the database, the field names are expected to be in snake_case.
	fieldName := strcase.ToSnake(filter.Field)

	switch filter.Operator {
	case types.OpEqual:
		qb.whereClause.WriteString(fmt.Sprintf("%s = $%d", fieldName, qb.argIndex))
		qb.args = append(qb.args, filter.Value)
		qb.argIndex++
	case types.OpNotEqual:
		qb.whereClause.WriteString(fmt.Sprintf("%s != $%d", fieldName, qb.argIndex))
		qb.args = append(qb.args, filter.Value)
		qb.argIndex++
	case types.OpLess:
		qb.whereClause.WriteString(fmt.Sprintf("%s < $%d", fieldName, qb.argIndex))
		qb.args = append(qb.args, filter.Value)
		qb.argIndex++
	case types.OpLessEq:
		qb.whereClause.WriteString(fmt.Sprintf("%s <= $%d", fieldName, qb.argIndex))
		qb.args = append(qb.args, filter.Value)
		qb.argIndex++
	case types.OpGreater:
		qb.whereClause.WriteString(fmt.Sprintf("%s > $%d", fieldName, qb.argIndex))
		qb.args = append(qb.args, filter.Value)
		qb.argIndex++
	case types.OpGreaterEq:
		qb.whereClause.WriteString(fmt.Sprintf("%s >= $%d", fieldName, qb.argIndex))
		qb.args = append(qb.args, filter.Value)
		qb.argIndex++
	case types.OpLike:
		qb.whereClause.WriteString(fmt.Sprintf("%s LIKE $%d", fieldName, qb.argIndex))
		qb.args = append(qb.args, filter.Value)
		qb.argIndex++
	case types.OpNotLike:
		qb.whereClause.WriteString(fmt.Sprintf("%s NOT LIKE $%d", fieldName, qb.argIndex))
		qb.args = append(qb.args, filter.Value)
		qb.argIndex++
	case types.OpIn, types.OpNotIn:
		// Handle IN and NOT IN with multiple values
		values := reflect.ValueOf(filter.Value)
		if values.Kind() != reflect.Slice && values.Kind() != reflect.Array {
			return errors.Newf("IN/NOT_IN operator requires slice/array, got %T", filter.Value)
		}

		if values.Len() == 0 {
			return errors.New("IN/NOT_IN operator requires at least one value")
		}

		placeholders := make([]string, values.Len())
		for i := range values.Len() {
			placeholders[i] = fmt.Sprintf("$%d", qb.argIndex)
			qb.args = append(qb.args, values.Index(i).Interface())
			qb.argIndex++
		}

		operator := "IN"
		if filter.Operator == types.OpNotIn {
			operator = "NOT IN"
		}

		qb.whereClause.WriteString(fmt.Sprintf("%s %s (%s)",
			fieldName, operator, strings.Join(placeholders, ", ")))
	default:
		return errors.Newf("unsupported operator: %s", filter.Operator)
	}

	return nil
}
