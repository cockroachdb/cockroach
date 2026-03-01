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

	filteredType reflect.Type
}

// // NewQueryBuilder creates a new SQL query builder.
// func NewQueryBuilder() *QueryBuilder {
// 	return &QueryBuilder{
// 		args:     make([]interface{}, 0),
// 		argIndex: 1,
// 	}
// }

// NewQueryBuilder creates a new SQL query builder.
func NewQueryBuilderWithTypeHint(filteredType reflect.Type) *QueryBuilder {
	return &QueryBuilder{
		args:         make([]interface{}, 0),
		argIndex:     1,
		filteredType: filteredType,
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

	if err := qb.buildFilterGroup(fs); err != nil {
		return "", nil, err
	}

	return qb.whereClause.String(), qb.args, nil
}

// buildFilterGroup writes the SQL for a FilterSet's Filters and
// SubGroups, joined by the FilterSet's Logic operator.
// Does NOT write "WHERE" — that's BuildWhere's job.
func (qb *QueryBuilder) buildFilterGroup(fs *types.FilterSet) error {
	logic := " AND "
	if fs.Logic == types.LogicOr {
		logic = " OR "
	}

	partIndex := 0

	for i, filter := range fs.Filters {
		if partIndex > 0 {
			qb.whereClause.WriteString(logic)
		}
		if err := qb.buildFilterCondition(filter, qb.filteredType); err != nil {
			return errors.Wrapf(err, "filter %d", i)
		}
		partIndex++
	}

	for i, sg := range fs.SubGroups {
		if partIndex > 0 {
			qb.whereClause.WriteString(logic)
		}
		qb.whereClause.WriteString("(")
		if err := qb.buildFilterGroup(&sg); err != nil {
			return errors.Wrapf(err, "sub_group %d", i)
		}
		qb.whereClause.WriteString(")")
		partIndex++
	}

	return nil
}

// buildFilterCondition builds a single filter condition for SQL.
// Note: filter.Field should already be translated to db column names
// by the repository using filters.FieldMapper before calling this.
func (qb *QueryBuilder) buildFilterCondition(
	filter types.FieldFilter, filteredType reflect.Type,
) error {

	// Default to using filter.Field directly as the column name.
	fieldName := filter.Field

	// If we have a filteredType, try to get the DB column name from struct tags
	if filteredType != nil {
		// Check that the field exists in the struct
		// If it exists, it gives us two information:
		// 1. The field type
		// 2. The correct DB column name via struct tags (if needed)
		structField, found := filteredType.FieldByName(fieldName)
		if !found {
			return errors.Newf("field %q does not exist in type %s", fieldName, filteredType.Name())
		}

		// Use the DB column name from struct tags if available,
		// else we snake_case the field name by default as convention.
		dbTag := structField.Tag.Get("db")
		if dbTag != "" {
			fieldName = dbTag
		} else {
			fieldName = strcase.ToSnake(fieldName)
		}
	}

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
	case types.OpContains:
		// Cast to text for ILIKE to support non-text types like UUID
		qb.whereClause.WriteString(fmt.Sprintf("%s::text ILIKE $%d", fieldName, qb.argIndex))
		qb.args = append(qb.args, "%"+fmt.Sprint(filter.Value)+"%")
		qb.argIndex++
	case types.OpStartsWith:
		// Cast to text for ILIKE to support non-text types like UUID
		qb.whereClause.WriteString(fmt.Sprintf("%s::text ILIKE $%d", fieldName, qb.argIndex))
		qb.args = append(qb.args, fmt.Sprint(filter.Value)+"%")
		qb.argIndex++
	case types.OpEndsWith:
		// Cast to text for ILIKE to support non-text types like UUID
		qb.whereClause.WriteString(fmt.Sprintf("%s::text ILIKE $%d", fieldName, qb.argIndex))
		qb.args = append(qb.args, "%"+fmt.Sprint(filter.Value))
		qb.argIndex++
	case types.OpPresent:
		qb.whereClause.WriteString(fmt.Sprintf("%s IS NOT NULL", fieldName))
		// No arg needed for IS NOT NULL
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

// BuildOrderBy builds a SQL ORDER BY clause from SortParams.
// Note: sort.SortBy should already be translated to db column names
// by the repository using filters.FieldMapper before calling this.
func (qb *QueryBuilder) BuildOrderBy(sort *types.SortParams, filteredType reflect.Type) string {
	if sort == nil || sort.SortBy == "" {
		return ""
	}

	// Default to using sort.SortBy directly as the column name.
	fieldName := sort.SortBy

	// If we have a filteredType, try to get the DB column name from struct tags
	if filteredType != nil {
		// Check that the field exists in the struct
		// If it exists, it gives us two information:
		// 1. The field type
		// 2. The correct DB column name via struct tags (if needed)
		if structField, found := filteredType.FieldByName(fieldName); found {
			// Use the DB column name from struct tags if available
			if dbTag := structField.Tag.Get("db"); dbTag != "" {
				fieldName = dbTag
			}
		}
	}

	direction := "ASC"
	if sort.SortOrder == types.SortDescending {
		direction = "DESC"
	}

	// Note: We don't use LOWER() here because it only works with text types.
	// Case-insensitive sorting for text fields should be handled at the database
	// schema level (e.g., using COLLATE) or by the repository if needed.
	return fmt.Sprintf(" ORDER BY %s %s", fieldName, direction)
}

// BuildLimitOffset builds SQL LIMIT and OFFSET clauses from PaginationParams
// Returns empty string if count is -1 (unlimited)
func (qb *QueryBuilder) BuildLimitOffset(pagination *types.PaginationParams) string {
	if pagination == nil {
		return ""
	}

	limit := pagination.GetLimit()
	offset := pagination.GetOffset()

	// If limit is -1, it means unlimited (no LIMIT clause)
	if limit == -1 {
		return ""
	}

	return fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
}

// BuildWithCount builds both a SELECT query and a COUNT query
// baseSelectQuery should be like "SELECT * FROM table"
// baseCountQuery should be like "SELECT count(*) FROM table"
// defaultSort is used if no sort is specified in FilterSet (can be nil for no default)
// Returns: selectQuery, countQuery, args (for both queries), error
func (qb *QueryBuilder) BuildWithCount(
	baseSelectQuery string, baseCountQuery string, fs *types.FilterSet, defaultSort *types.SortParams,
) (string, string, []interface{}, error) {
	// Build WHERE clause (shared between both queries)
	var whereClause string
	var args []interface{}
	if fs != nil && !fs.IsEmpty() {
		if err := fs.Validate(); err != nil {
			return "", "", nil, errors.Wrap(err, "invalid filter set")
		}

		var err error
		whereClause, args, err = qb.BuildWhere(fs)
		if err != nil {
			return "", "", nil, errors.Wrap(err, "failed to build WHERE clause")
		}
	}

	// Build count query (just base + WHERE, no sorting or pagination)
	var countQueryBuilder strings.Builder
	countQueryBuilder.WriteString(baseCountQuery)
	if whereClause != "" {
		countQueryBuilder.WriteString(" ")
		countQueryBuilder.WriteString(whereClause)
	}

	// Build SELECT query (base + WHERE + ORDER BY + LIMIT/OFFSET)
	var selectQueryBuilder strings.Builder
	selectQueryBuilder.WriteString(baseSelectQuery)
	if whereClause != "" {
		selectQueryBuilder.WriteString(" ")
		selectQueryBuilder.WriteString(whereClause)
	}

	// Add sorting to SELECT query
	sortToUse := defaultSort
	if fs != nil && fs.Sort != nil {
		sortToUse = fs.Sort
	}
	if sortToUse != nil {
		selectQueryBuilder.WriteString(qb.BuildOrderBy(sortToUse, qb.filteredType))
	}

	// Add pagination to SELECT query
	if fs != nil && fs.Pagination != nil {
		selectQueryBuilder.WriteString(qb.BuildLimitOffset(fs.Pagination))
	}

	return selectQueryBuilder.String(), countQueryBuilder.String(), args, nil
}
