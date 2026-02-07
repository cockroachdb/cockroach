// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scim

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/errors"
	filter "github.com/scim2/filter-parser/v2"
)

// ParseSCIMFilter parses a SCIM 2.0 filter string and converts it to a FilterSet.
// Example: `userName eq "john" and active eq true`
func ParseSCIMFilter(filterStr string) (filtertypes.FilterSet, error) {
	if filterStr == "" {
		return *filters.NewFilterSet(), nil
	}

	expr, err := filter.ParseFilter([]byte(filterStr))
	if err != nil {
		return *filters.NewFilterSet(), errors.Wrap(err, "failed to parse SCIM filter")
	}

	return expressionToFilterSet(expr)
}

// expressionToFilterSet recursively converts a SCIM filter expression to a FilterSet
func expressionToFilterSet(expr filter.Expression) (filtertypes.FilterSet, error) {
	switch e := expr.(type) {
	case *filter.AttributeExpression:
		return attributeExpressionToFilterSet(*e)

	case *filter.LogicalExpression:
		return logicalExpressionToFilterSet(*e)

	case *filter.NotExpression:
		return notExpressionToFilterSet(*e)

	case *filter.ValuePath:
		return *filters.NewFilterSet(), errors.New("complex value path filters (e.g., emails[type eq \"work\"]) are not yet supported")

	default:
		return *filters.NewFilterSet(), errors.Newf("unsupported SCIM expression type: %T", expr)
	}
}

// attributeExpressionToFilterSet converts a simple attribute comparison
// Example: userName eq "john"
func attributeExpressionToFilterSet(
	expr filter.AttributeExpression,
) (filtertypes.FilterSet, error) {
	fs := filtertypes.FilterSet{
		Logic: filtertypes.LogicAnd,
	}

	// Map SCIM operator to FilterOperator
	op, err := scimOperatorToFilterOperator(expr.Operator)
	if err != nil {
		return fs, err
	}

	// Get the field path (e.g., "userName", "name.familyName")
	fieldPath := attributePathToString(expr.AttributePath)

	// Add the filter
	fs.AddFilter(fieldPath, op, expr.CompareValue)

	return fs, nil
}

// logicalExpressionToFilterSet converts AND/OR expressions
// Example: userName eq "john" and active eq true
func logicalExpressionToFilterSet(expr filter.LogicalExpression) (filtertypes.FilterSet, error) {
	leftFS, err := expressionToFilterSet(expr.Left)
	if err != nil {
		return *filters.NewFilterSet(), errors.Wrap(err, "left expression")
	}

	rightFS, err := expressionToFilterSet(expr.Right)
	if err != nil {
		return *filters.NewFilterSet(), errors.Wrap(err, "right expression")
	}

	var logic filtertypes.LogicOperator
	opStr := strings.ToLower(string(expr.Operator))
	switch opStr {
	case "and":
		logic = filtertypes.LogicAnd
	case "or":
		logic = filtertypes.LogicOr
	default:
		return *filters.NewFilterSet(), errors.Newf("unsupported logical operator: %s", expr.Operator)
	}

	result := filtertypes.FilterSet{Logic: logic}

	// If a child can be safely inlined, append its filters directly.
	// This is safe when:
	//   - Same logic operator and no SubGroups (associativity: A and (B and C) = A and B and C)
	//   - Single filter with no SubGroups (logic is irrelevant for a single predicate)
	// Otherwise, nest it as a SubGroup to preserve grouping.
	mergeOrNest := func(child filtertypes.FilterSet) {
		canInline := len(child.SubGroups) == 0 &&
			(child.Logic == logic || len(child.Filters) <= 1)
		if canInline {
			result.Filters = append(result.Filters, child.Filters...)
		} else {
			result.SubGroups = append(result.SubGroups, child)
		}
	}

	mergeOrNest(leftFS)
	mergeOrNest(rightFS)

	return result, nil
}

// notExpressionToFilterSet handles NOT expressions
// Note: This is complex because FilterSet doesn't natively support NOT
// We'll need to convert "not (x eq y)" to "x ne y" where possible
func notExpressionToFilterSet(expr filter.NotExpression) (filtertypes.FilterSet, error) {
	// Try to convert the inner expression
	innerFS, err := expressionToFilterSet(expr.Expression)
	if err != nil {
		return *filters.NewFilterSet(), err
	}

	// If it's a single filter, try to negate the operator
	if len(innerFS.Filters) == 1 {
		filterItem := innerFS.Filters[0]
		negatedOp, err := negateOperator(filterItem.Operator)
		if err != nil {
			return *filters.NewFilterSet(), errors.Wrap(err, "cannot negate complex expression")
		}
		filterItem.Operator = negatedOp
		innerFS.Filters[0] = filterItem
		return innerFS, nil
	}

	// For complex expressions, we'd need to apply De Morgan's laws
	// This is complex - return error for now
	return *filters.NewFilterSet(), errors.New("NOT operator on complex expressions not yet supported")
}

// Helper functions

func scimOperatorToFilterOperator(
	scimOp filter.CompareOperator,
) (filtertypes.FilterOperator, error) {
	// Convert to string for comparison (CompareOperator has a String() method)
	opStr := strings.ToLower(string(scimOp))
	switch opStr {
	case "eq":
		return filtertypes.OpEqual, nil
	case "ne":
		return filtertypes.OpNotEqual, nil
	case "co":
		return filtertypes.OpContains, nil
	case "sw":
		return filtertypes.OpStartsWith, nil
	case "ew":
		return filtertypes.OpEndsWith, nil
	case "gt":
		return filtertypes.OpGreater, nil
	case "ge":
		return filtertypes.OpGreaterEq, nil
	case "lt":
		return filtertypes.OpLess, nil
	case "le":
		return filtertypes.OpLessEq, nil
	case "pr":
		return filtertypes.OpPresent, nil
	default:
		return "", errors.Newf("unsupported SCIM operator: %s", scimOp)
	}
}

func negateOperator(op filtertypes.FilterOperator) (filtertypes.FilterOperator, error) {
	switch op {
	case filtertypes.OpEqual:
		return filtertypes.OpNotEqual, nil
	case filtertypes.OpNotEqual:
		return filtertypes.OpEqual, nil
	case filtertypes.OpLess:
		return filtertypes.OpGreaterEq, nil
	case filtertypes.OpGreaterEq:
		return filtertypes.OpLess, nil
	case filtertypes.OpGreater:
		return filtertypes.OpLessEq, nil
	case filtertypes.OpLessEq:
		return filtertypes.OpGreater, nil
	default:
		return "", errors.Newf("cannot negate operator: %s", op)
	}
}

func attributePathToString(path filter.AttributePath) string {
	// Convert attribute path to dot notation
	// Example: name.familyName
	if path.SubAttribute != nil && *path.SubAttribute != "" {
		return path.AttributeName + "." + *path.SubAttribute
	}
	return path.AttributeName
}
