// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package filters

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/memory"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/sql"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
)

// Constructor functions
func NewFilterSet() *types.FilterSet {
	return &types.FilterSet{
		Filters: make([]types.FieldFilter, 0),
		Logic:   types.LogicAnd,
	}
}

// Convenience constructors for subpackages
func NewSQLQueryBuilder() *sql.QueryBuilder {
	return sql.NewQueryBuilder()
}

func NewMemoryFilterEvaluator() *memory.FilterEvaluator {
	return memory.NewFilterEvaluator()
}
