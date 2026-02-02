// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package query

import (
	"strconv"

	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/gin-gonic/gin"
)

// ParsePaginationParams extracts SCIM-style pagination from query parameters
// Example: ?startIndex=11&count=25
// Default count is -1 (unlimited) if not specified
func ParsePaginationParams(c *gin.Context, defaultCount int) *filtertypes.PaginationParams {
	startIndexStr := c.Query("startIndex")
	countStr := c.Query("count")

	// If no pagination parameters provided, return nil (no pagination)
	if startIndexStr == "" && countStr == "" {
		return nil
	}

	startIndex, _ := strconv.Atoi(c.DefaultQuery("startIndex", "1"))
	count, _ := strconv.Atoi(c.DefaultQuery("count", strconv.Itoa(defaultCount)))

	// Ensure minimum values per SCIM spec
	if startIndex < 1 {
		startIndex = 1
	}

	return &filtertypes.PaginationParams{
		StartIndex: startIndex,
		Count:      count,
	}
}

// ParseSortParams extracts SCIM-style sorting from query parameters
// Example: ?sortBy=userName&sortOrder=ascending
func ParseSortParams(c *gin.Context) *filtertypes.SortParams {
	sortBy := c.Query("sortBy")
	if sortBy == "" {
		return nil // No sorting requested
	}

	sortOrder := filtertypes.SortOrder(c.DefaultQuery("sortOrder", "ascending"))

	// Validate sortOrder, default to ascending if invalid
	if sortOrder != filtertypes.SortAscending && sortOrder != filtertypes.SortDescending {
		sortOrder = filtertypes.SortAscending
	}

	return &filtertypes.SortParams{
		SortBy:    sortBy,
		SortOrder: sortOrder,
	}
}

// ParseQueryParams extracts both pagination and sorting in one call
func ParseQueryParams(
	c *gin.Context, defaultCount int,
) (pagination *filtertypes.PaginationParams, sort *filtertypes.SortParams) {
	return ParsePaginationParams(c, defaultCount), ParseSortParams(c)
}
