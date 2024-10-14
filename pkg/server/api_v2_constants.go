// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import "strings"

// Shared url query string keys.
const (
	sortByKey    = "sortBy"
	sortOrderKey = "sortOrder"
	pageNumKey   = "pageNum"
	pageSizeKey  = "pageSize"
)

// validateSortOrderValue validates the sort order value and returns
// the sql sort order value and a boolean indicating if the value is
// valid. If the value is empty, it defaults to "ASC".
func validateSortOrderValue(sortOrder string) (string, bool) {
	toUpper := strings.ToUpper(sortOrder)
	switch toUpper {
	case "":
		return "ASC", true
	case "ASC", "DESC":
		return toUpper, true
	default:
		return "", false
	}
}
