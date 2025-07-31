// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"

// planDataSource contains the data source information for data
// produced by a planNode.
type planDataSource struct {
	// columns gives the result columns (always anonymous source).
	columns colinfo.ResultColumns

	// plan which can be used to retrieve the data.
	plan planNode
}
