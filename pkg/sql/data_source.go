// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
