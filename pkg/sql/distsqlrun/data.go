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

package distsqlrun

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// convertToColumnOrdering converts an Ordering type (as defined in data.proto)
// to a sqlbase.ColumnOrdering type.
func convertToColumnOrdering(specOrdering Ordering) sqlbase.ColumnOrdering {
	ordering := make(sqlbase.ColumnOrdering, len(specOrdering.Columns))
	for i, c := range specOrdering.Columns {
		ordering[i].ColIdx = int(c.ColIdx)
		if c.Direction == Ordering_Column_ASC {
			ordering[i].Direction = encoding.Ascending
		} else {
			ordering[i].Direction = encoding.Descending
		}
	}
	return ordering
}

// convertToSpecOrdering converts a sqlbase.ColumnOrdering type
// to an Ordering type (as defined in data.proto).
func convertToSpecOrdering(columnOrdering sqlbase.ColumnOrdering) Ordering {
	return ConvertToMappedSpecOrdering(columnOrdering, nil)
}

// ConvertToMappedSpecOrdering converts a sqlbase.ColumnOrdering type
// to an Ordering type (as defined in data.proto), using the column
// indices contained in planToStreamColMap.
func ConvertToMappedSpecOrdering(
	columnOrdering sqlbase.ColumnOrdering, planToStreamColMap []int,
) Ordering {
	specOrdering := Ordering{}
	specOrdering.Columns = make([]Ordering_Column, len(columnOrdering))
	for i, c := range columnOrdering {
		colIdx := c.ColIdx
		if planToStreamColMap != nil {
			colIdx = planToStreamColMap[c.ColIdx]
			if colIdx == -1 {
				panic(fmt.Sprintf("column %d in sort ordering not available", c.ColIdx))
			}
		}
		specOrdering.Columns[i].ColIdx = uint32(colIdx)
		if c.Direction == encoding.Ascending {
			specOrdering.Columns[i].Direction = Ordering_Column_ASC
		} else {
			specOrdering.Columns[i].Direction = Ordering_Column_DESC
		}
	}
	return specOrdering
}
