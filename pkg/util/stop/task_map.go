// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stop

import (
	"fmt"
	"sort"
	"strings"
)

// A TaskMap is returned by taskRegistry.RunningTasks().
type TaskMap map[string]int

// String implements fmt.Stringer and returns a sorted multi-line listing of
// the TaskMap.
func (tm TaskMap) String() string {
	var lines []string
	for location, num := range tm {
		lines = append(lines, fmt.Sprintf("%-6d %s", num, location))
	}
	sort.Sort(sort.Reverse(sort.StringSlice(lines)))
	return strings.Join(lines, "\n")
}
