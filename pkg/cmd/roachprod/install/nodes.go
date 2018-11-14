// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package install

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func allNodes(total int) []int {
	r := make([]int, total)
	for i := range r {
		r[i] = i + 1
	}
	return r
}

// ListNodes TODO(peter): document
func ListNodes(s string, total int) ([]int, error) {
	if s == "all" {
		return allNodes(total), nil
	}

	m := map[int]bool{}
	for _, p := range strings.Split(s, ",") {
		parts := strings.Split(p, "-")
		switch len(parts) {
		case 1:
			i, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, err
			}
			m[i] = true
		case 2:
			from, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, err
			}
			to, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, err
			}
			for i := from; i <= to; i++ {
				m[i] = true
			}
		default:
			return nil, fmt.Errorf("unable to parse nodes specification: %s", p)
		}
	}

	r := make([]int, 0, len(m))
	for i := range m {
		r = append(r, i)
	}
	sort.Ints(r)
	return r, nil
}
