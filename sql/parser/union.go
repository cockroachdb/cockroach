// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import "fmt"

// Union represents a UNION statement.
type Union struct {
	Type        string
	Left, Right SelectStatement
	All         bool
}

// Union.Type
const (
	astUnion     = "UNION"
	astExcept    = "EXCEPT"
	astIntersect = "INTERSECT"
)

func (node *Union) String() string {
	all := ""
	if node.All {
		all = " ALL"
	}
	return fmt.Sprintf("%s %s%s %s", node.Left, node.Type, all, node.Right)
}
