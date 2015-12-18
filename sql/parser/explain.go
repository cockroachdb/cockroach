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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"fmt"
	"strings"
)

// Explain represents an EXPLAIN statement.
type Explain struct {
	Options   []string
	Statement Statement
}

func (node *Explain) String() string {
	if len(node.Options) > 0 {
		return fmt.Sprintf("EXPLAIN (%s) %s",
			strings.Join(node.Options, ", "), node.Statement)
	}
	return fmt.Sprintf("EXPLAIN %s", node.Statement)
}
