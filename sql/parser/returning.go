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
//
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package parser

import "fmt"

// ReturningExprs represents RETURNING expressions.
type ReturningExprs SelectExprs

func (r ReturningExprs) String() string {
	if len(r) == 0 {
		return ""
	}
	return fmt.Sprintf(" RETURNING%s", SelectExprs(r))
}

// StatementType implements the Statement interface.
func (r ReturningExprs) StatementType() StatementType {
	if r != nil {
		return Rows
	}
	return RowsAffected
}
