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
// Author: Radu Berinde (radu@cockroachlabs.com)

package sqlbase

import "github.com/cockroachdb/cockroach/sql/parser"

// RunFilter runs a filter expression and returns whether the filter passes.
func RunFilter(filter parser.TypedExpr, evalCtx *parser.EvalContext) (bool, error) {
	if filter == nil {
		return true, nil
	}

	d, err := filter.Eval(evalCtx)
	if err != nil {
		return false, err
	}

	return d != parser.DNull && bool(*d.(*parser.DBool)), nil
}
