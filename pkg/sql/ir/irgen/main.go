// Copyright 2017 The Cockroach Authors.
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

package main

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/analyzer"
	"github.com/cockroachdb/cockroach/pkg/sql/ir/irgen/parser"
)

func main() {
	defs, err := parser.Parse("", os.Stdin)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
	_, err = analyzer.Analyze(defs)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
}
