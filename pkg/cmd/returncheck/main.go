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

package main

import (
	"os"

	"github.com/cockroachdb/returncheck"
	"github.com/kisielk/gotool"
)

func main() {
	targetPkg := "github.com/cockroachdb/cockroach/pkg/roachpb"
	targetTypeName := "Error"
	if err := returncheck.Run(gotool.ImportPaths(os.Args[1:]), targetPkg, targetTypeName); err != nil {
		os.Exit(1)
	}
}
