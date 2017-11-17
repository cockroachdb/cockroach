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

// cockroach-short is an entry point for a CockroachDB binary that excludes
// certain components that are slow to build or have heavyweight dependencies.
// At present, the only excluded component is the web UI.
//
// The ccl hook import below means building this will produce CCL'ed binaries.
// This file itself remains Apache2 to preserve the organization of ccl code
// under the /pkg/ccl subtree, but is unused for pure FLOSS builds.
package main

import (
	_ "github.com/cockroachdb/cockroach/pkg/ccl" // ccl init hooks
	"github.com/cockroachdb/cockroach/pkg/cli"
)

func main() {
	cli.Main()
}
