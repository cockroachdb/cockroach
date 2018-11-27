// Copyright 2014 The Cockroach Authors.
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

// cockroach-oss is an entry point for a CockroachDB binary that excludes all
// CCL-licensed code.
package main

import (
	"github.com/cockroachdb/cockroach/pkg/cli"
	_ "github.com/cockroachdb/cockroach/pkg/ui/distoss" // web UI init hooks
)

func main() {
	cli.Main()
}
