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
	"fmt"
	"os"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s path/to/docs", os.Args[0])
	os.Exit(-1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	outDir := os.Args[1]
	if stat, err := os.Stat(outDir); err != nil || !stat.IsDir() {
		fmt.Fprintf(os.Stderr, "%s does not exist", outDir)
		os.Exit(-1)
	}
	// TODO(dt): add diagrams support, respect os.Args[2] to pick mode.
	generateFuncsAndOps(outDir)
}
