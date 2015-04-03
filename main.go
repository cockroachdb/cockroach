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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"

	"github.com/cockroachdb/cockroach/server/cli"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

func init() {
	// If log directory has not been set, set -alsologtostderr to true.
	var hasLogDir, hasAlsoLogStderr bool
	for _, arg := range os.Args[1:] {
		switch arg {
		case "-log_dir", "--log_dir":
			hasLogDir = true
		case "-alsologtostderr", "--alsologtostderr":
			hasAlsoLogStderr = true
		}
	}
	if !hasLogDir && !hasAlsoLogStderr {
		flag.CommandLine.Set("alsologtostderr", "true")
	}
}

func main() {
	// Instruct Go to use all CPU cores.
	// TODO(spencer): this may be excessive and result in worse
	// performance. We should keep an eye on this as we move to
	// production workloads.
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)
	rand.Seed(util.NewPseudoSeed())
	log.V(1).Infof("running using %d processor cores", numCPU)

	if len(os.Args) == 1 {
		os.Args = append(os.Args, "help")
	}
	if err := cli.Run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Failed running command %q: %v\n", os.Args[1:], err)
		os.Exit(1)
	}
}
