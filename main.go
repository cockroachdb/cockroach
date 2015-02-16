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
	"math/rand"
	"os"
	"runtime"

	commander "code.google.com/p/go-commander"
	"github.com/cockroachdb/cockroach/server"
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

	c := commander.Commander{
		Name: "cockroach",
		Commands: []*commander.Command{
			server.CmdInit,
			server.CmdGetZone,
			server.CmdLsZones,
			server.CmdRmZone,
			server.CmdSetZone,
			server.CmdStart,
			{
				UsageLine: "listparams",
				Short:     "list all available parameters and their default values",
				Long: `
List all available parameters and their default values.
Note that parameter parsing stops after the first non-
option after the command name. Hence, the options need
to precede any additional arguments,

  cockroach <command> [options] [arguments].`,
				Run: func(cmd *commander.Command, args []string) {
					flag.CommandLine.PrintDefaults()
				},
			},
		},
	}

	if len(os.Args) == 1 {
		os.Args = append(os.Args, "help")
	}
	if err := c.Run(os.Args[1:]); err != nil {
		log.Fatalf("Failed running command %q: %v\n", os.Args[1:], err)
	}
}
