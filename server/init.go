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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"flag"
	"fmt"
	"os"

	commander "code.google.com/p/go-commander"
	"code.google.com/p/go-uuid/uuid"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/golang/glog"
)

// A CmdInit command initializes a new Cockroach cluster.
var CmdInit = &commander.Command{
	UsageLine: "init <bootstrap store>",
	Short:     "init new Cockroach cluster",
	Long: `
Initialize a new Cockroach cluster on this node. The cluster is
started with only a single replica, whose data is stored in the
directory specified by the first argument <bootstrap store>. The
format of the bootstrap store is given by the specification
below. Note that attributes should not specify in-memory ("mem").

  <comma-separated store attributes>=<data dir path>

For example:

  cockroach init hdd,7200rpm=/mnt/hda1

To start the cluster after initialization, run "cockroach start".
`,
	Run:  runInit,
	Flag: *flag.CommandLine,
}

// runInit.
func runInit(cmd *commander.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	// Initialize the engine based on the first argument and
	// then verify it's not in-memory.
	engines, err := initEngines(args[0])
	if err != nil {
		glog.Errorf("Failed to initialize engine %q: %v", args[0], err)
		return
	}
	e := engines[0]
	if _, ok := e.(*engine.InMem); ok {
		glog.Errorf("Cannot initialize a cluster using an in-memory store")
		return
	}
	// Generate a new UUID for cluster ID and bootstrap the cluster.
	clusterID := uuid.New()
	localDB, err := BootstrapCluster(clusterID, e)
	if err != nil {
		glog.Errorf("Failed to bootstrap cluster: %v", err)
		return
	}
	defer localDB.Close()
	fmt.Fprintf(os.Stdout, "Cockroach cluster %s has been initialized\n", clusterID)
	fmt.Fprintf(os.Stdout, "To start the cluster, run \"cockroach start\"\n")
}
