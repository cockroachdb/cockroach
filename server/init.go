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

	commander "code.google.com/p/go-commander"
	"code.google.com/p/go-uuid/uuid"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/golang/glog"
)

// A CmdInit command initializes a new Cockroach cluster.
var CmdInit = &commander.Command{
	UsageLine: "init <bootstrap-data-dir> <default-zone-config-filename>",
	Short:     "init new Cockroach cluster",
	Long: `
Initialize a new Cockroach cluster on this node. The cluster is
started with only a single replica, whose data is stored in the
directory specified by the first argument <bootstrap-data-dir>. The
format of the bootstrap data directory is given by the specification
below. Note that only SSD and HDD devices may be specified; in-memory
devices cannot be used to initialize a cluster.

  ssd=<data-dir> | hdd=<data-dir>

The provided zone configuration (specified by second argument
<default-zone-config-filename>) is installed as the default. In the
likely event that the default zone config provides for more than a
single replica, the first range will move to increase its replication
to the correct level upon start.

To start the cluster after initialization, run "cockroach start".
`,
	Run:  runInit,
	Flag: *flag.CommandLine,
}

// runInit.
func runInit(cmd *commander.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		return
	}
	// Specifying the disk type as HDD may be incorrect, but doesn't
	// matter for this bootstrap step.
	engine, err := initEngine(args[0])
	if err != nil {
		glog.Fatal(err)
	}
	if engine.Type() == storage.MEM {
		glog.Fatal("Cannot initialize a cockroach cluster using an in-memory storage device")
	}
	// Generate a new cluster UUID.
	clusterID := uuid.New()
	if _, err := BootstrapCluster(clusterID, engine); err != nil {
		glog.Fatal(err)
	}
	// TODO(spencer): install the default zone config.
	glog.Infof("Cockroach cluster %s has been initialized", clusterID)
	glog.Infof(`To start the cluster, run "cockroach start"`)
}
