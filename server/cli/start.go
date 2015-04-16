// Copyright 2015 The Cockroach Authors.
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
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package cli

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	commander "code.google.com/p/go-commander"
	"code.google.com/p/go-uuid/uuid"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// Context is the CLI Context used for the server.
var Context = server.NewContext()

// An initCmd command initializes a new Cockroach cluster.
var initCmd = &commander.Command{
	UsageLine: "init <storage-location>",
	Short:     "init new Cockroach cluster and start server",
	Long: `
Initialize a new Cockroach cluster using the first argument as the
storage location used to bootstrap the first replica of the first
range. If the storage location is already part of a pre-existing
cluster, the bootstrap will fail.

This command must be run before starting any nodes in the cluster.
The storage location specified here must be used as a device in the
-stores flag when starting this node in order to start the cluster.

For example:

  cockroach init /mnt/ssd1
`,
	Run:  runInit,
	Flag: *flag.CommandLine,
}

// runInit initializes the engine based on the first
// store. The bootstrap engine may not be an in-memory type.
func runInit(cmd *commander.Command, args []string) {
	// Require a single argument for storage location.
	if len(args) != 1 {
		cmd.Usage()
		return
	}

	// Generate a new UUID for cluster ID and bootstrap the cluster.
	clusterID := uuid.New()
	e := engine.NewRocksDB(proto.Attributes{}, args[0], 1<<20)
	stopper := util.NewStopper()
	if _, err := server.BootstrapCluster(clusterID, e, stopper); err != nil {
		log.Errorf("unable to bootstrap cluster: %s", err)
		return
	}
	stopper.Stop()

	log.Infof("cockroach cluster %s has been initialized", clusterID)
}

// A startCmd command starts nodes by joining the gossip network.
var startCmd = &commander.Command{
	UsageLine: "start -gossip=host1:port1[,host2:port2...] " +
		"-certs=<cert-dir> " +
		"-stores=(ssd=<data-dir>,hdd:7200rpm=<data-dir>|mem=<capacity-in-bytes>)[,...]",
	Short: "start node by joining the gossip network",
	Long: `
Start Cockroach node by joining the gossip network and exporting key
ranges stored on physical device(s). The gossip network is joined by
contacting one or more well-known hosts specified by the -gossip
flag. Every node should be run with the same list of bootstrap hosts
to guarantee a connected network. An alternate approach is to use a
single host for -gossip and round-robin DNS.

Each node exports data from one or more physical devices. These
devices are specified via the -stores flag. This is a comma-separated
list of paths to storage directories or for in-memory stores, the
number of bytes. Although the paths should be specified to correspond
uniquely to physical devices, this requirement isn't strictly
enforced.

For example:

  cockroach start -gossip=host1:port1,host2:port2 -stores=ssd=/mnt/ssd1,ssd=/mnt/ssd2

A node exports an HTTP API with the following endpoints:

  Health check:           /healthz
  Key-value REST:         ` + kv.RESTPrefix + `
  Structured Schema REST: ` + structured.StructuredKeyPrefix,
	Run:  runStart,
	Flag: *flag.CommandLine,
}

// runStart starts the cockroach node using -stores as the list of
// storage devices ("stores") on this machine and -gossip as the list
// of "well-known" hosts used to join this node to the cockroach
// cluster via the gossip network.
func runStart(cmd *commander.Command, args []string) {
	info := util.GetBuildInfo()
	log.Infof("build Vers: %s", info.Vers)
	log.Infof("build Tag:  %s", info.Tag)
	log.Infof("build Time: %s", info.Time)
	log.Infof("build Deps: %s", info.Deps)

	// First initialize the Context as it is used in other places.
	err := Context.Init()
	if err != nil {
		log.Errorf("failed to initialize context: %s", err)
		return
	}

	log.Info("starting cockroach cluster")
	stopper := util.NewStopper()
	stopper.AddWorker()
	s, err := server.NewServer(Context, stopper)
	if err != nil {
		log.Errorf("failed to start Cockroach server: %s", err)
		return
	}

	err = s.Start(false)
	if err != nil {
		log.Errorf("cockroach server exited with error: %s", err)
		return
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, os.Kill)
	// TODO(spencer): move this behind a build tag.
	signal.Notify(signalCh, syscall.SIGTERM)

	// Block until one of the signals above is received.
	gracefulShutdownCh := make(chan struct{})
	select {
	case <-stopper.ShouldStop():
		stopper.SetStopped()
		close(gracefulShutdownCh)
	case <-signalCh:
		log.Infof("initiating graceful shutdown of server")
		stopper.SetStopped()
		go func() {
			s.Stop()
			close(gracefulShutdownCh)
		}()
	}

	select {
	case <-signalCh:
		log.Warningf("second signal received, initiating hard shutdown")
	case <-time.After(time.Minute):
		log.Warningf("time limit reached, initiating hard shutdown")
		return
	case <-gracefulShutdownCh:
		log.Infof("server drained and shutdown completed")
	}
}

// A exterminateCmd command shuts down the node server.
var exterminateCmd = &commander.Command{
	UsageLine: "exterminate",
	Short:     "destroy all data held by the node",
	Long: `

First shuts down the system and then destroys all data held by the
node, cycling through each store specified by the -stores flag.
`,
	Run:  runExterminate,
	Flag: *flag.CommandLine,
}

// runExterminate destroys the data held in the specified stores.
func runExterminate(cmd *commander.Command, args []string) {
	err := Context.Init()
	if err != nil {
		log.Errorf("failed to initialize context: %s", err)
		return
	}

	// First attempt to shutdown the server. Note that an error of EOF just
	// means the HTTP server shutdown before the request to quit returned.
	if err := server.SendQuit(Context); err != nil {
		log.Infof("shutdown node %s: %s", Context.Addr, err)
	} else {
		log.Infof("shutdown node in anticipation of data extermination")
	}

	// Exterminate all data held in specified stores.
	for _, e := range Context.Engines {
		if rocksdb, ok := e.(*engine.RocksDB); ok {
			log.Infof("exterminating data from store %s", e)
			if err := rocksdb.Destroy(); err != nil {
				log.Fatalf("unable to destroy store %s: %s", e, err)
			}
		}
	}
	log.Infof("exterminated all data from stores %s", Context.Engines)
}

// A quitCmd command shuts down the node server.
var quitCmd = &commander.Command{
	UsageLine: "quit",
	Short:     "drain and shutdown node\n",
	Long: `
Shutdown the server. The first stage is drain, where any new requests
will be ignored by the server. When all extant requests have been
completed, the server exits.
`,
	Run:  runQuit,
	Flag: *flag.CommandLine,
}

// runQuit accesses the quit shutdown path.
func runQuit(cmd *commander.Command, args []string) {
	server.SendQuit(Context)
}
