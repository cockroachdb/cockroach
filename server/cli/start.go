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
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// Context is the CLI Context used for the server.
var Context = server.NewContext()

var cmdStartLongDescription = `

Start Cockroach node by joining the gossip network and exporting key
ranges stored on physical device(s). The gossip network is joined by
contacting one or more well-known hosts specified by the -gossip
command line flag. Every node should be run with the same list of
bootstrap hosts to guarantee a connected network. An alternate
approach is to use a single host for -gossip and round-robin DNS.

Each node exports data from one or more physical devices. These
devices are specified via the -stores command line flag. This is a
comma-separated list of paths to storage directories or for in-memory
stores, the number of bytes. Although the paths should be specified to
correspond uniquely to physical devices, this requirement isn't
strictly enforced.

A node exports an HTTP API with the following endpoints:

  Health check:           /healthz
  Key-value REST:         ` + kv.RESTPrefix + `
  Structured Schema REST: ` + structured.StructuredKeyPrefix

// An initCmd command initializes a new Cockroach cluster.
var initCmd = &commander.Command{
	UsageLine: "init -gossip=host1:port1[,host2:port2...] " +
		"-certs=<cert-dir> " +
		"-stores=(ssd=<data-dir>,hdd:7200rpm=<data-dir>,mem=<capacity-in-bytes>)[,...]",
	Short: "init new Cockroach cluster and start server",
	Long: `
Initialize a new Cockroach cluster on this node using the first
directory specified in the -stores command line flag as the only
replica of the first range.

For example:

  cockroach init -gossip=host1:port1,host2:port2 -stores=ssd=/mnt/ssd1,ssd=/mnt/ssd2

If any specified store is already part of a pre-existing cluster, the
bootstrap will fail.

After bootstrap initialization: ` + cmdStartLongDescription,
	Run:  runInit,
	Flag: *flag.CommandLine,
}

func runInit(cmd *commander.Command, args []string) {
	// Initialize the engine based on the first argument and
	// then verify it's not in-memory.

	err := Context.Init()
	if err != nil {
		log.Errorf("failed to initialize context: %v", err)
		return
	}
	e := Context.Engines[0]
	if _, ok := e.(*engine.InMem); ok {
		log.Errorf("cannot initialize a cluster using an in-memory store")
		return
	}
	// Generate a new UUID for cluster ID and bootstrap the cluster.
	clusterID := uuid.New()
	stopper := util.NewStopper()
	if _, err = server.BootstrapCluster(clusterID, e, stopper); err != nil {
		log.Errorf("failed to bootstrap cluster: %v", err)
		return
	}
	stopper.Stop()

	log.Infof("cockroach cluster %s has been initialized\n", clusterID)
	if Context.InitAndStart {
		runStart(cmd, args)
		return
	}
	log.Infof("to start the cluster, run \"cockroach start\"\n")
}

// A startCmd command starts nodes by joining the gossip network.
var startCmd = &commander.Command{
	UsageLine: "start -gossip=host1:port1[,host2:port2...] " +
		"-certs=<cert-dir> " +
		"-stores=(ssd=<data-dir>,hdd:7200rpm=<data-dir>|mem=<capacity-in-bytes>)[,...]",
	Short: "start node by joining the gossip network",
	Long:  cmdStartLongDescription,
	Run:   runStart,
	Flag:  *flag.CommandLine,
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
		log.Errorf("failed to initialize context: %v", err)
		return
	}

	log.Info("starting cockroach cluster")
	stopper := util.NewStopper()
	stopper.AddWorker()
	s, err := server.NewServer(Context, stopper)
	if err != nil {
		log.Errorf("failed to start Cockroach server: %v", err)
		return
	}

	isBootstrap := cmd.Name() == "init"
	err = s.Start(isBootstrap)
	if err != nil {
		log.Errorf("cockroach server exited with error: %v", err)
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

// A quitCmd command shuts down the node server.
var quitCmd = &commander.Command{
	UsageLine: "quit",
	Short: "shutdown node by entering it into draining mode first, followed " +
		"by exit\n",
	Long: `
Shutdown the server. The first stage is drain, where any new requests
will be ignored by the server. When all extant requests have been
completed, the server exits.
`,
	Run:  runQuit,
	Flag: *flag.CommandLine,
}

// runQuit accesses the quitquitquit shutdown path.
func runQuit(cmd *commander.Command, args []string) {
	server.RunQuit(Context)
}
