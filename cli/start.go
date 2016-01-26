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
// permissions and limitations under the License.
//
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package cli

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/uuid"

	"github.com/spf13/cobra"
)

// Context is the CLI Context used for the command-line client.
var context = NewContext()

var errMissingParams = errors.New("missing or invalid parameters")

// panicGuard wraps an errorless command into one wrapping panics into errors.
// This simplifies error handling for many commands for which more elaborate
// error handling isn't needed and would otherwise bloat the code.
//
// Deprecated: When introducing a new cobra.Command, simply return an error.
func panicGuard(cmdFn func(*cobra.Command, []string)) func(*cobra.Command, []string) error {
	return func(c *cobra.Command, args []string) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
			}
		}()
		cmdFn(c, args)
		return nil
	}
}

// panicf is only to be used when wrapped through panicGuard, since the
// stack trace doesn't matter then.
func panicf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// getJSON is a convenience wrapper around util.GetJSON that uses our Context to populate
// parts of the request.
func getJSON(hostport, path string, v interface{}) error {
	httpClient, err := context.GetHTTPClient()
	if err != nil {
		return err
	}
	return util.GetJSON(httpClient, context.HTTPRequestScheme(), hostport, path, v)
}

// initCmd command initializes a new Cockroach cluster.
var initCmd = &cobra.Command{
	Use:   "init --stores=...",
	Short: "init new Cockroach cluster",
	Long: `
Initialize a new Cockroach cluster using the --stores flag to specify one or
more storage locations. The first of these storage locations is used to
bootstrap the first replica of the first range. If any of the storage locations
are already part of a pre-existing cluster, the bootstrap will fail.
`,
	Example:      `  cockroach init --stores=ssd=/mnt/ssd1,ssd=/mnt/ssd2`,
	SilenceUsage: true,
	RunE:         runInit,
}

// runInit initializes the engine based on the first
// store. The bootstrap engine may not be an in-memory type.
func runInit(_ *cobra.Command, _ []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	// Default user for servers.
	context.User = security.NodeUser

	if err := context.InitStores(stopper); err != nil {
		return util.Errorf("failed to initialize stores: %s", err)
	}
	if len(context.Engines) > 1 {
		return util.Errorf("cannot bootstrap to more than one store")
	}

	return initCluster(stopper)
}

func initCluster(stopper *stop.Stopper) error {
	// Generate a new UUID for cluster ID and bootstrap the cluster.
	clusterID := uuid.NewUUID4().String()
	if _, err := server.BootstrapCluster(clusterID, context.Engines, stopper); err != nil {
		return util.Errorf("unable to bootstrap cluster: %s", err)
	}

	log.Infof("cockroach cluster %s has been initialized", clusterID)
	return nil
}

// startCmd command starts nodes by joining the gossip network.
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start a node by joining the gossip network",
	Long: `
Start a Cockroach node by joining the gossip network and exporting key ranges
stored on physical device(s). The gossip network is joined by contacting one or
more well-known hosts specified by the --gossip flag. Every node should be run
with the same list of bootstrap hosts to guarantee a connected network. An
alternate approach is to use a single host for --gossip and round-robin DNS.

Each node exports data from one or more physical devices. These devices are
specified via the --stores flag. This is a comma-separated list of paths to
storage directories or for in-memory stores, the number of bytes. Although the
paths should be specified to correspond uniquely to physical devices, this
requirement isn't strictly enforced. See the --stores flag help description for
additional details.`,
	Example:      `  cockroach start --certs=<dir> --gossip=host1:port1[,...] --stores=ssd=/mnt/ssd1,...`,
	SilenceUsage: true,
	RunE:         runStart,
}

// runStart starts the cockroach node using --stores as the list of
// storage devices ("stores") on this machine and --gossip as the list
// of "well-known" hosts used to join this node to the cockroach
// cluster via the gossip network.
func runStart(_ *cobra.Command, _ []string) error {
	info := util.GetBuildInfo()
	log.Infof("[build] %s @ %s (%s)", info.Tag, info.Time, info.Vers)

	// Default user for servers.
	context.User = security.NodeUser

	if context.EphemeralSingleNode {
		// TODO(marc): set this in the zones table when we have an entry
		// for the default cluster-wide zone config.
		config.DefaultZoneConfig.ReplicaAttrs = []roachpb.Attributes{{}}
		context.Stores = "mem=1073741824"
		context.GossipBootstrap = server.SelfGossipAddr
	}

	stopper := stop.NewStopper()
	if err := context.InitStores(stopper); err != nil {
		return util.Errorf("failed to initialize stores: %s", err)
	}

	if context.EphemeralSingleNode {
		// A separate stopper for bootstrapping so that we can properly shutdown
		// all of the stores.
		initStopper := stop.NewStopper()
		if err := initCluster(initStopper); err != nil {
			return err
		}
		initStopper.Stop()
	}

	if err := context.InitNode(); err != nil {
		return util.Errorf("failed to initialize node: %s", err)
	}

	log.Info("starting cockroach node")
	s, err := server.NewServer(&context.Context, stopper)
	if err != nil {
		return util.Errorf("failed to start Cockroach server: %s", err)
	}

	if err := s.Start(false); err != nil {
		return util.Errorf("cockroach server exited with error: %s", err)
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, os.Kill)
	// TODO(spencer): move this behind a build tag.
	signal.Notify(signalCh, syscall.SIGTERM)

	// Block until one of the signals above is received or the stopper
	// is stopped externally (for example, via the quit endpoint).
	select {
	case <-stopper.ShouldStop():
	case <-signalCh:
		go s.Stop()
	}

	log.Info("initiating graceful shutdown of server")

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if log.V(1) {
					log.Infof("running tasks:\n%s", stopper.RunningTasks())
				}
				log.Infof("%d running tasks", stopper.NumTasks())
			case <-stopper.ShouldStop():
				return
			}
		}
	}()

	select {
	case <-signalCh:
		log.Warningf("second signal received, initiating hard shutdown")
	case <-time.After(time.Minute):
		log.Warningf("time limit reached, initiating hard shutdown")
	case <-stopper.IsStopped():
		log.Infof("server drained and shutdown completed")
	}
	log.Flush()
	return nil
}

// exterminateCmd command shuts down the node server and
// destroys all data held by the node.
var exterminateCmd = &cobra.Command{
	Use:   "exterminate",
	Short: "destroy all data held by the node",
	Long: `
First shuts down the system and then destroys all data held by the
node, cycling through each store specified by the --stores flag.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runExterminate),
}

// runExterminate destroys the data held in the specified stores.
func runExterminate(_ *cobra.Command, _ []string) {
	stopper := stop.NewStopper()
	defer stopper.Stop()
	if err := context.InitStores(stopper); err != nil {
		panicf("failed to initialize context: %s", err)
	}

	runQuit(nil, nil)

	// Exterminate all data held in specified stores.
	for _, e := range context.Engines {
		if rocksdb, ok := e.(*engine.RocksDB); ok {
			log.Infof("exterminating data from store %s", e)
			if err := rocksdb.Destroy(); err != nil {
				panicf("unable to destroy store %s: %s", e, err)
			}
		}
	}
	log.Infof("exterminated all data from stores %s", context.Engines)
}

// quitCmd command shuts down the node server.
var quitCmd = &cobra.Command{
	Use:   "quit",
	Short: "drain and shutdown node\n",
	Long: `
Shutdown the server. The first stage is drain, where any new requests
will be ignored by the server. When all extant requests have been
completed, the server exits.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runQuit),
}

// runQuit accesses the quit shutdown path.
func runQuit(_ *cobra.Command, _ []string) {
	admin := client.NewAdminClient(&context.Context.Context, context.Addr, client.Quit)
	body, err := admin.Get()
	// TODO(tschottdorf): needs cleanup. An error here can happen if the shutdown
	// happened faster than the HTTP request made it back.
	if err != nil {
		panicf("shutdown node error: %s", err)
	}
	fmt.Printf("node drained and shutdown: %s\n", body)
}
