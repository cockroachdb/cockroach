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
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"

	"github.com/spf13/cobra"
)

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
	httpClient, err := cliContext.GetHTTPClient()
	if err != nil {
		return err
	}
	return util.GetJSON(httpClient, cliContext.HTTPRequestScheme(), hostport, path, v)
}

// startCmd starts a node by initializing the stores and joining
// the cluster.
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start a node",
	Long: `
Start a CockroachDB node, which will export data from one or more
storage devices, specified via --store flags.

If no cluster exists yet and this is the first node, no additional
flags are required. If the cluster already exists, and this node is
uninitialized, specify the --join flag to point to any healthy node
(or list of nodes) already part of the cluster.
`,
	Example:      `  cockroach start --insecure --store=attrs=ssd,path=/mnt/ssd1 [--join=host:port,[host:port]]`,
	SilenceUsage: true,
	RunE:         runStart,
}

func setDefaultCacheSize(ctx *server.Context) {
	if size, err := server.GetTotalMemory(); err == nil {
		// Default the cache size to 1/4 of total memory. A larger cache size
		// doesn't necessarily improve performance as this is memory that is
		// dedicated to uncompressed blocks in RocksDB. A larger value here will
		// compete with the OS buffer cache which holds compressed blocks.
		ctx.CacheSize = size / 4
	}
}

func initInsecure() error {
	if !cliContext.Insecure || insecure.isSet {
		return nil
	}
	// The --insecure flag was not specified on the command line, verify that the
	// host refers to a loopback address.
	if connHost != "" {
		addr, err := net.ResolveIPAddr("ip", connHost)
		if err != nil {
			return err
		}
		if !addr.IP.IsLoopback() {
			return fmt.Errorf("specify --insecure to listen on external address %s", connHost)
		}
	} else {
		cliContext.Addr = net.JoinHostPort("localhost", connPort)
		cliContext.HTTPAddr = net.JoinHostPort("localhost", httpPort)
	}
	return nil
}

// runStart starts the cockroach node using --store as the list of
// storage devices ("stores") on this machine and --join as the list
// of other active nodes used to join this node to the cockroach
// cluster, if this is its first time connecting.
func runStart(_ *cobra.Command, _ []string) error {
	if err := initInsecure(); err != nil {
		return err
	}

	// Default the log directory to the the "logs" subdirectory of the first
	// non-memory store. We only do this for the "start" command which is why
	// this work occurs here and not in an OnInitialize function.
	f := flag.Lookup("log-dir")
	if !log.DirSet() {
		for _, spec := range cliContext.Stores.Specs {
			if spec.InMemory {
				continue
			}
			if err := f.Value.Set(filepath.Join(spec.Path, "logs")); err != nil {
				return err
			}
			break
		}
	}

	// Make sure the path exists
	if err := os.MkdirAll(f.Value.String(), 0755); err != nil {
		return err
	}

	info := util.GetBuildInfo()
	log.Infof("[build] %s @ %s (%s)", info.Tag, info.Time, info.GoVersion)

	// Default user for servers.
	cliContext.User = security.NodeUser

	stopper := stop.NewStopper()
	if err := cliContext.InitStores(stopper); err != nil {
		return fmt.Errorf("failed to initialize stores: %s", err)
	}

	if err := cliContext.InitNode(); err != nil {
		return fmt.Errorf("failed to initialize node: %s", err)
	}

	log.Info("starting cockroach node")
	s, err := server.NewServer(&cliContext.Context, stopper)
	if err != nil {
		return fmt.Errorf("failed to start Cockroach server: %s", err)
	}

	// We don't do this in NewServer since we don't want it in tests.
	if err := s.SetupReportingURLs(); err != nil {
		return err
	}

	if err := s.Start(); err != nil {
		return fmt.Errorf("cockroach server exited with error: %s", err)
	}

	pgURL, err := cliContext.PGURL(connUser)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(os.Stdout, 2, 1, 2, ' ', 0)
	fmt.Fprintf(tw, "build:\t%s @ %s (%s)\n", info.Tag, info.Time, info.GoVersion)
	fmt.Fprintf(tw, "admin:\t%s\n", cliContext.AdminURL())
	fmt.Fprintf(tw, "sql:\t%s\n", pgURL)
	if len(cliContext.SocketFile) != 0 {
		fmt.Fprintf(tw, "socket:\t%s\n", cliContext.SocketFile)
	}
	fmt.Fprintf(tw, "logs:\t%s\n", flag.Lookup("log-dir").Value)
	for i, spec := range cliContext.Stores.Specs {
		fmt.Fprintf(tw, "store[%d]:\t%s\n", i, spec)
	}
	if err := tw.Flush(); err != nil {
		return err
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

	const msgDrain = "initiating graceful shutdown of server"
	log.Info(msgDrain)
	fmt.Fprintln(os.Stdout, msgDrain)

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
		log.Errorf("second signal received, initiating hard shutdown")
	case <-time.After(time.Minute):
		log.Errorf("time limit reached, initiating hard shutdown")
	case <-stopper.IsStopped():
		const msgDone = "server drained and shutdown completed"
		log.Infof(msgDone)
		fmt.Fprintln(os.Stdout, msgDone)
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
node, cycling through each store specified by --store flags.
`,
	SilenceUsage: true,
	RunE:         runExterminate,
}

// runExterminate destroys the data held in the specified stores.
func runExterminate(_ *cobra.Command, _ []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop()
	if err := cliContext.InitStores(stopper); err != nil {
		return util.Errorf("failed to initialize context: %s", err)
	}

	if err := runQuit(nil, nil); err != nil {
		return util.Errorf("shutdown node error: %s", err)
	}

	// Exterminate all data held in specified stores.
	for _, e := range cliContext.Engines {
		if rocksdb, ok := e.(*engine.RocksDB); ok {
			log.Infof("exterminating data from store %s", e)
			if err := rocksdb.Destroy(); err != nil {
				return util.Errorf("unable to destroy store %s: %s", e, err)
			}
		}
	}
	log.Infof("exterminated all data from stores %s", cliContext.Engines)
	return nil
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
	RunE:         runQuit,
}

// runQuit accesses the quit shutdown path.
func runQuit(_ *cobra.Command, _ []string) error {
	admin, err := client.NewAdminClient(&cliContext.Context.Context, cliContext.HTTPAddr, client.Quit)
	if err != nil {
		return err
	}
	body, err := admin.Get()
	// TODO(tschottdorf): needs cleanup. An error here can happen if the shutdown
	// happened faster than the HTTP request made it back.
	if err != nil {
		return err
	}
	fmt.Printf("node drained and shutdown: %s\n", body)
	return nil
}
