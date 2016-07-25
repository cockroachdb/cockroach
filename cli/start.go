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
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/build"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/server/serverpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/grpcutil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/sdnotify"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"

	"github.com/spf13/cobra"
)

var errMissingParams = errors.New("missing or invalid parameters")

// jemallocHeapDump is an optional function to be called at heap dump time.
// This will be non-nil when jemalloc is linked in with profiling enabled.
// The function takes a filename to write the profile to.
var jemallocHeapDump func(string) error

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
	if !serverCtx.Insecure || insecure.isSet {
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
		serverCtx.Addr = net.JoinHostPort("localhost", connPort)
		serverCtx.HTTPAddr = net.JoinHostPort("localhost", httpPort)
	}
	return nil
}

func initMemProfile(dir string) {
	memProfileInterval := envutil.EnvOrDefaultDuration("memprof_interval", -1)
	if memProfileInterval < 0 {
		return
	}
	if min := time.Second; memProfileInterval < min {
		log.Infof(context.TODO(), "fixing excessively short memory profiling interval: %s -> %s",
			memProfileInterval, min)
		memProfileInterval = min
	}

	if jemallocHeapDump != nil {
		log.Infof(context.TODO(), "writing go and jemalloc memory profiles to %s every %s", dir, memProfileInterval)
	} else {
		log.Infof(context.TODO(), "writing go only memory profiles to %s every %s", dir, memProfileInterval)
		log.Infof(context.TODO(), `to enable jmalloc profiling: "export MALLOC_CONF=prof:true" or "ln -s prof:true /etc/malloc.conf"`)
	}

	go func() {
		t := time.NewTicker(memProfileInterval)
		defer t.Stop()

		for {
			<-t.C

			func() {
				const format = "2006-01-02T15_04_05.999"
				suffix := timeutil.Now().Format(format)

				// Try jemalloc heap profile first, we only log errors.
				if jemallocHeapDump != nil {
					jepath := filepath.Join(dir, "jeprof."+suffix)
					if err := jemallocHeapDump(jepath); err != nil {
						log.Warningf(context.TODO(), "error writing jemalloc heap %s: %s", jepath, err)
					}
				}

				path := filepath.Join(dir, "memprof."+suffix)
				// Try writing a go heap profile.
				f, err := os.Create(path)
				if err != nil {
					log.Warningf(context.TODO(), "error creating go heap file %s", err)
					return
				}
				defer f.Close()
				if err = pprof.WriteHeapProfile(f); err != nil {
					log.Warningf(context.TODO(), "error writing go heap %s: %s", path, err)
					return
				}
			}()
		}
	}()
}

func initCPUProfile(dir string) {
	cpuProfileInterval := envutil.EnvOrDefaultDuration("cpuprof_interval", -1)
	if cpuProfileInterval < 0 {
		return
	}
	if min := time.Second; cpuProfileInterval < min {
		log.Infof(context.TODO(), "fixing excessively short cpu profiling interval: %s -> %s",
			cpuProfileInterval, min)
		cpuProfileInterval = min
	}

	go func() {
		t := time.NewTicker(cpuProfileInterval)
		defer t.Stop()

		var currentProfile *os.File
		defer func() {
			if currentProfile != nil {
				pprof.StopCPUProfile()
				currentProfile.Close()
			}
		}()

		for {
			func() {
				const format = "2006-01-02T15_04_05.999"
				suffix := timeutil.Now().Add(cpuProfileInterval).Format(format)
				f, err := os.Create(filepath.Join(dir, "cpuprof."+suffix))
				if err != nil {
					log.Warningf(context.TODO(), "error creating go cpu file %s", err)
					return
				}

				// Stop the current profile if it exists.
				if currentProfile != nil {
					pprof.StopCPUProfile()
					currentProfile.Close()
					currentProfile = nil
				}

				// Start the new profile.
				if err := pprof.StartCPUProfile(f); err != nil {
					log.Warningf(context.TODO(), "unable to start cpu profile: %v", err)
					f.Close()
					return
				}
				currentProfile = f
			}()

			<-t.C
		}
	}()
}

func initBlockProfile() {
	// Enable the block profile for a sample of mutex and channel operations.
	// Smaller values provide more accurate profiles but are more
	// expensive. 0 and 1 are special: 0 disables the block profile and
	// 1 captures 100% of block events. For other values, the profiler
	// will sample one event per X nanoseconds spent blocking.
	//
	// The block profile can be viewed with `go tool pprof
	// http://HOST:PORT/debug/pprof/block`
	d := envutil.EnvOrDefaultInt64("block_profile_rate",
		1000000 /* 1 sample per millisecond spent blocking */)
	runtime.SetBlockProfileRate(int(d))
}

func initCheckpointing(engines []engine.Engine) {
	checkpointInterval := envutil.EnvOrDefaultDuration("checkpoint_interval", -1)
	if checkpointInterval < 0 {
		return
	}
	if min := 10 * time.Second; checkpointInterval < min {
		log.Infof(context.TODO(), "fixing excessively short checkpointing interval: %s -> %s",
			checkpointInterval, min)
		checkpointInterval = min
	}

	go func() {
		t := time.NewTicker(checkpointInterval)
		defer t.Stop()

		for {
			<-t.C

			const format = "2006-01-02T15_04_05"
			dir := timeutil.Now().Format(format)
			start := timeutil.Now()
			for _, e := range engines {
				// Note that when dir is relative (as it is here) it is appended to the
				// engine's data directory.
				if err := e.Checkpoint(dir); err != nil {
					log.Warning(context.TODO(), err)
				}
			}
			log.Infof(context.TODO(), "created checkpoint %s: %.1fms", dir, timeutil.Since(start).Seconds()*1000)
		}
	}()
}

// runStart starts the cockroach node using --store as the list of
// storage devices ("stores") on this machine and --join as the list
// of other active nodes used to join this node to the cockroach
// cluster, if this is its first time connecting.
func runStart(_ *cobra.Command, args []string) error {
	if startBackground {
		return rerunBackground()
	}

	if err := initInsecure(); err != nil {
		return err
	}

	// Default the log directory to the "logs" subdirectory of the first
	// non-memory store. We only do this for the "start" command which is why
	// this work occurs here and not in an OnInitialize function.
	//
	// It is important that no logging occur before this point or the log files
	// will be created in $TMPDIR instead of their expected location.
	f := flag.Lookup("log-dir")
	if !log.DirSet() {
		for _, spec := range serverCtx.Stores.Specs {
			if spec.InMemory {
				continue
			}
			if err := f.Value.Set(filepath.Join(spec.Path, "logs")); err != nil {
				return err
			}
			break
		}
	}

	// Make sure the path exists.
	logDir := f.Value.String()
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	// We log build information to stdout (for the short summary), but also
	// to stderr to coincide with the full logs.
	info := build.GetInfo()
	log.Infof(context.TODO(), info.Short())

	initMemProfile(f.Value.String())
	initCPUProfile(f.Value.String())
	initBlockProfile()

	// Default user for servers.
	serverCtx.User = security.NodeUser

	stopper := initBacktrace(logDir)
	if err := serverCtx.InitStores(stopper); err != nil {
		return fmt.Errorf("failed to initialize stores: %s", err)
	}

	if err := serverCtx.InitNode(); err != nil {
		return fmt.Errorf("failed to initialize node: %s", err)
	}

	log.Info(context.TODO(), "starting cockroach node")
	s, err := server.NewServer(serverCtx, stopper)
	if err != nil {
		return fmt.Errorf("failed to start Cockroach server: %s", err)
	}

	if err := s.Start(); err != nil {
		return fmt.Errorf("cockroach server exited with error: %s", err)
	}

	// We don't do this in (*server.Server).Start() because we don't want it
	// in tests.
	if !envutil.EnvOrDefaultBool("skip_update_check", false) {
		s.PeriodicallyCheckForUpdates()
	}

	initCheckpointing(serverCtx.Engines)

	pgURL, err := serverCtx.PGURL(connUser)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(os.Stdout, 2, 1, 2, ' ', 0)
	fmt.Fprintf(tw, "build:\t%s @ %s (%s)\n", info.Tag, info.Time, info.GoVersion)
	fmt.Fprintf(tw, "admin:\t%s\n", serverCtx.AdminURL())
	fmt.Fprintf(tw, "sql:\t%s\n", pgURL)
	if len(serverCtx.SocketFile) != 0 {
		fmt.Fprintf(tw, "socket:\t%s\n", serverCtx.SocketFile)
	}
	fmt.Fprintf(tw, "logs:\t%s\n", flag.Lookup("log-dir").Value)
	for i, spec := range serverCtx.Stores.Specs {
		fmt.Fprintf(tw, "store[%d]:\t%s\n", i, spec)
	}
	for i, address := range serverCtx.JoinList {
		fmt.Fprintf(tw, "join[%d]:\t%s\n", i, address)
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
		go func() {
			if _, err := s.Drain(server.GracefulDrainModes); err != nil {
				log.Warning(context.TODO(), err)
			}
			s.Stop()
		}()
	}

	const msgDrain = "initiating graceful shutdown of server"
	log.Info(context.TODO(), msgDrain)
	fmt.Fprintln(os.Stdout, msgDrain)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if log.V(1) {
					log.Infof(context.TODO(), "running tasks:\n%s", stopper.RunningTasks())
				}
				log.Infof(context.TODO(), "%d running tasks", stopper.NumTasks())
			case <-stopper.ShouldStop():
				return
			}
		}
	}()

	select {
	case <-signalCh:
		log.Errorf(context.TODO(), "second signal received, initiating hard shutdown")
	case <-time.After(time.Minute):
		log.Errorf(context.TODO(), "time limit reached, initiating hard shutdown")
	case <-stopper.IsStopped():
		const msgDone = "server drained and shutdown completed"
		log.Infof(context.TODO(), msgDone)
		fmt.Fprintln(os.Stdout, msgDone)
	}
	log.Flush()
	return nil
}

// rerunBackground restarts the current process in the background.
func rerunBackground() error {
	args := make([]string, 0, len(os.Args))
	foundBackground := false
	for _, arg := range os.Args {
		if arg == "--background" || strings.HasPrefix(arg, "--background=") {
			foundBackground = true
			continue
		}
		args = append(args, arg)
	}
	if !foundBackground {
		args = append(args, "--background=false")
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return sdnotify.Exec(cmd)
}

func getGRPCConn() (*grpc.ClientConn, *stop.Stopper, error) {
	stopper := stop.NewStopper()
	rpcContext := rpc.NewContext(serverCtx.Context, hlc.NewClock(hlc.UnixNano),
		stopper)
	conn, err := rpcContext.GRPCDial(serverCtx.Addr)
	if err != nil {
		return nil, nil, err
	}
	return conn, stopper, nil
}

func getAdminClient() (serverpb.AdminClient, *stop.Stopper, error) {
	conn, stopper, err := getGRPCConn()
	if err != nil {
		return nil, nil, err
	}
	return serverpb.NewAdminClient(conn), stopper, nil
}

func stopperContext(stopper *stop.Stopper) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	stopper.AddCloser(stop.CloserFn(cancel))
	return ctx
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

// doShutdown attempts to trigger server shutdown. The string return
// value, if non-empty, indicates a reason why the state may be
// uncertain and thus that it may make sense to retry the shutdown.
func doShutdown(c serverpb.AdminClient, ctx context.Context, onModes []int32) (string, error) {
	stream, err := c.Drain(ctx, &serverpb.DrainRequest{
		On:       onModes,
		Shutdown: true,
	})
	if err != nil {
		return "", err
	}
	_, err = stream.Recv()
	if err == nil {
		for {
			if _, err := stream.Recv(); !grpcutil.IsClosedConnection(err) {
				return "", err
			}
			break
		}
		fmt.Println("ok")
		return "", nil
	}
	return fmt.Sprintf("%v", err), nil
}

// runQuit accesses the quit shutdown path.
func runQuit(_ *cobra.Command, _ []string) error {
	onModes := make([]int32, len(server.GracefulDrainModes))
	for i, m := range server.GracefulDrainModes {
		onModes[i] = int32(m)
	}

	c, stopper, err := getAdminClient()
	if err != nil {
		return err
	}
	defer stopper.Stop()

	ctx := stopperContext(stopper)

	retry, err := doShutdown(c, ctx, onModes)
	if err != nil {
		return err
	}
	if retry != "" {
		fmt.Fprintf(os.Stderr, "graceful shutdown failed, proceeding with hard shutdown: %s\n", retry)

		// Not passing drain modes tells the server to not bother and go
		// straight to shutdown.
		_, err := doShutdown(c, ctx, nil)
		if err != nil {
			return fmt.Errorf("hard shutdown failed: %v", err)
		}
	}
	return nil
}

// freezeClusterCmd command issues a cluster-wide freeze.
var freezeClusterCmd = &cobra.Command{
	Use:   "freeze-cluster",
	Short: "freeze the cluster in preparation for an update",
	Long: `
Disables all Raft groups and stops new commands from being executed in preparation
for a stop-the-world update of the cluster. Once the command has completed, the
nodes in the cluster should be terminated, all binaries updated, and only then
restarted. A failed or incomplete invocation of this command can be rolled back
using the --undo flag, or by restarting all the nodes in the cluster.
`,
	SilenceUsage: true,
	RunE:         runFreezeCluster,
}

func runFreezeCluster(_ *cobra.Command, _ []string) error {
	c, stopper, err := getAdminClient()
	if err != nil {
		return err
	}
	defer stopper.Stop()
	stream, err := c.ClusterFreeze(stopperContext(stopper),
		&serverpb.ClusterFreezeRequest{Freeze: !undoFreezeCluster})
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		fmt.Println(resp.Message)
	}
	fmt.Println("ok")
	return nil
}
