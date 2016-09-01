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

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sdnotify"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var errMissingParams = errors.New("missing or invalid parameters")

// jemallocHeapDump is an optional function to be called at heap dump time.
// This will be non-nil when jemalloc is linked in with profiling enabled.
// The function takes a filename to write the profile to.
var jemallocHeapDump func(string) error

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
	RunE:         maybeDecorateGRPCError(runStart),
}

func setDefaultSizeParameters(ctx *server.Config) {
	if size, err := server.GetTotalMemory(); err == nil {
		// Default the cache size to 1/4 of total memory. A larger cache size
		// doesn't necessarily improve performance as this is memory that is
		// dedicated to uncompressed blocks in RocksDB. A larger value here will
		// compete with the OS buffer cache which holds compressed blocks.
		ctx.CacheSize = size / 4

		// Default the SQL memory pool size to 1/4 of total memory. Again
		// we do not want to allow too much lest this will pressure
		// against OS buffers and decrease overall client throughput.
		ctx.SQLMemoryPoolSize = size / 4
	}
}

func initInsecure() error {
	if !serverCfg.Insecure || insecure.isSet {
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
		serverCfg.Addr = net.JoinHostPort("localhost", connPort)
		serverCfg.AdvertiseAddr = serverCfg.Addr
		serverCfg.HTTPAddr = net.JoinHostPort("localhost", httpPort)
	}
	return nil
}

func initMemProfile(dir string) {
	memProfileInterval := envutil.EnvOrDefaultDuration("COCKROACH_MEMPROF_INTERVAL", -1)
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
	cpuProfileInterval := envutil.EnvOrDefaultDuration("COCKROACH_CPUPROF_INTERVAL", -1)
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
	d := envutil.EnvOrDefaultInt64("COCKROACH_BLOCK_PROFILE_RATE",
		10000000 /* 1 sample per 10 milliseconds spent blocking */)
	runtime.SetBlockProfileRate(int(d))
}

// ErrorCode is the value to be used by main() as exit code in case of
// error. For most errors 1 is appropriate, but a signal termination
// can change this.
var ErrorCode = 1

// runStart starts the cockroach node using --store as the list of
// storage devices ("stores") on this machine and --join as the list
// of other active nodes used to join this node to the cockroach
// cluster, if this is its first time connecting.
func runStart(_ *cobra.Command, args []string) error {
	if startBackground {
		return rerunBackground()
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	// TODO(spencer): move this behind a build tag.
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGQUIT)

	tracer := tracing.NewTracer()
	startCtx := tracing.WithTracer(context.Background(), tracer)
	sp := tracer.StartSpan("server start")
	startCtx = opentracing.ContextWithSpan(startCtx, sp)

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
		for _, spec := range serverCfg.Stores.Specs {
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
	log.Eventf(startCtx, "created log directory %s", logDir)

	// We log build information to stdout (for the short summary), but also
	// to stderr to coincide with the full logs.
	info := build.GetInfo()
	log.Infof(startCtx, info.Short())

	initMemProfile(f.Value.String())
	initCPUProfile(f.Value.String())
	initBlockProfile()

	// Default user for servers.
	serverCfg.User = security.NodeUser

	stopper := initBacktrace(logDir)
	log.Event(startCtx, "initialized profiles")

	if err := serverCfg.InitNode(); err != nil {
		return fmt.Errorf("failed to initialize node: %s", err)
	}

	log.Info(startCtx, "starting cockroach node")
	if envVarsUsed := envutil.GetEnvVarsUsed(); len(envVarsUsed) > 0 {
		log.Infof(startCtx, "using local environment variables: %s", strings.Join(envVarsUsed, ", "))
	}
	s, err := server.NewServer(serverCfg, stopper)
	if err != nil {
		return fmt.Errorf("failed to start Cockroach server: %s", err)
	}

	if err := s.Start(startCtx); err != nil {
		return fmt.Errorf("cockroach server exited with error: %s", err)
	}
	sp.Finish()

	// We don't do this in (*server.Server).Start() because we don't want it
	// in tests.
	if !envutil.EnvOrDefaultBool("COCKROACH_SKIP_UPDATE_CHECK", false) {
		s.PeriodicallyCheckForUpdates()
	}

	pgURL, err := serverCfg.PGURL(connUser)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(os.Stdout, 2, 1, 2, ' ', 0)
	fmt.Fprintf(tw, "build:\t%s @ %s (%s)\n", info.Tag, info.Time, info.GoVersion)
	fmt.Fprintf(tw, "admin:\t%s\n", serverCfg.AdminURL())
	fmt.Fprintf(tw, "sql:\t%s\n", pgURL)
	if len(serverCfg.SocketFile) != 0 {
		fmt.Fprintf(tw, "socket:\t%s\n", serverCfg.SocketFile)
	}
	fmt.Fprintf(tw, "logs:\t%s\n", flag.Lookup("log-dir").Value)
	for i, spec := range serverCfg.Stores.Specs {
		fmt.Fprintf(tw, "store[%d]:\t%s\n", i, spec)
	}
	initialBoot := s.InitialBoot()
	nodeID := s.NodeID()
	if initialBoot {
		if nodeID == server.FirstNodeID {
			fmt.Fprintf(tw, "status:\tinitialized new cluster\n")
		} else {
			fmt.Fprintf(tw, "status:\tinitialized new node, joined pre-existing cluster\n")
		}
	} else {
		fmt.Fprintf(tw, "status:\trestarted pre-existing node\n")
	}
	fmt.Fprintf(tw, "clusterID:\t%s\n", s.ClusterID())
	fmt.Fprintf(tw, "nodeID:\t%d\n", nodeID)
	if err := tw.Flush(); err != nil {
		return err
	}

	var returnErr error

	// Block until one of the signals above is received or the stopper
	// is stopped externally (for example, via the quit endpoint).
	select {
	case <-stopper.ShouldStop():
	case sig := <-signalCh:
		log.Infof(context.TODO(), "received signal '%s'", sig)
		if sig == os.Interrupt {
			// Graceful shutdown after an interrupt should cause the process
			// to terminate with a non-zero exit code; however SIGTERM is
			// "legitimate" and should be acknowledged with a success exit
			// code. So we keep the error state here for later.
			returnErr = errors.New("interrupted")
			msgDouble := "Note: a second interrupt will skip graceful shutdown and terminate forcefully"
			fmt.Fprintln(os.Stdout, msgDouble)
		}
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
	case sig := <-signalCh:
		returnErr = fmt.Errorf("received signal '%s' during shutdown, initiating hard shutdown", sig)
		log.Errorf(context.TODO(), "%v", err)
		// This new signal is not welcome, as it interferes with the
		// graceful shutdown process.  On Unix, a signal that was not
		// handled gracefully by the application should be visible to
		// other processes as an exit code encoded as 128+signal number.

		// Also, on Unix, os.Signal is syscall.Signal and it's convertible to int.
		ErrorCode = 128 + int(sig.(syscall.Signal))
		// NB: we do not return here to go through log.Flush below.
	case <-time.After(time.Minute):
		returnErr = errors.New("time limit reached, initiating hard shutdown")
		log.Errorf(context.TODO(), "%v", err)
		// NB: we do not return here to go through log.Flush below.
	case <-stopper.IsStopped():
		const msgDone = "server drained and shutdown completed"
		log.Infof(context.TODO(), msgDone)
		fmt.Fprintln(os.Stdout, msgDone)
	}
	log.Flush()

	return returnErr
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
	rpcContext := rpc.NewContext(
		log.AmbientContext{}, serverCfg.Config, hlc.NewClock(hlc.UnixNano), stopper,
	)
	addr, err := addrWithDefaultHost(serverCfg.AdvertiseAddr)
	if err != nil {
		return nil, nil, err
	}
	conn, err := rpcContext.GRPCDial(addr)
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
	RunE:         maybeDecorateGRPCError(runQuit),
}

// doShutdown attempts to trigger a server shutdown. When given an empty
// onModes slice, it's a hard shutdown.
func doShutdown(ctx context.Context, c serverpb.AdminClient, onModes []int32) error {
	// This is kind of hairy, but should work well in practice. We want to
	// distinguish between the case in which we can't even connect to the
	// server (in which case we don't want our caller to try to come back with
	// a hard retry) and the case in which an attempt to shut down fails (times
	// out, or perhaps drops the connection while waiting). To that end, we
	// first run a noop DrainRequest. If that fails, we give up. Otherwise, we
	// wait for the "real" request to give us a response and then continue
	// reading until the connection drops (which then counts as a success, for
	// the connection dropping is likely the result of the Stopper having
	// reached the final stages of shutdown).
	for i, modes := range [][]int32{nil, onModes} {
		stream, err := c.Drain(ctx, &serverpb.DrainRequest{
			On:       modes,
			Shutdown: i > 0,
		})
		if err != nil {
			return err
		}
		// Only signal the caller to try again with a hard shutdown if we're
		// not already trying to do that, and if the initial connection attempt
		// (without shutdown) has succeeded.
		tryHard := i > 0 && len(onModes) > 0
		var hasResp bool
		for {
			if _, err := stream.Recv(); err != nil {
				if hasResp && grpcutil.IsClosedConnection(err) {
					// We're trying to shut down, and we already got a response
					// and now the connection is closed. This means we shut
					// down successfully.
					return nil
				}
				if tryHard {
					// Either we hadn't seen a response yet (in which case it's
					// likely that our connection broke while waiting for the
					// shutdown to happen); try again (and harder).
					return errTryHardShutdown{err}
				}
				// Case in which we don't even know whether the server is
				// running. No point in trying again.
				return err
			}
			if i == 0 {
				// Our liveness probe succeeded.
				break
			}
			hasResp = true
		}
	}
	return nil
}

type errTryHardShutdown struct{ error }

// runQuit accesses the quit shutdown path.
func runQuit(_ *cobra.Command, _ []string) (err error) {
	defer func() {
		if err == nil {
			fmt.Println("ok")
		}
	}()
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

	if err := doShutdown(ctx, c, onModes); err != nil {
		if _, ok := err.(*errTryHardShutdown); ok {
			fmt.Fprintf(
				os.Stderr, "graceful shutdown failed: %s\nproceeding with hard shutdown\n", err,
			)
		} else {
			return err
		}
	} else {
		return nil
	}
	// Not passing drain modes tells the server to not bother and go
	// straight to shutdown.
	return errors.Wrap(doShutdown(ctx, c, nil), "hard shutdown failed")
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
	RunE:         maybeDecorateGRPCError(runFreezeCluster),
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
