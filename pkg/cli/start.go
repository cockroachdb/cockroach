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
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os"
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
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

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
	Example: `  cockroach start --insecure --store=attrs=ssd,path=/mnt/ssd1 [--join=host:port,[host:port]]`,
	RunE:    MaybeShoutError(MaybeDecorateGRPCError(runStart)),
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

// maxSizePerProfile is the maximum total size in bytes for profiles per
// profile type.
var maxSizePerProfile = envutil.EnvOrDefaultInt64(
	"COCKROACH_MAX_SIZE_PER_PROFILE", 100<<20 /* 100 MB */)

// gcProfiles removes old profiles matching the specified prefix when the sum
// of newer profiles is larger than maxSize. Requires that the suffix used for
// the profiles indicates age (e.g. by using a date/timestamp suffix) such that
// sorting the filenames corresponds to ordering the profiles from oldest to
// newest.
func gcProfiles(dir, prefix string, maxSize int64) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Warning(context.Background(), err)
		return
	}
	var sum int64
	var found int
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]
		if !f.Mode().IsRegular() {
			continue
		}
		if !strings.HasPrefix(f.Name(), prefix) {
			continue
		}
		found++
		sum += f.Size()
		if found == 1 {
			// Always keep the most recent profile.
			continue
		}
		if sum <= maxSize {
			continue
		}
		if err := os.Remove(filepath.Join(dir, f.Name())); err != nil {
			log.Info(context.Background(), err)
		}
	}
}

func initMemProfile(ctx context.Context, dir string) {
	const jeprof = "jeprof."
	const memprof = "memprof."

	gcProfiles(dir, jeprof, maxSizePerProfile)
	gcProfiles(dir, memprof, maxSizePerProfile)

	memProfileInterval := envutil.EnvOrDefaultDuration("COCKROACH_MEMPROF_INTERVAL", -1)
	if memProfileInterval <= 0 {
		return
	}
	if min := time.Second; memProfileInterval < min {
		log.Infof(ctx, "fixing excessively short memory profiling interval: %s -> %s",
			memProfileInterval, min)
		memProfileInterval = min
	}

	if jemallocHeapDump != nil {
		log.Infof(ctx, "writing go and jemalloc memory profiles to %s every %s", dir, memProfileInterval)
	} else {
		log.Infof(ctx, "writing go only memory profiles to %s every %s", dir, memProfileInterval)
		log.Infof(ctx, `to enable jmalloc profiling: "export MALLOC_CONF=prof:true" or "ln -s prof:true /etc/malloc.conf"`)
	}

	go func() {
		ctx := context.Background()
		t := time.NewTicker(memProfileInterval)
		defer t.Stop()

		for {
			<-t.C

			func() {
				const format = "2006-01-02T15_04_05.999"
				suffix := timeutil.Now().Format(format)

				// Try jemalloc heap profile first, we only log errors.
				if jemallocHeapDump != nil {
					jepath := filepath.Join(dir, jeprof+suffix)
					if err := jemallocHeapDump(jepath); err != nil {
						log.Warningf(ctx, "error writing jemalloc heap %s: %s", jepath, err)
					}
					gcProfiles(dir, jeprof, maxSizePerProfile)
				}

				path := filepath.Join(dir, memprof+suffix)
				// Try writing a go heap profile.
				f, err := os.Create(path)
				if err != nil {
					log.Warningf(ctx, "error creating go heap file %s", err)
					return
				}
				defer f.Close()
				if err = pprof.WriteHeapProfile(f); err != nil {
					log.Warningf(ctx, "error writing go heap %s: %s", path, err)
					return
				}
				gcProfiles(dir, memprof, maxSizePerProfile)
			}()
		}
	}()
}

func initCPUProfile(ctx context.Context, dir string) {
	const cpuprof = "cpuprof."
	gcProfiles(dir, cpuprof, maxSizePerProfile)

	cpuProfileInterval := envutil.EnvOrDefaultDuration("COCKROACH_CPUPROF_INTERVAL", -1)
	if cpuProfileInterval <= 0 {
		return
	}
	if min := time.Second; cpuProfileInterval < min {
		log.Infof(ctx, "fixing excessively short cpu profiling interval: %s -> %s",
			cpuProfileInterval, min)
		cpuProfileInterval = min
	}

	go func() {
		defer log.RecoverAndReportPanic(ctx)

		ctx := context.Background()

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
				f, err := os.Create(filepath.Join(dir, cpuprof+suffix))
				if err != nil {
					log.Warningf(ctx, "error creating go cpu file %s", err)
					return
				}

				// Stop the current profile if it exists.
				if currentProfile != nil {
					pprof.StopCPUProfile()
					currentProfile.Close()
					currentProfile = nil
					gcProfiles(dir, cpuprof, maxSizePerProfile)
				}

				// Start the new profile.
				if err := pprof.StartCPUProfile(f); err != nil {
					log.Warningf(ctx, "unable to start cpu profile: %v", err)
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
	// The block profile can be viewed with `pprof http://HOST:PORT/debug/pprof/block`
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
func runStart(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		return usageAndError(cmd)
	}

	if ok, err := maybeRerunBackground(); ok {
		return err
	}

	// Use the server-specific values for some flags and settings.
	serverCfg.Insecure = serverInsecure
	serverCfg.SSLCertsDir = serverSSLCertsDir
	serverCfg.User = security.NodeUser

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	tracer := tracing.NewTracer()
	sp := tracer.StartSpan("server start")
	startCtx := opentracing.ContextWithSpan(context.Background(), sp)

	// Set up the logging and profiling output.
	// It is important that no logging occurs before this point or the log files
	// will be created in $TMPDIR instead of their expected location.
	stopper, err := setupAndInitializeLoggingAndProfiling(startCtx)
	if err != nil {
		return err
	}

	// Run the rest of the startup process in the background to avoid preventing
	// proper handling of signals if we get stuck on something during
	// initialization (#10138).
	var serverStatusMu struct {
		syncutil.Mutex
		// Used to synchronize server startup with server shutdown if something
		// interrupts the process during initialization (it isn't safe to try to
		// drain a server that doesn't exist, or to start a server after draining
		// has begun).
		created, draining bool
	}
	var s *server.Server
	errChan := make(chan error, 1)
	go func() {
		defer log.RecoverAndReportPanic(startCtx)

		defer sp.Finish()
		if err := func() error {
			if err := serverCfg.InitNode(); err != nil {
				return errors.Wrap(err, "failed to initialize node")
			}

			log.Info(startCtx, "starting cockroach node")
			if envVarsUsed := envutil.GetEnvVarsUsed(); len(envVarsUsed) > 0 {
				log.Infof(startCtx, "using local environment variables: %s", strings.Join(envVarsUsed, ", "))
			}

			var err error
			s, err = server.NewServer(serverCfg, stopper)
			if err != nil {
				return errors.Wrap(err, "failed to start Cockroach server")
			}

			serverStatusMu.Lock()
			serverStatusMu.created = true
			draining := serverStatusMu.draining
			serverStatusMu.Unlock()
			if draining {
				return nil
			}

			if err := s.Start(startCtx); err != nil {
				return errors.Wrap(err, "cockroach server exited with error")
			}

			// We don't do this in (*server.Server).Start() because we don't want it
			// in tests.
			if !envutil.EnvOrDefaultBool("COCKROACH_SKIP_UPDATE_CHECK", false) {
				s.PeriodicallyCheckForUpdates()
			}

			pgURL, err := serverCfg.PGURL(url.User(sqlConnUser))
			if err != nil {
				return err
			}

			var buf bytes.Buffer
			info := build.GetInfo()
			tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
			fmt.Fprintf(tw, "CockroachDB node starting at %s\n", timeutil.Now())
			fmt.Fprintf(tw, "build:\t%s %s @ %s (%s)\n", info.Distribution, info.Tag, info.Time, info.GoVersion)
			fmt.Fprintf(tw, "admin:\t%s\n", serverCfg.AdminURL())
			fmt.Fprintf(tw, "sql:\t%s\n", pgURL)
			if len(serverCfg.SocketFile) != 0 {
				fmt.Fprintf(tw, "socket:\t%s\n", serverCfg.SocketFile)
			}
			fmt.Fprintf(tw, "logs:\t%s\n", flag.Lookup("log-dir").Value)
			if serverCfg.Attrs != "" {
				fmt.Fprintf(tw, "attrs:\t%s\n", serverCfg.Attrs)
			}
			if len(serverCfg.Locality.Tiers) > 0 {
				fmt.Fprintf(tw, "locality:\t%s\n", serverCfg.Locality)
			}
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
			msg := buf.String()
			log.Infof(startCtx, "Startup message:\n%s", msg)
			fmt.Print(msg)
			return nil
		}(); err != nil {
			errChan <- err
		}
	}()

	shutdownSpan := tracer.StartSpan("server shutdown")
	defer shutdownSpan.Finish()
	shutdownCtx := opentracing.ContextWithSpan(context.Background(), shutdownSpan)
	var returnErr error

	// Block until one of the signals above is received or the stopper
	// is stopped externally (for example, via the quit endpoint).
	select {
	case err := <-errChan:
		return err
	case <-stopper.ShouldStop():
		// Server is being stopped externally and our job is finished
		// here since we don't know if it's a graceful shutdown or not.
		<-stopper.IsStopped()
		return nil
	case sig := <-signalCh:
		log.Infof(shutdownCtx, "received signal '%s'", sig)
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
			serverStatusMu.Lock()
			serverStatusMu.draining = true
			needToDrain := serverStatusMu.created
			serverStatusMu.Unlock()
			if needToDrain {
				if _, err := s.Drain(server.GracefulDrainModes); err != nil {
					log.Warning(shutdownCtx, err)
				}
			}
			stopper.Stop(shutdownCtx)
		}()
	}

	const msgDrain = "initiating graceful shutdown of server"
	log.Info(shutdownCtx, msgDrain)
	fmt.Fprintln(os.Stdout, msgDrain)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Infof(shutdownCtx, "%d running tasks", stopper.NumTasks())
			case <-stopper.ShouldStop():
				return
			}
		}
	}()

	select {
	case sig := <-signalCh:
		returnErr = fmt.Errorf("received signal '%s' during shutdown, initiating hard shutdown", sig)
		// This new signal is not welcome, as it interferes with the graceful
		// shutdown process. On Unix, a signal that was not handled gracefully by
		// the application should be visible to other processes as an exit code
		// encoded as 128+signal number.
		//
		// Also, on Unix, os.Signal is syscall.Signal and it's convertible to int.
		ErrorCode = 128 + int(sig.(syscall.Signal))
		// NB: we do not return here to go through log.Flush below.
	case <-time.After(time.Minute):
		returnErr = errors.New("time limit reached, initiating hard shutdown")
		// NB: we do not return here to go through log.Flush below.
	case <-stopper.IsStopped():
		const msgDone = "server drained and shutdown completed"
		log.Infof(shutdownCtx, msgDone)
		fmt.Fprintln(os.Stdout, msgDone)
	}
	log.Flush()

	return returnErr
}

// setupAndInitializeLoggingAndProfiling does what it says on the label.
// Prior to this however it determines suitable defaults for the
// logging output directory and the verbosity level of stderr logging.
// We only do this for the "start" command which is why this work
// occurs here and not in an OnInitialize function.
func setupAndInitializeLoggingAndProfiling(startCtx context.Context) (*stop.Stopper, error) {
	// Default the log directory to the "logs" subdirectory of the first
	// non-memory store. If more than one non-memory stores is detected,
	// print a warning.
	ambiguousLogDirs := false
	pf := cockroachCmd.PersistentFlags()
	f := pf.Lookup(logflags.LogDirName)
	if !log.DirSet() && !f.Changed {
		// We only override the log directory if the user has not explicitly
		// disabled file logging using --log-dir="".
		newDir := ""
		for _, spec := range serverCfg.Stores.Specs {
			if spec.InMemory {
				continue
			}
			if newDir != "" {
				ambiguousLogDirs = true
				break
			}
			newDir = filepath.Join(spec.Path, "logs")
		}
		if err := f.Value.Set(newDir); err != nil {
			return nil, err
		}
	}

	// We need an output directory below. We use the current directory unless
	// the log directory is configured, in which case we use that.
	outputDirectory := "."
	if logDir := f.Value.String(); logDir != "" {
		outputDirectory = logDir

		ls := pf.Lookup(logflags.LogToStderrName)
		if !ls.Changed {
			// Unless the settings were overridden by the user, silence
			// logging to stderr because the messages will go to a log file.
			if err := ls.Value.Set(log.Severity_NONE.String()); err != nil {
				return nil, err
			}
		}

		// Make sure the path exists.
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return nil, err
		}
		log.Eventf(startCtx, "created log directory %s", logDir)

		// Start the log file GC daemon to remove files that make the log
		// directory too large.
		log.StartGCDaemon()
	}

	if ambiguousLogDirs {
		// Note that we can't report this message earlier, because the log directory
		// may not have been ready before the call to MkdirAll() above.
		log.Shout(startCtx, log.Severity_WARNING, "multiple stores configured"+
			" and --log-dir not specified, you may want to specify --log-dir to disambiguate.")
	}

	// We log build information to stdout (for the short summary), but also
	// to stderr to coincide with the full logs.
	info := build.GetInfo()
	log.Infof(startCtx, info.Short())

	initMemProfile(startCtx, outputDirectory)
	initCPUProfile(startCtx, outputDirectory)
	initBlockProfile()

	// Disable Stopper task tracking as performing that call site tracking is
	// moderately expensive (certainly outweighing the infrequent benefit it
	// provides).
	stopper := initBacktrace(outputDirectory, stop.TrackTasks(false))
	log.Event(startCtx, "initialized profiles")

	return stopper, nil
}

func addrWithDefaultHost(addr string) (string, error) {
	host, port, err := net.SplitHostPort(baseCfg.Addr)
	if err != nil {
		return "", err
	}
	if host == "" {
		host = "localhost"
	}
	return net.JoinHostPort(host, port), nil
}

func getClientGRPCConn() (*grpc.ClientConn, *hlc.Clock, *stop.Stopper, error) {
	// 0 to disable max offset checks; this RPC context is not a member of the
	// cluster, so there's no need to enforce that its max offset is the same
	// as that of nodes in the cluster.
	clock := hlc.NewClock(hlc.UnixNano, 0)
	stopper := stop.NewStopper()
	rpcContext := rpc.NewContext(
		log.AmbientContext{},
		serverCfg.Config,
		clock,
		stopper,
	)
	addr, err := addrWithDefaultHost(serverCfg.AdvertiseAddr)
	if err != nil {
		return nil, nil, nil, err
	}
	conn, err := rpcContext.GRPCDial(addr)
	if err != nil {
		return nil, nil, nil, err
	}
	return conn, clock, stopper, nil
}

func getAdminClient() (serverpb.AdminClient, *stop.Stopper, error) {
	conn, _, stopper, err := getClientGRPCConn()
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
	RunE: MaybeDecorateGRPCError(runQuit),
}

// checkNodeRunning performs a no-op RPC and returns an error if it failed to
// connect to the server.
func checkNodeRunning(ctx context.Context, c serverpb.AdminClient) error {
	// Send a no-op Drain request.
	stream, err := c.Drain(ctx, &serverpb.DrainRequest{
		On:       nil,
		Shutdown: false,
	})
	if err != nil {
		return errors.Wrap(err, "Failed to connect to the node: error sending drain request")
	}
	// Ignore errors from the stream. We've managed to connect to the node above,
	// and that's all that this function is interested in.
	for {
		if _, err := stream.Recv(); err != nil {
			if err != io.EOF {
				log.Warningf(ctx, "unexpected error from no-op Drain request: %s", err)
			}
			break
		}
	}
	return nil
}

// doShutdown attempts to trigger a server shutdown. When given an empty
// onModes slice, it's a hard shutdown.
//
// errTryHardShutdown is returned if the caller should do a hard-shutdown.
func doShutdown(ctx context.Context, c serverpb.AdminClient, onModes []int32) error {
	// We want to distinguish between the case in which we can't even connect to
	// the server (in which case we don't want our caller to try to come back with
	// a hard retry) and the case in which an attempt to shut down fails (times
	// out, or perhaps drops the connection while waiting). To that end, we first
	// run a noop DrainRequest. If that fails, we give up.
	if err := checkNodeRunning(ctx, c); err != nil {
		return err
	}
	// Send a drain request and continue reading until the connection drops (which
	// then counts as a success, for the connection dropping is likely the result
	// of the Stopper having reached the final stages of shutdown).
	stream, err := c.Drain(ctx, &serverpb.DrainRequest{
		On:       onModes,
		Shutdown: true,
	})
	if err != nil {
		//  This most likely means that we shut down successfully. Note that
		//  sometimes the connection can be shut down even before a DrainResponse gets
		//  sent back to us, so we don't require a response on the stream (see
		//  #14184).
		if grpcutil.IsClosedConnection(err) {
			return nil
		}
		return errors.Wrap(err, "Error sending drain request")
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if grpcutil.IsClosedConnection(err) {
				return nil
			}
			// Unexpected error; the caller should try again (and harder).
			return errTryHardShutdown{err}
		}
	}
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
	ctx := stopperContext(stopper)
	defer stopper.Stop(ctx)

	errChan := make(chan error, 1)
	go func() {
		errChan <- doShutdown(ctx, c, onModes)
	}()
	select {
	case err := <-errChan:
		if err != nil {
			if _, ok := err.(errTryHardShutdown); ok {
				fmt.Printf("graceful shutdown failed: %s; proceeding with hard shutdown\n", err)
				break
			}
			return err
		}
		return nil
	case <-time.After(time.Minute):
		fmt.Println("timed out; proceeding with hard shutdown")
	}
	// Not passing drain modes tells the server to not bother and go
	// straight to shutdown.
	return errors.Wrap(doShutdown(ctx, c, nil), "hard shutdown failed")
}
