// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/util/sdnotify"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

// jemallocHeapDump is an optional function to be called at heap dump time.
// This will be non-nil when jemalloc is linked in with profiling enabled.
// The function takes a filename to write the profile to.
var jemallocHeapDump func(string) error

// startCmd starts a node by initializing the stores and joining
// the cluster.
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start a node in a multi-node cluster",
	Long: `
Start a CockroachDB node, which will export data from one or more
storage devices, specified via --store flags.

Specify the --join flag to point to another node or nodes that are
part of the same cluster. The other nodes do not need to be started
yet, and if the address of the other nodes to be added are not yet
known it is legal for the first node to join itself.

If --join is not specified, the cluster will also be initialized.
THIS BEHAVIOR IS DEPRECATED; consider using 'cockroach init' or
'cockroach start-single-node' instead.
`,
	Example: `  cockroach start --insecure --store=attrs=ssd,path=/mnt/ssd1 --join=host:port,[host:port]`,
	Args:    cobra.NoArgs,
	RunE:    maybeShoutError(MaybeDecorateGRPCError(runStartJoin)),
}

// startSingleNodeCmd starts a node by initializing the stores.
var startSingleNodeCmd = &cobra.Command{
	Use:   "start-single-node",
	Short: "start a single-node cluster",
	Long: `
Start a CockroachDB node, which will export data from one or more
storage devices, specified via --store flags.
The cluster will also be automatically initialized with
replication disabled (replication factor = 1).
`,
	Example: `  cockroach start-single-node --insecure --store=attrs=ssd,path=/mnt/ssd1`,
	Args:    cobra.NoArgs,
	RunE:    maybeShoutError(MaybeDecorateGRPCError(runStartSingleNode)),
}

// StartCmds exports startCmd and startSingleNodeCmds so that other
// packages can add flags to them.
var StartCmds = []*cobra.Command{startCmd, startSingleNodeCmd}

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
		log.Warningf(context.Background(), "%v", err)
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
			log.Infof(context.Background(), "%v", err)
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
		defer log.RecoverAndReportPanic(ctx, &serverCfg.Settings.SV)

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
	//
	// The utility of the block profile (aka blocking profile) has diminished
	// with the advent of the mutex profile. We currently leave the block profile
	// disabled by default as it has a non-zero performance impact.
	d := envutil.EnvOrDefaultInt64("COCKROACH_BLOCK_PROFILE_RATE", 0)
	runtime.SetBlockProfileRate(int(d))
}

func initMutexProfile() {
	// Enable the mutex profile for a fraction of mutex contention events.
	// Smaller values provide more accurate profiles but are more expensive. 0
	// and 1 are special: 0 disables the mutex profile and 1 captures 100% of
	// mutex contention events. For other values, the profiler will sample on
	// average 1/X events.
	//
	// The mutex profile can be viewed with `pprof http://HOST:PORT/debug/pprof/mutex`
	d := envutil.EnvOrDefaultInt("COCKROACH_MUTEX_PROFILE_RATE",
		1000 /* 1 sample per 1000 mutex contention events */)
	runtime.SetMutexProfileFraction(d)
}

var cacheSizeValue = newBytesOrPercentageValue(&serverCfg.CacheSize, memoryPercentResolver)
var sqlSizeValue = newBytesOrPercentageValue(&serverCfg.MemoryPoolSize, memoryPercentResolver)
var diskTempStorageSizeValue = newBytesOrPercentageValue(nil /* v */, nil /* percentResolver */)

func initExternalIODir(ctx context.Context, firstStore base.StoreSpec) (string, error) {
	externalIODir := startCtx.externalIODir
	if externalIODir == "" && !firstStore.InMemory {
		externalIODir = filepath.Join(firstStore.Path, "extern")
	}
	if externalIODir == "" || externalIODir == "disabled" {
		return "", nil
	}
	if !filepath.IsAbs(externalIODir) {
		return "", errors.Errorf("%s path must be absolute", cliflags.ExternalIODir.Name)
	}
	return externalIODir, nil
}

func initTempStorageConfig(
	ctx context.Context, st *cluster.Settings, stopper *stop.Stopper, useStore base.StoreSpec,
) (base.TempStorageConfig, error) {
	var recordPath string
	if !useStore.InMemory {
		recordPath = filepath.Join(useStore.Path, server.TempDirsRecordFilename)
	}

	var err error
	// Need to first clean up any abandoned temporary directories from
	// the temporary directory record file before creating any new
	// temporary directories in case the disk is completely full.
	if recordPath != "" {
		if err = storage.CleanupTempDirs(recordPath); err != nil {
			return base.TempStorageConfig{}, errors.Wrap(err, "could not cleanup temporary directories from record file")
		}
	}

	// The temp store size can depend on the location of the first regular store
	// (if it's expressed as a percentage), so we resolve that flag here.
	var tempStorePercentageResolver percentResolverFunc
	if !useStore.InMemory {
		dir := useStore.Path
		// Create the store dir, if it doesn't exist. The dir is required to exist
		// by diskPercentResolverFactory.
		if err = os.MkdirAll(dir, 0755); err != nil {
			return base.TempStorageConfig{}, errors.Wrapf(err, "failed to create dir for first store: %s", dir)
		}
		tempStorePercentageResolver, err = diskPercentResolverFactory(dir)
		if err != nil {
			return base.TempStorageConfig{}, errors.Wrapf(err, "failed to create resolver for: %s", dir)
		}
	} else {
		tempStorePercentageResolver = memoryPercentResolver
	}
	var tempStorageMaxSizeBytes int64
	if err = diskTempStorageSizeValue.Resolve(
		&tempStorageMaxSizeBytes, tempStorePercentageResolver,
	); err != nil {
		return base.TempStorageConfig{}, err
	}
	if !diskTempStorageSizeValue.IsSet() {
		// The default temp storage size is different when the temp
		// storage is in memory (which occurs when no temp directory
		// is specified and the first store is in memory).
		if startCtx.tempDir == "" && useStore.InMemory {
			tempStorageMaxSizeBytes = base.DefaultInMemTempStorageMaxSizeBytes
		} else {
			tempStorageMaxSizeBytes = base.DefaultTempStorageMaxSizeBytes
		}
	}

	// Initialize a base.TempStorageConfig based on first store's spec and
	// cli flags.
	tempStorageConfig := base.TempStorageConfigFromEnv(
		ctx,
		st,
		useStore,
		startCtx.tempDir,
		tempStorageMaxSizeBytes,
	)

	// Set temp directory to first store's path if the temp storage is not
	// in memory.
	tempDir := startCtx.tempDir
	if tempDir == "" && !tempStorageConfig.InMemory {
		tempDir = useStore.Path
	}
	// Create the temporary subdirectory for the temp engine.
	if tempStorageConfig.Path, err = storage.CreateTempDir(tempDir, server.TempDirPrefix, stopper); err != nil {
		return base.TempStorageConfig{}, errors.Wrap(err, "could not create temporary directory for temp storage")
	}

	// We record the new temporary directory in the record file (if it
	// exists) for cleanup in case the node crashes.
	if recordPath != "" {
		if err = storage.RecordTempDir(recordPath, tempStorageConfig.Path); err != nil {
			return base.TempStorageConfig{}, errors.Wrapf(
				err,
				"could not record temporary directory path to record file: %s",
				recordPath,
			)
		}
	}

	return tempStorageConfig, nil
}

// Checks if the passed-in engine type is default, and if so, resolves it to
// the storage engine last used to write to the store at dir (or rocksdb if
// a store wasn't found).
func resolveStorageEngineType(
	ctx context.Context, engineType enginepb.EngineType, cfg base.StorageConfig,
) enginepb.EngineType {
	if engineType == enginepb.EngineTypeDefault {
		engineType = enginepb.EngineTypePebble
		pebbleCfg := &storage.PebbleConfig{
			StorageConfig: cfg,
			Opts:          storage.DefaultPebbleOptions(),
		}
		pebbleCfg.Opts.EnsureDefaults()
		pebbleCfg.Opts.ReadOnly = true
		// Resolve encrypted env options in pebbleCfg and populate pebbleCfg.Opts.FS
		// if necessary (eg. encrypted-at-rest is enabled).
		_, _, err := storage.ResolveEncryptedEnvOptions(pebbleCfg)
		if err != nil {
			log.Infof(ctx, "unable to setup encrypted env to resolve past engine type: %s", err)
			return engineType
		}

		// Check if this storage directory was last written to by rocksdb. In that
		// case, default to opening a RocksDB engine.
		if version, err := pebble.GetVersion(cfg.Dir, pebbleCfg.Opts.FS); err == nil {
			if strings.HasPrefix(version, "rocksdb") {
				engineType = enginepb.EngineTypeRocksDB
			}
		}
	}
	return engineType
}

var errCannotUseJoin = errors.New("cannot use --join with 'cockroach start-single-node' -- use 'cockroach start' instead")

func runStartSingleNode(cmd *cobra.Command, args []string) error {
	joinFlag := flagSetForCmd(cmd).Lookup(cliflags.Join.Name)
	if joinFlag.Changed {
		return errCannotUseJoin
	}
	// Now actually set the flag as changed so that the start code
	// doesn't warn that it was not set.
	joinFlag.Changed = true
	return runStart(cmd, args, true /*disableReplication*/)
}

func runStartJoin(cmd *cobra.Command, args []string) error {
	return runStart(cmd, args, false /*disableReplication*/)
}

// runStart starts the cockroach node using --store as the list of
// storage devices ("stores") on this machine and --join as the list
// of other active nodes used to join this node to the cockroach
// cluster, if this is its first time connecting.
//
// If the argument disableReplication is true and we are starting
// a fresh cluster, the replication factor will be disabled in
// all zone configs.
func runStart(cmd *cobra.Command, args []string, disableReplication bool) error {
	tBegin := timeutil.Now()

	// First things first: if the user wants background processing,
	// relinquish the terminal ASAP by forking and exiting.
	//
	// If executing in the background, the function returns ok == true in
	// the parent process (regardless of err) and the parent exits at
	// this point.
	if ok, err := maybeRerunBackground(); ok {
		return err
	}

	// Change the permission mask for all created files.
	//
	// We're considering everything produced by a cockroach node
	// to potentially contain sensitive information, so it should
	// not be world-readable.
	disableOtherPermissionBits()

	// Set up the signal handlers. This also ensures that any of these
	// signals received beyond this point do not interrupt the startup
	// sequence until the point signals are checked below.
	// We want to set up signal handling before starting logging, because
	// logging uses buffering, and we want to be able to sync
	// the buffers in the signal handler below. If we started capturing
	// signals later, some startup logging might be lost.
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, drainSignals...)

	// Set up a cancellable context for the entire start command.
	// The context will be canceled at the end.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up a tracing span for the start process.  We want any logging
	// happening beyond this point to be accounted to this start
	// context, including logging related to the initialization of
	// the logging infrastructure below.
	// This span concludes when the startup goroutine started below
	// has completed.
	// TODO(andrei): we don't close the span on the early returns below.
	tracer := serverCfg.Settings.Tracer
	sp := tracer.StartRootSpan("server start", nil /* logTags */, tracing.NonRecordableSpan)
	ctx = opentracing.ContextWithSpan(ctx, sp)

	// Set up the logging and profiling output.
	//
	// We want to do this as early as possible, because most of the code
	// in CockroachDB may use logging, and until logging has been
	// initialized log files will be created in $TMPDIR instead of their
	// expected location.
	//
	// This initialization uses the various configuration parameters
	// initialized by flag handling (before runStart was called). Any
	// additional server configuration tweaks for the startup process
	// must be necessarily non-logging-related, as logging parameters
	// cannot be picked up beyond this point.
	stopper, err := setupAndInitializeLoggingAndProfiling(ctx, cmd)
	if err != nil {
		return err
	}

	// If any store has something to say against a server start-up
	// (e.g. previously detected corruption), listen to them now.
	if err := serverCfg.Stores.PriorCriticalAlertError(); err != nil {
		return err
	}

	// We don't care about GRPCs fairly verbose logs in most client commands,
	// but when actually starting a server, we enable them.
	grpcutil.SetSeverity(log.Severity_WARNING)

	// Check the --join flag.
	if !flagSetForCmd(cmd).Lookup(cliflags.Join.Name).Changed {
		log.Shout(ctx, log.Severity_WARNING,
			"running 'cockroach start' without --join is deprecated.\n"+
				"Consider using 'cockroach start-single-node' or 'cockroach init' instead.")
	}

	// Now perform additional configuration tweaks specific to the start
	// command.

	// Derive temporary/auxiliary directory specifications.
	if serverCfg.Settings.ExternalIODir, err = initExternalIODir(ctx, serverCfg.Stores.Specs[0]); err != nil {
		return err
	}

	// Build a minimal StorageConfig out of the first store's spec, with enough
	// attributes to be able to read encrypted-at-rest store directories.
	firstSpec := serverCfg.Stores.Specs[0]
	firstStoreConfig := base.StorageConfig{
		Attrs:           firstSpec.Attributes,
		Dir:             firstSpec.Path,
		Settings:        serverCfg.Settings,
		UseFileRegistry: firstSpec.UseFileRegistry,
		ExtraOptions:    firstSpec.ExtraOptions,
	}
	// If the storage engine is set to "default", check the engine type used in
	// this store directory in a past run. If this check fails for any reason,
	// use Pebble as the default engine type.
	serverCfg.StorageEngine = resolveStorageEngineType(ctx, serverCfg.StorageEngine, firstStoreConfig)

	// Next we initialize the target directory for temporary storage.
	// If encryption at rest is enabled in any fashion, we'll want temp
	// storage to be encrypted too. To achieve this, we use
	// the first encrypted store as temp dir target, if any.
	// If we can't find one, we use the first StoreSpec in the list.
	var specIdx = 0
	for i := range serverCfg.Stores.Specs {
		if serverCfg.Stores.Specs[i].ExtraOptions != nil {
			specIdx = i
		}
	}

	if serverCfg.TempStorageConfig, err = initTempStorageConfig(
		ctx, serverCfg.Settings, stopper, serverCfg.Stores.Specs[specIdx],
	); err != nil {
		return err
	}

	// Initialize the node's configuration from startup parameters.
	// This also reads the part of the configuration that comes from
	// environment variables.
	if err := serverCfg.InitNode(ctx); err != nil {
		return errors.Wrap(err, "failed to initialize node")
	}

	// The configuration is now ready to report to the user and the log
	// file. We had to wait after InitNode() so that all configuration
	// environment variables, which are reported too, have been read and
	// registered.
	reportConfiguration(ctx)

	// Until/unless CockroachDB embeds its own tz database, we want
	// an early sanity check. It's better to inform the user early
	// than to get surprising errors during SQL queries.
	if err := checkTzDatabaseAvailability(ctx); err != nil {
		return errors.Wrap(err, "failed to initialize node")
	}

	// ReadyFn will be called when the server has started listening on
	// its network sockets, but perhaps before it has done bootstrapping
	// and thus before Start() completes.
	serverCfg.ReadyFn = func(waitForInit bool) {
		// Inform the user if the network settings are suspicious. We need
		// to do that after starting to listen because we need to know
		// which advertise address NewServer() has decided.
		hintServerCmdFlags(ctx, cmd)

		// If another process was waiting on the PID (e.g. using a FIFO),
		// this is when we can tell them the node has started listening.
		if startCtx.pidFile != "" {
			log.Infof(ctx, "PID file: %s", startCtx.pidFile)
			if err := ioutil.WriteFile(startCtx.pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644); err != nil {
				log.Errorf(ctx, "failed writing the PID: %v", err)
			}
		}

		// If the invoker has requested an URL update, do it now that
		// the server is ready to accept SQL connections.
		// (Note: as stated above, ReadyFn is called after the server
		// has started listening on its socket, but possibly before
		// the cluster has been initialized and can start processing requests.
		// This is OK for SQL clients, as the connection will be accepted
		// by the network listener and will just wait/suspend until
		// the cluster initializes, at which point it will be picked up
		// and let the client go through, transparently.)
		if startCtx.listeningURLFile != "" {
			log.Infof(ctx, "listening URL file: %s", startCtx.listeningURLFile)
			// (Re-)compute the client connection URL. We cannot do this
			// earlier (e.g. above, in the runStart function) because
			// at this time the address and port have not been resolved yet.
			sCtx := rpc.MakeSecurityContext(serverCfg.Config)
			pgURL, err := sCtx.PGURL(url.User(security.RootUser))
			if err != nil {
				log.Errorf(ctx, "failed computing the URL: %v", err)
				return
			}

			if err = ioutil.WriteFile(startCtx.listeningURLFile, []byte(fmt.Sprintf("%s\n", pgURL)), 0644); err != nil {
				log.Errorf(ctx, "failed writing the URL: %v", err)
			}
		}

		if waitForInit {
			log.Shout(ctx, log.Severity_INFO,
				"initial startup completed.\n"+
					"Node will now attempt to join a running cluster, or wait for `cockroach init`.\n"+
					"Client connections will be accepted after this completes successfully.\n"+
					"Check the log file(s) for progress. ")
		}

		// Ensure the configuration logging is written to disk in case a
		// process is waiting for the sdnotify readiness to read important
		// information from there.
		log.Flush()

		// Signal readiness. This unblocks the process when running with
		// --background or under systemd.
		if err := sdnotify.Ready(); err != nil {
			log.Errorf(ctx, "failed to signal readiness using systemd protocol: %s", err)
		}
	}

	// DelayedBoostrapFn will be called if the boostrap process is
	// taking a bit long.
	serverCfg.DelayedBootstrapFn = func() {
		const msg = `The server appears to be unable to contact the other nodes in the cluster. Please try:

- starting the other nodes, if you haven't already;
- double-checking that the '--join' and '--listen'/'--advertise' flags are set up correctly;
- running the 'cockroach init' command if you are trying to initialize a new cluster.

If problems persist, please see %s.`
		docLink := base.DocsURL("cluster-setup-troubleshooting.html")
		if !startCtx.inBackground {
			log.Shoutf(context.Background(), log.Severity_WARNING, msg, docLink)
		} else {
			// Don't shout to stderr since the server will have detached by
			// the time this function gets called.
			log.Warningf(ctx, msg, docLink)
		}
	}

	// Set up the Geospatial library.
	// We need to make sure this happens before any queries involving geospatial data is executed.
	loc, err := geos.EnsureInit(geos.EnsureInitErrorDisplayPrivate, startCtx.geoLibsDir)
	if err != nil {
		log.Infof(ctx, "could not initialize GEOS - geospatial functions may not be available: %v", err)
	} else {
		log.Infof(ctx, "GEOS loaded from directory %s", loc)
	}

	// Beyond this point, the configuration is set and the server is
	// ready to start.
	log.Info(ctx, "starting cockroach node")

	// Run the rest of the startup process in a goroutine separate from
	// the main goroutine to avoid preventing proper handling of signals
	// if we get stuck on something during initialization (#10138).
	var serverStatusMu struct {
		syncutil.Mutex
		// Used to synchronize server startup with server shutdown if something
		// interrupts the process during initialization (it isn't safe to try to
		// drain a server that doesn't exist or is in the middle of starting up,
		// or to start a server after draining has begun).
		started, draining bool
	}
	var s *server.Server
	errChan := make(chan error, 1)
	go func() {
		// Ensure that the log files see the startup messages immediately.
		defer log.Flush()
		// If anything goes dramatically wrong, use Go's panic/recover
		// mechanism to intercept the panic and log the panic details to
		// the error reporting server.
		defer func() {
			if s != nil {
				// We only attempt to log the panic details if the server has
				// actually been started successfully. If there's no server,
				// we won't know enough to decide whether reporting is
				// permitted.
				log.RecoverAndReportPanic(ctx, &s.ClusterSettings().SV)
			}
		}()
		// When the start up goroutine completes, so can the start up span
		// defined above.
		defer sp.Finish()

		// Any error beyond this point should be reported through the
		// errChan defined above. However, in Go the code pattern "if err
		// != nil { return err }" is more common. Expecting contributors
		// to remember to write "if err != nil { errChan <- err }" beyond
		// this point is optimistic. To avoid any error, we capture all
		// the error returns in a closure, and do the errChan reporting,
		// if needed, when that function returns.
		if err := func() error {
			// Instantiate the server.
			var err error
			s, err = server.NewServer(serverCfg, stopper)
			if err != nil {
				return errors.Wrap(err, "failed to start server")
			}

			// Have we already received a signal to terminate? If so, just
			// stop here.
			serverStatusMu.Lock()
			draining := serverStatusMu.draining
			serverStatusMu.Unlock()
			if draining {
				return nil
			}

			// Attempt to start the server.
			if err := s.Start(ctx); err != nil {
				if le := (*server.ListenError)(nil); errors.As(err, &le) {
					const errorPrefix = "consider changing the port via --%s"
					if le.Addr == serverCfg.Addr {
						err = errors.Wrapf(err, errorPrefix, cliflags.ListenAddr.Name)
					} else if le.Addr == serverCfg.HTTPAddr {
						err = errors.Wrapf(err, errorPrefix, cliflags.ListenHTTPAddr.Name)
					}
				}

				return errors.Wrap(err, "cockroach server exited with error")
			}
			// Server started, notify the shutdown monitor running concurrently.
			serverStatusMu.Lock()
			serverStatusMu.started = true
			serverStatusMu.Unlock()

			// Start up the update check loop.
			// We don't do this in (*server.Server).Start() because we don't want it
			// in tests.
			if !cluster.TelemetryOptOut() {
				s.PeriodicallyCheckForUpdates(ctx)
			}

			initialBoot := s.InitialBoot()

			if disableReplication && initialBoot {
				// For start-single-node, set the default replication factor to
				// 1 so as to avoid warning message and unnecessary rebalance
				// churn.
				if err := cliDisableReplication(ctx, s); err != nil {
					log.Errorf(ctx, "could not disable replication: %v", err)
					return err
				}
				log.Shout(ctx, log.Severity_INFO,
					"Replication was disabled for this cluster.\n"+
						"When/if adding nodes in the future, update zone configurations to increase the replication factor.")
			}

			// Now inform the user that the server is running and tell the
			// user about its run-time derived parameters.
			var buf bytes.Buffer
			info := build.GetInfo()
			tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
			fmt.Fprintf(tw, "CockroachDB node starting at %s (took %0.1fs)\n", timeutil.Now(), timeutil.Since(tBegin).Seconds())
			fmt.Fprintf(tw, "build:\t%s %s @ %s (%s)\n", info.Distribution, info.Tag, info.Time, info.GoVersion)
			fmt.Fprintf(tw, "webui:\t%s\n", serverCfg.AdminURL())

			// (Re-)compute the client connection URL. We cannot do this
			// earlier (e.g. above, in the runStart function) because
			// at this time the address and port have not been resolved yet.
			sCtx := rpc.MakeSecurityContext(serverCfg.Config)
			pgURL, err := sCtx.PGURL(url.User(security.RootUser))
			if err != nil {
				log.Errorf(ctx, "failed computing the URL: %v", err)
				return err
			}
			fmt.Fprintf(tw, "sql:\t%s\n", pgURL)

			fmt.Fprintf(tw, "RPC client flags:\t%s\n", clientFlagsRPC())
			if len(serverCfg.SocketFile) != 0 {
				fmt.Fprintf(tw, "socket:\t%s\n", serverCfg.SocketFile)
			}
			fmt.Fprintf(tw, "logs:\t%s\n", flag.Lookup("log-dir").Value)
			if serverCfg.AuditLogDirName.IsSet() {
				fmt.Fprintf(tw, "SQL audit logs:\t%s\n", serverCfg.AuditLogDirName)
			}
			if serverCfg.Attrs != "" {
				fmt.Fprintf(tw, "attrs:\t%s\n", serverCfg.Attrs)
			}
			if len(serverCfg.Locality.Tiers) > 0 {
				fmt.Fprintf(tw, "locality:\t%s\n", serverCfg.Locality)
			}
			if s.TempDir() != "" {
				fmt.Fprintf(tw, "temp dir:\t%s\n", s.TempDir())
			}
			if ext := s.ClusterSettings().ExternalIODir; ext != "" {
				fmt.Fprintf(tw, "external I/O path: \t%s\n", ext)
			} else {
				fmt.Fprintf(tw, "external I/O path: \t<disabled>\n")
			}
			for i, spec := range serverCfg.Stores.Specs {
				fmt.Fprintf(tw, "store[%d]:\t%s\n", i, spec)
			}
			fmt.Fprintf(tw, "storage engine: \t%s\n", serverCfg.StorageEngine.String())
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

			if baseCfg.ClusterName != "" {
				fmt.Fprintf(tw, "cluster name:\t%s\n", baseCfg.ClusterName)
			}

			// Remember the cluster ID for log file rotation.
			clusterID := s.ClusterID().String()
			log.SetClusterID(clusterID)
			fmt.Fprintf(tw, "clusterID:\t%s\n", clusterID)
			fmt.Fprintf(tw, "nodeID:\t%d\n", nodeID)

			// Collect the formatted string and show it to the user.
			if err := tw.Flush(); err != nil {
				return err
			}
			msg := buf.String()
			log.Infof(ctx, "node startup completed:\n%s", msg)
			if !startCtx.inBackground && !log.LoggingToStderr(log.Severity_INFO) {
				fmt.Print(msg)
			}

			return nil
		}(); err != nil {
			errChan <- err
		}
	}()

	// The remainder of the main function executes concurrently with the
	// start up goroutine started above.
	//
	// It is concerned with determining when the server should stop
	// because the main process is being shut down -- either via a stop
	// message received from `cockroach quit` / `cockroach
	// decommission`, or a signal.

	// We'll want to log any shutdown activity against a separate span.
	shutdownSpan := tracer.StartSpan("server shutdown")
	defer shutdownSpan.Finish()
	shutdownCtx := opentracing.ContextWithSpan(context.Background(), shutdownSpan)

	// returnErr will be populated with the error to use to exit the
	// process (reported to the shell).
	var returnErr error

	stopWithoutDrain := make(chan struct{}) // closed if interrupted very early

	// Block until one of the signals above is received or the stopper
	// is stopped externally (for example, via the quit endpoint).
	select {
	case err := <-errChan:
		// SetSync both flushes and ensures that subsequent log writes are flushed too.
		log.SetSync(true)
		return err

	case <-stopper.ShouldStop():
		// Server is being stopped externally and our job is finished
		// here since we don't know if it's a graceful shutdown or not.
		<-stopper.IsStopped()
		// SetSync both flushes and ensures that subsequent log writes are flushed too.
		log.SetSync(true)
		return nil

	case sig := <-signalCh:
		// We start synchronizing log writes from here, because if a
		// signal was received there is a non-zero chance the sender of
		// this signal will follow up with SIGKILL if the shutdown is not
		// timely, and we don't want logs to be lost.
		log.SetSync(true)

		log.Infof(shutdownCtx, "received signal '%s'", sig)
		switch sig {
		case os.Interrupt:
			// Graceful shutdown after an interrupt should cause the process
			// to terminate with a non-zero exit code; however SIGTERM is
			// "legitimate" and should be acknowledged with a success exit
			// code. So we keep the error state here for later.
			returnErr = &cliError{
				exitCode: 1,
				// INFO because a single interrupt is rather innocuous.
				severity: log.Severity_INFO,
				cause:    errors.New("interrupted"),
			}
			msgDouble := "Note: a second interrupt will skip graceful shutdown and terminate forcefully"
			fmt.Fprintln(os.Stdout, msgDouble)

		case quitSignal:
			log.DumpStacks(shutdownCtx)
		}

		// Start the draining process in a separate goroutine so that it
		// runs concurrently with the timeout check below.
		go func() {
			serverStatusMu.Lock()
			serverStatusMu.draining = true
			drainingIsSafe := serverStatusMu.started
			serverStatusMu.Unlock()

			// drainingIsSafe may have been set in the meantime, but that's ok.
			// In the worst case, we're not draining a Server that has *just*
			// started. Not desirable, but not terrible either.
			if !drainingIsSafe {
				close(stopWithoutDrain)
				return
			}
			// Don't use shutdownCtx because this is in a goroutine that may
			// still be running after shutdownCtx's span has been finished.
			ac := log.AmbientContext{}
			ac.AddLogTag("server drain process", nil)
			drainCtx := ac.AnnotateCtx(context.Background())

			// Perform a graceful drain. We keep retrying forever, in
			// case there are many range leases or some unavailability
			// preventing progress. If the operator wants to expedite
			// the shutdown, they will need to make it ungraceful
			// via a 2nd signal.
			for {
				remaining, _, err := s.Drain(drainCtx)
				if err != nil {
					log.Errorf(drainCtx, "graceful drain failed: %v", err)
					break
				}
				if remaining == 0 {
					// No more work to do.
					break
				}
				// Avoid a busy wait with high CPU usage if the server replies
				// with an incomplete drain too quickly.
				time.Sleep(200 * time.Millisecond)
			}

			stopper.Stop(drainCtx)
		}()

	// Don't return: we're shutting down gracefully.

	case <-log.FatalChan():
		// A fatal error has occurred. Stop everything (gracelessly) to
		// avoid serving incorrect data while the final log messages are
		// being written.
		// https://github.com/cockroachdb/cockroach/issues/23414
		// TODO(bdarnell): This could be more graceless, for example by
		// reaching into the server objects and closing all the
		// connections while they're in use. That would be more in line
		// with the expected effect of a log.Fatal.
		stopper.Stop(shutdownCtx)
		// The logging goroutine is now responsible for killing this
		// process, so just block this goroutine.
		select {}
	}

	// At this point, a signal has been received to shut down the
	// process, and a goroutine is busy telling the server to drain and
	// stop. From this point on, we just have to wait until the server
	// indicates it has stopped.

	const msgDrain = "initiating graceful shutdown of server"
	log.Info(shutdownCtx, msgDrain)
	fmt.Fprintln(os.Stdout, msgDrain)

	// Notify the user every 5 second of the shutdown progress.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Infof(context.Background(), "%d running tasks", stopper.NumTasks())
			case <-stopper.ShouldStop():
				return
			case <-stopWithoutDrain:
				return
			}
		}
	}()

	// Meanwhile, we don't want to wait too long either, in case the
	// server is getting stuck and doesn't shut down in a timely manner.
	//
	// So we also pay attention to any additional signal received beyond
	// this point (maybe some service monitor was impatient and sends
	// another signal to hasten the shutdown process).
	//
	// If any such trigger to hasten occurs, we simply return, which
	// will cause the process to exit and the server goroutines to be
	// forcefully terminated.

	const hardShutdownHint = " - node may take longer to restart & clients may need to wait for leases to expire"
	for {
		select {
		case sig := <-signalCh:
			switch sig {
			case termSignal:
				// Double SIGTERM, or SIGTERM after another signal: continue
				// the graceful shutdown.
				log.Infof(shutdownCtx, "received additional signal '%s'; continuing graceful shutdown", sig)
				continue
			case quitSignal:
				// A SIGQUIT is received during the "graceful shutdown" phase
				// initiated by another signal. Take this as a request to get
				// the stacks at this point.
				log.DumpStacks(shutdownCtx)
			}

			// This new signal is not welcome, as it interferes with the graceful
			// shutdown process.
			log.Shoutf(shutdownCtx, log.Severity_ERROR,
				"received signal '%s' during shutdown, initiating hard shutdown%s",
				log.Safe(sig), log.Safe(hardShutdownHint))
			handleSignalDuringShutdown(sig)
			panic("unreachable")

		case <-stopper.IsStopped():
			const msgDone = "server drained and shutdown completed"
			log.Infof(shutdownCtx, msgDone)
			fmt.Fprintln(os.Stdout, msgDone)

		case <-stopWithoutDrain:
			const msgDone = "too early to drain; used hard shutdown instead"
			log.Infof(shutdownCtx, msgDone)
			fmt.Fprintln(os.Stdout, msgDone)
		}
		break
	}

	return returnErr
}

func hintServerCmdFlags(ctx context.Context, cmd *cobra.Command) {
	pf := flagSetForCmd(cmd)

	listenAddrSpecified := pf.Lookup(cliflags.ListenAddr.Name).Changed || pf.Lookup(cliflags.ServerHost.Name).Changed
	advAddrSpecified := pf.Lookup(cliflags.AdvertiseAddr.Name).Changed || pf.Lookup(cliflags.AdvertiseHost.Name).Changed

	if !listenAddrSpecified && !advAddrSpecified {
		host, _, _ := net.SplitHostPort(serverCfg.AdvertiseAddr)
		log.Shoutf(ctx, log.Severity_WARNING,
			"neither --listen-addr nor --advertise-addr was specified.\n"+
				"The server will advertise %q to other nodes, is this routable?\n\n"+
				"Consider using:\n"+
				"- for local-only servers:  --listen-addr=localhost\n"+
				"- for multi-node clusters: --advertise-addr=<host/IP addr>\n", host)
	}
}

func clientFlagsRPC() string {
	flags := []string{os.Args[0], "<client cmd>"}
	if serverCfg.AdvertiseAddr != "" {
		flags = append(flags, "--host="+serverCfg.AdvertiseAddr)
	}
	if startCtx.serverInsecure {
		flags = append(flags, "--insecure")
	} else {
		flags = append(flags, "--certs-dir="+startCtx.serverSSLCertsDir)
	}
	return strings.Join(flags, " ")
}

func checkTzDatabaseAvailability(ctx context.Context) error {
	if _, err := timeutil.LoadLocation("America/New_York"); err != nil {
		log.Errorf(ctx, "timeutil.LoadLocation: %v", err)
		reportedErr := errors.WithHint(
			errors.WithIssueLink(
				errors.New("unable to load named timezones"),
				errors.IssueLink{IssueURL: unimplemented.MakeURL(36864)}),
			"Check that the time zone database is installed on your system, or\n"+
				"set the ZONEINFO environment variable to a Go time zone .zip archive.")

		if envutil.EnvOrDefaultBool("COCKROACH_INCONSISTENT_TIME_ZONES", false) {
			// The user tells us they really know what they want.
			reportedErr := &formattedError{err: reportedErr}
			log.Shoutf(ctx, log.Severity_WARNING, "%v", reportedErr)
		} else {
			// Prevent a successful start.
			//
			// In the past, we were simply using log.Shout to emit an error,
			// informing the user that startup could continue with degraded
			// behavior.  However, usage demonstrated that users typically do
			// not see the error and instead run into silently incorrect SQL
			// results. To avoid this situation altogether, it's better to
			// stop early.
			return reportedErr
		}
	}
	return nil
}

func reportConfiguration(ctx context.Context) {
	serverCfg.Report(ctx)
	if envVarsUsed := envutil.GetEnvVarsUsed(); len(envVarsUsed) > 0 {
		log.Infof(ctx, "using local environment variables: %s", strings.Join(envVarsUsed, ", "))
	}
	// If a user ever reports "bad things have happened", any
	// troubleshooting steps will want to rule out that the user was
	// running as root in a multi-user environment, or using different
	// uid/gid across runs in the same data directory. To determine
	// this, it's easier if the information appears in the log file.
	log.Infof(ctx, "process identity: %s", sysutil.ProcessIdentity())
}

func maybeWarnMemorySizes(ctx context.Context) {
	// Is the cache configuration OK?
	if !cacheSizeValue.IsSet() {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "Using the default setting for --cache (%s).\n", cacheSizeValue)
		fmt.Fprintf(&buf, "  A significantly larger value is usually needed for good performance.\n")
		if size, err := status.GetTotalMemory(context.Background()); err == nil {
			fmt.Fprintf(&buf, "  If you have a dedicated server a reasonable setting is --cache=.25 (%s).",
				humanizeutil.IBytes(size/4))
		} else {
			fmt.Fprintf(&buf, "  If you have a dedicated server a reasonable setting is 25%% of physical memory.")
		}
		log.Warningf(ctx, "%s", buf.String())
	}

	// Check that the total suggested "max" memory is well below the available memory.
	if maxMemory, err := status.GetTotalMemory(ctx); err == nil {
		requestedMem := serverCfg.CacheSize + serverCfg.MemoryPoolSize
		maxRecommendedMem := int64(.75 * float64(maxMemory))
		if requestedMem > maxRecommendedMem {
			log.Shoutf(ctx, log.Severity_WARNING,
				"the sum of --max-sql-memory (%s) and --cache (%s) is larger than 75%% of total RAM (%s).\nThis server is running at increased risk of memory-related failures.",
				sqlSizeValue, cacheSizeValue, humanizeutil.IBytes(maxRecommendedMem))
		}
	}
}

func logOutputDirectory() string {
	return startCtx.logDir.String()
}

// setupAndInitializeLoggingAndProfiling does what it says on the label.
// Prior to this however it determines suitable defaults for the
// logging output directory and the verbosity level of stderr logging.
// We only do this for the "start" command which is why this work
// occurs here and not in an OnInitialize function.
func setupAndInitializeLoggingAndProfiling(
	ctx context.Context, cmd *cobra.Command,
) (stopper *stop.Stopper, err error) {
	// Default the log directory to the "logs" subdirectory of the first
	// non-memory store. If more than one non-memory stores is detected,
	// print a warning.
	ambiguousLogDirs := false
	lf := cmd.Flags().Lookup(logflags.LogDirName)
	if !startCtx.logDir.IsSet() && !lf.Changed {
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
		if err := startCtx.logDir.Set(newDir); err != nil {
			return nil, err
		}
	}

	if logDir := startCtx.logDir.String(); logDir != "" {
		ls := cockroachCmd.PersistentFlags().Lookup(logflags.LogToStderrName)
		if !ls.Changed {
			// Unless the settings were overridden by the user, silence
			// logging to stderr because the messages will go to a log file.
			if err := ls.Value.Set(log.Severity_NONE.String()); err != nil {
				return nil, err
			}
		}

		// Make sure the path exists.
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return nil, errors.Wrap(err, "unable to create log directory")
		}

		// Note that we configured the --log-dir flag to set
		// startContext.logDir. This is the point at which we set log-dir for the
		// util/log package. We don't want to set it earlier to avoid spuriously
		// creating a file in an incorrect log directory or if something is
		// accidentally logging after flag parsing but before the --background
		// dispatch has occurred.
		if err := flag.Lookup(logflags.LogDirName).Value.Set(logDir); err != nil {
			return nil, err
		}

		// Start the log file GC daemon to remove files that make the log
		// directory too large.
		log.StartGCDaemon(ctx)

		defer func() {
			if stopper != nil {
				// When the function complete successfully, start the loggers
				// for the storage engines. We need to do this at the end
				// because we need to register the loggers.
				stopper.AddCloser(storage.InitPebbleLogger(ctx))
				stopper.AddCloser(storage.InitRocksDBLogger(ctx))
			}
		}()
	}

	// Initialize the redirection of stderr and log redaction.  Note,
	// this function must be called even if there is no log directory
	// configured, to verify whether the combination of requested flags
	// is valid.
	if _, err := log.SetupRedactionAndStderrRedirects(); err != nil {
		return nil, err
	}

	// We want to be careful to still produce useful debug dumps if the
	// server configuration has disabled logging to files.
	outputDirectory := "."
	if p := logOutputDirectory(); p != "" {
		outputDirectory = p
	}
	serverCfg.GoroutineDumpDirName = filepath.Join(outputDirectory, base.GoroutineDumpDir)
	serverCfg.HeapProfileDirName = filepath.Join(outputDirectory, base.HeapProfileDir)

	if ambiguousLogDirs {
		// Note that we can't report this message earlier, because the log directory
		// may not have been ready before the call to MkdirAll() above.
		log.Shout(ctx, log.Severity_WARNING, "multiple stores configured"+
			" and --log-dir not specified, you may want to specify --log-dir to disambiguate.")
	}

	if auditLogDir := serverCfg.AuditLogDirName.String(); auditLogDir != "" && auditLogDir != outputDirectory {
		// Make sure the path for the audit log exists, if it's a different path than
		// the main log.
		if err := os.MkdirAll(auditLogDir, 0755); err != nil {
			return nil, err
		}
		log.Eventf(ctx, "created SQL audit log directory %s", auditLogDir)
	}

	if startCtx.serverInsecure {
		// Use a non-annotated context here since the annotation just looks funny,
		// particularly to new users (made worse by it always printing as [n?]).
		addr := startCtx.serverListenAddr
		if addr == "" {
			addr = "<all your IP addresses>"
		}
		log.Shoutf(context.Background(), log.Severity_WARNING,
			"RUNNING IN INSECURE MODE!\n\n"+
				"- Your cluster is open for any client that can access %s.\n"+
				"- Any user, even root, can log in without providing a password.\n"+
				"- Any user, connecting as root, can read or write any data in your cluster.\n"+
				"- There is no network encryption nor authentication, and thus no confidentiality.\n\n"+
				"Check out how to secure your cluster: %s",
			addr, log.Safe(base.DocsURL("secure-a-cluster.html")))
	}

	maybeWarnMemorySizes(ctx)

	// We log build information to stdout (for the short summary), but also
	// to stderr to coincide with the full logs.
	info := build.GetInfo()
	log.Infof(ctx, "%s", info.Short())

	initMemProfile(ctx, outputDirectory)
	initCPUProfile(ctx, outputDirectory)
	initBlockProfile()
	initMutexProfile()

	// Disable Stopper task tracking as performing that call site tracking is
	// moderately expensive (certainly outweighing the infrequent benefit it
	// provides).
	stopper = stop.NewStopper()
	log.Event(ctx, "initialized profiles")

	return stopper, nil
}

func addrWithDefaultHost(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	if host == "" {
		host = "localhost"
	}
	return net.JoinHostPort(host, port), nil
}

// getClientGRPCConn returns a ClientConn, a Clock and a method that blocks
// until the connection (and its associated goroutines) have terminated.
func getClientGRPCConn(
	ctx context.Context, cfg server.Config,
) (*grpc.ClientConn, *hlc.Clock, func(), error) {
	if ctx.Done() == nil {
		return nil, nil, nil, errors.New("context must be cancellable")
	}
	// 0 to disable max offset checks; this RPC context is not a member of the
	// cluster, so there's no need to enforce that its max offset is the same
	// as that of nodes in the cluster.
	clock := hlc.NewClock(hlc.UnixNano, 0)
	stopper := stop.NewStopper()
	rpcContext := rpc.NewContext(rpc.ContextOptions{
		AmbientCtx: log.AmbientContext{Tracer: cfg.Settings.Tracer},
		Config:     cfg.Config,
		Clock:      clock,
		Stopper:    stopper,
		Settings:   cfg.Settings,
	})
	addr, err := addrWithDefaultHost(cfg.AdvertiseAddr)
	if err != nil {
		stopper.Stop(ctx)
		return nil, nil, nil, err
	}
	// We use GRPCUnvalidatedDial() here because it does not matter
	// to which node we're talking to.
	conn, err := rpcContext.GRPCUnvalidatedDial(addr).Connect(ctx)
	if err != nil {
		stopper.Stop(ctx)
		return nil, nil, nil, err
	}
	stopper.AddCloser(stop.CloserFn(func() {
		_ = conn.Close()
	}))

	// Tie the lifetime of the stopper to that of the context.
	closer := func() {
		stopper.Stop(ctx)
	}
	return conn, clock, closer, nil
}
