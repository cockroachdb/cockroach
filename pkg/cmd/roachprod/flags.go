// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/util/flagutil"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var (
	// Do not populate providerOptsContainer here as we need to call InitProivders() first.
	providerOptsContainer vm.ProviderOptionsContainer
	pprofOpts             roachprod.PprofOpts
	numNodes              int
	numRacks              int
	username              string
	dryrun                bool
	destroyAllMine        bool
	destroyAllLocal       bool
	extendLifetime        time.Duration
	wipePreserveCerts     bool
	grafanaConfig         string
	grafanaurlOpen        bool
	grafanaDumpDir        string
	listDetails           bool
	listJSON              bool
	listMine              bool
	listPattern           string
	secure                = false
	tenantName            string
	extraSSHOptions       = ""
	nodeEnv               []string
	tag                   string
	external              = false
	pgurlCertsDir         string
	adminurlOpen          = false
	adminurlPath          = ""
	adminurlIPs           = false
	useTreeDist           = true
	sig                   = 9
	waitFlag              = false
	maxWait               = 0
	createVMOpts          = vm.DefaultCreateOpts()
	startOpts             = roachprod.DefaultStartOpts()
	stageOS               string
	stageDir              string
	logsDir               string
	logsFilter            string
	logsProgramFilter     string
	logsFrom              time.Time
	logsTo                time.Time
	logsInterval          time.Duration
	volumeCreateOpts      vm.VolumeCreateOpts
	listOpts              vm.ListOptions

	monitorOpts        install.MonitorOpts
	cachedHostsCluster string

	// hostCluster is used for multi-tenant functionality.
	hostCluster string
)

func initFlags() {
	rootCmd.PersistentFlags().BoolVarP(&config.Quiet, "quiet", "q",
		false || !term.IsTerminal(int(os.Stdout.Fd())), "disable fancy progress output")
	rootCmd.PersistentFlags().IntVarP(&config.MaxConcurrency, "max-concurrency", "", 32,
		"maximum number of operations to execute on nodes concurrently, set to zero for infinite",
	)

	createCmd.Flags().DurationVarP(&createVMOpts.Lifetime,
		"lifetime", "l", 12*time.Hour, "Lifetime of the cluster")
	createCmd.Flags().BoolVar(&createVMOpts.SSDOpts.UseLocalSSD,
		"local-ssd", true, "Use local SSD")
	createCmd.Flags().StringVar(&createVMOpts.SSDOpts.FileSystem,
		"filesystem", vm.Ext4, "The underlying file system(ext4/zfs). ext4 is used by default.")
	createCmd.Flags().BoolVar(&createVMOpts.SSDOpts.NoExt4Barrier,
		"local-ssd-no-ext4-barrier", true,
		`Mount the local SSD with the "-o nobarrier" flag. Ignored if --local-ssd=false is specified.`)
	createCmd.Flags().IntVarP(&numNodes,
		"nodes", "n", 4, "Total number of nodes, distributed across all clouds")
	createCmd.Flags().IntVarP(&createVMOpts.OsVolumeSize,
		"os-volume-size", "", 10, "OS disk volume size in GB")
	createCmd.Flags().StringSliceVarP(&createVMOpts.VMProviders,
		"clouds", "c", []string{gce.ProviderName},
		fmt.Sprintf(
			"The cloud provider(s) to use when creating new vm instances: %s",
			vm.AllProviderNames()))
	createCmd.Flags().BoolVar(&createVMOpts.GeoDistributed,
		"geo", false, "Create geo-distributed cluster")
	// N.B. We set "usage=roachprod" as the default, custom label for billing tracking.
	createCmd.Flags().StringToStringVar(&createVMOpts.CustomLabels,
		"label", map[string]string{"usage": "roachprod"},
		"The label(s) to be used when creating new vm instances, must be in '--label name=value' format "+
			"and value can't be empty string after trimming space, a value that has space must be quoted by single "+
			"quotes, gce label name only allows hyphens (-), underscores (_), lowercase characters, numbers and "+
			"international characters. Examples: usage=cloud-report-2021, namewithspaceinvalue='s o s'")

	// Allow each Provider to inject additional configuration flags
	for _, providerName := range vm.AllProviderNames() {
		if vm.Providers[providerName].Active() {
			providerOptsContainer[providerName].ConfigureCreateFlags(createCmd.Flags())

			for _, cmd := range []*cobra.Command{
				destroyCmd, extendCmd, listCmd, syncCmd, gcCmd,
			} {
				providerOptsContainer[providerName].ConfigureClusterFlags(cmd.Flags(), vm.AcceptMultipleProjects)
			}

			// createCmd only accepts a single GCE project, as opposed to all the other
			// commands.
			providerOptsContainer[providerName].ConfigureClusterFlags(createCmd.Flags(), vm.SingleProject)
		}
	}

	destroyCmd.Flags().BoolVarP(&destroyAllMine,
		"all-mine", "m", false, "Destroy all non-local clusters belonging to the current user")
	destroyCmd.Flags().BoolVarP(&destroyAllLocal,
		"all-local", "l", false, "Destroy all local clusters")

	extendCmd.Flags().DurationVarP(&extendLifetime,
		"lifetime", "l", 12*time.Hour, "Lifetime of the cluster")

	listCmd.Flags().BoolVarP(&listDetails,
		"details", "d", false, "Show cluster details")
	listCmd.Flags().BoolVar(&listJSON,
		"json", false, "Show cluster specs in a json format")
	listCmd.Flags().BoolVarP(&listMine,
		"mine", "m", false, "Show only clusters belonging to the current user")
	listCmd.Flags().StringVar(&listPattern,
		"pattern", "", "Show only clusters matching the regex pattern. Empty string matches everything.")

	adminurlCmd.Flags().BoolVar(&adminurlOpen, "open", false, "Open the url in a browser")
	adminurlCmd.Flags().StringVar(&adminurlPath,
		"path", "/", "Path to add to URL (e.g. to open a same page on each node)")
	adminurlCmd.Flags().BoolVar(&adminurlIPs,
		"ips", false, `Use Public IPs instead of DNS names in URL`)

	gcCmd.Flags().BoolVarP(&dryrun,
		"dry-run", "n", dryrun, "dry run (don't perform any actions)")
	gcCmd.Flags().StringVar(&config.SlackToken, "slack-token", "", "Slack bot token")

	pgurlCmd.Flags().BoolVar(&external,
		"external", false, "return pgurls for external connections")
	pgurlCmd.Flags().StringVar(&pgurlCertsDir,
		"certs-dir", "./certs", "cert dir to use for secure connections")

	pprofCmd.Flags().DurationVar(&pprofOpts.Duration,
		"duration", 30*time.Second, "Duration of profile to capture")
	pprofCmd.Flags().BoolVar(&pprofOpts.Heap,
		"heap", false, "Capture a heap profile instead of a CPU profile")
	pprofCmd.Flags().BoolVar(&pprofOpts.Open,
		"open", false, "Open the profile using `go tool pprof -http`")
	pprofCmd.Flags().IntVar(&pprofOpts.StartingPort,
		"starting-port", 9000, "Initial port to use when opening pprof's HTTP interface")

	ipCmd.Flags().BoolVar(&external,
		"external", false, "return external IP addresses")

	runCmd.Flags().StringVarP(&extraSSHOptions,
		"ssh-options", "O", "", "extra args to pass to ssh")

	startCmd.Flags().IntVarP(&numRacks,
		"racks", "r", 0, "the number of racks to partition the nodes into")
	startCmd.Flags().StringArrayVarP(&startOpts.ExtraArgs,
		"args", "a", nil, "node arguments")
	startCmd.Flags().StringArrayVarP(&nodeEnv,
		"env", "e", config.DefaultEnvVars(), "node environment variables")
	startCmd.Flags().BoolVar(&startOpts.EncryptedStores,
		"encrypt", startOpts.EncryptedStores, "start nodes with encryption at rest turned on")
	startCmd.Flags().BoolVar(&startOpts.SkipInit,
		"skip-init", startOpts.SkipInit, "skip initializing the cluster")
	startCmd.Flags().IntVar(&startOpts.InitTarget,
		"init-target", startOpts.InitTarget, "node on which to run initialization")
	startCmd.Flags().IntVar(&startOpts.StoreCount,
		"store-count", startOpts.StoreCount, "number of stores to start each node with")
	startCmd.Flags().BoolVar(&startOpts.ScheduleBackups,
		"schedule-backups", startOpts.ScheduleBackups,
		"create a cluster backup schedule once the cluster has started (by default, "+
			"full backup hourly and incremental every 15 minutes)")
	startCmd.Flags().StringVar(&startOpts.ScheduleBackupArgs, "schedule-backup-args", "",
		`Recurrence and scheduled backup options specification.
Default is "RECURRING '*/15 * * * *' FULL BACKUP '@hourly' WITH SCHEDULE OPTIONS first_run = 'now'"`)

	startTenantCmd.Flags().StringVarP(&hostCluster,
		"host-cluster", "H", "", "host cluster")
	_ = startTenantCmd.MarkFlagRequired("host-cluster")
	startTenantCmd.Flags().IntVarP(&startOpts.TenantID,
		"tenant-id", "t", startOpts.TenantID, "tenant ID")

	stopCmd.Flags().IntVar(&sig, "sig", sig, "signal to pass to kill")
	stopCmd.Flags().BoolVar(&waitFlag, "wait", waitFlag, "wait for processes to exit")
	stopCmd.Flags().IntVar(&maxWait, "max-wait", maxWait, "approx number of seconds to wait for processes to exit")

	syncCmd.Flags().BoolVar(&listOpts.IncludeVolumes, "include-volumes", false, "Include volumes when syncing")

	wipeCmd.Flags().BoolVar(&wipePreserveCerts, "preserve-certs", false, "do not wipe certificates")

	putCmd.Flags().BoolVar(&useTreeDist, "treedist", useTreeDist, "use treedist copy algorithm")

	stageCmd.Flags().StringVar(&stageOS, "os", "", "operating system override for staged binaries")
	stageCmd.Flags().StringVar(&stageDir, "dir", "", "destination for staged binaries")

	stageURLCmd.Flags().StringVar(&stageOS, "os", "", "operating system override for staged binaries")

	logsCmd.Flags().StringVar(&logsFilter,
		"filter", "", "re to filter log messages")
	logsCmd.Flags().Var(flagutil.Time(&logsFrom),
		"from", "time from which to stream logs")
	logsCmd.Flags().Var(flagutil.Time(&logsTo),
		"to", "time to which to stream logs")
	logsCmd.Flags().DurationVar(&logsInterval,
		"interval", 200*time.Millisecond, "interval to poll logs from host")
	logsCmd.Flags().StringVar(&logsDir,
		"logs-dir", "logs", "path to the logs dir, if remote, relative to username's home dir, ignored if local")
	logsCmd.Flags().StringVar(&logsProgramFilter,
		"logs-program", "^cockroach$", "regular expression of the name of program in log files to search")

	monitorCmd.Flags().BoolVar(&monitorOpts.IgnoreEmptyNodes,
		"ignore-empty-nodes", false,
		"Automatically detect the (subset of the given) nodes which to monitor "+
			"based on the presence of a nontrivial data directory.")

	monitorCmd.Flags().BoolVar(&monitorOpts.OneShot,
		"oneshot", false,
		"Report the status of all targeted nodes once, then exit. The exit "+
			"status is nonzero if (and only if) any node was found not running.")

	cachedHostsCmd.Flags().StringVar(&cachedHostsCluster,
		"cluster", "", "print hosts matching cluster")

	grafanaStartCmd.Flags().StringVar(&grafanaConfig,
		"grafana-config", "", "URI to grafana json config, supports local and http(s) schemes")

	grafanaURLCmd.Flags().BoolVar(&grafanaurlOpen,
		"open", false, "open the grafana dashboard url on the browser")

	grafanaDumpCmd.Flags().StringVar(&grafanaDumpDir, "dump-dir", "",
		"the absolute path to dump prometheus data to (use the contained 'prometheus-docker-run.sh' to visualize")

	initCmd.Flags().IntVar(&startOpts.InitTarget,
		"init-target", startOpts.InitTarget, "node on which to run initialization")

	rootStorageCmd.AddCommand(rootStorageCollectionCmd)
	rootStorageCollectionCmd.AddCommand(collectionStartCmd)
	rootStorageCollectionCmd.AddCommand(collectionStopCmd)
	rootStorageCollectionCmd.AddCommand(storageSnapshotCmd)
	rootStorageCollectionCmd.AddCommand(collectionListVolumes)
	collectionStartCmd.Flags().IntVarP(&volumeCreateOpts.Size,
		"volume-size", "s", 10,
		"the size of the volume in Gigabytes (GB) to create for each store. Note: This volume will be deleted "+
			"once the VM is deleted.")

	collectionStartCmd.Flags().BoolVar(&volumeCreateOpts.Encrypted,
		"volume-encrypted", false,
		"determines if the volume will be encrypted. Note: This volume will be deleted once the VM is deleted.")

	collectionStartCmd.Flags().StringVar(&volumeCreateOpts.Architecture,
		"volume-arch", "",
		"the architecture the volume should target. This flag is only relevant for gcp or azure. It is ignored "+
			"if supplied for other providers. Note: This volume will be deleted once the VM is deleted.")

	collectionStartCmd.Flags().IntVarP(&volumeCreateOpts.IOPS,
		"volume-iops", "i", 0,
		"the iops to provision for the volume. Note: This volume will be deleted once the VM is deleted.")

	collectionStartCmd.Flags().StringVarP(&volumeCreateOpts.Type,
		"volume-type", "t", "",
		"the volume type that should be created. Provide a volume type that is connected to"+
			" the provider chosen for the cluster. If no volume type is provided the provider default will be used. "+
			"Note: This volume will be deleted once the VM is deleted.")

	for _, cmd := range []*cobra.Command{createCmd, destroyCmd, extendCmd, logsCmd} {
		cmd.Flags().StringVarP(&username, "username", "u", os.Getenv("ROACHPROD_USER"),
			"Username to run under, detect if blank")
	}

	for _, cmd := range []*cobra.Command{statusCmd, monitorCmd, startCmd,
		stopCmd, runCmd, wipeCmd, reformatCmd, installCmd, putCmd, getCmd,
		sqlCmd, pgurlCmd, adminurlCmd, ipCmd,
	} {
		cmd.Flags().BoolVar(
			&ssh.InsecureIgnoreHostKey, "insecure-ignore-host-key", true, "don't check ssh host keys")
	}

	for _, cmd := range []*cobra.Command{startCmd, startTenantCmd} {
		cmd.Flags().BoolVar(&startOpts.Sequential,
			"sequential", startOpts.Sequential, "start nodes sequentially so node IDs match hostnames")
		cmd.Flags().Int64Var(&startOpts.NumFilesLimit, "num-files-limit", startOpts.NumFilesLimit,
			"limit the number of files that can be created by the cockroach process")
	}

	for _, cmd := range []*cobra.Command{
		startCmd, statusCmd, stopCmd, runCmd,
	} {
		cmd.Flags().StringVar(&tag, "tag", "", "the process tag")
	}

	for _, cmd := range []*cobra.Command{
		startCmd, putCmd, getCmd,
	} {
		cmd.Flags().BoolVar(new(bool), "scp", false, "DEPRECATED")
		_ = cmd.Flags().MarkDeprecated("scp", "always true")
	}

	for _, cmd := range []*cobra.Command{startCmd, sqlCmd} {
		cmd.Flags().StringVarP(&config.Binary,
			"binary", "b", config.Binary, "the remote cockroach binary to use")
	}
	for _, cmd := range []*cobra.Command{startCmd, startTenantCmd, sqlCmd, pgurlCmd, adminurlCmd, runCmd} {
		cmd.Flags().BoolVar(&secure,
			"secure", false, "use a secure cluster")
	}
	for _, cmd := range []*cobra.Command{pgurlCmd, sqlCmd} {
		cmd.Flags().StringVar(&tenantName,
			"tenant-name", "", "specific tenant to connect to")
	}

}
