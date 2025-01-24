// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/fluentbit"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/opentelemetry"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/flagutil"
	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"
	"golang.org/x/term"
)

var (
	// Do not populate providerOptsContainer here as we need to call InitProivders() first.
	providerOptsContainer vm.ProviderOptionsContainer
	pprofOpts             roachprod.PprofOpts
	numNodes              int
	numRacks              int
	username              string
	database              string
	dryrun                bool
	destroyAllMine        bool
	destroyAllLocal       bool
	extendLifetime        time.Duration
	wipePreserveCerts     bool
	grafanaConfig         string
	grafanaArch           string
	grafanaDumpDir        string
	jaegerConfigNodes     string
	listCost              bool
	listDetails           bool
	listJSON              bool
	listMine              bool
	listPattern           string
	isSecure              bool   // Set based on the values passed to --secure and --insecure
	secure                = true // DEPRECATED
	insecure              = envutil.EnvOrDefaultBool("COCKROACH_ROACHPROD_INSECURE", false)
	virtualClusterName    string
	sqlInstance           int
	extraSSHOptions       = ""
	nodeEnv               []string
	tag                   string
	external              = false
	pgurlCertsDir         string
	authMode              string
	adminurlPath          = ""
	adminurlIPs           = false
	urlOpen               = false
	useTreeDist           = true
	sig                   = 9
	waitFlag              = false
	gracePeriod           = 0
	deploySig             = 15
	deployWaitFlag        = true
	deployGracePeriod     = 300
	pause                 = time.Duration(0)
	createVMOpts          = vm.DefaultCreateOpts()
	startOpts             = roachprod.DefaultStartOpts()
	stageOS               string
	stageArch             string
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

	// storageCluster is used for cluster virtualization and multi-tenant functionality.
	storageCluster string

	roachprodUpdateRevert bool
	roachprodUpdateBranch string
	roachprodUpdateOS     string
	roachprodUpdateArch   string

	grafanaTags         []string
	grafanaDashboardUID string
	grafanaTimeRange    []int64

	sshKeyUser string

	fluentBitConfig fluentbit.Config

	opentelemetryConfig opentelemetry.Config

	fetchLogsTimeout time.Duration
)

func initRootCmdFlags(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().BoolVarP(&config.Quiet, "quiet", "q",
		!term.IsTerminal(int(os.Stdout.Fd())), "disable fancy progress output")
	rootCmd.PersistentFlags().IntVarP(&config.MaxConcurrency, "max-concurrency", "", 32,
		"maximum number of operations to execute on nodes concurrently, set to zero for infinite",
	)
	rootCmd.PersistentFlags().BoolVarP(&config.FastDNS, "fast-dns", "",
		term.IsTerminal(int(os.Stdout.Fd())), "enable fast DNS resolution via the standard Go net package")
	rootCmd.PersistentFlags().StringVarP(&config.EmailDomain, "email-domain", "",
		config.DefaultEmailDomain, "email domain for users")
	rootCmd.PersistentFlags().BoolVar(&config.UseSharedUser,
		"use-shared-user", true,
		fmt.Sprintf("use the shared user %q for ssh rather than your user %q",
			config.SharedUser, config.OSUser.Username))
}

func initCreateCmdFlags(createCmd *cobra.Command) {
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
	createCmd.Flags().StringVar(&createVMOpts.Arch, "arch", "",
		"architecture override for VM [amd64, arm64, fips]; N.B. fips implies amd64 with openssl")

	// N.B. We set "usage=roachprod" as the default, custom label for billing tracking.
	createCmd.Flags().StringToStringVar(&createVMOpts.CustomLabels,
		"label", map[string]string{"usage": "roachprod"},
		"The label(s) to be used when creating new vm instances, must be in '--label name=value' format "+
			"and value can't be empty string after trimming space, a value that has space must be quoted by single "+
			"quotes, gce label name only allows hyphens (-), underscores (_), lowercase characters, numbers and "+
			"international characters. Examples: usage=cloud-report-2021, namewithspaceinvalue='s o s'")

	// Allow each Provider to inject additional configuration flags
	for _, providerName := range vm.AllProviderNames() {
		provider := vm.Providers[providerName]
		if provider.Active() {
			providerOptsContainer[providerName].ConfigureCreateFlags(createCmd.Flags())
			// createCmd only accepts a single GCE project, as opposed to all the other
			// commands.
			provider.ConfigureProviderFlags(createCmd.Flags(), vm.SingleProject)
		}
	}
}

func initClusterFlagsForMultiProjects(
	rootCmd *cobra.Command, excludeFromClusterFlagsMulti []*cobra.Command,
) {
	for _, providerName := range vm.AllProviderNames() {
		provider := vm.Providers[providerName]
		if provider.Active() {
			for _, cmd := range rootCmd.Commands() {
				excludeCmd := false
				for _, c := range excludeFromClusterFlagsMulti {
					if cmd == c {
						excludeCmd = true
						break
					}
				}
				if excludeCmd {
					continue
				}
				provider.ConfigureProviderFlags(cmd.Flags(), vm.AcceptMultipleProjects)
			}
		}
	}
}

func initDestroyCmdFlags(destroyCmd *cobra.Command) {
	destroyCmd.Flags().BoolVarP(&destroyAllMine,
		"all-mine", "m", false, "Destroy all non-local clusters belonging to the current user")
	destroyCmd.Flags().BoolVarP(&destroyAllLocal,
		"all-local", "l", false, "Destroy all local clusters")
}

func initListCmdFlags(listCmd *cobra.Command) {
	listCmd.Flags().BoolVarP(&listCost,
		"cost", "c", os.Getenv("ROACHPROD_COST_ESTIMATES") == "true",
		"Show cost estimates",
	)
	listCmd.Flags().BoolVarP(&listDetails,
		"details", "d", false, "Show cluster details")
	listCmd.Flags().BoolVar(&listJSON,
		"json", false, "Show cluster specs in a json format")
	listCmd.Flags().BoolVarP(&listMine,
		"mine", "m", false, "Show only clusters belonging to the current user")
	listCmd.Flags().StringVar(&listPattern,
		"pattern", "", "Show only clusters matching the regex pattern. Empty string matches everything.")
}

func initAdminurlCmdFlags(adminurlCmd *cobra.Command) {
	adminurlCmd.Flags().StringVar(&adminurlPath,
		"path", "/", "Path to add to URL (e.g. to open a same page on each node)")
	adminurlCmd.Flags().BoolVar(&adminurlIPs,
		"ips", false, `Use Public IPs instead of DNS names in URL`)
}

func initPprofCmdFlags(pprofCmd *cobra.Command) {
	pprofCmd.Flags().DurationVar(&pprofOpts.Duration,
		"duration", 30*time.Second, "Duration of profile to capture")
	pprofCmd.Flags().BoolVar(&pprofOpts.Heap,
		"heap", false, "Capture a heap profile instead of a CPU profile")
	pprofCmd.Flags().BoolVar(&pprofOpts.Open,
		"open", false, "Open the profile using `go tool pprof -http`")
	pprofCmd.Flags().IntVar(&pprofOpts.StartingPort,
		"starting-port", 9000, "Initial port to use when opening pprof's HTTP interface")

}

func initStartCmdFlags(startCmd *cobra.Command) {
	startCmd.Flags().IntVarP(&numRacks,
		"racks", "r", 0, "the number of racks to partition the nodes into")
	startCmd.Flags().StringArrayVarP(&startOpts.ExtraArgs,
		"args", "a", nil, `node arguments (example: --args "--cache=25%" --args "--max-sql-memory=25%")`)
	startCmd.Flags().StringArrayVarP(&nodeEnv,
		"env", "e", config.DefaultEnvVars(), "node environment variables")
	startCmd.Flags().BoolVar(&startOpts.EncryptedStores,
		"encrypt", startOpts.EncryptedStores, "start nodes with encryption at rest turned on")
	startCmd.Flags().BoolVar(&startOpts.SkipInit,
		"skip-init", startOpts.SkipInit, "skip initializing the cluster")
	startCmd.Flags().BoolVar(&startOpts.IsRestart,
		"restart", startOpts.IsRestart, "restart an existing cluster (skips serial start and init)")
	startCmd.Flags().IntVar(&startOpts.InitTarget,
		"init-target", startOpts.InitTarget, "node on which to run initialization")
	startCmd.Flags().IntVar(&startOpts.StoreCount,
		"store-count", startOpts.StoreCount, "number of stores to start each node with")
	startCmd.Flags().IntVar(&startOpts.AdminUIPort,
		"admin-ui-port", startOpts.AdminUIPort, "port to serve the admin UI on")
}

func initStartInstanceCmdFlags(startInstanceCmd *cobra.Command) {
	startInstanceCmd.Flags().StringVarP(&storageCluster, "storage-cluster", "S", "", "storage cluster")
	_ = startInstanceCmd.MarkFlagRequired("storage-cluster")
	startInstanceCmd.Flags().IntVar(&startOpts.SQLInstance,
		"sql-instance", 0, "specific SQL/HTTP instance to connect to (this is a roachprod abstraction for separate-process deployments distinct from the internal instance ID)")
	startInstanceCmd.Flags().StringVar(&startOpts.VirtualClusterLocation, "external-nodes", startOpts.VirtualClusterLocation, "if set, starts service in external mode, as a separate process in the given nodes")
}

func initSyncCmdFlags(syncCmd *cobra.Command) {
	syncCmd.Flags().BoolVar(&listOpts.IncludeVolumes, "include-volumes", false, "Include volumes when syncing")
	syncCmd.Flags().StringArrayVarP(&listOpts.IncludeProviders, "clouds", "c",
		make([]string, 0), "Specify the cloud providers when syncing. Important: Use this flag only if you are certain that you want to sync with a specific cloud. All DNS host entries for other clouds will be erased from the DNS zone.")

}

func initStageCmdFlags(stageCmd *cobra.Command) {
	stageCmd.Flags().StringVar(&stageOS, "os", "", "operating system override for staged binaries")
	stageCmd.Flags().StringVar(&stageArch, "arch", "",
		"architecture override for staged binaries [amd64, arm64, fips]; N.B. fips implies amd64 with openssl")

	stageCmd.Flags().StringVar(&stageDir, "dir", "", "destination for staged binaries")

}

func initLogsCmdFlags(logsCmd *cobra.Command) {
	logsCmd.Flags().StringVar(&logsFilter,
		"filter", "", "re to filter log messages")
	logsCmd.Flags().Var(flagutil.Time(&logsFrom),
		"from", "time from which to stream logs (e.g., 2024-09-07T16:05:06Z)")
	logsCmd.Flags().Var(flagutil.Time(&logsTo),
		"to", "time to which to stream logs (e.g., 2024-09-07T17:05:06Z); if ommitted, command streams without returning")
	logsCmd.Flags().DurationVar(&logsInterval,
		"interval", 200*time.Millisecond, "interval to poll logs from host")
	logsCmd.Flags().StringVar(&logsDir,
		"logs-dir", "logs", "path to the logs dir, if remote, relative to username's home dir, ignored if local")
	logsCmd.Flags().StringVar(&logsProgramFilter,
		"logs-program", "^cockroach$", "regular expression of the name of program in log files to search")

}

func initStageURLCmdFlags(stageURLCmd *cobra.Command) {
	// N.B. stageURLCmd just prints the URL that stageCmd would use.
	stageURLCmd.Flags().StringVar(&stageOS, "os", "", "operating system override for staged binaries")
	stageURLCmd.Flags().StringVar(&stageArch, "arch", "",
		"architecture override for staged binaries [amd64, arm64, fips]; N.B. fips implies amd64 with openssl")
}

func initMonitorCmdFlags(monitorCmd *cobra.Command) {
	monitorCmd.Flags().BoolVar(&monitorOpts.OneShot,
		"oneshot", false,
		"Report the status of all targeted nodes once, then exit. The exit "+
			"status is nonzero if (and only if) any node was found not running.")
}

func initUpdateCmdFlags(updateCmd *cobra.Command) {
	updateCmd.Flags().BoolVar(&roachprodUpdateRevert, "revert", false, "restore roachprod to the previous version "+
		"which would have been renamed to roachprod.bak during the update process")
	updateCmd.Flags().StringVarP(&roachprodUpdateBranch, "branch", "b", "master", "git branch")
	updateCmd.Flags().StringVarP(&roachprodUpdateOS, "os", "o", "linux", "OS")
	updateCmd.Flags().StringVarP(&roachprodUpdateArch, "arch", "a", "amd64", "CPU architecture")
}

func initGrafanaAnnotationCmdFlags(grafanaAnnotationCmd *cobra.Command) {
	grafanaAnnotationCmd.Flags().StringArrayVar(&grafanaTags,
		"tags", []string{}, "grafana annotation tags")
	grafanaAnnotationCmd.Flags().StringVar(&grafanaDashboardUID,
		"dashboard-uid", "", "grafana dashboard UID")
	grafanaAnnotationCmd.Flags().Int64SliceVar(&grafanaTimeRange,
		"time-range", []int64{}, "grafana annotation time range in epoch time")

}

func initCollectionStartCmdFlags(collectionStartCmd *cobra.Command) {
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
}

func initGrafanaStartCmdFlags(grafanaStartCmd *cobra.Command) {
	grafanaStartCmd.Flags().StringVar(&grafanaConfig,
		"grafana-config", "", "URI to grafana json config, supports local and http(s) schemes")

	grafanaStartCmd.Flags().StringVar(&grafanaArch, "arch", "",
		"binary architecture override [amd64, arm64]")
}

func initFluentBitStartCmdFlags(fluentBitStartCmd *cobra.Command) {
	fluentBitStartCmd.Flags().StringVar(&fluentBitConfig.DatadogSite, "datadog-site", "us5.datadoghq.com",
		"Datadog site to send telemetry data to (e.g., us5.datadoghq.com)")

	fluentBitStartCmd.Flags().StringVar(&fluentBitConfig.DatadogAPIKey, "datadog-api-key", "",
		"Datadog API key")

	fluentBitStartCmd.Flags().StringVar(&fluentBitConfig.DatadogService, "datadog-service", "cockroachdb",
		"Datadog service name for emitted logs")

	fluentBitStartCmd.Flags().StringSliceVar(&fluentBitConfig.DatadogTags, "datadog-tags", []string{},
		"Datadog tags as a comma-separated list in the format KEY1:VAL1,KEY2:VAL2")

}

func initOpentelemetryStartCmdFlags(opentelemetryStartCmd *cobra.Command) {
	opentelemetryStartCmd.Flags().StringVar(&opentelemetryConfig.DatadogSite, "datadog-site", "us5.datadoghq.com",
		"Datadog site to send telemetry data to (e.g., us5.datadoghq.com)")

	opentelemetryStartCmd.Flags().StringVar(&opentelemetryConfig.DatadogAPIKey, "datadog-api-key", "",
		"Datadog API key")

	opentelemetryStartCmd.Flags().StringSliceVar(&opentelemetryConfig.DatadogTags, "datadog-tags", []string{},
		"Datadog tags as a comma-separated list in the format KEY1:VAL1,KEY2:VAL2")
}

func initGCCmdFlags(gcCmd *cobra.Command) {
	gcCmd.Flags().BoolVarP(&dryrun,
		"dry-run", "n", dryrun, "dry run (don't perform any actions)")
	gcCmd.Flags().StringVar(&config.SlackToken, "slack-token", "", "Slack bot token")
	// Allow each Provider to inject additional configuration flags
	for _, provider := range vm.Providers {
		if provider.Active() {
			provider.ConfigureClusterCleanupFlags(gcCmd.Flags())
		}
	}
}

func initFlagPgurlCertsDirForCmd(cmd *cobra.Command) {
	cmd.Flags().StringVar(&pgurlCertsDir,
		"certs-dir", install.CockroachNodeCertsDir, "cert dir to use for secure connections")
}

func initFlagAuthModeNDatabaseForCmd(cmd *cobra.Command) {
	cmd.Flags().StringVar(&authMode,
		"auth-mode", install.DefaultAuthMode().String(), fmt.Sprintf("form of authentication to use, valid auth-modes: %v", maps.Keys(install.PGAuthModes)))
	cmd.Flags().StringVar(&database, "database", "", "database to use")
}

func initFlagOpenForCmd(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&urlOpen, "open", false, "Open the url in a browser")
}

func initFlagUsernameForCmd(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&username, "username", "u", os.Getenv("ROACHPROD_USER"),
		"Username to run under, detect if blank")
}

// initFlagsStopProcessForCmd initializes Flags for processes that stop (kill) processes.
func initFlagsStopProcessForCmd(
	stopProcessesCmd *cobra.Command, sigPtr *int, waitPtr *bool, gracePeriodPtr *int,
) {
	// Cobra does not support reusing flags across multiple commands, especially
	// if the defaults differ, so we need to supply different flags for the case
	// where the defaults are different.
	// See: https://github.com/spf13/cobra/issues/1398
	stopProcessesCmd.Flags().IntVar(sigPtr, "sig", *sigPtr, "signal to pass to kill")
	stopProcessesCmd.Flags().BoolVar(waitPtr, "wait", *waitPtr, "wait for processes to exit")
	stopProcessesCmd.Flags().IntVar(gracePeriodPtr, "grace-period", *gracePeriodPtr, "approx number of seconds to wait for processes to exit, before a forceful shutdown (SIGKILL) is performed")
}

func initFlagsStartOpsForCmd(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&startOpts.ScheduleBackups,
		"schedule-backups", startOpts.ScheduleBackups,
		"create a cluster backup schedule once the cluster has started (by default, "+
			"full backup hourly and incremental every 15 minutes)")
	cmd.Flags().StringVar(&startOpts.ScheduleBackupArgs,
		"schedule-backup-args", startOpts.ScheduleBackupArgs,
		"Recurrence and scheduled backup options specification")
	cmd.Flags().Int64Var(&startOpts.NumFilesLimit, "num-files-limit", startOpts.NumFilesLimit,
		"limit the number of files that can be created by the cockroach process")
	cmd.Flags().IntVar(&startOpts.SQLPort,
		"sql-port", startOpts.SQLPort, "port on which to listen for SQL clients")
	cmd.Flags().BoolVar(&startOpts.EnableFluentSink,
		"enable-fluent-sink", startOpts.EnableFluentSink,
		"whether to enable the fluent-servers attribute in the CockroachDB logging configuration")
}

func initFlagInsecureIgnoreHostKeyForCmd(cmd *cobra.Command) {
	cmd.Flags().BoolVar(
		&ssh.InsecureIgnoreHostKey, "insecure-ignore-host-key", true, "don't check ssh host keys")
}

func initFlagTagForCmd(cmd *cobra.Command) {
	cmd.Flags().StringVar(&tag, "tag", "", "the process tag")
}

func initFlagSCPForCmd(cmd *cobra.Command) {
	cmd.Flags().BoolVar(new(bool), "scp", false, "DEPRECATED")
	_ = cmd.Flags().MarkDeprecated("scp", "always true")
}

func initFlagBinaryForCmd(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&config.Binary,
		"binary", "b", config.Binary, "the remote cockroach binary to use")
}

func initFlagInsecureForCmd(cmd *cobra.Command) {
	// TODO(renato): remove --secure once the default of secure
	// clusters has existed in roachprod long enough.
	cmd.Flags().BoolVar(&secure,
		"secure", secure, "use a secure cluster (DEPRECATED: clusters are secure by default; use --insecure to create insecure clusters.)")
	cmd.Flags().BoolVar(&insecure,
		"insecure", insecure, "use an insecure cluster")
}

func initFlagsClusterNSQLForCmd(cmd *cobra.Command) {
	cmd.Flags().StringVar(&virtualClusterName,
		"cluster", "", "specific virtual cluster to connect to")
	cmd.Flags().IntVar(&sqlInstance,
		"sql-instance", 0, "specific SQL/HTTP instance to connect to (this is a roachprod abstraction distinct from the internal instance ID)")
}

func initFlagDNSRequiredProvidersForCmd(cmd *cobra.Command) {
	cmd.Flags().StringSliceVar(&config.DNSRequiredProviders,
		"dns-required-providers", config.DefaultDNSRequiredProviders,
		"the cloud providers that must be active to refresh DNS entries",
	)
}
