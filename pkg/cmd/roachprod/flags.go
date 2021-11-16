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

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/util/flagutil"
	"github.com/spf13/cobra"
)

var (
	numNodes          int
	numRacks          int
	username          string
	dryrun            bool
	destroyAllMine    bool
	destroyAllLocal   bool
	extendLifetime    time.Duration
	wipePreserveCerts bool
	listDetails       bool
	listJSON          bool
	listMine          bool
	listPattern       string
	secure            = false
	extraSSHOptions   = ""
	nodeEnv           = []string{
		"COCKROACH_ENABLE_RPC_COMPRESSION=false",
		"COCKROACH_UI_RELEASE_NOTES_SIGNUP_DISMISSED=true",
	}
	nodeArgs          []string
	tag               string
	external          = false
	certsDir          string
	adminurlOpen      = false
	adminurlPath      = ""
	adminurlIPs       = false
	useTreeDist       = true
	encrypt           = false
	skipInit          = false
	quiet             = false
	sig               = 9
	waitFlag          = false
	createVMOpts      = vm.DefaultCreateOpts()
	startOpts         = install.StartOptsType{}
	stageOS           string
	stageDir          string
	logsDir           string
	logsFilter        string
	logsProgramFilter string
	logsFrom          time.Time
	logsTo            time.Time
	logsInterval      time.Duration
	maxConcurrency    int

	monitorIgnoreEmptyNodes bool
	monitorOneShot          bool

	cachedHostsCluster string
)

func initFlags() {
	rootCmd.PersistentFlags().BoolVarP(&quiet, "quiet", "q", false, "disable fancy progress output")
	rootCmd.PersistentFlags().IntVarP(&maxConcurrency, "max-concurrency", "", 32,
		"maximum number of operations to execute on nodes concurrently, set to zero for infinite",
	)
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

	createCmd.Flags().DurationVarP(&createVMOpts.Lifetime,
		"lifetime", "l", 12*time.Hour, "Lifetime of the cluster")
	createCmd.Flags().BoolVar(&createVMOpts.SSDOpts.UseLocalSSD,
		"local-ssd", true, "Use local SSD")
	createCmd.Flags().StringVar(&createVMOpts.SSDOpts.FileSystem,
		"filesystem", vm.Ext4, "The underlying file system(ext4/zfs). ext4 is used by default.")
	createCmd.Flags().BoolVar(&createVMOpts.SSDOpts.NoExt4Barrier,
		"local-ssd-no-ext4-barrier", true,
		`Mount the local SSD with the "-o nobarrier" flag. `+
			`Ignored if --local-ssd=false is specified.`)
	createCmd.Flags().IntVarP(&numNodes,
		"nodes", "n", 4, "Total number of nodes, distributed across all clouds")

	createCmd.Flags().IntVarP(&createVMOpts.OsVolumeSize,
		"os-volume-size", "", 10, "OS disk volume size in GB")

	createCmd.Flags().StringSliceVarP(&createVMOpts.VMProviders,
		"clouds", "c", []string{gce.ProviderName},
		fmt.Sprintf("The cloud provider(s) to use when creating new vm instances: %s", vm.AllProviderNames()))
	createCmd.Flags().BoolVar(&createVMOpts.GeoDistributed,
		"geo", false, "Create geo-distributed cluster")
	createCmd.Flags().StringToStringVar(&createVMOpts.CustomLabels,
		"label", make(map[string]string),
		"The label(s) to be used when creating new vm instances, must be in '--label name=value' format "+
			"and value can't be empty string after trimming space, a value that has space must be quoted by single "+
			"quotes, gce label name only allows hyphens (-), underscores (_), lowercase characters, numbers and "+
			"international characters. Examples: usage=cloud-report-2021, namewithspaceinvalue='s o s'")

	// Allow each Provider to inject additional configuration flags
	for _, p := range vm.Providers {
		p.Flags().ConfigureCreateFlags(createCmd.Flags())

		for _, cmd := range []*cobra.Command{
			destroyCmd, extendCmd, listCmd, syncCmd, gcCmd,
		} {
			p.Flags().ConfigureClusterFlags(cmd.Flags(), vm.AcceptMultipleProjects)
		}
		// createCmd only accepts a single GCE project, as opposed to all the other
		// commands.
		p.Flags().ConfigureClusterFlags(createCmd.Flags(), vm.SingleProject)
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

	adminurlCmd.Flags().BoolVar(
		&adminurlOpen, `open`, false, `Open the url in a browser`)
	adminurlCmd.Flags().StringVar(
		&adminurlPath, `path`, "/", `Path to add to URL (e.g. to open a same page on each node)`)
	adminurlCmd.Flags().BoolVar(
		&adminurlIPs, `ips`, false, `Use Public IPs instead of DNS names in URL`)

	gcCmd.Flags().BoolVarP(
		&dryrun, "dry-run", "n", dryrun, "dry run (don't perform any actions)")
	gcCmd.Flags().StringVar(&config.SlackToken, "slack-token", "", "Slack bot token")

	pgurlCmd.Flags().BoolVar(
		&external, "external", false, "return pgurls for external connections")
	pgurlCmd.Flags().StringVar(
		&certsDir, "certs-dir", "./certs", "cert dir to use for secure connections")

	pprofCmd.Flags().DurationVar(
		&pprofOptions.duration, "duration", 30*time.Second, "Duration of profile to capture")
	pprofCmd.Flags().BoolVar(
		&pprofOptions.heap, "heap", false, "Capture a heap profile instead of a CPU profile")
	pprofCmd.Flags().BoolVar(
		&pprofOptions.open, "open", false, "Open the profile using `go tool pprof -http`")
	pprofCmd.Flags().IntVar(
		&pprofOptions.startingPort, "starting-port", 9000, "Initial port to use when opening pprof's HTTP interface")

	ipCmd.Flags().BoolVar(
		&external, "external", false, "return external IP addresses")

	runCmd.Flags().BoolVar(
		&secure, "secure", false, "use a secure cluster")
	runCmd.Flags().StringVarP(
		&extraSSHOptions, "ssh-options", "O", "", "extra args to pass to ssh")

	startCmd.Flags().IntVarP(&numRacks,
		"racks", "r", 0, "the number of racks to partition the nodes into")

	stopCmd.Flags().IntVar(&sig, "sig", sig, "signal to pass to kill")
	stopCmd.Flags().BoolVar(&waitFlag, "wait", waitFlag, "wait for processes to exit")

	wipeCmd.Flags().BoolVar(&wipePreserveCerts, "preserve-certs", false, "do not wipe certificates")

	for _, cmd := range []*cobra.Command{
		startCmd, statusCmd, stopCmd, runCmd,
	} {
		cmd.Flags().StringVar(
			&tag, "tag", "", "the process tag")
	}

	for _, cmd := range []*cobra.Command{
		startCmd, putCmd, getCmd,
	} {
		cmd.Flags().BoolVar(new(bool), "scp", false, "DEPRECATED")
		_ = cmd.Flags().MarkDeprecated("scp", "always true")
	}

	putCmd.Flags().BoolVar(&useTreeDist, "treedist", useTreeDist, "use treedist copy algorithm")

	stageCmd.Flags().StringVar(&stageOS, "os", "", "operating system override for staged binaries")
	stageCmd.Flags().StringVar(&stageDir, "dir", "", "destination for staged binaries")

	stageURLCmd.Flags().StringVar(&stageOS, "os", "", "operating system override for staged binaries")

	logsCmd.Flags().StringVar(
		&logsFilter, "filter", "", "re to filter log messages")
	logsCmd.Flags().Var(
		flagutil.Time(&logsFrom), "from", "time from which to stream logs")
	logsCmd.Flags().Var(
		flagutil.Time(&logsTo), "to", "time to which to stream logs")
	logsCmd.Flags().DurationVar(
		&logsInterval, "interval", 200*time.Millisecond, "interval to poll logs from host")
	logsCmd.Flags().StringVar(
		&logsDir, "logs-dir", "logs", "path to the logs dir, if remote, relative to username's home dir, ignored if local")
	logsCmd.Flags().StringVar(
		&logsProgramFilter, "logs-program", "^cockroach$", "regular expression of the name of program in log files to search")

	monitorCmd.Flags().BoolVar(
		&monitorIgnoreEmptyNodes,
		"ignore-empty-nodes",
		false,
		"Automatically detect the (subset of the given) nodes which to monitor "+
			"based on the presence of a nontrivial data directory.")

	monitorCmd.Flags().BoolVar(
		&monitorOneShot,
		"oneshot",
		false,
		"Report the status of all targeted nodes once, then exit. The exit "+
			"status is nonzero if (and only if) any node was found not running.")

	cachedHostsCmd.Flags().StringVar(&cachedHostsCluster, "cluster", "", "print hosts matching cluster")

	for _, cmd := range []*cobra.Command{
		getCmd, putCmd, runCmd, startCmd, statusCmd, stopCmd,
		wipeCmd, pgurlCmd, adminurlCmd, sqlCmd, installCmd,
	} {
		switch cmd {
		case startCmd:
			cmd.Flags().BoolVar(
				&startOpts.Sequential, "sequential", true,
				"start nodes sequentially so node IDs match hostnames")
			cmd.Flags().StringArrayVarP(
				&nodeArgs, "args", "a", nil, "node arguments")
			cmd.Flags().StringArrayVarP(
				&nodeEnv, "env", "e", nodeEnv, "node environment variables")
			cmd.Flags().BoolVar(
				&startOpts.Encrypt, "encrypt", encrypt, "start nodes with encryption at rest turned on")
			cmd.Flags().BoolVar(
				&startOpts.SkipInit, "skip-init", skipInit, "skip initializing the cluster")
			cmd.Flags().IntVar(
				&startOpts.StoreCount, "store-count", 1, "number of stores to start each node with")
			fallthrough
		case sqlCmd:
			cmd.Flags().StringVarP(
				&config.Binary, "binary", "b", config.Binary,
				"the remote cockroach binary to use")
			fallthrough
		case pgurlCmd, adminurlCmd:
			cmd.Flags().BoolVar(
				&secure, "secure", false, "use a secure cluster")
		}
	}
}
