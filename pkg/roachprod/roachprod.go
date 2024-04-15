// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachprod

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/grafana"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/lock"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/aws"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/azure"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/local"
	"github.com/cockroachdb/cockroach/pkg/server/debug/replay"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"golang.org/x/sys/unix"
)

// verifyClusterName ensures that the given name conforms to
// our naming pattern of "<username>-<clustername>". The
// username must match one of the vm.Provider account names
// or the --username override.
func verifyClusterName(l *logger.Logger, clusterName, username string) error {
	if clusterName == "" {
		return fmt.Errorf("cluster name cannot be blank")
	}

	alphaNum, err := regexp.Compile(`^[a-zA-Z0-9\-]+$`)
	if err != nil {
		return err
	}
	if !alphaNum.MatchString(clusterName) {
		return errors.Errorf("cluster name must match %s", alphaNum.String())
	}

	if config.IsLocalClusterName(clusterName) {
		return nil
	}

	// Use the vm.Provider account names, or --username.
	var accounts []string
	if len(username) > 0 {
		accounts = []string{username}
	} else {
		seenAccounts := map[string]bool{}
		active, err := vm.FindActiveAccounts(l)
		if err != nil {
			return err
		}
		for _, account := range active {
			if !seenAccounts[account] {
				seenAccounts[account] = true
				cleanAccount := vm.DNSSafeAccount(account)
				if cleanAccount != account {
					l.Printf("WARN: using `%s' as username instead of `%s'", cleanAccount, account)
				}
				accounts = append(accounts, cleanAccount)
			}
		}
	}

	// If we see <account>-<something>, accept it.
	for _, account := range accounts {
		if strings.HasPrefix(clusterName, account+"-") && len(clusterName) > len(account)+1 {
			return nil
		}
	}

	// Try to pick out a reasonable cluster name from the input.
	var suffix string
	if i := strings.Index(clusterName, "-"); i != -1 {
		// The user specified a username prefix, but it didn't match an active
		// account name. For example, assuming the account is "peter", `roachprod
		// create joe-perf` should be specified as `roachprod create joe-perf -u
		// joe`.
		suffix = clusterName[i+1:]
	} else {
		// The user didn't specify a username prefix. For example, assuming the
		// account is "peter", `roachprod create perf` should be specified as
		// `roachprod create peter-perf`.
		suffix = clusterName
	}

	// Suggest acceptable cluster names.
	var suggestions []string
	for _, account := range accounts {
		suggestions = append(suggestions, fmt.Sprintf("%s-%s", account, suffix))
	}
	return fmt.Errorf("malformed cluster name %s, did you mean one of %s",
		clusterName, suggestions)
}

func sortedClusters() []string {
	var r []string
	syncedClusters.mu.Lock()
	defer syncedClusters.mu.Unlock()
	for n := range syncedClusters.clusters {
		r = append(r, n)
	}
	sort.Strings(r)
	return r
}

// newCluster initializes a SyncedCluster for the given cluster name.
//
// The cluster name can include a node selector (e.g. "foo:1-3").
func newCluster(
	l *logger.Logger, name string, opts ...install.ClusterSettingOption,
) (*install.SyncedCluster, error) {
	clusterSettings := install.MakeClusterSettings(opts...)
	nodeSelector := "all"
	{
		parts := strings.Split(name, ":")
		switch len(parts) {
		case 2:
			nodeSelector = parts[1]
			fallthrough
		case 1:
			name = parts[0]
		case 0:
			return nil, fmt.Errorf("no cluster specified")
		default:
			return nil, fmt.Errorf("invalid cluster name: %s", name)
		}
	}

	metadata, ok := readSyncedClusters(name)
	if !ok {
		err := errors.Newf(`unknown cluster: %s`, name)
		err = errors.WithHintf(err, "\nAvailable clusters:\n  %s\n", strings.Join(sortedClusters(), "\n  "))
		err = errors.WithHint(err, `Use "roachprod sync" to update the list of available clusters.`)
		return nil, err
	}

	if clusterSettings.DebugDir == "" {
		clusterSettings.DebugDir = os.ExpandEnv(config.DefaultDebugDir)
	}

	c, err := install.NewSyncedCluster(metadata, nodeSelector, clusterSettings)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// userClusterNameRegexp returns a regexp that matches all clusters owned by the
// current user.
func userClusterNameRegexp(l *logger.Logger) (*regexp.Regexp, error) {
	// In general, we expect that users will have the same
	// account name across the services they're using,
	// but we still want to function even if this is not
	// the case.
	seenAccounts := map[string]bool{}
	accounts, err := vm.FindActiveAccounts(l)
	if err != nil {
		return nil, err
	}
	pattern := ""
	for _, account := range accounts {
		if !seenAccounts[account] {
			seenAccounts[account] = true
			if len(pattern) > 0 {
				pattern += "|"
			}
			pattern += fmt.Sprintf("(^%s-)", regexp.QuoteMeta(account))
		}
	}
	return regexp.Compile(pattern)
}

// Version returns version/build information.
func Version(l *logger.Logger) string {
	info := build.GetInfo()
	return info.Long()
}

// CachedClusters iterates over all roachprod clusters from the local cache, in
// alphabetical order.
func CachedClusters(fn func(clusterName string, numVMs int)) {
	for _, name := range sortedClusters() {
		c, ok := CachedCluster(name)
		if !ok {
			return
		}
		fn(c.Name, len(c.VMs))
	}
}

// CachedCluster returns the cached information about a given cluster.
func CachedCluster(name string) (*cloud.Cluster, bool) {
	return readSyncedClusters(name)
}

// Sync grabs an exclusive lock on the roachprod state and then proceeds to
// read the current state from the cloud and write it out to disk. The locking
// protects both the reading and the writing in order to prevent the hazard
// caused by concurrent goroutines reading cloud state in a different order
// than writing it to disk.
func Sync(l *logger.Logger, options vm.ListOptions) (*cloud.Cloud, error) {
	if !config.Quiet {
		l.Printf("Syncing...")
	}
	unlock, err := lock.AcquireFilesystemLock(config.DefaultLockPath)
	if err != nil {
		return nil, err
	}
	defer unlock()

	cld, err := cloud.ListCloud(l, options)
	// ListCloud may fail for a provider, but we still want to continue as
	// the cluster the caller is trying to add and use may have been found.
	// Instead, we tell syncClustersCache not to remove any clusters as we
	// can't tell if a cluster was deleted or not found due to the error.
	// The next successful ListCloud call will clean it up.
	overwriteMissingClusters := err == nil
	if err := syncClustersCache(l, cld, overwriteMissingClusters); err != nil {
		return nil, err
	}

	var vms vm.List
	for _, c := range cld.Clusters {
		vms = append(vms, c.VMs...)
	}

	// Figure out if we're going to overwrite the DNS entries. We don't want to
	// overwrite if we don't have all the VMs of interest, so we only do it if we
	// have a list of all VMs from both AWS and GCE (so if both providers have
	// been used to get the VMs and for GCP also if we listed the VMs in the
	// default project).
	refreshDNS := true

	if p := vm.Providers[gce.ProviderName]; !p.Active() {
		refreshDNS = false
	} else {
		var defaultProjectFound bool
		for _, prj := range p.(*gce.Provider).GetProjects() {
			if prj == gce.DefaultProject() {
				defaultProjectFound = true
				break
			}
		}
		if !defaultProjectFound {
			refreshDNS = false
		}
	}
	if !vm.Providers[aws.ProviderName].Active() {
		refreshDNS = false
	}
	// DNS entries are maintained in the GCE DNS registry for all vms, from all
	// clouds.
	if refreshDNS {
		if !config.Quiet {
			l.Printf("Refreshing DNS entries...")
		}
		if err := gce.SyncDNS(l, vms); err != nil {
			l.Errorf("failed to update %s DNS: %v", gce.Subdomain, err)
		}
	} else {
		if !config.Quiet {
			l.Printf("Not refreshing DNS entries. We did not have all the VMs.")
		}
	}

	if err := vm.ProvidersSequential(vm.AllProviderNames(), func(p vm.Provider) error {
		return p.CleanSSH(l)
	}); err != nil {
		return nil, err
	}

	return cld, nil
}

// List returns a cloud.Cloud struct of all roachprod clusters matching clusterNamePattern.
// Alternatively, the 'listMine' option can be provided to get the clusters that are owned
// by the current user.
func List(
	l *logger.Logger, listMine bool, clusterNamePattern string, opts vm.ListOptions,
) (cloud.Cloud, error) {
	if err := LoadClusters(); err != nil {
		return cloud.Cloud{}, err
	}
	listPattern := regexp.MustCompile(".*")
	if clusterNamePattern == "" {
		if listMine {
			var err error
			listPattern, err = userClusterNameRegexp(l)
			if err != nil {
				return cloud.Cloud{}, err
			}
		}
	} else {
		if listMine {
			return cloud.Cloud{}, errors.New("'mine' option cannot be combined with 'pattern'")
		}
		var err error
		listPattern, err = regexp.Compile(clusterNamePattern)
		if err != nil {
			return cloud.Cloud{}, errors.Wrapf(err, "could not compile regex pattern: %s", clusterNamePattern)
		}
	}

	cld, err := Sync(l, opts)
	if err != nil {
		return cloud.Cloud{}, err
	}

	// Encode the filtered clusters and all the bad instances.
	filteredClusters := cld.Clusters.FilterByName(listPattern)
	filteredCloud := cloud.Cloud{
		Clusters:     filteredClusters,
		BadInstances: cld.BadInstances,
	}
	return filteredCloud, nil
}

// TruncateString truncates a string to maxLength and adds "..." to the end.
func TruncateString(s string, maxLength int) string {
	if len(s) > maxLength {
		return s[:maxLength-3] + "..."
	}
	return s
}

// Run runs a command on the nodes in a cluster.
func Run(
	ctx context.Context,
	l *logger.Logger,
	clusterName, SSHOptions, processTag string,
	secure bool,
	stdout, stderr io.Writer,
	cmdArray []string,
	options install.RunOptions,
) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName, install.SecureOption(secure), install.TagOption(processTag))
	if err != nil {
		return err
	}

	// Use "ssh" if an interactive session was requested (i.e. there is no
	// remote command to run).
	if len(cmdArray) == 0 {
		return c.SSH(ctx, l, strings.Split(SSHOptions, " "), cmdArray)
	}
	// If no nodes were specified, run on nodes derived from the clusterName.
	if len(options.Nodes) == 0 {
		options.Nodes = c.TargetNodes()
	}

	cmd := strings.TrimSpace(strings.Join(cmdArray, " "))
	return c.Run(ctx, l, stdout, stderr, options, TruncateString(cmd, 30), cmd)
}

// RunWithDetails runs a command on the nodes in a cluster.
func RunWithDetails(
	ctx context.Context,
	l *logger.Logger,
	clusterName, SSHOptions, processTag string,
	secure bool,
	cmdArray []string,
	options install.RunOptions,
) ([]install.RunResultDetails, error) {
	if err := LoadClusters(); err != nil {
		return nil, err
	}
	c, err := newCluster(l, clusterName, install.SecureOption(secure), install.TagOption(processTag))
	if err != nil {
		return nil, err
	}

	// Use "ssh" if an interactive session was requested (i.e. there is no
	// remote command to run).
	if len(cmdArray) == 0 {
		return nil, c.SSH(ctx, l, strings.Split(SSHOptions, " "), cmdArray)
	}
	// If no nodes were specified, run on nodes derived from the clusterName.
	if len(options.Nodes) == 0 {
		options.Nodes = c.TargetNodes()
	}
	cmd := strings.TrimSpace(strings.Join(cmdArray, " "))
	return c.RunWithDetails(ctx, l, options, TruncateString(cmd, 30), cmd)
}

// SQL runs `cockroach sql` on a remote cluster. If a single node is passed,
// an interactive session may start.
//
// NOTE: When querying a single-node in a cluster, a pseudo-terminal is attached
// to ssh which may result in an _interactive_ ssh session.
//
// CAUTION: this function should not be used by roachtest writers. Use syncedCluser.ExecSQL()
// instead.
func SQL(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	secure bool,
	tenantName string,
	tenantInstance int,
	cmdArray []string,
) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName, install.SecureOption(secure))
	if err != nil {
		return err
	}
	if len(c.Nodes) == 1 {
		return c.ExecOrInteractiveSQL(ctx, l, tenantName, tenantInstance, cmdArray)
	}

	results, err := c.ExecSQL(ctx, l, c.Nodes, tenantName, tenantInstance, cmdArray)
	if err != nil {
		return err
	}

	for i, r := range results {
		printSQLResult(l, i, r, cmdArray)
	}
	return nil
}

// printSQLResult does a best-effort attempt to print single-result-row-per-node
// result-sets gathered from many nodes as one-line-per-node instead of header
// separated n-line blocks, to improve the overall readability, falling back to
// normal header-plus-response-block per node otherwise.
func printSQLResult(l *logger.Logger, i int, r *install.RunResultDetails, args []string) {
	tableFormatted := false
	for i, c := range args {
		if c == "--format=table" || c == "--format" && len(args) > i+1 && args[i+1] == "table" {
			tableFormatted = true
			break
		}
	}

	singleResultLen, resultLine := 3, 1 // 3 is header, result, empty-trailing.
	if tableFormatted {
		// table output adds separator above the result, and a trailing row count.
		singleResultLen, resultLine = 5, 2
	}
	// If we got a header line and zero or one result lines, we can print the
	// result line as one-line-per-node, rather than a header per node and then
	// its n result lines, to make the aggregate output more readable. We can
	// detect this by splitting on newline into only as many lines as we expect,
	// and seeing if the final piece is empty or has the rest of >1 results in it.
	lines := strings.SplitN(r.CombinedOut, "\n", singleResultLen)
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		if i == 0 { // Print the header line of the results once.
			fmt.Printf("    %s\n", lines[0])
			if tableFormatted {
				fmt.Printf("    %s\n", lines[1])
			}
		}
		// Print the result line if there is one.
		if len(lines) > resultLine {
			fmt.Printf("%2d: %s\n", r.Node, lines[resultLine])
			return
		}
		// No result from this node, so print a blank for its ID.
		fmt.Printf("%2d:\n", r.Node)
		return
	}
	// Just print the roachprod header identifying the node, then the node's whole
	// response, including its internal header row.
	l.Printf("node %d:\n%s", r.Node, r.CombinedOut)
}

// IP gets the ip addresses of the nodes in a cluster.
func IP(l *logger.Logger, clusterName string, external bool) ([]string, error) {
	if err := LoadClusters(); err != nil {
		return nil, err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return nil, err
	}

	nodes := c.TargetNodes()
	ips := make([]string, len(nodes))

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		if external {
			ips[i] = c.Host(node)
		} else {
			ips[i], err = c.GetInternalIP(node)
			if err != nil {
				return nil, err
			}
		}
	}

	return ips, nil
}

// Status retrieves the status of nodes in a cluster.
func Status(
	ctx context.Context, l *logger.Logger, clusterName, processTag string,
) ([]install.NodeStatus, error) {
	if err := LoadClusters(); err != nil {
		return nil, err
	}
	c, err := newCluster(l, clusterName, install.TagOption(processTag))
	if err != nil {
		return nil, err
	}
	return c.Status(ctx, l)
}

// Stage stages release and edge binaries to the cluster.
// stageOS, stageDir, version can be "" to use default values
func Stage(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	stageOS, stageArch, stageDir, applicationName, version string,
) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}

	os := "linux"
	arch := "amd64"

	if c.IsLocal() {
		os = runtime.GOOS
		arch = runtime.GOARCH
	}
	if stageOS != "" {
		os = stageOS
	}
	if stageArch != "" {
		arch = stageArch
	}
	// N.B. it's technically possible to stage a binary for a different OS/arch; e.g., emulated amd64 on mac silicon.
	// However, we don't perform any other validation, hence a warning message is appropriate.
	if c.IsLocal() && (os != runtime.GOOS || arch != runtime.GOARCH) {
		l.Printf("WARN: locally staging %s/%s binaries on %s/%s", os, arch, runtime.GOOS, runtime.GOARCH)
	}

	dir := "."
	if stageDir != "" {
		dir = stageDir
	}

	return install.StageApplication(ctx, l, c, applicationName, version, os, vm.CPUArch(arch), dir)
}

// Reset resets all VMs in a cluster.
func Reset(l *logger.Logger, clusterName string) error {
	if err := LoadClusters(); err != nil {
		return err
	}

	if config.IsLocalClusterName(clusterName) {
		return nil
	}

	c, err := getCluster(l, clusterName)
	if err != nil {
		return err
	}

	return vm.FanOut(c.VMs, func(p vm.Provider, vms vm.List) error {
		return p.Reset(l, vms)
	})
}

// SetupSSH sets up the keys and host keys for the vms in the cluster.
func SetupSSH(ctx context.Context, l *logger.Logger, clusterName string) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	cld, err := Sync(l, vm.ListOptions{})
	if err != nil {
		return err
	}

	cloudCluster, ok := cld.Clusters[clusterName]
	if !ok {
		return fmt.Errorf("could not find %s in list of cluster", clusterName)
	}

	zones := make(map[string][]string, len(cloudCluster.VMs))
	for _, vm := range cloudCluster.VMs {
		zones[vm.Provider] = append(zones[vm.Provider], vm.Zone)
	}
	providers := make([]string, 0)
	for provider := range zones {
		providers = append(providers, provider)
	}

	// Configure SSH for machines in the zones we operate on.
	if err := vm.ProvidersSequential(providers, func(p vm.Provider) error {
		unlock, lockErr := lock.AcquireFilesystemLock(config.DefaultLockPath)
		if lockErr != nil {
			return lockErr
		}
		defer unlock()
		return p.ConfigSSH(l, zones[p.Name()])
	}); err != nil {
		return err
	}

	if err = cloudCluster.PrintDetails(l); err != nil {
		return err
	}
	// Run ssh-keygen -R serially on each new VM in case an IP address has been recycled
	for _, v := range cloudCluster.VMs {
		cmd := exec.Command("ssh-keygen", "-R", v.PublicIP)

		out, err := cmd.CombinedOutput()
		if err != nil {
			l.Printf("could not clear ssh key for hostname %s:\n%s", v.PublicIP, string(out))
		}

	}

	// Wait for the nodes in the cluster to start.
	if err := LoadClusters(); err != nil {
		return err
	}

	installCluster, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	if err := installCluster.Wait(ctx, l); err != nil {
		return err
	}
	// Fetch public keys from gcloud to set up ssh access for all users into the
	// shared ubuntu user.
	authorizedKeys, err := gce.GetUserAuthorizedKeys()
	if err != nil {
		return errors.Wrap(err, "failed to retrieve authorized keys from gcloud")
	}
	installCluster.AuthorizedKeys = authorizedKeys.AsSSH()
	return installCluster.SetupSSH(ctx, l)
}

// Extend extends the lifetime of the specified cluster to prevent it from being destroyed.
func Extend(l *logger.Logger, clusterName string, lifetime time.Duration) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := getCluster(l, clusterName)
	if err != nil {
		return err
	}

	if err := cloud.ExtendCluster(l, c, lifetime); err != nil {
		return err
	}

	// Reload the clusters and print details.
	c, err = getCluster(l, clusterName)
	if err != nil {
		return err
	}

	return c.PrintDetails(l)
}

// Default scheduled backup runs a full backup every hour and an incremental
// every 15 minutes.
const DefaultBackupSchedule = `RECURRING '*/15 * * * *' FULL BACKUP '@hourly' WITH SCHEDULE OPTIONS first_run = 'now'`

// DefaultStartOpts returns a StartOpts populated with default values.
func DefaultStartOpts() install.StartOpts {
	return install.StartOpts{
		EncryptedStores:    false,
		NumFilesLimit:      config.DefaultNumFilesLimit,
		SkipInit:           false,
		StoreCount:         1,
		VirtualClusterID:   2,
		ScheduleBackups:    false,
		ScheduleBackupArgs: DefaultBackupSchedule,
		InitTarget:         1,
		SQLPort:            0,
		VirtualClusterName: install.SystemInterfaceName,
		// TODO(DarrylWong): revert back to 0 once #117125 is addressed.
		AdminUIPort: config.DefaultAdminUIPort,
	}
}

// Start starts nodes on a cluster.
func Start(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	startOpts install.StartOpts,
	clusterSettingsOpts ...install.ClusterSettingOption,
) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName, clusterSettingsOpts...)
	if err != nil {
		return err
	}
	return c.Start(ctx, l, startOpts)
}

// Monitor monitors the status of cockroach nodes in a cluster.
func Monitor(
	ctx context.Context, l *logger.Logger, clusterName string, opts install.MonitorOpts,
) (chan install.NodeMonitorInfo, error) {
	c, err := newCluster(l, clusterName)
	if err != nil {
		return nil, err
	}
	return c.Monitor(l, ctx, opts), nil
}

// StopOpts is used to pass options to Stop.
type StopOpts struct {
	ProcessTag string
	Sig        int
	// If Wait is set, roachprod waits until the PID disappears (i.e. the
	// process has terminated).
	Wait bool // forced to true when Sig == 9
	// If MaxWait is set, roachprod waits that approximate number of seconds
	// until the PID disappears.
	MaxWait int

	// Options that only apply to StopServiceForVirtualCluster
	VirtualClusterID   int
	VirtualClusterName string
	SQLInstance        int
}

// DefaultStopOpts returns StopOpts populated with the default values used by Stop.
func DefaultStopOpts() StopOpts {
	return StopOpts{
		ProcessTag: "",
		Sig:        9,
		Wait:       false,
		MaxWait:    0,
	}
}

// Stop stops nodes on a cluster.
func Stop(ctx context.Context, l *logger.Logger, clusterName string, opts StopOpts) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName, install.TagOption(opts.ProcessTag))
	if err != nil {
		return err
	}
	return c.Stop(ctx, l, opts.Sig, opts.Wait, opts.MaxWait, "")
}

// Signal sends a signal to nodes in the cluster.
func Signal(ctx context.Context, l *logger.Logger, clusterName string, sig int) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	return c.Signal(ctx, l, sig)
}

// Init initializes the cluster.
func Init(ctx context.Context, l *logger.Logger, clusterName string, opts install.StartOpts) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	return c.Init(ctx, l, opts.GetInitTarget())
}

// Wipe wipes the nodes in a cluster.
func Wipe(ctx context.Context, l *logger.Logger, clusterName string, preserveCerts bool) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	return c.Wipe(ctx, l, preserveCerts)
}

// Reformat reformats disks in a cluster to use the specified filesystem.
func Reformat(ctx context.Context, l *logger.Logger, clusterName string, fs string) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}

	var fsCmd string
	switch fs {
	case vm.Zfs:
		if err := install.Install(ctx, l, c, []string{vm.Zfs}); err != nil {
			return err
		}
		fsCmd = `sudo zpool create -f data1 -m /mnt/data1 /dev/sdb`
	case vm.Ext4:
		fsCmd = `sudo mkfs.ext4 -F /dev/sdb && sudo mount -o defaults /dev/sdb /mnt/data1`
	default:
		return fmt.Errorf("unknown filesystem %q", fs)
	}

	err = c.Run(ctx, l, os.Stdout, os.Stderr, install.WithNodes(c.Nodes), "reformatting", fmt.Sprintf(`
set -euo pipefail
if sudo zpool list -Ho name 2>/dev/null | grep ^data1$; then
sudo zpool destroy -f data1
fi
if mountpoint -q /mnt/data1; then
sudo umount -f /mnt/data1
fi
%s
sudo chmod 777 /mnt/data1
`, fsCmd))
	if err != nil {
		return err
	}
	return nil
}

// Install installs third party software.
func Install(ctx context.Context, l *logger.Logger, clusterName string, software []string) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}

	// As seen in #103316, this can hit a 503 Service Unavailable when
	// trying to download the package, so we retry every 30 seconds
	// for up to 5 mins below. The caller may choose to fail or skip the test.
	return retry.WithMaxAttempts(ctx, retry.Options{
		InitialBackoff: 30 * time.Second,
		Multiplier:     1,
	}, 10, func() error {
		err := install.Install(ctx, l, c, software)
		err = errors.Wrapf(err, "retryable infrastructure error: could not install %s", software)
		if err != nil {
			l.Printf(err.Error())
		}
		return err
	})
}

// Download downloads 3rd party tools, using a GCS cache if possible.
func Download(
	ctx context.Context, l *logger.Logger, clusterName string, src, sha, dest string,
) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	return install.Download(ctx, l, c, src, sha, dest)
}

// DistributeCerts distributes certificates to the nodes in a cluster.
// If the certificates already exist, no action is taken.
func DistributeCerts(ctx context.Context, l *logger.Logger, clusterName string) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	return c.DistributeCerts(ctx, l)
}

// Put copies a local file to the nodes in a cluster.
func Put(
	ctx context.Context, l *logger.Logger, clusterName, src, dest string, useTreeDist bool,
) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName, install.UseTreeDistOption(useTreeDist))
	if err != nil {
		return err
	}
	return c.Put(ctx, l, c.Nodes, src, dest)
}

// Get copies a remote file from the nodes in a cluster.
// If the file is retrieved from multiple nodes the destination
// file name will be prefixed with the node number.
func Get(ctx context.Context, l *logger.Logger, clusterName, src, dest string) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	return c.Get(ctx, l, c.Nodes, src, dest)
}

type PGURLOptions struct {
	Secure             bool
	External           bool
	VirtualClusterName string
	SQLInstance        int
	Auth               install.PGAuthMode
}

// PgURL generates pgurls for the nodes in a cluster.
func PgURL(
	ctx context.Context, l *logger.Logger, clusterName, certsDir string, opts PGURLOptions,
) ([]string, error) {
	if err := LoadClusters(); err != nil {
		return nil, err
	}
	c, err := newCluster(l, clusterName, install.SecureOption(opts.Secure), install.PGUrlCertsDirOption(certsDir))
	if err != nil {
		return nil, err
	}
	nodes := c.TargetNodes()
	ips := make([]string, len(nodes))

	if opts.External {
		for i := 0; i < len(nodes); i++ {
			ips[i] = c.VMs[nodes[i]-1].PublicIP
		}
	} else {
		for i := 0; i < len(nodes); i++ {
			ip, err := c.GetInternalIP(nodes[i])
			if err == nil {
				ips[i] = ip
			}
		}
	}

	var urls []string
	for i, ip := range ips {
		desc, err := c.DiscoverService(ctx, nodes[i], opts.VirtualClusterName, install.ServiceTypeSQL, opts.SQLInstance)
		if err != nil {
			return nil, err
		}
		if ip == "" {
			return nil, errors.Errorf("empty ip: %v", ips)
		}
		urls = append(urls, c.NodeURL(ip, desc.Port, opts.VirtualClusterName, desc.ServiceMode, opts.Auth))
	}
	if len(urls) != len(nodes) {
		return nil, errors.Errorf("have nodes %v, but urls %v from ips %v", nodes, urls, ips)
	}
	return urls, nil
}

type urlConfig struct {
	path               string
	usePublicIP        bool
	openInBrowser      bool
	secure             bool
	port               int
	virtualClusterName string
	sqlInstance        int
}

func urlGenerator(
	ctx context.Context,
	c *install.SyncedCluster,
	l *logger.Logger,
	nodes install.Nodes,
	uConfig urlConfig,
) ([]string, error) {
	var urls []string
	for i, node := range nodes {
		host := vm.Name(c.Name, int(node)) + "." + gce.Subdomain

		// There are no DNS entries for local clusters.
		if c.IsLocal() {
			uConfig.usePublicIP = true
		}

		// verify DNS is working / fallback to IPs if not.
		if i == 0 && !uConfig.usePublicIP {
			if _, err := net.LookupHost(host); err != nil {
				l.Errorf("host %s is unreachable, falling back to public IPs. DNS entries might be outdated, run `roachprod sync`.", host)
				uConfig.usePublicIP = true
			}
		}

		if uConfig.usePublicIP {
			host = c.VMs[node-1].PublicIP
		}
		port := uConfig.port
		if port == 0 {
			desc, err := c.DiscoverService(
				ctx, node, uConfig.virtualClusterName, install.ServiceTypeUI, uConfig.sqlInstance,
			)
			if err != nil {
				return nil, err
			}
			port = desc.Port
		}
		scheme := "http"
		if uConfig.secure {
			scheme = "https"
		}
		if !strings.HasPrefix(uConfig.path, "/") {
			uConfig.path = "/" + uConfig.path
		}
		url := fmt.Sprintf("%s://%s:%d%s", scheme, host, port, uConfig.path)
		urls = append(urls, url)
		if uConfig.openInBrowser {
			cmd := browserCmd(url)

			if err := cmd.Run(); err != nil {
				return nil, err
			}
		}
	}
	return urls, nil
}

func browserCmd(url string) *exec.Cmd {
	var cmd string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		cmd = "/usr/bin/open"
	case "windows":
		cmd = "cmd"
		args = []string{"/c", "start"}
	default:
		cmd = "xdg-open"
	}
	args = append(args, url)
	return exec.Command(cmd, args...)
}

// AdminURL generates admin UI URLs for the nodes in a cluster.
func AdminURL(
	ctx context.Context,
	l *logger.Logger,
	clusterName, virtualClusterName string,
	sqlInstance int,
	path string,
	usePublicIP, openInBrowser, secure bool,
) ([]string, error) {
	if err := LoadClusters(); err != nil {
		return nil, err
	}
	c, err := newCluster(l, clusterName, install.SecureOption(secure))
	if err != nil {
		return nil, err
	}
	uConfig := urlConfig{
		path:               path,
		usePublicIP:        usePublicIP,
		openInBrowser:      openInBrowser,
		secure:             secure,
		virtualClusterName: virtualClusterName,
		sqlInstance:        sqlInstance,
	}
	return urlGenerator(ctx, c, l, c.TargetNodes(), uConfig)
}

// SQLPorts finds the SQL ports for a cluster.
func SQLPorts(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	secure bool,
	virtualClusterName string,
	sqlInstance int,
) ([]int, error) {
	if err := LoadClusters(); err != nil {
		return nil, err
	}
	c, err := newCluster(l, clusterName, install.SecureOption(secure))
	if err != nil {
		return nil, err
	}
	var ports []int
	for _, node := range c.Nodes {
		port, err := c.NodePort(ctx, node, virtualClusterName, sqlInstance)
		if err != nil {
			return nil, errors.Wrapf(err, "Error discovering SQL Port for node %d", node)
		}
		ports = append(ports, port)
	}
	return ports, nil
}

// AdminPorts finds the AdminUI ports for a cluster.
func AdminPorts(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	secure bool,
	virtualClusterName string,
	sqlInstance int,
) ([]int, error) {
	if err := LoadClusters(); err != nil {
		return nil, err
	}
	c, err := newCluster(l, clusterName, install.SecureOption(secure))
	if err != nil {
		return nil, err
	}
	var ports []int
	for _, node := range c.Nodes {
		port, err := c.NodeUIPort(ctx, node, virtualClusterName, sqlInstance)
		if err != nil {
			return nil, errors.Wrapf(err, "Error discovering UI Port for node %d", node)
		}
		ports = append(ports, port)
	}
	return ports, nil
}

// PprofOpts specifies the options needed by Pprof().
type PprofOpts struct {
	Heap         bool
	Open         bool
	StartingPort int
	Duration     time.Duration
}

// Pprof TODO
func Pprof(ctx context.Context, l *logger.Logger, clusterName string, opts PprofOpts) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}

	var profType string
	var description string
	if opts.Heap {
		description = "capturing heap profile"
		profType = "heap"
	} else {
		description = "capturing CPU profile"
		profType = "profile"
	}

	outputFiles := []string{}
	mu := &syncutil.Mutex{}
	pprofPath := fmt.Sprintf("debug/pprof/%s?seconds=%d", profType, int(opts.Duration.Seconds()))

	minTimeout := 30 * time.Second
	timeout := 2 * opts.Duration
	if timeout < minTimeout {
		timeout = minTimeout
	}

	httpClient := httputil.NewClientWithTimeout(timeout)
	startTime := timeutil.Now().Unix()
	err = c.Parallel(ctx, l, install.WithNodes(c.TargetNodes()).WithDisplay(description),
		func(ctx context.Context, node install.Node) (*install.RunResultDetails, error) {
			res := &install.RunResultDetails{Node: node}
			host := c.Host(node)
			port, err := c.NodeUIPort(ctx, node, "" /* virtualClusterName */, 0 /* sqlInstance */)
			if err != nil {
				return nil, err
			}
			scheme := "http"
			if c.Secure {
				scheme = "https"
			}
			outputFile := fmt.Sprintf("pprof-%s-%d-%s-%04d.out", profType, startTime, c.Name, node)
			outputDir := filepath.Dir(outputFile)
			file, err := os.CreateTemp(outputDir, ".pprof")
			if err != nil {
				res.Err = errors.Wrap(err, "create tmpfile for pprof download")
				return res, res.Err
			}

			defer func() {
				err := file.Close()
				if err != nil && !errors.Is(err, oserror.ErrClosed) {
					l.Errorf("warning: could not close temporary file")
				}
				err = os.Remove(file.Name())
				if err != nil && !oserror.IsNotExist(err) {
					l.Errorf("warning: could not remove temporary file")
				}
			}()

			pprofURL := fmt.Sprintf("%s://%s:%d/%s", scheme, host, port, pprofPath)
			resp, err := httpClient.Get(context.Background(), pprofURL)
			if err != nil {
				res.Err = err
				return res, res.Err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				res.Err = errors.Newf("unexpected status from pprof endpoint: %s", resp.Status)
				return res, res.Err
			}

			if _, err := io.Copy(file, resp.Body); err != nil {
				res.Err = err
				return res, res.Err
			}
			if err := file.Sync(); err != nil {
				res.Err = err
				return res, res.Err
			}
			if err := file.Close(); err != nil {
				res.Err = err
				return res, res.Err
			}
			if err := os.Rename(file.Name(), outputFile); err != nil {
				res.Err = err
				return res, res.Err
			}

			mu.Lock()
			outputFiles = append(outputFiles, outputFile)
			mu.Unlock()
			return res, nil
		})

	for _, s := range outputFiles {
		l.Printf("Created %s", s)
	}

	if err != nil {
		exit.WithCode(exit.UnspecifiedError())
	}

	if opts.Open {
		waitCommands := []*exec.Cmd{}
		for i, file := range outputFiles {
			port := opts.StartingPort + i
			cmd := exec.Command("go", "tool", "pprof",
				"-http", fmt.Sprintf(":%d", port),
				file)
			waitCommands = append(waitCommands, cmd)

			if err := cmd.Start(); err != nil {
				return err
			}
		}

		for _, cmd := range waitCommands {
			err := cmd.Wait()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Destroy TODO
func Destroy(
	l *logger.Logger, destroyAllMine bool, destroyAllLocal bool, clusterNames ...string,
) error {
	if err := LoadClusters(); err != nil {
		return errors.Wrap(err, "problem loading clusters")
	}
	// We want to avoid running ListCloud() if we are only trying to destroy a
	// local cluster.
	var cld *cloud.Cloud

	switch {
	case destroyAllMine:
		if len(clusterNames) != 0 {
			return errors.New("--all-mine cannot be combined with cluster names")
		}
		if destroyAllLocal {
			return errors.New("--all-mine cannot be combined with --all-local")
		}
		destroyPattern, err := userClusterNameRegexp(l)
		if err != nil {
			return err
		}
		// ListCloud may fail due to a transient provider error, but we may have still
		// found the cluster(s) we care about. Destroy the cluster(s) we know about
		// and let the caller retry.
		cld, _ = cloud.ListCloud(l, vm.ListOptions{IncludeEmptyClusters: true})
		clusters := cld.Clusters.FilterByName(destroyPattern)
		clusterNames = clusters.Names()

	case destroyAllLocal:
		if len(clusterNames) != 0 {
			return errors.New("--all-local cannot be combined with cluster names")
		}

		clusterNames = local.Clusters()

	default:
		if len(clusterNames) == 0 {
			return errors.New("no cluster name provided")
		}
	}

	if err := ctxgroup.GroupWorkers(
		context.TODO(),
		len(clusterNames),
		func(ctx context.Context, idx int) error {
			name := clusterNames[idx]
			if config.IsLocalClusterName(name) {
				return destroyLocalCluster(ctx, l, name)
			}
			if cld == nil {
				// ListCloud may fail due to a transient provider error, but we may have still
				// found the cluster(s) we care about. Destroy the cluster(s) we know about
				// and let the caller retry.
				cld, _ = cloud.ListCloud(l, vm.ListOptions{IncludeEmptyClusters: true})
			}
			return destroyCluster(cld, l, name)
		}); err != nil {
		return err
	}
	l.Printf("OK")
	return nil
}

func destroyCluster(cld *cloud.Cloud, l *logger.Logger, clusterName string) error {
	c, ok := cld.Clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster %s does not exist", clusterName)
	}
	if c.IsEmptyCluster() {
		l.Printf("Destroying empty cluster %s with 0 nodes", clusterName)
	} else {
		l.Printf("Destroying cluster %s with %d nodes", clusterName, len(c.VMs))
	}

	return cloud.DestroyCluster(l, c)
}

func destroyLocalCluster(ctx context.Context, l *logger.Logger, clusterName string) error {
	if _, ok := readSyncedClusters(clusterName); !ok {
		return fmt.Errorf("cluster %s does not exist", clusterName)
	}

	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	if err := c.Wipe(ctx, l, false); err != nil {
		return err
	}
	return local.DeleteCluster(l, clusterName)
}

// ClusterAlreadyExistsError is returned when the cluster name passed to Create is already used by another cluster.
type ClusterAlreadyExistsError struct {
	name string
}

func (e *ClusterAlreadyExistsError) Error() string {
	return fmt.Sprintf("cluster %s already exists", e.name)
}

func cleanupFailedCreate(l *logger.Logger, clusterName string) error {
	c, err := getCluster(l, clusterName)
	if err != nil {
		// If the cluster doesn't exist, we didn't manage to create any VMs
		// before failing. Not an error.
		//nolint:returnerrcheck
		return nil
	}
	return cloud.DestroyCluster(l, c)
}

func AddLabels(l *logger.Logger, clusterName string, labels map[string]string) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}

	return vm.FanOut(c.VMs, func(p vm.Provider, vms vm.List) error {
		return p.AddLabels(l, vms, labels)
	})
}

func RemoveLabels(l *logger.Logger, clusterName string, labels []string) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}

	return vm.FanOut(c.VMs, func(p vm.Provider, vms vm.List) error {
		return p.RemoveLabels(l, vms, labels)
	})
}

// Create TODO
func Create(
	ctx context.Context,
	l *logger.Logger,
	username string,
	numNodes int,
	createVMOpts vm.CreateOpts,
	providerOptsContainer vm.ProviderOptionsContainer,
) (retErr error) {
	if numNodes <= 0 || numNodes >= 1000 {
		// Upper limit is just for safety.
		return fmt.Errorf("number of nodes must be in [1..999]")
	}
	clusterName := createVMOpts.ClusterName
	if err := verifyClusterName(l, clusterName, username); err != nil {
		return err
	}

	isLocal := config.IsLocalClusterName(clusterName)
	if isLocal {
		// To ensure that multiple processes don't create local clusters at
		// the same time (causing port collisions), acquire the lock file.
		unlockFn, err := lock.AcquireFilesystemLock(config.DefaultLockPath)
		if err != nil {
			return err
		}
		defer unlockFn()
	}

	if err := LoadClusters(); err != nil {
		return errors.Wrap(err, "problem loading clusters")
	}

	if !isLocal {
		// ListCloud may fail due to a transient provider error, but
		// we may not even be creating a cluster with that provider.
		// If the cluster does exist, and we didn't find it, it will
		// fail on the provider's end.
		cld, _ := cloud.ListCloud(l, vm.ListOptions{})
		if _, ok := cld.Clusters[clusterName]; ok {
			return &ClusterAlreadyExistsError{name: clusterName}
		}

		defer func() {
			if retErr == nil {
				return
			}
			l.Errorf("Cleaning up partially-created cluster (prev err: %s)", retErr)
			if err := cleanupFailedCreate(l, clusterName); err != nil {
				l.Errorf("Error while cleaning up partially-created cluster: %s", err)
			} else {
				l.Printf("Cleaning up OK")
			}
		}()
	} else {
		if _, ok := readSyncedClusters(clusterName); ok {
			return &ClusterAlreadyExistsError{name: clusterName}
		}

		// If the local cluster is being created, force the local Provider to be used
		createVMOpts.VMProviders = []string{local.ProviderName}
	}

	if createVMOpts.SSDOpts.FileSystem == vm.Zfs {
		for _, provider := range createVMOpts.VMProviders {
			if provider != gce.ProviderName {
				return fmt.Errorf(
					"creating a node with --filesystem=zfs is currently only supported on gce",
				)
			}
		}
	}

	l.Printf("Creating cluster %s with %d nodes...", clusterName, numNodes)
	if createErr := cloud.CreateCluster(l, numNodes, createVMOpts, providerOptsContainer); createErr != nil {
		return createErr
	}

	if config.IsLocalClusterName(clusterName) {
		// No need for ssh for local clusters.
		return LoadClusters()
	}
	l.Printf("Created cluster %s; setting up SSH...", clusterName)
	return SetupSSH(ctx, l, clusterName)
}

func Grow(ctx context.Context, l *logger.Logger, clusterName string, numNodes int) error {
	if numNodes <= 0 || numNodes >= 1000 {
		// Upper limit is just for safety.
		return fmt.Errorf("number of nodes must be in [1..999]")
	}

	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}

	err = cloud.GrowCluster(l, &c.Cluster, numNodes)
	if err != nil {
		return err
	}
	return SetupSSH(ctx, l, clusterName)
}

// GC garbage-collects expired clusters, unused SSH key pairs in AWS, and unused
// DNS records.
func GC(l *logger.Logger, dryrun bool) error {
	if err := LoadClusters(); err != nil {
		return err
	}

	// Use the `addOpFn` helper to run GC operations concurrently and collect
	// errors.
	errorsChan := make(chan error, 8)
	var wg sync.WaitGroup
	addOpFn := func(fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errorsChan <- fn()
		}()
	}

	// GCAwsKeyPairs has no dependencies and can start immediately.
	addOpFn(func() error {
		return cloud.GCAWSKeyPairs(l, dryrun)
	})

	// ListCloud may fail for a provider, but we can still attempt GC on
	// the clusters we do have.
	cld, _ := cloud.ListCloud(l, vm.ListOptions{IncludeEmptyClusters: true})
	addOpFn(func() error {
		return cloud.GCClusters(l, cld, dryrun)
	})
	addOpFn(func() error {
		return cloud.GCDNS(l, cld, dryrun)
	})

	// Wait for all operations to finish and combine all errors.
	go func() {
		wg.Wait()
		close(errorsChan)
	}()
	var combinedErrors error
	for err := range errorsChan {
		combinedErrors = errors.CombineErrors(combinedErrors, err)
	}
	return combinedErrors
}

// LogsOpts TODO
type LogsOpts struct {
	Dir, Filter, ProgramFilter string
	Interval                   time.Duration
	From, To                   time.Time
	Out                        io.Writer
}

// Logs TODO
func Logs(l *logger.Logger, clusterName, dest, username string, logsOpts LogsOpts) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	return c.Logs(
		l, logsOpts.Dir, dest, username, logsOpts.Filter, logsOpts.ProgramFilter,
		logsOpts.Interval, logsOpts.From, logsOpts.To, logsOpts.Out,
	)
}

// StageURL TODO
func StageURL(
	l *logger.Logger, applicationName, version, stageOS string, stageArch string,
) ([]*url.URL, error) {
	os := runtime.GOOS
	if stageOS != "" {
		os = stageOS
	}
	arch := runtime.GOARCH
	if stageArch != "" {
		arch = stageArch
	}
	urls, err := install.URLsForApplication(applicationName, version, os, vm.CPUArch(arch))
	if err != nil {
		return nil, err
	}
	return urls, nil
}

// InitProviders initializes providers and returns a map that indicates
// if a provider is active or inactive.
func InitProviders() map[string]string {
	providersState := make(map[string]string)

	if err := aws.Init(); err != nil {
		providersState[aws.ProviderName] = "Inactive - " + err.Error()
	} else {
		providersState[aws.ProviderName] = "Active"
	}

	if err := gce.Init(); err != nil {
		providersState[gce.ProviderName] = "Inactive - " + err.Error()
	} else {
		providersState[gce.ProviderName] = "Active"
	}

	if err := azure.Init(); err != nil {
		providersState[azure.ProviderName] = "Inactive - " + err.Error()
	} else {
		providersState[azure.ProviderName] = "Active"
	}

	if err := local.Init(localVMStorage{}); err != nil {
		providersState[local.ProviderName] = "Inactive - " + err.Error()
	} else {
		providersState[local.ProviderName] = "Active"
	}

	return providersState
}

// StartGrafana spins up a prometheus and grafana instance on the last node provided and scrapes
// from all other nodes.
func StartGrafana(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	arch vm.CPUArch,
	grafanaURL string,
	grafanaJSON []string,
	promCfg *prometheus.Config, // passed iff grafanaURL is empty
) error {
	if (grafanaURL != "" || len(grafanaJSON) > 0) && promCfg != nil {
		return errors.New("cannot pass grafanaURL or grafanaJSON and a non empty promCfg")
	}
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	nodes, err := install.ListNodes("all", len(c.VMs))
	if err != nil {
		return err
	}

	if promCfg == nil {
		promCfg = &prometheus.Config{}
		// Configure the prometheus/grafana servers to run on the last node in the cluster
		promCfg.WithPrometheusNode(nodes[len(nodes)-1])

		// Configure scraping on all nodes in the cluster
		promCfg.WithCluster(nodes)
		promCfg.WithNodeExporter(nodes)
		// Scrape all workload prometheus ports, just in case.
		for _, i := range nodes {
			promCfg.WithWorkload(fmt.Sprintf("workload_on_n%d", i), i, 0 /* use default port */)
		}

		// By default, spin up a grafana server
		promCfg.Grafana.Enabled = true
		if grafanaURL != "" {
			promCfg.WithGrafanaDashboard(grafanaURL)
		}
		for _, str := range grafanaJSON {
			promCfg.WithGrafanaDashboardJSON(str)
		}
	}
	_, err = prometheus.Init(ctx, l, c, arch, *promCfg)
	if err != nil {
		return err
	}
	url, err := GrafanaURL(ctx, l, clusterName, false)
	if err != nil {
		return err
	}
	l.Printf("Grafana dashboard: %s", url)
	return nil
}

// StopGrafana shuts down prometheus and grafana servers on the last node in
// the cluster, if they exist.
func StopGrafana(ctx context.Context, l *logger.Logger, clusterName string, dumpDir string) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	nodes, err := install.ListNodes("all", len(c.VMs))
	if err != nil {
		return err
	}
	if err := prometheus.Shutdown(ctx, c, l, nodes, dumpDir); err != nil {
		return err
	}
	return nil
}

// GrafanaURL returns a url to the grafana dashboard
func GrafanaURL(
	ctx context.Context, l *logger.Logger, clusterName string, openInBrowser bool,
) (string, error) {
	if err := LoadClusters(); err != nil {
		return "", err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return "", err
	}
	nodes, err := install.ListNodes("all", len(c.VMs))
	if err != nil {
		return "", err
	}
	// grafana is assumed to be running on the last node in the target
	grafanaNode := install.Nodes{nodes[len(nodes)-1]}

	uConfig := urlConfig{
		usePublicIP:   true,
		openInBrowser: openInBrowser,
		secure:        false,
		port:          3000,
	}
	urls, err := urlGenerator(ctx, c, l, grafanaNode, uConfig)
	if err != nil {
		return "", err
	}
	return urls[0], nil
}

func AddGrafanaAnnotation(
	ctx context.Context, host string, secure bool, req grafana.AddAnnotationRequest,
) error {
	return grafana.AddAnnotation(ctx, host, secure, req)
}

// PrometheusSnapshot takes a snapshot of prometheus and stores the snapshot and
// a script to spin up a docker instance for it to the given directory. We
// assume the last node contains the prometheus server.
func PrometheusSnapshot(
	ctx context.Context, l *logger.Logger, clusterName string, dumpDir string,
) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	nodes, err := install.ListNodes("all", len(c.VMs))
	if err != nil {
		return err
	}

	promNode := install.Nodes{nodes[len(nodes)-1]}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := prometheus.Snapshot(ctx, c, l, promNode, dumpDir); err != nil {
		l.Printf("failed to get prometheus snapshot: %v", err)
		return err
	}
	return nil
}

// SnapshotTTL controls how long volume snapshots are kept around.
const SnapshotTTL = 30 * 24 * time.Hour // 30 days

// CreateSnapshot snapshots all the persistent volumes attached to nodes in the
// named cluster.
func CreateSnapshot(
	ctx context.Context, l *logger.Logger, clusterName string, vsco vm.VolumeSnapshotCreateOpts,
) ([]vm.VolumeSnapshot, error) {
	if err := LoadClusters(); err != nil {
		return nil, err
	}

	c, err := newCluster(l, clusterName)
	if err != nil {
		return nil, err
	}

	nodes := c.TargetNodes()
	nodesStatus, err := c.Status(ctx, l)

	if err != nil {
		return nil, err
	}

	// 1-indexed node IDs.
	statusByNodeID := make(map[int]install.NodeStatus)
	for _, status := range nodesStatus {
		statusByNodeID[status.NodeID] = status
	}

	// TODO(irfansharif): Add validation that we're using some released version,
	// probably the predecessor one. Also ensure that any running CRDB processes
	// have been stopped since we're taking raw disk snapshots cluster-wide.

	volumesSnapshotMu := struct {
		syncutil.Mutex
		snapshots []vm.VolumeSnapshot
	}{}
	if err := c.Parallel(ctx, l, install.WithNodes(nodes),
		func(ctx context.Context, node install.Node) (*install.RunResultDetails, error) {
			res := &install.RunResultDetails{Node: node}

			cVM := c.VMs[node-1]
			crdbVersion := statusByNodeID[int(node)].Version
			if crdbVersion == "" {
				crdbVersion = "unknown"
			}
			crdbVersion = strings.TrimPrefix(crdbVersion, "cockroach-")
			// N.B. snapshot name cannot exceed 63 characters, so we use short sha for dev version.
			if index := strings.Index(crdbVersion, "dev-"); index != -1 {
				sha := crdbVersion[index+4:]
				if len(sha) > 7 {
					crdbVersion = crdbVersion[:index+4] + sha[:7]
				}
			}

			labels := map[string]string{
				"roachprod-node-src-spec": cVM.MachineType,
				"roachprod-cluster-node":  cVM.Name,
				"roachprod-crdb-version":  crdbVersion,
				vm.TagCluster:             clusterName,
				vm.TagRoachprod:           "true",
				vm.TagLifetime:            SnapshotTTL.String(),
				vm.TagCreated: strings.ToLower(
					strings.ReplaceAll(timeutil.Now().Format(time.RFC3339), ":", "_")), // format according to gce label naming requirements
			}
			for k, v := range vsco.Labels {
				labels[k] = v
			}

			if err := vm.ForProvider(cVM.Provider, func(provider vm.Provider) error {
				volumes, err := provider.ListVolumes(l, &cVM)
				if err != nil {
					return err
				}

				if len(volumes) == 0 {
					return fmt.Errorf("node %d does not have any non-bootable persistent volumes attached", node)
				}

				for _, volume := range volumes {
					snapshotFingerprintInfix := strings.ReplaceAll(
						fmt.Sprintf("%s-n%d", crdbVersion, len(nodes)), ".", "-")
					snapshotName := fmt.Sprintf("%s-%s-%04d", vsco.Name, snapshotFingerprintInfix, node)
					if len(snapshotName) > 63 {
						return fmt.Errorf("snapshot name %q exceeds 63 characters; shorten name prefix and use description arg. for more context", snapshotName)
					}
					volumeSnapshot, err := provider.CreateVolumeSnapshot(l, volume,
						vm.VolumeSnapshotCreateOpts{
							Name:        snapshotName,
							Labels:      labels,
							Description: vsco.Description,
						})
					if err != nil {
						return err
					}
					l.Printf("created volume snapshot %s (id=%s) for volume %s on %s/n%d\n",
						volumeSnapshot.Name, volumeSnapshot.ID, volume.Name, volume.ProviderResourceID, node)
					volumesSnapshotMu.Lock()
					volumesSnapshotMu.snapshots = append(volumesSnapshotMu.snapshots, volumeSnapshot)
					volumesSnapshotMu.Unlock()
				}
				return nil
			}); err != nil {
				res.Err = err
			}
			return res, nil
		}); err != nil {
		return nil, err
	}

	sort.Sort(vm.VolumeSnapshots(volumesSnapshotMu.snapshots))
	return volumesSnapshotMu.snapshots, nil
}

func ListSnapshots(
	ctx context.Context, l *logger.Logger, provider string, vslo vm.VolumeSnapshotListOpts,
) ([]vm.VolumeSnapshot, error) {
	var volumeSnapshots []vm.VolumeSnapshot
	if err := vm.ForProvider(provider, func(provider vm.Provider) error {
		var err error
		volumeSnapshots, err = provider.ListVolumeSnapshots(l, vslo)
		return err
	}); err != nil {
		return nil, err
	}
	return volumeSnapshots, nil
}

func DeleteSnapshots(
	ctx context.Context, l *logger.Logger, provider string, snapshots ...vm.VolumeSnapshot,
) error {
	return vm.ForProvider(provider, func(provider vm.Provider) error {
		return provider.DeleteVolumeSnapshots(l, snapshots...)
	})
}

func ApplySnapshots(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	snapshots []vm.VolumeSnapshot,
	opts vm.VolumeCreateOpts,
) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}

	if n := len(c.TargetNodes()); n != len(snapshots) {
		return fmt.Errorf("mismatched number of snapshots (%d) to node count (%d)", len(snapshots), n)
		// TODO(irfansharif): Validate labels (version, instance types).
	}

	// Detach and delete existing volumes. This is destructive.
	if err := c.Parallel(ctx, l, install.WithNodes(c.TargetNodes()),
		func(ctx context.Context, node install.Node) (*install.RunResultDetails, error) {
			res := &install.RunResultDetails{Node: node}

			cVM := &c.VMs[node-1]
			if err := vm.ForProvider(cVM.Provider, func(provider vm.Provider) error {
				volumes, err := provider.ListVolumes(l, cVM)
				if err != nil {
					return err
				}
				for _, volume := range volumes {
					if err := provider.DeleteVolume(l, volume, cVM); err != nil {
						return err
					}
					l.Printf("detached and deleted volume %s from %s", volume.ProviderResourceID, cVM.Name)
				}
				return nil
			}); err != nil {
				res.Err = err
			}
			return res, nil
		}); err != nil {
		return err
	}

	return c.Parallel(ctx, l, install.WithNodes(c.TargetNodes()),
		func(ctx context.Context, node install.Node) (*install.RunResultDetails, error) {
			res := &install.RunResultDetails{Node: node}

			volumeOpts := opts // make a copy
			volumeOpts.Labels = map[string]string{}
			for k, v := range opts.Labels {
				volumeOpts.Labels[k] = v
			}

			// TODO: same issue as above if the target nodes are not sequential starting from 1
			cVM := &c.VMs[node-1]
			if err := vm.ForProvider(cVM.Provider, func(provider vm.Provider) error {
				volumeOpts.Zone = cVM.Zone
				// NB: The "-1" signifies that it's the first attached non-boot volume.
				// This is typical naming convention in GCE clusters.
				volumeOpts.Name = fmt.Sprintf("%s-%04d-1", clusterName, node)
				volumeOpts.SourceSnapshotID = snapshots[node-1].ID

				volumes, err := provider.ListVolumes(l, cVM)
				if err != nil {
					return err
				}
				for _, vol := range volumes {
					if vol.Name == volumeOpts.Name {
						l.Printf(
							"volume (%s) is already attached to node %d skipping volume creation", vol.ProviderResourceID, node)
						return nil
					}
				}

				volumeOpts.Labels[vm.TagCluster] = clusterName
				volumeOpts.Labels[vm.TagLifetime] = cVM.Lifetime.String()
				volumeOpts.Labels[vm.TagRoachprod] = "true"
				volumeOpts.Labels[vm.TagCreated] = strings.ToLower(
					strings.ReplaceAll(timeutil.Now().Format(time.RFC3339), ":", "_")) // format according to gce label naming requirements

				volume, err := provider.CreateVolume(l, volumeOpts)
				if err != nil {
					return err
				}
				l.Printf("created volume %s", volume.ProviderResourceID)

				device, err := cVM.AttachVolume(l, volume)
				if err != nil {
					return err
				}
				l.Printf("attached volume %s to %s", volume.ProviderResourceID, cVM.ProviderID)

				// Save the cluster to cache.
				if err := saveCluster(l, &c.Cluster); err != nil {
					return err
				}

				var buf bytes.Buffer
				if err := c.Run(ctx, l, &buf, &buf, install.WithNodes([]install.Node{node}),
					"mounting volume", genMountCommands(device, "/mnt/data1")); err != nil {
					l.Printf(buf.String())
					return err
				}
				l.Printf("mounted %s to %s", volume.ProviderResourceID, cVM.ProviderID)

				return nil
			}); err != nil {
				res.Err = err
			}
			return res, nil
		})
}

func genMountCommands(devicePath, mountDir string) string {
	return strings.Join([]string{
		"sudo mkdir -p " + mountDir,
		"sudo mount -o discard,defaults " + devicePath + " " + mountDir,
		"sudo chmod 0777 " + mountDir,
	}, " && ")
}

func isWorkloadCollectorVolume(v vm.Volume) bool {
	if v, ok := v.Labels["roachprod_collector"]; ok && v == "true" {
		return true
	}
	return false
}

const (
	otelCollectorPort   = 4317
	jaegerUIPort        = 16686
	jaegerContainerName = "jaeger"
	jaegerImageName     = "jaegertracing/all-in-one:latest"
)

// StartJaeger starts a jaeger instance on the last node in the given
// cluster and configures the cluster to use it.
func StartJaeger(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	virtualClusterName string,
	secure bool,
	configureNodes string,
) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName, install.SecureOption(secure))
	if err != nil {
		return err
	}

	// TODO(ssd): Currently this just uses the all-in-one docker
	// container with in memory storage. Might be nicer to just
	// install from source or get linux binaries and start them
	// with systemd. For now this just matches what we've been
	// copy and pasting.
	jaegerNode := c.TargetNodes()[len(c.TargetNodes())-1:]
	err = install.InstallTool(ctx, l, c, jaegerNode, "docker", l.Stdout, l.Stderr)
	if err != nil {
		return err
	}
	startCmd := fmt.Sprintf("docker run -d --name %s -p %[2]d:%[2]d -p %[3]d:%[3]d %s",
		jaegerContainerName,
		otelCollectorPort,
		jaegerUIPort,
		jaegerImageName)
	err = c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(jaegerNode), "start jaegertracing/all-in-one using docker", startCmd)
	if err != nil {
		return err
	}

	otelCollectionHost, err := c.GetInternalIP(jaegerNode[0])
	if err != nil {
		return err
	}
	otelCollectionHostPort := net.JoinHostPort(otelCollectionHost, strconv.Itoa(otelCollectorPort))
	setupStmt := fmt.Sprintf("SET CLUSTER SETTING trace.opentelemetry.collector='%s'", otelCollectionHostPort)

	if configureNodes != "" {
		nodes, err := install.ListNodes(configureNodes, len(c.VMs))
		if err != nil {
			return err
		}
		_, err = c.ExecSQL(ctx, l, nodes, virtualClusterName, 0, []string{"-e", setupStmt})
		if err != nil {
			return err
		}
	}

	url, err := JaegerURL(ctx, l, clusterName, false)
	if err != nil {
		return err
	}

	l.Printf("To use with CRDB: %s", setupStmt)
	l.Printf("Jaeger UI: %s", url)
	return nil
}

// StopJaeger stops and removes the jaeger container.
func StopJaeger(ctx context.Context, l *logger.Logger, clusterName string) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	jaegerNode := c.TargetNodes()[len(c.TargetNodes())-1:]
	stopCmd := fmt.Sprintf("docker stop %s", jaegerContainerName)
	err = c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(jaegerNode), stopCmd, stopCmd)
	if err != nil {
		return err
	}
	rmCmd := fmt.Sprintf("docker rm %s", jaegerContainerName)
	return c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(jaegerNode), rmCmd, rmCmd)
}

// JaegerURL returns a url to the jaeger UI, assuming it was installed
// on the lat node in the given cluster.
func JaegerURL(
	ctx context.Context, l *logger.Logger, clusterName string, openInBrowser bool,
) (string, error) {
	if err := LoadClusters(); err != nil {
		return "", err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return "", err
	}
	jaegerNode := c.TargetNodes()[len(c.TargetNodes())-1:]
	urls, err := urlGenerator(ctx, c, l, jaegerNode, urlConfig{
		usePublicIP:   true,
		openInBrowser: openInBrowser,
		secure:        false,
		port:          jaegerUIPort,
	})
	if err != nil {
		return "", err
	}
	return urls[0], nil
}

// DestroyDNS destroys the DNS records for the given cluster.
func DestroyDNS(ctx context.Context, l *logger.Logger, clusterName string) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	return vm.FanOutDNS(c.VMs, func(p vm.DNSProvider, vms vm.List) error {
		return p.DeleteRecordsBySubdomain(ctx, c.Name)
	})
}

// StorageCollectionPerformAction either starts or stops workload collection on
// a target cluster.
//
// On start it attaches a volume to each of the nodes specified in the cluster
// specifications and sends an HTTP request to the nodes. The nodes must be
// started with the COCKROACH_STORAGE_WORKLOAD_COLLECTOR environment variable.
// Otherwise, the HTTP endpoint will not be setup. Once a node receives the
// request it will perform a checkpoint which can take several minutes to
// complete. Until the checkpoint finishes the request will block. See
// HandleRequest() in pkg/server/debug/replay/replay.go for additional details.
// On stop this sends an HTTP request to each of the nodes in the cluster
// specification. On list-volumes it will read the local cache for the cluster
// to output the list of volumes attached to the nodes.
func StorageCollectionPerformAction(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	action string,
	opts vm.VolumeCreateOpts,
) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}

	if opts.Labels == nil {
		opts.Labels = map[string]string{}
	}
	opts.Labels["roachprod_collector"] = "true"
	mountDir := "/mnt/capture/"
	switch action {
	case "start":
		if err := createAttachMountVolumes(ctx, l, c, opts, mountDir); err != nil {
			return err
		}
	case "stop":
	case "list-volumes":
		printNodeToVolumeMapping(c)
		return nil
	default:
		return errors.Errorf("Expected one of start or stop as the action got: %s", action)
	}

	printNodeToVolumeMapping(c)
	return sendCaptureCommand(ctx, l, c, action, mountDir)
}

func printNodeToVolumeMapping(c *install.SyncedCluster) {
	nodes := c.TargetNodes()
	for _, n := range nodes {
		cVM := c.VMs[n-1]
		for _, volume := range cVM.NonBootAttachedVolumes {
			if isWorkloadCollectorVolume(volume) {
				fmt.Printf("Node ID: %d (Name: %s) -> Volume Name: %s (ID: %s)\n", n, cVM.Name, volume.Name, volume.ProviderResourceID)
			}
		}
	}
}

func sendCaptureCommand(
	ctx context.Context, l *logger.Logger, c *install.SyncedCluster, action string, captureDir string,
) error {
	nodes := c.TargetNodes()
	httpClient := httputil.NewClientWithTimeout(0 /* timeout: None */)
	_, _, err := c.ParallelE(ctx, l, install.WithNodes(nodes).WithDisplay(fmt.Sprintf("Performing workload capture %s", action)),
		func(ctx context.Context, node install.Node) (*install.RunResultDetails, error) {
			port, err := c.NodeUIPort(ctx, node, "" /* virtualClusterName */, 0 /* sqlInstance */)
			if err != nil {
				return nil, err
			}
			res := &install.RunResultDetails{Node: node}
			host := c.Host(node)
			scheme := "http"
			if c.Secure {
				scheme = "https"
			}

			debugUrl := url.URL{
				Scheme: scheme,
				Host:   net.JoinHostPort(host, strconv.Itoa(port)),
				Path:   "/debug/workload_capture",
			}

			r, err := httpClient.Get(ctx, debugUrl.String())
			if err != nil {
				res.Err = errors.New("Failed to retrieve current store workload collection state")
				return res, res.Err
			}
			storeState := replay.ResponseType{}
			err = json.NewDecoder(r.Body).Decode(&storeState)
			if err != nil {
				res.Err = errors.New("Failed to decode response from node")
				return res, res.Err
			}

			for _, info := range storeState.Data {
				wpa := replay.WorkloadCollectorPerformActionRequest{
					StoreID: info.StoreID,
					Action:  action,
				}
				if captureDir != "" {
					wpa.CaptureDirectory = path.Join(
						captureDir,
						"store_"+strconv.Itoa(info.StoreID),
						timeutil.Now().Format("20060102150405"),
					)
				}

				jsonValue, err := json.Marshal(wpa)
				if err != nil {
					res.Err = err
					return res, res.Err
				}

				response, err := httpClient.Post(ctx, debugUrl.String(), httputil.JSONContentType, bytes.NewBuffer(jsonValue))
				if err != nil {
					res.Err = err
					return res, res.Err
				}

				if response.StatusCode != http.StatusOK {
					serverErrorMessage, err := io.ReadAll(response.Body)
					if err != nil {
						res.Err = err
						return res, res.Err
					}
					res.Err = errors.Newf("%s", string(serverErrorMessage))
					return res, res.Err
				}
			}
			return res, res.Err
		})
	return err
}

func createAttachMountVolumes(
	ctx context.Context,
	l *logger.Logger,
	c *install.SyncedCluster,
	opts vm.VolumeCreateOpts,
	mountDir string,
) error {
	nodes := c.TargetNodes()
	for idx, n := range nodes {
		curNode := nodes[idx : idx+1]

		cVM := &c.VMs[n-1]
		err := vm.ForProvider(cVM.Provider, func(provider vm.Provider) error {
			opts.Name = fmt.Sprintf("%s-n%d", c.Name, n)
			for _, vol := range cVM.NonBootAttachedVolumes {
				if vol.Name == opts.Name {
					l.Printf(
						"A volume (%s) is already attached to node %d skipping volume creation", vol.ProviderResourceID, n)
					return nil
				}
			}
			opts.Zone = cVM.Zone

			volume, err := provider.CreateVolume(l, opts)
			if err != nil {
				return err
			}
			l.Printf("Created Volume %s", volume.ProviderResourceID)
			device, err := cVM.AttachVolume(l, volume)
			if err != nil {
				return err
			}
			// Save the cluster to cache
			err = saveCluster(l, &c.Cluster)
			if err != nil {
				return err
			}
			l.Printf("Attached Volume %s to %s", volume.ProviderResourceID, cVM.ProviderID)
			err = c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(curNode),
				"Mounting volume", genMountCommands(device, mountDir))
			return err
		})

		if err != nil {
			return err
		}
		l.Printf("Successfully mounted volume to %s", cVM.ProviderID)
	}
	return nil
}

// CreateLoadBalancer creates a load balancer for the SQL service on the given
// cluster. Currently only supports GCE.
func CreateLoadBalancer(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	secure bool,
	virtualClusterName string,
	sqlInstance int,
) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName, install.SecureOption(secure))
	if err != nil {
		return err
	}

	// If virtualClusterName is not provided, use the system interface name.
	if virtualClusterName == "" {
		virtualClusterName = install.SystemInterfaceName
	}

	// Find the SQL ports for the service on all nodes.
	services, err := c.DiscoverServices(
		ctx, virtualClusterName, install.ServiceTypeSQL,
		install.ServiceNodePredicate(c.TargetNodes()...), install.ServiceInstancePredicate(sqlInstance),
	)
	if err != nil {
		return err
	}

	port := config.DefaultSQLPort
	if len(services) == 0 {
		l.Errorf("WARNING: %s SQL service not found on cluster %s, using default SQL port %d",
			virtualClusterName, clusterName, port)
	} else {
		port = services[0].Port
		// Confirm that the service has the same port on all nodes.
		for _, service := range services[1:] {
			if port != service.Port {
				return errors.Errorf("service %s must share the same port on all nodes, different ports found %d and %d",
					virtualClusterName, port, service.Port)
			}
		}
	}

	// Create a load balancer for the service's port.
	err = vm.FanOut(c.VMs, func(provider vm.Provider, vms vm.List) error {
		createErr := provider.CreateLoadBalancer(l, vms, port)
		if createErr != nil {
			l.Errorf("Cleaning up partially-created load balancer (prev err: %s)", createErr)
			cleanupErr := provider.DeleteLoadBalancer(l, vms, port)
			if cleanupErr != nil {
				l.Errorf("Error while cleaning up partially-created load balancer: %s", cleanupErr)
			} else {
				l.Printf("Cleaned up partially-created load balancer")
			}
			return errors.CombineErrors(createErr, cleanupErr)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// For secure clusters, the load balancer IP needs to be added to the
	// cluster's certificate.
	if secure {
		err = c.RedistributeNodeCert(ctx, l)
		if err != nil {
			return err
		}
		// Send a SIGHUP to the nodes to reload the certificates.
		err = c.Signal(ctx, l, int(unix.SIGHUP))
		if err != nil {
			return err
		}
	}
	return nil
}

func getCluster(l *logger.Logger, clusterName string) (*cloud.Cluster, error) {
	// ListCloud may fail due to a transient provider error, but
	// we may have still found the cluster we care about. It will
	// fail below if it can't find the cluster.
	cld, err := cloud.ListCloud(l, vm.ListOptions{})
	c, ok := cld.Clusters[clusterName]
	if !ok {
		if err != nil {
			return &cloud.Cluster{}, errors.Wrapf(err, "cluster %s not found", clusterName)
		}
		return &cloud.Cluster{}, fmt.Errorf("cluster %s does not exist", clusterName)
	}

	return c, nil
}
