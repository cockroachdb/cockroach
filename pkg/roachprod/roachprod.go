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
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
		active, err := vm.FindActiveAccounts()
		if err != nil {
			return err
		}
		for _, account := range active {
			if !seenAccounts[account] {
				seenAccounts[account] = true
				cleanAccount := vm.DNSSafeAccount(account)
				if cleanAccount != account {
					log.Infof(context.TODO(), "WARN: using `%s' as username instead of `%s'", cleanAccount, account)
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
	accounts, err := vm.FindActiveAccounts()
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
func CachedClusters(l *logger.Logger, fn func(clusterName string, numVMs int)) {
	for _, name := range sortedClusters() {
		c, ok := readSyncedClusters(name)
		if !ok {
			return
		}
		fn(c.Name, len(c.VMs))
	}
}

// acquireFilesystemLock acquires a filesystem lock so that two concurrent
// synchronizations of roachprod state don't clobber each other.
func acquireFilesystemLock() (unlockFn func(), _ error) {
	lockFile := os.ExpandEnv("$HOME/.roachprod/LOCK")
	f, err := os.Create(lockFile)
	if err != nil {
		return nil, errors.Wrapf(err, "creating lock file %q", lockFile)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		f.Close()
		return nil, errors.Wrap(err, "acquiring lock on %q")
	}
	return func() {
		f.Close()
	}, nil
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
	unlock, err := acquireFilesystemLock()
	if err != nil {
		return nil, err
	}
	defer unlock()

	cld, err := cloud.ListCloud(l, options)
	if err != nil {
		return nil, err
	}
	if err := syncClustersCache(cld); err != nil {
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
			fmt.Fprintf(l.Stderr, "failed to update %s DNS: %v", gce.Subdomain, err)
		}
	} else {
		if !config.Quiet {
			l.Printf("Not refreshing DNS entries. We did not have all the VMs.")
		}
	}

	if err := vm.ProvidersSequential(vm.AllProviderNames(), func(p vm.Provider) error {
		return p.CleanSSH()
	}); err != nil {
		return nil, err
	}

	return cld, nil
}

// List returns a cloud.Cloud struct of all roachprod clusters matching clusterNamePattern.
// Alternatively, the 'listMine' option can be provided to get the clusters that are owned
// by the current user.
func List(l *logger.Logger, listMine bool, clusterNamePattern string) (cloud.Cloud, error) {
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

	cld, err := Sync(l, vm.ListOptions{})
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

// Run runs a command on the nodes in a cluster.
func Run(
	ctx context.Context,
	l *logger.Logger,
	clusterName, SSHOptions, processTag string,
	secure bool,
	stdout, stderr io.Writer,
	cmdArray []string,
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

	cmd := strings.TrimSpace(strings.Join(cmdArray, " "))
	title := cmd
	if len(title) > 30 {
		title = title[:27] + "..."
	}
	return c.Run(ctx, l, stdout, stderr, c.Nodes, title, cmd)
}

// RunWithDetails runs a command on the nodes in a cluster.
func RunWithDetails(
	ctx context.Context,
	l *logger.Logger,
	clusterName, SSHOptions, processTag string,
	secure bool,
	cmdArray []string,
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

	cmd := strings.TrimSpace(strings.Join(cmdArray, " "))
	title := cmd
	if len(title) > 30 {
		title = title[:27] + "..."
	}
	return c.RunWithDetails(ctx, l, c.Nodes, title, cmd)
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
		return c.ExecOrInteractiveSQL(ctx, l, tenantName, cmdArray)
	}
	return c.ExecSQL(ctx, l, tenantName, cmdArray)
}

// IP gets the ip addresses of the nodes in a cluster.
func IP(
	ctx context.Context, l *logger.Logger, clusterName string, external bool,
) ([]string, error) {
	if err := LoadClusters(); err != nil {
		return nil, err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return nil, err
	}

	nodes := c.TargetNodes()
	ips := make([]string, len(nodes))

	if external {
		for i := 0; i < len(nodes); i++ {
			ips[i] = c.VMs[nodes[i]-1].PublicIP
		}
	} else {
		var err error
		if err := c.Parallel(l, "", len(nodes), 0, func(i int) (*install.RunResultDetails, error) {
			node := nodes[i]
			res := &install.RunResultDetails{Node: node}
			res.Stdout, res.Err = c.GetInternalIP(l, ctx, node)
			ips[i] = res.Stdout
			return res, err
		}, install.DefaultSSHRetryOpts); err != nil {
			return nil, err
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
	stageOS, stageDir, applicationName, version string,
) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}

	os := "linux"
	if stageOS != "" {
		os = stageOS
	} else if c.IsLocal() {
		os = runtime.GOOS
	}

	dir := "."
	if stageDir != "" {
		dir = stageDir
	}

	return install.StageApplication(ctx, l, c, applicationName, version, os, dir)
}

// Reset resets all VMs in a cluster.
func Reset(l *logger.Logger, clusterName string) error {
	if err := LoadClusters(); err != nil {
		return err
	}

	if config.IsLocalClusterName(clusterName) {
		return nil
	}

	cld, err := cloud.ListCloud(l, vm.ListOptions{})
	if err != nil {
		return err
	}
	c, ok := cld.Clusters[clusterName]
	if !ok {
		return errors.New("cluster not found")
	}

	return vm.FanOut(c.VMs, func(p vm.Provider, vms vm.List) error {
		return p.Reset(vms)
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
		return p.ConfigSSH(zones[p.Name()])
	}); err != nil {
		return err
	}

	cloudCluster.PrintDetails(l)
	// Run ssh-keygen -R serially on each new VM in case an IP address has been recycled
	for _, v := range cloudCluster.VMs {
		cmd := exec.Command("ssh-keygen", "-R", v.PublicIP)
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Infof(context.TODO(), "could not clear ssh key for hostname %s:\n%s", v.PublicIP, string(out))
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
	// For GCP clusters we need to use the config.OSUser even if the client
	// requested the shared user.
	for i := range installCluster.VMs {
		if cloudCluster.VMs[i].Provider == gce.ProviderName {
			installCluster.VMs[i].RemoteUser = config.OSUser.Username
		}
	}
	if err := installCluster.Wait(ctx, l); err != nil {
		return err
	}
	// Fetch public keys from gcloud to set up ssh access for all users into the
	// shared ubuntu user.
	installCluster.AuthorizedKeys, err = gce.GetUserAuthorizedKeys()
	if err != nil {
		return errors.Wrap(err, "failed to retrieve authorized keys from gcloud")
	}
	return installCluster.SetupSSH(ctx, l)
}

// Extend extends the lifetime of the specified cluster to prevent it from being destroyed.
func Extend(l *logger.Logger, clusterName string, lifetime time.Duration) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	cld, err := cloud.ListCloud(l, vm.ListOptions{})
	if err != nil {
		return err
	}

	c, ok := cld.Clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster %s does not exist", clusterName)
	}

	if err := cloud.ExtendCluster(c, lifetime); err != nil {
		return err
	}

	// Reload the clusters and print details.
	cld, err = cloud.ListCloud(l, vm.ListOptions{})
	if err != nil {
		return err
	}

	c, ok = cld.Clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster %s does not exist", clusterName)
	}

	c.PrintDetails(l)
	return nil
}

// DefaultStartOpts returns a StartOpts populated with default values.
func DefaultStartOpts() install.StartOpts {
	return install.StartOpts{
		Sequential:         true,
		EncryptedStores:    false,
		NumFilesLimit:      config.DefaultNumFilesLimit,
		SkipInit:           false,
		StoreCount:         1,
		TenantID:           2,
		ScheduleBackups:    false,
		ScheduleBackupArgs: "",
		InitTarget:         1,
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
	return c.Stop(ctx, l, opts.Sig, opts.Wait, opts.MaxWait)
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

	err = c.Run(ctx, l, os.Stdout, os.Stderr, c.Nodes, "reformatting", fmt.Sprintf(`
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
	return install.Install(ctx, l, c, software)
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
func Get(l *logger.Logger, clusterName, src, dest string) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	return c.Get(l, c.Nodes, src, dest)
}

type PGURLOptions struct {
	Secure     bool
	External   bool
	TenantName string
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
		var err error
		if err := c.Parallel(l, "", len(nodes), 0, func(i int) (*install.RunResultDetails, error) {
			node := nodes[i]
			res := &install.RunResultDetails{Node: node}
			res.Stdout, res.Err = c.GetInternalIP(l, ctx, node)
			ips[i] = res.Stdout
			return res, err
		}, install.DefaultSSHRetryOpts); err != nil {
			return nil, err
		}
	}

	var urls []string
	for i, ip := range ips {
		if ip == "" {
			return nil, errors.Errorf("empty ip: %v", ips)
		}
		urls = append(urls, c.NodeURL(ip, c.NodePort(nodes[i]), opts.TenantName))
	}
	if len(urls) != len(nodes) {
		return nil, errors.Errorf("have nodes %v, but urls %v from ips %v", nodes, urls, ips)
	}
	return urls, nil
}

type urlConfig struct {
	path          string
	usePublicIP   bool
	openInBrowser bool
	secure        bool
	port          int
}

func urlGenerator(
	c *install.SyncedCluster, l *logger.Logger, nodes install.Nodes, config urlConfig,
) ([]string, error) {
	var urls []string
	for i, node := range nodes {
		host := vm.Name(c.Name, int(node)) + "." + gce.Subdomain

		// verify DNS is working / fallback to IPs if not.
		if i == 0 && !config.usePublicIP {
			if _, err := net.LookupHost(host); err != nil {
				fmt.Fprintf(l.Stderr, "no valid DNS (yet?). might need to re-run `sync`?\n")
				config.usePublicIP = true
			}
		}

		if config.usePublicIP {
			host = c.VMs[node-1].PublicIP
		}
		if config.port == 0 {
			config.port = c.NodeUIPort(node)
		}
		scheme := "http"
		if c.Secure {
			scheme = "https"
		}
		if !strings.HasPrefix(config.path, "/") {
			config.path = "/" + config.path
		}
		url := fmt.Sprintf("%s://%s:%d%s", scheme, host, config.port, config.path)
		urls = append(urls, url)
		if config.openInBrowser {
			if err := exec.Command("python", "-m", "webbrowser", url).Run(); err != nil {
				return nil, err
			}
		}
	}
	return urls, nil
}

// AdminURL generates admin UI URLs for the nodes in a cluster.
func AdminURL(
	l *logger.Logger, clusterName, path string, usePublicIP, openInBrowser, secure bool,
) ([]string, error) {
	if err := LoadClusters(); err != nil {
		return nil, err
	}
	c, err := newCluster(l, clusterName, install.SecureOption(secure))
	if err != nil {
		return nil, err
	}
	uConfig := urlConfig{
		path:          path,
		usePublicIP:   usePublicIP,
		openInBrowser: openInBrowser,
		secure:        secure,
	}
	return urlGenerator(c, l, c.TargetNodes(), uConfig)
}

// PprofOpts specifies the options needed by Pprof().
type PprofOpts struct {
	Heap         bool
	Open         bool
	StartingPort int
	Duration     time.Duration
}

// Pprof TODO
func Pprof(l *logger.Logger, clusterName string, opts PprofOpts) error {
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
	nodes := c.TargetNodes()
	failed, err := c.ParallelE(l, description, len(nodes), 0, func(i int) (*install.RunResultDetails, error) {
		node := nodes[i]
		res := &install.RunResultDetails{Node: node}
		host := c.Host(node)
		port := c.NodeUIPort(node)
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
				fmt.Fprintf(l.Stderr, "warning: could not close temporary file")
			}
			err = os.Remove(file.Name())
			if err != nil && !oserror.IsNotExist(err) {
				fmt.Fprintf(l.Stderr, "warning: could not remove temporary file")
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
	}, install.DefaultSSHRetryOpts)

	for _, s := range outputFiles {
		l.Printf("Created %s", s)
	}

	if err != nil {
		sort.Slice(failed, func(i, j int) bool { return failed[i].Index < failed[j].Index })
		for _, f := range failed {
			fmt.Fprintf(l.Stderr, "%d: %+v: %s\n", f.Index, f.Err, f.Out)
		}
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
		cld, err = cloud.ListCloud(l, vm.ListOptions{})
		if err != nil {
			return err
		}
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
				var err error
				cld, err = cloud.ListCloud(l, vm.ListOptions{})
				if err != nil {
					return err
				}
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
	l.Printf("Destroying cluster %s with %d nodes", clusterName, len(c.VMs))
	return cloud.DestroyCluster(c)
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
	cld, err := cloud.ListCloud(l, vm.ListOptions{})
	if err != nil {
		return err
	}
	c, ok := cld.Clusters[clusterName]
	if !ok {
		// If the cluster doesn't exist, we didn't manage to create any VMs
		// before failing. Not an error.
		return nil
	}
	return cloud.DestroyCluster(c)
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
		unlockFn, err := acquireFilesystemLock()
		if err != nil {
			return err
		}
		defer unlockFn()
	}

	if err := LoadClusters(); err != nil {
		return errors.Wrap(err, "problem loading clusters")
	}

	if !isLocal {
		cld, err := cloud.ListCloud(l, vm.ListOptions{})
		if err != nil {
			return err
		}
		if _, ok := cld.Clusters[clusterName]; ok {
			return &ClusterAlreadyExistsError{name: clusterName}
		}

		defer func() {
			if retErr == nil {
				return
			}
			fmt.Fprintf(l.Stderr, "Cleaning up partially-created cluster (prev err: %s)\n", retErr)
			if err := cleanupFailedCreate(l, clusterName); err != nil {
				fmt.Fprintf(l.Stderr, "Error while cleaning up partially-created cluster: %s\n", err)
			} else {
				fmt.Fprintf(l.Stderr, "Cleaning up OK\n")
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

	l.Printf("Creating cluster %s with %d nodes", clusterName, numNodes)
	if createErr := cloud.CreateCluster(l, numNodes, createVMOpts, providerOptsContainer); createErr != nil {
		return createErr
	}

	if config.IsLocalClusterName(clusterName) {
		// No need for ssh for local clusters.
		return LoadClusters()
	}
	return SetupSSH(ctx, l, clusterName)
}

// GC garbage-collects expired clusters and unused SSH keypairs in AWS.
func GC(l *logger.Logger, dryrun bool) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	cld, err := cloud.ListCloud(l, vm.ListOptions{})
	if err == nil {
		// GCClusters depends on ListCloud so only call it if ListCloud runs without errors
		err = cloud.GCClusters(l, cld, dryrun)
	}
	otherErr := cloud.GCAWSKeyPairs(dryrun)
	return errors.CombineErrors(err, otherErr)
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
		logsOpts.Dir, dest, username, logsOpts.Filter, logsOpts.ProgramFilter,
		logsOpts.Interval, logsOpts.From, logsOpts.To, logsOpts.Out,
	)
}

// StageURL TODO
func StageURL(l *logger.Logger, applicationName, version, stageOS string) ([]*url.URL, error) {
	os := runtime.GOOS
	if stageOS != "" {
		os = stageOS
	}
	urls, err := install.URLsForApplication(applicationName, version, os)
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
	grafanaURL string,
	promCfg *prometheus.Config, // passed iff grafanaURL is empty
) error {
	if grafanaURL != "" && promCfg != nil {
		return errors.New("cannot pass grafanaURL and a non empty promCfg")
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
	}
	_, err = prometheus.Init(ctx, l, c, *promCfg)
	if err != nil {
		return err
	}
	url, err := GrafanaURL(ctx, l, clusterName, false)
	if err != nil {
		return err
	}
	fmt.Printf("Grafana dashboard: %s\n", url)
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
	urls, err := urlGenerator(c, l, grafanaNode, uConfig)
	if err != nil {
		return "", err
	}
	return urls[0], nil
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

// SnapshotVolume snapshots any of the volumes attached to the nodes in a
// cluster specification.
func SnapshotVolume(
	ctx context.Context, l *logger.Logger, clusterName, name, description string,
) error {
	if err := LoadClusters(); err != nil {
		return err
	}
	c, err := newCluster(l, clusterName)
	if err != nil {
		return err
	}
	nodes := c.TargetNodes()
	nodesStatus, err := c.Status(ctx, l)
	if err != nil {
		return err
	}
	for nodeSpecIdx, nodeID := range nodes {
		cVM := c.VMs[nodeID-1]
		labels := map[string]string{
			"roachprod-node-src-spec": cVM.MachineType,
			"roachprod-cluster-node":  cVM.Name,
			"roachprod-crdb-version":  nodesStatus[nodeSpecIdx].Version,
		}
		foundMatchingVolume := false
		if len(cVM.NonBootAttachedVolumes) == 0 {
			l.Printf("Node %d does not have any non-bootable volumes attached. Did you run `sync --include-volumes`?",
				nodeID)
		}
		for _, volume := range cVM.NonBootAttachedVolumes {
			if isWorkloadCollectorVolume(volume) {
				l.Printf("Creating snapshot for node %d volume %s\n", nodeID, volume.Name)
				nameSuffix := ""
				if len(nodes) != 1 {
					nameSuffix = fmt.Sprintf("-%d", nodeID)
				}
				err := vm.ForProvider(cVM.Provider, func(provider vm.Provider) error {
					sID, err := provider.SnapshotVolume(volume, name+nameSuffix, description, labels)
					if err != nil {
						return err
					}
					l.Printf("Created snapshot %s for volume %s (%s)\n", sID, volume.Name, volume.ProviderResourceID)
					foundMatchingVolume = true
					return nil
				})
				if err != nil {
					return err
				}
			}
			if !foundMatchingVolume {
				l.Printf("No volumes matched the workload collector filter for node %d. "+
					"Volumes are missing the `roachprod_collector` label.", nodeID)
			}
		}
	}
	return nil
}

func generateVolumeName(clusterName string, nodeID install.Node) string {
	return fmt.Sprintf("%s-n%d", clusterName, nodeID)
}

func genMountCommands(devicePath, mountDir string) string {
	return strings.Join([]string{
		"sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard " + devicePath,
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

	mountDir := "/mnt/capture/"
	switch action {
	case "start":
		err = createAttachMountVolumes(ctx, l, c, opts, mountDir)
		if err != nil {
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
	_, err := c.ParallelE(l,
		fmt.Sprintf("Performing workload capture %s", action),
		len(nodes),
		0,
		func(i int) (*install.RunResultDetails, error) {
			node := nodes[i]
			res := &install.RunResultDetails{Node: node}
			host := c.Host(node)
			port := c.NodeUIPort(node)
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
		}, install.DefaultSSHRetryOpts)
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
	var labels = map[string]string{"roachprod_collector": "true"}
	for idx, n := range nodes {
		curNode := nodes[idx : idx+1]

		cVM := &c.VMs[n-1]
		err := vm.ForProvider(cVM.Provider, func(provider vm.Provider) error {
			opts.Name = generateVolumeName(c.Name, n)
			for _, vol := range cVM.NonBootAttachedVolumes {
				if vol.Name == opts.Name {
					l.Printf(
						"A volume (%s) is already attached to node %d skipping volume creation", vol.ProviderResourceID, n)
					return nil
				}
			}
			opts.Zone = cVM.Zone
			opts.Labels = labels

			volume, err := provider.CreateVolume(opts)
			if err != nil {
				return err
			}
			l.Printf("Created Volume %s", volume.ProviderResourceID)
			device, err := cVM.AttachVolume(volume)
			if err != nil {
				return err
			}
			// Save the cluster to cache
			err = saveCluster(&c.Cluster)
			if err != nil {
				return err
			}
			l.Printf("Attached Volume %s to %s", volume.ProviderResourceID, cVM.ProviderID)
			err = c.Run(ctx, l, l.Stdout, l.Stderr, curNode,
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
