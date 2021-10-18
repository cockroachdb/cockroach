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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	cld "github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ui"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/aws"
	_ "github.com/cockroachdb/cockroach/pkg/roachprod/vm/azure"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/local"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"golang.org/x/sys/unix"
)

// Version returns version/build information.
func Version() string {
	info := build.GetInfo()
	return info.Long()
}

// CachedHosts returns a list of all roachprod clsuters from local cache.
func CachedHosts(cachedHostsCluster string) ([]string, error) {
	if err := loadClusters(); err != nil {
		return nil, err
	}

	names := make([]string, 0, len(install.Clusters))
	for name := range install.Clusters {
		names = append(names, name)
	}
	sort.Strings(names)

	var retLines []string
	for _, name := range names {
		c := install.Clusters[name]
		if strings.HasPrefix(c.Name, "teamcity") {
			continue
		}
		newLine := c.Name
		// when invokved by bash-completion, cachedHostsCluster is what the user
		// has currently typed -- if this cluster matches that, expand its hosts.
		if strings.HasPrefix(cachedHostsCluster, c.Name) {
			for i := range c.VMs {
				newLine += fmt.Sprintf(" %s:%d", c.Name, i+1)
			}
		}
		retLines = append(retLines, newLine)

	}
	return retLines, nil
}

// Sync grabs an exclusive lock on the roachprod state and then proceeds to
// read the current state from the cloud and write it out to disk. The locking
// protects both the reading and the writing in order to prevent the hazard
// caused by concurrent goroutines reading cloud state in a different order
// than writing it to disk.
func Sync(quiet bool) (*cld.Cloud, error) {
	lockFile := os.ExpandEnv("$HOME/.roachprod/LOCK")
	if !quiet {
		fmt.Println("Syncing...")
	}
	// Acquire a filesystem lock so that two concurrent synchronizations of
	// roachprod state don't clobber each other.
	f, err := os.Create(lockFile)
	if err != nil {
		return nil, errors.Wrapf(err, "creating lock file %q", lockFile)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		return nil, errors.Wrap(err, "acquiring lock on %q")
	}
	defer f.Close()
	cloud, err := cld.ListCloud()
	if err != nil {
		return nil, err
	}
	if err := syncHosts(cloud); err != nil {
		return nil, err
	}

	var vms vm.List
	for _, c := range cloud.Clusters {
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
		if !quiet {
			fmt.Println("Refreshing DNS entries...")
		}
		if err := gce.SyncDNS(vms); err != nil {
			fmt.Fprintf(os.Stderr, "failed to update %s DNS: %v", gce.Subdomain, err)
		}
	} else {
		if !quiet {
			fmt.Println("Not refreshing DNS entries. We did not have all the VMs.")
		}
	}

	if err := vm.ProvidersSequential(vm.AllProviderNames(), func(p vm.Provider) error {
		return p.CleanSSH()
	}); err != nil {
		return nil, err
	}

	if err := vm.ProvidersSequential(vm.AllProviderNames(), func(p vm.Provider) error {
		return p.ConfigSSH()
	}); err != nil {
		return nil, err
	}
	return cloud, nil
}

// List returns a list of all roachprod clusters matching clusterNamePattern.
func List(quiet, listMine, listDetails, listJSON bool, clusterNamePattern []string) error {
	listPattern := regexp.MustCompile(".*")
	switch len(clusterNamePattern) {
	case 0:
		if listMine {
			var err error
			listPattern, err = userClusterNameRegexp()
			if err != nil {
				return err
			}
		}
	case 1:
		if listMine {
			return errors.New("--mine cannot be combined with a pattern")
		}
		var err error
		listPattern, err = regexp.Compile(clusterNamePattern[0])
		if err != nil {
			return errors.Wrapf(err, "could not compile regex pattern: %s", clusterNamePattern[0])
		}
	default:
		return errors.New("only a single pattern may be listed")
	}

	cloud, err := Sync(quiet)
	if err != nil {
		return err
	}

	// Filter and sort by cluster names for stable output.
	var names []string
	filteredCloud := cloud.Clone()
	for name := range cloud.Clusters {
		if listPattern.MatchString(name) {
			names = append(names, name)
		} else {
			delete(filteredCloud.Clusters, name)
		}
	}
	sort.Strings(names)

	if listJSON {
		if listDetails {
			return errors.New("--json cannot be combined with --detail")
		}

		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(filteredCloud); err != nil {
			return err
		}
	} else {
		// Align columns left and separate with at least two spaces.
		tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
		for _, name := range names {
			c := filteredCloud.Clusters[name]
			if listDetails {
				c.PrintDetails()
			} else {
				fmt.Fprintf(tw, "%s:\t%s\t%d", c.Name, c.Clouds(), len(c.VMs))
				if !c.IsLocal() {
					fmt.Fprintf(tw, "\t(%s)", c.LifetimeRemaining().Round(time.Second))
				} else {
					fmt.Fprintf(tw, "\t(-)")
				}
				fmt.Fprintf(tw, "\n")
			}
		}
		if err := tw.Flush(); err != nil {
			return err
		}

		// Optionally print any dangling instances with errors
		if listDetails {
			collated := filteredCloud.BadInstanceErrors()

			// Sort by Error() value for stable output
			var errors ui.ErrorsByError
			for err := range collated {
				errors = append(errors, err)
			}
			sort.Sort(errors)

			for _, e := range errors {
				fmt.Printf("%s: %s\n", e, collated[e].Names())
			}
		}
	}
	return nil
}

// Run runs a command on the nodes in a cluster.
func Run(clusterOpts ClusterOpts, SSHOptions string, cmdArray []string) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}

	// Use "ssh" if an interactive session was requested (i.e. there is no
	// remote command to run).
	if len(cmdArray) == 0 {
		return c.SSH(strings.Split(SSHOptions, " "), cmdArray)
	}

	cmd := strings.TrimSpace(strings.Join(cmdArray, " "))
	title := cmd
	if len(title) > 30 {
		title = title[:27] + "..."
	}
	return c.Run(os.Stdout, os.Stderr, c.Nodes, title, cmd)
}

// SQL runs `cockroach sql` on a remote cluster.
func SQL(clusterOpts ClusterOpts, cmdArray []string) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	cockroach, ok := c.Impl.(install.Cockroach)
	if !ok {
		return errors.New("sql is only valid on cockroach clusters")
	}
	return cockroach.SQL(c, cmdArray)
}

// IP gets the ip addresses of the nodes in a cluster.
func IP(clusterOpts ClusterOpts, external bool) ([]string, error) {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return nil, err
	}

	nodes := c.ServerNodes()
	ips := make([]string, len(nodes))

	if external {
		for i := 0; i < len(nodes); i++ {
			ips[i] = c.VMs[nodes[i]-1]
		}
	} else {
		c.Parallel("", len(nodes), 0, func(i int) ([]byte, error) {
			var err error
			ips[i], err = c.GetInternalIP(nodes[i])
			return nil, err
		})
	}

	return ips, nil
}

// Status retrieves the status of nodes in a cluster.
func Status(clusterOpts ClusterOpts) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	c.Status()
	return nil
}

// Stage stages release and edge binaries to the cluster.
// stageOS, stageDir, version can be "" to use default values
func Stage(clusterOpts ClusterOpts, stageOS, stageDir, applicationName, version string) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}

	os := "linux"
	if stageOS != "" {
		os = stageOS
	} else if c.IsLocal() {
		os = runtime.GOOS
	}
	var debugArch, releaseArch, libExt string
	switch os {
	case "linux":
		debugArch, releaseArch, libExt = "linux-gnu-amd64", "linux-amd64", ".so"
	case "darwin":
		debugArch, releaseArch, libExt = "darwin-amd64", "darwin-10.9-amd64", ".dylib"
	case "windows":
		debugArch, releaseArch, libExt = "windows-amd64", "windows-6.2-amd64", ".dll"
	default:
		return errors.Errorf("cannot stage binary on %s", os)
	}

	dir := "."
	if stageDir != "" {
		dir = stageDir
	}

	switch applicationName {
	case "cockroach":
		sha, err := install.StageRemoteBinary(
			c, applicationName, "cockroach/cockroach", version, debugArch, dir,
		)
		if err != nil {
			return err
		}
		// NOTE: libraries may not be present in older versions.
		// Use the sha for the binary to download the same remote library.
		for _, library := range []string{"libgeos", "libgeos_c"} {
			if err := install.StageOptionalRemoteLibrary(
				c,
				library,
				fmt.Sprintf("cockroach/lib/%s", library),
				sha,
				debugArch,
				libExt,
				dir,
			); err != nil {
				return err
			}
		}
		return nil
	case "workload":
		_, err := install.StageRemoteBinary(
			c, applicationName, "cockroach/workload", version, "" /* arch */, dir,
		)
		return err
	case "release":
		return install.StageCockroachRelease(c, version, releaseArch, dir)
	default:
		return fmt.Errorf("unknown application %s", applicationName)
	}
}

// Reset resets all VMs in a cluster.
func Reset(clusterOpts ClusterOpts, numNodes int, username string) error {
	if numNodes <= 0 || numNodes >= 1000 {
		// Upper limit is just for safety.
		return fmt.Errorf("number of nodes must be in [1..999]")
	}

	clusterName, err := verifyClusterName(clusterOpts.Name, username)
	if err != nil {
		return err
	}

	if clusterName == config.Local {
		return nil
	}

	cloud, err := cld.ListCloud()
	if err != nil {
		return err
	}
	c, ok := cloud.Clusters[clusterName]
	if !ok {
		return errors.New("cluster not found")
	}

	return vm.FanOut(c.VMs, func(p vm.Provider, vms vm.List) error {
		return p.Reset(vms)
	})
}

// SetupSSH sets up the keys and host keys for the vms in the cluster.
func SetupSSH(clusterOpts ClusterOpts, quiet bool, username string) error {
	clusterName, err := verifyClusterName(clusterOpts.Name, username)
	if err != nil {
		return err
	}
	cloud, err := Sync(quiet)
	if err != nil {
		return err
	}
	cloudCluster, ok := cloud.Clusters[clusterName]
	if !ok {
		return fmt.Errorf("could not find %s in list of cluster", clusterName)
	}
	cloudCluster.PrintDetails()
	// Run ssh-keygen -R serially on each new VM in case an IP address has been recycled
	for _, v := range cloudCluster.VMs {
		cmd := exec.Command("ssh-keygen", "-R", v.PublicIP)
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Infof(context.TODO(), "could not clear ssh key for hostname %s:\n%s", v.PublicIP, string(out))
		}

	}

	// Wait for the nodes in the cluster to start.
	install.Clusters = map[string]*install.SyncedCluster{}
	if err := loadClusters(); err != nil {
		return err
	}
	installCluster, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	// For GCP clusters we need to use the config.OSUser even if the client
	// requested the shared user.
	for i := range installCluster.VMs {
		if cloudCluster.VMs[i].Provider == gce.ProviderName {
			installCluster.Users[i] = config.OSUser.Username
		}
	}
	if err := installCluster.Wait(); err != nil {
		return err
	}
	// Fetch public keys from gcloud to set up ssh access for all users into the
	// shared ubuntu user.
	installCluster.AuthorizedKeys, err = gce.GetUserAuthorizedKeys()
	if err != nil {
		return errors.Wrap(err, "failed to retrieve authorized keys from gcloud")
	}
	return installCluster.SetupSSH()
}

// Extend extends the lifetime of the specified cluster to prevent it from being destroyed.
func Extend(clusterOpts ClusterOpts, lifetime time.Duration, username string) error {
	clusterName, err := verifyClusterName(clusterOpts.Name, username)
	if err != nil {
		return err
	}

	cloud, err := cld.ListCloud()
	if err != nil {
		return err
	}

	c, ok := cloud.Clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster %s does not exist", clusterName)
	}

	if err := cld.ExtendCluster(c, lifetime); err != nil {
		return err
	}

	// Reload the clusters and print details.
	cloud, err = cld.ListCloud()
	if err != nil {
		return err
	}

	c, ok = cloud.Clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster %s does not exist", clusterName)
	}

	c.PrintDetails()
	return nil
}

// Start starts nodes on a cluster.
func Start(clusterOpts ClusterOpts) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	c.Start()
	return nil
}

// Monitor monitors the status of cockroach nodes in a cluster.
func Monitor(clusterOpts ClusterOpts, monitorIgnoreEmptyNodes, monitorOneShot bool) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	for msg := range c.Monitor(monitorIgnoreEmptyNodes, monitorOneShot) {
		if msg.Err != nil {
			msg.Msg += "error: " + msg.Err.Error()
		}
		thisError := errors.Newf("%d: %s", msg.Index, msg.Msg)
		if msg.Err != nil || strings.Contains(msg.Msg, "dead") {
			err = errors.CombineErrors(err, thisError)
		}
		fmt.Println(thisError.Error())
	}
	return err
}

// Stop starts nodes on a cluster.
func Stop(clusterOpts ClusterOpts, sig int, wait bool) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	c.Stop(sig, wait)
	return nil
}

// Init initializes the cluster.
func Init(clusterOpts ClusterOpts, username string) error {
	_, err := verifyClusterName(clusterOpts.Name, username)
	if err != nil {
		return err
	}

	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	c.Init()
	return nil
}

// Wipe wipes the nodes in a cluster.
func Wipe(clusterOpts ClusterOpts, wipePreserveCerts bool) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	c.Wipe(wipePreserveCerts)
	return nil
}

// Reformat reformats disks in a cluster to use the specified filesystem.
func Reformat(clusterOpts ClusterOpts, fs string) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}

	var fsCmd string
	switch fs {
	case vm.Zfs:
		if err := install.Install(c, []string{vm.Zfs}); err != nil {
			return err
		}
		fsCmd = `sudo zpool create -f data1 -m /mnt/data1 /dev/sdb`
	case vm.Ext4:
		fsCmd = `sudo mkfs.ext4 -F /dev/sdb && sudo mount -o defaults /dev/sdb /mnt/data1`
	default:
		return fmt.Errorf("unknown filesystem %q", fs)
	}

	err = c.Run(os.Stdout, os.Stderr, c.Nodes, "reformatting", fmt.Sprintf(`
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
		fmt.Fprintf(os.Stderr, "%s\n", err)
	}
	return nil
}

// Install installs third party software.
func Install(clusterOpts ClusterOpts, software []string) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	return install.Install(c, software)
}

// Download downloads 3rd party tools, using a GCS cache if possible.
func Download(clusterOpts ClusterOpts, src, sha, dest string) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	return install.Download(c, src, sha, dest)
}

// Distribute distributes certificates to the nodes in a cluster.
// If the certificates already exist, no action is taken.
func DistributeCerts(clusterOpts ClusterOpts) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	c.DistributeCerts()
	return nil
}

// Put copies a local file to the nodes in a cluster.
func Put(clusterOpts ClusterOpts, src, dest string) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	c.Put(src, dest)
	return nil
}

// Get copies a remote file from the nodes in a cluster.
// If the file is retrieved from multiple nodes the destination
// file name will be prefixed with the node number.
func Get(clusterOpts ClusterOpts, src, dest string) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	c.Get(src, dest)
	return nil
}

// PgURL generates pgurls for the nodes in a cluster.
func PgURL(clusterOpts ClusterOpts, external bool) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	nodes := c.ServerNodes()
	ips := make([]string, len(nodes))

	if external {
		for i := 0; i < len(nodes); i++ {
			ips[i] = c.VMs[nodes[i]-1]
		}
	} else {
		c.Parallel("", len(nodes), 0, func(i int) ([]byte, error) {
			var err error
			ips[i], err = c.GetInternalIP(nodes[i])
			return nil, err
		})
	}

	var urls []string
	for i, ip := range ips {
		if ip == "" {
			return errors.Errorf("empty ip: %v", ips)
		}
		urls = append(urls, c.Impl.NodeURL(c, ip, c.Impl.NodePort(c, nodes[i])))
	}
	fmt.Println(strings.Join(urls, " "))
	if len(urls) != len(nodes) {
		return errors.Errorf("have nodes %v, but urls %v from ips %v", nodes, urls, ips)
	}
	return nil
}

// AdminURL generates admin UI URLs for the nodes in a cluster.
func AdminURL(clusterOpts ClusterOpts, adminurlIPs, adminurlOpen bool, adminurlPath string) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}

	for i, node := range c.ServerNodes() {
		host := vm.Name(c.Name, node) + "." + gce.Subdomain

		// verify DNS is working / fallback to IPs if not.
		if i == 0 && !adminurlIPs {
			if _, err := net.LookupHost(host); err != nil {
				fmt.Fprintf(os.Stderr, "no valid DNS (yet?). might need to re-run `sync`?\n")
				adminurlIPs = true
			}
		}

		if adminurlIPs {
			host = c.VMs[node-1]
		}
		port := install.GetAdminUIPort(c.Impl.NodePort(c, node))
		scheme := "http"
		if c.Secure {
			scheme = "https"
		}
		if !strings.HasPrefix(adminurlPath, "/") {
			adminurlPath = "/" + adminurlPath
		}
		url := fmt.Sprintf("%s://%s:%d%s", scheme, host, port, adminurlPath)
		if adminurlOpen {
			if err := exec.Command("python", "-m", "webbrowser", url).Run(); err != nil {
				return err
			}
		} else {
			fmt.Println(url)
		}
	}
	return nil
}

func Pprof(
	clusterOpts ClusterOpts, duration time.Duration, heap, open bool, startingPort int,
) error {
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}

	var profType string
	var description string
	if heap {
		description = "capturing heap profile"
		profType = "heap"
	} else {
		description = "capturing CPU profile"
		profType = "profile"
	}

	outputFiles := []string{}
	mu := &syncutil.Mutex{}
	pprofPath := fmt.Sprintf("debug/pprof/%s?seconds=%d", profType, int(duration.Seconds()))

	minTimeout := 30 * time.Second
	timeout := 2 * duration
	if timeout < minTimeout {
		timeout = minTimeout
	}

	httpClient := httputil.NewClientWithTimeout(timeout)
	startTime := timeutil.Now().Unix()
	failed, err := c.ParallelE(description, len(c.ServerNodes()), 0, func(i int) ([]byte, error) {
		host := c.VMs[i]
		port := install.GetAdminUIPort(c.Impl.NodePort(c, i))
		scheme := "http"
		if c.Secure {
			scheme = "https"
		}
		outputFile := fmt.Sprintf("pprof-%s-%d-%s-%04d.out", profType, startTime, c.Name, i+1)
		outputDir := filepath.Dir(outputFile)
		file, err := ioutil.TempFile(outputDir, ".pprof")
		if err != nil {
			return nil, errors.Wrap(err, "create tmpfile for pprof download")
		}

		defer func() {
			err := file.Close()
			if err != nil && !errors.Is(err, oserror.ErrClosed) {
				fmt.Fprintf(os.Stderr, "warning: could not close temporary file")
			}
			err = os.Remove(file.Name())
			if err != nil && !oserror.IsNotExist(err) {
				fmt.Fprintf(os.Stderr, "warning: could not remove temporary file")
			}
		}()

		pprofURL := fmt.Sprintf("%s://%s:%d/%s", scheme, host, port, pprofPath)
		resp, err := httpClient.Get(context.Background(), pprofURL)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, errors.Newf("unexpected status from pprof endpoint: %s", resp.Status)
		}

		if _, err := io.Copy(file, resp.Body); err != nil {
			return nil, err
		}
		if err := file.Sync(); err != nil {
			return nil, err
		}
		if err := file.Close(); err != nil {
			return nil, err
		}
		if err := os.Rename(file.Name(), outputFile); err != nil {
			return nil, err
		}

		mu.Lock()
		outputFiles = append(outputFiles, outputFile)
		mu.Unlock()
		return nil, nil
	})

	for _, s := range outputFiles {
		fmt.Printf("Created %s\n", s)
	}

	if err != nil {
		sort.Slice(failed, func(i, j int) bool { return failed[i].Index < failed[j].Index })
		for _, f := range failed {
			fmt.Fprintf(os.Stderr, "%d: %+v: %s\n", f.Index, f.Err, f.Out)
		}
		os.Exit(1)
	}

	if open {
		waitCommands := []*exec.Cmd{}
		for i, file := range outputFiles {
			port := startingPort + i
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

func Destroy(clusters []ClusterOpts, destroyAllMine bool, username string) error {
	type cloudAndName struct {
		name  string
		cloud *cld.Cloud
	}
	var cns []cloudAndName
	switch len(clusters) {
	case 0:
		if !destroyAllMine {
			return errors.New("no cluster name provided")
		}

		destroyPattern, err := userClusterNameRegexp()
		if err != nil {
			return err
		}

		cloud, err := cld.ListCloud()
		if err != nil {
			return err
		}

		for name := range cloud.Clusters {
			if destroyPattern.MatchString(name) {
				cns = append(cns, cloudAndName{name: name, cloud: cloud})
			}
		}

	default:
		if destroyAllMine {
			return errors.New("--all-mine cannot be combined with cluster names")
		}

		var cloud *cld.Cloud
		for _, cluster := range clusters {
			clusterName, err := verifyClusterName(cluster.Name, username)
			if err != nil {
				return err
			}

			if clusterName != config.Local {
				if cloud == nil {
					cloud, err = cld.ListCloud()
					if err != nil {
						return err
					}
				}

				cns = append(cns, cloudAndName{name: clusterName, cloud: cloud})
			} else {
				if err := destroyLocalCluster(cluster); err != nil {
					return err
				}
			}
		}
	}

	if err := ctxgroup.GroupWorkers(context.TODO(), len(cns), func(ctx context.Context, idx int) error {
		return destroyCluster(cns[idx].cloud, cns[idx].name)
	}); err != nil {
		return err
	}
	fmt.Println("OK")
	return nil
}

func destroyCluster(cloud *cld.Cloud, clusterName string) error {
	c, ok := cloud.Clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster %s does not exist", clusterName)
	}
	fmt.Printf("Destroying cluster %s with %d nodes\n", clusterName, len(c.VMs))
	return cld.DestroyCluster(c)
}

func destroyLocalCluster(clusterOpts ClusterOpts) error {
	if _, ok := install.Clusters[config.Local]; !ok {
		return fmt.Errorf("cluster %s does not exist", config.Local)
	}
	clusterOpts.Name = config.Local
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	c.Wipe(false)
	for _, i := range c.Nodes {
		err := os.RemoveAll(fmt.Sprintf(os.ExpandEnv("${HOME}/local/%d"), i))
		if err != nil {
			return err
		}
	}
	return os.Remove(filepath.Join(os.ExpandEnv(config.DefaultHostDir), c.Name))
}

type clusterAlreadyExistsError struct {
	name string
}

func (e *clusterAlreadyExistsError) Error() string {
	return fmt.Sprintf("cluster %s already exists", e.name)
}

func newClusterAlreadyExistsError(name string) error {
	return &clusterAlreadyExistsError{name: name}
}

func cleanupFailedCreate(clusterName string) error {
	cloud, err := cld.ListCloud()
	if err != nil {
		return err
	}
	c, ok := cloud.Clusters[clusterName]
	if !ok {
		// If the cluster doesn't exist, we didn't manage to create any VMs
		// before failing. Not an error.
		return nil
	}
	return cld.DestroyCluster(c)
}

func Create(
	numNodes int, username string, createVMOpts vm.CreateOpts, clusterOpts ClusterOpts,
) (retErr error) {
	fmt.Println("Create - New Library Triggered")
	if numNodes <= 0 || numNodes >= 1000 {
		// Upper limit is just for safety.
		return fmt.Errorf("number of nodes must be in [1..999]")
	}

	clusterName, err := verifyClusterName(clusterOpts.Name, username)
	if err != nil {
		return err
	}
	createVMOpts.ClusterName = clusterName

	defer func() {
		if retErr == nil || clusterName == config.Local {
			return
		}
		if errors.HasType(retErr, (*clusterAlreadyExistsError)(nil)) {
			return
		}
		fmt.Fprintf(os.Stderr, "Cleaning up partially-created cluster (prev err: %s)\n", retErr)
		if err := cleanupFailedCreate(clusterName); err != nil {
			fmt.Fprintf(os.Stderr, "Error while cleaning up partially-created cluster: %s\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "Cleaning up OK\n")
		}
	}()

	if clusterName != config.Local {
		cloud, err := cld.ListCloud()
		if err != nil {
			return err
		}
		if _, ok := cloud.Clusters[clusterName]; ok {
			return newClusterAlreadyExistsError(clusterName)
		}
	} else {
		if _, ok := install.Clusters[clusterName]; ok {
			return newClusterAlreadyExistsError(clusterName)
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

	fmt.Printf("Creating cluster %s with %d nodes\n", clusterName, numNodes)
	if createErr := cld.CreateCluster(numNodes, createVMOpts); createErr != nil {
		return createErr
	}

	// Just create directories for the local cluster as there's no need for ssh.
	if clusterName == config.Local {
		for i := 0; i < numNodes; i++ {
			err := os.MkdirAll(fmt.Sprintf(os.ExpandEnv("${HOME}/local/%d"), i+1), 0755)
			if err != nil {
				return err
			}
		}
		return nil
	}
	return SetupSSH(clusterOpts, clusterOpts.Quiet, username)
}

// GC garbage-collects expired clusters and unused SSH keypairs in AWS.
func GC(dryrun bool, slackToken string) error {
	config.SlackToken = slackToken
	cloud, err := cld.ListCloud()
	if err == nil {
		// GCClusters depends on ListCloud so only call it if ListCloud runs without errors
		err = cld.GCClusters(cloud, dryrun)
	}
	otherErr := cld.GCAWSKeyPairs(dryrun)
	return errors.CombineErrors(err, otherErr)
}

type LogsOpts struct {
	Dir, Filter, ProgramFilter string
	Interval                   time.Duration
	From, To                   time.Time
	Out                        io.Writer
}

func Logs(logsOpts LogsOpts, clusterOpts ClusterOpts, dest, username string) error {
	fmt.Println("New library")
	c, err := newCluster(clusterOpts)
	if err != nil {
		return err
	}
	return c.Logs(logsOpts.Dir, dest, username, logsOpts.Filter, logsOpts.ProgramFilter, logsOpts.Interval, logsOpts.From, logsOpts.To, logsOpts.Out)
}
