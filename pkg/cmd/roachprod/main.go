// Copyright 2018 The Cockroach Authors.
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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	cld "github.com/cockroachdb/cockroach/pkg/cmd/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/config"
	rperrors "github.com/cockroachdb/cockroach/pkg/cmd/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/ui"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm/aws"
	_ "github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm/azure"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm/local"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/flagutil"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

var rootCmd = &cobra.Command{
	Use:   "roachprod [command] (flags)",
	Short: "roachprod tool for manipulating test clusters",
	Long: `roachprod is a tool for manipulating ephemeral test clusters, allowing easy
creating, destruction, starting, stopping and wiping of clusters along with
running load generators.

Examples:

  roachprod create local -n 3
  roachprod start local
  roachprod sql local:2 -- -e "select * from crdb_internal.node_runtime_info"
  roachprod stop local
  roachprod wipe local
  roachprod destroy local

The above commands will create a "local" 3 node cluster, start a cockroach
cluster on these nodes, run a sql command on the 2nd node, stop, wipe and
destroy the cluster.
`,
}

var (
	numNodes          int
	numRacks          int
	username          string
	dryrun            bool
	destroyAllMine    bool
	extendLifetime    time.Duration
	wipePreserveCerts bool
	listDetails       bool
	listJSON          bool
	listMine          bool
	secure            = false
	nodeEnv           = []string{
		"COCKROACH_ENABLE_RPC_COMPRESSION=false",
		"COCKROACH_UI_RELEASE_NOTES_SIGNUP_DISMISSED=true",
	}
	nodeArgs          []string
	tag               string
	external          = false
	adminurlOpen      = false
	adminurlPath      = ""
	adminurlIPs       = false
	useTreeDist       = true
	encrypt           = false
	skipInit          = false
	quiet             = false
	sig               = 9
	waitFlag          = false
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

func sortedClusters() []string {
	var r []string
	for n := range install.Clusters {
		r = append(r, n)
	}
	sort.Strings(r)
	return r
}

func newCluster(name string) (*install.SyncedCluster, error) {
	nodeNames := "all"
	{
		parts := strings.Split(name, ":")
		switch len(parts) {
		case 2:
			nodeNames = parts[1]
			fallthrough
		case 1:
			name = parts[0]
		case 0:
			return nil, fmt.Errorf("no cluster specified")
		default:
			return nil, fmt.Errorf("invalid cluster name: %s", name)
		}
	}

	c, ok := install.Clusters[name]
	if !ok {
		err := errors.Newf(`unknown cluster: %s`, name)
		err = errors.WithHintf(err, `
Available clusters:
  %s
`, strings.Join(sortedClusters(), "\n  "))
		err = errors.WithHint(err, `Use "roachprod sync" to update the list of available clusters.`)
		return nil, err
	}

	c.Impl = install.Cockroach{}
	if numRacks > 0 {
		for i := range c.Localities {
			rack := fmt.Sprintf("rack=%d", i%numRacks)
			if c.Localities[i] != "" {
				rack = "," + rack
			}
			c.Localities[i] += rack
		}
	}

	nodes, err := install.ListNodes(nodeNames, len(c.VMs))
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		if n > len(c.VMs) {
			return nil, fmt.Errorf("invalid node spec %s, cluster contains %d nodes",
				nodeNames, len(c.VMs))
		}
	}
	c.Nodes = nodes
	c.Secure = secure
	c.Env = strings.Join(nodeEnv, " ")
	c.Args = nodeArgs
	if tag != "" {
		c.Tag = "/" + tag
	}
	c.UseTreeDist = useTreeDist
	c.Quiet = quiet || !term.IsTerminal(int(os.Stdout.Fd()))
	c.MaxConcurrency = maxConcurrency
	return c, nil
}

// verifyClusterName ensures that the given name conforms to
// our naming pattern of "<username>-<clustername>". The
// username must match one of the vm.Provider account names
// or the --username override.
func verifyClusterName(clusterName string) (string, error) {
	if len(clusterName) == 0 {
		return "", fmt.Errorf("cluster name cannot be blank")
	}
	if clusterName == config.Local {
		return clusterName, nil
	}

	alphaNum, err := regexp.Compile(`^[a-zA-Z0-9\-]+$`)
	if err != nil {
		return "", err
	}
	if !alphaNum.MatchString(clusterName) {
		return "", errors.Errorf("cluster name must match %s", alphaNum.String())
	}

	// Use the vm.Provider account names, or --username.
	var accounts []string
	if len(username) > 0 {
		accounts = []string{username}
	} else {
		seenAccounts := map[string]bool{}
		active, err := vm.FindActiveAccounts()
		if err != nil {
			return "", err
		}
		for _, account := range active {
			if !seenAccounts[account] {
				seenAccounts[account] = true
				cleanAccount := vm.DNSSafeAccount(account)
				if cleanAccount != account {
					log.Printf("WARN: using `%s' as username instead of `%s'", cleanAccount, account)
				}
				accounts = append(accounts, cleanAccount)
			}
		}
	}

	// If we see <account>-<something>, accept it.
	for _, account := range accounts {
		if strings.HasPrefix(clusterName, account+"-") && len(clusterName) > len(account)+1 {
			return clusterName, nil
		}
	}

	// Try to pick out a reasonable cluster name from the input.
	i := strings.Index(clusterName, "-")
	suffix := clusterName
	if i != -1 {
		// The user specified a username prefix, but it didn't match an active
		// account name. For example, assuming the account is "peter", `roachprod
		// create joe-perf` should be specified as `roachprod create joe-perf -u
		// joe`.
		suffix = clusterName[i+1:]
	} else {
		// The user didn't specify a username prefix. For example, assuming the
		// account is "peter", `roachprod create perf` should be specified as
		// `roachprod create peter-perf`.
		_ = 0
	}

	// Suggest acceptable cluster names.
	var suggestions []string
	for _, account := range accounts {
		suggestions = append(suggestions, fmt.Sprintf("%s-%s", account, suffix))
	}
	return "", fmt.Errorf("malformed cluster name %s, did you mean one of %s",
		clusterName, suggestions)
}

// Provide `cobra.Command` functions with a standard return code handler.
// Exit codes come from rperrors.Error.ExitCode().
//
// If the wrapped error tree of an error does not contain an instance of
// rperrors.Error, the error will automatically be wrapped with
// rperrors.Unclassified.
func wrap(f func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		err := f(cmd, args)
		if err != nil {
			roachprodError, ok := rperrors.AsError(err)
			if !ok {
				roachprodError = rperrors.Unclassified{Err: err}
				err = roachprodError
			}

			cmd.Printf("Error: %+v\n", err)

			os.Exit(roachprodError.ExitCode())
		}
	}
}

var createVMOpts vm.CreateOpts

type clusterAlreadyExistsError struct {
	name string
}

func (e *clusterAlreadyExistsError) Error() string {
	return fmt.Sprintf("cluster %s already exists", e.name)
}

func newClusterAlreadyExistsError(name string) error {
	return &clusterAlreadyExistsError{name: name}
}

var createCmd = &cobra.Command{
	Use:   "create <cluster>",
	Short: "create a cluster",
	Long: `Create a local or cloud-based cluster.

A cluster is composed of a set of nodes, configured during cluster creation via
the --nodes flag. Creating a cluster does not start any processes on the nodes
other than the base system processes (e.g. sshd). See "roachprod start" for
starting cockroach nodes and "roachprod {run,ssh}" for running arbitrary
commands on the nodes of a cluster.

Cloud Clusters

  Cloud-based clusters are ephemeral and come with a lifetime (specified by the
  --lifetime flag) after which they will be automatically
  destroyed. Cloud-based clusters require the associated command line tool for
  the cloud to be installed and configured (e.g. "gcloud auth login").

  Clusters names are required to be prefixed by the authenticated user of the
  cloud service. The suffix is an arbitrary string used to distinguish
  clusters. For example, "marc-test" is a valid cluster name for the user
  "marc". The authenticated user for the cloud service is automatically
  detected and can be override by the ROACHPROD_USER environment variable or
  the --username flag.

  The machine type and the use of local SSD storage can be specified during
  cluster creation via the --{cloud}-machine-type and --local-ssd flags. The
  machine-type is cloud specified. For example, --gce-machine-type=n1-highcpu-8
  requests the "n1-highcpu-8" machine type for a GCE-based cluster. No attempt
  is made (or desired) to abstract machine types across cloud providers. See
  the cloud provider's documentation for details on the machine types
  available.

Local Clusters

  A local cluster stores the per-node data in ${HOME}/local on the machine
  roachprod is being run on. Local clusters requires local ssh access. Unlike
  cloud clusters there can be only a single local cluster, the local cluster is
  always named "local", and has no expiration (unlimited lifetime).
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) (retErr error) {
		if numNodes <= 0 || numNodes >= 1000 {
			// Upper limit is just for safety.
			return fmt.Errorf("number of nodes must be in [1..999]")
		}

		clusterName, err := verifyClusterName(args[0])
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
		return setupSSH(clusterName)
	}),
}

var setupSSHCmd = &cobra.Command{
	Use:   "setup-ssh <cluster>",
	Short: "set up ssh for a cluster",
	Long: `Sets up the keys and host keys for the vms in the cluster.

It first resets the machine credentials as though the cluster were newly created
using the cloud provider APIs and then proceeds to ensure that the hosts can
SSH into eachother and lastly adds additional public keys to AWS hosts as read
from the GCP project. This operation is performed as the last step of creating
a new cluster but can be useful to re-run if the operation failed previously or
if the user would like to update the keys on the remote hosts.
`,

	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) (retErr error) {
		clusterName, err := verifyClusterName(args[0])
		if err != nil {
			return err
		}
		return setupSSH(clusterName)
	}),
}

func setupSSH(clusterName string) error {
	cloud, err := syncCloud(quiet)
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
			log.Printf("could not clear ssh key for hostname %s:\n%s", v.PublicIP, string(out))
		}

	}

	// Wait for the nodes in the cluster to start.
	install.Clusters = map[string]*install.SyncedCluster{}
	if err := loadClusters(); err != nil {
		return err
	}
	installCluster, err := newCluster(clusterName)
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

var destroyCmd = &cobra.Command{
	Use:   "destroy [ --all-mine | <cluster 1> [<cluster 2> ...] ]",
	Short: "destroy clusters",
	Long: `Destroy one or more local or cloud-based clusters.

The destroy command accepts the names of the clusters to destroy. Alternatively,
the --all-mine flag can be provided to destroy all clusters that are owned by the
current user.

Destroying a cluster releases the resources for a cluster. For a cloud-based
cluster the machine and associated disk resources are freed. For a local
cluster, any processes started by roachprod are stopped, and the ${HOME}/local
directory is removed.
`,
	Args: cobra.ArbitraryArgs,
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		type cloudAndName struct {
			name  string
			cloud *cld.Cloud
		}
		var cns []cloudAndName
		switch len(args) {
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
			for _, arg := range args {
				clusterName, err := verifyClusterName(arg)
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
					if err := destroyLocalCluster(); err != nil {
						return err
					}
				}
			}
		}

		if err := ctxgroup.GroupWorkers(cmd.Context(), len(cns), func(ctx context.Context, idx int) error {
			return destroyCluster(cns[idx].cloud, cns[idx].name)
		}); err != nil {
			return err
		}
		fmt.Println("OK")
		return nil
	}),
}

func destroyCluster(cloud *cld.Cloud, clusterName string) error {
	c, ok := cloud.Clusters[clusterName]
	if !ok {
		return fmt.Errorf("cluster %s does not exist", clusterName)
	}
	fmt.Printf("Destroying cluster %s with %d nodes\n", clusterName, len(c.VMs))
	return cld.DestroyCluster(c)
}

func destroyLocalCluster() error {
	if _, ok := install.Clusters[config.Local]; !ok {
		return fmt.Errorf("cluster %s does not exist", config.Local)
	}
	c, err := newCluster(config.Local)
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

var cachedHostsCmd = &cobra.Command{
	Use:   "cached-hosts",
	Short: "list all clusters (and optionally their host numbers) from local cache",
	Args:  cobra.NoArgs,
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		if err := loadClusters(); err != nil {
			return err
		}

		names := make([]string, 0, len(install.Clusters))
		for name := range install.Clusters {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			c := install.Clusters[name]
			if strings.HasPrefix(c.Name, "teamcity") {
				continue
			}
			fmt.Print(c.Name)
			// when invokved by bash-completion, cachedHostsCluster is what the user
			// has currently typed -- if this cluster matches that, expand its hosts.
			if strings.HasPrefix(cachedHostsCluster, c.Name) {
				for i := range c.VMs {
					fmt.Printf(" %s:%d", c.Name, i+1)
				}
			}
			fmt.Println()
		}
		return nil
	}),
}

var listCmd = &cobra.Command{
	Use:   "list [--details] [ --mine | <cluster name regex> ]",
	Short: "list all clusters",
	Long: `List all clusters.

The list command accepts an optional positional argument, which is a regular
expression that will be matched against the cluster name pattern.  Alternatively,
the --mine flag can be provided to list the clusters that are owned by the current
user.

The default output shows one line per cluster, including the local cluster if
it exists:

  ~ roachprod list
  local:     [local]    1  (-)
  marc-test: [aws gce]  4  (5h34m35s)
  Syncing...

The second column lists the cloud providers that host VMs for the cluster.

The third and fourth columns are the number of nodes in the cluster and the
time remaining before the cluster will be automatically destroyed. Note that
local clusters do not have an expiration.

The --details flag adjusts the output format to include per-node details:

  ~ roachprod list --details
  local [local]: (no expiration)
    localhost		127.0.0.1	127.0.0.1
  marc-test: [aws gce] 5h33m57s remaining
    marc-test-0001	marc-test-0001.us-east1-b.cockroach-ephemeral	10.142.0.18	35.229.60.91
    marc-test-0002	marc-test-0002.us-east1-b.cockroach-ephemeral	10.142.0.17	35.231.0.44
    marc-test-0003	marc-test-0003.us-east1-b.cockroach-ephemeral	10.142.0.19	35.229.111.100
    marc-test-0004	marc-test-0004.us-east1-b.cockroach-ephemeral	10.142.0.20	35.231.102.125
  Syncing...

The first and second column are the node hostname and fully qualified name
respectively. The third and fourth column are the private and public IP
addresses.

The --json flag sets the format of the command output to json.

Listing clusters has the side-effect of syncing ssh keys/configs and the local
hosts file.
`,
	Args: cobra.RangeArgs(0, 1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		listPattern := regexp.MustCompile(".*")
		switch len(args) {
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
			listPattern, err = regexp.Compile(args[0])
			if err != nil {
				return errors.Wrapf(err, "could not compile regex pattern: %s", args[0])
			}
		default:
			return errors.New("only a single pattern may be listed")
		}

		cloud, err := syncCloud(quiet)
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
	}),
}

// userClusterNameRegexp returns a regexp that matches all clusters owned by the
// current user.
func userClusterNameRegexp() (*regexp.Regexp, error) {
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

// TODO(peter): Do we need this command given that the "list" command syncs as
// a side-effect. If you don't care about the list output, just "roachprod list
// &>/dev/null".
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "sync ssh keys/config and hosts files",
	Long:  ``,
	Args:  cobra.NoArgs,
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		_, err := syncCloud(quiet)
		return err
	}),
}

var lockFile = os.ExpandEnv("$HOME/.roachprod/LOCK")

var bashCompletion = os.ExpandEnv("$HOME/.roachprod/bash-completion.sh")

// syncCloud grabs an exclusive lock on the roachprod state and then proceeds to
// read the current state from the cloud and write it out to disk. The locking
// protects both the reading and the writing in order to prevent the hazard
// caused by concurrent goroutines reading cloud state in a different order
// than writing it to disk.
func syncCloud(quiet bool) (*cld.Cloud, error) {
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

	_ = rootCmd.GenBashCompletionFile(bashCompletion)

	if err := vm.ProvidersSequential(vm.AllProviderNames(), func(p vm.Provider) error {
		return p.ConfigSSH()
	}); err != nil {
		return nil, err
	}
	return cloud, nil
}

var gcCmd = &cobra.Command{
	Use:   "gc",
	Short: "GC expired clusters\n",
	Long: `Garbage collect expired clusters.

Destroys expired clusters, sending email if properly configured. Usually run
hourly by a cronjob so it is not necessary to run manually.
`,
	Args: cobra.NoArgs,
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		cloud, err := cld.ListCloud()
		if err != nil {
			return err
		}
		return cld.GCClusters(cloud, dryrun)
	}),
}

var extendCmd = &cobra.Command{
	Use:   "extend <cluster>",
	Short: "extend the lifetime of a cluster",
	Long: `Extend the lifetime of the specified cluster to prevent it from being
destroyed:

  roachprod extend marc-test --lifetime=6h
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		clusterName, err := verifyClusterName(args[0])
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

		if err := cld.ExtendCluster(c, extendLifetime); err != nil {
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
	}),
}

const tagHelp = `
The --tag flag can be used to to associate a tag with the process. This tag can
then be used to restrict the processes which are operated on by the status and
stop commands. Tags can have a hierarchical component by utilizing a slash
separated string similar to a filesystem path. A tag matches if a prefix of the
components match. For example, the tag "a/b" will match both "a/b" and
"a/b/c/d".
`

var startCmd = &cobra.Command{
	Use:   "start <cluster>",
	Short: "start nodes on a cluster",
	Long: `Start nodes on a cluster.

The --secure flag can be used to start nodes in secure mode (i.e. using
certs). When specified, there is a one time initialization for the cluster to
create and distribute the certs. Note that running some modes in secure mode
and others in insecure mode is not a supported Cockroach configuration.

As a debugging aid, the --sequential flag starts the nodes sequentially so node
IDs match hostnames. Otherwise nodes are started are parallel.

The --binary flag specifies the remote binary to run. It is up to the roachprod
user to ensure this binary exists, usually via "roachprod put". Note that no
cockroach software is installed by default on a newly created cluster.

The --args and --env flags can be used to pass arbitrary command line flags and
environment variables to the cockroach process.
` + tagHelp + `
The "start" command takes care of setting up the --join address and specifying
reasonable defaults for other flags. One side-effect of this convenience is
that node 1 is special and if started, is used to auto-initialize the cluster.
The --skip-init flag can be used to avoid auto-initialization (which can then
separately be done using the "init" command).

If the COCKROACH_DEV_LICENSE environment variable is set the enterprise.license
cluster setting will be set to its value.
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		c.Start()
		return nil
	}),
}

var stopCmd = &cobra.Command{
	Use:   "stop <cluster> [--sig] [--wait]",
	Short: "stop nodes on a cluster",
	Long: `Stop nodes on a cluster.

Stop roachprod created processes running on the nodes in a cluster, including
processes started by the "start", "run" and "ssh" commands. Every process
started by roachprod is tagged with a ROACHPROD=<node> environment variable
which is used by "stop" to locate the processes and terminate them. By default
processes are killed with signal 9 (SIGKILL) giving them no chance for a graceful
exit.

The --sig flag will pass a signal to kill to allow us finer control over how we
shutdown cockroach. The --wait flag causes stop to loop waiting for all
processes with the ROACHPROD=<node> environment variable to exit. Note that
stop will wait forever if you specify --wait with a non-terminating signal
(e.g. SIGHUP). --wait defaults to true for signal 9 (SIGKILL) and false for all
other signals.
` + tagHelp + `
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		wait := waitFlag
		if sig == 9 /* SIGKILL */ && !cmd.Flags().Changed("wait") {
			wait = true
		}
		c.Stop(sig, wait)
		return nil
	}),
}

var initCmd = &cobra.Command{
	Use:   "init <cluster>",
	Short: "initialize the cluster",
	Long: `Initialize the cluster.

The "init" command bootstraps the cluster (using "cockroach init"). It also sets
default cluster settings. It's intended to be used in conjunction with
'roachprod start --skip-init'.
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		clusterName, err := verifyClusterName(args[0])
		if err != nil {
			return err
		}

		c, err := newCluster(clusterName)
		if err != nil {
			return err
		}
		c.Init()
		return nil
	}),
}

var statusCmd = &cobra.Command{
	Use:   "status <cluster>",
	Short: "retrieve the status of nodes in a cluster",
	Long: `Retrieve the status of nodes in a cluster.

The "status" command outputs the binary and PID for the specified nodes:

  ~ roachprod status local
  local: status 3/3
     1: cockroach 29688
     2: cockroach 29687
     3: cockroach 29689
` + tagHelp + `
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		c.Status()
		return nil
	}),
}

var logsCmd = &cobra.Command{
	Use:   "logs",
	Short: "retrieve and merge logs in a cluster",
	Long: `Retrieve and merge logs in a cluster.

The "logs" command runs until terminated. It works similarly to get but is
specifically focused on retrieving logs periodically and then merging them
into a single stream.
`,
	Args: cobra.RangeArgs(1, 2),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		var dest string
		if len(args) == 2 {
			dest = args[1]
		} else {
			dest = c.Name + ".logs"
		}
		return c.Logs(logsDir, dest, username, logsFilter, logsProgramFilter, logsInterval, logsFrom, logsTo, cmd.OutOrStdout())
	}),
}

var monitorCmd = &cobra.Command{
	Use:   "monitor",
	Short: "monitor the status of nodes in a cluster",
	Long: `Monitor the status of cockroach nodes in a cluster.

The "monitor" command runs until terminated. At startup it outputs a line for
each specified node indicating the status of the node (either the PID of the
node if alive, or "dead" otherwise). It then watches for changes in the status
of nodes, outputting a line whenever a change is detected:

  ~ roachprod monitor local
  1: 29688
  3: 29689
  2: 29687
  3: dead
  3: 30718
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
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
	}),
}

var wipeCmd = &cobra.Command{
	Use:   "wipe <cluster>",
	Short: "wipe a cluster",
	Long: `Wipe the nodes in a cluster.

The "wipe" command first stops any processes running on the nodes in a cluster
(via the "stop" command) and then deletes the data directories used by the
nodes.
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		c.Wipe(wipePreserveCerts)
		return nil
	}),
}

var reformatCmd = &cobra.Command{
	Use:   "reformat <cluster> <filesystem>",
	Short: "reformat disks in a cluster\n",
	Long: `
Reformat disks in a cluster to use the specified filesystem.

WARNING: Reformatting will delete all existing data in the cluster.

Filesystem options:
  ext4
  zfs

When running with ZFS, you can create a snapshot of the filesystem's current
state using the 'zfs snapshot' command:

  $ roachprod run <cluster> 'sudo zfs snapshot data1@pristine'

You can then nearly instantaneously restore the filesystem to this state with
the 'zfs rollback' command:

  $ roachprod run <cluster> 'sudo zfs rollback data1@pristine'

`,

	Args: cobra.ExactArgs(2),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}

		var fsCmd string
		switch fs := args[1]; fs {
		case "zfs":
			if err := install.Install(c, []string{"zfs"}); err != nil {
				return err
			}
			fsCmd = `sudo zpool create -f data1 -m /mnt/data1 /dev/sdb`
		case "ext4":
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
	}),
}

var runCmd = &cobra.Command{
	Use:     "run <cluster> <command> [args]",
	Aliases: []string{"ssh"},
	Short:   "run a command on the nodes in a cluster",
	Long: `Run a command on the nodes in a cluster.
`,
	Args: cobra.MinimumNArgs(1),
	Run: wrap(func(_ *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}

		// Use "ssh" if an interactive session was requested (i.e. there is no
		// remote command to run).
		if len(args) == 1 {
			return c.SSH(nil, args[1:])
		}

		cmd := strings.TrimSpace(strings.Join(args[1:], " "))
		title := cmd
		if len(title) > 30 {
			title = title[:27] + "..."
		}
		return c.Run(os.Stdout, os.Stderr, c.Nodes, title, cmd)
	}),
}

var resetCmd = &cobra.Command{
	Use:   "reset <cluster>",
	Short: "reset *all* VMs in a cluster",
	Long: `Reset a cloud VM. This may not be implemented for all
environments and will fall back to a no-op.`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) (retErr error) {
		if numNodes <= 0 || numNodes >= 1000 {
			// Upper limit is just for safety.
			return fmt.Errorf("number of nodes must be in [1..999]")
		}

		clusterName, err := verifyClusterName(args[0])
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
	}),
}

var installCmd = &cobra.Command{
	Use:   "install <cluster> <software>",
	Short: "install 3rd party software",
	Long: `Install third party software. Currently available installation options are:

    ` + strings.Join(install.SortedCmds(), "\n    ") + `
`,
	Args: cobra.MinimumNArgs(2),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		return install.Install(c, args[1:])
	}),
}

var downloadCmd = &cobra.Command{
	Use:   "download <cluster> <url> <sha256> [DESTINATION]",
	Short: "download 3rd party tools",
	Long:  "Downloads 3rd party tools, using a GCS cache if possible.",
	Args:  cobra.RangeArgs(3, 4),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		src, sha := args[1], args[2]
		var dest string
		if len(args) == 4 {
			dest = args[3]
		}
		return install.Download(c, src, sha, dest)
	}),
}

var stageCmd = &cobra.Command{
	Use:   "stage <cluster> <application> [<sha/version>]",
	Short: "stage cockroach binaries",
	Long: `Stages release and edge binaries to the cluster.

Currently available application options are:
  cockroach - Cockroach Unofficial. Can provide an optional SHA, otherwise
              latest build version is used.
  workload  - Cockroach workload application.
  release   - Official CockroachDB Release. Must provide a specific release
              version.

Some examples of usage:
  -- stage edge build of cockroach build at a specific SHA:
  roachprod stage my-cluster cockroach e90e6903fee7dd0f88e20e345c2ddfe1af1e5a97

  -- Stage the most recent edge build of the workload tool:
  roachprod stage my-cluster workload

  -- Stage the official release binary of CockroachDB at version 2.0.5
  roachprod stage my-cluster release v2.0.5
`,
	Args: cobra.RangeArgs(2, 3),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
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

		applicationName := args[1]
		versionArg := ""
		if len(args) == 3 {
			versionArg = args[2]
		}
		switch applicationName {
		case "cockroach":
			sha, err := install.StageRemoteBinary(
				c, applicationName, "cockroach/cockroach", versionArg, debugArch, dir,
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
				c, applicationName, "cockroach/workload", versionArg, "" /* arch */, dir,
			)
			return err
		case "release":
			return install.StageCockroachRelease(c, versionArg, releaseArch, dir)
		default:
			return fmt.Errorf("unknown application %s", applicationName)
		}
	}),
}

var distributeCertsCmd = &cobra.Command{
	Use:   "distribute-certs <cluster>",
	Short: "distribute certificates to the nodes in a cluster",
	Long: `Distribute certificates to the nodes in a cluster.
If the certificates already exist, no action is taken. Note that this command is
invoked automatically when a secure cluster is bootstrapped by "roachprod
start."
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		c.DistributeCerts()
		return nil
	}),
}

var putCmd = &cobra.Command{
	Use:   "put <cluster> <src> [<dest>]",
	Short: "copy a local file to the nodes in a cluster",
	Long: `Copy a local file to the nodes in a cluster.
`,
	Args: cobra.RangeArgs(2, 3),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		src := args[1]
		dest := path.Base(src)
		if len(args) == 3 {
			dest = args[2]
		}
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		c.Put(src, dest)
		return nil
	}),
}

var getCmd = &cobra.Command{
	Use:   "get <cluster> <src> [<dest>]",
	Short: "copy a remote file from the nodes in a cluster",
	Long: `Copy a remote file from the nodes in a cluster. If the file is retrieved from
multiple nodes the destination file name will be prefixed with the node number.
`,
	Args: cobra.RangeArgs(2, 3),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		src := args[1]
		dest := path.Base(src)
		if len(args) == 3 {
			dest = args[2]
		}
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		c.Get(src, dest)
		return nil
	}),
}

var sqlCmd = &cobra.Command{
	Use:   "sql <cluster> -- [args]",
	Short: "run `cockroach sql` on a remote cluster",
	Long:  "Run `cockroach sql` on a remote cluster.\n",
	Args:  cobra.MinimumNArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}
		cockroach, ok := c.Impl.(install.Cockroach)
		if !ok {
			return errors.New("sql is only valid on cockroach clusters")
		}
		return cockroach.SQL(c, args[1:])
	}),
}

var pgurlCmd = &cobra.Command{
	Use:   "pgurl <cluster>",
	Short: "generate pgurls for the nodes in a cluster",
	Long: `Generate pgurls for the nodes in a cluster.
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
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
	}),
}

var pprofOptions = struct {
	heap         bool
	open         bool
	startingPort int
	duration     time.Duration
}{}

var pprofCmd = &cobra.Command{
	Use:     "pprof <cluster>",
	Args:    cobra.ExactArgs(1),
	Aliases: []string{"pprof-heap"},
	Short:   "capture a pprof profile from the specified nodes",
	Long: `Capture a pprof profile from the specified nodes.

Examples:

    # Capture CPU profile for all nodes in the cluster
    roachprod pprof CLUSTERNAME
    # Capture CPU profile for the first node in the cluster for 60 seconds
    roachprod pprof CLUSTERNAME:1 --duration 60s
    # Capture a Heap profile for the first node in the cluster
    roachprod pprof CLUSTERNAME:1 --heap
    # Same as above
    roachprod pprof-heap CLUSTERNAME:1
`,
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
		if err != nil {
			return err
		}

		var profType string
		var description string
		if cmd.CalledAs() == "pprof-heap" || pprofOptions.heap {
			description = "capturing heap profile"
			profType = "heap"
		} else {
			description = "capturing CPU profile"
			profType = "profile"
		}

		outputFiles := []string{}
		mu := &syncutil.Mutex{}
		pprofPath := fmt.Sprintf("debug/pprof/%s?seconds=%d", profType, int(pprofOptions.duration.Seconds()))

		minTimeout := 30 * time.Second
		timeout := 2 * pprofOptions.duration
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

		if pprofOptions.open {
			waitCommands := []*exec.Cmd{}
			for i, file := range outputFiles {
				port := pprofOptions.startingPort + i
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
	}),
}

var adminurlCmd = &cobra.Command{
	Use:     "adminurl <cluster>",
	Aliases: []string{"admin", "adminui"},
	Short:   "generate admin UI URLs for the nodes in a cluster\n",
	Long: `Generate admin UI URLs for the nodes in a cluster.
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
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
	}),
}

var ipCmd = &cobra.Command{
	Use:   "ip <cluster>",
	Short: "get the IP addresses of the nodes in a cluster",
	Long: `Get the IP addresses of the nodes in a cluster.
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0])
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

		for _, ip := range ips {
			fmt.Println(ip)
		}
		return nil
	}),
}

func main() {
	// The commands are displayed in the order they are added to rootCmd. Note
	// that gcCmd and adminurlCmd contain a trailing \n in their Short help in
	// order to separate the commands into logical groups.
	cobra.EnableCommandSorting = false
	rootCmd.AddCommand(
		createCmd,
		resetCmd,
		destroyCmd,
		extendCmd,
		listCmd,
		syncCmd,
		gcCmd,
		setupSSHCmd,

		statusCmd,
		monitorCmd,
		startCmd,
		stopCmd,
		initCmd,
		runCmd,
		wipeCmd,
		reformatCmd,
		installCmd,
		distributeCertsCmd,
		putCmd,
		getCmd,
		stageCmd,
		downloadCmd,
		sqlCmd,
		ipCmd,
		pgurlCmd,
		adminurlCmd,
		logsCmd,
		pprofCmd,
		cachedHostsCmd,
	)
	rootCmd.BashCompletionFunction = fmt.Sprintf(`__custom_func()
	{
		# only complete the 2nd arg, e.g. adminurl <foo>
		if ! [ $c -eq 2 ]; then
			return
		fi

		# don't complete commands which do not accept a cluster/host arg
		case ${last_command} in
			%s)
				return
				;;
		esac

		local hosts_out
		if hosts_out=$(roachprod cached-hosts --cluster="${cur}" 2>/dev/null); then
				COMPREPLY=( $( compgen -W "${hosts_out[*]}" -- "$cur" ) )
		fi

	}`,
		strings.Join(func(cmds ...*cobra.Command) (s []string) {
			for _, cmd := range cmds {
				s = append(s, fmt.Sprintf("%s_%s", rootCmd.Name(), cmd.Name()))
			}
			return s
		}(createCmd, listCmd, syncCmd, gcCmd), " | "),
	)

	rootCmd.PersistentFlags().BoolVarP(
		&quiet, "quiet", "q", false, "disable fancy progress output")
	rootCmd.PersistentFlags().IntVarP(
		&maxConcurrency, "max-concurrency", "", 32,
		"maximum number of operations to execute on nodes concurrently, set to zero for infinite")
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
		"all-mine", "m", false, "Destroy all clusters belonging to the current user")

	extendCmd.Flags().DurationVarP(&extendLifetime,
		"lifetime", "l", 12*time.Hour, "Lifetime of the cluster")

	listCmd.Flags().BoolVarP(&listDetails,
		"details", "d", false, "Show cluster details")
	listCmd.Flags().BoolVar(&listJSON,
		"json", false, "Show cluster specs in a json format")
	listCmd.Flags().BoolVarP(&listMine,
		"mine", "m", false, "Show only clusters belonging to the current user")

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
				&install.StartOpts.Sequential, "sequential", true,
				"start nodes sequentially so node IDs match hostnames")
			cmd.Flags().StringArrayVarP(
				&nodeArgs, "args", "a", nil, "node arguments")
			cmd.Flags().StringArrayVarP(
				&nodeEnv, "env", "e", nodeEnv, "node environment variables")
			cmd.Flags().BoolVar(
				&install.StartOpts.Encrypt, "encrypt", encrypt, "start nodes with encryption at rest turned on")
			cmd.Flags().BoolVar(
				&install.StartOpts.SkipInit, "skip-init", skipInit, "skip initializing the cluster")
			cmd.Flags().IntVar(
				&install.StartOpts.StoreCount, "store-count", 1, "number of stores to start each node with")
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

		if cmd.Long == "" {
			cmd.Long = cmd.Short
		}
		cmd.Long += fmt.Sprintf(`
Node specification

  By default the operation is performed on all nodes in <cluster>. A subset of
  nodes can be specified by appending :<nodes> to the cluster name. The syntax
  of <nodes> is a comma separated list of specific node IDs or range of
  IDs. For example:

    roachprod %[1]s marc-test:1-3,8-9

  will perform %[1]s on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9
`, cmd.Name())
	}

	var err error
	config.OSUser, err = user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to lookup current user: %s\n", err)
		os.Exit(1)
	}

	if err := initDirs(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	if err := loadClusters(); err != nil {
		// We don't want to exit as we may be looking at the help message.
		fmt.Printf("problem loading clusters: %s\n", err)
	}

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
