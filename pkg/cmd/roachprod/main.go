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
	"net/url"
	"os"
	"os/user"
	"path"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/grafana"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ui"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
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
	Version: "details:\n" + build.GetInfo().Long(),
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

  The underlying filesystem can be provided using the --filesystem flag.
  Use --filesystem=zfs, for zfs, and --filesystem=ext4, for ext4. The default
  file system is ext4. The filesystem flag only works on gce currently.

Local Clusters

  A local cluster stores the per-node data in ${HOME}/local on the machine
  roachprod is being run on. Whether a cluster is local is specified on creation
  by using the name 'local' or 'local-<anything>'. Local clusters have no expiration.
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) (retErr error) {
		createVMOpts.ClusterName = args[0]
		return roachprod.Create(context.Background(), config.Logger, username, numNodes, createVMOpts, providerOptsContainer)
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
		return roachprod.SetupSSH(context.Background(), config.Logger, args[0])
	}),
}

var destroyCmd = &cobra.Command{
	Use:   "destroy [ --all-mine | --all-local | <cluster 1> [<cluster 2> ...] ]",
	Short: "destroy clusters",
	Long: `Destroy one or more local or cloud-based clusters.

The destroy command accepts the names of the clusters to destroy. Alternatively,
the --all-mine flag can be provided to destroy all (non-local) clusters that are
owned by the current user, or the --all-local flag can be provided to destroy
all local clusters.

Destroying a cluster releases the resources for a cluster. For a cloud-based
cluster the machine and associated disk resources are freed. For a local
cluster, any processes started by roachprod are stopped, and the node
directories inside ${HOME}/local directory are removed.
`,
	Args: cobra.ArbitraryArgs,
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		return roachprod.Destroy(config.Logger, destroyAllMine, destroyAllLocal, args...)
	}),
}

var cachedHostsCmd = &cobra.Command{
	Use:   "cached-hosts",
	Short: "list all clusters (and optionally their host numbers) from local cache",
	Args:  cobra.NoArgs,
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		roachprod.CachedClusters(config.Logger, func(clusterName string, numVMs int) {
			if strings.HasPrefix(clusterName, "teamcity") {
				return
			}
			fmt.Printf("%s", clusterName)
			// When invoked by bash-completion, cachedHostsCluster is what the user
			// has currently typed -- if this cluster matches that, expand its hosts.
			if strings.HasPrefix(cachedHostsCluster, clusterName) {
				for i := 1; i <= numVMs; i++ {
					fmt.Printf(" %s:%d", clusterName, i)
				}
			}
			fmt.Printf("\n")
		})
		return nil
	}),
}

var listCmd = &cobra.Command{
	Use:   "list [--details | --json] [ --mine | --pattern ]",
	Short: "list all clusters",
	Long: `List all clusters.

The list command accepts a flag --pattern which is a regular
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
	Args: cobra.NoArgs,
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		if listJSON && listDetails {
			return errors.New("'json' option cannot be combined with 'details' option")
		}
		filteredCloud, err := roachprod.List(config.Logger, listMine, listPattern)
		if err != nil {
			return err
		}

		// sort by cluster names for stable output.
		names := make([]string, len(filteredCloud.Clusters))
		i := 0
		for name := range filteredCloud.Clusters {
			names[i] = name
			i++
		}
		sort.Strings(names)

		if listJSON {
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
					c.PrintDetails(config.Logger)
				} else {
					fmt.Fprintf(tw, "%s\t%s\t%d", c.Name, c.Clouds(), len(c.VMs))
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

var bashCompletion = os.ExpandEnv("$HOME/.roachprod/bash-completion.sh")

// TODO(peter): Do we need this command given that the "list" command syncs as
// a side-effect. If you don't care about the list output, just "roachprod list
// &>/dev/null".
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "sync ssh keys/config and hosts files",
	Long:  ``,
	Args:  cobra.NoArgs,
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		_, err := roachprod.Sync(config.Logger, vm.ListOptions{IncludeVolumes: listOpts.IncludeVolumes})
		_ = rootCmd.GenBashCompletionFile(bashCompletion)
		return err
	}),
}

var gcCmd = &cobra.Command{
	Use:   "gc",
	Short: "GC expired clusters and unused AWS keypairs\n",
	Long: `Garbage collect expired clusters and unused SSH keypairs in AWS.

Destroys expired clusters, sending email if properly configured. Usually run
hourly by a cronjob so it is not necessary to run manually.
`,
	Args: cobra.NoArgs,
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		return roachprod.GC(config.Logger, dryrun)
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
		return roachprod.Extend(config.Logger, args[0], extendLifetime)
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
IDs match hostnames. Otherwise nodes are started in parallel.

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
		clusterSettingsOpts := []install.ClusterSettingOption{
			install.TagOption(tag),
			install.PGUrlCertsDirOption(pgurlCertsDir),
			install.SecureOption(secure),
			install.UseTreeDistOption(useTreeDist),
			install.EnvOption(nodeEnv),
			install.NumRacksOption(numRacks),
		}
		return roachprod.Start(context.Background(), config.Logger, args[0], startOpts, clusterSettingsOpts...)
	}),
}

var stopCmd = &cobra.Command{
	Use:   "stop <cluster> [--sig] [--wait]",
	Short: "stop nodes on a cluster",
	Long: `Stop nodes on a cluster.

Stop roachprod created processes running on the nodes in a cluster, including
processes started by the "start", "run" and "ssh" commands. Every process
started by roachprod is tagged with a ROACHPROD environment variable which is
used by "stop" to locate the processes and terminate them. By default processes
are killed with signal 9 (SIGKILL) giving them no chance for a graceful exit.

The --sig flag will pass a signal to kill to allow us finer control over how we
shutdown cockroach. The --wait flag causes stop to loop waiting for all
processes with the right ROACHPROD environment variable to exit. Note that stop
will wait forever if you specify --wait with a non-terminating signal (e.g.
SIGHUP), unless you also configure --max-wait.
--wait defaults to true for signal 9 (SIGKILL) and false for all other signals.
` + tagHelp + `
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		wait := waitFlag
		if sig == 9 /* SIGKILL */ && !cmd.Flags().Changed("wait") {
			wait = true
		}
		stopOpts := roachprod.StopOpts{Wait: wait, MaxWait: maxWait, ProcessTag: tag, Sig: sig}
		return roachprod.Stop(context.Background(), config.Logger, args[0], stopOpts)
	}),
}

var startTenantCmd = &cobra.Command{
	Use:   "start-tenant <tenant-cluster> --host-cluster <host-cluster>",
	Short: "start a tenant",
	Long: `Start SQL instances for a non-system tenant.

The --host-cluster flag must be used to specify a host cluster (with optional
node selector) which is already running. The command will create the tenant on
the host cluster if it does not exist already. The host and tenant can use the
same underlying cluster, as long as different subsets of nodes are selected.

The --tenant-id flag can be used to specify the tenant ID; it defaults to 2.

The --secure flag can be used to start nodes in secure mode (i.e. using
certs). When specified, there is a one time initialization for the cluster to
create and distribute the certs. Note that running some modes in secure mode
and others in insecure mode is not a supported Cockroach configuration.

As a debugging aid, the --sequential flag starts the nodes sequentially so node
IDs match hostnames. Otherwise nodes are started in parallel.

The --binary flag specifies the remote binary to run. It is up to the roachprod
user to ensure this binary exists, usually via "roachprod put". Note that no
cockroach software is installed by default on a newly created cluster.

The --args and --env flags can be used to pass arbitrary command line flags and
environment variables to the cockroach process.
` + tagHelp + `
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		tenantCluster := args[0]
		clusterSettingsOpts := []install.ClusterSettingOption{
			install.TagOption(tag),
			install.PGUrlCertsDirOption(pgurlCertsDir),
			install.SecureOption(secure),
			install.UseTreeDistOption(useTreeDist),
			install.EnvOption(nodeEnv),
			install.NumRacksOption(numRacks),
		}
		return roachprod.StartTenant(context.Background(), config.Logger, tenantCluster, hostCluster, startOpts, clusterSettingsOpts...)
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
		return roachprod.Init(context.Background(), config.Logger, args[0], startOpts)
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
		statuses, err := roachprod.Status(context.Background(), config.Logger, args[0], tag)
		if err != nil {
			return err
		}
		for _, status := range statuses {
			if status.Err != nil {
				config.Logger.Printf("  %2d: %s %s\n", status.NodeID, status.Err.Error())
			} else if !status.Running {
				config.Logger.Printf("  %2d: not running\n", status.NodeID)
			} else {
				config.Logger.Printf("  %2d: %s %s\n", status.NodeID, status.Version, status.Pid)
			}
		}
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
		logsOpts := roachprod.LogsOpts{
			Dir: logsDir, Filter: logsFilter, ProgramFilter: logsProgramFilter,
			Interval: logsInterval, From: logsFrom, To: logsTo, Out: cmd.OutOrStdout(),
		}
		var dest string
		if len(args) == 2 {
			dest = args[1]
		} else {
			dest = args[0] + ".logs"
		}
		return roachprod.Logs(config.Logger, args[0], dest, username, logsOpts)
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
		messages, err := roachprod.Monitor(context.Background(), config.Logger, args[0], monitorOpts)
		if err != nil {
			return err
		}
		for msg := range messages {
			if msg.Err != nil {
				msg.Msg += "error: " + msg.Err.Error()
			}
			thisError := errors.Newf("%d: %s", msg.Node, msg.Msg)
			if msg.Err != nil || strings.Contains(msg.Msg, "dead") {
				err = errors.CombineErrors(err, thisError)
			}
			fmt.Println(thisError.Error())
		}
		return err
	}),
}

var signalCmd = &cobra.Command{
	Use:   "signal <cluster> <signal>",
	Short: "send signal to cluster",
	Long:  "Send a POSIX signal to the nodes in a cluster, specified by its integer code.",
	Args:  cobra.ExactArgs(2),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		sig, err := strconv.ParseInt(args[1], 10, 8)
		if err != nil {
			return errors.Wrapf(err, "invalid signal argument")
		}
		return roachprod.Signal(context.Background(), config.Logger, args[0], int(sig))
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
		return roachprod.Wipe(context.Background(), config.Logger, args[0], wipePreserveCerts)
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
		return roachprod.Reformat(context.Background(), config.Logger, args[0], args[1])
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
		return roachprod.Run(context.Background(), config.Logger, args[0], extraSSHOptions, tag, secure, os.Stdout, os.Stderr, args[1:])
	}),
}

var resetCmd = &cobra.Command{
	Use:   "reset <cluster>",
	Short: "reset *all* VMs in a cluster",
	Long: `Reset a cloud VM. This may not be implemented for all
environments and will fall back to a no-op.`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) (retErr error) {
		return roachprod.Reset(config.Logger, args[0])
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
		return roachprod.Install(context.Background(), config.Logger, args[0], args[1:])
	}),
}

var downloadCmd = &cobra.Command{
	Use:   "download <cluster> <url> <sha256> [DESTINATION]",
	Short: "download 3rd party tools",
	Long:  "Downloads 3rd party tools, using a GCS cache if possible.",
	Args:  cobra.RangeArgs(3, 4),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		src, sha := args[1], args[2]
		var dest string
		if len(args) == 4 {
			dest = args[3]
		}
		return roachprod.Download(context.Background(), config.Logger, args[0], src, sha, dest)
	}),
}

var stageURLCmd = &cobra.Command{
	Use:   "stageurl <application> [<sha/version>]",
	Short: "print URL to cockroach binaries",
	Long: `Prints URL for release and edge binaries.

Currently available application options are:
  cockroach - Cockroach Unofficial. Can provide an optional SHA, otherwise
              latest build version is used.
  workload  - Cockroach workload application.
  release   - Official CockroachDB Release. Must provide a specific release
              version.
`,
	Args: cobra.RangeArgs(1, 2),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		versionArg := ""
		if len(args) == 2 {
			versionArg = args[1]
		}
		urls, err := roachprod.StageURL(config.Logger, args[0], versionArg, stageOS)
		if err != nil {
			return err
		}
		for _, u := range urls {
			fmt.Println(u)
		}
		return nil
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
		versionArg := ""
		if len(args) == 3 {
			versionArg = args[2]
		}
		return roachprod.Stage(context.Background(), config.Logger, args[0], stageOS, stageDir, args[1], versionArg)
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
		return roachprod.DistributeCerts(context.Background(), config.Logger, args[0])
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
		return roachprod.Put(context.Background(), config.Logger, args[0], src, dest, useTreeDist)
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
		return roachprod.Get(config.Logger, args[0], src, dest)
	}),
}

var sqlCmd = &cobra.Command{
	Use:   "sql <cluster> -- [args]",
	Short: "run `cockroach sql` on a remote cluster",
	Long:  "Run `cockroach sql` on a remote cluster.\n",
	Args:  cobra.MinimumNArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		return roachprod.SQL(context.Background(), config.Logger, args[0], secure, tenantName, args[1:])
	}),
}

var pgurlCmd = &cobra.Command{
	Use:   "pgurl <cluster>",
	Short: "generate pgurls for the nodes in a cluster",
	Long: `Generate pgurls for the nodes in a cluster.
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		urls, err := roachprod.PgURL(context.Background(), config.Logger, args[0], pgurlCertsDir, roachprod.PGURLOptions{
			External:   external,
			Secure:     secure,
			TenantName: tenantName,
		})
		if err != nil {
			return err
		}
		fmt.Println(strings.Join(urls, " "))
		return nil
	}),
}

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
		if cmd.CalledAs() == "pprof-heap" {
			pprofOpts.Heap = true
		}
		return roachprod.Pprof(config.Logger, args[0], pprofOpts)
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
		urls, err := roachprod.AdminURL(config.Logger, args[0], adminurlPath, adminurlIPs, adminurlOpen, secure)
		if err != nil {
			return err
		}
		for _, url := range urls {
			fmt.Println(url)
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
		ips, err := roachprod.IP(context.Background(), config.Logger, args[0], external)
		if err != nil {
			return err
		}
		for _, ip := range ips {
			fmt.Println(ip)
		}
		return nil
	}),
}

var versionCmd = &cobra.Command{
	Use:   `version`,
	Short: `print version information`,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println(roachprod.Version(config.Logger))
		return nil
	},
}

var getProvidersCmd = &cobra.Command{
	Use:   `get-providers`,
	Short: `print providers state (active/inactive)`,
	RunE: func(cmd *cobra.Command, args []string) error {
		providers := roachprod.InitProviders()
		for provider, state := range providers {
			fmt.Printf("%s: %s\n", provider, state)
		}
		return nil
	},
}

var grafanaStartCmd = &cobra.Command{
	Use:   `grafana-start <cluster>`,
	Short: `spins up a prometheus and grafana instance on the last node in the cluster`,
	Args:  cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		var grafanaDashboardJSONs []string
		var grafanaConfigURL string
		if grafanaConfig != "" {
			url, err := url.Parse(grafanaConfig)
			if err != nil {
				return err
			}
			switch url.Scheme {
			case "http", "https":
				grafanaConfigURL = grafanaConfig
			case "file", "":
				if data, err := grafana.GetDashboardJSONFromFile(url.Path); err != nil {
					return err
				} else {
					grafanaDashboardJSONs = []string{data}
				}
			default:
				return errors.Newf("unsupported scheme %s", url.Scheme)
			}
		} else {
			var err error
			if grafanaDashboardJSONs, err = grafana.GetDefaultDashboardJSONs(); err != nil {
				return err
			}
		}

		return roachprod.StartGrafana(context.Background(), config.Logger, args[0],
			grafanaConfigURL, grafanaDashboardJSONs, nil)
	}),
}

var grafanaStopCmd = &cobra.Command{
	Use:   `grafana-stop <cluster>`,
	Short: `spins down prometheus and grafana instances on the last node in the cluster`,
	Args:  cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		return roachprod.StopGrafana(context.Background(), config.Logger, args[0], "")
	}),
}

var grafanaDumpCmd = &cobra.Command{
	Use:   `grafana-dump <cluster>`,
	Short: `dump prometheus data to the specified directory`,
	Args:  cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		if grafanaDumpDir == "" {
			return errors.New("--dump-dir unspecified")
		}
		return roachprod.PrometheusSnapshot(context.Background(), config.Logger, args[0], grafanaDumpDir)
	}),
}

var grafanaURLCmd = &cobra.Command{
	Use:   `grafanaurl <cluster>`,
	Short: `returns a url to the grafana dashboard`,
	Args:  cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		url, err := roachprod.GrafanaURL(context.Background(), config.Logger, args[0],
			grafanaurlOpen)
		if err != nil {
			return err
		}
		fmt.Println(url)
		return nil
	}),
}

var rootStorageCmd = &cobra.Command{
	Use:   `storage`,
	Short: "storage enables administering storage related commands and configurations",
	Args:  cobra.MinimumNArgs(1),
}

var rootStorageCollectionCmd = &cobra.Command{
	Use: `collection`,
	Short: "the collection command allows for enable or disabling the storage workload " +
		"collector for a provided cluster (including a subset of nodes). The storage workload " +
		"collection is defined in pebble replay/workload_capture.go.",
	Args: cobra.MinimumNArgs(1),
}

var collectionStartCmd = &cobra.Command{
	Use:   `start <cluster>`,
	Short: "start the workload collector for a provided cluster (including a subset of nodes)",
	Args:  cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		cluster := args[0]
		return roachprod.StorageCollectionPerformAction(
			context.Background(),
			config.Logger,
			cluster,
			"start",
			volumeCreateOpts,
		)
	}),
}

var collectionStopCmd = &cobra.Command{
	Use:   `stop <cluster>`,
	Short: "stop the workload collector for a provided cluster (including a subset of nodes)",
	Args:  cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		cluster := args[0]
		return roachprod.StorageCollectionPerformAction(
			context.Background(),
			config.Logger,
			cluster,
			"stop",
			volumeCreateOpts,
		)
	}),
}

var collectionListVolumes = &cobra.Command{
	Use:   `list-volumes <cluster>`,
	Short: "list the nodes and their attached collector volumes",
	Args:  cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		cluster := args[0]
		return roachprod.StorageCollectionPerformAction(
			context.Background(),
			config.Logger,
			cluster,
			"list-volumes",
			volumeCreateOpts,
		)
	}),
}

var storageSnapshotCmd = &cobra.Command{
	Use:   `snapshot <cluster> <name> <description>`,
	Short: "snapshot a clusters workload collector volume",
	Args:  cobra.ExactArgs(3),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		cluster := args[0]
		name := args[1]
		desc := args[2]
		return roachprod.SnapshotVolume(context.Background(), config.Logger, cluster, name, desc)
	}),
}

var fixLongRunningAWSHostnamesCmd = &cobra.Command{
	Use:   "fix-long-running-aws-hostnames <cluster>",
	Short: "changes the hostnames of VMs in long-running AWS clusters",
	Long: `This is a temporary workaround, and will be removed once we no longer ` +
		`have AWS clusters that were created with the default hostname.`,

	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) (retErr error) {
		return roachprod.FixLongRunningAWSHostnames(context.Background(), config.Logger, args[0])
	}),
}

func main() {
	_ = roachprod.InitProviders()
	providerOptsContainer = vm.CreateProviderOptionsContainer()
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
		startTenantCmd,
		initCmd,
		runCmd,
		signalCmd,
		wipeCmd,
		reformatCmd,
		installCmd,
		distributeCertsCmd,
		putCmd,
		getCmd,
		stageCmd,
		stageURLCmd,
		downloadCmd,
		sqlCmd,
		ipCmd,
		pgurlCmd,
		adminurlCmd,
		logsCmd,
		pprofCmd,
		cachedHostsCmd,
		versionCmd,
		getProvidersCmd,
		grafanaStartCmd,
		grafanaStopCmd,
		grafanaDumpCmd,
		grafanaURLCmd,
		rootStorageCmd,
		fixLongRunningAWSHostnamesCmd,
	)
	setBashCompletionFunction()

	// Add help about specifying nodes
	for _, cmd := range []*cobra.Command{
		getCmd, putCmd, runCmd, startCmd, statusCmd, stopCmd, signalCmd,
		wipeCmd, pgurlCmd, adminurlCmd, sqlCmd, installCmd,
	} {
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

	initFlags()

	var err error
	config.OSUser, err = user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to lookup current user: %s\n", err)
		os.Exit(1)
	}

	if err := roachprod.InitDirs(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	if err := roachprod.LoadClusters(); err != nil {
		// We don't want to exit as we may be looking at the help message.
		fmt.Printf("problem loading clusters: %s\n", err)
	}

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
