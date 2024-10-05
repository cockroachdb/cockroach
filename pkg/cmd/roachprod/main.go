// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/update"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ui"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
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
	Version:          "details:\n" + build.GetInfo().Long(),
	PersistentPreRun: validateAndConfigure,
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
		opts := cloud.ClusterCreateOpts{Nodes: numNodes, CreateOpts: createVMOpts, ProviderOptsContainer: providerOptsContainer}
		return roachprod.Create(context.Background(), config.Logger, username, &opts)
	}),
}

var growCmd = &cobra.Command{
	Use:   `grow <cluster> <num-nodes>`,
	Short: `grow a cluster by adding nodes`,
	Long: `grow a cluster by adding the specified number of nodes to it.

The cluster has to be a managed cluster (i.e., a cluster created with the
gce-managed flag). Only Google Cloud clusters currently support adding nodes.
The new nodes will use the instance template that was used to create the cluster
originally (Nodes will be created in the same zone as the existing nodes, or if
the cluster is geographically distributed, the nodes will be fairly distributed
across the zones of the cluster).
`,
	Args: cobra.ExactArgs(2),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		count, err := strconv.ParseInt(args[1], 10, 8)
		if err != nil || count < 1 {
			return errors.Wrapf(err, "invalid num-nodes argument")
		}
		return roachprod.Grow(context.Background(), config.Logger, args[0], int(count))
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
		roachprod.CachedClusters(func(clusterName string, numVMs int) {
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
		filteredCloud, err := roachprod.List(config.Logger, listMine, listPattern, vm.ListOptions{ComputeEstimatedCost: true})

		if err != nil {
			return err
		}

		// sort by cluster names for stable output.
		names := make([]string, len(filteredCloud.Clusters))
		maxClusterName := 0
		i := 0
		for name := range filteredCloud.Clusters {
			names[i] = name
			if len(name) > maxClusterName {
				maxClusterName = len(name)
			}
			i++
		}
		sort.Strings(names)

		p := message.NewPrinter(language.English)
		if listJSON {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			if err := enc.Encode(filteredCloud); err != nil {
				return err
			}
		} else {
			machineType := func(clusterVMs vm.List) string {
				return clusterVMs[0].MachineType
			}
			cpuArch := func(clusterVMs vm.List) string {
				// Display CPU architecture and family.
				if clusterVMs[0].CPUArch == "" {
					// N.B. Either a local cluster or unsupported cloud provider.
					return ""
				}
				if clusterVMs[0].CPUFamily != "" {
					return clusterVMs[0].CPUFamily
				}
				if clusterVMs[0].CPUArch != vm.ArchAMD64 {
					return string(clusterVMs[0].CPUArch)
				}
				// AMD64 is the default, so don't display it.
				return ""
			}
			// Align columns left and separate with at least two spaces.
			tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', tabwriter.AlignRight)
			// N.B. colors use escape codes which don't play nice with tabwriter [1].
			// We use a hacky workaround below to color the empty string.
			// [1] https://github.com/golang/go/issues/12073

			if !listDetails {
				// Print header only if we are not printing cluster details.
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n",
					"Cluster", "Clouds", "Size", "VM", "Arch",
					color.HiWhiteString("$/hour"), color.HiWhiteString("$ Spent"),
					color.HiWhiteString("Uptime"), color.HiWhiteString("TTL"),
					color.HiWhiteString("$/TTL"))
				// Print separator.
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n",
					"", "", "", "",
					color.HiWhiteString(""), color.HiWhiteString(""),
					color.HiWhiteString(""), color.HiWhiteString(""),
					color.HiWhiteString(""))
			}
			totalCostPerHour := 0.0
			for _, name := range names {
				c := filteredCloud.Clusters[name]
				if listDetails {
					if err = c.PrintDetails(config.Logger); err != nil {
						return err
					}
				} else {
					// N.B. Tabwriter doesn't support per-column alignment. It looks odd to have the cluster names right-aligned,
					// so we make it left-aligned.
					fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s", name+strings.Repeat(" ", maxClusterName-len(name)), c.Clouds(),
						len(c.VMs), machineType(c.VMs), cpuArch(c.VMs))
					if !c.IsLocal() {
						colorByCostBucket := func(cost float64) func(string, ...interface{}) string {
							switch {
							case cost <= 100:
								return color.HiGreenString
							case cost <= 1000:
								return color.HiBlueString
							default:
								return color.HiRedString
							}
						}
						timeRemaining := c.LifetimeRemaining().Round(time.Second)
						formatTTL := func(ttl time.Duration) string {
							if c.VMs[0].Preemptible {
								return color.HiMagentaString(ttl.String())
							} else {
								return color.HiBlueString(ttl.String())
							}
						}
						cost := c.CostPerHour
						totalCostPerHour += cost
						alive := timeutil.Since(c.CreatedAt).Round(time.Minute)
						costSinceCreation := cost * float64(alive) / float64(time.Hour)
						costRemaining := cost * float64(timeRemaining) / float64(time.Hour)
						if cost > 0 {
							fmt.Fprintf(tw, "\t%s\t%s\t%s\t%s\t%s\t",
								color.HiGreenString(p.Sprintf("$%.2f", cost)),
								colorByCostBucket(costSinceCreation)(p.Sprintf("$%.2f", costSinceCreation)),
								color.HiWhiteString(alive.String()),
								formatTTL(timeRemaining),
								colorByCostBucket(costRemaining)(p.Sprintf("$%.2f", costRemaining)))
						} else {
							fmt.Fprintf(tw, "\t%s\t%s\t%s\t%s\t%s\t",
								color.HiGreenString(""),
								color.HiGreenString(""),
								color.HiWhiteString(alive.String()),
								formatTTL(timeRemaining),
								color.HiGreenString(""))
						}
					} else {
						fmt.Fprintf(tw, "\t(-)")
					}
					fmt.Fprintf(tw, "\n")
				}
			}
			if err := tw.Flush(); err != nil {
				return err
			}

			if totalCostPerHour > 0 {
				_, _ = p.Printf("\nTotal cost per hour: $%.2f\n", totalCostPerHour)
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

var loadBalanceCmd = &cobra.Command{
	Use:   "load-balance <cluster>",
	Short: "create a load balancer for a cluster",
	Long: `Create a load balancer for a specific service (port), system by default, for the given cluster.

The load balancer is created using the cloud provider's load balancer service.
Currently only Google Cloud is supported, and the cluster must have been created
with the --gce-managed flag. On Google Cloud a load balancer consists of various
components that include backend services, health checks and forwarding rules.
These resources will automatically be destroyed when the cluster is destroyed.
`,

	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		return roachprod.CreateLoadBalancer(context.Background(), config.Logger,
			args[0], secure, virtualClusterName, sqlInstance,
		)
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
		stopOpts := roachprod.StopOpts{Wait: wait, GracePeriod: gracePeriod, ProcessTag: tag, Sig: sig}
		return roachprod.Stop(context.Background(), config.Logger, args[0], stopOpts)
	}),
}

var startInstanceCmd = &cobra.Command{
	Use:   "start-sql <name> --storage-cluster <storage-cluster> [--external-cluster <virtual-cluster-nodes>]",
	Short: "start the SQL/HTTP service for a virtual cluster as a separate process",
	Long: `Start SQL/HTTP instances for a virtual cluster as separate processes.

The --storage-cluster flag must be used to specify a storage cluster
(with optional node selector) which is already running. The command
will create the virtual cluster on the storage cluster if it does not
exist already.  If creating multiple virtual clusters on the same
node, the --sql-instance flag must be passed to differentiate them.

The instance is started in shared process (in memory) mode by
default. To start an external process instance, pass the
--external-cluster flag indicating where the SQL server processes
should be started.

The --secure flag can be used to start nodes in secure mode (i.e. using
certs). When specified, there is a one time initialization for the cluster to
create and distribute the certs. Note that running some modes in secure mode
and others in insecure mode is not a supported Cockroach configuration.

As a debugging aid, the --sequential flag starts the services
sequentially; otherwise services are started in parallel.

The --binary flag specifies the remote binary to run, if starting
external services. It is up to the roachprod user to ensure this
binary exists, usually via "roachprod put". Note that no cockroach
software is installed by default on a newly created cluster.

The --args and --env flags can be used to pass arbitrary command line flags and
environment variables to the cockroach process.
` + tagHelp + `
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
		// TODO(DarrylWong): remove once #117125 is addressed.
		startOpts.AdminUIPort = 0

		startOpts.Target = install.StartSharedProcessForVirtualCluster
		// If the user passed an `--external-nodes` option, we are
		// starting a separate process virtual cluster.
		if startOpts.VirtualClusterLocation != "" {
			startOpts.Target = install.StartServiceForVirtualCluster
		}

		startOpts.VirtualClusterName = args[0]
		return roachprod.StartServiceForVirtualCluster(
			context.Background(), config.Logger, storageCluster, startOpts, clusterSettingsOpts...,
		)
	}),
}

var stopInstanceCmd = &cobra.Command{
	Use:   "stop-sql <cluster> --cluster <name> --sql-instance <instance> [--sig] [--wait]",
	Short: "stop sql instances on a cluster",
	Long: `Stop sql instances on a cluster.

Stop roachprod created virtual clusters (shared or separate process). By default,
separate processes are killed with signal 9 (SIGKILL) giving them no chance for a
graceful exit.

The --sig flag will pass a signal to kill to allow us finer control over how we
shutdown processes. The --wait flag causes stop to loop waiting for all
processes to exit. Note that stop will wait forever if you specify --wait with a
non-terminating signal (e.g. SIGHUP), unless you also configure --max-wait.

--wait defaults to true for signal 9 (SIGKILL) and false for all other signals.
`,
	Args: cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		wait := waitFlag
		if sig == 9 /* SIGKILL */ && !cmd.Flags().Changed("wait") {
			wait = true
		}
		stopOpts := roachprod.StopOpts{
			Wait:               wait,
			GracePeriod:        gracePeriod,
			Sig:                sig,
			VirtualClusterName: virtualClusterName,
			SQLInstance:        sqlInstance,
		}
		clusterName := args[0]
		return roachprod.StopServiceForVirtualCluster(context.Background(), config.Logger, clusterName, secure, stopOpts)
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
				// TODO(irfansharif): Surface the staged version here?
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
		eventChan, err := roachprod.Monitor(context.Background(), config.Logger, args[0], monitorOpts)
		if err != nil {
			return err
		}
		for info := range eventChan {
			fmt.Println(info.String())
		}

		return nil
	}),
}

var signalCmd = &cobra.Command{
	Use:   "signal <cluster> <signal>",
	Short: "send signal to cluster",
	Long:  "Send a POSIX signal, specified by its integer code, to every process started via roachprod in a cluster.",
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
		return roachprod.Run(context.Background(), config.Logger, args[0], extraSSHOptions, tag,
			secure, os.Stdout, os.Stderr, args[1:], install.RunOptions{FailOption: install.FailSlow})
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
  cockroach  - Cockroach nightly builds. Can provide an optional SHA, otherwise
               latest build version is used.
  workload   - Cockroach workload application.
  release    - Official CockroachDB Release. Must provide a specific release
               version.
  customized - Cockroach customized builds, usually generated by running
               ./scripts/tag-custom-build.sh. Must provide a specific tag.
`,
	Args: cobra.RangeArgs(1, 2),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		versionArg := ""
		if len(args) == 2 {
			versionArg = args[1]
		}
		urls, err := roachprod.StageURL(config.Logger, args[0], versionArg, stageOS, stageArch)
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
  cockroach  - Cockroach nightly builds. Can provide an optional SHA, otherwise
               latest build version is used.
  workload   - Cockroach workload application.
  release    - Official CockroachDB Release. Must provide a specific release
               version.
  customized - Cockroach customized builds, usually generated by running
               ./scripts/tag-custom-build.sh. Must provide a specific tag.

Some examples of usage:
  -- stage edge build of cockroach build at a specific SHA:
  roachprod stage my-cluster cockroach e90e6903fee7dd0f88e20e345c2ddfe1af1e5a97

  -- Stage the most recent edge build of the workload tool:
  roachprod stage my-cluster workload

  -- Stage the official release binary of CockroachDB at version 2.0.5
  roachprod stage my-cluster release v2.0.5

  -- Stage customized binary of CockroachDB at version v23.2.0-alpha.2-4375-g7cd2b76ed00
  roachprod stage my-cluster customized v23.2.0-alpha.2-4375-g7cd2b76ed00
`,
	Args: cobra.RangeArgs(2, 3),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		versionArg := ""
		if len(args) == 3 {
			versionArg = args[2]
		}
		return roachprod.Stage(context.Background(), config.Logger, args[0], stageOS, stageArch, stageDir, args[1], versionArg)
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
		return roachprod.Get(context.Background(), config.Logger, args[0], src, dest)
	}),
}

var sqlCmd = &cobra.Command{
	Use:   "sql <cluster> -- [args]",
	Short: "run `cockroach sql` on a remote cluster",
	Long:  "Run `cockroach sql` on a remote cluster.\n",
	Args:  cobra.MinimumNArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		return roachprod.SQL(context.Background(), config.Logger, args[0], secure, virtualClusterName, sqlInstance, args[1:])
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
			External:           external,
			Secure:             secure,
			VirtualClusterName: virtualClusterName,
			SQLInstance:        sqlInstance,
			Auth:               install.AuthRootCert,
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
		return roachprod.Pprof(context.Background(), config.Logger, args[0], pprofOpts)
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
		urls, err := roachprod.AdminURL(
			context.Background(), config.Logger, args[0], virtualClusterName, sqlInstance, adminurlPath, adminurlIPs, urlOpen, secure,
		)
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
		ips, err := roachprod.IP(config.Logger, args[0], external)
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
	Short: `spins up a prometheus and grafana instance on the last node in the cluster; NOTE: for arm64 clusters, use --arch arm64`,
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
		arch := vm.ArchAMD64
		if grafanaArch == "arm64" {
			arch = vm.ArchARM64
		}
		return roachprod.StartGrafana(context.Background(), config.Logger, args[0], arch,
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
			urlOpen)
		if err != nil {
			return err
		}
		fmt.Println(url)
		return nil
	}),
}

var grafanaAnnotationCmd = &cobra.Command{
	Use:   `grafana-annotation <host> <text> --secure --tags [<tag1>, ...] --dashboard-uid <dashboard-uid> --time-range [<start-time>, <end-time>]`,
	Short: `adds an annotation to the specified grafana instance`,
	Long: fmt.Sprintf(`Adds an annotation to the specified grafana instance

--secure indicates if the grafana instance needs an authentication token to connect
to. If set, a service account json and audience will be read in from the environment
variables %s and %s to attempt authentication through google IDP.

--tags specifies the tags the annotation should have.

--dashboard-uid specifies the dashboard you want the annotation to be created in. If
left empty, creates the annotation in the organization instead.

--time-range can be used to specify in epoch millisecond time the annotation's timestamp.
If left empty, creates the annotation at the current time. If only start-time is specified,
creates an annotation at start-time. If both start-time and end-time are specified,
creates an annotation over time range.

Example:
# Create an annotation over time range 1-100 on the centralized grafana instance, which needs authentication.
roachprod grafana-annotation grafana.testeng.crdb.io example-annotation-event --secure --tags my-cluster --tags test-run-1 --dashboard-uid overview --time-range 1,100
`, grafana.ServiceAccountJson, grafana.ServiceAccountAudience),
	Args: cobra.ExactArgs(2),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		req := grafana.AddAnnotationRequest{
			Text:         args[1],
			Tags:         grafanaTags,
			DashboardUID: grafanaDashboardUID,
		}

		switch len(grafanaTimeRange) {
		case 0:
			// Grafana API will default to adding annotation at current time.
		case 1:
			// Okay to only specify the start time.
			req.StartTime = grafanaTimeRange[0]
		case 2:
			req.StartTime = grafanaTimeRange[0]
			req.EndTime = grafanaTimeRange[1]
		default:
			return errors.Newf("Too many arguments for --time-range, expected 1 or 2, got: %d", len(grafanaTimeRange))
		}

		return roachprod.AddGrafanaAnnotation(context.Background(), args[0] /* host */, secure, req)
	}),
}

var jaegerStartCmd = &cobra.Command{
	Use:   `jaeger-start <cluster>`,
	Short: `starts a jaeger container on the last node in the cluster`,
	Args:  cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		return roachprod.StartJaeger(context.Background(), config.Logger, args[0],
			virtualClusterName, secure, jaegerConfigNodes)
	}),
}

var jaegerStopCmd = &cobra.Command{
	Use:   `jaeger-stop <cluster>`,
	Short: `stops a running jaeger container on the last node in the cluster`,
	Args:  cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		return roachprod.StopJaeger(context.Background(), config.Logger, args[0])
	}),
}

var jaegerURLCmd = &cobra.Command{
	Use:   `jaegerurl <cluster>`,
	Short: `returns the URL of the cluster's jaeger UI`,
	Args:  cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		url, err := roachprod.JaegerURL(context.Background(), config.Logger, args[0],
			urlOpen)
		if err != nil {
			return err
		}
		fmt.Println(url)
		return nil
	}),
}

var destroyDNSCmd = &cobra.Command{
	Use:   `destroy-dns <cluster>`,
	Short: `cleans up DNS entries for the cluster`,
	Args:  cobra.ExactArgs(1),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		return roachprod.DestroyDNS(context.Background(), config.Logger, args[0])
	}),
}

var snapshotCmd = &cobra.Command{
	Use:   `snapshot`,
	Short: "snapshot enables creating/listing/deleting/applying cluster snapshots",
	Args:  cobra.MinimumNArgs(1),
}

var snapshotCreateCmd = &cobra.Command{
	Use:   `create <cluster> <name> <description>`,
	Short: "snapshot a named cluster, using the given snapshot name and description",
	Args:  cobra.ExactArgs(3),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		cluster := args[0]
		name := args[1]
		desc := args[2]
		snapshots, err := roachprod.CreateSnapshot(context.Background(), config.Logger, cluster, vm.VolumeSnapshotCreateOpts{
			Name:        name,
			Description: desc,
		})
		if err != nil {
			return err
		}
		for _, snapshot := range snapshots {
			config.Logger.Printf("created snapshot %s (id: %s)", snapshot.Name, snapshot.ID)
		}
		return nil
	}),
}

var snapshotListCmd = &cobra.Command{
	Use:   `list <provider> [<name>]`,
	Short: "list all snapshots for the given cloud provider, optionally filtering by the given name",
	Args:  cobra.RangeArgs(1, 2),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		provider := args[0]
		var name string
		if len(args) == 2 {
			name = args[1]
		}
		snapshots, err := roachprod.ListSnapshots(context.Background(), config.Logger, provider,
			vm.VolumeSnapshotListOpts{
				NamePrefix: name,
			},
		)
		if err != nil {
			return err
		}
		for _, snapshot := range snapshots {
			config.Logger.Printf("found snapshot %s (id: %s)", snapshot.Name, snapshot.ID)
		}
		return nil
	}),
}

var snapshotDeleteCmd = &cobra.Command{
	Use:   `delete <provider> <name>`,
	Short: "delete all snapshots for the given cloud provider optionally filtering by the given name",
	Args:  cobra.ExactArgs(2),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		provider, name := args[0], args[1]
		snapshots, err := roachprod.ListSnapshots(ctx, config.Logger, provider,
			vm.VolumeSnapshotListOpts{
				NamePrefix: name,
			},
		)
		if err != nil {
			return err
		}

		for _, snapshot := range snapshots {
			config.Logger.Printf("deleting snapshot %s (id: %s)", snapshot.Name, snapshot.ID)
		}
		if !dryrun {
			if err := roachprod.DeleteSnapshots(ctx, config.Logger, provider, snapshots...); err != nil {
				return err
			}
		}
		config.Logger.Printf("done")
		return nil
	}),
}

var snapshotApplyCmd = &cobra.Command{
	Use:   `apply <provider> <name> <cluster> `,
	Short: "apply the named snapshots from the given cloud provider to the named cluster",
	Args:  cobra.ExactArgs(3),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		provider, name, cluster := args[0], args[1], args[2]
		snapshots, err := roachprod.ListSnapshots(ctx, config.Logger, provider,
			vm.VolumeSnapshotListOpts{
				NamePrefix: name,
			},
		)
		if err != nil {
			return err
		}

		return roachprod.ApplySnapshots(ctx, config.Logger, cluster, snapshots, vm.VolumeCreateOpts{
			Size: 500, // TODO(irfansharif): Make this configurable?
			Labels: map[string]string{
				vm.TagUsage: "roachprod",
			},
		})
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
		_, err := roachprod.CreateSnapshot(context.Background(), config.Logger, cluster, vm.VolumeSnapshotCreateOpts{
			Name:        name,
			Description: desc,
		})
		return err
	}),
}

// Before executing any command, validate and canonicalize args.
func validateAndConfigure(cmd *cobra.Command, args []string) {
	// Skip validation for commands that are self-sufficient.
	switch cmd.Name() {
	case "help", "version", "list":
		return
	}

	printErrAndExit := func(err error) {
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}
	}

	// Validate architecture flag, if set.
	if archOpt := cmd.Flags().Lookup("arch"); archOpt != nil && archOpt.Changed {
		arch := vm.CPUArch(strings.ToLower(archOpt.Value.String()))

		if arch != vm.ArchAMD64 && arch != vm.ArchARM64 && arch != vm.ArchFIPS {
			printErrAndExit(fmt.Errorf("unsupported architecture %q", arch))
		}
		if string(arch) != archOpt.Value.String() {
			// Set the canonical value.
			_ = cmd.Flags().Set("arch", string(arch))
		}
	}

	// Validate cloud providers, if set.
	providersSet := make(map[string]struct{})
	for _, p := range createVMOpts.VMProviders {
		if _, ok := vm.Providers[p]; !ok {
			printErrAndExit(fmt.Errorf("unknown cloud provider %q", p))
		}
		if _, ok := providersSet[p]; ok {
			printErrAndExit(fmt.Errorf("duplicate cloud provider specified %q", p))
		}
		providersSet[p] = struct{}{}
	}
}

var updateCmd = &cobra.Command{
	Use:   "update",
	Short: "check gs://cockroach-nightly for a new roachprod binary; update if available",
	Long: "Attempts to download the latest roachprod binary (on master) from gs://cockroach-nightly. " +
		" Swaps the current binary with it. The current roachprod binary will be backed up" +
		" and can be restored via `roachprod update --revert`.",
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		currentBinary, err := os.Executable()
		if err != nil {
			return err
		}

		if roachprodUpdateRevert {
			if update.PromptYesNo("Revert to previous version? Note: this will replace the" +
				" current roachprod binary with a previous roachprod.bak binary.") {
				if err := update.SwapBinary(currentBinary, currentBinary+".bak"); err != nil {
					return err
				}
				fmt.Println("roachprod successfully reverted, run `roachprod -v` to confirm.")
			}
			return nil
		}

		newBinary := currentBinary + ".new"
		if err := update.DownloadLatestRoachprod(newBinary, roachprodUpdateBranch, roachprodUpdateOS, roachprodUpdateArch); err != nil {
			return err
		}

		if update.PromptYesNo("Continue with update? This will overwrite any existing roachprod.bak binary.") {
			if err := update.SwapBinary(currentBinary, newBinary); err != nil {
				return errors.WithDetail(err, "unable to update binary")
			}

			fmt.Println("Update successful: run `roachprod -v` to confirm.")
		}
		return nil
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
		growCmd,
		resetCmd,
		destroyCmd,
		extendCmd,
		loadBalanceCmd,
		listCmd,
		syncCmd,
		gcCmd,
		setupSSHCmd,
		statusCmd,
		monitorCmd,
		startCmd,
		stopCmd,
		startInstanceCmd,
		stopInstanceCmd,
		initCmd,
		runCmd,
		signalCmd,
		wipeCmd,
		destroyDNSCmd,
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
		grafanaAnnotationCmd,
		rootStorageCmd,
		snapshotCmd,
		updateCmd,
		jaegerStartCmd,
		jaegerStopCmd,
		jaegerURLCmd,
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

	updateTime, sha, err := update.CheckLatest(roachprodUpdateBranch, roachprodUpdateOS, roachprodUpdateArch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: failed to check if a more recent 'roachprod' binary exists: %s\n", err)
	} else {
		age, err := update.TimeSinceUpdate(updateTime)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARN: unable to check mtime of 'roachprod' binary: %s\n", err)
		} else if age.Hours() >= 14*24 {
			fmt.Fprintf(os.Stderr, "WARN: roachprod binary is >= 2 weeks old (%s); latest sha: %q\nWARN: Consider updating the binary: `roachprod update`\n\n", age, sha)
		}
	}

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
