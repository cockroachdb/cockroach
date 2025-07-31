// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ui"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/aws"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/local"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

// scpTimeout is the timeout enforced on every `scp` command performed
// by roachprod. The default of 10 minutes should be more than
// sufficient for roachtests and common roachprod operations.
//
// Callers can customize the timeout using the ROACHPROD_SCP_TIMEOUT
// environment variable.
var scpTimeout = func() time.Duration {
	var defaultTimeout = 10 * time.Minute

	if durStr := os.Getenv("ROACHPROD_SCP_TIMEOUT"); durStr != "" {
		dur, err := time.ParseDuration(durStr)
		if err != nil {
			panic(fmt.Errorf("invalid scp timeout %q: %w", durStr, err))
		}

		return dur
	}

	return defaultTimeout
}()

// A SyncedCluster is created from the cluster metadata in the synced clusters
// cache and is used as the target for installing and managing various software
// components.
type SyncedCluster struct {
	// Cluster metadata, obtained from the respective cloud provider.
	cloud.Cluster

	// Nodes is used by most commands (e.g. Start, Stop, Monitor). It describes
	// the list of nodes the operation pertains to.
	Nodes Nodes

	ClusterSettings

	Localities []string

	// AuthorizedKeys is used by SetupSSH to add additional authorized keys.
	AuthorizedKeys []byte

	// sessionProvider is a function that returns a new session. It serves as a
	// testing hook, and if null the default session implementations will be used.
	sessionProvider func(node Node, cmd string) session
}

// NewSyncedCluster creates a SyncedCluster, given the cluster metadata, node
// selector string, and settings.
//
// See ListNodes for a description of the node selector string.
func NewSyncedCluster(
	metadata *cloud.Cluster, nodeSelector string, settings ClusterSettings,
) (*SyncedCluster, error) {
	c := &SyncedCluster{
		Cluster:         *metadata,
		ClusterSettings: settings,
	}
	c.Localities = make([]string, len(c.VMs))
	for i := range c.VMs {
		locality, err := c.VMs[i].Locality()
		if err != nil {
			return nil, err
		}
		if c.NumRacks > 0 {
			if locality != "" {
				locality += ","
			}
			locality += fmt.Sprintf("rack=%d", i%c.NumRacks)
		}
		c.Localities[i] = locality
	}

	nodes, err := ListNodes(nodeSelector, len(c.VMs))
	if err != nil {
		return nil, err
	}
	c.Nodes = nodes
	return c, nil
}

// ErrAfterRetry marks an error that has occurred/persisted after retries
var ErrAfterRetry = errors.New("error occurred after retries")

// The first retry is after 5s, the second and final is after 25s
var DefaultRetryOpt = &retry.Options{
	InitialBackoff: 5 * time.Second,
	Multiplier:     5,
	MaxBackoff:     1 * time.Minute,
	// This will run a total of 3 times `runWithMaybeRetry`
	MaxRetries: 2,
}

var DefaultShouldRetryFn = func(res *RunResultDetails) bool { return rperrors.IsTransient(res.Err) }

// defaultSCPRetry won't retry if the error output contains any of the following
// substrings, in which cases retries are unlikely to help.
var noScpRetrySubstrings = []string{"no such file or directory", "permission denied", "connection timed out"}
var defaultSCPShouldRetryFn = func(res *RunResultDetails) bool {
	out := strings.ToLower(res.Output(false))
	for _, s := range noScpRetrySubstrings {
		if strings.Contains(out, s) {
			return false
		}
	}
	return true
}

// runWithMaybeRetry will run the specified function `f` at least once, or only
// once if `retryOpts` is nil
//
// Any RunResultDetails containing a non nil err from `f` is passed to `shouldRetryFn` which,
// if it returns true, will result in `f` being retried using the `RetryOpts`
// If the `shouldRetryFn` is not specified (nil), then retries will be performed
// regardless of the previous result / error.
//
// If a non-nil error (as opposed to the result containing a non-nil error) is returned,
// the function will *not* be retried.
//
// We operate on a pointer to RunResultDetails as it has already been
// captured in a *RunResultDetails[] in Run, but here we may enrich with attempt
// number and a wrapper error.
func runWithMaybeRetry(
	ctx context.Context,
	l *logger.Logger,
	retryOpts *retry.Options,
	shouldRetryFn func(*RunResultDetails) bool,
	f func(ctx context.Context) (*RunResultDetails, error),
) (*RunResultDetails, error) {
	if retryOpts == nil {
		res, err := f(ctx)
		if err != nil {
			return nil, err
		}
		res.Attempt = 1
		return res, nil
	}

	var res = &RunResultDetails{}
	var cmdErr, err error

	for r := retry.StartWithCtx(ctx, *retryOpts); r.Next(); {
		res, err = f(ctx)
		if err != nil {
			// non retryable roachprod error
			return nil, err
		}
		res.Attempt = r.CurrentAttempt() + 1
		if res.Err != nil {
			cmdErr = errors.CombineErrors(cmdErr, res.Err)
			if shouldRetryFn == nil || shouldRetryFn(res) {
				l.Printf("encountered [%v] on attempt %v of %v", res.Err, r.CurrentAttempt()+1, retryOpts.MaxRetries+1)
				continue
			}
		}
		break
	}

	if res.Attempt > 1 {
		if res.Err != nil {
			// An error cannot be marked with more than one reference error. Since res.Err may already be marked, we create
			// a new error here and mark it.
			res.Err = errors.Mark(errors.Wrapf(cmdErr, "error persisted after %v attempts", res.Attempt), ErrAfterRetry)
		} else {
			l.Printf("command successful after %v attempts", res.Attempt)
		}
	}
	return res, nil
}

func scpWithRetry(
	ctx context.Context, l *logger.Logger, src, dest string,
) (*RunResultDetails, error) {
	scpCtx, cancel := context.WithTimeout(ctx, scpTimeout)
	defer cancel()

	return runWithMaybeRetry(scpCtx, l, DefaultRetryOpt, defaultSCPShouldRetryFn,
		func(ctx context.Context) (*RunResultDetails, error) { return scp(ctx, l, src, dest) })
}

// Host returns the public IP of a node.
func (c *SyncedCluster) Host(n Node) string {
	return c.VMs[n-1].PublicIP
}

func (c *SyncedCluster) user(n Node) string {
	return c.VMs[n-1].RemoteUser
}

func (c *SyncedCluster) locality(n Node) string {
	return c.Localities[n-1]
}

// IsLocal returns true if this is a local cluster (see vm/local).
func (c *SyncedCluster) IsLocal() bool {
	return config.IsLocalClusterName(c.Name)
}

func (c *SyncedCluster) localVMDir(n Node) string {
	return local.VMDir(c.Name, int(n))
}

// TargetNodes is the fully expanded, ordered list of nodes that any given
// roachprod command is intending to target.
//
//	$ roachprod create local -n 4
//	$ roachprod start local          # [1, 2, 3, 4]
//	$ roachprod start local:2-4      # [2, 3, 4]
//	$ roachprod start local:2,1,4    # [1, 2, 4]
func (c *SyncedCluster) TargetNodes() Nodes {
	return append(Nodes{}, c.Nodes...)
}

// GetInternalIP returns the internal IP address of the specified node.
func (c *SyncedCluster) GetInternalIP(n Node) (string, error) {
	if c.IsLocal() {
		return c.Host(n), nil
	}

	ip := c.VMs[n-1].PrivateIP
	if ip == "" {
		return "", errors.Errorf("no private IP for node %d", n)
	}
	return ip, nil
}

// roachprodEnvValue returns the value of the ROACHPROD environment variable
// that is set when starting a process. This value is used to recognize the
// correct process, when monitoring or stopping.
//
// Normally, the value is of the form:
//
//	[<local-cluster-name>/]<node-id>[/tag]
//
// Examples:
//
//   - non-local cluster without tags:
//     ROACHPROD=1
//
//   - non-local cluster with tag foo:
//     ROACHPROD=1/foo
//
//   - non-local cluster with hierarchical tag foo/bar:
//     ROACHPROD=1/foo/bar
//
//   - local cluster:
//     ROACHPROD=local-foo/1
//
//   - local cluster with tag bar:
//     ROACHPROD=local-foo/1/bar
func (c *SyncedCluster) roachprodEnvValue(node Node) string {
	var parts []string
	if c.IsLocal() {
		parts = append(parts, c.Name)
	}
	parts = append(parts, fmt.Sprintf("%d", node))
	if c.Tag != "" {
		parts = append(parts, c.Tag)
	}
	return strings.Join(parts, "/")
}

func envVarRegex(name, value string) string {
	escaped := strings.ReplaceAll(value, "/", "\\/")
	// We look for either a trailing space or a slash (in which case, we
	// tolerate any remaining tag suffix). The env var may also be the
	// last environment variable declared, so we also account for that.
	return fmt.Sprintf(`(%[1]s=%[2]s$|%[1]s=%[2]s[ \/])`, name, escaped)
}

// roachprodEnvRegex returns a regexp that matches the ROACHPROD value for the
// given node.
func (c *SyncedCluster) roachprodEnvRegex(node Node) string {
	return envVarRegex("ROACHPROD", c.roachprodEnvValue(node))
}

// validateHostnameCmd wraps the command given with a check that the
// remote node is still part of the `SyncedCluster`. When `cmd` is
// empty, the command returned can be used to validate whether the
// host matches the name expected by roachprod, and can be used to
// validate the contents of the cache for that cluster.
//
// Since `SyncedCluster` is created from a potentially stale cache, it
// is possible for the following events to happen:
//
// Client 1:
//   - cluster A is created and information is persisted in roachprod's cache.
//   - cluster's A lifetime expires, VMs are destroyed.
//
// Client 2
//   - cluster B is created; public IP of one of the VMs of cluster
//     A is reused and assigned to one of cluster B's VMs.
//
// Client 1:
//   - command with side-effects is run on cluster A (e.g.,
//     `roachprod stop`). Public IP now belongs to a VM in cluster
//     B. Client unknowingly disturbs cluster B, thinking it's cluster A.
//
// Client 2:
//   - notices that cluster B is behaving strangely (e.g., cockroach
//     process died). A lot of time is spent trying to debug the
//     issue.
//
// This scenario is possible and has happened a number of times in the
// past (for one such occurrence, see #97100). A particularly
// problematic instance of this bug happens when "cluster B" is
// running a roachtest. This interaction leads to issues that are hard
// to debug or make sense of, which ultimately wastes a lot of
// engineering time.
//
// By wrapping every command with a hostname check as is done here, we
// ensure that the cached cluster information is still correct.
func (c *SyncedCluster) validateHostnameCmd(cmd string, node Node) string {
	isValidHost := fmt.Sprintf("[[ `hostname` == '%s' ]]", vm.Name(c.Name, int(node)))
	errMsg := fmt.Sprintf("expected host to be part of %s, but is `hostname`", c.Name)
	elseBranch := "fi"
	if cmd != "" {
		elseBranch = fmt.Sprintf(`
else
    %s
fi
`, cmd)
	}

	return fmt.Sprintf(`
if ! %s; then
    echo "%s"
    exit 1
%s
`, isValidHost, errMsg, elseBranch)
}

// validateHost will run `validateHostnameCmd` on the node(s) passed to
// make sure it still belongs to the SyncedCluster. Returns an error
// when the hostnames don't match, indicating that the roachprod cache
// is stale.
func (c *SyncedCluster) validateHost(ctx context.Context, l *logger.Logger, nodes Nodes) error {
	if c.IsLocal() {
		return nil
	}

	// Retry on different nodes, in case some of the VMs are unreachable.
	// While this does indicate something is likely wrong, we don't want to
	// fail here as some callers want to tolerate this, e.g. fetching logs
	// after a test failure shouldn't fail just because one VM is down.
	var combinedErr error
	retryOpts := *DefaultRetryOpt
	retryOpts.MaxRetries = 4
	r := retry.StartWithCtx(ctx, retryOpts)
	for nodeIdx := 0; r.Next(); nodeIdx = (nodeIdx + 1) % len(nodes) {
		node := nodes[nodeIdx]
		cmd := c.validateHostnameCmd("", node)

		err := c.Run(ctx, l, l.Stdout, l.Stderr, WithNodes(Nodes{node}).WithRetryDisabled(), "validate-ssh-host", cmd)
		if err != nil {
			if !rperrors.IsTransient(err) {
				return err
			}
			combinedErr = errors.CombineErrors(combinedErr, err)
			continue
		}
		return nil
	}
	return combinedErr
}

// cmdDebugName is the suffix of the generated ssh debug file
// If it is "", a suffix will be generated from the cmd string
func (c *SyncedCluster) newSession(
	l *logger.Logger, node Node, cmd string, options ...remoteSessionOption,
) session {
	if c.sessionProvider != nil {
		return c.sessionProvider(node, cmd)
	}
	if c.IsLocal() {
		return newLocalSession(cmd)
	}
	command := &remoteCommand{
		node: node,
		user: c.user(node),
		host: c.Host(node),
		cmd:  c.validateHostnameCmd(cmd, node),
	}

	for _, opt := range options {
		opt(command)
	}
	return newRemoteSession(l, command)
}

// Stop is used to stop processes or virtual clusters.
//
// It sends a signal to all processes that have been started with
// ROACHPROD env var and optionally waits until the processes stop. If
// the virtualClusterLabel is not empty, then only the corresponding
// virtual cluster is stopped (stopping the corresponding sql server
// process for separate process deployments, or stopping the service
// for shared-process configurations.)
//
// When Stop needs to kill a process without other flags, the signal
// is 9 (SIGKILL) and wait is true. If gracePeriod is non-zero, Stop
// stops waiting after that approximate number of seconds, sending a
// SIGKILL if the process is still running after that time.
func (c *SyncedCluster) Stop(
	ctx context.Context,
	l *logger.Logger,
	sig int,
	wait bool,
	gracePeriod int,
	virtualClusterLabel string,
) error {
	// virtualClusterDisplay includes information about the virtual
	// cluster associated with OS processes being stopped in this
	// function.
	var virtualClusterDisplay string
	// virtualClusterName is the virtualClusterName associated with the
	// label passed, if any.
	var virtualClusterName string
	// killProcesses indicates whether processed need to be stopped.
	killProcesses := true

	if virtualClusterLabel != "" {
		name, sqlInstance, err := VirtualClusterInfoFromLabel(virtualClusterLabel)
		if err != nil {
			return err
		}

		services, err := c.DiscoverServices(ctx, name, ServiceTypeSQL)
		if err != nil {
			return err
		}

		if len(services) == 0 {
			return fmt.Errorf("no service for virtual cluster %q", virtualClusterName)
		}

		virtualClusterName = name
		if services[0].ServiceMode == ServiceModeShared {
			// For shared process virtual clusters, we just stop the service
			// via SQL.
			killProcesses = false
		} else {
			virtualClusterDisplay = fmt.Sprintf(" virtual cluster %q, instance %d", virtualClusterName, sqlInstance)
		}

	}

	if killProcesses {
		display := fmt.Sprintf("%s: stopping%s", c.Name, virtualClusterDisplay)
		if wait {
			display += " and waiting"
		}
		return c.kill(ctx, l, "stop", display, sig, wait, gracePeriod, virtualClusterLabel)
	} else {
		cmd := fmt.Sprintf("ALTER TENANT '%s' STOP SERVICE", virtualClusterName)
		res, err := c.ExecSQL(ctx, l, c.Nodes[:1], "", 0, DefaultAuthMode(), "", /* database */
			[]string{"-e", cmd})
		if err != nil || res[0].Err != nil {
			if len(res) > 0 {
				return errors.CombineErrors(err, res[0].Err)
			}

			return err
		}
	}

	return nil
}

// Signal sends a signal to the CockroachDB process.
func (c *SyncedCluster) Signal(ctx context.Context, l *logger.Logger, sig int) error {
	display := fmt.Sprintf("%s: sending signal %d", c.Name, sig)
	return c.kill(ctx, l, "signal", display, sig, false /* wait */, 0 /* gracePeriod */, "")
}

// kill sends the signal sig to all nodes in the cluster using the kill command.
// cmdName and display specify the roachprod subcommand and a status message,
// for output/logging. If wait is true, the command will wait for the processes
// to exit, up to gracePeriod seconds.
func (c *SyncedCluster) kill(
	ctx context.Context,
	l *logger.Logger,
	cmdName, display string,
	sig int,
	wait bool,
	gracePeriod int,
	virtualClusterLabel string,
) error {
	const timedOutMessage = "timed out"

	if sig == int(unix.SIGKILL) {
		// `kill -9` without wait is never what a caller wants. See #77334.
		wait = true
	}
	return c.Parallel(ctx, l, WithNodes(c.Nodes).WithDisplay(display).WithRetryDisabled(),
		func(ctx context.Context, node Node) (*RunResultDetails, error) {
			var waitCmd string
			if wait {
				waitCmd = fmt.Sprintf(`
  for pid in ${pids}; do
    echo "${pid}: checking" >> %[1]s/roachprod.log
    waitcnt=0
    while kill -0 ${pid}; do
      if [ %[2]d -gt 0 -a $waitcnt -gt %[2]d ]; then
         echo "${pid}: max %[2]d attempts reached, aborting wait" >>%[1]s/roachprod.log
         echo "%[3]s"
         break
      fi
      kill -0 ${pid} >> %[1]s/roachprod.log 2>&1
      echo "${pid}: still alive [$?]" >> %[1]s/roachprod.log
      ps axeww -o pid -o command >> %[1]s/roachprod.log
      sleep 1
      waitcnt=$(expr $waitcnt + 1)
    done
    echo "${pid}: dead" >> %[1]s/roachprod.log
  done`,
					c.LogDir(node, "", 0), // [1]
					gracePeriod,           // [2]
					timedOutMessage,       // [3]
				)
			}

			var virtualClusterFilter string
			if virtualClusterLabel != "" {
				virtualClusterFilter = fmt.Sprintf(
					"grep -E '%s' |",
					envVarRegex("ROACHPROD_VIRTUAL_CLUSTER", virtualClusterLabel),
				)
			}

			// NB: the awkward-looking `awk` invocation serves to avoid having the
			// awk process match its own output from `ps`.
			cmd := fmt.Sprintf(`
mkdir -p %[1]s
mkdir -p %[2]s
echo ">>> roachprod %[1]s: $(date)" >> %[2]s/roachprod.log
ps axeww -o pid -o command >> %[2]s/roachprod.log
pids=$(ps axeww -o pid -o command | \
  %[3]s \
  sed 's/export ROACHPROD=//g' | \
  awk '/%[4]s/ { print $1 }')
if [ -n "${pids}" ]; then
  kill -%[5]d ${pids}
%[6]s
fi`,
				cmdName,                   // [1]
				c.LogDir(node, "", 0),     // [2]
				virtualClusterFilter,      // [3]
				c.roachprodEnvRegex(node), // [4]
				sig,                       // [5]
				waitCmd,                   // [6]
			)

			res, err := c.runCmdOnSingleNode(ctx, l, node, cmd, defaultCmdOpts("kill"))
			if err != nil {
				return res, err
			}

			// If the process has not terminated after the grace period,
			// perform a forceful termination.
			if wait && sig != int(unix.SIGKILL) && strings.Contains(res.CombinedOut, timedOutMessage) {
				l.Printf("n%d did not shutdown after %ds, performing a SIGKILL", node, gracePeriod)
				return res, errors.Wrapf(
					c.kill(ctx, l, cmdName, display, int(unix.SIGKILL), true, 0, virtualClusterLabel),
					"failed to forcefully terminate n%d", node,
				)
			}

			return res, nil
		})
}

// Wipe TODO(peter): document
func (c *SyncedCluster) Wipe(ctx context.Context, l *logger.Logger, preserveCerts bool) error {
	display := fmt.Sprintf("%s: wiping", c.Name)
	if err := c.Stop(ctx, l, int(unix.SIGKILL), true /* wait */, 0 /* gracePeriod */, ""); err != nil {
		return err
	}
	err := c.Parallel(ctx, l, WithNodes(c.Nodes).WithDisplay(display), func(ctx context.Context, node Node) (*RunResultDetails, error) {
		var cmd string
		if c.IsLocal() {
			// Not all shells like brace expansion, so we'll do it here
			paths := []string{
				"data*",
				"logs*",
				"cockroach-*.sh",
			}
			if !preserveCerts {
				paths = append(paths, fmt.Sprintf("%s*", CockroachNodeCertsDir))
				paths = append(paths, fmt.Sprintf("%s*", CockroachNodeTenantCertsDir))
			}
			for _, dir := range paths {
				cmd += fmt.Sprintf(`rm -fr %s/%s ;`, c.localVMDir(node), dir)
			}
		} else {
			rmCmds := []string{
				fmt.Sprintf(`sudo find /mnt/data* -maxdepth 1 -type f -not -name %s -exec rm -f {} \;`, vm.InitializedFile),
				`sudo rm -fr /mnt/data*/{auxiliary,local,tmp,cassandra,cockroach,cockroach-temp*,mongo-data}`,
				`sudo rm -fr cockroach-*.sh`,
				`sudo rm -fr logs* data*`,
			}
			if !preserveCerts {
				rmCmds = append(rmCmds,
					fmt.Sprintf("sudo rm -fr %s*", CockroachNodeCertsDir),
					fmt.Sprintf("sudo rm -fr %s*", CockroachNodeTenantCertsDir),
				)
			}

			cmd = strings.Join(rmCmds, " && ")
		}
		return c.runCmdOnSingleNode(ctx, l, node, cmd, defaultCmdOpts("wipe"))
	})
	if err != nil {
		return err
	}

	err = c.Cluster.DeletePrometheusConfig(ctx, l)
	if err != nil {
		l.Printf("WARNING: failed to delete the prometheus config (already wiped?): %s", err)
	}

	return nil
}

// NodeStatus contains details about the status of a node.
type NodeStatus struct {
	NodeID  int
	Running bool
	Version string
	Pid     string
	Err     error
}

// Status TODO(peter): document
func (c *SyncedCluster) Status(ctx context.Context, l *logger.Logger) ([]NodeStatus, error) {
	display := fmt.Sprintf("%s: status", c.Name)
	res, _, err := c.ParallelE(ctx, l, WithNodes(c.Nodes).WithDisplay(display), func(ctx context.Context, node Node) (*RunResultDetails, error) {
		binary := cockroachNodeBinary(c, node)
		cmd := fmt.Sprintf(`out=$(ps axeww -o pid -o ucomm -o command | \
  sed 's/export ROACHPROD=//g' | \
  awk '/%s/ {print $2, $1}'`,
			c.roachprodEnvRegex(node))
		cmd += ` | sort | uniq);
vers=$(` + binary + ` version 2>/dev/null | awk '/Build Tag:/ {print $NF}')
if [ -n "${out}" -a -n "${vers}" ]; then
  echo ${out} | sed "s/cockroach/cockroach-${vers}/g"
elif [ -n "${vers}" ]; then
  echo "not-running cockroach-${vers}"
else
  echo ${out}
fi
`
		return c.runCmdOnSingleNode(ctx, l, node, cmd, defaultCmdOpts("status"))
	})

	if err != nil {
		return nil, err
	}

	statuses := make([]NodeStatus, len(c.Nodes))
	for i, res := range res {
		msg := strings.TrimSpace(res.CombinedOut)
		if msg == "" || strings.HasPrefix(msg, "not-running") {
			statuses[i] = NodeStatus{NodeID: int(res.Node), Running: false}
			if msg != "" {
				info := strings.Split(msg, " ")
				statuses[i].Version = info[1]
			}
			continue
		}
		info := strings.Split(msg, " ")
		statuses[i] = NodeStatus{NodeID: int(res.Node), Running: true, Version: info[0], Pid: info[1]}
	}
	return statuses, nil
}

// RunResultDetails holds details of the result of commands executed by Run().
type RunResultDetails struct {
	Node             Node
	Stdout           string
	Stderr           string
	CombinedOut      string
	Err              error
	RemoteExitStatus int
	Attempt          int
}

func newRunResultDetails(node Node, err error) *RunResultDetails {
	res := RunResultDetails{
		Node: node,
		Err:  err,
	}
	if exitCode, success := rperrors.GetExitCode(err); success {
		res.RemoteExitStatus = exitCode
	}

	return &res
}

// Output prints either the combined, or separated stdout and stderr command output
func (r *RunResultDetails) Output(decorate bool) string {
	var builder strings.Builder
	outputExists := false
	writeVal := func(label string, value string) {
		s := strings.TrimSpace(value)
		if builder.Len() > 0 {
			builder.WriteByte('\n')
		}

		if s == "" {
			if decorate {
				builder.WriteString(label)
				builder.WriteString(": <empty>")
			}
			return
		}

		if label != "" && decorate {
			builder.WriteString(label)
			builder.WriteString(":")
		}
		builder.WriteString(s)
		builder.WriteString("\n")
		outputExists = true
	}

	writeVal("stdout", r.Stdout)
	writeVal("stderr", r.Stderr)

	// Only if stderr and stdout are empty do we check the combined output.
	if !outputExists {
		builder.Reset()
		writeVal("", r.CombinedOut)
	}

	if !outputExists {
		return ""
	}
	return builder.String()
}

// RunCmdOptions is used to configure the behavior of `runCmdOnSingleNode`
type RunCmdOptions struct {
	combinedOut             bool
	includeRoachprodEnvVars bool
	// If true, logs the expanded result if it differs from the original command.
	logExpandedCmd bool
	stdin          io.Reader
	stdout, stderr io.Writer
	remoteOptions  []remoteSessionOption
	expanderConfig ExpanderConfig
}

// Default RunCmdOptions enable combining output (stdout and stderr) and capturing ssh (verbose) debug output.
func defaultCmdOpts(debugName string) RunCmdOptions {
	return RunCmdOptions{
		combinedOut:   true,
		remoteOptions: []remoteSessionOption{withDebugName(debugName)},
	}
}

// Unlike defaultCmdOpts, ssh (verbose) debug output capture is _disabled_.
func cmdOptsWithDebugDisabled() RunCmdOptions {
	return RunCmdOptions{
		combinedOut:   true,
		remoteOptions: []remoteSessionOption{withDebugDisabled()},
	}
}

// runCmdOnSingleNode is a common entry point for all commands that run on a single node,
// including user commands from roachtests and roachprod commands.
// The `opts` struct is used to configure the behavior of the command, including
// - whether stdout and stderr or combined output is desired
// - specifying the stdin, stdout, and stderr streams
// - specifying the remote session options
// - whether the command should be run with the ROACHPROD env variable (true for all user commands)
//
// NOTE: do *not* return a `nil` `*RunResultDetails` in this function:
// we want to support callers being able to use
// `errors.CombineErrors(err, res.Err)` when they don't care about the
// origin of the error.
func (c *SyncedCluster) runCmdOnSingleNode(
	ctx context.Context, l *logger.Logger, node Node, cmd string, opts RunCmdOptions,
) (*RunResultDetails, error) {
	var noResult RunResultDetails
	// Argument template expansion is node specific (e.g. for {store-dir}).
	e := expander{node: node}
	expandedCmd, err := e.expand(ctx, l, c, opts.expanderConfig, cmd)
	if err != nil {
		return &noResult, errors.WithDetailf(err, "error expanding command: %s", cmd)
	}
	if opts.logExpandedCmd && expandedCmd != cmd {
		l.Printf("Node %d expanded cmd: %s", e.node, expandedCmd)
	}

	nodeCmd := expandedCmd
	// Be careful about changing these command strings. In particular, we need
	// to support running commands in the background on both local and remote
	// nodes. For example:
	//
	//   roachprod run cluster -- "sleep 60 &> /dev/null < /dev/null &"
	//
	// That command should return immediately. And a "roachprod status" should
	// reveal that the sleep command is running on the cluster.
	if opts.includeRoachprodEnvVars {
		envVars := append([]string{
			fmt.Sprintf("ROACHPROD=%s", c.roachprodEnvValue(node)), "GOTRACEBACK=crash",
		}, config.DefaultEnvVars()...)
		nodeCmd = fmt.Sprintf(`export %s && bash -c %s`,
			strings.Join(envVars, " "), ssh.Escape1(expandedCmd))
		if c.IsLocal() {
			nodeCmd = fmt.Sprintf("cd %s; %s", c.localVMDir(node), nodeCmd)
		}
	}

	// This default can be overridden by the caller, and is hence specified first
	sessionOpts := []remoteSessionOption{withDebugName(GenFilenameFromArgs(20, expandedCmd))}
	sess := c.newSession(l, node, nodeCmd, append(sessionOpts, opts.remoteOptions...)...)
	defer sess.Close()

	if opts.stdin != nil {
		sess.SetStdin(opts.stdin)
	}
	if opts.stdout == nil {
		opts.stdout = io.Discard
	}
	if opts.stderr == nil {
		opts.stderr = io.Discard
	}

	var res *RunResultDetails
	if opts.combinedOut {
		out, cmdErr := sess.CombinedOutput(ctx)
		res = newRunResultDetails(node, cmdErr)
		res.CombinedOut = string(out)
	} else {
		// We stream the output if running on a single node.
		var stdoutBuffer, stderrBuffer bytes.Buffer

		multStdout := io.MultiWriter(&stdoutBuffer, opts.stdout)
		multStderr := io.MultiWriter(&stderrBuffer, opts.stderr)
		sess.SetStdout(multStdout)
		sess.SetStderr(multStderr)

		res = newRunResultDetails(node, sess.Run(ctx))
		res.Stderr = stderrBuffer.String()
		res.Stdout = stdoutBuffer.String()
	}

	if res.Err != nil {
		output := res.Output(true)
		// Somewhat arbitrary limit to give us a chance to see some of the output
		// in the failure_*.log, since the full output is in the run_*.log.
		oLen := len(output)
		if oLen > 1024 {
			output = "<truncated> ... " + output[oLen-1024:oLen-1]
		}

		if output == "" {
			output = "<no output>"
		}
		detailMsg := fmt.Sprintf("Node %d. Command with error:\n```\n%s\n```\n%s", node, cmd, output)
		res.Err = errors.WithDetail(res.Err, detailMsg)
	}
	return res, nil
}

// Run a command on >= 1 node in the cluster.
//
// When running on just one node, the command output is streamed to stdout.
// When running on multiple nodes, the commands run in parallel, their output
// is cached and then emitted all together once all commands are completed.
//
// stdout: Where stdout messages are written
// stderr: Where stderr messages are written
// nodes: The cluster nodes where the command will be run.
// title: A description of the command being run that is output to the logs.
// cmd: The command to run.
func (c *SyncedCluster) Run(
	ctx context.Context,
	l *logger.Logger,
	stdout, stderr io.Writer,
	options RunOptions,
	title, cmd string,
) error {
	// Stream output if we're running the command on only 1 node.
	stream := len(options.Nodes) == 1
	// If the user has not specified a display string, and we are not streaming,
	// we set a default display string.
	if !stream && options.Display == "" {
		options = options.WithDisplay(fmt.Sprintf("%s:%v: %s", c.Name, options.Nodes, title))
	}
	results, _, err := c.ParallelE(ctx, l, options, func(ctx context.Context, node Node) (*RunResultDetails, error) {
		opts := RunCmdOptions{
			combinedOut:             !stream,
			includeRoachprodEnvVars: true,
			stdout:                  stdout,
			stderr:                  stderr,
			expanderConfig:          options.ExpanderConfig,
			logExpandedCmd:          options.LogExpandedCmd,
		}
		result, err := c.runCmdOnSingleNode(ctx, l, node, cmd, opts)
		return result, err
	})

	if err != nil {
		return err
	}

	return processResults(results, stream, stdout)
}

// processResults returns the error from the RunResultDetails with the highest RemoteExitStatus
func processResults(results []*RunResultDetails, stream bool, stdout io.Writer) error {

	// Easier to read output when we indent each line of the output. If an error is
	// present, we also include the error message at the top.
	format := func(s string, e error) string {
		s = strings.ReplaceAll(s, "\n", "\n\t")
		if e != nil {
			return fmt.Sprintf("\t<err> %v\n\t%s", e, s)
		}

		if s == "" {
			return "\t<ok>"
		}
		return fmt.Sprintf("\t<ok>\n\t%s", s)
	}

	var resultWithError *RunResultDetails
	for i, r := range results {
		// We no longer wait for all nodes to complete before returning in the case of an error (#100403)
		// which means that some node results may be nil.
		if r == nil {
			continue
		}

		// Emit the cached output of each result. When stream == true, the output is emitted
		// as it is generated in `runCmdOnSingleNode`.
		if !stream {
			fmt.Fprintf(stdout, "  %2d: %s\n", i+1, format(r.Output(true), r.Err))
		}

		if r.Err != nil {
			if resultWithError == nil {
				resultWithError = r
				continue
			}

			if r.RemoteExitStatus > resultWithError.RemoteExitStatus {
				resultWithError = r
			}
		}
	}

	if resultWithError != nil {
		return resultWithError.Err
	}
	return nil
}

// RunWithDetails runs a command on the specified nodes and returns results
// details and an error. By default, this will wait for all commands to complete
// before returning unless encountering a roachprod error, unless one of the
// FailOptions has been explicitly set in the RunOptions.
func (c *SyncedCluster) RunWithDetails(
	ctx context.Context, l *logger.Logger, options RunOptions, title, cmd string,
) ([]RunResultDetails, error) {
	// If the user has not specified a display string, and we are not streaming,
	// we set a default display string.
	if options.Display == "" {
		options = options.WithDisplay(fmt.Sprintf("%s:%v: %s", c.Name, options.Nodes, title))
	}
	if options.FailOption == FailDefault {
		options = options.WithFailSlow()
	}

	// Failing slow here allows us to capture the output of all nodes even if one fails with a command error.
	resultPtrs, _, err := c.ParallelE(ctx, l, options, func(ctx context.Context, node Node) (*RunResultDetails, error) {
		opts := RunCmdOptions{
			includeRoachprodEnvVars: true,
			stdout:                  l.Stdout,
			stderr:                  l.Stderr,
			expanderConfig:          options.ExpanderConfig,
			logExpandedCmd:          options.LogExpandedCmd,
		}
		result, err := c.runCmdOnSingleNode(ctx, l, node, cmd, opts)
		return result, err
	})

	if err != nil {
		return nil, err
	}

	// Return values to preserve API
	results := make([]RunResultDetails, len(options.Nodes))
	for i, v := range resultPtrs {
		if v != nil {
			results[i] = *v
		}
	}
	return results, nil
}

// Wait TODO(peter): document
func (c *SyncedCluster) Wait(ctx context.Context, l *logger.Logger) error {
	display := fmt.Sprintf("%s: waiting for nodes to start", c.Name)
	results, hasError, err := c.ParallelE(ctx, l, WithNodes(c.Nodes).WithDisplay(display).WithRetryDisabled(),
		func(ctx context.Context, node Node) (*RunResultDetails, error) {
			res := &RunResultDetails{Node: node}
			var err error
			cmd := fmt.Sprintf("test -e %s -a -e %s", vm.DisksInitializedFile, vm.OSInitializedFile)
			// N.B. we disable ssh debug output capture, lest we end up with _thousands_ of useless .log files.
			opts := cmdOptsWithDebugDisabled()
			for j := 0; j < 600; j++ {
				res, err = c.runCmdOnSingleNode(ctx, l, node, cmd, opts)
				if err != nil {
					return nil, err
				}

				if res.Err != nil {
					time.Sleep(500 * time.Millisecond)
					continue
				}
				return res, nil
			}
			res.Err = errors.Wrapf(res.Err, "timed out after 5m")
			logContent, err := c.runCmdOnSingleNode(ctx, nil, node, fmt.Sprintf("tail -n %d %s", 20, vm.StartupLogs), cmdOptsWithDebugDisabled())
			if err = errors.CombineErrors(err, logContent.Err); err != nil {
				l.Printf("could not fetch startup logs: %v", err)
			} else {
				l.Printf("  %2d: startup failed, last 20 lines of output:\n%s", node, logContent.CombinedOut)
				l.Printf("  %2d: view the full log in %s", node, vm.StartupLogs)
			}
			l.Printf("  %2d: %v", node, res.Err)
			return res, nil
		})

	if err != nil {
		return err
	}

	if hasError {
		errs := []error{}
		for _, res := range results {
			if res != nil && res.Err != nil {
				errs = append(errs, res.Err)
			}
		}
		return errors.Wrap(errors.Join(errs...), "not all nodes booted successfully")
	}
	return nil
}

// SetupSSH configures the cluster for use with SSH. This is generally run after
// the cloud.Cluster has been synced which resets the SSH credentials on the
// machines and sets them up for the current user. This method enables the
// hosts to talk to each other and optionally configures additional keys to be
// added to the hosts via the c.AuthorizedKeys field. It does so in the following
// steps:
//
//  1. Creates an ssh key pair on the first host to be used on all hosts if
//     none exists.
//  2. Distributes the public key, private key, and authorized_keys file from
//     the first host to the others.
//  3. Merges the data in c.AuthorizedKeys with the existing authorized_keys
//     files on all hosts.
//
// This call strives to be idempotent.
func (c *SyncedCluster) SetupSSH(ctx context.Context, l *logger.Logger) error {
	if c.IsLocal() {
		return nil
	}

	if len(c.Nodes) == 0 || len(c.VMs) == 0 {
		return fmt.Errorf("%s: invalid cluster: nodes=%d hosts=%d",
			c.Name, len(c.Nodes), len(c.VMs))
	}

	// Generate an ssh key that we'll distribute to all the nodes in the
	// cluster in order to allow inter-node ssh.
	results, _, err := c.ParallelE(ctx, l, WithNodes(c.Nodes[0:1]).WithDisplay("generating ssh key"),
		func(ctx context.Context, n Node) (*RunResultDetails, error) {
			// Create the ssh key and then tar up the public, private and
			// authorized_keys files and output them to stdout. We'll take this output
			// and pipe it back into tar on the other nodes in the cluster.
			cmd := `
test -f .ssh/id_rsa || \
  (ssh-keygen -q -f .ssh/id_rsa -t rsa -N '' && \
   cat .ssh/id_rsa.pub >> .ssh/authorized_keys);
tar cf - .ssh/id_rsa .ssh/id_rsa.pub .ssh/authorized_keys
`
			runOpts := defaultCmdOpts("ssh-gen-key")
			runOpts.combinedOut = false
			return c.runCmdOnSingleNode(ctx, l, n, cmd, runOpts)
		})

	if err != nil {
		return err
	}

	sshTar := []byte(results[0].Stdout)
	// Skip the first node which is where we generated the key.
	nodes := c.Nodes[1:]
	if err := c.Parallel(ctx, l, WithNodes(nodes).WithDisplay("distributing ssh key"),
		func(ctx context.Context, node Node) (*RunResultDetails, error) {
			runOpts := defaultCmdOpts("ssh-dist-key")
			runOpts.stdin = bytes.NewReader(sshTar)
			return c.runCmdOnSingleNode(ctx, l, node, `tar xf -`, runOpts)
		}); err != nil {
		return err
	}

	// Populate the known_hosts file with both internal and external IPs of all
	// nodes in the cluster. Internal IPs are populated within its provider peers
	// only, and external IPs are populated for all providers. Note that as a side
	// effect, this creates the known hosts file in unhashed format, working
	// around a limitation of jsch (which is used in jepsen tests).

	mu := syncutil.Mutex{}
	type nodeInfo struct {
		node Node
		ip   string
	}
	// Build a list of internal IPs for each provider and
	// public IPs for all nodes.
	providerPrivateIPs := make(map[string][]nodeInfo)
	publicIPs := make([]string, 0, len(c.Nodes))
	for _, node := range c.Nodes {
		v := c.VMs[node-1]
		providerPrivateIPs[v.Provider] = append(providerPrivateIPs[v.Provider], nodeInfo{node: node, ip: v.PrivateIP})
		publicIPs = append(publicIPs, c.Host(node))
	}

	providerKnownHostData := make(map[string][]byte)
	providers := maps.Keys(providerPrivateIPs)

	// Only need to scan on the first node of each provider.
	firstNodes := make([]Node, len(providers))
	for i, provider := range providers {
		firstNodes[i] = providerPrivateIPs[provider][0].node
	}
	if err := c.Parallel(ctx, l, WithNodes(firstNodes).WithDisplay("scanning hosts"),
		func(ctx context.Context, node Node) (*RunResultDetails, error) {
			// Scan a combination of all remote IPs and local IPs pertaining to this
			// node's cloud provider.
			scanIPs := append([]string{}, publicIPs...)
			nodeProvider := c.VMs[node-1].Provider
			for _, nodeInfo := range providerPrivateIPs[nodeProvider] {
				scanIPs = append(scanIPs, nodeInfo.ip)
			}

			// ssh-keyscan may return fewer than the desired number of entries if the
			// remote nodes are not responding yet, so we loop until we have a scan that
			// found host keys for all the public IPs. Merge the newly scanned keys
			// with the existing list to make this process idempotent.
			cmd := `
set -e
tmp="$(tempfile -d ~/.ssh -p 'roachprod' )"
on_exit() {
    rm -f "${tmp}"
}
trap on_exit EXIT
for i in {1..20}; do
  ssh-keyscan -T 60 -t rsa ` + strings.Join(scanIPs, " ") + ` > "${tmp}"
  if [[ "$(wc < ${tmp} -l)" -eq "` + fmt.Sprint(len(scanIPs)) + `" ]]; then
    [[ -f .ssh/known_hosts ]] && cat .ssh/known_hosts >> "${tmp}"
    sort -u < "${tmp}"
    exit 0
  fi
  sleep 1
done
exit 1
`
			runOpts := defaultCmdOpts("ssh-scan-hosts")
			runOpts.combinedOut = false
			res, err := c.runCmdOnSingleNode(ctx, l, node, cmd, runOpts)
			if err != nil {
				return nil, err
			}

			if res.Err != nil {
				return res, nil
			}
			mu.Lock()
			defer mu.Unlock()
			providerKnownHostData[nodeProvider] = []byte(res.Stdout)
			return res, nil
		}); err != nil {
		return err
	}

	if err := c.Parallel(ctx, l, WithNodes(c.Nodes).WithDisplay("distributing known_hosts"),
		func(ctx context.Context, node Node) (*RunResultDetails, error) {
			provider := c.VMs[node-1].Provider
			const cmd = `
known_hosts_data="$(cat)"
set -e
tmp="$(tempfile -p 'roachprod' -m 0644 )"
on_exit() {
    rm -f "${tmp}"
}
trap on_exit EXIT
echo "${known_hosts_data}" > "${tmp}"
cat "${tmp}" >> ~/.ssh/known_hosts
# If our bootstrapping user is not the shared user install all of the
# relevant ssh files from the bootstrapping user into the shared user's
# .ssh directory.
if [[ "$(whoami)" != "` + config.SharedUser + `" ]]; then
    # Ensure that the shared user has a .ssh directory
    sudo -u ` + config.SharedUser +
				` bash -c "mkdir -p ~` + config.SharedUser + `/.ssh"
    # This somewhat absurd incantation ensures that we properly shell quote
    # filenames so that they both aren't expanded and work even if the filenames
    # include spaces.
    sudo find ~/.ssh -type f -execdir bash -c 'install \
        --owner ` + config.SharedUser + ` \
        --group ` + config.SharedUser + ` \
        --mode $(stat -c "%a" '"'"'{}'"'"') \
        '"'"'{}'"'"' ~` + config.SharedUser + `/.ssh' \;
fi
`
			runOpts := defaultCmdOpts("ssh-dist-known-hosts")
			runOpts.stdin = bytes.NewReader(providerKnownHostData[provider])
			return c.runCmdOnSingleNode(ctx, l, node, cmd, runOpts)
		}); err != nil {
		return err
	}

	if len(c.AuthorizedKeys) > 0 {
		// When clusters are created using cloud APIs they only have a
		// subset of desired keys installed on a subset of users. This
		// code distributes additional authorized_keys the current user
		// (i.e., the shared user).
		if err := c.Parallel(ctx, l, WithNodes(c.Nodes).WithDisplay("adding additional authorized keys"),
			func(ctx context.Context, node Node) (*RunResultDetails, error) {
				const cmd = `
keys_data="$(cat)"
set -e
tmp1="$(tempfile -d ~/.ssh -p 'roachprod' )"
tmp2="$(tempfile -d ~/.ssh -p 'roachprod' )"
on_exit() {
    rm -f "${tmp1}" "${tmp2}"
}
trap on_exit EXIT
cat ~/.ssh/authorized_keys > "${tmp1}"
echo "${keys_data}" >> "${tmp1}"
sort -u < "${tmp1}" > "${tmp2}"
install --mode 0600 "${tmp2}" ~/.ssh/authorized_keys
`
				runOpts := defaultCmdOpts("ssh-add-extra-keys")
				runOpts.stdin = bytes.NewReader(c.AuthorizedKeys)
				return c.runCmdOnSingleNode(ctx, l, node, cmd, runOpts)
			}); err != nil {
			return err
		}
	}

	return nil
}

const (
	// CockroachNodeCertsDir is the certs directory that lives
	// on the cockroach node itself.
	CockroachNodeCertsDir       = "certs"
	CockroachNodeTenantCertsDir = "tenant-certs"
	certsTarName                = "certs.tar"
	tenantCertFile              = "client-tenant.%d.crt"
)

func tenantCertsTarName(virtualClusterID int) string {
	return fmt.Sprintf("%s-%d.tar", CockroachNodeTenantCertsDir, virtualClusterID)
}

// DistributeCerts will generate and distribute certificates to all the nodes.
func (c *SyncedCluster) DistributeCerts(
	ctx context.Context, l *logger.Logger, redistribute bool,
) error {
	if !redistribute && c.checkForCertificates(ctx, l) {
		return nil
	}

	nodeNames, err := c.createNodeCertArguments(l)
	if err != nil {
		return err
	}

	// Generate the ca, client and node certificates on the first node.
	display := fmt.Sprintf("%s: initializing certs", c.Name)
	if err := c.Parallel(ctx, l, WithNodes(c.Nodes[0:1]).WithDisplay(display),
		func(ctx context.Context, node Node) (*RunResultDetails, error) {
			var cmd string
			if c.IsLocal() {
				cmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(1))
			}
			cmd += fmt.Sprintf(`
%[6]s
rm -fr %[2]s
mkdir -p %[2]s
VERSION=$(%[1]s version --build-tag)
VERSION=${VERSION::5}
TENANT_SCOPE_OPT=""
if [[ $VERSION = v22.2 ]]; then
       TENANT_SCOPE_OPT="--tenant-scope $(echo {1..100} | tr ' ' ',')"
fi
%[1]s cert create-ca --certs-dir=%[2]s --ca-key=%[2]s/ca.key
%[1]s cert create-client root --certs-dir=%[2]s --ca-key=%[2]s/ca.key $TENANT_SCOPE_OPT
%[1]s cert create-client %[3]s --certs-dir=%[2]s --ca-key=%[2]s/ca.key $TENANT_SCOPE_OPT
%[1]s cert create-node %[4]s --certs-dir=%[2]s --ca-key=%[2]s/ca.key
tar cvf %[5]s %[2]s
`, cockroachNodeBinary(c, 1), CockroachNodeCertsDir, DefaultUser, strings.Join(nodeNames, " "), certsTarName, SuppressMetamorphicConstantsEnvVar())

			return c.runCmdOnSingleNode(ctx, l, node, cmd, defaultCmdOpts("init-certs"))
		},
	); err != nil {
		return err
	}

	tarfile, cleanup, err := c.getFileFromFirstNode(ctx, l, certsTarName)
	if err != nil {
		return err
	}
	defer cleanup()

	// Skip the first node which is where we generated the certs.
	nodes := allNodes(len(c.VMs))[1:]
	if err = c.distributeLocalCertsTar(ctx, l, tarfile, nodes, 0); err != nil {
		return err
	}
	if redistribute {
		if err = c.Signal(ctx, l, int(unix.SIGHUP)); err != nil {
			return err
		}
	}
	return nil
}

// RedistributeNodeCert will generate a new node cert to capture any new hosts
// and distribute the updated certificate to all the nodes.
func (c *SyncedCluster) RedistributeNodeCert(ctx context.Context, l *logger.Logger) error {
	nodeNames, err := c.createNodeCertArguments(l)
	if err != nil {
		return err
	}

	// Generate only the node certificate on the first node.
	display := fmt.Sprintf("%s: initializing node cert", c.Name)
	if err := c.Parallel(ctx, l, WithNodes(c.Nodes[0:1]).WithDisplay(display),
		func(ctx context.Context, node Node) (*RunResultDetails, error) {
			var cmd string
			if c.IsLocal() {
				cmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(1))
			}
			cmd += fmt.Sprintf(`
%[6]s
rm -fr %[2]s/node*
mkdir -p %[2]s
%[1]s cert create-node %[4]s --certs-dir=%[2]s --ca-key=%[2]s/ca.key
tar cvf %[5]s %[2]s
`, cockroachNodeBinary(c, 1), CockroachNodeCertsDir, DefaultUser, strings.Join(nodeNames, " "), certsTarName, SuppressMetamorphicConstantsEnvVar())

			return c.runCmdOnSingleNode(ctx, l, node, cmd, defaultCmdOpts("redist-node-cert"))
		},
	); err != nil {
		return err
	}

	tarfile, cleanup, err := c.getFileFromFirstNode(ctx, l, certsTarName)
	if err != nil {
		return err
	}
	defer cleanup()

	// Skip the first node which is where we generated the certs.
	nodes := allNodes(len(c.VMs))[1:]

	if err := c.distributeLocalCertsTar(ctx, l, tarfile, nodes, 0); err != nil {
		return err
	}
	// Send a SIGHUP to the nodes to reload the certificates.
	return c.Signal(ctx, l, int(unix.SIGHUP))
}

// DistributeTenantCerts will generate and distribute certificates to all of the
// nodes, using the host cluster to generate tenant certificates.
func (c *SyncedCluster) DistributeTenantCerts(
	ctx context.Context, l *logger.Logger, hostCluster *SyncedCluster, virtualClusterID int,
) error {
	if hostCluster.checkForTenantCertificates(ctx, l, virtualClusterID) {
		return nil
	}

	if !hostCluster.checkForCertificates(ctx, l) {
		return errors.New("host cluster missing certificate bundle")
	}

	nodeNames, err := c.createNodeCertArguments(l)
	if err != nil {
		return err
	}

	certsTar := tenantCertsTarName(virtualClusterID)
	if err := hostCluster.createTenantCertBundle(
		ctx, l, tenantCertsTarName(virtualClusterID), virtualClusterID, nodeNames,
	); err != nil {
		return err
	}

	tarfile, cleanup, err := hostCluster.getFileFromFirstNode(ctx, l, certsTar)
	if err != nil {
		return err
	}
	defer cleanup()
	return c.distributeLocalCertsTar(ctx, l, tarfile, allNodes(len(c.VMs)), 1)
}

// createTenantCertBundle creates a client cert bundle with the given name using
// the first node in the cluster. The nodeNames provided are added to all node
// and tenant-client certs.
//
// This function assumes it is running on a host cluster node that already has
// had the main cert bundle created.
func (c *SyncedCluster) createTenantCertBundle(
	ctx context.Context,
	l *logger.Logger,
	bundleName string,
	virtualClusterID int,
	nodeNames []string,
) error {
	display := fmt.Sprintf("%s: initializing tenant certs", c.Name)
	return c.Parallel(ctx, l, WithNodes(c.Nodes[0:1]).WithDisplay(display),
		func(ctx context.Context, node Node) (*RunResultDetails, error) {
			cmd := "set -e;"
			if c.IsLocal() {
				cmd += fmt.Sprintf(`cd %s ; `, c.localVMDir(1))
			}
			cmd += fmt.Sprintf(`
%[7]s
CERT_DIR=%[1]s-%[5]d/certs
CA_KEY=%[2]s/ca.key

rm -fr $CERT_DIR
mkdir -p $CERT_DIR
cp %[2]s/ca.crt $CERT_DIR
SHARED_ARGS="--certs-dir=$CERT_DIR --ca-key=$CA_KEY"
VERSION=$(%[3]s version --build-tag)
VERSION=${VERSION::3}
TENANT_SCOPE_OPT=""
if [[ $VERSION = v22 ]]; then
        TENANT_SCOPE_OPT="--tenant-scope %[5]d"
fi
%[3]s cert create-node %[4]s $SHARED_ARGS
%[3]s cert create-tenant-client %[5]d %[4]s $SHARED_ARGS
%[3]s cert create-client root $TENANT_SCOPE_OPT $SHARED_ARGS
tar cvf %[6]s $CERT_DIR
`,
				CockroachNodeTenantCertsDir,
				CockroachNodeCertsDir,
				cockroachNodeBinary(c, node),
				strings.Join(nodeNames, " "),
				virtualClusterID,
				bundleName,
				SuppressMetamorphicConstantsEnvVar(),
			)

			return c.runCmdOnSingleNode(ctx, l, node, cmd, defaultCmdOpts("create-tenant-cert-bundle"))
		})
}

// getFile retrieves the given file from the first node in the cluster. The
// filename is assumed to be relative from the home directory of the node's
// user.
func (c *SyncedCluster) getFileFromFirstNode(
	ctx context.Context, l *logger.Logger, name string,
) (string, func(), error) {
	var tmpfileName string
	cleanup := func() {}
	if c.IsLocal() {
		tmpfileName = os.ExpandEnv(filepath.Join(c.localVMDir(1), name))
	} else {
		tmpfile, err := os.CreateTemp("", name)
		if err != nil {
			return "", nil, err
		}
		_ = tmpfile.Close()
		cleanup = func() {
			_ = os.Remove(tmpfile.Name()) // clean up
		}

		srcFileName := fmt.Sprintf("%s@%s:%s", c.user(1), c.Host(1), name)
		if res, _ := scpWithRetry(ctx, l, srcFileName, tmpfile.Name()); res.Err != nil {
			cleanup()
			return "", nil, res.Err
		}
		tmpfileName = tmpfile.Name()
	}
	return tmpfileName, cleanup, nil
}

// checkForCertificates checks if the cluster already has a certs bundle created
// on the first node.
func (c *SyncedCluster) checkForCertificates(ctx context.Context, l *logger.Logger) bool {
	dir := ""
	if c.IsLocal() {
		dir = c.localVMDir(1)
	}
	return c.fileExistsOnFirstNode(ctx, l, filepath.Join(dir, certsTarName))
}

// checkForTenantCertificates checks if the cluster already has a tenant-certs
// bundle created on the first node and if a tenant certificate exists for the
// given virtual cluster ID.
func (c *SyncedCluster) checkForTenantCertificates(
	ctx context.Context, l *logger.Logger, virtualClusterID int,
) bool {
	dir := ""
	if c.IsLocal() {
		dir = c.localVMDir(1)
	}
	if !c.fileExistsOnFirstNode(ctx, l, filepath.Join(dir, tenantCertsTarName(virtualClusterID))) {
		return false
	}
	return c.fileExistsOnFirstNode(ctx, l, filepath.Join(c.CertsDir(1), fmt.Sprintf(tenantCertFile, virtualClusterID)))
}

func (c *SyncedCluster) fileExistsOnFirstNode(
	ctx context.Context, l *logger.Logger, path string,
) bool {
	l.Printf("%s: checking %s", c.Name, path)
	runOpts := defaultCmdOpts("check-file-exists")
	runOpts.includeRoachprodEnvVars = true
	res, _ := c.runCmdOnSingleNode(ctx, l, 1, `test -e `+path, runOpts)
	// We only return true if the command succeeded.
	return res != nil && res.RemoteExitStatus == 0
}

// createNodeCertArguments returns a list of strings appropriate for use as
// SubjectAlternativeName arguments to the ./cockroach cert create-node command.
// It gathers all the internal and external IP addresses and hostnames for every
// node in the cluster, and finally any load balancer IPs.
func (c *SyncedCluster) createNodeCertArguments(l *logger.Logger) ([]string, error) {
	nodeNames := []string{"localhost"}
	if c.IsLocal() {
		// For local clusters, we only need to add one of the VM IP addresses.
		return append(nodeNames, "$(hostname)", c.VMs[0].PublicIP), nil
	}
	// Gather the internal and external IP addresse and hostname for every node in the cluster, even
	// if it won't be added to the cluster itself we still add the IP address
	// to the node cert.
	for _, n := range allNodes(len(c.VMs)) {
		ip, err := c.GetInternalIP(n)
		if err != nil {
			return nil, err
		}
		nodeNames = append(nodeNames, ip, c.Host(n), fmt.Sprintf("%s-%04d", c.Name, n))
		// AWS nodes internally have a DNS name in the form ip-<ip-addresss>
		// where dots are replaced with dashes.
		// See https://docs.aws.amazon.com/vpc/latest/userguide/vpc-dns.html#vpc-dns-hostnames
		if c.VMs[n-1].Provider == aws.ProviderName {
			nodeNames = append(nodeNames, "ip-"+strings.ReplaceAll(ip, ".", "-"))
		}
	}
	// Add any load balancers IPs to the list of names.
	lbAddresses, err := c.ListLoadBalancers(l)
	if err != nil {
		return nil, err
	}
	for _, lb := range lbAddresses {
		nodeNames = append(nodeNames, lb.IP)
	}
	return nodeNames, nil
}

// distributeLocalTar distributes the given file to the given nodes and untars it.
func (c *SyncedCluster) distributeLocalCertsTar(
	ctx context.Context, l *logger.Logger, filename string, nodes Nodes, stripComponents int,
) error {
	// Read the certs.tar file we just downloaded. We'll be piping it to the
	// other nodes in the cluster.
	certsTar, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	display := c.Name + ": distributing certs"
	return c.Parallel(ctx, l, WithNodes(nodes).WithDisplay(display),
		func(ctx context.Context, node Node) (*RunResultDetails, error) {
			var cmd string
			if c.IsLocal() {
				cmd = fmt.Sprintf("cd %s ; ", c.localVMDir(node))
			}
			if stripComponents > 0 {
				cmd += fmt.Sprintf("tar --strip-components=%d -xf -", stripComponents)
			} else {
				cmd += "tar xf -"
			}

			runOpts := defaultCmdOpts("dist-local-certs")
			runOpts.stdin = bytes.NewReader(certsTar)
			return c.runCmdOnSingleNode(ctx, l, node, cmd, runOpts)
		})
}

const progressDone = "=======================================>"
const progressTodo = "----------------------------------------"

func formatProgress(p float64) string {
	i := int(math.Ceil(float64(len(progressDone)) * (1 - p)))
	if i > len(progressDone) {
		i = len(progressDone)
	}
	if i < 0 {
		i = 0
	}
	return fmt.Sprintf("[%s%s] %.0f%%", progressDone[i:], progressTodo[:i], 100*p)
}

// PutString into the specified file on the specified remote node(s).
func (c *SyncedCluster) PutString(
	ctx context.Context, l *logger.Logger, nodes Nodes, content string, dest string, mode os.FileMode,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "syncedCluster.PutString")
	}

	temp, err := os.CreateTemp("", filepath.Base(dest))
	if err != nil {
		return errors.Wrap(err, "cluster.PutString")
	}
	if _, err := temp.WriteString(content); err != nil {
		return errors.Wrap(err, "cluster.PutString")
	}
	temp.Close()
	src := temp.Name()

	if err := os.Chmod(src, mode); err != nil {
		return errors.Wrap(err, "cluster.PutString")
	}
	// NB: we intentionally don't remove the temp files. This is because roachprod
	// will symlink them when running locally.

	return errors.Wrap(c.Put(ctx, l, nodes, src, dest), "syncedCluster.PutString")
}

// Put TODO(peter): document
func (c *SyncedCluster) Put(
	ctx context.Context, l *logger.Logger, nodes Nodes, src string, dest string,
) error {
	if err := c.validateHost(ctx, l, nodes); err != nil {
		return err
	}
	// Check if source file exists and if it's a symlink.
	var potentialSymlinkPath string
	var err error
	if potentialSymlinkPath, err = filepath.EvalSymlinks(src); err != nil {
		return err
	}

	absSrc, err := filepath.Abs(src)
	if err != nil {
		return fmt.Errorf("error computing absolute path for %s: %w", src, err)
	}
	absSymlink, err := filepath.Abs(potentialSymlinkPath)
	if err != nil {
		return fmt.Errorf("error computing absolute path for %s: %w", potentialSymlinkPath, err)
	}
	// Different paths imply it is a symlink.
	if absSrc != absSymlink {
		// Get target symlink access mode.
		var symlinkTargetInfo fs.FileInfo
		if symlinkTargetInfo, err = os.Stat(potentialSymlinkPath); err != nil {
			return err
		}
		redColor, resetColor := "\033[31m", "\033[0m"
		l.Printf(redColor+"WARNING: Source file is a symlink to %s"+resetColor, absSymlink)
		l.Printf(redColor+"WARNING: Remote file will inherit the target permissions '%v'."+resetColor, symlinkTargetInfo.Mode())
	}

	// NB: This value was determined with a few experiments. Higher values were
	// not tested.
	const treeDistFanout = 10

	// If scp does not support the -R flag, treedist mode is slower as it ends up
	// transferring everything through this machine anyway.
	useTreeDist := c.UseTreeDist && sshVersion3()
	var detail string
	if !c.IsLocal() {
		if useTreeDist {
			detail = " (dist)"
		} else {
			detail = " (scp)"
		}
	}
	l.Printf("%s: putting%s %s %s on nodes %v\n", c.Name, detail, src, dest, nodes)

	type result struct {
		index int
		err   error
	}

	results := make(chan result, len(nodes))
	var wg sync.WaitGroup
	wg.Add(len(nodes))

	// We currently don't accept any custom expander configurations in
	// this function.
	var expanderConfig ExpanderConfig

	// Each destination for the copy needs a source to copy from. We create a
	// channel that has capacity for each destination. If we try to add a source
	// and the channel is full we can simply drop that source as we know we won't
	// need to use it.
	sources := make(chan int, len(nodes))
	pushSource := func(i int) {
		select {
		case sources <- i:
		default:
		}
	}

	if useTreeDist {
		// In treedist mode, only add the local source initially.
		pushSource(-1)
	} else {
		// In non-treedist mode, add the local source N times (once for each
		// destination).
		for range nodes {
			pushSource(-1)
		}
	}

	mkpath := func(i int, dest string) (string, error) {
		if i == -1 {
			return src, nil
		}
		// Expand the destination to allow, for example, putting directly
		// into {store-dir}.
		e := expander{
			node: nodes[i],
		}
		dest, err := e.expand(ctx, l, c, expanderConfig, dest)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s@%s:%s", c.user(nodes[i]), c.Host(nodes[i]), dest), nil
	}

	spinner := ui.NewDefaultTaskSpinner(l, "")
	nodeTaskStatus := func(nodeID Node, msg string, done bool) {
		spinner.TaskStatus(nodeID, fmt.Sprintf("  %2d: %s", nodeID, msg), done)
	}

	for i := range nodes {
		nodeTaskStatus(nodes[i], "copying", false)
		go func(i int, dest string) {
			defer wg.Done()
			if c.IsLocal() {
				// Expand the destination to allow, for example, putting directly
				// into {store-dir}.
				e := expander{
					node: nodes[i],
				}
				var err error
				dest, err = e.expand(ctx, l, c, expanderConfig, dest)
				if err != nil {
					results <- result{i, err}
					return
				}
				if _, err := os.Stat(src); err != nil {
					results <- result{i, err}
					return
				}
				from, err := filepath.Abs(src)
				if err != nil {
					results <- result{i, err}
					return
				}
				// TODO(jlinder): this does not take into account things like
				// roachprod put local:1 /some/file.txt /some/dir
				// and will replace 'dir' with the contents of file.txt, instead
				// of creating /some/dir/file.txt.
				var to string
				if filepath.IsAbs(dest) {
					to = dest
				} else {
					to = filepath.Join(c.localVMDir(nodes[i]), dest)
				}
				// Remove the destination if it exists, ignoring errors which we'll
				// handle via the os.Symlink() call.
				_ = os.Remove(to)
				results <- result{i, os.Symlink(from, to)}
				return
			}

			// Determine the source to copy from.
			//
			// TODO(peter): Take the cluster topology into account. We should
			// preferentially use a source in the same region and only perform a
			// single copy between regions. We have the region information and
			// achieving this approach is likely a generalization of the current
			// code.
			srcIndex := <-sources
			from, err := mkpath(srcIndex, dest)
			if err != nil {
				results <- result{i, err}
				return
			}
			// TODO(peter): For remote-to-remote copies, should the destination use
			// the internal IP address? The external address works, but it might be
			// slower.
			to, err := mkpath(i, dest)
			if err != nil {
				results <- result{i, err}
				return
			}

			res, _ := scpWithRetry(ctx, l, from, to)
			results <- result{i, res.Err}

			if res.Err != nil {
				// The copy failed. Re-add the original source.
				pushSource(srcIndex)
			} else {
				// The copy failed. Re-add the original source if it is remote.
				if srcIndex != -1 {
					pushSource(srcIndex)
				}
				// Add fanout number of new sources for the destination.
				for j := 0; j < treeDistFanout; j++ {
					pushSource(i)
				}
			}
		}(i, dest)
	}

	defer spinner.Start()()
	go func() {
		wg.Wait()
		close(results)
	}()

	var finalErr error
	setErr := func(e error) {
		if finalErr == nil {
			finalErr = e
		}
	}

	for {
		r, ok := <-results
		if !ok {
			break
		}
		if r.err != nil {
			setErr(r.err)
			nodeTaskStatus(nodes[r.index], r.err.Error(), true)
		} else {
			nodeTaskStatus(nodes[r.index], "done", true)
		}
	}
	spinner.MaybeLogTasks(l)

	if finalErr != nil {
		return errors.Wrapf(finalErr, "put %q failed", src)
	}
	return nil
}

// Logs will sync the logs from src to dest with each node's logs under dest in
// directories per node and stream the merged logs to out.
// For example, if dest is "tpcc-test.logs" then the logs for each node will be
// stored like:
//
//	tpcc-test.logs/1.logs/...
//	tpcc-test.logs/2.logs/...
//	...
//
// Log file syncing uses rsync which attempts to be efficient when deciding
// which files to update. The logs are merged by calling
// `cockroach debug merge-logs <dest>/*/*` with the optional flag for filter.
// The syncing and merging happens in a loop which pauses <interval> between
// iterations and takes some care with the from/to flags in merge-logs to make
// new logs appear to be streamed. If <from> is zero streaming begins from now.
// If to is non-zero, when the stream of logs passes to, the function returns.
// TODO(herko): This command does not support virtual clusters yet.
func (c *SyncedCluster) Logs(
	l *logger.Logger,
	src, dest, filter, programFilter string,
	interval time.Duration,
	from, to time.Time,
	out io.Writer,
) error {
	if err := c.validateHost(context.TODO(), l, c.Nodes); err != nil {
		return err
	}
	rsyncNodeLogs := func(ctx context.Context, node Node) error {
		base := fmt.Sprintf("%d.logs", node)
		local := filepath.Join(dest, base) + "/"
		rsyncArgs := []string{"-az", "--size-only"}
		userHomeDir := ""
		if !config.UseSharedUser {
			userHomeDir = config.OSUser.HomeDir
		}
		var remote string
		if c.IsLocal() {
			// This here is a bit of a hack to guess that the parent of the log dir is
			// the "home" for the local node and that the srcBase is relative to that.
			localHome := filepath.Dir(c.LogDir(node, "", 0))
			remote = filepath.Join(localHome, src) + "/"
		} else {
			logDir := src
			if !filepath.IsAbs(logDir) && userHomeDir != "" {
				logDir = filepath.Join(userHomeDir, logDir)
			}
			remote = fmt.Sprintf("%s@%s:%s/", c.user(node), c.Host(node), logDir)
			// Use control master to mitigate SSH connection setup cost.
			rsyncArgs = append(rsyncArgs, "--rsh", "ssh "+
				"-o StrictHostKeyChecking=no "+
				"-o ControlMaster=auto "+
				"-o ControlPath=~/.ssh/%r@%h:%p "+
				"-o UserKnownHostsFile=/dev/null "+
				"-o ControlPersist=2m "+
				strings.Join(sshAuthArgs(), " "))
		}
		rsyncArgs = append(rsyncArgs, remote, local)
		cmd := exec.CommandContext(ctx, "rsync", rsyncArgs...)
		var stderrBuf bytes.Buffer
		cmd.Stdout = os.Stdout
		cmd.Stderr = &stderrBuf

		if err := cmd.Run(); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return errors.Wrapf(err, "failed to rsync from %v to %v:\n%s\n",
				src, dest, stderrBuf.String())
		}
		return nil
	}
	rsyncLogs := func(ctx context.Context) error {
		g, gctx := errgroup.WithContext(ctx)
		for i := range c.Nodes {
			node := c.Nodes[i]
			g.Go(func() error {
				return rsyncNodeLogs(gctx, node)
			})
		}
		return g.Wait()
	}
	mergeLogs := func(ctx context.Context, prev, t time.Time) error {
		cmd := exec.CommandContext(ctx, "cockroach", "debug", "merge-logs",
			dest+"/*/*",
			"--from", prev.Format(time.RFC3339),
			"--to", t.Format(time.RFC3339))
		if filter != "" {
			cmd.Args = append(cmd.Args, "--filter", filter)
		}
		if programFilter != "" {
			cmd.Args = append(cmd.Args, "--program-filter", programFilter)
		}
		// For local clusters capture the cluster ID from the sync path because the
		// host information is useless.
		if c.IsLocal() {
			cmd.Args = append(cmd.Args,
				"--file-pattern", "^(?:.*/)?(?P<id>[0-9]+).*/"+log.FileNamePattern+"$",
				"--prefix", "${id}> ")
		}
		cmd.Stdout = out
		var errBuf bytes.Buffer
		cmd.Stderr = &errBuf
		if err := cmd.Run(); err != nil && ctx.Err() == nil {
			return errors.Wrapf(err, "failed to run cockroach debug merge-logs:\n%v", errBuf.String())
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := os.MkdirAll(dest, 0755); err != nil {
		return errors.Wrapf(err, "failed to create destination directory")
	}
	// Cancel context upon signaling.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer func() { signal.Stop(ch); close(ch) }()
	go func() { <-ch; cancel() }()
	// TODO(ajwerner): consider SIGHUP-ing cockroach before the rsync to avoid the delays
	prev := from
	if prev.IsZero() {
		prev = timeutil.Now().Add(-2 * time.Second).Truncate(time.Microsecond)
	}
	for to.IsZero() || prev.Before(to) {
		// Subtract ~1 second to deal with the flush delay in util/log.
		t := timeutil.Now().Add(-1100 * time.Millisecond).Truncate(time.Microsecond)
		if err := rsyncLogs(ctx); err != nil {
			return errors.Wrapf(err, "failed to sync logs")
		}
		if !to.IsZero() && t.After(to) {
			t = to
		}
		if err := mergeLogs(ctx, prev, t); err != nil {
			return err
		}
		prev = t
		if !to.IsZero() && !prev.Before(to) {
			return nil
		}
		select {
		case <-time.After(interval):
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

// Get TODO(peter): document
func (c *SyncedCluster) Get(
	ctx context.Context, l *logger.Logger, nodes Nodes, src, dest string,
) error {
	if err := c.validateHost(ctx, l, nodes); err != nil {
		return err
	}
	// TODO(peter): Only get 10 nodes at a time. When a node completes, output a
	// line indicating that.
	var detail string
	if !c.IsLocal() {
		detail = " (scp)"
	}
	l.Printf("%s: getting%s %s %s on nodes %v\n", c.Name, detail, src, dest, nodes)

	type result struct {
		index int
		err   error
	}

	results := make(chan result, len(nodes))

	spinner := ui.NewDefaultTaskSpinner(l, "")
	nodeTaskStatus := func(nodeID Node, msg string, done bool) {
		spinner.TaskStatus(nodeID, fmt.Sprintf("  %2d: %s", nodeID, msg), done)
	}

	// We currently don't accept any custom expander configurations in
	// this function.
	var expanderConfig ExpanderConfig

	var wg sync.WaitGroup
	for i := range nodes {
		nodeTaskStatus(nodes[i], "copying", false)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			src := src
			dest := dest
			if len(nodes) > 1 {
				base := fmt.Sprintf("%d.%s", nodes[i], filepath.Base(dest))
				dest = filepath.Join(filepath.Dir(dest), base)
			}

			// Expand the source to allow, for example, getting from {store-dir}.
			e := expander{
				node: nodes[i],
			}
			src, err := e.expand(ctx, l, c, expanderConfig, src)
			if err != nil {
				results <- result{i, err}
				return
			}

			progress := func(p float64) {
				nodeTaskStatus(nodes[i], formatProgress(p), false)
			}

			if c.IsLocal() {
				if !filepath.IsAbs(src) {
					src = filepath.Join(c.localVMDir(nodes[i]), src)
				}

				var copy func(src, dest string, info os.FileInfo) error
				copy = func(src, dest string, info os.FileInfo) error {
					// Make sure the destination file is world readable.
					// See:
					// https://github.com/cockroachdb/cockroach/issues/44843
					mode := info.Mode() | 0444
					if info.IsDir() {
						if err := os.MkdirAll(dest, mode); err != nil {
							return err
						}

						infos, err := os.ReadDir(src)
						if err != nil {
							return err
						}

						for _, dirEntry := range infos {
							fileInfo, err := dirEntry.Info()
							if err != nil {
								return err
							}
							if err := copy(
								filepath.Join(src, fileInfo.Name()),
								filepath.Join(dest, fileInfo.Name()),
								fileInfo,
							); err != nil {
								return err
							}
						}
						return nil
					}

					if !mode.IsRegular() {
						return nil
					}

					out, err := os.Create(dest)
					if err != nil {
						return err
					}
					defer out.Close()

					if err := os.Chmod(out.Name(), mode); err != nil {
						return err
					}

					in, err := os.Open(src)
					if err != nil {
						return err
					}
					defer in.Close()

					p := &ssh.ProgressWriter{
						Writer:   out,
						Done:     0,
						Total:    info.Size(),
						Progress: progress,
					}
					_, err = io.Copy(p, in)
					return err
				}

				info, err := os.Stat(src)
				if err != nil {
					results <- result{i, err}
					return
				}
				err = copy(src, dest, info)
				results <- result{i, err}
				return
			}

			res, _ := scpWithRetry(ctx, l, fmt.Sprintf("%s@%s:%s", c.user(nodes[0]), c.Host(nodes[i]), src), dest)
			if res.Err == nil {
				// Make sure all created files and directories are world readable.
				// The CRDB process intentionally sets a 0007 umask (resulting in
				// non-world-readable files). This creates annoyances during CI
				// that we circumvent wholesale by adding o+r back here.
				// See:
				//
				// https://github.com/cockroachdb/cockroach/issues/44843
				chmod := func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					const oRead = 0004
					if mode := info.Mode(); mode&oRead == 0 {
						if err := os.Chmod(path, mode|oRead); err != nil {
							return err
						}
					}
					return nil
				}
				res.Err = filepath.Walk(dest, chmod)
			}

			results <- result{i, res.Err}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var finalErr error
	setErr := func(e error) {
		if finalErr == nil {
			finalErr = e
		}
	}

	defer spinner.Start()()
	for {
		r, ok := <-results
		if !ok {
			break
		}
		if r.err != nil {
			setErr(r.err)
			nodeTaskStatus(nodes[r.index], r.err.Error(), true)
		} else {
			nodeTaskStatus(nodes[r.index], "done", true)
		}
	}
	spinner.MaybeLogTasks(l)

	if finalErr != nil {
		return errors.Wrapf(finalErr, "get %s failed", src)
	}
	return nil
}

// pgurls returns a map of PG URLs for the given nodes.
func (c *SyncedCluster) pgurls(
	ctx context.Context, l *logger.Logger, nodes Nodes, virtualClusterName string, sqlInstance int,
) (map[Node]string, error) {
	hosts, err := c.pghosts(ctx, l, nodes)
	if err != nil {
		return nil, err
	}
	m := make(map[Node]string, len(hosts))
	for node, host := range hosts {
		desc, err := c.DiscoverService(ctx, node, virtualClusterName, ServiceTypeSQL, sqlInstance)
		if err != nil {
			return nil, err
		}
		m[node] = c.NodeURL(host, desc.Port, virtualClusterName, desc.ServiceMode, DefaultAuthMode(), "" /* database */)
	}
	return m, nil
}

// pghosts returns a map of IP addresses for the given nodes.
func (c *SyncedCluster) pghosts(
	ctx context.Context, l *logger.Logger, nodes Nodes,
) (map[Node]string, error) {
	m := make(map[Node]string, len(nodes))

	for i := 0; i < len(nodes); i++ {
		ip, err := c.GetInternalIP(nodes[i])
		if err == nil {
			m[nodes[i]] = ip
		}
	}

	return m, nil
}

// resolveLoadBalancerURL resolves the load balancer postgres URL for the given
// virtual cluster and SQL instance. Returns an empty string if a load balancer
// is not found.
func (c *SyncedCluster) loadBalancerURL(
	ctx context.Context,
	l *logger.Logger,
	virtualClusterName string,
	sqlInstance int,
	auth PGAuthMode,
) (string, error) {
	services, err := c.DiscoverServices(ctx, virtualClusterName, ServiceTypeSQL)
	if err != nil {
		return "", err
	}
	port := config.DefaultSQLPort
	serviceMode := ServiceModeExternal
	for _, service := range services {
		if service.VirtualClusterName == virtualClusterName && service.Instance == sqlInstance {
			serviceMode = service.ServiceMode
			port = service.Port
			break
		}
	}
	address, err := c.FindLoadBalancer(l, port)
	if err != nil {
		return "", err
	}
	loadBalancerURL := c.NodeURL(address.IP, address.Port, virtualClusterName, serviceMode, auth, "" /* database */)
	return loadBalancerURL, nil
}

// SSH creates an interactive shell connecting the caller to the first
// node on the cluster (or to all nodes in an iterm2 split screen if
// supported).
//
// CAUTION: this script will `exec` the ssh utility, so it must not be
// used in any roachtest code. This is for use within `./roachprod`
// exclusively.
func (c *SyncedCluster) SSH(ctx context.Context, l *logger.Logger, sshArgs, args []string) error {
	targetNode := c.Nodes[0]
	if err := c.validateHost(ctx, l, Nodes{targetNode}); err != nil {
		return err
	}

	if len(c.Nodes) != 1 && len(args) == 0 {
		// If trying to ssh to more than 1 node and the ssh session is interactive,
		// try sshing with an iTerm2 split screen configuration.
		sshed, err := maybeSplitScreenSSHITerm2(c)
		if sshed {
			return err
		}
	}

	// Perform template expansion on the arguments.
	e := expander{
		node: targetNode,
	}
	var expandedArgs []string
	// We currently don't accept any custom expander configurations in
	// this function.
	var expanderConfig ExpanderConfig
	for _, arg := range args {
		expandedArg, err := e.expand(ctx, l, c, expanderConfig, arg)
		if err != nil {
			return err
		}
		expandedArgs = append(expandedArgs, strings.Split(expandedArg, " ")...)
	}

	var allArgs []string
	if c.IsLocal() {
		allArgs = []string{
			"/bin/bash", "-c",
		}
		cmd := fmt.Sprintf("cd %s ; ", c.localVMDir(targetNode))
		if len(args) == 0 /* interactive */ {
			cmd += "/bin/bash "
		}
		if len(args) > 0 {
			cmd += fmt.Sprintf("export ROACHPROD=%s ; ", c.roachprodEnvValue(targetNode))
			cmd += strings.Join(expandedArgs, " ")
		}
		allArgs = append(allArgs, cmd)
	} else {
		allArgs = []string{
			"ssh",
			fmt.Sprintf("%s@%s", c.user(targetNode), c.Host(targetNode)),
			"-o", "UserKnownHostsFile=/dev/null",
			"-o", "StrictHostKeyChecking=no",
		}
		allArgs = append(allArgs, sshAuthArgs()...)
		allArgs = append(allArgs, sshArgs...)
		if len(args) > 0 {
			allArgs = append(allArgs, fmt.Sprintf(
				"export ROACHPROD=%s ;", c.roachprodEnvValue(targetNode),
			))
		}
		allArgs = append(allArgs, expandedArgs...)
	}

	sshPath, err := exec.LookPath(allArgs[0])
	if err != nil {
		return err
	}
	return syscall.Exec(sshPath, allArgs, os.Environ())
}

var sshVersion3Internal struct {
	value bool
	once  sync.Once
}

// sshVersion3 returns true if ssh uses an SSL library at major version 3.
func sshVersion3() bool {
	sshVersion3Internal.once.Do(func() {
		cmd := exec.Command("ssh", "-V")
		out, err := cmd.CombinedOutput()
		if err != nil {
			panic(fmt.Sprintf("error running ssh -V: %v\nOutput:\n%s", err, string(out)))
		}
		sshVersion3Internal.value = strings.Contains(string(out), "SSL 3.")
	})
	return sshVersion3Internal.value
}

// scp return type conforms to what runWithMaybeRetry expects. A nil error
// is always returned here since the only error that can happen is an scp error
// which we do want to be able to retry.
func scp(ctx context.Context, l *logger.Logger, src, dest string) (*RunResultDetails, error) {
	args := []string{
		// Enable recursive copies, compression.
		"scp", "-r", "-C",
		"-o", "StrictHostKeyChecking=no",
		"-o", "ConnectTimeout=10",
	}
	if sshVersion3() {
		// Have scp do a direct transfer between two remote hosts (SSH to src node
		// and execute SCP there using agent-forwarding).
		args = append(args, "-R", "-A")
	}
	args = append(args, sshAuthArgs()...)
	args = append(args, src, dest)
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.WaitDelay = time.Second // make sure the call below returns when the context is canceled

	out, err := cmd.CombinedOutput()
	if err != nil {
		err = rperrors.NewSSHError(errors.Wrapf(err, "~ %s\n%s", strings.Join(args, " "), out))
	}

	res := newRunResultDetails(-1, err)
	res.CombinedOut = string(out)
	return res, nil
}

// Parallel runs a user-defined function across the nodes in the
// cluster. If any of the commands fail, Parallel will log each failure
// and return an error.
//
// A user may also pass in a RetryOpts to control how the function is retried
// in the case of a failure.
//
// See ParallelE for more information.
func (c *SyncedCluster) Parallel(
	ctx context.Context,
	l *logger.Logger,
	options RunOptions,
	fn func(ctx context.Context, n Node) (*RunResultDetails, error),
) error {
	results, hasError, err := c.ParallelE(ctx, l, options, fn)
	// `err` is an unexpected roachprod error, which we return immediately.
	if err != nil {
		return err
	}

	// `hasError` is true if any of the commands returned an error.
	if hasError {
		for _, r := range results {
			// Since this function is potentially returning a single error despite
			// having run on multiple nodes, we combine all the errors into a single
			// error.
			if r != nil && r.Err != nil {
				err = errors.CombineErrors(err, r.Err)
				l.Errorf("%d: %+v: %s", r.Node, r.Err, r.CombinedOut)
			}
		}
		return errors.Wrap(err, "one or more parallel execution failure(s)")
	}
	return nil
}

type ParallelResult struct {
	// Index is the order position in which the node was passed to Parallel.
	// This is useful in maintaining the order of results.
	Index int
	*RunResultDetails
}

// ParallelE runs the given function in parallel on the specified nodes.
//
// By default, this will fail fast, unless explicitly specified otherwise in the
// RunOptions, if a command error occurs on any node, and return a slice
// containing all results up to that point, along with a boolean indicating that
// at least one error occurred. If `WithFailSlow()` is passed in, then the
// function will wait for all invocations to complete before returning.
//
// ParallelE only returns an error for roachprod itself, not any command errors run
// on the cluster.
//
// ParallelE runs at most `concurrency` (or  `config.MaxConcurrency` if it is lower) in parallel.
// If `concurrency` is 0, then it defaults to `len(nodes)`.
//
// The function returns pointers to *RunResultDetails as we may enrich
// the result with retry information (attempt number, wrapper error).
//
// RetryOpts controls the retry behavior in the case that
// the function fails, but returns a nil error. A non-nil error returned by the
// function denotes a roachprod error and will not be retried regardless of the
// retry options.
// NB: Result order is the same as input node order
func (c *SyncedCluster) ParallelE(
	ctx context.Context,
	l *logger.Logger,
	options RunOptions,
	fn func(ctx context.Context, n Node) (*RunResultDetails, error),
) ([]*RunResultDetails, bool, error) {
	// Function specific default for FailOption if not specified.
	if options.FailOption == FailDefault {
		options.FailOption = FailFast
	}

	count := len(options.Nodes)
	if options.Concurrency == 0 || options.Concurrency > count {
		options.Concurrency = count
	}
	if config.MaxConcurrency > 0 && options.Concurrency > config.MaxConcurrency {
		options.Concurrency = config.MaxConcurrency
	}

	completed := make(chan ParallelResult, count)
	errorChannel := make(chan error)

	var wg sync.WaitGroup
	wg.Add(count)

	groupCtx, groupCancel := context.WithCancel(ctx)
	defer groupCancel()
	var index int
	startNext := func() {
		// If we needed to react to a context cancellation here we would need to
		// nest this goroutine in another one and select on the groupCtx. However,
		// since anything intensive here is a command over ssh, and we are threading
		// the context through, a cancellation will be handled by the command itself.
		go func(i int) {
			defer wg.Done()
			// This is rarely expected to return an error, but we fail fast in case.
			// Command errors, which are far more common, will be contained within the result.
			res, err := runWithMaybeRetry(
				groupCtx, l, options.RetryOptions, options.ShouldRetryFn,
				func(ctx context.Context) (*RunResultDetails, error) { return fn(ctx, options.Nodes[i]) },
			)
			if err != nil {
				errorChannel <- err
				return
			}
			// The index is captured here so that we can maintain the order of results.
			completed <- ParallelResult{Index: i, RunResultDetails: res}
		}(index)
		index++
	}

	for index < options.Concurrency {
		startNext()
	}

	go func() {
		defer close(completed)
		defer close(errorChannel)
		wg.Wait()
	}()

	spinner := ui.NewDefaultCountingSpinner(l, options.Display, count)
	if options.Display != "" {
		defer spinner.Start()()
	}

	var hasError bool
	n := 0
	results := make([]*RunResultDetails, count)
	for done := false; !done; {
		select {
		case r, ok := <-completed:
			if ok {
				results[r.Index] = r.RunResultDetails
				n++
				if r.Err != nil { // Command error
					hasError = true
					if options.FailOption != FailSlow {
						groupCancel()
						return results, true, nil
					}
				}
				if index < count {
					startNext()
				}
			}
			done = !ok
		case err, ok := <-errorChannel: // Roachprod error
			if ok {
				groupCancel()
				return nil, false, err
			}
		}
		spinner.CountStatus(n)
	}

	return results, hasError, nil
}

// Init initializes the cluster. It does it through node 1 (as per TargetNodes)
// to maintain parity with auto-init behavior of `roachprod start` (when
// --skip-init) is not specified.
func (c *SyncedCluster) Init(ctx context.Context, l *logger.Logger, node Node) error {
	if err := c.initializeCluster(ctx, l, node); err != nil {
		return errors.WithDetail(err, "install.Init() failed: unable to initialize cluster.")
	}

	if err := c.setClusterSettings(ctx, l, node, ""); err != nil {
		return errors.WithDetail(err, "install.Init() failed: unable to set cluster settings.")
	}

	return nil
}

// allPublicAddrs returns a string that can be used when starting cockroach to
// indicate the location of all nodes in the cluster.
func (c *SyncedCluster) allPublicAddrs(ctx context.Context) (string, error) {
	var addrs []string
	for _, node := range c.Nodes {
		port, err := c.NodePort(ctx, node, "" /* virtualClusterName */, 0 /* sqlInstance */)
		if err != nil {
			return "", err
		}
		addrs = append(addrs, fmt.Sprintf("%s:%d", c.Host(node), port))
	}

	return strings.Join(addrs, ","), nil
}

// WithNodes creates a new copy of SyncedCluster with the given nodes.
func (c *SyncedCluster) WithNodes(nodes Nodes) *SyncedCluster {
	clusterCopy := *c
	clusterCopy.Nodes = nodes
	return &clusterCopy
}

// GenFilenameFromArgs given a list of cmd args, returns an alphahumeric string up to
// `maxLen` in length with hyphen delimiters, suitable for use in a filename.
// e.g. ["/bin/bash", "-c", "'sudo dmesg > dmesg.txt'"] -> binbash-c-sudo-dmesg
func GenFilenameFromArgs(maxLen int, args ...string) string {
	cmd := strings.Join(args, " ")
	var sb strings.Builder
	lastCharSpace := true

	writeByte := func(b byte) {
		if b == ' ' {
			if lastCharSpace {
				return
			}
			sb.WriteByte('-')
			lastCharSpace = true
		} else if ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z') || ('0' <= b && b <= '9') {
			sb.WriteByte(b)
			lastCharSpace = false
		}
	}

	for i := 0; i < len(cmd); i++ {
		writeByte(cmd[i])
		if sb.Len() == maxLen {
			return sb.String()
		}
	}

	return sb.String()
}
