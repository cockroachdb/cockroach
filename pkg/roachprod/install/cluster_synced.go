// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"bufio"
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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/template"
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

// validateHost will run `validateHostnameCmd` on the node passed to
// make sure it still belongs to the SyncedCluster. Returns an error
// when the hostnames don't match, indicating that the roachprod cache
// is stale.
func (c *SyncedCluster) validateHost(ctx context.Context, l *logger.Logger, node Node) error {
	if c.IsLocal() {
		return nil
	}
	cmd := c.validateHostnameCmd("", node)
	return c.Run(ctx, l, l.Stdout, l.Stderr, WithNodes(Nodes{node}), "validate-ssh-host", cmd)
}

// cmdDebugName is the suffix of the generated ssh debug file
// If it is "", a suffix will be generated from the cmd string
func (c *SyncedCluster) newSession(
	l *logger.Logger, node Node, cmd string, options ...remoteSessionOption,
) session {
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
		res, err := c.ExecSQL(ctx, l, c.Nodes[:1], "", 0, []string{
			"-e", fmt.Sprintf("ALTER TENANT '%s' STOP SERVICE", virtualClusterName),
		})
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
	return c.Parallel(ctx, l, WithNodes(c.Nodes).WithDisplay(display), func(ctx context.Context, node Node) (*RunResultDetails, error) {
		var cmd string
		if c.IsLocal() {
			// Not all shells like brace expansion, so we'll do it here
			dirs := []string{"data*", "logs*"}
			if !preserveCerts {
				dirs = append(dirs, fmt.Sprintf("%s*", CockroachNodeCertsDir))
				dirs = append(dirs, fmt.Sprintf("%s*", CockroachNodeTenantCertsDir))
			}
			for _, dir := range dirs {
				cmd += fmt.Sprintf(`rm -fr %s/%s ;`, c.localVMDir(node), dir)
			}
		} else {
			rmCmds := []string{
				`sudo find /mnt/data* -maxdepth 1 -type f -not -name .roachprod-initialized -exec rm -f {} \;`,
				`sudo rm -fr /mnt/data*/{auxiliary,local,tmp,cassandra,cockroach,cockroach-temp*,mongo-data}`,
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

// MonitorProcessSkipped represents a cockroach process whose status
// was not checked.
type MonitorProcessSkipped struct {
	VirtualClusterName string
	SQLInstance        int
}

// MonitorProcessRunning represents the cockroach process running on a
// node.
type MonitorProcessRunning struct {
	VirtualClusterName string
	SQLInstance        int
	PID                string
}

// MonitorProcessDead represents the cockroach process dying on a node.
type MonitorProcessDead struct {
	VirtualClusterName string
	SQLInstance        int
	ExitCode           string
}

type MonitorError struct {
	Err error
}

// MonitorNoCockroachProcessesError is the error returned when the
// monitor is called on a node that is not running a `cockroach`
// process by the time the monitor runs.
var MonitorNoCockroachProcessesError = errors.New("no cockroach processes running")

// NodeMonitorInfo is a message describing a cockroach process' status.
type NodeMonitorInfo struct {
	// The index of the node (in a SyncedCluster) at which the message originated.
	Node Node
	// Event describes what happened to the node; it is one of
	// MonitorProcessSkipped (no store directory was found);
	// MonitorProcessRunning, sent when cockroach is running on a node;
	// MonitorProcessDead, when the cockroach process stops running on a
	// node; or MonitorError, typically indicate networking issues or
	// nodes that have (physically) shut down.
	Event interface{}
}

func (nmi NodeMonitorInfo) String() string {
	var status string

	virtualClusterDesc := func(name string, instance int) string {
		if name == SystemInterfaceName {
			return "system interface"
		}

		return fmt.Sprintf("virtual cluster %q, instance %d", name, instance)
	}

	switch event := nmi.Event.(type) {
	case MonitorProcessRunning:
		status = fmt.Sprintf("cockroach process for %s is running (PID: %s)",
			virtualClusterDesc(event.VirtualClusterName, event.SQLInstance), event.PID,
		)
	case MonitorProcessSkipped:
		status = fmt.Sprintf("%s was skipped", virtualClusterDesc(event.VirtualClusterName, event.SQLInstance))
	case MonitorProcessDead:
		status = fmt.Sprintf("cockroach process for %s died (exit code %s)",
			virtualClusterDesc(event.VirtualClusterName, event.SQLInstance), event.ExitCode,
		)
	case MonitorError:
		status = fmt.Sprintf("error: %s", event.Err.Error())
	}

	return fmt.Sprintf("n%d: %s", nmi.Node, status)
}

// MonitorOpts is used to pass the options needed by Monitor.
type MonitorOpts struct {
	OneShot          bool // Report the status of all targeted nodes once, then exit.
	IgnoreEmptyNodes bool // Only monitor nodes with a nontrivial data directory.
}

// Monitor writes NodeMonitorInfo for the cluster nodes to the returned channel.
// Infos sent to the channel always have the Node the event refers to, and the
// event itself. See documentation for NodeMonitorInfo for possible event types.
//
// If OneShot is true, infos are retrieved only once for each node and the
// channel is subsequently closed; otherwise the process continues indefinitely
// (emitting new information as the status of the cockroach process changes).
//
// If IgnoreEmptyNodes is true, tenants on which no CockroachDB data is found
// (in {store-dir}) will not be probed and single event, MonitorTenantSkipped,
// will be emitted for each tenant.
//
// Note that the monitor will only send events for tenants that exist
// at the time this function is called. In other words, this function
// will not emit events for tenants started *after* a call to
// Monitor().
func (c *SyncedCluster) Monitor(
	l *logger.Logger, ctx context.Context, opts MonitorOpts,
) chan NodeMonitorInfo {
	ch := make(chan NodeMonitorInfo)
	nodes := c.TargetNodes()
	var wg sync.WaitGroup
	monitorCtx, cancel := context.WithCancel(ctx)

	// sendEvent sends the NodeMonitorInfo passed through the channel
	// that is listened to by the caller. Bails if the context is
	// canceled.
	sendEvent := func(info NodeMonitorInfo) {
		// if the monitor's context is already canceled, do not attempt to
		// send the error down the channel, as it is most likely *caused*
		// by the cancelation itself.
		if monitorCtx.Err() != nil {
			return
		}

		select {
		case ch <- info:
			// We were able to send the info through the channel.
		case <-monitorCtx.Done():
			// Don't block trying to send the info.
		}
	}

	const (
		separator  = "|"
		skippedMsg = "skipped"
		runningMsg = "running"
		deadMsg    = "dead"
	)

	wg.Add(len(nodes))
	for i := range nodes {
		go func(i int) {
			defer wg.Done()
			node := nodes[i]

			// We first find out all cockroach processes that are currently
			// running in this node.
			cockroachProcessesCmd := fmt.Sprintf(`ps axeww -o command | `+
				`grep -E '%s' | `+ // processes started by roachprod
				`grep -E -o 'ROACHPROD_VIRTUAL_CLUSTER=[^ ]*' | `+ // ROACHPROD_VIRTUAL_CLUSTER indicates this is a cockroach process
				`cut -d= -f2`, // grab the virtual cluster label
				c.roachprodEnvRegex(node),
			)

			result, err := c.runCmdOnSingleNode(
				ctx, l, node, cockroachProcessesCmd, defaultCmdOpts("list-processes"),
			)
			if err := errors.CombineErrors(err, result.Err); err != nil {
				err := errors.Wrap(err, "failed to list cockroach processes")
				sendEvent(NodeMonitorInfo{Node: node, Event: MonitorError{err}})
				return
			}

			type virtualClusterInfo struct {
				Name     string
				Instance int
			}

			// Make the collection of virtual clusters a set to handle the
			// unlikely but possible case that, in `local` runs, we'll find
			// two processes associated with the same virtual cluster
			// label. This can happen if we invoke the command above while the
			// parent cockroach process already created the child,
			// background process, but has not terminated yet.
			vcs := map[virtualClusterInfo]struct{}{}
			vcLines := strings.TrimSuffix(result.CombinedOut, "\n")
			if vcLines == "" {
				sendEvent(NodeMonitorInfo{Node: node, Event: MonitorError{MonitorNoCockroachProcessesError}})
				return
			}
			for _, label := range strings.Split(vcLines, "\n") {
				name, instance, err := VirtualClusterInfoFromLabel(label)
				if err != nil {
					sendEvent(NodeMonitorInfo{Node: node, Event: MonitorError{err}})
					return
				}
				vcs[virtualClusterInfo{name, instance}] = struct{}{}
			}

			data := struct {
				OneShot     bool
				Node        Node
				IgnoreEmpty bool
				Store       string
				Local       bool
				Separator   string
				SkippedMsg  string
				RunningMsg  string
				DeadMsg     string
				Processes   []virtualClusterInfo
			}{
				OneShot:     opts.OneShot,
				Node:        node,
				IgnoreEmpty: opts.IgnoreEmptyNodes,
				Store:       c.NodeDir(node, 1 /* storeIndex */),
				Local:       c.IsLocal(),
				Separator:   separator,
				SkippedMsg:  skippedMsg,
				RunningMsg:  runningMsg,
				DeadMsg:     deadMsg,
				Processes:   maps.Keys(vcs),
			}

			storeFor := func(name string, instance int) string {
				return c.InstanceStoreDir(node, name, instance)
			}

			localPIDFile := func(name string, instance int) string {
				return filepath.Join(c.LogDir(node, name, instance), "cockroach.pid")
			}

			// NB.: we parse the output of every line this script
			// prints. Every call to `echo` must match the parsing logic
			// down below in order to produce structured results to the
			// caller.
			snippet := `
dead_parent() {
  ! ps -p "$1" >/dev/null || ps -o ucomm -p "$1" | grep -q defunct
}
{{ range .Processes }}
monitor_process_{{$.Node}}_{{.Name}}_{{.Instance}}() {
  {{ if $.IgnoreEmpty }}
  if ! ls {{storeFor .Name .Instance}}/marker.* 1> /dev/null 2>&1; then
    echo "{{.Name}}{{$.Separator}}{{.Instance}}{{$.Separator}}{{$.SkippedMsg}}"
    return 0
  fi
  {{- end}}
  # Init with -1 so that when cockroach is initially dead, we print
  # a dead event for it.
  lastpid=-1
  while :; do
    # if parent process terminated, quit as well.
    if dead_parent "$1"; then
      return 0
    fi
    {{ if $.Local }}
    pidFile=$(cat "{{pidFile .Name .Instance}}")
    # Make sure the process is still running
    pid=$(test -n "${pidFile}" && ps -p "${pidFile}" >/dev/null && echo "${pidFile}")
    pid=${pid:-0} # default to 0
    status="unknown"
    {{- else }}
    # When CRDB is not running, this is zero.
    pid=$(systemctl show "{{virtualClusterLabel .Name .Instance}}" --property MainPID --value)
    status=$(systemctl show "{{virtualClusterLabel .Name .Instance}}" --property ExecMainStatus --value)
    {{- end }}
    if [[ "${lastpid}" == -1 && "${pid}" != 0 ]]; then
      # On the first iteration through the loop, if the process is running,
      # don't register a PID change (which would trigger an erroneous dead
      # event).
      lastpid=0
    fi
    # Output a dead event whenever the PID changes from a nonzero value to
    # any other value. In particular, we emit a dead event when the node stops
    # (lastpid is nonzero, pid is zero), but not when the process then starts
    # again (lastpid is zero, pid is nonzero).
    if [ "${pid}" != "${lastpid}" ]; then
      if [ "${lastpid}" != 0 ]; then
        if [ "${pid}" != 0 ]; then
          # If the PID changed but neither is zero, then the status refers to
          # the new incarnation. We lost the actual exit status of the old PID.
          status="unknown"
        fi
    	  echo "{{.Name}}{{$.Separator}}{{.Instance}}{{$.Separator}}{{$.DeadMsg}}{{$.Separator}}${status}"
      fi
  	  if [ "${pid}" != 0 ]; then
  		  echo "{{.Name}}{{$.Separator}}{{.Instance}}{{$.Separator}}{{$.RunningMsg}}{{$.Separator}}${pid}"
      fi
      lastpid=${pid}
    fi
    {{ if $.OneShot }}
      return 0
    {{- end }}
    sleep 1
    if [ "${pid}" != 0 ]; then
      while kill -0 "${pid}" && ! dead_parent "$1"; do
        sleep 1
      done
    fi
  done
}
{{ end }}

# monitor every cockroach process in parallel.
{{ range .Processes }}
monitor_process_{{$.Node}}_{{.Name}}_{{.Instance}} $$ &
{{ end }}

wait
`

			t := template.Must(template.New("script").Funcs(template.FuncMap{
				"storeFor":            storeFor,
				"pidFile":             localPIDFile,
				"virtualClusterLabel": VirtualClusterLabel,
			}).Parse(snippet))
			var buf bytes.Buffer
			if err := t.Execute(&buf, data); err != nil {
				err := errors.Wrap(err, "failed to execute template")
				sendEvent(NodeMonitorInfo{Node: node, Event: MonitorError{err}})
				return
			}

			// This is the exception to funneling all SSH traffic through `c.runCmdOnSingleNode`
			sess := c.newSession(l, node, buf.String(), withDebugDisabled())
			defer sess.Close()

			p, err := sess.StdoutPipe()
			if err != nil {
				err := errors.Wrap(err, "failed to read stdout pipe")
				sendEvent(NodeMonitorInfo{Node: node, Event: MonitorError{err}})
				return
			}
			// Request a PTY so that the script will receive a SIGPIPE when the
			// session is closed.
			if err := sess.RequestPty(); err != nil {
				err := errors.Wrap(err, "failed to request PTY")
				sendEvent(NodeMonitorInfo{Node: node, Event: MonitorError{err}})
				return
			}

			var readerWg sync.WaitGroup
			readerWg.Add(1)
			go func(p io.Reader) {
				defer readerWg.Done()
				r := bufio.NewReader(p)
				for {
					line, _, err := r.ReadLine()
					if err == io.EOF {
						sendEvent(NodeMonitorInfo{Node: node, Event: MonitorError{err}})
						return
					}
					if err != nil {
						err := errors.Wrap(err, "error reading from session")
						sendEvent(NodeMonitorInfo{Node: node, Event: MonitorError{err}})
						return
					}

					parts := strings.Split(string(line), separator)
					ensureNumParts := func(n int) {
						if len(parts) < n {
							panic(fmt.Errorf("invalid output from monitor: %q", line))
						}
					}
					// Every event is expected to have at least 3 parts. If
					// that's not the case, panic explicitly below. Otherwise,
					// we'd get a slice out of bounds error and the error
					// message would not include the actual problematic line,
					// which would make understanding the failure more
					// difficult.
					ensureNumParts(3) // name, instance, event

					// Virtual cluster name and instance are the first fields of
					// every event type.
					name, instanceStr := parts[0], parts[1]
					instance, _ := strconv.Atoi(instanceStr)
					switch parts[2] {
					case skippedMsg:
						sendEvent(NodeMonitorInfo{Node: node, Event: MonitorProcessSkipped{
							VirtualClusterName: name, SQLInstance: instance,
						}})
					case runningMsg:
						ensureNumParts(4)
						pid := parts[3]
						sendEvent(NodeMonitorInfo{Node: node, Event: MonitorProcessRunning{
							VirtualClusterName: name, SQLInstance: instance, PID: pid,
						}})
					case deadMsg:
						ensureNumParts(4)
						exitCode := parts[3]
						sendEvent(NodeMonitorInfo{Node: node, Event: MonitorProcessDead{
							VirtualClusterName: name, SQLInstance: instance, ExitCode: exitCode,
						}})
					default:
						err := fmt.Errorf("internal error: unrecognized output from monitor: %q", line)
						sendEvent(NodeMonitorInfo{Node: node, Event: MonitorError{err}})
					}
				}
			}(p)

			if err := sess.Start(); err != nil {
				err := errors.Wrap(err, "failed to start session")
				sendEvent(NodeMonitorInfo{Node: node, Event: MonitorError{err}})
				return
			}

			// Watch for context cancellation, which can happen if the test
			// fails, or if the monitor loop exits.
			go func() {
				<-monitorCtx.Done()
				sess.Close()
			}()

			readerWg.Wait()
			// We must call `sess.Wait()` only after finishing reading from the stdout
			// pipe. Otherwise it can be closed under us, causing the reader to loop
			// infinitely receiving a non-`io.EOF` error.
			if err := sess.Wait(); err != nil {
				// If we got an error waiting for the session but the context
				// is already canceled, do not send an error through the
				// channel; context cancelation happens at the user's request
				// or when the test finishes. In either case, the monitor
				// should quiesce. Reporting the error is confusing and can be
				// spammy in the case of multiple monitors.
				if monitorCtx.Err() != nil {
					return
				}

				err := errors.Wrap(err, "failed to wait for session")
				sendEvent(NodeMonitorInfo{Node: node, Event: MonitorError{err}})
				return
			}
		}(i)
	}
	go func() {
		wg.Wait()
		cancel()
		close(ch)
	}()

	return ch
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
	stdin                   io.Reader
	stdout, stderr          io.Writer
	remoteOptions           []remoteSessionOption
}

func defaultCmdOpts(debugName string) RunCmdOptions {
	return RunCmdOptions{
		combinedOut:   true,
		remoteOptions: []remoteSessionOption{withDebugName(debugName)},
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
	expandedCmd, err := e.expand(ctx, l, c, cmd)
	if err != nil {
		return &noResult, errors.WithDetailf(err, "error expanding command: %s", cmd)
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
	_, hasError, err := c.ParallelE(ctx, l, WithNodes(c.Nodes).WithDisplay(display).WithRetryDisabled(),
		func(ctx context.Context, node Node) (*RunResultDetails, error) {
			res := &RunResultDetails{Node: node}
			var err error
			cmd := fmt.Sprintf("test -e %s", vm.DisksInitializedFile)
			opts := defaultCmdOpts("wait-init")
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
			l.Printf("  %2d: %v", node, res.Err)
			return res, nil
		})

	if err != nil {
		return err
	}

	if hasError {
		return errors.New("not all nodes booted successfully")
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
func (c *SyncedCluster) DistributeCerts(ctx context.Context, l *logger.Logger) error {
	if c.checkForCertificates(ctx, l) {
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
VERSION=${VERSION::3}
TENANT_SCOPE_OPT=""
if [[ $VERSION = v22 ]]; then
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
	return c.distributeLocalCertsTar(ctx, l, tarfile, nodes, 0)
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
	return c.distributeLocalCertsTar(ctx, l, tarfile, nodes, 0)
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
	if err := c.validateHost(ctx, l, nodes[0]); err != nil {
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
	lines := make([]string, len(nodes))
	var linesMu syncutil.Mutex
	var wg sync.WaitGroup
	wg.Add(len(nodes))

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
		dest, err := e.expand(ctx, l, c, dest)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s@%s:%s", c.user(nodes[i]), c.Host(nodes[i]), dest), nil
	}

	for i := range nodes {
		go func(i int, dest string) {
			defer wg.Done()

			if c.IsLocal() {
				// Expand the destination to allow, for example, putting directly
				// into {store-dir}.
				e := expander{
					node: nodes[i],
				}
				var err error
				dest, err = e.expand(ctx, l, c, dest)
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

	go func() {
		wg.Wait()
		close(results)
	}()

	var writer ui.Writer
	var ticker *time.Ticker
	if !config.Quiet {
		ticker = time.NewTicker(100 * time.Millisecond)
	} else {
		ticker = time.NewTicker(1000 * time.Millisecond)
	}
	defer ticker.Stop()
	var finalErr error
	setErr := func(e error) {
		if finalErr == nil {
			finalErr = e
		}
	}

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	for done := false; !done; {
		select {
		case <-ticker.C:
			if config.Quiet && l.File == nil {
				fmt.Printf(".")
			}
		case r, ok := <-results:
			done = !ok
			if ok {
				func() {
					linesMu.Lock()
					defer linesMu.Unlock()
					if r.err != nil {
						setErr(r.err)
						lines[r.index] = r.err.Error()
					} else {
						lines[r.index] = "done"
					}
				}()
			}
		}
		if !config.Quiet {
			func() {
				linesMu.Lock()
				defer linesMu.Unlock()
				for i := range lines {
					fmt.Fprintf(&writer, "  %2d: ", nodes[i])
					if lines[i] != "" {
						fmt.Fprintf(&writer, "%s", lines[i])
					} else {
						fmt.Fprintf(&writer, "%s", spinner[spinnerIdx%len(spinner)])
					}
					fmt.Fprintf(&writer, "\n")
				}
			}()
			_ = writer.Flush(l.Stdout)
			spinnerIdx++
		}
	}

	if config.Quiet && l.File != nil {
		l.Printf("\n")
		func() {
			linesMu.Lock()
			defer linesMu.Unlock()
			for i := range lines {
				l.Printf("  %2d: %s", nodes[i], lines[i])
			}
		}()
	}

	if finalErr != nil {
		return errors.Wrapf(finalErr, "put %q failed", src)
	}
	return nil
}

// Logs will sync the logs from c to dest with each nodes logs under dest in
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
// <user> allows retrieval of logs from a roachprod cluster being run by another
// user and assumes that the current user used to create c has the ability to
// sudo into <user>.
// TODO(herko): This command does not support virtual clusters yet.
func (c *SyncedCluster) Logs(
	l *logger.Logger,
	src, dest, user, filter, programFilter string,
	interval time.Duration,
	from, to time.Time,
	out io.Writer,
) error {
	if err := c.validateHost(context.TODO(), l, c.Nodes[0]); err != nil {
		return err
	}

	rsyncNodeLogs := func(ctx context.Context, node Node) error {
		base := fmt.Sprintf("%d.logs", node)
		local := filepath.Join(dest, base) + "/"
		sshUser := c.user(node)
		rsyncArgs := []string{"-az", "--size-only"}
		var remote string
		if c.IsLocal() {
			// This here is a bit of a hack to guess that the parent of the log dir is
			// the "home" for the local node and that the srcBase is relative to that.
			localHome := filepath.Dir(c.LogDir(node, "", 0))
			remote = filepath.Join(localHome, src) + "/"
		} else {
			logDir := src
			if !filepath.IsAbs(logDir) && user != "" && user != sshUser {
				logDir = "~" + user + "/" + logDir
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
			// Use rsync-path flag to sudo into user if different from sshUser.
			if user != "" && user != sshUser {
				rsyncArgs = append(rsyncArgs, "--rsync-path",
					fmt.Sprintf("sudo -u %s rsync", user))
			}
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
	if err := c.validateHost(context.TODO(), l, nodes[0]); err != nil {
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

	var writer ui.Writer
	results := make(chan result, len(nodes))
	lines := make([]string, len(nodes))
	var linesMu syncutil.Mutex

	var wg sync.WaitGroup
	for i := range nodes {
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
			src, err := e.expand(ctx, l, c, src)
			if err != nil {
				results <- result{i, err}
				return
			}

			progress := func(p float64) {
				linesMu.Lock()
				defer linesMu.Unlock()
				lines[i] = formatProgress(p)
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

	var ticker *time.Ticker
	if config.Quiet {
		ticker = time.NewTicker(100 * time.Millisecond)
	} else {
		ticker = time.NewTicker(1000 * time.Millisecond)
	}
	defer ticker.Stop()
	var finalErr error
	setErr := func(e error) {
		if finalErr == nil {
			finalErr = e
		}
	}

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	for done := false; !done; {
		select {
		case <-ticker.C:
			if config.Quiet && l.File == nil {
				fmt.Printf(".")
			}
		case r, ok := <-results:
			done = !ok
			if ok {
				func() {
					linesMu.Lock()
					defer linesMu.Unlock()
					if r.err != nil {
						setErr(r.err)
						lines[r.index] = r.err.Error()
					} else {
						lines[r.index] = "done"
					}
				}()
			}
		}
		if !config.Quiet && l.File == nil {
			func() {
				linesMu.Lock()
				defer linesMu.Unlock()
				for i := range lines {
					fmt.Fprintf(&writer, "  %2d: ", nodes[i])
					if lines[i] != "" {
						fmt.Fprintf(&writer, "%s", lines[i])
					} else {
						fmt.Fprintf(&writer, "%s", spinner[spinnerIdx%len(spinner)])
					}
					fmt.Fprintf(&writer, "\n")
				}
			}()
			_ = writer.Flush(l.Stdout)
			spinnerIdx++
		}
	}

	if config.Quiet && l.File != nil {
		func() {
			linesMu.Lock()
			defer linesMu.Unlock()
			for i := range lines {
				l.Printf("  %2d: %s", nodes[i], lines[i])
			}
		}()
	}

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
		m[node] = c.NodeURL(host, desc.Port, virtualClusterName, desc.ServiceMode, AuthUserCert)
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

// SSH creates an interactive shell connecting the caller to the first
// node on the cluster (or to all nodes in an iterm2 split screen if
// supported).
//
// CAUTION: this script will `exec` the ssh utility, so it must not be
// used in any roachtest code. This is for use within `./roachprod`
// exclusively.
func (c *SyncedCluster) SSH(ctx context.Context, l *logger.Logger, sshArgs, args []string) error {
	targetNode := c.Nodes[0]
	if err := c.validateHost(ctx, l, targetNode); err != nil {
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
	for _, arg := range args {
		expandedArg, err := e.expand(ctx, l, c, arg)
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

	var writer ui.Writer
	out := l.Stdout
	if options.Display == "" {
		out = io.Discard
	}

	var ticker *time.Ticker
	if !config.Quiet {
		ticker = time.NewTicker(100 * time.Millisecond)
	} else {
		ticker = time.NewTicker(1000 * time.Millisecond)
		fmt.Fprintf(out, "%s", options.Display)
		if l.File != nil {
			fmt.Fprintf(out, "\n")
		}
	}
	defer ticker.Stop()

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	var hasError bool
	n := 0
	results := make([]*RunResultDetails, count)
	for done := false; !done; {
		select {
		case <-ticker.C:
			if config.Quiet && l.File == nil {
				fmt.Fprintf(out, ".")
			}
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

		if !config.Quiet && l.File == nil {
			fmt.Fprint(&writer, options.Display)
			fmt.Fprintf(&writer, " %d/%d", n, count)
			if !done {
				fmt.Fprintf(&writer, " %s", spinner[spinnerIdx%len(spinner)])
			}
			fmt.Fprintf(&writer, "\n")
			_ = writer.Flush(out)
			spinnerIdx++
		}
	}

	if config.Quiet && l.File == nil {
		fmt.Fprintf(out, "\n")
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
