// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/config"
	rperrors "github.com/cockroachdb/cockroach/pkg/cmd/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/ui"
	clog "github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// ClusterImpl TODO(peter): document
type ClusterImpl interface {
	Start(c *SyncedCluster, extraArgs []string)
	CertsDir(c *SyncedCluster, index int) string
	NodeDir(c *SyncedCluster, index, storeIndex int) string
	LogDir(c *SyncedCluster, index int) string
	NodeURL(c *SyncedCluster, host string, port int) string
	NodePort(c *SyncedCluster, index int) int
	NodeUIPort(c *SyncedCluster, index int) int
}

// A SyncedCluster is created from the information in the synced hosts file
// and is used as the target for installing and managing various software
// components.
//
// TODO(benesch): unify with CloudCluster.
type SyncedCluster struct {
	// name, vms, users, localities are populated at init time.
	Name       string
	VMs        []string
	Users      []string
	Localities []string
	VPCs       []string
	// all other fields are populated in newCluster.
	Nodes          []int
	Secure         bool
	Env            string
	Args           []string
	Tag            string
	Impl           ClusterImpl
	UseTreeDist    bool
	Quiet          bool
	MaxConcurrency int // used in Parallel
	// AuthorizedKeys is used by SetupSSH to add additional authorized keys.
	AuthorizedKeys []byte

	// Used to stash debug information.
	DebugDir string
}

func (c *SyncedCluster) host(index int) string {
	return c.VMs[index-1]
}

func (c *SyncedCluster) user(index int) string {
	return c.Users[index-1]
}

func (c *SyncedCluster) locality(index int) string {
	return c.Localities[index-1]
}

// IsLocal TODO(peter): document
//
// TODO(tschottdorf): roachprod should cleanly encapsulate the home directory
// which is currently the biggest culprit for awkward one-offs.
func (c *SyncedCluster) IsLocal() bool {
	return c.Name == config.Local
}

// ServerNodes is the fully expanded, ordered list of nodes that any given
// roachprod command is intending to target.
//
//  $ roachprod create local -n 4
//  $ roachprod start local          # [1, 2, 3, 4]
//  $ roachprod start local:2-4      # [2, 3, 4]
//  $ roachprod start local:2,1,4    # [1, 2, 4]
func (c *SyncedCluster) ServerNodes() []int {
	return append([]int{}, c.Nodes...)
}

// GetInternalIP returns the internal IP address of the specified node.
func (c *SyncedCluster) GetInternalIP(index int) (string, error) {
	if c.IsLocal() {
		return c.host(index), nil
	}

	session, err := c.newSession(index)
	if err != nil {
		return "", errors.Wrapf(err, "GetInternalIP: failed dial %s:%d", c.Name, index)
	}
	defer session.Close()

	var stdout, stderr strings.Builder
	session.SetStdout(&stdout)
	session.SetStderr(&stderr)
	cmd := `hostname --all-ip-addresses`
	if err := session.Run(cmd); err != nil {
		return "", errors.Wrapf(err,
			"GetInternalIP: failed to execute hostname on %s:%d:\n(stdout) %s\n(stderr) %s",
			c.Name, index, stdout.String(), stderr.String())
	}
	ip := strings.TrimSpace(stdout.String())
	if ip == "" {
		return "", errors.Errorf(
			"empty internal IP returned, stdout:\n%s\nstderr:\n%s",
			stdout.String(), stderr.String(),
		)
	}
	return ip, nil
}

// Start TODO(peter): document
func (c *SyncedCluster) Start() {
	c.Impl.Start(c, c.Args)
}

func (c *SyncedCluster) newSession(i int) (session, error) {
	if c.IsLocal() {
		return newLocalSession(), nil
	}
	return newRemoteSession(c.user(i), c.host(i), c.DebugDir)
}

// Stop is used to stop cockroach on all nodes in the cluster.
//
// It sends a signal to all processes that have been started with ROACHPROD env
// var and optionally waits until the processes stop.
//
// When running roachprod stop without other flags, the signal is 9 (SIGKILL)
// and wait is true.
func (c *SyncedCluster) Stop(sig int, wait bool) {
	display := fmt.Sprintf("%s: stopping", c.Name)
	if wait {
		display += " and waiting"
	}
	c.Parallel(display, len(c.Nodes), 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(c.Nodes[i])
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		var waitCmd string
		if wait {
			waitCmd = fmt.Sprintf(`
  for pid in ${pids}; do
    echo "${pid}: checking" >> %[1]s/roachprod.log
    while kill -0 ${pid}; do
      kill -0 ${pid} >> %[1]s/roachprod.log 2>&1
      echo "${pid}: still alive [$?]" >> %[1]s/roachprod.log
      ps axeww -o pid -o command >> %[1]s/roachprod.log
      sleep 1
    done
    echo "${pid}: dead" >> %[1]s/roachprod.log
  done`,
				c.Impl.LogDir(c, c.Nodes[i]), // [1]
			)
		}

		// NB: the awkward-looking `awk` invocation serves to avoid having the
		// awk process match its own output from `ps`.
		cmd := fmt.Sprintf(`
mkdir -p %[1]s
echo ">>> roachprod stop: $(date)" >> %[1]s/roachprod.log
ps axeww -o pid -o command >> %[1]s/roachprod.log
pids=$(ps axeww -o pid -o command | \
  sed 's/export ROACHPROD=//g' | \
  awk '/ROACHPROD=(%[2]d%[3]s)[ \/]/ { print $1 }')
if [ -n "${pids}" ]; then
  kill -%[4]d ${pids}
%[5]s
fi`,
			c.Impl.LogDir(c, c.Nodes[i]), // [1]
			c.Nodes[i],                   // [2]
			c.escapedTag(),               // [3]
			sig,                          // [4]
			waitCmd,                      // [5]
		)
		return sess.CombinedOutput(cmd)
	})
}

// Wipe TODO(peter): document
func (c *SyncedCluster) Wipe(preserveCerts bool) {
	display := fmt.Sprintf("%s: wiping", c.Name)
	c.Stop(9, true /* wait */)
	c.Parallel(display, len(c.Nodes), 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(c.Nodes[i])
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		var cmd string
		if c.IsLocal() {
			// Not all shells like brace expansion, so we'll do it here
			dirs := []string{"data", "logs"}
			if !preserveCerts {
				dirs = append(dirs, "certs*")
			}
			for _, dir := range dirs {
				cmd += fmt.Sprintf(`rm -fr ${HOME}/local/%d/%s ;`, c.Nodes[i], dir)
			}
		} else {
			cmd = `sudo find /mnt/data* -maxdepth 1 -type f -exec rm -f {} \; &&
sudo rm -fr /mnt/data*/{auxiliary,local,tmp,cassandra,cockroach,cockroach-temp*,mongo-data} &&
sudo rm -fr logs &&
`
			if !preserveCerts {
				cmd += "sudo rm -fr certs* ;\n"
			}
		}
		return sess.CombinedOutput(cmd)
	})
}

// Status TODO(peter): document
func (c *SyncedCluster) Status() {
	display := fmt.Sprintf("%s: status", c.Name)
	results := make([]string, len(c.Nodes))
	c.Parallel(display, len(c.Nodes), 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(c.Nodes[i])
		if err != nil {
			results[i] = err.Error()
			return nil, nil
		}
		defer sess.Close()

		binary := cockroachNodeBinary(c, c.Nodes[i])
		cmd := fmt.Sprintf(`out=$(ps axeww -o pid -o ucomm -o command | \
  sed 's/export ROACHPROD=//g' | \
  awk '/ROACHPROD=(%d%s)[ \/]/ {print $2, $1}'`,
			c.Nodes[i], c.escapedTag())
		cmd += ` | sort | uniq);
vers=$(` + binary + ` version 2>/dev/null | awk '/Build Tag:/ {print $NF}')
if [ -n "${out}" -a -n "${vers}" ]; then
  echo ${out} | sed "s/cockroach/cockroach-${vers}/g"
else
  echo ${out}
fi
`
		out, err := sess.CombinedOutput(cmd)
		var msg string
		if err != nil {
			return nil, errors.Wrapf(err, "~ %s\n%s", cmd, out)
		}
		msg = strings.TrimSpace(string(out))
		if msg == "" {
			msg = "not running"
		}
		results[i] = msg
		return nil, nil
	})

	for i, r := range results {
		fmt.Printf("  %2d: %s\n", c.Nodes[i], r)
	}
}

// NodeMonitorInfo is a message describing a cockroach process' status.
type NodeMonitorInfo struct {
	// The index of the node (in a SyncedCluster) at which the message originated.
	Index int
	// A message about the node. This is either a PID, "dead", "nc exited", or
	// "skipped".
	// Anything but a PID or "skipped" is an indication that there is some
	// problem with the node and that the process is not running.
	Msg string
	// Err is an error that may occur when trying to probe the status of the node.
	// If Err is non-nil, Msg is empty. After an error is returned, the node with
	// the given index will no longer be probed. Errors typically indicate networking
	// issues or nodes that have (physically) shut down.
	Err error
}

// Monitor writes NodeMonitorInfo for the cluster nodes to the returned channel.
// Infos sent to the channel always have the Index and exactly one of Msg or Err
// set.
//
// If oneShot is true, infos are retrieved only once for each node and the
// channel is subsequently closed; otherwise the process continues indefinitely
// (emitting new information as the status of the cockroach process changes).
//
// If ignoreEmptyNodes is true, nodes on which no CockroachDB data is found
// (in {store-dir}) will not be probed and single message, "skipped", will
// be emitted for them.
func (c *SyncedCluster) Monitor(ignoreEmptyNodes bool, oneShot bool) chan NodeMonitorInfo {
	ch := make(chan NodeMonitorInfo)
	nodes := c.ServerNodes()
	var wg sync.WaitGroup

	for i := range nodes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sess, err := c.newSession(nodes[i])
			if err != nil {
				ch <- NodeMonitorInfo{Index: nodes[i], Err: err}
				wg.Done()
				return
			}
			defer sess.Close()

			p, err := sess.StdoutPipe()
			if err != nil {
				ch <- NodeMonitorInfo{Index: nodes[i], Err: err}
				wg.Done()
				return
			}

			// On each monitored node, we loop looking for a cockroach process.
			data := struct {
				OneShot     bool
				IgnoreEmpty bool
				Store       string
				Port        int
				Local       bool
			}{
				OneShot:     oneShot,
				IgnoreEmpty: ignoreEmptyNodes,
				Store:       Cockroach{}.NodeDir(c, nodes[i], 1 /* storeIndex */),
				Port:        Cockroach{}.NodePort(c, nodes[i]),
				Local:       c.IsLocal(),
			}

			snippet := `
{{ if .IgnoreEmpty }}
if [ ! -f "{{.Store}}/CURRENT" ]; then
  echo "skipped"
  exit 0
fi
{{- end}}

# Init with -1 so that when cockroach is initially dead, we print
# a dead event for it.
lastpid=-1
while :; do
{{ if .Local }}
  pid=$(lsof -i :{{.Port}} -sTCP:LISTEN | awk '!/COMMAND/ {print $2}')
	pid=${pid:-0} # default to 0
	status="unknown"
{{- else }}
  # When CRDB is not running, this is zero.
	pid=$(systemctl show cockroach --property MainPID --value)
	status=$(systemctl show cockroach --property ExecMainStatus --value)
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
    	echo "dead (exit status ${status})"
    fi
		if [ "${pid}" != 0 ]; then
			echo "${pid}"
    fi
    lastpid=${pid}
  fi

{{ if .OneShot }}
  exit 0
{{- end }}

  sleep 1
  if [ "${pid}" != 0 ]; then
    while kill -0 "${pid}"; do
      sleep 1
    done
  fi
done
`

			t := template.Must(template.New("script").Parse(snippet))
			var buf bytes.Buffer
			if err := t.Execute(&buf, data); err != nil {
				ch <- NodeMonitorInfo{Index: nodes[i], Err: err}
				return
			}

			// Request a PTY so that the script will receive a SIGPIPE when the
			// session is closed.
			if err := sess.RequestPty(); err != nil {
				ch <- NodeMonitorInfo{Index: nodes[i], Err: err}
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
						return
					}
					ch <- NodeMonitorInfo{Index: nodes[i], Msg: string(line)}
				}
			}(p)

			if err := sess.Start(buf.String()); err != nil {
				ch <- NodeMonitorInfo{Index: nodes[i], Err: err}
				return
			}

			readerWg.Wait()
			// We must call `sess.Wait()` only after finishing reading from the stdout
			// pipe. Otherwise it can be closed under us, causing the reader to loop
			// infinitely receiving a non-`io.EOF` error.
			if err := sess.Wait(); err != nil {
				ch <- NodeMonitorInfo{Index: nodes[i], Err: err}
				return
			}
		}(i)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	return ch
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
func (c *SyncedCluster) Run(stdout, stderr io.Writer, nodes []int, title, cmd string) error {
	// Stream output if we're running the command on only 1 node.
	stream := len(nodes) == 1
	var display string
	if !stream {
		display = fmt.Sprintf("%s: %s", c.Name, title)
	}

	errs := make([]error, len(nodes))
	results := make([]string, len(nodes))
	c.Parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(nodes[i])
		if err != nil {
			errs[i] = err
			results[i] = err.Error()
			return nil, nil
		}
		defer sess.Close()

		// Argument template expansion is node specific (e.g. for {store-dir}).
		e := expander{
			node: nodes[i],
		}
		expandedCmd, err := e.expand(c, cmd)
		if err != nil {
			return nil, err
		}

		// Be careful about changing these command strings. In particular, we need
		// to support running commands in the background on both local and remote
		// nodes. For example:
		//
		//   roachprod run cluster -- "sleep 60 &> /dev/null < /dev/null &"
		//
		// That command should return immediately. And a "roachprod status" should
		// reveal that the sleep command is running on the cluster.
		nodeCmd := fmt.Sprintf(`export ROACHPROD=%d%s GOTRACEBACK=crash && bash -c %s`,
			nodes[i], c.Tag, ssh.Escape1(expandedCmd))
		if c.IsLocal() {
			nodeCmd = fmt.Sprintf("cd ${HOME}/local/%d ; %s", nodes[i], nodeCmd)
		}

		if stream {
			sess.SetStdout(stdout)
			sess.SetStderr(stderr)
			errs[i] = sess.Run(nodeCmd)
			if errs[i] != nil {
				detailMsg := fmt.Sprintf("Node %d. Command with error:\n```\n%s\n```\n", nodes[i], cmd)
				err = errors.WithDetail(errs[i], detailMsg)
				err = rperrors.ClassifyCmdError(err)
				errs[i] = err
			}
			return nil, nil
		}

		out, err := sess.CombinedOutput(nodeCmd)
		msg := strings.TrimSpace(string(out))
		if err != nil {
			detailMsg := fmt.Sprintf("Node %d. Command with error:\n```\n%s\n```\n", nodes[i], cmd)
			err = errors.WithDetail(err, detailMsg)
			err = rperrors.ClassifyCmdError(err)
			errs[i] = err
			msg += fmt.Sprintf("\n%v", err)
		}
		results[i] = msg
		return nil, nil
	})

	if !stream {
		for i, r := range results {
			fmt.Fprintf(stdout, "  %2d: %s\n", nodes[i], r)
		}
	}

	return rperrors.SelectPriorityError(errs)
}

// Wait TODO(peter): document
func (c *SyncedCluster) Wait() error {
	display := fmt.Sprintf("%s: waiting for nodes to start", c.Name)
	errs := make([]error, len(c.Nodes))
	c.Parallel(display, len(c.Nodes), 0, func(i int) ([]byte, error) {
		for j := 0; j < 600; j++ {
			sess, err := c.newSession(c.Nodes[i])
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			defer sess.Close()

			_, err = sess.CombinedOutput("test -e /mnt/data1/.roachprod-initialized")
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			return nil, nil
		}
		errs[i] = errors.New("timed out after 5m")
		return nil, nil
	})

	var foundErr bool
	for i, err := range errs {
		if err != nil {
			fmt.Printf("  %2d: %v\n", c.Nodes[i], err)
			foundErr = true
		}
	}
	if foundErr {
		return errors.New("not all nodes booted successfully")
	}
	return nil
}

// SetupSSH configures the cluster for use with SSH. This is generally run after
// the cloud.Cluster has been synced which resets the SSH credentials on the
// machines and sets them up for the current user. This method enables the
// hosts to talk to eachother and optionally confiures additional keys to be
// added to the hosts via the c.AuthorizedKeys field. It does so in the following
// steps:
//
//   1. Creates an ssh key pair on the first host to be used on all hosts if
//      none exists.
//   2. Distributes the public key, private key, and authorized_keys file from
//      the first host to the others.
//   3. Merges the data in c.AuthorizedKeys with the existing authorized_keys
//      files on all hosts.
//
// This call strives to be idempotent.
func (c *SyncedCluster) SetupSSH() error {
	if c.IsLocal() {
		return nil
	}

	if len(c.Nodes) == 0 || len(c.Users) == 0 || len(c.VMs) == 0 {
		return fmt.Errorf("%s: invalid cluster: nodes=%d users=%d hosts=%d",
			c.Name, len(c.Nodes), len(c.Users), len(c.VMs))
	}

	// Generate an ssh key that we'll distribute to all of the nodes in the
	// cluster in order to allow inter-node ssh.
	var sshTar []byte
	c.Parallel("generating ssh key", 1, 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(1)
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		// Create the ssh key and then tar up the public, private and
		// authorized_keys files and output them to stdout. We'll take this output
		// and pipe it back into tar on the other nodes in the cluster.
		cmd := `
test -f .ssh/id_rsa || \
  (ssh-keygen -q -f .ssh/id_rsa -t rsa -N '' && \
   cat .ssh/id_rsa.pub >> .ssh/authorized_keys);
tar cf - .ssh/id_rsa .ssh/id_rsa.pub .ssh/authorized_keys
`

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		sess.SetStdout(&stdout)
		sess.SetStderr(&stderr)

		if err := sess.Run(cmd); err != nil {
			return nil, errors.Wrapf(err, "%s: stderr:\n%s", cmd, stderr.String())
		}
		sshTar = stdout.Bytes()
		return nil, nil
	})

	// Skip the first node which is where we generated the key.
	nodes := c.Nodes[1:]
	c.Parallel("distributing ssh key", len(nodes), 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(nodes[i])
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		sess.SetStdin(bytes.NewReader(sshTar))
		cmd := `tar xf -`
		if out, err := sess.CombinedOutput(cmd); err != nil {
			return nil, errors.Wrapf(err, "%s: output:\n%s", cmd, out)
		}
		return nil, nil
	})

	// Populate the known_hosts file with both internal and external IPs of all
	// of the nodes in the cluster. Note that as a side effect, this creates the
	// known hosts file in unhashed format, working around a limitation of jsch
	// (which is used in jepsen tests).
	ips := make([]string, len(c.Nodes), len(c.Nodes)*2)
	c.Parallel("retrieving hosts", len(c.Nodes), 0, func(i int) ([]byte, error) {
		for j := 0; j < 20 && ips[i] == ""; j++ {
			var err error
			ips[i], err = c.GetInternalIP(c.Nodes[i])
			if err != nil {
				return nil, errors.Wrapf(err, "pgurls")
			}
			time.Sleep(time.Second)
		}
		if ips[i] == "" {
			return nil, fmt.Errorf("retrieved empty IP address")
		}
		return nil, nil
	})
	for _, i := range c.Nodes {
		ips = append(ips, c.host(i))
	}
	var knownHostsData []byte
	c.Parallel("scanning hosts", 1, 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(c.Nodes[i])
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		// ssh-keyscan may return fewer than the desired number of entries if the
		// remote nodes are not responding yet, so we loop until we have a scan that
		// found host keys for all of the IPs. Merge the newly scanned keys with the
		// existing list to make this process idempotent.
		cmd := `
set -e
tmp="$(tempfile -d ~/.ssh -p 'roachprod' )"
on_exit() {
    rm -f "${tmp}"
}
trap on_exit EXIT
for i in {1..20}; do
  ssh-keyscan -T 60 -t rsa ` + strings.Join(ips, " ") + ` > "${tmp}"
  if [[ "$(wc < ${tmp} -l)" -eq "` + fmt.Sprint(len(ips)) + `" ]]; then
    [[ -f .ssh/known_hosts ]] && cat .ssh/known_hosts >> "${tmp}"
    sort -u < "${tmp}"
    exit 0
  fi
  sleep 1
done
exit 1
`
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		sess.SetStdout(&stdout)
		sess.SetStderr(&stderr)
		if err := sess.Run(cmd); err != nil {
			return nil, errors.Wrapf(err, "%s: stderr:\n%s", cmd, stderr.String())
		}
		knownHostsData = stdout.Bytes()
		return nil, nil
	})
	c.Parallel("distributing known_hosts", len(c.Nodes), 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(c.Nodes[i])
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		sess.SetStdin(bytes.NewReader(knownHostsData))
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
		if out, err := sess.CombinedOutput(cmd); err != nil {
			return nil, errors.Wrapf(err, "%s: output:\n%s", cmd, out)
		}
		return nil, nil
	})
	if len(c.AuthorizedKeys) > 0 {
		// When clusters are created using cloud APIs they only have a subset of
		// desired keys installed on a subset of users. This code distributes
		// additional authorized_keys to both the current user (your username on
		// gce and the shared user on aws) as well as to the shared user on both
		// platforms.
		c.Parallel("adding additional authorized keys", len(c.Nodes), 0, func(i int) ([]byte, error) {
			sess, err := c.newSession(c.Nodes[i])
			if err != nil {
				return nil, err
			}
			defer sess.Close()

			sess.SetStdin(bytes.NewReader(c.AuthorizedKeys))
			const cmd = `
keys_data="$(cat)"
set -e
tmp1="$(tempfile -d ~/.ssh -p 'roachprod' )"
tmp2="$(tempfile -d ~/.ssh -p 'roachprod' )"
on_exit() {
    rm -f "${tmp1}" "${tmp2}"
}
trap on_exit EXIT
if [[ -f ~/.ssh/authorized_keys ]]; then
    cat ~/.ssh/authorized_keys > "${tmp1}"
fi
echo "${keys_data}" >> "${tmp1}"
sort -u < "${tmp1}" > "${tmp2}"
install --mode 0600 "${tmp2}" ~/.ssh/authorized_keys
if [[ "$(whoami)" != "` + config.SharedUser + `" ]]; then
    sudo install --mode 0600 \
        --owner ` + config.SharedUser + `\
        --group ` + config.SharedUser + `\
        "${tmp2}" ~` + config.SharedUser + `/.ssh/authorized_keys
fi
`
			if out, err := sess.CombinedOutput(cmd); err != nil {
				return nil, errors.Wrapf(err, "~ %s\n%s", cmd, out)
			}
			return nil, nil
		})
	}

	return nil
}

// DistributeCerts will generate and distribute certificates to all of the
// nodes.
func (c *SyncedCluster) DistributeCerts() {
	dir := ""
	if c.IsLocal() {
		dir = `${HOME}/local/1`
	}

	// Check to see if the certs have already been initialized.
	var existsErr error
	display := fmt.Sprintf("%s: checking certs", c.Name)
	c.Parallel(display, 1, 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(1)
		if err != nil {
			return nil, err
		}
		defer sess.Close()
		_, existsErr = sess.CombinedOutput(`test -e ` + filepath.Join(dir, `certs.tar`))
		return nil, nil
	})

	if existsErr == nil {
		return
	}

	// Gather the internal IP addresses for every node in the cluster, even
	// if it won't be added to the cluster itself we still add the IP address
	// to the node cert.
	var msg string
	display = fmt.Sprintf("%s: initializing certs", c.Name)
	nodes := allNodes(len(c.VMs))
	var ips []string
	if !c.IsLocal() {
		ips = make([]string, len(nodes))
		c.Parallel("", len(nodes), 0, func(i int) ([]byte, error) {
			var err error
			ips[i], err = c.GetInternalIP(nodes[i])
			return nil, errors.Wrapf(err, "IPs")
		})
	}

	// Generate the ca, client and node certificates on the first node.
	c.Parallel(display, 1, 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(1)
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		var nodeNames []string
		if c.IsLocal() {
			// For local clusters, we only need to add one of the VM IP addresses.
			nodeNames = append(nodeNames, "$(hostname)", c.VMs[0])
		} else {
			// Add both the local and external IP addresses, as well as the
			// hostnames to the node certificate.
			nodeNames = append(nodeNames, ips...)
			nodeNames = append(nodeNames, c.VMs...)
			for i := range c.VMs {
				nodeNames = append(nodeNames, fmt.Sprintf("%s-%04d", c.Name, i+1))
				// On AWS nodes internally have a DNS name in the form ip-<ip address>
				// where dots have been replaces with dashes.
				// See https://docs.aws.amazon.com/vpc/latest/userguide/vpc-dns.html#vpc-dns-hostnames
				if strings.Contains(c.Localities[i], "cloud=aws") {
					nodeNames = append(nodeNames, "ip-"+strings.ReplaceAll(ips[i], ".", "-"))
				}
			}
		}

		var cmd string
		if c.IsLocal() {
			cmd = `cd ${HOME}/local/1 ; `
		}
		cmd += fmt.Sprintf(`
rm -fr certs
mkdir -p certs
%[1]s cert create-ca --certs-dir=certs --ca-key=certs/ca.key
%[1]s cert create-client root --certs-dir=certs --ca-key=certs/ca.key
%[1]s cert create-node localhost %[2]s --certs-dir=certs --ca-key=certs/ca.key
tar cvf certs.tar certs
`, cockroachNodeBinary(c, 1), strings.Join(nodeNames, " "))
		if out, err := sess.CombinedOutput(cmd); err != nil {
			msg = fmt.Sprintf("%s: %v", out, err)
		}
		return nil, nil
	})

	if msg != "" {
		fmt.Fprintln(os.Stderr, msg)
		os.Exit(1)
	}

	var tmpfileName string
	if c.IsLocal() {
		tmpfileName = os.ExpandEnv(filepath.Join(dir, "certs.tar"))
	} else {
		// Retrieve the certs.tar that was created on the first node.
		tmpfile, err := ioutil.TempFile("", "certs")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		_ = tmpfile.Close()
		defer func() {
			_ = os.Remove(tmpfile.Name()) // clean up
		}()

		if err := func() error {
			return c.scp(fmt.Sprintf("%s@%s:certs.tar", c.user(1), c.host(1)), tmpfile.Name())
		}(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		tmpfileName = tmpfile.Name()
	}

	// Read the certs.tar file we just downloaded. We'll be piping it to the
	// other nodes in the cluster.
	certsTar, err := ioutil.ReadFile(tmpfileName)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Skip the first node which is where we generated the certs.
	display = c.Name + ": distributing certs"
	nodes = nodes[1:]
	c.Parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(nodes[i])
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		sess.SetStdin(bytes.NewReader(certsTar))
		var cmd string
		if c.IsLocal() {
			cmd = fmt.Sprintf(`cd ${HOME}/local/%d ; `, nodes[i])
		}
		cmd += `tar xf -`
		if out, err := sess.CombinedOutput(cmd); err != nil {
			return nil, errors.Wrapf(err, "~ %s\n%s", cmd, out)
		}
		return nil, nil
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

// Put TODO(peter): document
func (c *SyncedCluster) Put(src, dest string) {
	// NB: This value was determined with a few experiments. Higher values were
	// not tested.
	const treeDistFanout = 10

	var detail string
	if !c.IsLocal() {
		if c.UseTreeDist {
			detail = " (dist)"
		} else {
			detail = " (scp)"
		}
	}
	fmt.Printf("%s: putting%s %s %s\n", c.Name, detail, src, dest)

	type result struct {
		index int
		err   error
	}

	results := make(chan result, len(c.Nodes))
	lines := make([]string, len(c.Nodes))
	var linesMu syncutil.Mutex
	var wg sync.WaitGroup
	wg.Add(len(c.Nodes))

	// Each destination for the copy needs a source to copy from. We create a
	// channel that has capacity for each destination. If we try to add a source
	// and the channel is full we can simply drop that source as we know we won't
	// need to use it.
	sources := make(chan int, len(c.Nodes))
	pushSource := func(i int) {
		select {
		case sources <- i:
		default:
		}
	}

	if c.UseTreeDist {
		// In treedist mode, only add the local source initially.
		pushSource(-1)
	} else {
		// In non-treedist mode, add the local source N times (once for each
		// destination).
		for range c.Nodes {
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
			node: c.Nodes[i],
		}
		dest, err := e.expand(c, dest)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s@%s:%s", c.user(c.Nodes[i]), c.host(c.Nodes[i]), dest), nil
	}

	for i := range c.Nodes {
		go func(i int, dest string) {
			defer wg.Done()

			if c.IsLocal() {
				// Expand the destination to allow, for example, putting directly
				// into {store-dir}.
				e := expander{
					node: c.Nodes[i],
				}
				var err error
				dest, err = e.expand(c, dest)
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
					to = fmt.Sprintf(os.ExpandEnv("${HOME}/local/%d/%s"), c.Nodes[i], dest)
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

			err = c.scp(from, to)
			results <- result{i, err}

			if err != nil {
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
	if !c.Quiet {
		ticker = time.NewTicker(100 * time.Millisecond)
	} else {
		ticker = time.NewTicker(1000 * time.Millisecond)
	}
	defer ticker.Stop()
	haveErr := false

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	for done := false; !done; {
		select {
		case <-ticker.C:
			if c.Quiet {
				fmt.Printf(".")
			}
		case r, ok := <-results:
			done = !ok
			if ok {
				linesMu.Lock()
				if r.err != nil {
					haveErr = true
					lines[r.index] = r.err.Error()
				} else {
					lines[r.index] = "done"
				}
				linesMu.Unlock()
			}
		}
		if !c.Quiet {
			linesMu.Lock()
			for i := range lines {
				fmt.Fprintf(&writer, "  %2d: ", c.Nodes[i])
				if lines[i] != "" {
					fmt.Fprintf(&writer, "%s", lines[i])
				} else {
					fmt.Fprintf(&writer, "%s", spinner[spinnerIdx%len(spinner)])
				}
				fmt.Fprintf(&writer, "\n")
			}
			linesMu.Unlock()
			_ = writer.Flush(os.Stdout)
			spinnerIdx++
		}
	}

	if c.Quiet {
		fmt.Printf("\n")
		linesMu.Lock()
		for i := range lines {
			fmt.Printf("  %2d: %s\n", c.Nodes[i], lines[i])
		}
		linesMu.Unlock()
	}

	if haveErr {
		log.Fatalf("put %s failed", src)
	}
}

// Logs will sync the logs from c to dest with each nodes logs under dest in
// directories per node and stream the merged logs to out.
// For example, if dest is "tpcc-test.logs" then the logs for each node will be
// stored like:
//
//  tpcc-test.logs/1.logs/...
//  tpcc-test.logs/2.logs/...
//  ...
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
func (c *SyncedCluster) Logs(
	src, dest, user, filter, programFilter string,
	interval time.Duration,
	from, to time.Time,
	out io.Writer,
) error {
	rsyncNodeLogs := func(ctx context.Context, idx int) error {
		base := fmt.Sprintf("%d.logs", c.Nodes[idx-1])
		local := filepath.Join(dest, base) + "/"
		sshUser := c.user(c.Nodes[idx-1])
		rsyncArgs := []string{"-az", "--size-only"}
		var remote string
		if c.IsLocal() {
			// This here is a bit of a hack to guess that the parent of the log dir is
			// the "home" for the local node and that the srcBase is relative to that.
			localHome := filepath.Dir(c.Impl.LogDir(c, idx))
			remote = filepath.Join(localHome, src) + "/"
		} else {
			logDir := src
			if !filepath.IsAbs(logDir) && user != "" && user != sshUser {
				logDir = "~" + user + "/" + logDir
			}
			remote = fmt.Sprintf("%s@%s:%s/", c.user(c.Nodes[idx-1]),
				c.host(c.Nodes[idx-1]), logDir)
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
			return errors.Errorf("failed to rsync from %v to %v: %v\n%s",
				src, dest, err, stderrBuf.String())
		}
		return nil
	}
	rsyncLogs := func(ctx context.Context) error {
		g, gctx := errgroup.WithContext(ctx)
		for i := range c.Nodes {
			idx := c.Nodes[i]
			g.Go(func() error {
				return rsyncNodeLogs(gctx, idx)
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
				"--file-pattern", "^(?:.*/)?(?P<id>[0-9]+).*/"+clog.FileNamePattern+"$",
				"--prefix", "${id}> ")
		}
		cmd.Stdout = out
		var errBuf bytes.Buffer
		cmd.Stderr = &errBuf
		if err := cmd.Run(); err != nil && ctx.Err() == nil {
			return fmt.Errorf("failed to run cockroach debug merge-logs:%v\n%v",
				err, errBuf.String())
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := os.MkdirAll(dest, 0755); err != nil {
		return errors.Errorf("failed to create destination directory: %v", err)
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
			return errors.Errorf("failed to sync logs: %v", err)
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
func (c *SyncedCluster) Get(src, dest string) {
	// TODO(peter): Only get 10 nodes at a time. When a node completes, output a
	// line indicating that.
	var detail string
	if !c.IsLocal() {
		detail = " (scp)"
	}
	fmt.Printf("%s: getting%s %s %s\n", c.Name, detail, src, dest)

	type result struct {
		index int
		err   error
	}

	var writer ui.Writer
	results := make(chan result, len(c.Nodes))
	lines := make([]string, len(c.Nodes))
	var linesMu syncutil.Mutex

	var wg sync.WaitGroup
	for i := range c.Nodes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			src := src
			dest := dest
			if len(c.Nodes) > 1 {
				base := fmt.Sprintf("%d.%s", c.Nodes[i], filepath.Base(dest))
				dest = filepath.Join(filepath.Dir(dest), base)
			}

			progress := func(p float64) {
				linesMu.Lock()
				defer linesMu.Unlock()
				lines[i] = formatProgress(p)
			}

			if c.IsLocal() {
				if !filepath.IsAbs(src) {
					src = filepath.Join(fmt.Sprintf(os.ExpandEnv("${HOME}/local/%d"), c.Nodes[i]), src)
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

						infos, err := ioutil.ReadDir(src)
						if err != nil {
							return err
						}

						for _, info := range infos {
							if err := copy(
								filepath.Join(src, info.Name()),
								filepath.Join(dest, info.Name()),
								info,
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

			err := c.scp(fmt.Sprintf("%s@%s:%s", c.user(c.Nodes[0]), c.host(c.Nodes[i]), src), dest)
			if err == nil {
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
				err = filepath.Walk(dest, chmod)
			}

			results <- result{i, err}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var ticker *time.Ticker
	if !c.Quiet {
		ticker = time.NewTicker(100 * time.Millisecond)
	} else {
		ticker = time.NewTicker(1000 * time.Millisecond)
	}
	defer ticker.Stop()
	haveErr := false

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	for done := false; !done; {
		select {
		case <-ticker.C:
			if c.Quiet {
				fmt.Printf(".")
			}
		case r, ok := <-results:
			done = !ok
			if ok {
				linesMu.Lock()
				if r.err != nil {
					haveErr = true
					lines[r.index] = r.err.Error()
				} else {
					lines[r.index] = "done"
				}
				linesMu.Unlock()
			}
		}
		if !c.Quiet {
			linesMu.Lock()
			for i := range lines {
				fmt.Fprintf(&writer, "  %2d: ", c.Nodes[i])
				if lines[i] != "" {
					fmt.Fprintf(&writer, "%s", lines[i])
				} else {
					fmt.Fprintf(&writer, "%s", spinner[spinnerIdx%len(spinner)])
				}
				fmt.Fprintf(&writer, "\n")
			}
			linesMu.Unlock()
			_ = writer.Flush(os.Stdout)
			spinnerIdx++
		}
	}

	if c.Quiet {
		fmt.Printf("\n")
		linesMu.Lock()
		for i := range lines {
			fmt.Printf("  %2d: %s\n", c.Nodes[i], lines[i])
		}
		linesMu.Unlock()
	}

	if haveErr {
		log.Fatalf("get %s failed", src)
	}
}

func (c *SyncedCluster) pgurls(nodes []int) map[int]string {
	hosts := c.pghosts(nodes)
	m := make(map[int]string, len(hosts))
	for node, host := range hosts {
		m[node] = c.Impl.NodeURL(c, host, c.Impl.NodePort(c, node))
	}
	return m
}

func (c *SyncedCluster) pghosts(nodes []int) map[int]string {
	ips := make([]string, len(nodes))
	c.Parallel("", len(nodes), 0, func(i int) ([]byte, error) {
		var err error
		ips[i], err = c.GetInternalIP(nodes[i])
		return nil, errors.Wrapf(err, "pghosts")
	})

	m := make(map[int]string, len(ips))
	for i, ip := range ips {
		m[nodes[i]] = ip
	}
	return m
}

// SSH TODO(peter): document
func (c *SyncedCluster) SSH(sshArgs, args []string) error {
	if len(c.Nodes) != 1 && len(args) == 0 {
		// If trying to ssh to more than 1 node and the ssh session is interative,
		// try sshing with an iTerm2 split screen configuration.
		sshed, err := maybeSplitScreenSSHITerm2(c)
		if sshed {
			return err
		}
	}

	// Perform template expansion on the arguments.
	e := expander{
		node: c.Nodes[0],
	}
	var expandedArgs []string
	for _, arg := range args {
		expandedArg, err := e.expand(c, arg)
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
		cmd := fmt.Sprintf("cd ${HOME}/local/%d ; ", c.Nodes[0])
		if len(args) == 0 /* interactive */ {
			cmd += "/bin/bash "
		}
		if len(args) > 0 {
			cmd += fmt.Sprintf("export ROACHPROD=%d%s ; ", c.Nodes[0], c.Tag)
			cmd += strings.Join(expandedArgs, " ")
		}
		allArgs = append(allArgs, cmd)
	} else {
		allArgs = []string{
			"ssh",
			fmt.Sprintf("%s@%s", c.user(c.Nodes[0]), c.host(c.Nodes[0])),
			"-o", "UserKnownHostsFile=/dev/null",
			"-o", "StrictHostKeyChecking=no",
		}
		allArgs = append(allArgs, sshAuthArgs()...)
		allArgs = append(allArgs, sshArgs...)
		if len(args) > 0 {
			allArgs = append(allArgs, fmt.Sprintf("export ROACHPROD=%d%s ;", c.Nodes[0], c.Tag))
		}
		allArgs = append(allArgs, expandedArgs...)
	}

	sshPath, err := exec.LookPath(allArgs[0])
	if err != nil {
		return err
	}
	return syscall.Exec(sshPath, allArgs, os.Environ())
}

func (c *SyncedCluster) scp(src, dest string) error {
	args := []string{
		"scp", "-r", "-C",
		"-o", "StrictHostKeyChecking=no",
	}
	args = append(args, sshAuthArgs()...)
	args = append(args, src, dest)
	cmd := exec.Command(args[0], args[1:]...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "~ %s\n%s", strings.Join(args, " "), out)
	}
	return nil
}

// ParallelResult captures the result of a user-defined function
// passed to Parallel or ParallelE.
type ParallelResult struct {
	Index int
	Out   []byte
	Err   error
}

// Parallel runs a user-defined function across the nodes in the
// cluster. If any of the commands fail, Parallel will log an error
// and exit the program.
//
// See ParallelE for more information.
func (c *SyncedCluster) Parallel(
	display string, count, concurrency int, fn func(i int) ([]byte, error),
) {
	failed, err := c.ParallelE(display, count, concurrency, fn)
	if err != nil {
		sort.Slice(failed, func(i, j int) bool { return failed[i].Index < failed[j].Index })
		for _, f := range failed {
			fmt.Fprintf(os.Stderr, "%d: %+v: %s\n", f.Index, f.Err, f.Out)
		}
		log.Fatal("command failed")
	}
}

// ParallelE runs the given function in parallel across the given
// nodes, returning an error if function returns an error.
//
// ParallelE runs the user-defined functions on the first `count`
// nodes in the cluster. It runs at most `concurrency` (or
// `c.MaxConcurrency` if it is lower) in parallel. If `concurrency` is
// 0, then it defaults to `count`.
//
// If err is non-nil, the slice of ParallelResults will contain the
// results from any of the failed invocations.
func (c *SyncedCluster) ParallelE(
	display string, count, concurrency int, fn func(i int) ([]byte, error),
) ([]ParallelResult, error) {
	if concurrency == 0 || concurrency > count {
		concurrency = count
	}
	if c.MaxConcurrency > 0 && concurrency > c.MaxConcurrency {
		concurrency = c.MaxConcurrency
	}

	results := make(chan ParallelResult, count)
	var wg sync.WaitGroup
	wg.Add(count)

	var index int
	startNext := func() {
		go func(i int) {
			defer wg.Done()
			out, err := fn(i)
			results <- ParallelResult{i, out, err}
		}(index)
		index++
	}

	for index < concurrency {
		startNext()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var writer ui.Writer
	out := io.Writer(os.Stdout)
	if display == "" {
		out = ioutil.Discard
	}

	var ticker *time.Ticker
	if !c.Quiet {
		ticker = time.NewTicker(100 * time.Millisecond)
	} else {
		ticker = time.NewTicker(1000 * time.Millisecond)
		fmt.Fprintf(out, "%s", display)
	}
	defer ticker.Stop()
	complete := make([]bool, count)
	var failed []ParallelResult

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	for done := false; !done; {
		select {
		case <-ticker.C:
			if c.Quiet {
				fmt.Fprintf(out, ".")
			}
		case r, ok := <-results:
			if r.Err != nil {
				failed = append(failed, r)
			}
			done = !ok
			if ok {
				complete[r.Index] = true
			}
			if index < count {
				startNext()
			}
		}

		if !c.Quiet {
			fmt.Fprint(&writer, display)
			var n int
			for i := range complete {
				if complete[i] {
					n++
				}
			}
			fmt.Fprintf(&writer, " %d/%d", n, len(complete))
			if !done {
				fmt.Fprintf(&writer, " %s", spinner[spinnerIdx%len(spinner)])
			}
			fmt.Fprintf(&writer, "\n")
			_ = writer.Flush(out)
			spinnerIdx++
		}
	}

	if c.Quiet {
		fmt.Fprintf(out, "\n")
	}

	if len(failed) > 0 {
		return failed, errors.New("one or more parallel execution failure")
	}
	return nil, nil
}

func (c *SyncedCluster) escapedTag() string {
	return strings.Replace(c.Tag, "/", "\\/", -1)
}

// Init initializes the cluster. It does it through node 1 (as per ServerNodes)
// to maintain parity with auto-init behavior of `roachprod start` (when
// --skip-init) is not specified. The implementation should be kept in
// sync with Cockroach.Start.
func (c *SyncedCluster) Init() {
	r := c.Impl.(Cockroach)
	h := &crdbInstallHelper{c: c, r: r}

	// See (Cockroach).Start. We reserve a few special operations for the first
	// node, so we strive to maintain the same here for interoperability.
	const firstNodeIdx = 0

	vers, err := getCockroachVersion(c, c.ServerNodes()[firstNodeIdx])
	if err != nil {
		log.Fatalf("unable to retrieve cockroach version: %v", err)
	}

	if !vers.AtLeast(version.MustParse("v20.1.0")) {
		log.Fatal("`roachprod init` only supported for v20.1 and beyond")
	}

	fmt.Printf("%s: initializing cluster\n", h.c.Name)
	initOut, err := h.initializeCluster(firstNodeIdx)
	if err != nil {
		log.Fatalf("unable to initialize cluster: %v", err)
	}
	if initOut != "" {
		fmt.Println(initOut)
	}

	fmt.Printf("%s: setting cluster settings\n", h.c.Name)
	clusterSettingsOut, err := h.setClusterSettings(firstNodeIdx)
	if err != nil {
		log.Fatalf("unable to set cluster settings: %v", err)
	}
	if clusterSettingsOut != "" {
		fmt.Println(clusterSettingsOut)
	}
}
