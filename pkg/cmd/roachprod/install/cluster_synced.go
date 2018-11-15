// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package install

import (
	"bufio"
	"bytes"
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/ui"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"

	"github.com/pkg/errors"
)

// ClusterImpl TODO(peter): document
type ClusterImpl interface {
	Start(c *SyncedCluster, extraArgs []string)
	NodeDir(c *SyncedCluster, index int) string
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
	Nodes       []int
	LoadGen     int
	Secure      bool
	Env         string
	Args        []string
	Tag         string
	Impl        ClusterImpl
	UseTreeDist bool
	Quiet       bool
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

// ServerNodes TODO(peter): document
func (c *SyncedCluster) ServerNodes() []int {
	if c.LoadGen == -1 {
		return c.Nodes
	}
	newNodes := make([]int, 0, len(c.Nodes))
	for _, i := range c.Nodes {
		if i != c.LoadGen {
			newNodes = append(newNodes, i)
		}
	}
	return newNodes
}

// GetInternalIP returns the internal IP address of the specified node.
func (c *SyncedCluster) GetInternalIP(index int) (string, error) {
	if c.IsLocal() {
		return c.host(index), nil
	}

	session, err := c.newSession(index)
	if err != nil {
		return "", nil
	}
	defer session.Close()

	cmd := `hostname --all-ip-addresses`
	out, err := session.CombinedOutput(cmd)
	if err != nil {
		return "", nil
	}
	return strings.TrimSpace(string(out)), nil
}

// Start TODO(peter): document
func (c *SyncedCluster) Start() {
	c.Impl.Start(c, c.Args)
}

func (c *SyncedCluster) newSession(i int) (session, error) {
	if c.IsLocal() {
		return newLocalSession(), nil
	}
	return newRemoteSession(c.user(i), c.host(i))
}

// Stop TODO(peter): document
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
  done
`, c.Impl.LogDir(c, c.Nodes[i]))
		}

		// NB: the awkward-looking `awk` invocation serves to avoid having the
		// awk process match its own output from `ps`.
		cmd := fmt.Sprintf(`
mkdir -p logs
echo ">>> roachprod stop: $(date)" >> %[1]s/roachprod.log
ps axeww -o pid -o command >> %[1]s/roachprod.log
pids=$(ps axeww -o pid -o command | \
  sed 's/export ROACHPROD=//g' | \
  awk '/ROACHPROD=(%[2]d%[3]s)[ \/]/ { print $1 }')
if [ -n "${pids}" ]; then
  kill -%[4]d ${pids}
%[5]s
fi
`, c.Impl.LogDir(c, c.Nodes[i]), c.Nodes[i], c.escapedTag(), sig, waitCmd)
		return sess.CombinedOutput(cmd)
	})
}

// Wipe TODO(peter): document
func (c *SyncedCluster) Wipe() {
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
			for _, dir := range []string{"certs*", "data", "logs"} {
				cmd += fmt.Sprintf(`rm -fr ${HOME}/local/%d/%s ;`, c.Nodes[i], dir)
			}
		} else {
			cmd = `find /mnt/data* -maxdepth 1 -type f -exec rm -f {} \; ;
rm -fr /mnt/data*/{auxiliary,local,tmp,cassandra,cockroach,cockroach-temp*,mongo-data} \; ;
rm -fr certs* ;
`
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

// NodeMonitorInfo TODO(peter): document
type NodeMonitorInfo struct {
	Index int
	Msg   string
}

// Monitor TODO(peter): document
func (c *SyncedCluster) Monitor() chan NodeMonitorInfo {
	ch := make(chan NodeMonitorInfo)
	nodes := c.ServerNodes()

	for i := range nodes {
		go func(i int) {
			sess, err := c.newSession(nodes[i])
			if err != nil {
				ch <- NodeMonitorInfo{nodes[i], err.Error()}
				return
			}
			defer sess.Close()

			p, err := sess.StdoutPipe()
			if err != nil {
				ch <- NodeMonitorInfo{nodes[i], err.Error()}
				return
			}

			go func(p io.Reader) {
				r := bufio.NewReader(p)
				for {
					line, _, err := r.ReadLine()
					if err == io.EOF {
						return
					}
					ch <- NodeMonitorInfo{nodes[i], string(line)}
				}
			}(p)

			// On each monitored node, we loop looking for a cockroach process. In
			// order to avoid polling with lsof, if we find a live process we use nc
			// (netcat) to connect to the rpc port which will block until the server
			// either decides to kill the connection or the process is killed.
			cmd := fmt.Sprintf(`
lastpid=0
while :; do
  pid=$(lsof -i :%[1]d -sTCP:LISTEN | awk '!/COMMAND/ {print $2}')
  if [ "${pid}" != "${lastpid}" ]; then
    if [ -n "${lastpid}" -a -z "${pid}" ]; then
      echo dead
    fi
    lastpid=${pid}
    if [ -n "${pid}" ]; then
      echo ${pid}
    fi
  fi

  if [ -n "${lastpid}" ]; then
    nc localhost %[1]d >/dev/null 2>&1
    echo nc exited
  else
    sleep 1
  fi
done
`,
				Cockroach{}.NodePort(c, nodes[i]))

			// Request a PTY so that the script will receive will receive a SIGPIPE
			// when the session is closed.
			if err := sess.RequestPty(); err != nil {
				ch <- NodeMonitorInfo{nodes[i], err.Error()}
				return
			}
			// Give the session a valid stdin pipe so that nc won't exit immediately.
			// When nc does exit, we write to stdout, which has a side effect of
			// checking whether the stdout pipe has broken. This allows us to detect
			// when the roachprod process is killed.
			inPipe, err := sess.StdinPipe()
			if err != nil {
				ch <- NodeMonitorInfo{nodes[i], err.Error()}
				return
			}
			defer inPipe.Close()
			if err := sess.Run(cmd); err != nil {
				ch <- NodeMonitorInfo{nodes[i], err.Error()}
				return
			}
		}(i)
	}

	return ch
}

// Run TODO(peter): document
func (c *SyncedCluster) Run(stdout, stderr io.Writer, nodes []int, title, cmd string) error {
	// Stream output if we're running the command on only 1 node.
	stream := len(nodes) == 1
	var display string
	if !stream {
		display = fmt.Sprintf("%s: %s", c.Name, title)
	}

	errors := make([]error, len(nodes))
	results := make([]string, len(nodes))
	c.Parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(nodes[i])
		if err != nil {
			errors[i] = err
			results[i] = err.Error()
			return nil, nil
		}
		defer sess.Close()

		// Argument template expansion is node specific (e.g. for {store-dir}).
		e := expander{
			node: nodes[i],
		}
		expandedCmd := e.expand(c, cmd)

		// Be careful about changing these command strings. In particular, we need
		// to support running commands in the background on both local and remote
		// nodes. For example:
		//
		//   roachprod run cluster -- "sleep 60 &> /dev/null < /dev/null &"
		//
		// That command should return immediately. And a "roachprod status" should
		// reveal that the sleep command is running on the cluster.
		nodeCmd := fmt.Sprintf(`export ROACHPROD=%d%s && bash -c %s`,
			nodes[i], c.Tag, ssh.Escape1(expandedCmd))
		if c.IsLocal() {
			nodeCmd = fmt.Sprintf("cd ${HOME}/local/%d ; %s", nodes[i], nodeCmd)
		}

		if stream {
			sess.SetStdout(stdout)
			sess.SetStderr(stderr)
			errors[i] = sess.Run(nodeCmd)
			return nil, nil
		}

		out, err := sess.CombinedOutput(nodeCmd)
		msg := strings.TrimSpace(string(out))
		if err != nil {
			errors[i] = err
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

	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
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

// SetupSSH TODO(peter): document
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
	var msg string
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
			msg = fmt.Sprintf("~ %s\n%v\n%s", cmd, err, stderr.String())
		} else {
			sshTar = stdout.Bytes()
		}
		return nil, nil
	})

	if msg != "" {
		fmt.Fprintln(os.Stderr, msg)
		return nil
	}

	// Skip the the first node which is where we generated the key.
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
			return nil, errors.Wrapf(err, "~ %s\n%s", cmd, out)
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
	c.Parallel("scanning hosts", len(c.Nodes), 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(c.Nodes[i])
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		// Note that this is not idempotent. If we run the ssh-keyscan multiple
		// times on the same node the known_hosts file will grow. This isn't a
		// problem for the usage here which only performs the keyscan once when the
		// cluster is created. ssh-keyscan may return fewer than the desired number
		// of entries if the remote nodes are not responding yet, so we loop until
		// we have a scan that found host keys for all of the IPs.
		cmd := `
for i in {1..20}; do
  ssh-keyscan -T 60 -t rsa ` + strings.Join(ips, " ") + ` > .ssh/known_hosts.tmp
  if [ "$(cat .ssh/known_hosts.tmp | wc -l)" -eq "` + fmt.Sprint(len(ips)) + `" ]; then
    cat .ssh/known_hosts.tmp >> .ssh/known_hosts
    rm -f .ssh/known_hosts.tmp
    exit 0
  fi
  sleep 1
done
exit 1
`
		if out, err := sess.CombinedOutput(cmd); err != nil {
			return nil, errors.Wrapf(err, "~ %s\n%s", cmd, out)
		}
		return nil, nil
	})

	return nil
}

// CockroachVersions TODO(peter): document
func (c *SyncedCluster) CockroachVersions() map[string]int {
	sha := make(map[string]int)
	var mu syncutil.Mutex

	display := fmt.Sprintf("%s: cockroach version", c.Name)
	nodes := c.ServerNodes()
	c.Parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(c.Nodes[i])
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		cmd := config.Binary + " version | awk '/Build Tag:/ {print $NF}'"
		out, err := sess.CombinedOutput(cmd)
		var s string
		if err != nil {
			s = fmt.Sprintf("%s: %v", out, err)
		} else {
			s = strings.TrimSpace(string(out))
		}
		mu.Lock()
		sha[s]++
		mu.Unlock()
		return nil, err
	})

	return sha
}

// RunLoad TODO(peter): document
func (c *SyncedCluster) RunLoad(cmd string, stdout, stderr io.Writer) error {
	if c.LoadGen == 0 {
		log.Fatalf("%s: no load generator node specified", c.Name)
	}

	display := fmt.Sprintf("%s: retrieving IP addresses", c.Name)
	nodes := c.ServerNodes()
	ips := make([]string, len(nodes))
	c.Parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
		var err error
		ips[i], err = c.GetInternalIP(nodes[i])
		return nil, err
	})

	session, err := ssh.NewSSHSession(c.user(c.LoadGen), c.host(c.LoadGen))
	if err != nil {
		return err
	}
	defer session.Close()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer func() {
		signal.Stop(ch)
		close(ch)
	}()
	go func() {
		_, ok := <-ch
		if ok {
			c.stopLoad()
		}
	}()

	session.Stdout = stdout
	session.Stderr = stderr
	fmt.Fprintln(stdout, cmd)

	var urls []string
	for i, ip := range ips {
		urls = append(urls, c.Impl.NodeURL(c, ip, c.Impl.NodePort(c, nodes[i])))
	}
	prefix := fmt.Sprintf("ulimit -n 16384; export ROACHPROD=%d%s && ", c.LoadGen, c.Tag)
	return session.Run(prefix + cmd + " " + strings.Join(urls, " "))
}

const progressDone = "=======================================>"
const progressTodo = "----------------------------------------"

func formatProgress(p float64) string {
	i := int(math.Ceil(float64(len(progressDone)) * (1 - p)))
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

	mkpath := func(i int) string {
		if i == -1 {
			return src
		}
		return fmt.Sprintf("%s@%s:%s", c.user(c.Nodes[i]), c.host(c.Nodes[i]), dest)
	}

	for i := range c.Nodes {
		go func(i int) {
			defer wg.Done()

			if c.IsLocal() {
				if _, err := os.Stat(src); err != nil {
					results <- result{i, err}
					return
				}
				from, err := filepath.Abs(src)
				if err != nil {
					results <- result{i, err}
					return
				}
				to := fmt.Sprintf(os.ExpandEnv("${HOME}/local/%d/%s"), c.Nodes[i], dest)
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
			from := mkpath(srcIndex)
			// TODO(peter): For remote-to-remote copies, should the destination use
			// the internal IP address? The external address works, but it might be
			// slower.
			to := mkpath(i)
			err := c.scp(from, to)
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
		}(i)
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
					if info.IsDir() {
						if err := os.MkdirAll(dest, info.Mode()); err != nil {
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

					if !info.Mode().IsRegular() {
						return nil
					}

					out, err := os.Create(dest)
					if err != nil {
						return err
					}
					defer out.Close()

					if err := os.Chmod(out.Name(), info.Mode()); err != nil {
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
	ips := make([]string, len(nodes))
	c.Parallel("", len(nodes), 0, func(i int) ([]byte, error) {
		var err error
		ips[i], err = c.GetInternalIP(nodes[i])
		return nil, errors.Wrapf(err, "pgurls")
	})

	m := make(map[int]string, len(ips))
	for i, ip := range ips {
		m[nodes[i]] = c.Impl.NodeURL(c, ip, c.Impl.NodePort(c, nodes[i]))
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
		arg = e.expand(c, arg)
		expandedArgs = append(expandedArgs, strings.Split(arg, " ")...)
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

func (c *SyncedCluster) stopLoad() {
	if c.LoadGen == 0 {
		log.Fatalf("no load generator node specified for cluster: %s", c.Name)
	}

	display := fmt.Sprintf("%s: stopping load", c.Name)
	c.Parallel(display, 1, 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(c.LoadGen)
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		cmd := fmt.Sprintf("kill -9 $(lsof -t -i :%d -i :%d) 2>/dev/null || true",
			Cockroach{}.NodePort(c, c.Nodes[i]),
			Cassandra{}.NodePort(c, c.Nodes[i]))
		return sess.CombinedOutput(cmd)
	})
}

// Parallel TODO(peter): document
func (c *SyncedCluster) Parallel(
	display string, count, concurrency int, fn func(i int) ([]byte, error),
) {
	if concurrency == 0 || concurrency > count {
		concurrency = count
	}

	type result struct {
		index int
		out   []byte
		err   error
	}

	results := make(chan result, count)
	var wg sync.WaitGroup
	wg.Add(count)

	var index int
	startNext := func() {
		go func(i int) {
			defer wg.Done()
			out, err := fn(i)
			results <- result{i, out, err}
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
	var failed []result

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	for done := false; !done; {
		select {
		case <-ticker.C:
			if c.Quiet {
				fmt.Fprintf(out, ".")
			}
		case r, ok := <-results:
			if r.err != nil {
				failed = append(failed, r)
			}
			done = !ok
			if ok {
				complete[r.index] = true
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
		sort.Slice(failed, func(i, j int) bool { return failed[i].index < failed[j].index })
		for _, f := range failed {
			fmt.Fprintf(os.Stderr, "%d: %+v: %s\n", f.index, f.err, f.out)
		}
		log.Fatal("command failed")
	}
}

func (c *SyncedCluster) escapedTag() string {
	return strings.Replace(c.Tag, "/", "\\/", -1)
}
