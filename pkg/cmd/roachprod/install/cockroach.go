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
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/pkg/errors"
)

// StartOpts TODO(peter): document
var StartOpts struct {
	Encrypt    bool
	Sequential bool
}

// Cockroach TODO(peter): document
type Cockroach struct{}

func cockroachNodeBinary(c *SyncedCluster, i int) string {
	if filepath.IsAbs(config.Binary) {
		return config.Binary
	}
	if !c.IsLocal() {
		return "./" + config.Binary
	}

	path := filepath.Join(fmt.Sprintf(os.ExpandEnv("${HOME}/local/%d"), i), config.Binary)
	if _, err := os.Stat(path); err == nil {
		return path
	}

	// For "local" clusters we have to find the binary to run and translate it to
	// an absolute path. First, look for the binary in PATH.
	path, err := exec.LookPath(config.Binary)
	if err != nil {
		if strings.HasPrefix(config.Binary, "/") {
			return config.Binary
		}
		// We're unable to find the binary in PATH and "binary" is a relative path:
		// look in the cockroach repo.
		gopath := os.Getenv("GOPATH")
		if gopath == "" {
			return config.Binary
		}
		path = gopath + "/src/github.com/cockroachdb/cockroach/" + config.Binary
		var err2 error
		path, err2 = exec.LookPath(path)
		if err2 != nil {
			return config.Binary
		}
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return config.Binary
	}
	return path
}

func getCockroachVersion(c *SyncedCluster, i int) (*version.Version, error) {
	sess, err := c.newSession(i)
	if err != nil {
		return nil, err
	}
	defer sess.Close()

	cmd := cockroachNodeBinary(c, i) + " version"
	out, err := sess.CombinedOutput(cmd)
	if err != nil {
		return nil, errors.Wrapf(err, "~ %s\n%s", cmd, out)
	}

	matches := regexp.MustCompile(`(?m)^Build Tag:\s+(.*)$`).FindSubmatch(out)
	if len(matches) != 2 {
		return nil, fmt.Errorf("unable to parse cockroach version output:%s", out)
	}

	return version.Parse(string(matches[1]))
}

// GetAdminUIPort returns the admin UI port for ths specified RPC port.
func GetAdminUIPort(connPort int) int {
	return connPort + 1
}

func argExists(args []string, target string) int {
	for i, arg := range args {
		if arg == target || strings.HasPrefix(arg, target+"=") {
			return i
		}
	}
	return -1
}

// Start implements the ClusterImpl.NodeDir interface.
func (r Cockroach) Start(c *SyncedCluster, extraArgs []string) {
	// Check to see if node 1 was started indicating the cluster was
	// bootstrapped.
	var bootstrapped bool
	for _, i := range c.ServerNodes() {
		if i == 1 {
			bootstrapped = true
			break
		}
	}

	if c.Secure && bootstrapped {
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

		if existsErr != nil {
			// Gather the internal IP addresses for every node in the cluster, even
			// if it won't be added to the cluster itself we still add the IP address
			// to the node cert.
			var msg string
			display := fmt.Sprintf("%s: initializing certs", c.Name)
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

			// Skip the the first node which is where we generated the certs.
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
	}

	display := fmt.Sprintf("%s: starting", c.Name)
	host1 := c.host(1)
	nodes := c.ServerNodes()

	// If we're creating nodes that span VPC (e.g. AWS multi-region or
	// multi-cloud), we'll tell the nodes to advertise their public IPs
	// so that attaching nodes to the cluster Just Works.
	var advertisePublicIP bool
	for i, vpc := range c.VPCs {
		if i > 0 && vpc != c.VPCs[0] {
			advertisePublicIP = true
			break
		}
	}

	p := 0
	if StartOpts.Sequential {
		p = 1
	}
	c.Parallel(display, len(nodes), p, func(i int) ([]byte, error) {
		vers, err := getCockroachVersion(c, nodes[i])
		if err != nil {
			return nil, err
		}

		sess, err := c.newSession(nodes[i])
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		port := r.NodePort(c, nodes[i])

		var args []string
		if c.Secure {
			if c.IsLocal() {
				args = append(args, fmt.Sprintf("--certs-dir=${HOME}/local/%d/certs", nodes[i]))
			} else {
				args = append(args, "--certs-dir=certs")
			}
		} else {
			args = append(args, "--insecure")
		}
		dir := c.Impl.NodeDir(c, nodes[i])
		logDir := c.Impl.LogDir(c, nodes[i])
		if idx := argExists(extraArgs, "--store"); idx == -1 {
			args = append(args, "--store=path="+dir)
		}
		args = append(args, "--log-dir="+logDir)
		args = append(args, "--background")
		if vers.AtLeast(version.MustParse("v1.1.0")) {
			cache := 25
			if c.IsLocal() {
				cache /= len(nodes)
				if cache == 0 {
					cache = 1
				}
			}
			args = append(args, fmt.Sprintf("--cache=%d%%", cache))
			args = append(args, fmt.Sprintf("--max-sql-memory=%d%%", cache))
		}
		if c.IsLocal() {
			// This avoids annoying firewall prompts on Mac OS X.
			if vers.AtLeast(version.MustParse("v2.1.0")) {
				args = append(args, "--listen-addr=127.0.0.1")
			} else {
				args = append(args, "--host=127.0.0.1")
			}
		}
		args = append(args, fmt.Sprintf("--port=%d", port))
		args = append(args, fmt.Sprintf("--http-port=%d", GetAdminUIPort(port)))
		if locality := c.locality(nodes[i]); locality != "" {
			if idx := argExists(extraArgs, "--locality"); idx == -1 {
				args = append(args, "--locality="+locality)
			}
		}
		if nodes[i] != 1 {
			args = append(args, fmt.Sprintf("--join=%s:%d", host1, r.NodePort(c, 1)))
		}
		if advertisePublicIP {
			args = append(args, fmt.Sprintf("--advertise-host=%s", c.host(i)))
		}

		var keyCmd string
		if StartOpts.Encrypt {
			// Encryption at rest is turned on for the cluster.
			// TODO(windchan7): allow key size to be specified through flags.
			encryptArgs := "--enterprise-encryption=path=%s,key=%s/aes-128.key,old-key=plain"
			var storeDir string
			if idx := argExists(extraArgs, "--store"); idx == -1 {
				storeDir = dir
			} else {
				storeDir = strings.TrimPrefix(extraArgs[idx], "--store=")
			}
			encryptArgs = fmt.Sprintf(encryptArgs, storeDir, storeDir)
			args = append(args, encryptArgs)

			// Command to create the store key.
			keyCmd = fmt.Sprintf("mkdir -p %[1]s; if [ ! -e %[1]s/aes-128.key ]; then openssl rand -out %[1]s/aes-128.key 48; fi; ", storeDir)
		}

		// Argument template expansion is node specific (e.g. for {store-dir}).
		e := expander{
			node: nodes[i],
		}
		for _, arg := range extraArgs {
			arg = e.expand(c, arg)
			args = append(args, strings.Split(arg, " ")...)
		}

		binary := cockroachNodeBinary(c, nodes[i])
		// NB: this is awkward as when the process fails, the test runner will show an
		// unhelpful empty error (since everything has been redirected away). This is
		// unfortunately equally awkward to address.
		cmd := "mkdir -p " + logDir + "; "
		// TODO(peter): The ps and lslocks stuff is intended to debug why killing
		// of a cockroach process sometimes doesn't release file locks immediately.
		cmd += `echo ">>> roachprod start: $(date)" >> ` + logDir + "/roachprod.log; " +
			`ps axeww -o pid -o command >> ` + logDir + "/roachprod.log; " +
			`[ -x /usr/bin/lslocks ] && /usr/bin/lslocks >> ` + logDir + "/roachprod.log; "
		cmd += keyCmd +
			fmt.Sprintf(" export ROACHPROD=%d%s && ", nodes[i], c.Tag) +
			c.Env + " " + binary + " start " + strings.Join(args, " ") +
			" >> " + logDir + "/cockroach.stdout 2>> " + logDir + "/cockroach.stderr" +
			" || (x=$?; cat " + logDir + "/cockroach.stderr; exit $x)"
		if out, err := sess.CombinedOutput(cmd); err != nil {
			return nil, errors.Wrapf(err, "~ %s\n%s", cmd, out)
		}
		// NB: if cockroach started successfully, we ignore the output as it is
		// some harmless start messaging.
		return nil, nil
	})

	if bootstrapped {
		license := envutil.EnvOrDefaultString("COCKROACH_DEV_LICENSE", "")
		if license == "" {
			fmt.Printf("%s: COCKROACH_DEV_LICENSE unset: enterprise features will be unavailable\n",
				c.Name)
		}

		var msg string
		display = fmt.Sprintf("%s: initializing cluster settings", c.Name)
		c.Parallel(display, 1, 0, func(i int) ([]byte, error) {
			sess, err := c.newSession(1)
			if err != nil {
				return nil, err
			}
			defer sess.Close()

			var cmd string
			if c.IsLocal() {
				cmd = `cd ${HOME}/local/1 ; `
			}
			dir := c.Impl.NodeDir(c, nodes[i])
			cmd += `
if ! test -e ` + dir + `/settings-initialized ; then
  COCKROACH_CONNECT_TIMEOUT=0 ` + cockroachNodeBinary(c, 1) + " sql --url " +
				r.NodeURL(c, "localhost", r.NodePort(c, 1)) + " -e " +
				fmt.Sprintf(`"
SET CLUSTER SETTING server.remote_debugging.mode = 'any';
SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing';
SET CLUSTER SETTING enterprise.license = '%s';"`, license) + ` &&
			touch ` + dir + `/settings-initialized
fi
`
			out, err := sess.CombinedOutput(cmd)
			if err != nil {
				return nil, errors.Wrapf(err, "~ %s\n%s", cmd, out)
			}
			msg = strings.TrimSpace(string(out))
			return nil, nil
		})

		if msg != "" {
			fmt.Println(msg)
		}
	}
}

// NodeDir implements the ClusterImpl.NodeDir interface.
func (Cockroach) NodeDir(c *SyncedCluster, index int) string {
	if c.IsLocal() {
		return os.ExpandEnv(fmt.Sprintf("${HOME}/local/%d/data", index))
	}
	return "/mnt/data1/cockroach"
}

// LogDir implements the ClusterImpl.NodeDir interface.
func (Cockroach) LogDir(c *SyncedCluster, index int) string {
	dir := "${HOME}/logs"
	if c.IsLocal() {
		dir = os.ExpandEnv(fmt.Sprintf("${HOME}/local/%d/logs", index))
	}
	return dir
}

// NodeURL implements the ClusterImpl.NodeDir interface.
func (Cockroach) NodeURL(c *SyncedCluster, host string, port int) string {
	url := fmt.Sprintf("'postgres://root@%s:%d", host, port)
	if c.Secure {
		url += "?sslcert=certs%2Fnode.crt&sslkey=certs%2Fnode.key&" +
			"sslrootcert=certs%2Fca.crt&sslmode=verify-full"
	} else {
		url += "?sslmode=disable"
	}
	url += "'"
	return url
}

// NodePort implements the ClusterImpl.NodeDir interface.
func (Cockroach) NodePort(c *SyncedCluster, index int) int {
	const basePort = 26257
	port := basePort
	if c.IsLocal() {
		port += (index - 1) * 2
	}
	return port
}

// NodeUIPort implements the ClusterImpl.NodeDir interface.
func (r Cockroach) NodeUIPort(c *SyncedCluster, index int) int {
	return GetAdminUIPort(r.NodePort(c, index))
}

// SQL implements the ClusterImpl.NodeDir interface.
func (r Cockroach) SQL(c *SyncedCluster, args []string) error {
	if len(args) == 0 || len(c.Nodes) == 1 {
		// If no arguments, we're going to get an interactive SQL shell. Require
		// exactly one target and ask SSH to provide a pseudoterminal.
		if len(args) == 0 && len(c.Nodes) != 1 {
			return fmt.Errorf("invalid number of nodes for interactive sql: %d", len(c.Nodes))
		}
		url := r.NodeURL(c, "localhost", r.NodePort(c, c.Nodes[0]))
		binary := cockroachNodeBinary(c, c.Nodes[0])
		allArgs := []string{binary, "sql", "--url", url}
		allArgs = append(allArgs, ssh.Escape(args))
		return c.SSH([]string{"-t"}, allArgs)
	}

	// Otherwise, assume the user provided the "-e" flag, so we can reasonably
	// execute the query on all specified nodes.
	type result struct {
		node   int
		output string
	}
	resultChan := make(chan result, len(c.Nodes))

	display := fmt.Sprintf("%s: executing sql", c.Name)
	c.Parallel(display, len(c.Nodes), 0, func(i int) ([]byte, error) {
		sess, err := c.newSession(c.Nodes[i])
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		var cmd string
		if c.IsLocal() {
			cmd = fmt.Sprintf(`cd ${HOME}/local/%d ; `, c.Nodes[i])
		}
		cmd += cockroachNodeBinary(c, c.Nodes[i]) + " sql --url " +
			r.NodeURL(c, "localhost", r.NodePort(c, c.Nodes[i])) + " " +
			ssh.Escape(args)

		out, err := sess.CombinedOutput(cmd)
		if err != nil {
			return nil, errors.Wrapf(err, "~ %s\n%s", cmd, out)
		}

		resultChan <- result{node: c.Nodes[i], output: string(out)}
		return nil, nil
	})

	results := make([]result, 0, len(c.Nodes))
	for range c.Nodes {
		results = append(results, <-resultChan)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].node < results[j].node
	})
	for _, r := range results {
		fmt.Printf("node %d:\n%s", r.node, r.output)
	}

	return nil
}
