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
	"fmt"
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
	"github.com/cockroachdb/errors"
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
		c.DistributeCerts()
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

	env := func() string {
		var buf strings.Builder
		for _, v := range os.Environ() {
			if strings.HasPrefix(v, "COCKROACH_") {
				if buf.Len() > 0 {
					buf.WriteString(" ")
				}
				buf.WriteString(v)
			}
		}
		if len(c.Env) > 0 {
			if buf.Len() > 0 {
				buf.WriteString(" ")
			}
			buf.WriteString(c.Env)
		}
		return buf.String()
	}()

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
			args = append(args, "--certs-dir="+c.Impl.CertsDir(c, nodes[i]))
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
			args = append(args, fmt.Sprintf("--advertise-host=%s", c.host(i+1)))
		} else if !c.IsLocal() {
			// Explicitly advertise by IP address so that we don't need to
			// deal with cross-region name resolution. The `hostname -I`
			// prints all IP addresses for the host and then we'll select
			// the first from the list.
			args = append(args, "--advertise-host=$(hostname -I | awk '{print $1}')")
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
			expandedArg, err := e.expand(c, arg)
			if err != nil {
				return nil, err
			}
			args = append(args, strings.Split(expandedArg, " ")...)
		}

		// For a one-node cluster, use start-single-node to disable replication.
		startCmd := "start"
		if len(c.VMs) == 1 && vers.AtLeast(version.MustParse("v19.2.0")) {
			startCmd = "start-single-node"
		}

		binary := cockroachNodeBinary(c, nodes[i])
		// NB: this is awkward as when the process fails, the test runner will show an
		// unhelpful empty error (since everything has been redirected away). This is
		// unfortunately equally awkward to address.
		cmd := "ulimit -c unlimited; mkdir -p " + logDir + "; "
		// TODO(peter): The ps and lslocks stuff is intended to debug why killing
		// of a cockroach process sometimes doesn't release file locks immediately.
		cmd += `echo ">>> roachprod start: $(date)" >> ` + logDir + "/roachprod.log; " +
			`ps axeww -o pid -o command >> ` + logDir + "/roachprod.log; " +
			`[ -x /usr/bin/lslocks ] && /usr/bin/lslocks >> ` + logDir + "/roachprod.log; "
		cmd += keyCmd +
			fmt.Sprintf(" export ROACHPROD=%d%s && ", nodes[i], c.Tag) +
			"GOTRACEBACK=crash " +
			"COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING=1 " +
			// Turn stats mismatch into panic, see:
			// https://github.com/cockroachdb/cockroach/issues/38720#issuecomment-539136246
			// Disabled because we have a local repro in
			// https://github.com/cockroachdb/cockroach/issues/37815#issuecomment-545650087
			//
			// "COCKROACH_ENFORCE_CONSISTENT_STATS=true " +
			env + " " + binary + " " + startCmd + " " + strings.Join(args, " ") +
			" >> " + logDir + "/cockroach.stdout.log 2>> " + logDir + "/cockroach.stderr.log" +
			" || (x=$?; cat " + logDir + "/cockroach.stderr.log; exit $x)"
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

// CertsDir implements the ClusterImpl.NodeDir interface.
func (Cockroach) CertsDir(c *SyncedCluster, index int) string {
	dir := "${HOME}/certs"
	if c.IsLocal() {
		dir = os.ExpandEnv(fmt.Sprintf("${HOME}/local/%d/certs", index))
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
