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
	"log"
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

func cockroachNodeBinary(c *SyncedCluster, node int) string {
	if filepath.IsAbs(config.Binary) {
		return config.Binary
	}
	if !c.IsLocal() {
		return "./" + config.Binary
	}
	return cockroachLocalNodeLocation(c, node, config.Binary)
}

func cockroachLocalNodeLocation(c *SyncedCluster, node int, checkPath string) string {
	path := filepath.Join(fmt.Sprintf(os.ExpandEnv("${HOME}/local/%d"), node), checkPath)
	if _, err := os.Stat(path); err == nil {
		return path
	}

	// For "local" clusters we have to find the checkPath to run and translate it to
	// an absolute path. First, look for the checkPath in PATH.
	path, err := exec.LookPath(checkPath)
	if err != nil {
		if strings.HasPrefix(checkPath, "/") {
			return checkPath
		}
		// We're unable to find the checkPath in PATH and checkPath is a relative path:
		// look in the cockroach repo.
		gopath := os.Getenv("GOPATH")
		if gopath == "" {
			return checkPath
		}
		path = gopath + "/src/github.com/cockroachdb/cockroach/" + checkPath
		var err2 error
		path, err2 = exec.LookPath(path)
		if err2 != nil {
			return checkPath
		}
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return checkPath
	}
	return path
}

func getCockroachVersion(c *SyncedCluster, node int) (*version.Version, error) {
	sess, err := c.newSession(node)
	if err != nil {
		return nil, err
	}
	defer sess.Close()

	cmd := cockroachNodeBinary(c, node) + " version"
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

// Start implements the ClusterImpl.NodeDir interface, and powers `roachprod
// start`. Starting the first node is special-cased quite a bit, it's used to
// distribute certs, set cluster settings, and initialize the cluster. Also,
// if we're only starting a single node in the cluster and it happens to be the
// "first" node (node 1, as understood by SyncedCluster.ServerNodes), we use
// `start-single-node` (this was written to provide a short hand to start a
// single node cluster with a replication factor of one).
func (r Cockroach) Start(c *SyncedCluster, extraArgs []string) {
	h := &crdbStartHelper{c: c, r: r}
	h.distributeCerts()

	nodes := c.ServerNodes()
	var parallelism = 0
	if StartOpts.Sequential {
		parallelism = 1
	}

	fmt.Printf("%s: starting nodes\n", c.Name)
	c.Parallel("", len(nodes), parallelism, func(nodeIdx int) ([]byte, error) {
		vers, err := getCockroachVersion(c, nodes[nodeIdx])
		if err != nil {
			return nil, err
		}

		// NB: if cockroach started successfully, we ignore the output as it is
		// some harmless start messaging.
		if _, err := h.startNode(nodeIdx, extraArgs, vers); err != nil {
			return nil, err
		}

		// We reserve a few special operations (bootstrapping, and setting
		// cluster settings) for node 1.
		if node := nodes[nodeIdx]; node != 1 {
			return nil, nil
		}

		// NB: The code blocks below are not parallelized, so it's safe for us
		// to use fmt.Printf style logging.

		// 1. We don't init when invoking with `start-single-node`.
		// 2. For nodes running <20.1, the --join flags are constructed in a manner
		//    such that the first node doesn't have any (see `generateStartArgs`),
		//    which prompts CRDB to auto-initialize. For nodes running >=20.1, we
		//    need to explicitly initialize.
		shouldInit := !h.useStartSingleNode(vers) && vers.AtLeast(version.MustParse("v20.1.0"))
		if shouldInit {
			fmt.Printf("%s: initializing cluster", h.c.Name)
			initOut, err := h.initializeCluster(nodeIdx)
			if err != nil {
				log.Fatalf("unable to initialize cluster: %v", err)
			}

			if initOut != "" {
				fmt.Println(initOut)
			}
		}

		if !vers.AtLeast(version.MustParse("v20.1.0")) {
			// Given #51897 remains unresolved, master-built roachprod is used
			// to run roachtests against the 20.1 branch. Some of those
			// roachtests test mixed-version clusters that start off at 19.2.
			// Consequently, we manually add this `cluster-bootstrapped` file
			// where roachprod expects to find it for already-initialized
			// clusters. This is a pretty gross hack, that we should address by
			// addressing #51897.
			//
			// TODO(irfansharif): Remove this once #51897 is resolved.
			markBootstrap := fmt.Sprintf("touch %s/%s", h.c.Impl.NodeDir(h.c, nodes[nodeIdx]), "cluster-bootstrapped")
			cmdOut, err := h.run(nodeIdx, markBootstrap)
			if err != nil {
				log.Fatalf("unable to run cmd: %v", err)
			}
			if cmdOut != "" {
				fmt.Println(cmdOut)
			}
		}

		fmt.Printf("%s: setting cluster settings", h.c.Name)
		clusterSettingsOut, err := h.setClusterSettings(nodeIdx)
		if err != nil {
			log.Fatalf("unable to set cluster settings: %v", err)
		}
		if clusterSettingsOut != "" {
			fmt.Println(clusterSettingsOut)
		}
		return nil, nil
	})
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
	c.Parallel(display, len(c.Nodes), 0, func(nodeIdx int) ([]byte, error) {
		sess, err := c.newSession(c.Nodes[nodeIdx])
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		var cmd string
		if c.IsLocal() {
			cmd = fmt.Sprintf(`cd ${HOME}/local/%d ; `, c.Nodes[nodeIdx])
		}
		cmd += cockroachNodeBinary(c, c.Nodes[nodeIdx]) + " sql --url " +
			r.NodeURL(c, "localhost", r.NodePort(c, c.Nodes[nodeIdx])) + " " +
			ssh.Escape(args)

		out, err := sess.CombinedOutput(cmd)
		if err != nil {
			return nil, errors.Wrapf(err, "~ %s\n%s", cmd, out)
		}

		resultChan <- result{node: c.Nodes[nodeIdx], output: string(out)}
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

type crdbStartHelper struct {
	c *SyncedCluster
	r Cockroach
}

func (h *crdbStartHelper) startNode(
	nodeIdx int, extraArgs []string, vers *version.Version,
) (string, error) {
	startCmd, err := h.generateStartCmd(nodeIdx, extraArgs, vers)
	if err != nil {
		return "", err
	}

	nodes := h.c.ServerNodes()
	sess, err := h.c.newSession(nodes[nodeIdx])
	if err != nil {
		return "", err
	}
	defer sess.Close()

	out, err := sess.CombinedOutput(startCmd)
	if err != nil {
		return "", errors.Wrapf(err, "~ %s\n%s", startCmd, out)
	}
	return strings.TrimSpace(string(out)), nil
}

func (h *crdbStartHelper) generateStartCmd(
	nodeIdx int, extraArgs []string, vers *version.Version,
) (string, error) {
	args, err := h.generateStartArgs(nodeIdx, extraArgs, vers)
	if err != nil {
		return "", err
	}

	// For a one-node cluster, use `start-single-node` to disable replication.
	// For everything else we'll fall back to using `cockroach start`.
	var startCmd string
	if h.useStartSingleNode(vers) {
		startCmd = "start-single-node"
	} else {
		startCmd = "start"
	}

	nodes := h.c.ServerNodes()
	logDir := h.c.Impl.LogDir(h.c, nodes[nodeIdx])
	binary := cockroachNodeBinary(h.c, nodes[nodeIdx])
	geoLibsFlag := ""
	if h.c.IsLocal() {
		geoLibsFlag = fmt.Sprintf(
			"--geo-libs=%s",
			cockroachLocalNodeLocation(h.c, nodes[nodeIdx], "lib"),
		)
	}
	keyCmd := h.generateKeyCmd(nodeIdx, extraArgs)

	// NB: this is awkward as when the process fails, the test runner will show an
	// unhelpful empty error (since everything has been redirected away). This is
	// unfortunately equally awkward to address.
	cmd := fmt.Sprintf(`
		ulimit -c unlimited; mkdir -p %[1]s;
		echo ">>> roachprod start: $(date)" >> %[1]s/roachprod.log;
		ps axeww -o pid -o command >> %[1]s/roachprod.log;
		[ -x /usr/bin/lslocks ] && /usr/bin/lslocks >> %[1]s/roachprod.log; %[2]s
		export ROACHPROD=%[3]d%[4]s;
		GOTRACEBACK=crash COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING=1 %[5]s \
		%[6]s %[7]s %[8]s %[9]s >> %[1]s/cockroach.stdout.log \
		                 2>> %[1]s/cockroach.stderr.log \
			|| (x=$?; cat %[1]s/cockroach.stderr.log; exit $x)`,
		logDir,                  // [1]
		keyCmd,                  // [2]
		nodes[nodeIdx],          // [3]
		h.c.Tag,                 // [4]
		h.getEnvVars(),          // [5]
		binary,                  // [6]
		startCmd,                // [7]
		strings.Join(args, " "), // [8]
		geoLibsFlag,             // [9]
	)
	return cmd, nil
}

func (h *crdbStartHelper) generateStartArgs(
	nodeIdx int, extraArgs []string, vers *version.Version,
) ([]string, error) {
	var args []string
	nodes := h.c.ServerNodes()

	args = append(args, "--background")
	if h.c.Secure {
		args = append(args, "--certs-dir="+h.c.Impl.CertsDir(h.c, nodes[nodeIdx]))
	} else {
		args = append(args, "--insecure")
	}

	dir := h.c.Impl.NodeDir(h.c, nodes[nodeIdx])
	logDir := h.c.Impl.LogDir(h.c, nodes[nodeIdx])
	if idx := argExists(extraArgs, "--store"); idx == -1 {
		args = append(args, "--store=path="+dir)
	}
	args = append(args, "--log-dir="+logDir)

	if vers.AtLeast(version.MustParse("v1.1.0")) {
		cache := 25
		if h.c.IsLocal() {
			cache /= len(nodes)
			if cache == 0 {
				cache = 1
			}
		}
		args = append(args, fmt.Sprintf("--cache=%d%%", cache))
		args = append(args, fmt.Sprintf("--max-sql-memory=%d%%", cache))
	}
	if h.c.IsLocal() {
		// This avoids annoying firewall prompts on Mac OS X.
		if vers.AtLeast(version.MustParse("v2.1.0")) {
			args = append(args, "--listen-addr=127.0.0.1")
		} else {
			args = append(args, "--host=127.0.0.1")
		}
	}

	port := h.r.NodePort(h.c, nodes[nodeIdx])
	args = append(args, fmt.Sprintf("--port=%d", port))
	args = append(args, fmt.Sprintf("--http-port=%d", GetAdminUIPort(port)))
	if locality := h.c.locality(nodes[nodeIdx]); locality != "" {
		if idx := argExists(extraArgs, "--locality"); idx == -1 {
			args = append(args, "--locality="+locality)
		}
	}

	if !h.useStartSingleNode(vers) {
		// --join flags are unsupported/unnecessary in `cockroach
		// start-single-node`. That aside, setting up --join flags is a bit
		// precise. We have every node point to node 1. For clusters running
		// <20.1, we have node 1 not point to anything (which in turn is used to
		// trigger auto-initialization node 1). For clusters running >=20.1,
		// node 1 also points to itself, and an explicit `cockroach init` is
		// needed.
		if nodes[nodeIdx] != 1 || vers.AtLeast(version.MustParse("v20.1.0")) {
			args = append(args, fmt.Sprintf("--join=%s:%d", h.c.host(1), h.r.NodePort(h.c, 1)))
		}
	}

	if h.shouldAdvertisePublicIP() {
		args = append(args, fmt.Sprintf("--advertise-host=%s", h.c.host(nodeIdx+1)))
	} else if !h.c.IsLocal() {
		// Explicitly advertise by IP address so that we don't need to
		// deal with cross-region name resolution. The `hostname -I`
		// prints all IP addresses for the host and then we'll select
		// the first from the list.
		args = append(args, "--advertise-host=$(hostname -I | awk '{print $1}')")
	}

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
	}

	// Argument template expansion is node specific (e.g. for {store-dir}).
	e := expander{
		node: nodes[nodeIdx],
	}
	for _, arg := range extraArgs {
		expandedArg, err := e.expand(h.c, arg)
		if err != nil {
			return nil, err
		}
		args = append(args, strings.Split(expandedArg, " ")...)
	}

	return args, nil
}

func (h *crdbStartHelper) initializeCluster(nodeIdx int) (string, error) {
	nodes := h.c.ServerNodes()
	initCmd := h.generateInitCmd(nodeIdx)

	sess, err := h.c.newSession(nodes[nodeIdx])
	if err != nil {
		return "", err
	}
	defer sess.Close()

	out, err := sess.CombinedOutput(initCmd)
	if err != nil {
		return "", errors.Wrapf(err, "~ %s\n%s", initCmd, out)
	}
	return strings.TrimSpace(string(out)), nil
}

func (h *crdbStartHelper) setClusterSettings(nodeIdx int) (string, error) {
	nodes := h.c.ServerNodes()
	clusterSettingCmd := h.generateClusterSettingCmd(nodeIdx)

	sess, err := h.c.newSession(nodes[nodeIdx])
	if err != nil {
		return "", err
	}
	defer sess.Close()

	out, err := sess.CombinedOutput(clusterSettingCmd)
	if err != nil {
		return "", errors.Wrapf(err, "~ %s\n%s", clusterSettingCmd, out)
	}
	return strings.TrimSpace(string(out)), nil
}

func (h *crdbStartHelper) generateClusterSettingCmd(nodeIdx int) string {
	nodes := h.c.ServerNodes()
	license := envutil.EnvOrDefaultString("COCKROACH_DEV_LICENSE", "")
	if license == "" {
		fmt.Printf("%s: COCKROACH_DEV_LICENSE unset: enterprise features will be unavailable\n",
			h.c.Name)
	}

	var clusterSettingCmd string
	if h.c.IsLocal() {
		clusterSettingCmd = `cd ${HOME}/local/1 ; `
	}

	binary := cockroachNodeBinary(h.c, nodeIdx)
	path := fmt.Sprintf("%s/%s", h.c.Impl.NodeDir(h.c, nodes[nodeIdx]), "settings-initialized")
	url := h.r.NodeURL(h.c, "localhost", h.r.NodePort(h.c, 1))

	clusterSettingCmd += fmt.Sprintf(`
		if ! test -e %s ; then
			COCKROACH_CONNECT_TIMEOUT=0 %s sql --url %s -e "
				SET CLUSTER SETTING server.remote_debugging.mode = 'any';
				SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing';
				SET CLUSTER SETTING enterprise.license = '%s';" \
			&& touch %s
		fi`, path, binary, url, license, path)
	return clusterSettingCmd
}

func (h *crdbStartHelper) generateInitCmd(nodeIdx int) string {
	nodes := h.c.ServerNodes()

	var initCmd string
	if h.c.IsLocal() {
		initCmd = `cd ${HOME}/local/1 ; `
	}

	path := fmt.Sprintf("%s/%s", h.c.Impl.NodeDir(h.c, nodes[nodeIdx]), "cluster-bootstrapped")
	url := h.r.NodeURL(h.c, "localhost", h.r.NodePort(h.c, nodes[nodeIdx]))
	binary := cockroachNodeBinary(h.c, nodeIdx)

	initCmd += fmt.Sprintf(`
		if ! test -e %[1]s ; then
			COCKROACH_CONNECT_TIMEOUT=0 %[2]s init --url %[3]s && touch %[1]s
		fi`, path, binary, url)
	return initCmd
}

func (h *crdbStartHelper) generateKeyCmd(nodeIdx int, extraArgs []string) string {
	if !StartOpts.Encrypt {
		return ""
	}

	nodes := h.c.ServerNodes()
	var storeDir string
	if idx := argExists(extraArgs, "--store"); idx == -1 {
		storeDir = h.c.Impl.NodeDir(h.c, nodes[nodeIdx])
	} else {
		storeDir = strings.TrimPrefix(extraArgs[idx], "--store=")
	}

	// Command to create the store key.
	keyCmd := fmt.Sprintf(`
		mkdir -p %[1]s;
		if [ ! -e %[1]s/aes-128.key ]; then
			openssl rand -out %[1]s/aes-128.key 48;
		fi;`, storeDir)
	return keyCmd
}

func (h *crdbStartHelper) useStartSingleNode(vers *version.Version) bool {
	return len(h.c.VMs) == 1 && vers.AtLeast(version.MustParse("v19.2.0"))
}

// distributeCerts, like the name suggests, distributes certs if it's a secure
// cluster and we're starting n1.
func (h *crdbStartHelper) distributeCerts() {
	for _, node := range h.c.ServerNodes() {
		if node == 1 && h.c.Secure {
			h.c.DistributeCerts()
			break
		}
	}
}

func (h *crdbStartHelper) shouldAdvertisePublicIP() bool {
	// If we're creating nodes that span VPC (e.g. AWS multi-region or
	// multi-cloud), we'll tell the nodes to advertise their public IPs
	// so that attaching nodes to the cluster Just Works.
	for i, vpc := range h.c.VPCs {
		if i > 0 && vpc != h.c.VPCs[0] {
			return true
		}
	}
	return false
}

func (h *crdbStartHelper) getEnvVars() string {
	var buf strings.Builder
	for _, v := range os.Environ() {
		if strings.HasPrefix(v, "COCKROACH_") {
			if buf.Len() > 0 {
				buf.WriteString(" ")
			}
			buf.WriteString(v)
		}
	}
	if len(h.c.Env) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(h.c.Env)
	}
	return buf.String()
}

func (h *crdbStartHelper) run(nodeIdx int, cmd string) (string, error) {
	nodes := h.c.ServerNodes()

	sess, err := h.c.newSession(nodes[nodeIdx])
	if err != nil {
		return "", err
	}
	defer sess.Close()

	out, err := sess.CombinedOutput(cmd)
	if err != nil {
		return "", errors.Wrapf(err, "~ %s\n%s", cmd, out)
	}
	return strings.TrimSpace(string(out)), nil
}
