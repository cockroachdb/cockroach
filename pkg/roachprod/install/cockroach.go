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
	_ "embed" // required for go:embed
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"github.com/alessio/shellescape"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
)

//go:embed scripts/start.sh
var startScript string

func cockroachNodeBinary(c *SyncedCluster, node int) string {
	if filepath.IsAbs(config.Binary) {
		return config.Binary
	}
	if !c.IsLocal() {
		return "./" + config.Binary
	}

	path := filepath.Join(c.localVMDir(node), config.Binary)
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

func getCockroachVersion(c *SyncedCluster, node int) (*version.Version, error) {
	sess, err := c.newSession(node)
	if err != nil {
		return nil, err
	}
	defer sess.Close()

	cmd := cockroachNodeBinary(c, node) + " version"
	out, err := sess.CombinedOutput(cmd + " --build-tag")
	if err != nil {
		return nil, errors.Wrapf(err, "~ %s --build-tag\n%s", cmd, out)
	}

	verString := strings.TrimSpace(string(out))
	return version.Parse(verString)
}

func argExists(args []string, target string) int {
	for i, arg := range args {
		if arg == target || strings.HasPrefix(arg, target+"=") {
			return i
		}
	}
	return -1
}

// Start the cockroach process on the cluster.
//
// Starting the first node is special-cased quite a bit, it's used to distribute
// certs, set cluster settings, and initialize the cluster. Also, if we're only
// starting a single node in the cluster and it happens to be the "first" node
// (node 1, as understood by SyncedCluster.TargetNodes), we use
// `start-single-node` (this was written to provide a short hand to start a
// single node cluster with a replication factor of one).
func (c *SyncedCluster) Start(startOpts StartOpts) error {
	if err := c.distributeCerts(); err != nil {
		return err
	}

	nodes := c.TargetNodes()
	var parallelism = 0
	if startOpts.Sequential {
		parallelism = 1
	}

	fmt.Printf("%s: starting nodes\n", c.Name)
	return c.Parallel("", len(nodes), parallelism, func(nodeIdx int) ([]byte, error) {
		vers, err := getCockroachVersion(c, nodes[nodeIdx])
		if err != nil {
			return nil, err
		}

		// NB: if cockroach started successfully, we ignore the output as it is
		// some harmless start messaging.
		if _, err := c.startNode(nodeIdx, startOpts, vers); err != nil {
			return nil, err
		}

		// We reserve a few special operations (bootstrapping, and setting
		// cluster settings) for node 1.
		if node := nodes[nodeIdx]; node != 1 {
			return nil, nil
		}

		// NB: The code blocks below are not parallelized, so it's safe for us
		// to use fmt.Printf style logging.

		// 1. We don't init invoked using `--skip-init`.
		// 2. We don't init when invoking with `start-single-node`.
		// 3. For nodes running <20.1, the --join flags are constructed in a
		//    manner such that the first node doesn't have any (see
		//   `generateStartArgs`),which prompts CRDB to auto-initialize. For
		//    nodes running >=20.1, we need to explicitly initialize.

		if startOpts.SkipInit {
			return nil, nil
		}

		shouldInit := !c.useStartSingleNode()
		if shouldInit {
			fmt.Printf("%s: initializing cluster\n", c.Name)
			initOut, err := c.initializeCluster(nodeIdx)
			if err != nil {
				return nil, errors.WithDetail(err, "unable to initialize cluster")
			}

			if initOut != "" {
				fmt.Println(initOut)
			}
		}

		// We're sure to set cluster settings after having initialized the
		// cluster.

		fmt.Printf("%s: setting cluster settings\n", c.Name)
		clusterSettingsOut, err := c.setClusterSettings(nodeIdx)
		if err != nil {
			return nil, errors.Wrap(err, "unable to set cluster settings")
		}
		if clusterSettingsOut != "" {
			fmt.Println(clusterSettingsOut)
		}
		return nil, nil
	})
}

// NodeDir returns the data directory for the given node and store.
func (c *SyncedCluster) NodeDir(nodeIndex, storeIndex int) string {
	if c.IsLocal() {
		if storeIndex != 1 {
			panic("NodeDir only supports one store for local deployments")
		}
		return filepath.Join(c.localVMDir(nodeIndex), "data")
	}
	return fmt.Sprintf("/mnt/data%d/cockroach", storeIndex)
}

// LogDir returns the logs directory for the given node.
func (c *SyncedCluster) LogDir(nodeIndex int) string {
	if c.IsLocal() {
		return filepath.Join(c.localVMDir(nodeIndex), "logs")
	}
	return "logs"
}

// CertsDir returns the certificate directory for the given node.
func (c *SyncedCluster) CertsDir(nodeIndex int) string {
	if c.IsLocal() {
		return filepath.Join(c.localVMDir(nodeIndex), "certs")
	}
	return "certs"
}

// NodeURL constructs a postgres URL.
func (c *SyncedCluster) NodeURL(host string, port int) string {
	var u url.URL
	u.User = url.User("root")
	u.Scheme = "postgres"
	u.Host = fmt.Sprintf("%s:%d", host, port)
	v := url.Values{}
	if c.Secure {
		v.Add("sslcert", c.PGUrlCertsDir+"/client.root.crt")
		v.Add("sslkey", c.PGUrlCertsDir+"/client.root.key")
		v.Add("sslrootcert", c.PGUrlCertsDir+"/ca.crt")
		v.Add("sslmode", "verify-full")
	} else {
		v.Add("sslmode", "disable")
	}
	u.RawQuery = v.Encode()
	return "'" + u.String() + "'"
}

// NodePort returns the SQL port for the given node.
func (c *SyncedCluster) NodePort(nodeIndex int) int {
	return c.VMs[nodeIndex-1].SQLPort
}

// NodeUIPort returns the AdminUI port for the given node.
func (c *SyncedCluster) NodeUIPort(nodeIndex int) int {
	return c.VMs[nodeIndex-1].AdminUIPort
}

// SQL runs `cockroach sql`, which starts a SQL shell or runs a SQL command.
//
// In interactive mode, there must be exactly one node target (as per
// TargetNodes).
//
// In non-interactive mode, a command specified via the `-e` flag is run against
// all nodes.
func (c *SyncedCluster) SQL(args []string) error {
	if len(args) == 0 || len(c.Nodes) == 1 {
		// If no arguments, we're going to get an interactive SQL shell. Require
		// exactly one target and ask SSH to provide a pseudoterminal.
		if len(args) == 0 && len(c.Nodes) != 1 {
			return fmt.Errorf("invalid number of nodes for interactive sql: %d", len(c.Nodes))
		}
		url := c.NodeURL("localhost", c.NodePort(c.Nodes[0]))
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
	if err := c.Parallel(display, len(c.Nodes), 0, func(nodeIdx int) ([]byte, error) {
		sess, err := c.newSession(c.Nodes[nodeIdx])
		if err != nil {
			return nil, err
		}
		defer sess.Close()

		var cmd string
		if c.IsLocal() {
			cmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(c.Nodes[nodeIdx]))
		}
		cmd += cockroachNodeBinary(c, c.Nodes[nodeIdx]) + " sql --url " +
			c.NodeURL("localhost", c.NodePort(c.Nodes[nodeIdx])) + " " +
			ssh.Escape(args)

		out, err := sess.CombinedOutput(cmd)
		if err != nil {
			return nil, errors.Wrapf(err, "~ %s\n%s", cmd, out)
		}

		resultChan <- result{node: c.Nodes[nodeIdx], output: string(out)}
		return nil, nil
	}); err != nil {
		return err
	}

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

func (c *SyncedCluster) startNode(
	nodeIdx int, startOpts StartOpts, vers *version.Version,
) (string, error) {
	startCmd, err := c.generateStartCmd(nodeIdx, startOpts, vers)
	if err != nil {
		return "", err
	}

	nodes := c.TargetNodes()
	if err := func() error {
		sess, err := c.newSession(nodes[nodeIdx])
		if err != nil {
			return err
		}
		defer sess.Close()

		sess.SetStdin(strings.NewReader(startCmd))
		var cmd string
		if c.IsLocal() {
			cmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(nodes[nodeIdx]))
		}
		cmd += `cat > cockroach.sh && chmod +x cockroach.sh`
		if out, err := sess.CombinedOutput(cmd); err != nil {
			return errors.Wrapf(err, "failed to upload start script: %s", out)
		}

		return nil
	}(); err != nil {
		return "", err
	}

	sess, err := c.newSession(nodes[nodeIdx])
	if err != nil {
		return "", err
	}
	defer sess.Close()

	var cmd string
	if c.IsLocal() {
		cmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(nodes[nodeIdx]))
	}
	cmd += "./cockroach.sh"
	out, err := sess.CombinedOutput(cmd)
	if err != nil {
		return "", errors.Wrapf(err, "~ %s\n%s", cmd, out)
	}
	return strings.TrimSpace(string(out)), nil
}

func (c *SyncedCluster) generateStartCmd(
	nodeIdx int, startOpts StartOpts, vers *version.Version,
) (string, error) {

	args, advertiseFirstIP, err := c.generateStartArgs(nodeIdx, startOpts, vers)
	if err != nil {
		return "", err
	}

	// For a one-node cluster, use `start-single-node` to disable replication.
	// For everything else we'll fall back to using `cockroach start`.
	var startCmd string
	if c.useStartSingleNode() {
		startCmd = "start-single-node"
	} else {
		startCmd = "start"
	}
	nodes := c.TargetNodes()
	return execStartTemplate(startTemplateData{
		LogDir: c.LogDir(nodes[nodeIdx]),
		KeyCmd: c.generateKeyCmd(nodeIdx, startOpts),
		EnvVars: append(append([]string{
			fmt.Sprintf("ROACHPROD=%s", c.roachprodEnvValue(nodes[nodeIdx])),
			"GOTRACEBACK=crash",
			"COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING=1",
		}, c.Env...), getEnvVars()...),
		Binary:           cockroachNodeBinary(c, nodes[nodeIdx]),
		StartCmd:         startCmd,
		Args:             args,
		MemoryMax:        config.MemoryMax,
		Local:            c.IsLocal(),
		AdvertiseFirstIP: advertiseFirstIP,
	})
}

type startTemplateData struct {
	Local            bool
	AdvertiseFirstIP bool
	LogDir           string
	Binary           string
	StartCmd         string
	KeyCmd           string
	MemoryMax        string
	Args             []string
	EnvVars          []string
}

func execStartTemplate(data startTemplateData) (string, error) {
	tpl, err := template.New("start").
		Funcs(template.FuncMap{"shesc": func(i interface{}) string {
			return shellescape.Quote(fmt.Sprint(i))
		}}).
		Delims("#{", "#}").
		Parse(startScript)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	if err := tpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (c *SyncedCluster) generateStartArgs(
	nodeIdx int, startOpts StartOpts, vers *version.Version,
) (_ []string, _advertiseFirstIP bool, _ error) {
	var args []string
	nodes := c.TargetNodes()

	if c.Secure {
		args = append(args, `--certs-dir`, c.CertsDir(nodes[nodeIdx]))
	} else {
		args = append(args, "--insecure")
	}

	var storeDirs []string
	if idx := argExists(startOpts.ExtraArgs, "--store"); idx == -1 {
		for i := 1; i <= startOpts.StoreCount; i++ {
			storeDir := c.NodeDir(nodes[nodeIdx], i)
			storeDirs = append(storeDirs, storeDir)
			// Place a store{i} attribute on each store to allow for zone configs
			// that use specific stores.
			args = append(args, `--store`,
				`path=`+storeDir+`,attrs=`+fmt.Sprintf("store%d", i))
		}
	} else {
		storeDir := strings.TrimPrefix(startOpts.ExtraArgs[idx], "--store=")
		storeDirs = append(storeDirs, storeDir)
	}

	if startOpts.Encrypt {
		// Encryption at rest is turned on for the cluster.
		for _, storeDir := range storeDirs {
			// TODO(windchan7): allow key size to be specified through flags.
			encryptArgs := "path=%s,key=%s/aes-128.key,old-key=plain"
			encryptArgs = fmt.Sprintf(encryptArgs, storeDir, storeDir)
			args = append(args, `--enterprise-encryption`, encryptArgs)
		}
	}

	logDir := c.LogDir(nodes[nodeIdx])
	if vers.AtLeast(version.MustParse("v21.1.0-alpha.0")) {
		// Specify exit-on-error=false to work around #62763.
		args = append(args, "--log", `file-defaults: {dir: '`+logDir+`', exit-on-error: false}`)
	} else {
		args = append(args, `--log-dir`, logDir)
	}

	cache := 25
	if c.IsLocal() {
		cache /= len(nodes)
		if cache == 0 {
			cache = 1
		}
	}
	args = append(args, fmt.Sprintf("--cache=%d%%", cache))
	args = append(args, fmt.Sprintf("--max-sql-memory=%d%%", cache))

	if c.IsLocal() {
		// This avoids annoying firewall prompts on Mac OS X.
		args = append(args, "--listen-addr=127.0.0.1")
	}

	args = append(args,
		fmt.Sprintf("--port=%d", c.NodePort(nodes[nodeIdx])),
		fmt.Sprintf("--http-port=%d", c.NodeUIPort(nodes[nodeIdx])),
	)
	if locality := c.locality(nodes[nodeIdx]); locality != "" {
		if idx := argExists(startOpts.ExtraArgs, "--locality"); idx == -1 {
			args = append(args, "--locality="+locality)
		}
	}

	if !c.useStartSingleNode() {
		// --join flags are unsupported/unnecessary in `cockroach
		// start-single-node`. That aside, setting up --join flags is a bit
		// precise. We have every node point to node 1. For clusters running
		// <20.1, we have node 1 not point to anything (which in turn is used to
		// trigger auto-initialization node 1). Since 20.1, node 1 also points to
		// itself, and an explicit `cockroach init` is needed.
		args = append(args, fmt.Sprintf("--join=%s:%d", c.host(1), c.NodePort(1)))
	}

	var advertiseFirstIP bool
	if c.shouldAdvertisePublicIP() {
		args = append(args, fmt.Sprintf("--advertise-host=%s", c.host(nodeIdx+1)))
	} else if !c.IsLocal() {
		// Explicitly advertise by IP address so that we don't need to
		// deal with cross-region name resolution. The `hostname -I`
		// prints all IP addresses for the host and then we'll select
		// the first from the list. This has to be done on the server,
		// so we pass the information that this needs to be done along.
		advertiseFirstIP = true
	}

	// Argument template expansion is node specific (e.g. for {store-dir}).
	e := expander{
		node: nodes[nodeIdx],
	}
	for _, arg := range startOpts.ExtraArgs {
		expandedArg, err := e.expand(c, arg)
		if err != nil {
			return nil, false, err
		}
		args = append(args, strings.Split(expandedArg, " ")...)
	}

	return args, advertiseFirstIP, nil
}

func (c *SyncedCluster) initializeCluster(nodeIdx int) (string, error) {
	nodes := c.TargetNodes()
	initCmd := c.generateInitCmd(nodeIdx)

	sess, err := c.newSession(nodes[nodeIdx])
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

func (c *SyncedCluster) setClusterSettings(nodeIdx int) (string, error) {
	nodes := c.TargetNodes()
	clusterSettingCmd := c.generateClusterSettingCmd(nodeIdx)

	sess, err := c.newSession(nodes[nodeIdx])
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

func (c *SyncedCluster) generateClusterSettingCmd(nodeIdx int) string {
	nodes := c.TargetNodes()
	license := envutil.EnvOrDefaultString("COCKROACH_DEV_LICENSE", "")
	if license == "" {
		fmt.Printf("%s: COCKROACH_DEV_LICENSE unset: enterprise features will be unavailable\n",
			c.Name)
	}

	var clusterSettingCmd string
	if c.IsLocal() {
		clusterSettingCmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(1))
	}

	binary := cockroachNodeBinary(c, nodes[nodeIdx])
	path := fmt.Sprintf("%s/%s", c.NodeDir(nodes[nodeIdx], 1 /* storeIndex */), "settings-initialized")
	url := c.NodeURL("localhost", c.NodePort(1))

	// We ignore failures to set remote_debugging.mode, which was
	// removed in v21.2.
	clusterSettingCmd += fmt.Sprintf(`
		if ! test -e %s ; then
			COCKROACH_CONNECT_TIMEOUT=0 %s sql --url %s -e "SET CLUSTER SETTING server.remote_debugging.mode = 'any'" || true;
			COCKROACH_CONNECT_TIMEOUT=0 %s sql --url %s -e "
				SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing';
				SET CLUSTER SETTING enterprise.license = '%s';" \
			&& touch %s
		fi`, path, binary, url, binary, url, license, path)
	return clusterSettingCmd
}

func (c *SyncedCluster) generateInitCmd(nodeIdx int) string {
	nodes := c.TargetNodes()

	var initCmd string
	if c.IsLocal() {
		initCmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(1))
	}

	path := fmt.Sprintf("%s/%s", c.NodeDir(nodes[nodeIdx], 1 /* storeIndex */), "cluster-bootstrapped")
	url := c.NodeURL("localhost", c.NodePort(nodes[nodeIdx]))
	binary := cockroachNodeBinary(c, nodes[nodeIdx])
	initCmd += fmt.Sprintf(`
		if ! test -e %[1]s ; then
			COCKROACH_CONNECT_TIMEOUT=0 %[2]s init --url %[3]s && touch %[1]s
		fi`, path, binary, url)
	return initCmd
}

func (c *SyncedCluster) generateKeyCmd(nodeIdx int, startOpts StartOpts) string {
	if !startOpts.Encrypt {
		return ""
	}

	nodes := c.TargetNodes()
	var storeDirs []string
	if idx := argExists(startOpts.ExtraArgs, "--store"); idx == -1 {
		for i := 1; i <= startOpts.StoreCount; i++ {
			storeDir := c.NodeDir(nodes[nodeIdx], i)
			storeDirs = append(storeDirs, storeDir)
		}
	} else {
		storeDir := strings.TrimPrefix(startOpts.ExtraArgs[idx], "--store=")
		storeDirs = append(storeDirs, storeDir)
	}

	// Command to create the store key.
	var keyCmd strings.Builder
	for _, storeDir := range storeDirs {
		fmt.Fprintf(&keyCmd, `
			mkdir -p %[1]s;
			if [ ! -e %[1]s/aes-128.key ]; then
				openssl rand -out %[1]s/aes-128.key 48;
			fi;`, storeDir)
	}
	return keyCmd.String()
}

func (c *SyncedCluster) useStartSingleNode() bool {
	return len(c.VMs) == 1
}

// distributeCerts distributes certs if it's a secure cluster and we're
// starting n1.
func (c *SyncedCluster) distributeCerts() error {
	for _, node := range c.TargetNodes() {
		if node == 1 && c.Secure {
			return c.DistributeCerts()
		}
	}
	return nil
}

func (c *SyncedCluster) shouldAdvertisePublicIP() bool {
	// If we're creating nodes that span VPC (e.g. AWS multi-region or
	// multi-cloud), we'll tell the nodes to advertise their public IPs
	// so that attaching nodes to the cluster Just Works.
	for i := range c.VMs {
		if i > 0 && c.VMs[i].VPC != c.VMs[0].VPC {
			return true
		}
	}
	return false
}

// getEnvVars returns all COCKROACH_* environment variables, in the form
// "key=value".
func getEnvVars() []string {
	var sl []string
	for _, v := range os.Environ() {
		if strings.HasPrefix(v, "COCKROACH_") {
			sl = append(sl, v)
		}
	}
	return sl
}
