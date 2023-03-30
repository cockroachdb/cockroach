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
	"context"
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
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

//go:embed scripts/start.sh
var startScript string

func cockroachNodeBinary(c *SyncedCluster, node Node) string {
	if filepath.IsAbs(c.Binary) {
		return c.Binary
	}
	if !c.IsLocal() {
		return "./" + c.Binary
	}

	path := filepath.Join(c.localVMDir(node), c.Binary)
	if _, err := os.Stat(path); err == nil {
		return path
	}

	// For "local" clusters we have to find the binary to run and translate it to
	// an absolute path. First, look for the binary in PATH.
	path, err := exec.LookPath(c.Binary)
	if err != nil {
		if strings.HasPrefix(c.Binary, "/") {
			return c.Binary
		}
		// We're unable to find the binary in PATH and "binary" is a relative path:
		// look in the cockroach repo.
		gopath := os.Getenv("GOPATH")
		if gopath == "" {
			return c.Binary
		}
		path = gopath + "/src/github.com/cockroachdb/cockroach/" + c.Binary
		var err2 error
		path, err2 = exec.LookPath(path)
		if err2 != nil {
			return c.Binary
		}
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return c.Binary
	}
	return path
}

func argExists(args []string, target string) int {
	for i, arg := range args {
		if arg == target || strings.HasPrefix(arg, target+"=") {
			return i
		}
	}
	return -1
}

// StartOpts houses the options needed by Start().
type StartOpts struct {
	Target     StartTarget
	Sequential bool
	// ExtraArgs are extra arguments used when starting the node. Multiple
	// arguments should be passed as separate items in the slice. For example:
	//   Instead of: []string{"--flag foo bar"}
	//   Use:        []string{"--flag", "foo", "bar"}
	ExtraArgs []string

	// ScheduleBackups starts a backup schedule once the cluster starts
	ScheduleBackups    bool
	ScheduleBackupArgs string

	// systemd limits on resources.
	NumFilesLimit int64

	// InitTarget is the node that Start will run Init on and the default node
	// that will be used when constructing join arguments.
	InitTarget int

	// -- Options that apply only to StartDefault target --

	SkipInit        bool
	StoreCount      int
	EncryptedStores bool

	// -- Options that apply only to StartTenantSQL target --
	TenantID  int
	KVAddrs   string
	KVCluster *SyncedCluster
}

// startSQLTimeout identifies the COCKROACH_CONNECT_TIMEOUT to use (in seconds)
// for sql cmds within syncedCluster.Start().
const startSQLTimeout = 1200

// StartTarget identifies what flavor of cockroach we are starting.
type StartTarget int

const (
	// StartDefault starts a "full" (KV+SQL) node (the default).
	StartDefault StartTarget = iota
	// StartTenantSQL starts a tenant SQL-only process.
	StartTenantSQL
	// StartTenantProxy starts a tenant SQL proxy process.
	StartTenantProxy
)

const defaultInitTarget = Node(1)

func (st StartTarget) String() string {
	return [...]string{
		StartDefault:     "default",
		StartTenantSQL:   "tenant SQL",
		StartTenantProxy: "tenant proxy",
	}[st]
}

// GetInitTarget returns the Node that should be used for
// initialization operations.
func (so StartOpts) GetInitTarget() Node {
	if so.InitTarget == 0 {
		return defaultInitTarget
	}
	return Node(so.InitTarget)
}

// Start the cockroach process on the cluster.
//
// Starting the first node is special-cased quite a bit, it's used to distribute
// certs, set cluster settings, and initialize the cluster. Also, if we're only
// starting a single node in the cluster and it happens to be the "first" node
// (node 1, as understood by SyncedCluster.TargetNodes), we use
// `start-single-node` (this was written to provide a short hand to start a
// single node cluster with a replication factor of one).
func (c *SyncedCluster) Start(ctx context.Context, l *logger.Logger, startOpts StartOpts) error {
	if startOpts.Target == StartTenantProxy {
		return fmt.Errorf("start tenant proxy not implemented")
	}
	switch startOpts.Target {
	case StartDefault:
		if err := c.distributeCerts(ctx, l); err != nil {
			return err
		}
	case StartTenantSQL:
		if err := c.distributeTenantCerts(ctx, l, startOpts.KVCluster, startOpts.TenantID); err != nil {
			return err
		}
	}

	nodes := c.TargetNodes()
	var parallelism = 0
	if startOpts.Sequential {
		parallelism = 1
	}

	l.Printf("%s: starting nodes", c.Name)

	// SSH retries are disabled by passing nil RunRetryOpts
	if err := c.Parallel(l, "", len(nodes), parallelism, func(nodeIdx int) (*RunResultDetails, error) {
		node := nodes[nodeIdx]
		res := &RunResultDetails{Node: node}
		// NB: if cockroach started successfully, we ignore the output as it is
		// some harmless start messaging.
		if _, err := c.startNode(ctx, l, node, startOpts); err != nil {
			res.Err = err
			return res, err
		}

		// Code that follows applies only for regular nodes.
		if startOpts.Target != StartDefault {
			return res, nil
		}

		// We reserve a few special operations (bootstrapping, and setting
		// cluster settings) to the InitTarget.
		if startOpts.GetInitTarget() != node {
			return res, nil
		}

		// NB: The code blocks below are not parallelized, so it's safe for us
		// to use fmt.Printf style logging.

		// 1. We don't init invoked using `--skip-init`.
		// 2. We don't init when invoking with `start-single-node`.

		if startOpts.SkipInit {
			return res, nil
		}

		// For single node clusters, this can be skipped because during the c.StartNode call above,
		// the `--start-single-node` flag will handle all of this for us.
		shouldInit := !c.useStartSingleNode()
		if shouldInit {
			if err := c.initializeCluster(ctx, l, node); err != nil {
				res.Err = err
				return res, errors.Wrap(err, "failed to initialize cluster")
			}
		}
		if err := c.setClusterSettings(ctx, l, node); err != nil {
			res.Err = err
			return res, errors.Wrap(err, "failed to set cluster settings")
		}
		return res, nil
	}, DefaultSSHRetryOpts); err != nil {
		return err
	}

	// Only after a successful cluster initialization should we attempt to schedule backups.
	if startOpts.ScheduleBackups && !startOpts.SkipInit && config.CockroachDevLicense != "" {
		return c.createFixedBackupSchedule(ctx, l, startOpts.ScheduleBackupArgs)
	}
	return nil
}

// NodeDir returns the data directory for the given node and store.
func (c *SyncedCluster) NodeDir(node Node, storeIndex int) string {
	if c.IsLocal() {
		if storeIndex != 1 {
			panic("NodeDir only supports one store for local deployments")
		}
		return filepath.Join(c.localVMDir(node), "data")
	}
	return fmt.Sprintf("/mnt/data%d/cockroach", storeIndex)
}

// LogDir returns the logs directory for the given node.
func (c *SyncedCluster) LogDir(node Node) string {
	if c.IsLocal() {
		return filepath.Join(c.localVMDir(node), "logs")
	}
	return "logs"
}

// CertsDir returns the certificate directory for the given node.
func (c *SyncedCluster) CertsDir(node Node) string {
	if c.IsLocal() {
		return filepath.Join(c.localVMDir(node), "certs")
	}
	return "certs"
}

// NodeURL constructs a postgres URL.
func (c *SyncedCluster) NodeURL(host string, port int, tenantName string) string {
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
	if tenantName != "" {
		v.Add("options", fmt.Sprintf("-ccluster=%s", tenantName))
	}
	u.RawQuery = v.Encode()
	return "'" + u.String() + "'"
}

// NodePort returns the SQL port for the given node.
func (c *SyncedCluster) NodePort(node Node) int {
	return c.VMs[node-1].SQLPort
}

// NodeUIPort returns the AdminUI port for the given node.
func (c *SyncedCluster) NodeUIPort(node Node) int {
	return c.VMs[node-1].AdminUIPort
}

// ExecOrInteractiveSQL ssh's onto a single node and executes `./ cockroach sql`
// with the provided args, potentially opening an interactive session. Note
// that the caller can pass the `--e` flag to execute sql cmds and exit the
// session. See `./cockroch sql -h` for more options.
//
// CAUTION: this function should not be used by roachtest writers. Use ExecSQL below.
func (c *SyncedCluster) ExecOrInteractiveSQL(
	ctx context.Context, l *logger.Logger, tenantName string, args []string,
) error {
	if len(c.Nodes) != 1 {
		return fmt.Errorf("invalid number of nodes for interactive sql: %d", len(c.Nodes))
	}
	url := c.NodeURL("localhost", c.NodePort(c.Nodes[0]), tenantName)
	binary := cockroachNodeBinary(c, c.Nodes[0])
	allArgs := []string{binary, "sql", "--url", url}
	allArgs = append(allArgs, ssh.Escape(args))
	return c.SSH(ctx, l, []string{"-t"}, allArgs)
}

// ExecSQL runs a `cockroach sql` .
// It is assumed that the args include the -e flag.
func (c *SyncedCluster) ExecSQL(
	ctx context.Context, l *logger.Logger, tenantName string, args []string,
) error {
	type result struct {
		node   Node
		output string
	}
	resultChan := make(chan result, len(c.Nodes))

	display := fmt.Sprintf("%s: executing sql", c.Name)
	if err := c.Parallel(l, display, len(c.Nodes), 0, func(nodeIdx int) (*RunResultDetails, error) {
		node := c.Nodes[nodeIdx]

		var cmd string
		if c.IsLocal() {
			cmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(node))
		}
		cmd += cockroachNodeBinary(c, node) + " sql --url " +
			c.NodeURL("localhost", c.NodePort(node), tenantName) + " " +
			ssh.Escape(args)

		sess := c.newSession(l, node, cmd, withDebugName("run-sql"))
		defer sess.Close()

		out, cmdErr := sess.CombinedOutput(ctx)
		res := newRunResultDetails(node, cmdErr)
		res.CombinedOut = out

		if res.Err != nil {
			return res, errors.Wrapf(res.Err, "~ %s\n%s", cmd, res.CombinedOut)
		}
		resultChan <- result{node: node, output: string(res.CombinedOut)}
		return res, nil
	}, DefaultSSHRetryOpts); err != nil {
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
		l.Printf("node %d:\n%s", r.node, r.output)
	}

	return nil
}

func (c *SyncedCluster) startNode(
	ctx context.Context, l *logger.Logger, node Node, startOpts StartOpts,
) (string, error) {
	startCmd, err := c.generateStartCmd(ctx, l, node, startOpts)
	if err != nil {
		return "", err
	}

	if err := func() error {
		var cmd string
		if c.IsLocal() {
			cmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(node))
		}
		cmd += `cat > cockroach.sh && chmod +x cockroach.sh`

		sess := c.newSession(l, node, cmd)
		defer sess.Close()

		sess.SetStdin(strings.NewReader(startCmd))
		if out, err := sess.CombinedOutput(ctx); err != nil {
			return errors.Wrapf(err, "failed to upload start script: %s", out)
		}

		return nil
	}(); err != nil {
		return "", err
	}

	var cmd string
	if c.IsLocal() {
		cmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(node))
	}
	cmd += "./cockroach.sh"

	sess := c.newSession(l, node, cmd)
	defer sess.Close()

	out, err := sess.CombinedOutput(ctx)
	if err != nil {
		return "", errors.Wrapf(err, "~ %s\n%s", cmd, out)
	}
	return strings.TrimSpace(string(out)), nil
}

func (c *SyncedCluster) generateStartCmd(
	ctx context.Context, l *logger.Logger, node Node, startOpts StartOpts,
) (string, error) {
	args, err := c.generateStartArgs(ctx, l, node, startOpts)
	if err != nil {
		return "", err
	}
	keyCmd, err := c.generateKeyCmd(ctx, l, node, startOpts)
	if err != nil {
		return "", err
	}

	return execStartTemplate(startTemplateData{
		LogDir: c.LogDir(node),
		KeyCmd: keyCmd,
		EnvVars: append(append([]string{
			fmt.Sprintf("ROACHPROD=%s", c.roachprodEnvValue(node)),
			"GOTRACEBACK=crash",
			"COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING=1",
		}, c.Env...), getEnvVars()...),
		Binary:        cockroachNodeBinary(c, node),
		Args:          args,
		MemoryMax:     config.MemoryMax,
		NumFilesLimit: startOpts.NumFilesLimit,
		Local:         c.IsLocal(),
	})
}

type startTemplateData struct {
	Local         bool
	LogDir        string
	Binary        string
	KeyCmd        string
	MemoryMax     string
	NumFilesLimit int64
	Args          []string
	EnvVars       []string
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

// generateStartArgs generates cockroach binary arguments for starting a node.
// The first argument is the command (e.g. "start").
func (c *SyncedCluster) generateStartArgs(
	ctx context.Context, l *logger.Logger, node Node, startOpts StartOpts,
) ([]string, error) {
	var args []string

	switch startOpts.Target {
	case StartDefault:
		if c.useStartSingleNode() {
			args = []string{"start-single-node"}
		} else {
			args = []string{"start"}
		}

	case StartTenantSQL:
		args = []string{"mt", "start-sql"}

	case StartTenantProxy:
		// args = []string{"mt", "start-proxy"}
		panic("unimplemented")

	default:
		return nil, errors.Errorf("unsupported start target %v", startOpts.Target)
	}

	// Flags common to all targets.

	if c.Secure {
		args = append(args, `--certs-dir`, c.CertsDir(node))
	} else {
		args = append(args, "--insecure")
	}

	logDir := c.LogDir(node)
	idx1 := argExists(startOpts.ExtraArgs, "--log")
	idx2 := argExists(startOpts.ExtraArgs, "--log-config-file")

	// if neither --log nor --log-config-file are present
	if idx1 == -1 && idx2 == -1 {
		// Specify exit-on-error=false to work around #62763.
		args = append(args, "--log", `file-defaults: {dir: '`+logDir+`', exit-on-error: false}`)
	}

	listenHost := ""
	if c.IsLocal() {
		// This avoids annoying firewall prompts on Mac OS X.
		listenHost = "127.0.0.1"
	}

	if startOpts.Target == StartTenantSQL {
		args = append(args, fmt.Sprintf("--sql-addr=%s:%d", listenHost, c.NodePort(node)))
	} else {
		args = append(args, fmt.Sprintf("--listen-addr=%s:%d", listenHost, c.NodePort(node)))
	}
	args = append(args, fmt.Sprintf("--http-addr=%s:%d", listenHost, c.NodeUIPort(node)))

	if !c.IsLocal() {
		advertiseHost := ""
		if c.shouldAdvertisePublicIP() {
			advertiseHost = c.Host(node)
		} else {
			advertiseHost = c.VMs[node-1].PrivateIP
		}
		args = append(args,
			fmt.Sprintf("--advertise-addr=%s:%d", advertiseHost, c.NodePort(node)),
		)
	}

	// --join flags are unsupported/unnecessary in `cockroach start-single-node`.
	if startOpts.Target == StartDefault && !c.useStartSingleNode() {
		initTarget := startOpts.GetInitTarget()
		args = append(args, fmt.Sprintf("--join=%s:%d", c.Host(initTarget), c.NodePort(initTarget)))
	}
	if startOpts.Target == StartTenantSQL {
		args = append(args, fmt.Sprintf("--kv-addrs=%s", startOpts.KVAddrs))
		args = append(args, fmt.Sprintf("--tenant-id=%d", startOpts.TenantID))
	}

	if startOpts.Target == StartDefault {
		args = append(args, c.generateStartFlagsKV(node, startOpts)...)
	}

	if startOpts.Target == StartDefault || startOpts.Target == StartTenantSQL {
		args = append(args, c.generateStartFlagsSQL()...)
	}

	args = append(args, startOpts.ExtraArgs...)

	// Argument template expansion is node specific (e.g. for {store-dir}).
	e := expander{
		node: node,
	}
	for i, arg := range args {
		expandedArg, err := e.expand(ctx, l, c, arg)
		if err != nil {
			return nil, err
		}
		args[i] = expandedArg
	}

	return args, nil
}

// generateStartFlagsKV generates `cockroach start` arguments that are relevant
// for the KV and storage layers (and consequently are never used by
// `cockroach mt start-sql`).
func (c *SyncedCluster) generateStartFlagsKV(node Node, startOpts StartOpts) []string {
	var args []string
	var storeDirs []string
	if idx := argExists(startOpts.ExtraArgs, "--store"); idx == -1 {
		for i := 1; i <= startOpts.StoreCount; i++ {
			storeDir := c.NodeDir(node, i)
			storeDirs = append(storeDirs, storeDir)
			// Place a store{i} attribute on each store to allow for zone configs
			// that use specific stores. Note that `i` is 1 most of the time, since
			// it's the i-th store on the *current* node. This isn't always useful,
			// for example it doesn't let one single out a specific node. We add
			// nodeX-flavor attributes for that.
			args = append(args, `--store`,
				fmt.Sprintf(`path=%s,attrs=store%d:node%d:node%dstore%d`, storeDir, i, node, node, i))
		}
	} else if startOpts.ExtraArgs[idx] == "--store=" {
		// The flag and path were provided together. Strip the flag prefix.
		storeDir := strings.TrimPrefix(startOpts.ExtraArgs[idx], "--store=")
		storeDirs = append(storeDirs, storeDir)
	} else {
		// Else, the store flag and path were specified as separate arguments. The
		// path is the subsequent arg.
		storeDirs = append(storeDirs, startOpts.ExtraArgs[idx+1])
	}

	if startOpts.EncryptedStores {
		// Encryption at rest is turned on for the cluster.
		for _, storeDir := range storeDirs {
			// TODO(windchan7): allow key size to be specified through flags.
			encryptArgs := "path=%s,key=%s/aes-128.key,old-key=plain"
			encryptArgs = fmt.Sprintf(encryptArgs, storeDir, storeDir)
			args = append(args, `--enterprise-encryption`, encryptArgs)
		}
	}

	args = append(args, fmt.Sprintf("--cache=%d%%", c.maybeScaleMem(25)))

	if locality := c.locality(node); locality != "" {
		if idx := argExists(startOpts.ExtraArgs, "--locality"); idx == -1 {
			args = append(args, "--locality="+locality)
		}
	}
	return args
}

// generateStartFlagsSQL generates `cockroach start` and `cockroach mt
// start-sql` arguments that are relevant for the SQL layers, used by both KV
// and storage layers (and in particular, are never used by `
func (c *SyncedCluster) generateStartFlagsSQL() []string {
	var args []string
	args = append(args, fmt.Sprintf("--max-sql-memory=%d%%", c.maybeScaleMem(25)))
	return args
}

// maybeScaleMem is used to scale down a memory percentage when the cluster is
// local.
func (c *SyncedCluster) maybeScaleMem(val int) int {
	if c.IsLocal() {
		val /= len(c.Nodes)
		if val == 0 {
			val = 1
		}
	}
	return val
}

func (c *SyncedCluster) initializeCluster(ctx context.Context, l *logger.Logger, node Node) error {
	l.Printf("%s: initializing cluster\n", c.Name)
	cmd := c.generateInitCmd(node)

	sess := c.newSession(l, node, cmd, withDebugName("init-cluster"))
	defer sess.Close()

	out, err := sess.CombinedOutput(ctx)
	if err != nil {
		return errors.Wrapf(err, "~ %s\n%s", cmd, out)
	}

	if out := strings.TrimSpace(string(out)); out != "" {
		l.Printf(out)
	}
	return nil
}

func (c *SyncedCluster) setClusterSettings(ctx context.Context, l *logger.Logger, node Node) error {
	l.Printf("%s: setting cluster settings", c.Name)
	cmd := c.generateClusterSettingCmd(l, node)

	sess := c.newSession(l, node, cmd, withDebugName("set-cluster-settings"))
	defer sess.Close()

	out, err := sess.CombinedOutput(ctx)
	if err != nil {
		return errors.Wrapf(err, "~ %s\n%s", cmd, out)
	}
	if out := strings.TrimSpace(string(out)); out != "" {
		l.Printf(out)
	}
	return nil
}

func (c *SyncedCluster) generateClusterSettingCmd(l *logger.Logger, node Node) string {
	if config.CockroachDevLicense == "" {
		l.Printf("%s: COCKROACH_DEV_LICENSE unset: enterprise features will be unavailable\n",
			c.Name)
	}

	var clusterSettingCmd string
	if c.IsLocal() {
		clusterSettingCmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(node))
	}

	binary := cockroachNodeBinary(c, node)
	path := fmt.Sprintf("%s/%s", c.NodeDir(node, 1 /* storeIndex */), "settings-initialized")
	url := c.NodeURL("localhost", c.NodePort(node), "" /* tenantName */)

	// We ignore failures to set remote_debugging.mode, which was
	// removed in v21.2.
	clusterSettingCmd += fmt.Sprintf(`
		if ! test -e %s ; then
			COCKROACH_CONNECT_TIMEOUT=%d %s sql --url %s -e "SET CLUSTER SETTING server.remote_debugging.mode = 'any'" || true;
			COCKROACH_CONNECT_TIMEOUT=%d %s sql --url %s -e "
				SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing';
				SET CLUSTER SETTING enterprise.license = '%s';" \
			&& touch %s
		fi`, path, startSQLTimeout, binary, url, startSQLTimeout, binary, url, config.CockroachDevLicense, path)
	return clusterSettingCmd
}

func (c *SyncedCluster) generateInitCmd(node Node) string {
	var initCmd string
	if c.IsLocal() {
		initCmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(node))
	}

	path := fmt.Sprintf("%s/%s", c.NodeDir(node, 1 /* storeIndex */), "cluster-bootstrapped")
	url := c.NodeURL("localhost", c.NodePort(node), "" /* tenantName */)
	binary := cockroachNodeBinary(c, node)
	initCmd += fmt.Sprintf(`
		if ! test -e %[1]s ; then
			COCKROACH_CONNECT_TIMEOUT=%[4]d %[2]s init --url %[3]s && touch %[1]s
		fi`, path, binary, url, startSQLTimeout)
	return initCmd
}

func (c *SyncedCluster) generateKeyCmd(
	ctx context.Context, l *logger.Logger, node Node, startOpts StartOpts,
) (string, error) {
	if !startOpts.EncryptedStores {
		return "", nil
	}

	var storeDirs []string
	if storeArgIdx := argExists(startOpts.ExtraArgs, "--store"); storeArgIdx == -1 {
		for i := 1; i <= startOpts.StoreCount; i++ {
			storeDir := c.NodeDir(node, i)
			storeDirs = append(storeDirs, storeDir)
		}
	} else if startOpts.ExtraArgs[storeArgIdx] == "--store=" {
		// The flag and path were provided together. Strip the flag prefix.
		storeDir := strings.TrimPrefix(startOpts.ExtraArgs[storeArgIdx], "--store=")
		storeDirs = append(storeDirs, storeDir)
	} else {
		// Else, the store flag and path were specified as separate arguments. The
		// path is the subsequent arg.
		storeDirs = append(storeDirs, startOpts.ExtraArgs[storeArgIdx+1])
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

	e := expander{node: node}
	expanded, err := e.expand(ctx, l, c, keyCmd.String())
	if err != nil {
		return "", err
	}
	return expanded, nil
}

func (c *SyncedCluster) useStartSingleNode() bool {
	return len(c.VMs) == 1
}

// distributeCerts distributes certs if it's a secure cluster and we're
// starting n1.
func (c *SyncedCluster) distributeCerts(ctx context.Context, l *logger.Logger) error {
	for _, node := range c.TargetNodes() {
		if node == 1 && c.Secure {
			return c.DistributeCerts(ctx, l)
		}
	}
	return nil
}

// distributeCerts distributes certs if it's a secure cluster.
func (c *SyncedCluster) distributeTenantCerts(
	ctx context.Context, l *logger.Logger, hostCluster *SyncedCluster, tenantID int,
) error {
	if c.Secure {
		return c.DistributeTenantCerts(ctx, l, hostCluster, tenantID)
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

// createFixedBackupSchedule creates a cluster backup schedule which, by
// default, runs an incremental every 15 minutes and a full every hour. On
// `roachprod create`, the user can provide a different recurrence using the
// 'schedule-backup-args' flag. If roachprod is local, the backups get stored in
// nodelocal, and otherwise in 'gs://cockroachdb-backup-testing'.
// This cmd also ensures that only one schedule will be created for the cluster.
func (c *SyncedCluster) createFixedBackupSchedule(
	ctx context.Context, l *logger.Logger, scheduledBackupArgs string,
) error {
	externalStoragePath := `gs://cockroachdb-backup-testing`
	for _, cloud := range c.Clouds() {
		if !strings.Contains(cloud, gce.ProviderName) {
			l.Printf(`no scheduled backup created as there exists a vm not on google cloud`)
			return nil
		}
	}
	l.Printf("%s: creating backup schedule", c.Name)
	auth := "AUTH=implicit"
	collectionPath := fmt.Sprintf(`%s/roachprod-scheduled-backups/%s/%v?%s`,
		externalStoragePath, c.Name, timeutil.Now().UnixNano(), auth)

	// Default scheduled backup runs a full backup every hour and an incremental
	// every 15 minutes.
	scheduleArgs := `RECURRING '*/15 * * * *' FULL BACKUP '@hourly' WITH SCHEDULE OPTIONS first_run = 'now'`
	if scheduledBackupArgs != "" {
		scheduleArgs = scheduledBackupArgs
	}
	createScheduleCmd := fmt.Sprintf(`CREATE SCHEDULE IF NOT EXISTS test_only_backup FOR BACKUP INTO '%s' %s`,
		collectionPath, scheduleArgs)

	node := c.Nodes[0]
	binary := cockroachNodeBinary(c, node)
	url := c.NodeURL("localhost", c.NodePort(node), "" /* tenantName */)
	fullCmd := fmt.Sprintf(`COCKROACH_CONNECT_TIMEOUT=%d %s sql --url %s -e %q`,
		startSQLTimeout, binary, url, createScheduleCmd)
	// Instead of using `c.ExecSQL()`, use the more flexible c.newSession(), which allows us to
	// 1) prefix the schedule backup cmd with COCKROACH_CONNECT_TIMEOUT.
	// 2) run the command against the first node in the cluster target.
	sess := c.newSession(l, node, fullCmd, withDebugName("init-backup-schedule"))
	defer sess.Close()

	out, err := sess.CombinedOutput(ctx)
	if err != nil {
		return errors.Wrapf(err, "~ %s\n%s", fullCmd, out)
	}

	if out := strings.TrimSpace(string(out)); out != "" {
		l.Printf(out)
	}
	return nil
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
