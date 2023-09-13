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
	"runtime"
	"strings"
	"text/template"

	"github.com/alessio/shellescape"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

	// JoinTargets is the list of nodes that will be used when constructing join
	// arguments. If empty, the default is to use the InitTarget.
	JoinTargets []int

	// SQLPort is the port on which the cockroach process is listening for SQL
	// connections. Default (0) will find an open port.
	SQLPort int

	// AdminUIPort is the port on which the cockroach process is listening for
	// HTTP traffic for the Admin UI. Default (0) will find an open port.
	AdminUIPort int

	// -- Options that apply only to StartDefault target --

	SkipInit        bool
	StoreCount      int
	EncryptedStores bool

	// -- Options that apply only to StartTenantSQL target --
	TenantName     string
	TenantID       int
	TenantInstance int
	KVAddrs        string
	KVCluster      *SyncedCluster
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

// GetJoinTargets returns the list of Nodes that should be used for
// join operations. If no join targets are specified, the init target
// is used.
func (so StartOpts) GetJoinTargets() []Node {
	nodes := make([]Node, len(so.JoinTargets))
	for i, n := range so.JoinTargets {
		nodes[i] = Node(n)
	}
	if len(nodes) == 0 {
		nodes = []Node{so.GetInitTarget()}
	}
	return nodes
}

// maybeRegisterServices registers the SQL and Admin UI DNS services for the
// cluster if no previous services for the tenant or host cluster are found. Any
// ports specified in the startOpts are used for the services. If no ports are
// specified, a search for open ports will be performed and selected for use.
func (c *SyncedCluster) maybeRegisterServices(
	ctx context.Context, l *logger.Logger, startOpts StartOpts,
) error {
	serviceMap, err := c.MapServices(ctx, startOpts.TenantName, startOpts.TenantInstance)
	if err != nil {
		return err
	}
	tenantName := SystemTenantName
	serviceMode := ServiceModeShared
	if startOpts.Target == StartTenantSQL {
		tenantName = startOpts.TenantName
		serviceMode = ServiceModeExternal
	}

	mu := syncutil.Mutex{}
	servicesToRegister := make(ServiceDescriptors, 0)
	err = c.Parallel(ctx, l, c.Nodes, func(ctx context.Context, node Node) (*RunResultDetails, error) {
		services := make(ServiceDescriptors, 0)
		res := &RunResultDetails{Node: node}
		if _, ok := serviceMap[node][ServiceTypeSQL]; !ok {
			services = append(services, ServiceDesc{
				TenantName:  tenantName,
				ServiceType: ServiceTypeSQL,
				ServiceMode: serviceMode,
				Node:        node,
				Port:        startOpts.SQLPort,
				Instance:    startOpts.TenantInstance,
			})
		}
		if _, ok := serviceMap[node][ServiceTypeUI]; !ok {
			services = append(services, ServiceDesc{
				TenantName:  tenantName,
				ServiceType: ServiceTypeUI,
				ServiceMode: serviceMode,
				Node:        node,
				Port:        startOpts.AdminUIPort,
				Instance:    startOpts.TenantInstance,
			})
		}
		requiredPorts := 0
		for _, service := range services {
			if service.Port == 0 {
				requiredPorts++
			}
		}
		if requiredPorts > 0 {
			openPorts, err := c.FindOpenPorts(ctx, l, node, config.DefaultOpenPortStart, requiredPorts)
			if err != nil {
				res.Err = err
				return res, errors.Wrapf(err, "failed to find %d open ports", requiredPorts)
			}
			for idx := range services {
				if services[idx].Port != 0 {
					continue
				}
				services[idx].Port = openPorts[0]
				openPorts = openPorts[1:]
			}
		}
		if err != nil {
			return nil, err
		}

		mu.Lock()
		defer mu.Unlock()
		servicesToRegister = append(servicesToRegister, services...)
		return res, nil
	})
	if err != nil {
		return err
	}
	return c.RegisterServices(ctx, servicesToRegister)
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
	// Local clusters do not support specifying ports. An error is returned if we
	// detect that they were set.
	if c.IsLocal() && (startOpts.SQLPort != 0 || startOpts.AdminUIPort != 0) {
		// We don't need to return an error if the ports are the default values
		// specified in DefaultStartOps, as these have not been specified explicitly
		// by the user.
		if startOpts.SQLPort != config.DefaultSQLPort || startOpts.AdminUIPort != config.DefaultAdminUIPort {
			return fmt.Errorf("local clusters do not support specifying ports")
		}
		startOpts.SQLPort = 0
		startOpts.AdminUIPort = 0
	}

	err := c.maybeRegisterServices(ctx, l, startOpts)
	if err != nil {
		return err
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
	if err := c.Parallel(ctx, l, nodes, func(ctx context.Context, node Node) (*RunResultDetails, error) {
		// NB: if cockroach started successfully, we ignore the output as it is
		// some harmless start messaging.
		res, err := c.startNode(ctx, l, node, startOpts)
		if err != nil || res.Err != nil {
			// If err is non-nil, then this will not be retried, but if res.Err is non-nil, it will be.
			return res, err
		}

		// Code that follows applies only for regular nodes.
		// We reserve a few special operations (bootstrapping, and setting
		// cluster settings) to the InitTarget.
		if startOpts.Target != StartDefault || startOpts.GetInitTarget() != node || startOpts.SkipInit {
			return res, nil
		}

		// NB: The code blocks below are not parallelized, so it's safe for us
		// to use fmt.Printf style logging.

		// For single node clusters, this can be skipped because during the c.StartNode call above,
		// the `--start-single-node` flag will handle all of this for us.
		shouldInit := !c.useStartSingleNode()
		if shouldInit {
			if res, err := c.initializeCluster(ctx, l, node); err != nil || res.Err != nil {
				// If err is non-nil, then this will not be retried, but if res.Err is non-nil, it will be.
				return res, err
			}
		}
		return c.setClusterSettings(ctx, l, node)
	}, WithConcurrency(parallelism)); err != nil {
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
func (c *SyncedCluster) LogDir(node Node, tenantName string, instance int) string {
	dirName := "logs" + tenantDirSuffix(tenantName, instance)
	if c.IsLocal() {
		return filepath.Join(c.localVMDir(node), dirName)
	}
	return dirName
}

// TenantStoreDir returns the data directory for a given tenant.
func (c *SyncedCluster) TenantStoreDir(node Node, tenantName string, instance int) string {
	dataDir := fmt.Sprintf("data%s", tenantDirSuffix(tenantName, instance))
	if c.IsLocal() {
		return filepath.Join(c.localVMDir(node), dataDir)
	}

	return dataDir
}

// tenantDirSuffix returns the suffix to use for tenant-specific directories.
// This suffix consists of the tenant name and instance number (if applicable).
func tenantDirSuffix(tenantName string, instance int) string {
	if tenantName != "" && tenantName != SystemTenantName {
		return fmt.Sprintf("-%s-%d", tenantName, instance)
	}
	return ""
}

// CertsDir returns the certificate directory for the given node.
func (c *SyncedCluster) CertsDir(node Node) string {
	if c.IsLocal() {
		return filepath.Join(c.localVMDir(node), "certs")
	}
	return "certs"
}

// NodeURL constructs a postgres URL. If sharedTenantName is not empty, it will
// be used as the virtual cluster name in the URL. This is used to connect to a
// shared process hosting multiple tenants.
func (c *SyncedCluster) NodeURL(host string, port int, sharedTenantName string) string {
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
	if sharedTenantName != "" {
		v.Add("options", fmt.Sprintf("-ccluster=%s", sharedTenantName))
	}
	u.RawQuery = v.Encode()
	return "'" + u.String() + "'"
}

// NodePort returns the system tenant's SQL port for the given node.
func (c *SyncedCluster) NodePort(ctx context.Context, node Node) (int, error) {
	desc, err := c.DiscoverService(ctx, node, SystemTenantName, ServiceTypeSQL, 0)
	if err != nil {
		return 0, err
	}
	return desc.Port, nil
}

// NodeUIPort returns the system tenant's AdminUI port for the given node.
func (c *SyncedCluster) NodeUIPort(ctx context.Context, node Node) (int, error) {
	desc, err := c.DiscoverService(ctx, node, SystemTenantName, ServiceTypeUI, 0)
	if err != nil {
		return 0, err
	}
	return desc.Port, nil
}

// ExecOrInteractiveSQL ssh's onto a single node and executes `./ cockroach sql`
// with the provided args, potentially opening an interactive session. Note
// that the caller can pass the `--e` flag to execute sql cmds and exit the
// session. See `./cockroch sql -h` for more options.
//
// CAUTION: this function should not be used by roachtest writers. Use ExecSQL below.
func (c *SyncedCluster) ExecOrInteractiveSQL(
	ctx context.Context, l *logger.Logger, tenantName string, tenantInstance int, args []string,
) error {
	if len(c.Nodes) != 1 {
		return fmt.Errorf("invalid number of nodes for interactive sql: %d", len(c.Nodes))
	}
	desc, err := c.DiscoverService(ctx, c.Nodes[0], tenantName, ServiceTypeSQL, tenantInstance)
	if err != nil {
		return err
	}
	if tenantName == "" {
		tenantName = SystemTenantName
	}
	sharedTenantName := ""
	if desc.ServiceMode == ServiceModeShared {
		sharedTenantName = tenantName
	}
	url := c.NodeURL("localhost", desc.Port, sharedTenantName)
	binary := cockroachNodeBinary(c, c.Nodes[0])
	allArgs := []string{binary, "sql", "--url", url}
	allArgs = append(allArgs, ssh.Escape(args))
	return c.SSH(ctx, l, []string{"-t"}, allArgs)
}

// ExecSQL runs a `cockroach sql` .
// It is assumed that the args include the -e flag.
func (c *SyncedCluster) ExecSQL(
	ctx context.Context,
	l *logger.Logger,
	nodes Nodes,
	tenantName string,
	tenantInstance int,
	args []string,
) error {
	display := fmt.Sprintf("%s: executing sql", c.Name)
	results, _, err := c.ParallelE(ctx, l, nodes, func(ctx context.Context, node Node) (*RunResultDetails, error) {
		desc, err := c.DiscoverService(ctx, node, tenantName, ServiceTypeSQL, tenantInstance)
		if err != nil {
			return nil, err
		}
		sharedTenantName := ""
		if desc.ServiceMode == ServiceModeShared {
			sharedTenantName = tenantName
		}
		var cmd string
		if c.IsLocal() {
			cmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(node))
		}
		cmd += cockroachNodeBinary(c, node) + " sql --url " +
			c.NodeURL("localhost", desc.Port, sharedTenantName) + " " +
			ssh.Escape(args)

		return c.runCmdOnSingleNode(ctx, l, node, cmd, defaultCmdOpts("run-sql"))
	}, WithDisplay(display), WithWaitOnFail())

	if err != nil {
		return err
	}

	for _, r := range results {
		l.Printf("node %d:\n%s", r.Node, r.CombinedOut)
	}

	return nil
}

func (c *SyncedCluster) startNode(
	ctx context.Context, l *logger.Logger, node Node, startOpts StartOpts,
) (*RunResultDetails, error) {
	startCmd, err := c.generateStartCmd(ctx, l, node, startOpts)
	if err != nil {
		return nil, err
	}
	var uploadCmd string
	if c.IsLocal() {
		uploadCmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(node))
	}
	uploadCmd += `cat > cockroach.sh && chmod +x cockroach.sh`

	var res = &RunResultDetails{}
	uploadOpts := defaultCmdOpts("upload-start-script")
	uploadOpts.stdin = strings.NewReader(startCmd)
	res, err = c.runCmdOnSingleNode(ctx, l, node, uploadCmd, uploadOpts)
	if err != nil || res.Err != nil {
		return res, err
	}

	var runScriptCmd string
	if c.IsLocal() {
		runScriptCmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(node))
	}
	runScriptCmd += "./cockroach.sh"
	return c.runCmdOnSingleNode(ctx, l, node, runScriptCmd, defaultCmdOpts("run-start-script"))
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
		LogDir: c.LogDir(node, startOpts.TenantName, startOpts.TenantInstance),
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

	logDir := c.LogDir(node, startOpts.TenantName, startOpts.TenantInstance)
	idx1 := argExists(startOpts.ExtraArgs, "--log")
	idx2 := argExists(startOpts.ExtraArgs, "--log-config-file")

	// if neither --log nor --log-config-file are present
	if idx1 == -1 && idx2 == -1 {
		// Specify exit-on-error=false to work around #62763.
		args = append(args, "--log", `file-defaults: {dir: '`+logDir+`', exit-on-error: false}`)
	}

	listenHost := ""
	if c.IsLocal() && runtime.GOOS == "darwin " {
		// This avoids annoying firewall prompts on Mac OS X.
		listenHost = "127.0.0.1"
	}

	tenantName := startOpts.TenantName
	instance := startOpts.TenantInstance
	var sqlPort int
	if startOpts.Target == StartTenantSQL {
		desc, err := c.DiscoverService(ctx, node, tenantName, ServiceTypeSQL, instance)
		if err != nil {
			return nil, err
		}
		sqlPort = desc.Port
		args = append(args, fmt.Sprintf("--sql-addr=%s:%d", listenHost, sqlPort))
	} else {
		tenantName = SystemTenantName
		// System tenant instance is always 0.
		instance = 0
		desc, err := c.DiscoverService(ctx, node, tenantName, ServiceTypeSQL, instance)
		if err != nil {
			return nil, err
		}
		sqlPort = desc.Port
		args = append(args, fmt.Sprintf("--listen-addr=%s:%d", listenHost, sqlPort))
	}
	desc, err := c.DiscoverService(ctx, node, tenantName, ServiceTypeUI, instance)
	if err != nil {
		return nil, err
	}
	args = append(args, fmt.Sprintf("--http-addr=%s:%d", listenHost, desc.Port))

	if !c.IsLocal() {
		advertiseHost := ""
		if c.shouldAdvertisePublicIP() {
			advertiseHost = c.Host(node)
		} else {
			advertiseHost = c.VMs[node-1].PrivateIP
		}
		args = append(args,
			fmt.Sprintf("--advertise-addr=%s:%d", advertiseHost, sqlPort),
		)
	}

	// --join flags are unsupported/unnecessary in `cockroach start-single-node`.
	if startOpts.Target == StartDefault && !c.useStartSingleNode() {
		joinTargets := startOpts.GetJoinTargets()
		addresses := make([]string, len(joinTargets))
		for i, joinNode := range startOpts.GetJoinTargets() {
			desc, err := c.DiscoverService(ctx, joinNode, SystemTenantName, ServiceTypeSQL, 0)
			if err != nil {
				return nil, err
			}
			addresses[i] = fmt.Sprintf("%s:%d", c.Host(joinNode), desc.Port)
		}
		args = append(args, fmt.Sprintf("--join=%s", strings.Join(addresses, ",")))
	}
	if startOpts.Target == StartTenantSQL {
		args = append(args, fmt.Sprintf("--kv-addrs=%s", startOpts.KVAddrs))
		args = append(args, fmt.Sprintf("--tenant-id=%d", startOpts.TenantID))
	}

	if startOpts.Target == StartDefault {
		args = append(args, c.generateStartFlagsKV(node, startOpts)...)
	}

	if startOpts.Target == StartDefault || startOpts.Target == StartTenantSQL {
		args = append(args, c.generateStartFlagsSQL(node, startOpts)...)
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
// and storage layers.
func (c *SyncedCluster) generateStartFlagsSQL(node Node, startOpts StartOpts) []string {
	var args []string
	args = append(args, fmt.Sprintf("--max-sql-memory=%d%%", c.maybeScaleMem(25)))
	if startOpts.Target == StartTenantSQL {
		args = append(args, "--store", c.TenantStoreDir(node, startOpts.TenantName, startOpts.TenantInstance))
	}
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

func (c *SyncedCluster) initializeCluster(
	ctx context.Context, l *logger.Logger, node Node,
) (*RunResultDetails, error) {
	l.Printf("%s: initializing cluster\n", c.Name)
	cmd, err := c.generateInitCmd(ctx, node)
	if err != nil {
		return nil, err
	}

	res, err := c.runCmdOnSingleNode(ctx, l, node, cmd, defaultCmdOpts("init-cluster"))
	if res != nil {
		out := strings.TrimSpace(res.CombinedOut)
		if out != "" {
			l.Printf(out)
		}
	}
	return res, err
}

func (c *SyncedCluster) setClusterSettings(
	ctx context.Context, l *logger.Logger, node Node,
) (*RunResultDetails, error) {
	l.Printf("%s: setting cluster settings", c.Name)
	cmd, err := c.generateClusterSettingCmd(ctx, l, node)
	if err != nil {
		return nil, err
	}

	res, err := c.runCmdOnSingleNode(ctx, l, node, cmd, defaultCmdOpts("set-cluster-settings"))
	if res != nil {
		out := strings.TrimSpace(res.CombinedOut)
		if out != "" {
			l.Printf(out)
		}
	}
	return res, err
}

func (c *SyncedCluster) generateClusterSettingCmd(
	ctx context.Context, l *logger.Logger, node Node,
) (string, error) {
	if config.CockroachDevLicense == "" {
		l.Printf("%s: COCKROACH_DEV_LICENSE unset: enterprise features will be unavailable\n",
			c.Name)
	}

	clusterSettings := map[string]string{
		"cluster.organization": "Cockroach Labs - Production Testing",
		"enterprise.license":   config.CockroachDevLicense,
	}
	for name, value := range c.ClusterSettings.ClusterSettings {
		clusterSettings[name] = value
	}
	var clusterSettingsString string
	for name, value := range clusterSettings {
		clusterSettingsString += fmt.Sprintf("SET CLUSTER SETTING %s = '%s';\n", name, value)
	}

	var clusterSettingsCmd string
	if c.IsLocal() {
		clusterSettingsCmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(node))
	}

	binary := cockroachNodeBinary(c, node)
	path := fmt.Sprintf("%s/%s", c.NodeDir(node, 1 /* storeIndex */), "settings-initialized")
	port, err := c.NodePort(ctx, node)
	if err != nil {
		return "", err
	}
	url := c.NodeURL("localhost", port, SystemTenantName /* tenantName */)

	clusterSettingsCmd += fmt.Sprintf(`
		if ! test -e %s ; then
			COCKROACH_CONNECT_TIMEOUT=%d %s sql --url %s -e "%s" && touch %s
		fi`, path, startSQLTimeout, binary, url, clusterSettingsString, path)
	return clusterSettingsCmd, nil
}

func (c *SyncedCluster) generateInitCmd(ctx context.Context, node Node) (string, error) {
	var initCmd string
	if c.IsLocal() {
		initCmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(node))
	}

	path := fmt.Sprintf("%s/%s", c.NodeDir(node, 1 /* storeIndex */), "cluster-bootstrapped")
	port, err := c.NodePort(ctx, node)
	if err != nil {
		return "", err
	}
	url := c.NodeURL("localhost", port, SystemTenantName /* tenantName */)
	binary := cockroachNodeBinary(c, node)
	initCmd += fmt.Sprintf(`
		if ! test -e %[1]s ; then
			COCKROACH_CONNECT_TIMEOUT=%[4]d %[2]s init --url %[3]s && touch %[1]s
		fi`, path, binary, url, startSQLTimeout)
	return initCmd, nil
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
	if !c.Secure {
		return nil
	}
	for _, node := range c.TargetNodes() {
		if node == 1 {
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
	externalStoragePath := fmt.Sprintf("gs://%s", testutils.BackupTestingBucket())
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
	port, err := c.NodePort(ctx, node)
	if err != nil {
		return err
	}
	url := c.NodeURL("localhost", port, SystemTenantName /* tenantName */)
	fullCmd := fmt.Sprintf(`COCKROACH_CONNECT_TIMEOUT=%d %s sql --url %s -e %q`,
		startSQLTimeout, binary, url, createScheduleCmd)
	// Instead of using `c.ExecSQL()`, use `c.runCmdOnSingleNode()`, which allows us to
	// 1) prefix the schedule backup cmd with COCKROACH_CONNECT_TIMEOUT.
	// 2) run the command against the first node in the cluster target.
	res, err := c.runCmdOnSingleNode(ctx, l, node, fullCmd, defaultCmdOpts("init-backup-schedule"))
	if err != nil || res.Err != nil {
		out := ""
		if res != nil {
			out = res.CombinedOut
		}
		return errors.Wrapf(err, "~ %s\n%s", fullCmd, out)
	}

	if out := strings.TrimSpace(res.CombinedOut); out != "" {
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
