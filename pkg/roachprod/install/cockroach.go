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
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"text/template"

	"github.com/alessio/shellescape"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

//go:embed scripts/start.sh
var startScript string

// sharedProcessVirtualClusterNode is a constant node that is used
// whenever we register a service descriptor for a shared-process
// virtual cluster. Since these virtual clusters use the system
// interface process, the service descriptor just represents the
// existence of the virtual cluster; at service discovery time, ports
// are resolved based on existing services for the system interface.
var sharedProcessVirtualClusterNode = Node(1)

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
	Target StartTarget
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

	// -- Options that apply only to the StartServiceForVirtualCluster target --
	VirtualClusterName string
	VirtualClusterID   int
	SQLInstance        int
	KVAddrs            string
	KVCluster          *SyncedCluster
}

func (s *StartOpts) IsVirtualCluster() bool {
	return s.Target == StartSharedProcessForVirtualCluster || s.Target == StartServiceForVirtualCluster
}

// StartTarget identifies what flavor of cockroach we are starting.
type StartTarget int

const (
	// StartDefault starts a "full" (KV+SQL) node (the default).
	StartDefault StartTarget = iota
	// StartSharedProcessForVirtualCluster starts an in-memory tenant
	// (shared process) for a virtual cluster.
	StartSharedProcessForVirtualCluster
	// StartServiceForVirtualCluster starts a SQL/HTTP-only server
	// process for a virtual cluster.
	StartServiceForVirtualCluster
	// StartRoutingProxy starts the SQL proxy process to route
	// connections to multiple virtual clusters.
	StartRoutingProxy

	// startSQLTimeout identifies the COCKROACH_CONNECT_TIMEOUT to use (in seconds)
	// for sql cmds within syncedCluster.Start().
	startSQLTimeout = 1200
	// NoSQLTimeout indicates that a `cockroach sql` call is expected to
	// succeed immediately (i.e., the server is known to be accepting
	// requests at the time the call is made).
	NoSQLTimeout = 0

	defaultInitTarget = Node(1)
)

func (st StartTarget) String() string {
	return [...]string{
		StartDefault:                  "default",
		StartServiceForVirtualCluster: "SQL/HTTP instance for virtual cluster",
		StartRoutingProxy:             "SQL proxy for multiple tenants",
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

// maybeRegisterServices registers the SQL and Admin UI DNS services
// for the cluster if no previous services for the virtual or storage
// cluster are found. Any ports specified in the startOpts are used
// for the services. If no ports are specified, a search for open
// ports will be performed and selected for use.
func (c *SyncedCluster) maybeRegisterServices(
	ctx context.Context, l *logger.Logger, startOpts StartOpts,
) error {
	serviceMap, err := c.MapServices(ctx, startOpts.VirtualClusterName, startOpts.SQLInstance)
	if err != nil {
		return err
	}

	var servicesToRegister ServiceDescriptors
	switch startOpts.Target {
	case StartDefault:
		startOpts.VirtualClusterName = SystemInterfaceName
		servicesToRegister, err = c.servicesWithOpenPortSelection(
			ctx, l, startOpts, ServiceModeShared, serviceMap,
		)
	case StartSharedProcessForVirtualCluster:
		// Specifying a sql instance for shared process virtual clusters
		// doesn't make sense, so return an error in that case to make it
		// explicit.
		if startOpts.SQLInstance != 0 {
			err = fmt.Errorf("sql instance must be unset for shared process deployments")
			break
		}

		for _, serviceType := range []ServiceType{ServiceTypeSQL, ServiceTypeUI} {
			if _, ok := serviceMap[sharedProcessVirtualClusterNode][serviceType]; !ok {
				servicesToRegister = append(servicesToRegister, ServiceDesc{
					VirtualClusterName: startOpts.VirtualClusterName,
					ServiceType:        serviceType,
					ServiceMode:        ServiceModeShared,
					Node:               sharedProcessVirtualClusterNode,
				})
			}
		}
	case StartServiceForVirtualCluster:
		servicesToRegister, err = c.servicesWithOpenPortSelection(
			ctx, l, startOpts, ServiceModeExternal, serviceMap,
		)
	}

	if err != nil {
		return err
	}
	return c.RegisterServices(ctx, servicesToRegister)
}

// servicesWithOpenPortSelection returns services to be registered for
// cases where a new cockroach process is being instantiated and needs
// to being to available ports. This happens when we start the system
// interface process, or when we start SQL servers for separate
// process virtual clusters.
func (c *SyncedCluster) servicesWithOpenPortSelection(
	ctx context.Context,
	l *logger.Logger,
	startOpts StartOpts,
	serviceMode ServiceMode,
	serviceMap NodeServiceMap,
) (ServiceDescriptors, error) {
	var mu syncutil.Mutex
	var servicesToRegister ServiceDescriptors
	err := c.Parallel(ctx, l, c.Nodes, func(ctx context.Context, node Node) (*RunResultDetails, error) {
		services := make(ServiceDescriptors, 0)
		res := &RunResultDetails{Node: node}
		if _, ok := serviceMap[node][ServiceTypeSQL]; !ok {
			services = append(services, ServiceDesc{
				VirtualClusterName: startOpts.VirtualClusterName,
				ServiceType:        ServiceTypeSQL,
				ServiceMode:        ServiceModeExternal,
				Node:               node,
				Port:               startOpts.SQLPort,
				Instance:           startOpts.SQLInstance,
			})
		}
		if _, ok := serviceMap[node][ServiceTypeUI]; !ok {
			services = append(services, ServiceDesc{
				VirtualClusterName: startOpts.VirtualClusterName,
				ServiceType:        ServiceTypeUI,
				ServiceMode:        ServiceModeExternal,
				Node:               node,
				Port:               startOpts.AdminUIPort,
				Instance:           startOpts.SQLInstance,
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

		mu.Lock()
		defer mu.Unlock()
		servicesToRegister = append(servicesToRegister, services...)
		return res, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find services to register: %w", err)
	}

	return servicesToRegister, nil
}

// Start cockroach on the cluster. For non-multitenant deployments or
// SQL instances that are deployed as external services, this will
// start a cockroach process on the nodes. For shared-process
// virtualization, this creates the virtual cluster metadata (if
// necessary) but does not create any new processes.
//
// Starting the first node is special-cased quite a bit, it's used to distribute
// certs, set cluster settings, and initialize the cluster. Also, if we're only
// starting a single node in the cluster and it happens to be the "first" node
// (node 1, as understood by SyncedCluster.TargetNodes), we use
// `start-single-node` (this was written to provide a short hand to start a
// single node cluster with a replication factor of one).
func (c *SyncedCluster) Start(ctx context.Context, l *logger.Logger, startOpts StartOpts) error {
	if startOpts.Target == StartRoutingProxy {
		return fmt.Errorf("start SQL proxy not implemented")
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

	if startOpts.IsVirtualCluster() {
		startOpts.VirtualClusterID, err = c.upsertVirtualClusterMetadata(ctx, l, startOpts)
		if err != nil {
			return err
		}

		l.Printf("virtual cluster ID: %d", startOpts.VirtualClusterID)

		if err := c.distributeTenantCerts(ctx, l, startOpts.KVCluster, startOpts.VirtualClusterID); err != nil {
			return err
		}
	} else {
		if err := c.distributeCerts(ctx, l); err != nil {
			return err
		}
	}

	l.Printf("%s: starting nodes", c.Name)
	// For single node non-virtual clusters, `init` can be skipped
	// because during the c.StartNode call above, the
	// `--start-single-node` flag will handle all of this for us.
	shouldInit := startOpts.Target == StartDefault && !c.useStartSingleNode() && !startOpts.SkipInit
	for _, node := range c.Nodes {
		// NB: if cockroach started successfully, we ignore the output as it is
		// some harmless start messaging.
		res, err := c.startNode(ctx, l, node, startOpts)
		if err != nil || res.Err != nil {
			// If err is non-nil, then this will not be retried, but if res.Err is non-nil, it will be.
			return errors.CombineErrors(err, res.Err)
		}
		// We reserve a few special operations (bootstrapping, and setting
		// cluster settings) to the InitTarget.
		if startOpts.Target == StartDefault {
			if startOpts.GetInitTarget() != node || startOpts.SkipInit {
				continue
			}
		}
		if shouldInit {
			if res, err = c.initializeCluster(ctx, l, node); err != nil || res.Err != nil {
				// If err is non-nil, then this will not be retried, but if res.Err is non-nil, it will be.
				return errors.CombineErrors(err, res.Err)
			}
		}
	}

	if !startOpts.SkipInit {
		if err := c.waitForDefaultTargetCluster(ctx, l, startOpts); err != nil {
			return errors.Wrap(err, "failed to wait for default target cluster")
		}
		c.createAdminUserForSecureCluster(ctx, l, startOpts)
		if err = c.setClusterSettings(ctx, l, startOpts.GetInitTarget(), startOpts.VirtualClusterName); err != nil {
			return err
		}

		// Only after a successful cluster initialization should we attempt to schedule backups.
		if startOpts.ScheduleBackups && shouldInit && config.CockroachDevLicense != "" {
			return c.createFixedBackupSchedule(ctx, l, startOpts.ScheduleBackupArgs)
		}
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
func (c *SyncedCluster) LogDir(node Node, virtualClusterName string, instance int) string {
	dirName := "logs" + virtualClusterDirSuffix(virtualClusterName, instance)
	if c.IsLocal() {
		return filepath.Join(c.localVMDir(node), dirName)
	}
	return dirName
}

// InstanceStoreDir returns the data directory for the given instance of
// the given virtual cluster.
func (c *SyncedCluster) InstanceStoreDir(
	node Node, virtualClusterName string, instance int,
) string {
	dataDir := fmt.Sprintf("data%s", virtualClusterDirSuffix(virtualClusterName, instance))
	if c.IsLocal() {
		return filepath.Join(c.localVMDir(node), dataDir)
	}

	return dataDir
}

// virtualClusterDirSuffix returns the suffix to use for directories specific
// to one virtual cluster.
// This suffix consists of the tenant name and instance number (if applicable).
func virtualClusterDirSuffix(virtualClusterName string, instance int) string {
	if virtualClusterName != "" && virtualClusterName != SystemInterfaceName {
		return fmt.Sprintf("-%s-%d", virtualClusterName, instance)
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
// shared process running services for multiple virtual clusters.
func (c *SyncedCluster) NodeURL(
	host string, port int, virtualClusterName string, serviceMode ServiceMode,
) string {
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

	// Add the virtual cluster name option explicitly for shared-process
	// tenants or for the system tenant. This is to make sure we connect
	// to the system tenant in case we have previously changed the
	// default virtual cluster.
	if (serviceMode == ServiceModeShared && virtualClusterName != "") ||
		virtualClusterName == SystemInterfaceName {
		v.Add("options", fmt.Sprintf("-ccluster=%s", virtualClusterName))
	}
	u.RawQuery = v.Encode()
	return "'" + u.String() + "'"
}

// NodePort returns the system tenant's SQL port for the given node.
func (c *SyncedCluster) NodePort(ctx context.Context, node Node) (int, error) {
	desc, err := c.DiscoverService(ctx, node, SystemInterfaceName, ServiceTypeSQL, 0)
	if err != nil {
		return 0, err
	}
	return desc.Port, nil
}

// NodeUIPort returns the system tenant's AdminUI port for the given node.
func (c *SyncedCluster) NodeUIPort(ctx context.Context, node Node) (int, error) {
	desc, err := c.DiscoverService(ctx, node, SystemInterfaceName, ServiceTypeUI, 0)
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
	ctx context.Context, l *logger.Logger, virtualClusterName string, sqlInstance int, args []string,
) error {
	if len(c.Nodes) != 1 {
		return fmt.Errorf("invalid number of nodes for interactive sql: %d", len(c.Nodes))
	}
	desc, err := c.DiscoverService(ctx, c.Nodes[0], virtualClusterName, ServiceTypeSQL, sqlInstance)
	if err != nil {
		return err
	}
	url := c.NodeURL("localhost", desc.Port, virtualClusterName, desc.ServiceMode)
	binary := cockroachNodeBinary(c, c.Nodes[0])
	allArgs := []string{binary, "sql", "--url", url}
	allArgs = append(allArgs, ssh.Escape(args))
	return c.SSH(ctx, l, []string{"-t"}, allArgs)
}

// ExecSQL runs a `cockroach sql` and returns the output.  If the call
// is intended to run a SQL statement, the caller must pass the "-e"
// (or "--execute") flag explicitly.
func (c *SyncedCluster) ExecSQL(
	ctx context.Context,
	l *logger.Logger,
	nodes Nodes,
	virtualClusterName string,
	sqlInstance int,
	args []string,
) ([]*RunResultDetails, error) {
	display := fmt.Sprintf("%s: executing sql", c.Name)
	results, _, err := c.ParallelE(ctx, l, nodes, func(ctx context.Context, node Node) (*RunResultDetails, error) {
		desc, err := c.DiscoverService(ctx, node, virtualClusterName, ServiceTypeSQL, sqlInstance)
		if err != nil {
			return nil, err
		}
		var cmd string
		if c.IsLocal() {
			cmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(node))
		}
		cmd += cockroachNodeBinary(c, node) + " sql --url " +
			c.NodeURL("localhost", desc.Port, virtualClusterName, desc.ServiceMode) + " " +
			ssh.Escape(args)

		return c.runCmdOnSingleNode(ctx, l, node, cmd, defaultCmdOpts("run-sql"))
	}, WithDisplay(display), WithWaitOnFail())

	return results, err
}

func (c *SyncedCluster) startNode(
	ctx context.Context, l *logger.Logger, node Node, startOpts StartOpts,
) (*RunResultDetails, error) {
	if startOpts.Target == StartSharedProcessForVirtualCluster {
		return &RunResultDetails{}, nil
	}

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
		LogDir: c.LogDir(node, startOpts.VirtualClusterName, startOpts.SQLInstance),
		KeyCmd: keyCmd,
		EnvVars: append(append([]string{
			fmt.Sprintf("ROACHPROD=%s", c.roachprodEnvValue(node)),
			"GOTRACEBACK=crash",
			"COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING=1",
		}, c.Env...), getEnvVars()...),
		Binary:              cockroachNodeBinary(c, node),
		Args:                args,
		MemoryMax:           config.MemoryMax,
		NumFilesLimit:       startOpts.NumFilesLimit,
		VirtualClusterLabel: VirtualClusterLabel(startOpts.VirtualClusterName, startOpts.SQLInstance),
		Local:               c.IsLocal(),
	})
}

type startTemplateData struct {
	Local               bool
	LogDir              string
	Binary              string
	KeyCmd              string
	MemoryMax           string
	NumFilesLimit       int64
	VirtualClusterLabel string
	Args                []string
	EnvVars             []string
}

// VirtualClusterLabel is the value used to "label" virtual cluster
// (cockroach) processes running locally or in a VM. This is used by
// roachprod to monitor identify such processes and monitor them.
func VirtualClusterLabel(virtualClusterName string, sqlInstance int) string {
	if virtualClusterName == "" || virtualClusterName == SystemInterfaceName {
		return "cockroach-system"
	}

	return fmt.Sprintf("cockroach-%s_%d", virtualClusterName, sqlInstance)
}

// VirtualClusterInfoFromLabel takes as parameter a tenant label
// produced with `VirtuaLClusterLabel()` and returns the corresponding
// tenant name and instance.
func VirtualClusterInfoFromLabel(virtualClusterLabel string) (string, int, error) {
	var (
		sqlInstance          int
		sqlInstanceStr       string
		labelWithoutInstance string
		err                  error
	)

	sep := "_"
	parts := strings.Split(virtualClusterLabel, sep)

	// Note that this logic assumes that virtual cluster names cannot
	// have a '_' character, which is currently (Sep 2023) the case.
	switch len(parts) {
	case 1:
		// This should be a system tenant (no instance identifier)
		labelWithoutInstance = parts[0]

	case 2:
		// SQL instance process: instance number is after the '_' character.
		labelWithoutInstance, sqlInstanceStr = parts[0], parts[1]
		sqlInstance, err = strconv.Atoi(sqlInstanceStr)
		if err != nil {
			return "", 0, fmt.Errorf("invalid virtual cluster label: %s", virtualClusterLabel)
		}

	default:
		return "", 0, fmt.Errorf("invalid virtual cluster label: %s", virtualClusterLabel)
	}

	// Remove the "cockroach-" prefix added by VirtualClusterLabel.
	virtualClusterName := strings.TrimPrefix(labelWithoutInstance, "cockroach-")
	return virtualClusterName, sqlInstance, nil
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

	case StartServiceForVirtualCluster:
		args = []string{"mt", "start-sql"}

	case StartRoutingProxy:
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

	logDir := c.LogDir(node, startOpts.VirtualClusterName, startOpts.SQLInstance)
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

	virtualClusterName := startOpts.VirtualClusterName
	instance := startOpts.SQLInstance
	var sqlPort int
	if startOpts.Target == StartServiceForVirtualCluster {
		desc, err := c.DiscoverService(ctx, node, virtualClusterName, ServiceTypeSQL, instance)
		if err != nil {
			return nil, err
		}
		sqlPort = desc.Port
		args = append(args, fmt.Sprintf("--sql-addr=%s:%d", listenHost, sqlPort))
	} else {
		virtualClusterName = SystemInterfaceName
		// System interface instance is always 0.
		instance = 0
		desc, err := c.DiscoverService(ctx, node, virtualClusterName, ServiceTypeSQL, instance)
		if err != nil {
			return nil, err
		}
		sqlPort = desc.Port
		args = append(args, fmt.Sprintf("--listen-addr=%s:%d", listenHost, sqlPort))
	}
	desc, err := c.DiscoverService(ctx, node, virtualClusterName, ServiceTypeUI, instance)
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
			desc, err := c.DiscoverService(ctx, joinNode, SystemInterfaceName, ServiceTypeSQL, 0)
			if err != nil {
				return nil, err
			}
			addresses[i] = fmt.Sprintf("%s:%d", c.Host(joinNode), desc.Port)
		}
		args = append(args, fmt.Sprintf("--join=%s", strings.Join(addresses, ",")))
	}
	if startOpts.Target == StartServiceForVirtualCluster {
		args = append(args, fmt.Sprintf("--kv-addrs=%s", startOpts.KVAddrs))
		args = append(args, fmt.Sprintf("--tenant-id=%d", startOpts.VirtualClusterID))
	}

	if startOpts.Target == StartDefault {
		args = append(args, c.generateStartFlagsKV(node, startOpts)...)
	}

	if startOpts.Target == StartDefault || startOpts.Target == StartServiceForVirtualCluster {
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
	} else if strings.HasPrefix(startOpts.ExtraArgs[idx], "--store=") {
		// The flag and path were provided together. Strip the flag prefix.
		storeDir := strings.TrimPrefix(startOpts.ExtraArgs[idx], "--store=")
		if !strings.Contains(storeDir, "type=mem") {
			storeDirs = append(storeDirs, storeDir)
		}
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

var maxSQLMemoryRE = regexp.MustCompile(`^--max-sql-memory=(\d+)%$`)

// generateStartFlagsSQL generates `cockroach start` and `cockroach mt
// start-sql` arguments that are relevant for the SQL layers, used by both KV
// and storage layers.
func (c *SyncedCluster) generateStartFlagsSQL(node Node, startOpts StartOpts) []string {
	var args []string
	formatArg := func(m int) string {
		return fmt.Sprintf("--max-sql-memory=%d%%", c.maybeScaleMem(m))
	}

	if idx := argExists(startOpts.ExtraArgs, "--max-sql-memory"); idx == -1 {
		args = append(args, formatArg(25))
	} else {
		arg := startOpts.ExtraArgs[idx]
		matches := maxSQLMemoryRE.FindStringSubmatch(arg)
		mem, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("invalid --max-sql-memory parameter: %s", arg))
		}

		startOpts.ExtraArgs[idx] = formatArg(int(mem))
	}

	if startOpts.Target == StartServiceForVirtualCluster {
		args = append(args, "--store", c.InstanceStoreDir(node, startOpts.VirtualClusterName, startOpts.SQLInstance))
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

// waitForDefaultTargetCluster checks for the existence of a
// config-profile flag that leads to the use of an application tenant
// as 'default target cluster'; if that is the case, we wait for all
// nodes to be aware of the cluster setting before proceding. Without
// this logic, follow-up tasks in the process of creating the cluster
// could run before the cluster setting is propagated, and they would
// apply to the system tenant instead.
func (c *SyncedCluster) waitForDefaultTargetCluster(
	ctx context.Context, l *logger.Logger, startOpts StartOpts,
) error {
	var hasCustomTargetCluster bool
	for _, arg := range startOpts.ExtraArgs {
		// If there is a config profile and that is set to either a '+app'
		// profile or 'replication-source', we know that the default
		// target cluster setting will be set to the application tenant.
		if strings.Contains(arg, "config-profile") &&
			(strings.Contains(arg, "+app") || strings.Contains(arg, "replication-source")) {
			hasCustomTargetCluster = true
			break
		}
	}

	if !hasCustomTargetCluster {
		return nil
	}

	l.Printf("waiting for default target cluster")
	retryOpts := retry.Options{MaxRetries: 20}
	return retryOpts.Do(ctx, func(ctx context.Context) error {
		// TODO(renato): use server.controller.default_target_cluster once
		// 23.1 is no longer supported.
		const stmt = "SHOW CLUSTER SETTING server.controller.default_tenant"
		res, err := c.ExecSQL(ctx, l, Nodes{startOpts.GetInitTarget()}, SystemInterfaceName, 0, []string{"-e", stmt})
		if err != nil {
			return errors.Wrap(err, "error reading cluster setting")
		}

		if len(res) > 0 {
			if res[0].Err != nil {
				return errors.Wrapf(res[0].Err, "node %d", res[0].Node)
			}

			if strings.Contains(res[0].CombinedOut, "system") {
				return errors.Newf("target cluster on n%d is still system", res[0].Node)
			}
		}

		// Once we know the cluster setting points to the default target
		// cluster, we attempt to run a dummy SQL statement until that
		// succeeds (i.e., until the target cluster is able to handle
		// requests.)
		const pingStmt = "SELECT 1;"
		res, err = c.ExecSQL(ctx, l, Nodes{startOpts.GetInitTarget()}, "", 0, []string{"-e", pingStmt})
		if err != nil {
			return errors.Wrap(err, "error connecting to default target cluster")
		}

		if res[0] != nil && res[0].Err != nil {
			err = errors.CombineErrors(err, res[0].Err)
		}

		return err
	})
}

// createAdminUserForSecureCluster creates a `roach` user with admin
// privileges. The password used matches the virtual cluster name
// ('system' for the storage cluster). If it cannot be created, this
// function will log an error and continue. Roachprod is used in a
// variety of contexts within roachtests, and a failure to create a
// user might be "expected" depending on what the test is doing.
func (c *SyncedCluster) createAdminUserForSecureCluster(
	ctx context.Context, l *logger.Logger, startOpts StartOpts,
) {
	if !c.Secure {
		return
	}

	const username = "roach"
	// N.B.: although using the same username/password combination would
	// be easier to remember, if we do it for the system interface and
	// virtual clusters we would be unable to log-in to the virtual
	// cluster console due to #109691.
	//
	// TODO(renato): use the same combination once we're able to select
	// the virtual cluster we are connecting to in the console.
	var password = startOpts.VirtualClusterName
	if startOpts.VirtualClusterName == "" {
		password = SystemInterfaceName
	}

	stmts := strings.Join([]string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS %s WITH LOGIN PASSWORD '%s'", username, password),
		fmt.Sprintf("GRANT ADMIN TO %s", username),
	}, "; ")

	// We retry a few times here because cockroach process might not be
	// ready to serve connections at this point. We also can't use
	// `COCKROACH_CONNECT_TIMEOUT` in this case because that would not
	// work for shared-process virtual clusters.
	retryOpts := retry.Options{MaxRetries: 20}
	if err := retryOpts.Do(ctx, func(ctx context.Context) error {
		results, err := c.ExecSQL(
			ctx, l, Nodes{startOpts.GetInitTarget()}, startOpts.VirtualClusterName, startOpts.SQLInstance, []string{
				"-e", stmts,
			})

		if err != nil || results[0].Err != nil {
			err := errors.CombineErrors(err, results[0].Err)
			return err
		}

		return nil
	}); err != nil {
		l.Printf("not creating default admin user due to error: %v", err)
		return
	}

	var virtualClusterInfo string
	if startOpts.VirtualClusterName != "" && startOpts.VirtualClusterName != SystemInterfaceName {
		virtualClusterInfo = fmt.Sprintf(" for virtual cluster %s", startOpts.VirtualClusterName)
	}

	l.Printf("log into DB console%s with user=%s password=%s", virtualClusterInfo, username, password)
}

func (c *SyncedCluster) setClusterSettings(
	ctx context.Context, l *logger.Logger, node Node, virtualCluster string,
) error {
	l.Printf("%s: setting cluster settings", c.Name)
	cmd, err := c.generateClusterSettingCmd(ctx, l, node, virtualCluster)
	if err != nil {
		return err
	}

	res, err := c.runCmdOnSingleNode(ctx, l, node, cmd, defaultCmdOpts("set-cluster-settings"))
	if res != nil {
		out := strings.TrimSpace(res.CombinedOut)
		if out != "" {
			l.Printf(out)
		}
	}

	if res != nil && res.Err != nil {
		err = errors.CombineErrors(err, res.Err)
	}

	return err
}

func (c *SyncedCluster) generateClusterSettingCmd(
	ctx context.Context, l *logger.Logger, node Node, virtualCluster string,
) (string, error) {
	if config.CockroachDevLicense == "" {
		l.Printf("%s: COCKROACH_DEV_LICENSE unset: enterprise features will be unavailable\n",
			c.Name)
	}

	var tenantPrefix string
	if virtualCluster != "" && virtualCluster != SystemInterfaceName {
		tenantPrefix = fmt.Sprintf("ALTER TENANT '%s' ", virtualCluster)
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
		clusterSettingsString += fmt.Sprintf("%sSET CLUSTER SETTING %s = '%s';\n", tenantPrefix, name, value)
	}

	var clusterSettingsCmd string
	if c.IsLocal() {
		clusterSettingsCmd = fmt.Sprintf(`cd %s ; `, c.localVMDir(node))
	}

	binary := cockroachNodeBinary(c, node)
	var pathPrefix string
	if virtualCluster != "" {
		pathPrefix = fmt.Sprintf("%s_", virtualCluster)
	}
	path := fmt.Sprintf("%s/%ssettings-initialized", c.NodeDir(node, 1 /* storeIndex */), pathPrefix)
	port, err := c.NodePort(ctx, node)
	if err != nil {
		return "", err
	}
	url := c.NodeURL("localhost", port, SystemInterfaceName /* virtualClusterName */, ServiceModeShared)

	// We use `mkdir -p` here since the directory may not exist if an in-memory
	// store is used.
	clusterSettingsCmd += fmt.Sprintf(`
		if ! test -e %s ; then
			COCKROACH_CONNECT_TIMEOUT=%d %s sql --url %s -e "%s" && mkdir -p %s && touch %s
		fi`, path, startSQLTimeout, binary, url, clusterSettingsString, c.NodeDir(node, 1 /* storeIndex */), path)
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
	url := c.NodeURL("localhost", port, SystemInterfaceName /* virtualClusterName */, ServiceModeShared)
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

// upsertVirtualClusterMetadata creates the virtual cluster metadata,
// if necessary, and marks the service as started internally. We only
// need to run the statements in this function against a single
// connection to the storage cluster.
func (c *SyncedCluster) upsertVirtualClusterMetadata(
	ctx context.Context, l *logger.Logger, startOpts StartOpts,
) (int, error) {
	runSQL := func(stmt string) (string, error) {
		results, err := startOpts.KVCluster.ExecSQL(ctx, l, startOpts.KVCluster.Nodes[:1], "", 0, []string{
			"--format", "json", "-e", stmt,
		})
		if err != nil {
			return "", err
		}
		if results[0].Err != nil {
			return "", results[0].Err
		}

		return results[0].CombinedOut, nil
	}

	virtualClusterIDByName := func(name string) (int, error) {
		type tenantRow struct {
			ID string `json:"id"`
		}

		query := fmt.Sprintf(
			"SELECT id FROM system.tenants WHERE name = '%s'", startOpts.VirtualClusterName,
		)

		existsOut, err := runSQL(query)
		if err != nil {
			return -1, err
		}

		var tenants []tenantRow
		if err := json.Unmarshal([]byte(existsOut), &tenants); err != nil {
			return -1, fmt.Errorf("failed to unmarshal system.tenants output: %w", err)
		}

		if len(tenants) == 0 {
			return -1, nil
		}

		n, err := strconv.Atoi(tenants[0].ID)
		if err != nil {
			return -1, fmt.Errorf("failed to parse virtual cluster ID: %w", err)
		}

		return n, nil
	}

	virtualClusterID, err := virtualClusterIDByName(startOpts.VirtualClusterName)
	if err != nil {
		return -1, err
	}

	l.Printf("Starting virtual cluster")
	serviceMode := "SHARED"
	if startOpts.Target == StartServiceForVirtualCluster {
		serviceMode = "EXTERNAL"
	}

	var virtualClusterStmts []string
	if virtualClusterID <= 0 {
		// If the virtual cluster metadata does not exist yet, create it.
		virtualClusterStmts = append(virtualClusterStmts,
			fmt.Sprintf("CREATE TENANT '%s'", startOpts.VirtualClusterName),
		)
	}

	virtualClusterStmts = append(virtualClusterStmts, fmt.Sprintf(
		"ALTER TENANT '%s' START SERVICE %s",
		startOpts.VirtualClusterName, serviceMode),
	)

	_, err = runSQL(strings.Join(virtualClusterStmts, "; "))
	if err != nil {
		return -1, err
	}

	return virtualClusterIDByName(startOpts.VirtualClusterName)
}

// distributeCerts distributes certs if it's a secure cluster.
func (c *SyncedCluster) distributeTenantCerts(
	ctx context.Context, l *logger.Logger, storageCluster *SyncedCluster, virtualClusterID int,
) error {
	if c.Secure {
		return c.DistributeTenantCerts(ctx, l, storageCluster, virtualClusterID)
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
	url := c.NodeURL("localhost", port, SystemInterfaceName /* virtualClusterName */, ServiceModeShared)
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
