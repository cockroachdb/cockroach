// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil"
	"github.com/cockroachdb/errors"
)

// FailureArgs describes the args passed to a failure mode.
//
// For now, this interface is not necessarily needed. However, it sets up for
// future failure injection work when we want a failure controller to be able
// to parse args from a YAML file and pass them to a failure controller.
type FailureArgs interface {
}

// FailureMode describes a failure that can be injected into a system.
type FailureMode interface {
	// SupportedDeploymentMode returns true if the FailureMode supports running
	// on the given deployment mode.
	SupportedDeploymentMode(mode roachprodutil.DeploymentMode) bool

	// Setup any dependencies required for the failure to be injected. The
	// same args passed to Setup, must be passed to Cleanup. Setup is a
	// pre-requisite to calling all other methods.
	Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// Inject a failure into the system. The same args passed to Inject
	// must be passed to Recover, WaitForFailureToPropagate, and WaitForFailureToRecover.
	Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// Recover reverses the effects of Inject. Must be called after a failure
	// mode has been injected.
	Recover(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// Cleanup uninstalls any dependencies that were installed by Setup.
	Cleanup(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// WaitForFailureToPropagate waits until the failure is at full effect. Must
	// be called after a failure mode has been injected. Should only monitor, not
	// modify the cluster state, i.e. is idempotent and can be called multiple
	// times or not at all with no visible side effects.
	WaitForFailureToPropagate(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// WaitForFailureToRecover waits until the failure was recovered completely along with any
	// side effects. Must be called with no active failure mode, i.e. after Recover has been
	// called. Should only monitor, not modify the state of the cluster.
	WaitForFailureToRecover(ctx context.Context, l *logger.Logger, args FailureArgs) error
}

type diskDevice struct {
	name  string
	major int
	minor int
}

// GenericFailure is a generic helper struct that FailureModes can embed to
// provide commonly used functionality that doesn't differ between failure modes,
// e.g. running remote commands on the cluster.
type GenericFailure struct {
	c                     *install.SyncedCluster
	defaultVirtualCluster virtualClusterOpt
	// runTitle is the title to prefix command output with.
	runTitle          string
	networkInterfaces []string
	diskDevice        diskDevice
	// map of node and virtual cluster selector to connection pools.
	connCache         map[string]*gosql.DB
	localCertsPath    string
	replicationFactor int
	processes         processMap
}

func makeGenericFailure(
	clusterName string, l *logger.Logger, clusterOpts ClusterOptions, failureModeName string,
) (*GenericFailure, error) {
	c, err := roachprod.GetClusterFromCache(l, clusterName, install.SecureOption(clusterOpts.secure))
	if err != nil {
		return nil, err
	}

	genericFailure := GenericFailure{
		c:                     c,
		defaultVirtualCluster: clusterOpts.defaultVirtualCluster,
		runTitle:              failureModeName,
		connCache:             make(map[string]*gosql.DB, len(c.Nodes)),
		localCertsPath:        clusterOpts.localCertsPath,
		replicationFactor:     clusterOpts.replicationFactor,
	}
	if genericFailure.defaultVirtualCluster.name == "" {
		genericFailure.defaultVirtualCluster.name = install.SystemInterfaceName
	}
	return &genericFailure, nil
}

func (f *GenericFailure) Run(
	ctx context.Context, l *logger.Logger, node install.Nodes, args ...string,
) error {
	cmd := strings.Join(args, " ")
	l.Printf("running cmd: %s", cmd)
	// In general, most failures shouldn't be run locally out of caution.
	if f.c.IsLocal() {
		l.Printf("Local cluster detected, skipping command execution")
		return nil
	}
	return f.c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(node), fmt.Sprintf("%s-%d", f.runTitle, node), cmd)
}

func (f *GenericFailure) RunWithDetails(
	ctx context.Context, l *logger.Logger, node install.Nodes, args ...string,
) (install.RunResultDetails, error) {
	cmd := strings.Join(args, " ")
	// In general, most failures shouldn't be run locally out of caution.
	if f.c.IsLocal() {
		l.Printf("Local cluster detected, logging command instead of running:\n%s", cmd)
		return install.RunResultDetails{}, nil
	}
	res, err := f.c.RunWithDetails(ctx, l, install.WithNodes(node), fmt.Sprintf("%s-%d", f.runTitle, node), cmd)
	if err != nil {
		return install.RunResultDetails{}, err
	}
	return res[0], nil
}

// Conn returns a connection to the given node. The connection is cached.
func (f *GenericFailure) Conn(
	ctx context.Context,
	l *logger.Logger,
	node install.Nodes,
	virtualClusterName string,
	sqlInstance int,
) (*gosql.DB, error) {
	nodeSelector := fmt.Sprintf("%d:%s_%d", node[0]-1, virtualClusterName, sqlInstance)
	if f.connCache[nodeSelector] == nil {
		// We are connecting to the cluster from a local machine, e.g. from the test runner or
		// roachprod CLI, so we need to use the certs found locally.
		c := f.c.WithCerts(f.localCertsPath)
		desc, err := c.ServiceDescriptor(ctx, node[0], virtualClusterName, install.ServiceTypeSQL, sqlInstance)
		if err != nil {
			return nil, err
		}
		ip := c.Host(node[0])
		if ip == "" {
			return nil, errors.Errorf("empty ip for node %d", node)
		}
		authMode := install.DefaultAuthMode()
		if !c.Secure {
			authMode = install.AuthRootCert
		}
		nodeURL := c.NodeURL(ip, desc.Port, virtualClusterName, desc.ServiceMode, authMode, "" /* database */)
		nodeURL = strings.Trim(nodeURL, "'")
		pgurl, err := url.Parse(nodeURL)
		if err != nil {
			return nil, err
		}
		vals := make(url.Values)
		vals.Add("connect_timeout", "30")
		nodeURL = pgurl.String() + "&" + vals.Encode()
		l.Printf("Creating %s connection to node %d at %s", virtualClusterName, node[0], nodeURL)
		f.connCache[nodeSelector], err = gosql.Open("postgres", nodeURL)
		if err != nil {
			return nil, err
		}
	}

	return f.connCache[nodeSelector], nil
}

func (f *GenericFailure) CloseConnections() {
	for _, db := range f.connCache {
		if db != nil {
			_ = db.Close()
		}
	}
}

// NetworkInterfaces returns the network interfaces used by the VMs in the cluster.
// Assumes that all VMs are using the same machine type and will have the same
// network interfaces.
func (f *GenericFailure) NetworkInterfaces(
	ctx context.Context, l *logger.Logger,
) ([]string, error) {
	if f.networkInterfaces == nil {
		res, err := f.c.RunWithDetails(ctx, l, install.WithNodes(f.c.Nodes[:1]), "Get Network Interfaces", "ip -o link show | awk -F ': ' '{print $2}'")
		if err != nil {
			return nil, errors.Wrapf(err, "error when determining network interfaces")
		}
		interfaces := strings.Split(strings.TrimSpace(res[0].Stdout), "\n")
		for _, iface := range interfaces {
			f.networkInterfaces = append(f.networkInterfaces, strings.TrimSpace(iface))
		}
	}
	return f.networkInterfaces, nil
}

func getDiskDevice(ctx context.Context, f *GenericFailure, l *logger.Logger) error {
	if f.diskDevice.name == "" {
		res, err := f.c.RunWithDetails(ctx, l, install.WithNodes(f.c.Nodes[:1]), "Get Disk Device", "lsblk -o NAME,MAJ:MIN,MOUNTPOINTS | grep /mnt/data1 | awk '{print $1, $2}'")
		if err != nil {
			return errors.Wrapf(err, "error when determining block device")
		}
		parts := strings.Split(strings.TrimSpace(res[0].Stdout), " ")
		if len(parts) != 2 {
			return errors.Newf("unexpected output from lsblk: %s", res[0].Stdout)
		}
		f.diskDevice.name = strings.TrimSpace(parts[0])
		major, minor, found := strings.Cut(parts[1], ":")
		if !found {
			return errors.Newf("unexpected output from lsblk: %s", res[0].Stdout)
		}
		if f.diskDevice.major, err = strconv.Atoi(major); err != nil {
			return err
		}
		if f.diskDevice.minor, err = strconv.Atoi(minor); err != nil {
			return err
		}
	}
	return nil
}

func (f *GenericFailure) DiskDeviceName(ctx context.Context, l *logger.Logger) (string, error) {
	if err := getDiskDevice(ctx, f, l); err != nil {
		return "", err
	}
	return "/dev/" + f.diskDevice.name, nil
}

func (f *GenericFailure) DiskDeviceMajorMinor(
	ctx context.Context, l *logger.Logger,
) (int, int, error) {
	if err := getDiskDevice(ctx, f, l); err != nil {
		return 0, 0, err
	}
	return f.diskDevice.major, f.diskDevice.minor, nil
}

func (f *GenericFailure) PingNode(
	ctx context.Context, l *logger.Logger, node install.Nodes, options ...virtualClusterOptFunc,
) error {
	virtualClusterName, sqlInstance := f.targetVirtualCluster(options)
	db, err := f.Conn(ctx, l, node, virtualClusterName, sqlInstance)
	if err != nil {
		return err
	}
	return db.PingContext(ctx)
}

// WaitForSQLReady waits until the corresponding node's SQL subsystem is fully initialized and ready
// to serve SQL clients.
func (f *GenericFailure) WaitForSQLReady(
	ctx context.Context,
	l *logger.Logger,
	node install.Nodes,
	timeout time.Duration,
	options ...virtualClusterOptFunc,
) error {
	virtualClusterName, sqlInstance := f.targetVirtualCluster(options)
	db, err := f.Conn(ctx, l, node, virtualClusterName, sqlInstance)
	if err != nil {
		return err
	}
	err = roachprod.WaitForSQLReady(ctx, db,
		install.WithMaxRetries(0),
		install.WithMaxDuration(timeout),
	)
	return errors.Wrapf(err, "never connected to node %d", node)
}

// WaitForSQLUnavailable pings a node until the SQL connection is unavailable.
func (f *GenericFailure) WaitForSQLUnavailable(
	ctx context.Context,
	l *logger.Logger,
	node install.Nodes,
	timeout time.Duration,
	options ...virtualClusterOptFunc,
) error {
	virtualClusterName, sqlInstance := f.targetVirtualCluster(options)
	db, err := f.Conn(ctx, l, node, virtualClusterName, sqlInstance)
	if err != nil {
		return err
	}
	err = roachprod.WaitForSQLUnavailable(ctx, db, l,
		install.WithMaxRetries(0),
		install.WithMaxDuration(timeout),
	)
	return errors.Wrapf(err, "connections to node %d still available after %s", node, timeout)
}

// IsProcessRunning returns if the cockroach process on the given node is still
// alive according to systemd.
func (f *GenericFailure) IsProcessRunning(
	ctx context.Context, l *logger.Logger, node install.Nodes, options ...virtualClusterOptFunc,
) (bool, error) {
	virtualClusterName, sqlInstance := f.targetVirtualCluster(options)
	isRunning, _, err := roachprod.IsProcessRunning(ctx, f.c, l, node, roachprod.SystemdProcessName(virtualClusterName, sqlInstance))
	return isRunning, err
}

// WaitForProcessDeath checks systemd until the cockroach process is no longer running
// or the timeout is reached.
func (f *GenericFailure) WaitForProcessDeath(
	ctx context.Context,
	l *logger.Logger,
	node install.Nodes,
	timeout time.Duration,
	options ...virtualClusterOptFunc,
) error {
	virtualClusterName, sqlInstance := f.targetVirtualCluster(options)
	err := roachprod.WaitForProcessDeath(ctx, f.c, l, node, virtualClusterName, sqlInstance,
		install.WithMaxRetries(0),
		install.WithMaxDuration(timeout),
	)

	return errors.Wrapf(err, "n%d process never exited after %s", node, timeout)
}

func (f *GenericFailure) StartNodes(
	ctx context.Context, l *logger.Logger, nodes install.Nodes, options ...virtualClusterOptFunc,
) error {
	virtualClusterName, sqlInstance := f.targetVirtualCluster(options)
	target := install.StartDefault
	if !install.IsSystemInterface(virtualClusterName) {
		isExternal, err := f.c.IsExternalService(ctx, virtualClusterName)
		if err != nil {
			return err
		}
		if isExternal {
			target = install.StartServiceForVirtualCluster
		} else {
			target = install.StartSharedProcessForVirtualCluster
		}
	}

	return f.c.WithNodes(nodes).Start(ctx, l, install.StartOpts{
		Target:             target,
		VirtualClusterName: virtualClusterName,
		SQLInstance:        sqlInstance,
		IsRestart:          true,
		// For restarts, Start() will iterate through each node in the StorageCluster
		// until one succeeds, so it's fine to pass the entire cluster.
		StorageCluster: f.c,
	})
}

// forEachNode is a helper function that calls fn for each node in nodes.
func forEachNode(nodes install.Nodes, fn func(install.Nodes) error) error {
	// TODO (darryl): Consider parallelizing this, for now all usages
	// are fast enough for sequential calls.
	for _, node := range nodes {
		if err := fn(install.Nodes{node}); err != nil {
			return err
		}
	}
	return nil
}

func runAsync(
	ctx context.Context, l *logger.Logger, f func(context.Context) error,
) (<-chan error, func()) {
	errCh := make(chan error, 1)
	asyncCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		err := roachprodutil.PanicAsError(asyncCtx, l, func(context.Context) error {
			return f(asyncCtx)
		})
		errCh <- err
		close(errCh)
	}()
	return errCh, cancel
}

func (f *GenericFailure) WaitForReplication(
	ctx context.Context,
	l *logger.Logger,
	node install.Nodes,
	replicationFactor int,
	timeout time.Duration,
) error {
	// We want to wait for both the system and secondary tenant ranges to be replicated.
	db, err := f.Conn(ctx, l, node, install.SystemInterfaceName, 0)
	if err != nil {
		return err
	}
	// Default to a replication factor of 3 if not specified. We could query and
	// extract out the lowest replication factor among all zone configs, but it seems
	// easier to just let the caller specify it if they want a stronger guarantee.
	if replicationFactor == 0 {
		replicationFactor = 3
	}

	return roachprod.WaitForReplication(ctx, db, l,
		replicationFactor, roachprod.AtLeastReplicationFactor,
		install.RetryEveryDuration(time.Second),
		install.WithMaxDuration(timeout),
	)
}

// WaitForBalancedReplicas blocks until the replica count across each store is less than
// `range_rebalance_threshold` percent from the mean. Note that this doesn't wait for
// rebalancing to _fully_ finish; there can still be range events that happen after this.
// We don't know what kind of background workloads may be running concurrently and creating
// range events, so lets just get to a state "close enough", i.e. a state that the allocator
// would consider balanced.
func (f *GenericFailure) WaitForBalancedReplicas(
	ctx context.Context, l *logger.Logger, node install.Nodes, timeout time.Duration,
) error {
	// We want to wait for both the system and secondary tenant ranges to be rebalanced.
	db, err := f.Conn(ctx, l, node, install.SystemInterfaceName, 0)
	if err != nil {
		return err
	}

	return roachprod.WaitForBalancedReplicas(ctx, db, l,
		install.RetryEveryDuration(3*time.Second),
		install.WithMaxDuration(timeout),
	)
}

// WaitForRestartedNodesToStabilize is a helper that waits for nodes
// to stabilize after a restart.
func (f *GenericFailure) WaitForRestartedNodesToStabilize(
	ctx context.Context, l *logger.Logger, nodes install.Nodes, timeout time.Duration,
) error {
	// We want our timeout to apply to the entire operation, so handle the context ourselves
	// and pass 0 (infinite) timeouts to the individual steps.
	timeoutCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	// First, we block until we are able to connect to each of the nodes
	// as we will use SQL connections to check the status of the cluster.
	if err := forEachNode(nodes, func(n install.Nodes) error {
		return f.WaitForSQLReady(timeoutCtx, l, n, 0 /* timeout */)
	}); err != nil {
		return err
	}

	// Then, we wait for ranges to be fully replicated. If the restarted nodes were only
	// briefly offline, this will block until the restarted nodes catch up. If the restarted
	// nodes were down long enough for the cluster to consider them dead, the ranges will
	// have been rebalanced to other nodes and this will be a noop.
	if err := f.WaitForReplication(timeoutCtx, l, nodes, f.replicationFactor, 0 /* timeout */); err != nil {
		return err
	}

	// Finally, we also have to block until the cluster is done rebalancing replicas.
	// If replicas were not moved around during the downtime, this will likely be a noop.
	return f.WaitForBalancedReplicas(timeoutCtx, l, nodes, 0 /* timeout */)
}

// WaitForSQLLivenessTTL blocks until the default SQL liveness TTL duration has passed.
// This is useful when we are injecting failures that potentially cause SQL processes to
// be unable to heartbeat, e.g. we partition the sqlliveness leaseholder from
// the separate process servers. A common pattern may be to recover from such a
// failure then wait to see if any of the SQL processes exit.
func (f *GenericFailure) WaitForSQLLivenessTTL(ctx context.Context) error {
	// 40 seconds is the default server.sqlliveness.ttl. We could query for the actual
	// value, but it is an application setting which means we'd have to query _every_
	// tenant to find the maximum. However, we may not even be able to query the tenants
	// if they have already exited.
	//
	// Instead, lets assume that most clusters don't touch this value and best effort
	// wait for the default value.
	ttlDuration := 40 * time.Second
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(ttlDuration):
	}

	return nil
}

// CaptureProcesses captures all running processes on the specified nodes.
// This is used with StopProcesses and RestartProcesses to easily stop and
// start all processes running on a node without having to explicitly specify
// every process.
func (f *GenericFailure) CaptureProcesses(
	ctx context.Context, l *logger.Logger, nodes install.Nodes,
) {
	f.processes = make(processMap)
	// Capture the processes running on the nodes.
	monitorChan := f.c.WithNodes(nodes).Monitor(l, ctx, install.MonitorOpts{OneShot: true})
	for e := range monitorChan {
		if p, ok := e.Event.(install.MonitorProcessRunning); ok {
			f.processes.add(p.VirtualClusterName, p.SQLInstance, e.Node)
		}
	}
	l.Printf("captured processes: %+v", f.processes)
}

// StopProcesses stops all processes captured by the last CaptureProcesses call.
func (f *GenericFailure) StopProcesses(ctx context.Context, l *logger.Logger) error {
	for _, virtualClusterName := range f.processes.getStopOrder() {
		instanceMap := f.processes[virtualClusterName]
		for instance, nodeMap := range instanceMap {
			var stopNodes install.Nodes
			for node := range nodeMap {
				stopNodes = append(stopNodes, node)
			}
			l.Printf("Stopping process %s on nodes %v", virtualClusterName, stopNodes)
			label := install.VirtualClusterLabel(virtualClusterName, instance)
			stopOpts := roachprod.DefaultStopOpts()
			err := f.c.WithNodes(stopNodes).Stop(ctx, l, stopOpts.Sig, true, stopOpts.GracePeriod, label)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// RestartProcesses restarts all processes captured by the last CaptureProcesses call.
func (f *GenericFailure) RestartProcesses(ctx context.Context, l *logger.Logger) error {
	// Restart the processes.
	for _, virtualClusterName := range f.processes.getStartOrder() {
		instanceMap := f.processes[virtualClusterName]
		for instance, nodeMap := range instanceMap {
			var nodes install.Nodes
			for node := range nodeMap {
				nodes = append(nodes, node)
			}
			l.Printf("Starting process %s on nodes %v", virtualClusterName, nodes)
			err := f.c.WithNodes(nodes).Start(ctx, l, install.StartOpts{
				VirtualClusterName: virtualClusterName,
				SQLInstance:        instance,
				IsRestart:          true,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// MaybeRestartSeparateProcesses restarts any stopped separate processes captured by the
// last CaptureProcesses call.
func (f *GenericFailure) MaybeRestartSeparateProcesses(
	ctx context.Context, l *logger.Logger,
) error {
	// Restart the processes.
	for _, virtualClusterName := range f.processes.getStartOrder() {
		if install.IsSystemInterface(virtualClusterName) {
			continue
		}
		instanceMap := f.processes[virtualClusterName]
		for instance, nodeMap := range instanceMap {
			// We need to start each process sequentially as only some of the processes may
			// have exited out.
			for node := range nodeMap {
				l.Printf("attempting to start process %s on node %v", virtualClusterName, node)
				err := f.c.WithNodes(install.Nodes{node}).Start(ctx, l, install.StartOpts{
					VirtualClusterName: virtualClusterName,
					SQLInstance:        instance,
					IsRestart:          true,
				})
				if err != nil {
					l.Printf("failed to restart process %s on node %v: %v", virtualClusterName, node, err)
				}
			}
		}
	}
	return nil
}

func (f *GenericFailure) targetVirtualCluster(opts []virtualClusterOptFunc) (string, int) {
	o := &virtualClusterOpt{
		name:     f.defaultVirtualCluster.name,
		instance: f.defaultVirtualCluster.instance,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o.name, o.instance
}
