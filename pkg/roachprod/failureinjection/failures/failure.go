// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
//
// For now, this interface is not necessarily needed, however it sets up for
// future failure injection work when we want a failure controller to be
// able to inject multiple different types of failures.
type FailureMode interface {
	Description() string

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
	// TODO(Darryl): support specifying virtual clusters
	c *install.SyncedCluster
	// runTitle is the title to prefix command output with.
	runTitle          string
	networkInterfaces []string
	diskDevice        diskDevice
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
	ctx context.Context, l *logger.Logger, nodes install.Nodes,
) error {
	// TODO(darryl): Consider having failure modes accept a db connection pool
	// in makeFailureFunc() or having each failureMode manage it's own pool.
	res, err := f.c.ExecSQL(
		ctx, l, nodes, install.SystemInterfaceName,
		0, install.AuthUserCert, "", /* database */
		[]string{"-e", "SELECT 1"},
	)

	return errors.CombineErrors(err, res[0].Err)
}

func (f *GenericFailure) WaitForSQLReady(
	ctx context.Context, l *logger.Logger, node install.Nodes, timeout time.Duration,
) error {
	start := timeutil.Now()
	err := retryForDuration(ctx, timeout, func() error {
		if err := f.PingNode(ctx, l, node); err == nil {
			l.Printf("Connected to node %d after %s", node, timeutil.Since(start))
			return nil
		}
		return errors.Newf("unable to connect to node %d", node)
	})

	return errors.Wrapf(err, "never connected to node %d after %s", node, timeout)
}

// WaitForSQLUnavailable pings a node until the SQL connection is unavailable.
func (f *GenericFailure) WaitForSQLUnavailable(
	ctx context.Context, l *logger.Logger, node install.Nodes, timeout time.Duration,
) error {
	start := timeutil.Now()
	err := retryForDuration(ctx, timeout, func() error {
		if err := f.PingNode(ctx, l, node); err != nil {
			l.Printf("Connections to node %d unavailable after %s", node, timeutil.Since(start))
			//nolint:returnerrcheck
			return nil
		}
		return errors.Newf("unable to connect to node %d", node)
	})

	return errors.Wrapf(err, "connections to node %d never unavailable after %s", node, timeout)
}

func (f *GenericFailure) StopCluster(
	ctx context.Context, l *logger.Logger, stopOpts roachprod.StopOpts,
) error {
	return f.c.Stop(ctx, l, stopOpts.Sig, stopOpts.Wait, stopOpts.GracePeriod, "" /* VirtualClusterName*/)
}

func (f *GenericFailure) StartCluster(ctx context.Context, l *logger.Logger) error {
	return f.StartNodes(ctx, l, f.c.Nodes)
}

func (f *GenericFailure) StartNodes(
	ctx context.Context, l *logger.Logger, nodes install.Nodes,
) error {
	// Invoke the cockroach start script directly so we restart the nodes with the same
	// arguments as before.
	return f.Run(ctx, l, nodes, "./cockroach.sh")
}

// retryForDuration retries the given function until it returns nil or
// the context timeout is exceeded.
func retryForDuration(ctx context.Context, timeout time.Duration, fn func() error) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	retryOpts := retry.Options{MaxRetries: 0}
	r := retry.StartWithCtx(timeoutCtx, retryOpts)
	for r.Next() {
		if err := fn(); err == nil {
			return nil
		}
	}
	return errors.Newf("failed after %s", timeout)
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
