// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
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

	// Setup any dependencies required for the failure to be injected.
	Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// Inject a failure into the system.
	Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// Restore reverses the effects of Inject. The same args passed to Inject
	// must be passed to Restore.
	Restore(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// Cleanup uninstalls any dependencies that were installed by Setup.
	Cleanup(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// WaitForFailureToPropagate waits until the failure is at full effect.
	WaitForFailureToPropagate(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// WaitForFailureToRestore waits until the failure was restored completely along with any side effects.
	WaitForFailureToRestore(ctx context.Context, l *logger.Logger, args FailureArgs) error
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
}

func (f *GenericFailure) Run(
	ctx context.Context, l *logger.Logger, node install.Nodes, args ...string,
) error {
	cmd := strings.Join(args, " ")
	// In general, most failures shouldn't be run locally out of caution.
	if f.c.IsLocal() {
		l.Printf("Local cluster detected, logging command instead of running:\n%s", cmd)
		return nil
	}
	return f.c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(node), f.runTitle, cmd)
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
	res, err := f.c.RunWithDetails(ctx, l, install.WithNodes(node), f.runTitle, cmd)
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
