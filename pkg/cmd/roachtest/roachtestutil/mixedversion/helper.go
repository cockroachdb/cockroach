// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mixedversion

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"path"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

type (
	// ServiceDescriptor encapsulates the information about where a
	// service (system tenant or otherwise) is running.
	ServiceDescriptor struct {
		// Name is the name of the service ("system" for the system
		// tenant, or the tenant name otherwise.)
		Name string

		// Nodes is the set of nodes in the cluster where clients can
		// connect to that service.
		Nodes option.NodeListOption
	}

	// Service implements helper functions on behalf of a specific
	// service. Internal fields are provided by the testRunner struct,
	// allowing us to connect to a specific node and check live the test
	// runner's view of cluster versions, etc.
	Service struct {
		*ServiceContext

		ctx             context.Context
		connFunc        func(int) *gosql.DB
		stepLogger      *logger.Logger
		clusterVersions *atomic.Value
	}

	// Helper is the struct passed to `stepFunc`s (user-provided or
	// implemented by the framework) that provides helper functions that
	// mixed-version tests can use.
	Helper struct {
		System *Service
		Tenant *Service

		testContext Context

		ctx context.Context
		// bgCount keeps track of the number of background tasks started
		// with `helper.Background()`. The counter is used to generate
		// unique log file names.
		bgCount    int64
		runner     *testRunner
		stepLogger *logger.Logger
	}
)

func (s *Service) Connect(node int) *gosql.DB {
	return s.connFunc(node)
}

func (s *Service) RandomDB(rng *rand.Rand) (int, *gosql.DB) {
	node := s.Descriptor.Nodes.SeededRandNode(rng)[0]
	return node, s.Connect(node)
}

// prepareQuery returns a connection to one of the `nodes` provided
// and logs the query and gateway node in the step's log file. Called
// before the query is actually performed.
func (s *Service) prepareQuery(
	rng *rand.Rand, nodes option.NodeListOption, query string, args ...any,
) (*gosql.DB, error) {
	node := nodes.SeededRandNode(rng)[0]
	db := s.Connect(node)

	v, err := s.NodeVersion(node)
	if err != nil {
		return nil, err
	}
	logSQL(
		s.stepLogger, node, v, s.Descriptor.Name, query, args...,
	)

	return db, nil
}

func (s *Service) Query(rng *rand.Rand, query string, args ...interface{}) (*gosql.Rows, error) {
	db, err := s.prepareQuery(rng, s.Descriptor.Nodes, query, args...)
	handleInternalError(err)
	return db.QueryContext(s.ctx, query, args...)
}

func (s *Service) QueryRow(rng *rand.Rand, query string, args ...interface{}) *gosql.Row {
	db, err := s.prepareQuery(rng, s.Descriptor.Nodes, query, args...)
	handleInternalError(err)
	return db.QueryRowContext(s.ctx, query, args...)
}

func (s *Service) Exec(rng *rand.Rand, query string, args ...interface{}) error {
	return s.ExecWithGateway(rng, s.Descriptor.Nodes, query, args...)
}

func (s *Service) ExecWithGateway(
	rng *rand.Rand, nodes option.NodeListOption, query string, args ...interface{},
) error {
	db, err := s.prepareQuery(rng, nodes, query, args...)
	if err != nil {
		return err
	}

	_, err = db.ExecContext(s.ctx, query, args...)
	return err
}

func (s *Service) ClusterVersion(rng *rand.Rand) (roachpb.Version, error) {
	if s.Finalizing {
		n, db := s.RandomDB(rng)
		s.stepLogger.Printf("querying cluster version through node %d", n)
		cv, err := clusterupgrade.ClusterVersion(s.ctx, db)
		if err != nil {
			return roachpb.Version{}, fmt.Errorf("failed to query cluster version: %w", err)
		}

		return cv, nil
	}

	return loadAtomicVersions(s.clusterVersions)[0], nil
}

func (s *Service) ClusterVersionAtLeast(rng *rand.Rand, v string) (bool, error) {
	minVersion, err := roachpb.ParseVersion(v)
	if err != nil {
		return false, err
	}

	currentVersion, err := s.ClusterVersion(rng)
	if err != nil {
		return false, err
	}

	return currentVersion.AtLeast(minVersion), nil
}

func (h *Helper) DefaultService() *Service {
	if h.Tenant != nil {
		return h.Tenant
	}

	return h.System
}

func (h *Helper) Context() *ServiceContext {
	return h.DefaultService().ServiceContext
}

func (h *Helper) IsFinalizing() bool {
	return h.testContext.Finalizing()
}

func (h *Helper) Connect(node int) *gosql.DB {
	return h.DefaultService().Connect(node)
}

func (h *Helper) RandomDB(rng *rand.Rand) (int, *gosql.DB) {
	return h.DefaultService().RandomDB(rng)
}

// Query performs `db.QueryContext` on a randomly picked database node. The
// query and the node picked are logged in the logs of the step that calls this
// function.
func (h *Helper) Query(rng *rand.Rand, query string, args ...interface{}) (*gosql.Rows, error) {
	return h.DefaultService().Query(rng, query, args...)
}

// QueryRow performs `db.QueryRowContext` on a randomly picked
// database node. The query and the node picked are logged in the logs
// of the step that calls this function.
func (h *Helper) QueryRow(rng *rand.Rand, query string, args ...interface{}) *gosql.Row {
	return h.DefaultService().QueryRow(rng, query, args...)
}

// Exec performs `db.ExecContext` on a randomly picked database node.
// The query and the node picked are logged in the logs of the step
// that calls this function.
func (h *Helper) Exec(rng *rand.Rand, query string, args ...interface{}) error {
	return h.DefaultService().Exec(rng, query, args...)
}

// ExecWithGateway is like Exec, but allows the caller to specify the
// set of nodes that should be used as gateway. Especially useful in
// combination with Context methods, for example:
//
//	h.ExecWithGateway(rng, h.Context().NodesInNextVersion(), "SELECT 1")
func (h *Helper) ExecWithGateway(
	rng *rand.Rand, nodes option.NodeListOption, query string, args ...interface{},
) error {
	return h.DefaultService().ExecWithGateway(rng, nodes, query, args...)
}

// Background allows test authors to create functions that run in the
// background in mixed-version hooks.
func (h *Helper) Background(
	name string, fn func(context.Context, *logger.Logger) error,
) context.CancelFunc {
	return h.runner.background.Start(name, func(ctx context.Context) error {
		bgLogger, err := h.loggerFor(name)
		if err != nil {
			return fmt.Errorf("failed to create logger for background function %q: %w", name, err)
		}

		err = panicAsError(bgLogger, func() error { return fn(ctx, bgLogger) })
		if err != nil {
			if isContextCanceled(ctx) {
				return err
			}

			err := errors.Wrapf(err, "error in background function %s", name)
			return h.runner.testFailure(err, bgLogger, nil)
		}

		return nil
	})
}

// BackgroundCommand has the same semantics of `Background()`; the
// command passed will run and the test will fail if the command is
// not successful.
func (h *Helper) BackgroundCommand(cmd string, nodes option.NodeListOption) context.CancelFunc {
	desc := fmt.Sprintf("run command: %q", cmd)
	return h.Background(desc, func(ctx context.Context, l *logger.Logger) error {
		l.Printf("running command `%s` on nodes %v in the background", cmd, nodes)
		return h.runner.cluster.RunE(ctx, option.WithNodes(nodes), cmd)
	})
}

// ExpectDeath alerts the testing infrastructure that a node is
// expected to die. Regular restarts as part of the mixedversion
// testing are already taken into account. This function should only
// be used by tests that perform their own node restarts or chaos
// events.
func (h *Helper) ExpectDeath() {
	h.ExpectDeaths(1)
}

// ExpectDeaths is the general version of `ExpectDeath()`.
func (h *Helper) ExpectDeaths(n int) {
	h.runner.monitor.ExpectDeaths(n)
}

// ClusterVersion returns the currently active cluster version. Avoids
// querying the database if we are not running migrations, since the
// test runner has cached version of the cluster versions.
//
// WARNING: since this function uses the cached cluster version, it is
// NOT safe to be called by tests that cause the cluster version to
// change by means other than an upgrade (e.g., a cluster wipe). Use
// `clusterupgrade.ClusterVersion` in that case.
func (h *Helper) ClusterVersion(rng *rand.Rand) (roachpb.Version, error) {
	return h.DefaultService().ClusterVersion(rng)
}

// ClusterVersionAtLeast checks whether the cluster version is at
// least the cluster version string passed.
//
// The warning in (*Helper).ClusterVersion() applies here too.
func (h *Helper) ClusterVersionAtLeast(rng *rand.Rand, v string) (bool, error) {
	return h.DefaultService().ClusterVersionAtLeast(rng, v)
}

// loggerFor creates a logger instance to be used by background
// functions (created by calling `Background` on the helper
// instance). It is similar to the logger instances created for
// mixed-version steps, but with the `background_` prefix.
func (h *Helper) loggerFor(name string) (*logger.Logger, error) {
	atomic.AddInt64(&h.bgCount, 1)

	fileName := invalidChars.ReplaceAllString(strings.ToLower(name), "")
	fileName = fmt.Sprintf("background_%s_%d", fileName, h.bgCount)
	fileName = path.Join(logPrefix, fileName)

	return prefixedLogger(h.runner.logger, fileName)
}

// logSQL standardizes the logging when a SQL statement or query is
// run using one of the Helper methods. It includes the node used as
// gateway, along with the version currently running on it, for ease
// of debugging. If a non-empty `virtualCluster` is passed (UA
// clusters), that is also logged.
func logSQL(
	l *logger.Logger,
	node int,
	v *clusterupgrade.Version,
	virtualCluster string,
	stmt string,
	args ...interface{},
) {
	var lines []string
	addLine := func(format string, args ...interface{}) {
		lines = append(lines, fmt.Sprintf(format, args...))
	}

	addLine("running SQL")
	addLine("Node:      %d (%s)", node, v)
	addLine("Tenant:    %s", virtualCluster)
	addLine("Statement: %s", stmt)
	addLine("Arguments: %v", args)

	l.Printf("%s", strings.Join(lines, "\n"))
}

// handleInternalError can be used when the caller does not expect any
// errors from a function call. If the error value provided is not
// nil, we'll panic with an internal error message.
func handleInternalError(err error) {
	if err == nil {
		return
	}

	panic(fmt.Errorf("mixedversion internal error: %w", err))
}
