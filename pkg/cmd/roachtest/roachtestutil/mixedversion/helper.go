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
)

// Helper is the struct passed to user-functions providing helper
// functions that mixed-version tests can use.
type Helper struct {
	Context *Context

	ctx context.Context
	// bgCount keeps track of the number of background tasks started
	// with `helper.Background()`. The counter is used to generate
	// unique log file names.
	bgCount    int64
	runner     *testRunner
	stepLogger *logger.Logger
}

func (h *Helper) RandomNode(prng *rand.Rand, nodes option.NodeListOption) int {
	return nodes[prng.Intn(len(nodes))]
}

// RandomDB returns a (nodeID, connection) tuple for a randomly picked
// cockroach node according to the parameters passed.
func (h *Helper) RandomDB(prng *rand.Rand, nodes option.NodeListOption) (int, *gosql.DB) {
	node := h.RandomNode(prng, nodes)
	return node, h.Connect(node)
}

// Query performs `db.QueryContext` on a randomly picked database node. The
// query and the node picked are logged in the logs of the step that calls this
// function.
func (h *Helper) Query(rng *rand.Rand, query string, args ...interface{}) (*gosql.Rows, error) {
	node, db := h.RandomDB(rng, h.runner.crdbNodes)
	h.stepLogger.Printf("running SQL statement:\n%s\nArgs: %v\nNode: %d", query, args, node)
	return db.QueryContext(h.ctx, query, args...)
}

// QueryRow performs `db.QueryRowContext` on a randomly picked
// database node. The query and the node picked are logged in the logs
// of the step that calls this function.
func (h *Helper) QueryRow(rng *rand.Rand, query string, args ...interface{}) *gosql.Row {
	node, db := h.RandomDB(rng, h.runner.crdbNodes)
	h.stepLogger.Printf("running SQL statement:\n%s\nArgs: %v\nNode: %d", query, args, node)
	return db.QueryRowContext(h.ctx, query, args...)
}

// Exec performs `db.ExecContext` on a randomly picked database node.
// The query and the node picked are logged in the logs of the step
// that calls this function.
func (h *Helper) Exec(rng *rand.Rand, query string, args ...interface{}) error {
	return h.ExecWithGateway(rng, h.runner.crdbNodes, query, args...)
}

// ExecWithGateway is like Exec, but allows the caller to specify the
// set of nodes that should be used as gateway. Especially useful in
// combination with Context methods, for example:
//
//	h.ExecWithGateway(rng, h.Context.NodesInNextVersion(), "SELECT 1")
func (h *Helper) ExecWithGateway(
	rng *rand.Rand, nodes option.NodeListOption, query string, args ...interface{},
) error {
	node, db := h.RandomDB(rng, nodes)
	h.stepLogger.Printf("running SQL statement:\n%s\nArgs: %v\nNode: %d", query, args, node)
	_, err := db.ExecContext(h.ctx, query, args...)
	return err
}

func (h *Helper) Connect(node int) *gosql.DB {
	return h.runner.conn(node)
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

			desc := fmt.Sprintf("error in background function %s: %s", name, err)
			return h.runner.testFailure(desc, bgLogger, nil)
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
		return h.runner.cluster.RunE(ctx, nodes, cmd)
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
	if h.Context.Finalizing {
		n, db := h.RandomDB(rng, h.runner.crdbNodes)
		h.stepLogger.Printf("querying cluster version through node %d", n)
		cv, err := clusterupgrade.ClusterVersion(h.ctx, db)
		if err != nil {
			return roachpb.Version{}, fmt.Errorf("failed to query cluster version: %w", err)
		}

		return cv, nil
	}

	return loadAtomicVersions(h.runner.clusterVersions)[0], nil
}

// ClusterVersionAtLeast checks whether the cluster version is at
// least the cluster version string passed.
//
// The warning in (*Helper).ClusterVersion() applies here too.
func (h *Helper) ClusterVersionAtLeast(rng *rand.Rand, v string) (bool, error) {
	minVersion, err := roachpb.ParseVersion(v)
	if err != nil {
		return false, err
	}

	currentVersion, err := h.ClusterVersion(rng)
	if err != nil {
		return false, err
	}

	return currentVersion.AtLeast(minVersion), nil
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
