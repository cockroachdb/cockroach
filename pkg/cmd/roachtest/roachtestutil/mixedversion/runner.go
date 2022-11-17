// Copyright 2022 The Cockroach Authors.
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
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/sync/errgroup"
)

type (
	testRunner struct {
		ctx       context.Context
		plan      *TestPlan
		cluster   cluster.Cluster
		crdbNodes option.NodeListOption
		seed      int64
		logger    *logger.Logger

		binaryVersions  []roachpb.Version
		clusterVersions []roachpb.Version

		connCache []*gosql.DB
	}
)

var (
	invalidChars = regexp.MustCompile(`[^a-zA-Z0-9 \-_]`)
)

func newTestRunner(
	ctx context.Context,
	plan *TestPlan,
	l *logger.Logger,
	c cluster.Cluster,
	crdbNodes option.NodeListOption,
	randomSeed int64,
) *testRunner {
	return &testRunner{
		ctx:       ctx,
		plan:      plan,
		logger:    l,
		cluster:   c,
		crdbNodes: crdbNodes,
		seed:      randomSeed,
	}
}

// run implements the test running logic, which boils down to running
// each step in sequence.
func (tr *testRunner) run() error {
	defer tr.closeConnections()

	for _, step := range tr.plan.steps {
		if err := tr.runStep(step); err != nil {
			return err
		}
	}

	return nil
}

// runStep contains the logic of running a single test step, called
// recursively in the case of sequentialRunStep and concurrentRunStep.
func (tr *testRunner) runStep(step testStep) error {
	if ss, ok := step.(singleStep); ok && ss.ID() > 1 {
		// if we are running a singleStep that is *not* the first step,
		// we can initialize the database connections. This represents the
		// assumption that the first step in the test plan is the one that
		// sets up binaries and make the `cockroach` process available on
		// the nodes.
		if err := tr.maybeInitConnections(); err != nil {
			return err
		}
		if err := tr.refreshBinaryVersions(); err != nil {
			return err
		}
		if err := tr.refreshClusterVersions(); err != nil {
			return err
		}
	}

	switch s := step.(type) {
	case sequentialRunStep:
		for _, ss := range s.steps {
			if err := tr.runStep(ss); err != nil {
				return err
			}
		}
		return nil

	case concurrentRunStep:
		group, _ := errgroup.WithContext(tr.ctx)
		for _, cs := range s.delayedSteps {
			cs := cs
			group.Go(func() error {
				return tr.runStep(cs)
			})
		}
		return group.Wait()

	case delayedStep:
		time.Sleep(s.delay)
		return tr.runStep(s.step)

	default:
		ss := s.(singleStep)
		stepLogger, err := tr.loggerFor(ss)
		if err != nil {
			return err
		}

		tr.logStep("STARTING", ss, stepLogger)
		start := timeutil.Now()
		defer func() {
			prefix := fmt.Sprintf("FINISHED [%s]", timeutil.Since(start))
			tr.logStep(prefix, ss, stepLogger)
		}()
		if err := ss.Run(tr.ctx, stepLogger, tr.cluster, tr.conn); err != nil {
			return tr.reportError(err, ss, stepLogger)
		}

		return nil
	}
}

// reportError augments the error passed with extra
// information. Specifically, the error message will include the ID of
// the step that failed, the random seed used, the binary version on
// each node when the error occurred, and the cluster version before
// and after the step (in case the failure happened *while* the
// cluster version was updating).
func (tr *testRunner) reportError(err error, step singleStep, l *logger.Logger) error {
	errMsg := fmt.Sprintf("mixed-version test failure while running step %d (%s): %s",
		step.ID(), step.Description(), err,
	)
	debugInfo := func(label, value string) string {
		return fmt.Sprintf("%-40s%s", label+":", value)
	}
	seedInfo := debugInfo("test random seed", strconv.FormatInt(tr.seed, 10))
	binaryVersions := debugInfo("binary versions", formatVersions(tr.binaryVersions))
	clusterVersionsBefore := debugInfo("cluster versions before failure", formatVersions(tr.clusterVersions))
	var clusterVersionsAfter string
	if err := tr.refreshClusterVersions(); err == nil {
		clusterVersionsBefore += "\n"
		clusterVersionsAfter = debugInfo("cluster versions after failure", formatVersions(tr.clusterVersions))
	} else {
		tr.logger.Printf("failed to fetch cluster versions after failure: %s", err)
	}

	if err := renameFailedLogger(l); err != nil {
		tr.logger.Printf("could not rename failed step logger: %v", err)
	}

	return fmt.Errorf(
		"%s\n%s\n%s\n%s%s",
		errMsg, seedInfo, binaryVersions, clusterVersionsBefore, clusterVersionsAfter,
	)
}

func (tr *testRunner) logStep(prefix string, step singleStep, l *logger.Logger) {
	dashes := strings.Repeat("-", 10)
	l.Printf("%[1]s %s (%d): %s %[1]s", dashes, prefix, step.ID(), step.Description())
}

// loggerFor creates a logger instance to be used by a test step. Logs
// will be available under `mixed-version-test/{ID}.log`, making it
// easy to go from the IDs displayed in the test plan to the
// corresponding output of that step.
func (tr *testRunner) loggerFor(step singleStep) (*logger.Logger, error) {
	name := invalidChars.ReplaceAllString(strings.ToLower(step.Description()), "")
	name = fmt.Sprintf("%d_%s", step.ID(), name)

	prefix := fmt.Sprintf("%s/%s", logPrefix, name)
	return prefixedLogger(tr.logger, prefix)
}

// refreshBinaryVersions updates the internal `binaryVersions` field
// with the binary version running on each node of the cluster.
func (tr *testRunner) refreshBinaryVersions() error {
	tr.binaryVersions = make([]roachpb.Version, 0, len(tr.crdbNodes))
	for _, node := range tr.crdbNodes {
		bv, err := clusterupgrade.BinaryVersion(tr.conn(node))
		if err != nil {
			return fmt.Errorf("failed to get binary version for node %d: %w", node, err)
		}
		tr.binaryVersions = append(tr.binaryVersions, bv)
	}

	return nil
}

// refreshClusterVersions updates the internal `clusterVersions` field
// with the current view of the cluster version in each of the nodes
// of the cluster.
func (tr *testRunner) refreshClusterVersions() error {
	tr.clusterVersions = make([]roachpb.Version, 0, len(tr.crdbNodes))
	for _, node := range tr.crdbNodes {
		cv, err := clusterupgrade.ClusterVersion(tr.ctx, tr.conn(node))
		if err != nil {
			return fmt.Errorf("failed to get cluster version for node %d: %w", node, err)
		}
		tr.clusterVersions = append(tr.clusterVersions, cv)
	}

	return nil
}

// maybeInitConnections initialize connections if the connection cache
// is empty.
func (tr *testRunner) maybeInitConnections() error {
	if tr.connCache != nil {
		return nil
	}

	tr.connCache = make([]*gosql.DB, len(tr.crdbNodes))
	for _, node := range tr.crdbNodes {
		conn, err := tr.cluster.ConnE(tr.ctx, tr.logger, node)
		if err != nil {
			return fmt.Errorf("failed to connect to node %d: %w", node, err)
		}

		tr.connCache[node-1] = conn
	}

	return nil
}

// conn returns a database connection to the given node. Assumes the
// connection cache has been previously initialized.
func (tr *testRunner) conn(node int) *gosql.DB {
	return tr.connCache[node-1]
}

func (tr *testRunner) closeConnections() {
	for _, db := range tr.connCache {
		if db != nil {
			_ = db.Close()
		}
	}
}

func renameFailedLogger(l *logger.Logger) error {
	currentFileName := l.File.Name()
	newLogName := strings.TrimSuffix(currentFileName, filepath.Ext(currentFileName))
	newLogName += "_FAILED.log"
	return os.Rename(currentFileName, newLogName)
}

func formatVersions(versions []roachpb.Version) string {
	var pairs []string
	for idx, version := range versions {
		pairs = append(pairs, fmt.Sprintf("%d: %s", idx+1, version))
	}

	return fmt.Sprintf("[%s]", strings.Join(pairs, ", "))
}
