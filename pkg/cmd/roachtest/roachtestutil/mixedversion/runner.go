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
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type (
	// Helper is the struct passed to user-functions providing helper
	// functions that mixed-version tests can use.
	Helper struct {
		ctx         context.Context
		testContext *Context
		// bgCount keeps track of the number of background tasks started
		// with `helper.Background()`. The counter is used to generate
		// unique log file names.
		bgCount    int64
		runner     *testRunner
		stepLogger *logger.Logger
	}

	// backgroundEvent is the struct sent by background steps when they
	// finish (successfully or not).
	backgroundEvent struct {
		Name string
		Err  error
	}

	backgroundRunner struct {
		group  ctxgroup.Group
		ctx    context.Context
		events chan backgroundEvent
	}

	testFailure struct {
		summarized     bool
		description    string
		seed           int64
		binaryVersions []roachpb.Version
		// Cluster versions before and after the failure occurred. Before
		// each step is executed, the test runner will cache each node's
		// view of the cluster version; after a failure occurs, we'll try
		// to read the cluster version from every node again. This context
		// is added to the failure message displayed to the user with the
		// intention of highlighting whether the cluster version changed
		// during the failure, which is useful for test failures that
		// happen while the upgrade is finalizing.
		clusterVersionsBefore []roachpb.Version
		clusterVersionsAfter  []roachpb.Version
	}

	testRunner struct {
		ctx       context.Context
		cancel    context.CancelFunc
		plan      *TestPlan
		cluster   cluster.Cluster
		crdbNodes option.NodeListOption
		seed      int64
		logger    *logger.Logger

		binaryVersions  atomic.Value
		clusterVersions atomic.Value

		background *backgroundRunner

		connCache []*gosql.DB
	}
)

var (
	// everything that is not an alphanum or a few special characters
	invalidChars = regexp.MustCompile(`[^a-zA-Z0-9 \-_\.]`)
)

func newTestRunner(
	ctx context.Context,
	cancel context.CancelFunc,
	plan *TestPlan,
	l *logger.Logger,
	c cluster.Cluster,
	crdbNodes option.NodeListOption,
	randomSeed int64,
) *testRunner {
	return &testRunner{
		ctx:        ctx,
		cancel:     cancel,
		plan:       plan,
		logger:     l,
		cluster:    c,
		crdbNodes:  crdbNodes,
		background: newBackgroundRunner(ctx),
		seed:       randomSeed,
	}
}

// run implements the test running logic, which boils down to running
// each step in sequence.
func (tr *testRunner) run() error {
	defer tr.closeConnections()
	defer func() {
		tr.logger.Printf("canceling mixed-version test context")
		tr.cancel()
		// Wait for some time so that any background tasks are properly
		// canceled and their cancelation messages are displayed in the
		// logs accordingly.
		time.Sleep(5 * time.Second)
	}()

	stepsErr := make(chan error)
	go func() {
		defer close(stepsErr)
		for _, step := range tr.plan.steps {
			if err := tr.runStep(tr.ctx, step); err != nil {
				stepsErr <- err
				return
			}
		}
	}()

	for {
		select {
		case err := <-stepsErr:
			return err
		case event := <-tr.background.CompletedEvents():
			if event.Err == nil {
				tr.logger.Printf("background step finished: %s", event.Name)
				continue
			}

			return fmt.Errorf("background step `%s` returned error: %w", event.Name, event.Err)
		}
	}
}

// runStep contains the logic of running a single test step, called
// recursively in the case of sequentialRunStep and concurrentRunStep.
func (tr *testRunner) runStep(ctx context.Context, step testStep) error {
	if ss, ok := step.(singleStep); ok {
		if ss.ID() == 1 {
			// if this is the first singleStep of the plan, ensure it is an
			// "initialization step" (i.e., cockroach nodes are ready after
			// it executes). This is an assumption of the test runner and
			// makes for clear error messages if that assumption is broken.
			if err := tr.ensureInitializationStep(ss); err != nil {
				return err
			}
		} else {
			// update the runner's view of the cluster's binary and cluster
			// versions before every non-initialization `singleStep` is
			// executed
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
	}

	switch s := step.(type) {
	case sequentialRunStep:
		for _, ss := range s.steps {
			if err := tr.runStep(ctx, ss); err != nil {
				return err
			}
		}
		return nil

	case concurrentRunStep:
		group := ctxgroup.WithContext(tr.ctx)
		for _, cs := range s.delayedSteps {
			cs := cs
			group.GoCtx(func(concurrentCtx context.Context) error {
				return tr.runStep(concurrentCtx, cs)
			})
		}
		return group.Wait()

	case delayedStep:
		time.Sleep(s.delay)
		return tr.runStep(ctx, s.step)

	default:
		ss := s.(singleStep)
		stepLogger, err := tr.loggerFor(ss)
		if err != nil {
			return err
		}

		if ss.Background() {
			tr.startBackgroundStep(ss, stepLogger)
			return nil
		}

		return tr.runSingleStep(ctx, ss, stepLogger)
	}
}

// runSingleStep takes care of the logic of running a `singleStep`,
// including logging start and finish times, wrapping the error (if
// any) with useful information, and renaming the log file to indicate
// failure. This logic is the same whether running a step in the
// background or not.
func (tr *testRunner) runSingleStep(ctx context.Context, ss singleStep, l *logger.Logger) error {
	tr.logStep("STARTING", ss, l)
	tr.logVersions(l)
	start := timeutil.Now()
	defer func() {
		prefix := fmt.Sprintf("FINISHED [%s]", timeutil.Since(start))
		tr.logStep(prefix, ss, l)
	}()
	if err := ss.Run(ctx, l, tr.cluster, tr.newHelper(ctx, l)); err != nil {
		if isContextCanceled(err) {
			l.Printf("step terminated (context canceled)")
			return nil
		}
		return tr.stepError(err, ss, l)
	}

	return nil
}

func (tr *testRunner) startBackgroundStep(ss singleStep, l *logger.Logger) {
	tr.background.Start(ss.Description(), func(ctx context.Context) error {
		return tr.runSingleStep(ctx, ss, l)
	})
}

// stepError generates a `testFailure` error by augmenting the error
// passed with extra information. Specifically, the error message will
// include the ID of the step that failed, the random seed used, the
// binary version on each node when the error occurred, and the
// cluster version before and after the step (in case the failure
// happened *while* the cluster version was updating).
func (tr *testRunner) stepError(err error, step singleStep, l *logger.Logger) error {
	desc := fmt.Sprintf("mixed-version test failure while running step %d (%s): %s",
		step.ID(), step.Description(), err,
	)

	return tr.testFailure(desc, l)
}

// testFailure generates a `testFailure` with the given
// description. It logs the error to the logger passed, and renames
// the underlying file to include the "FAILED" prefix to help in
// debugging.
func (tr *testRunner) testFailure(desc string, l *logger.Logger) error {
	clusterVersionsBefore := tr.clusterVersions
	if err := tr.refreshClusterVersions(); err != nil {
		tr.logger.Printf("failed to fetch cluster versions after failure: %s", err)
	}
	clusterVersionsAfter := tr.clusterVersions

	tf := &testFailure{
		description:           desc,
		seed:                  tr.seed,
		binaryVersions:        loadAtomicVersions(tr.binaryVersions),
		clusterVersionsBefore: loadAtomicVersions(clusterVersionsBefore),
		clusterVersionsAfter:  loadAtomicVersions(clusterVersionsAfter),
	}

	// Print the test failure on the step's logger for convenience, and
	// to reduce cross referencing of logs.
	l.Printf("%v", tf)

	if err := renameFailedLogger(l); err != nil {
		tr.logger.Printf("could not rename failed step logger: %v", err)
	}

	return tf
}

func (tr *testRunner) logStep(prefix string, step singleStep, l *logger.Logger) {
	dashes := strings.Repeat("-", 10)
	l.Printf("%[1]s %s (%d): %s %[1]s", dashes, prefix, step.ID(), step.Description())
}

// logVersions writes the current cached versions of the binary and
// cluster versions on each node. The cached versions should exist for
// all steps but the first one (when we start the cluster itself).
func (tr *testRunner) logVersions(l *logger.Logger) {
	binaryVersions := loadAtomicVersions(tr.binaryVersions)
	clusterVersions := loadAtomicVersions(tr.clusterVersions)

	if binaryVersions == nil || clusterVersions == nil {
		return
	}

	l.Printf("binary versions: %s", formatVersions(binaryVersions))
	l.Printf("cluster versions: %s", formatVersions(clusterVersions))
}

// loggerFor creates a logger instance to be used by a test step. Logs
// will be available under `mixed-version-test/{ID}.log`, making it
// easy to go from the IDs displayed in the test plan to the
// corresponding output of that step.
func (tr *testRunner) loggerFor(step singleStep) (*logger.Logger, error) {
	name := invalidChars.ReplaceAllString(strings.ToLower(step.Description()), "")
	name = fmt.Sprintf("%d_%s", step.ID(), name)

	prefix := path.Join(logPrefix, name)
	return prefixedLogger(tr.logger, prefix)
}

// refreshBinaryVersions updates the internal `binaryVersions` field
// with the binary version running on each node of the cluster. We use
// the `atomic` package here as this function may be called by two
// steps that are running concurrently.
func (tr *testRunner) refreshBinaryVersions() error {
	newBinaryVersions := make([]roachpb.Version, 0, len(tr.crdbNodes))
	for _, node := range tr.crdbNodes {
		bv, err := clusterupgrade.BinaryVersion(tr.conn(node))
		if err != nil {
			return fmt.Errorf("failed to get binary version for node %d: %w", node, err)
		}
		newBinaryVersions = append(newBinaryVersions, bv)
	}

	tr.binaryVersions.Store(newBinaryVersions)
	return nil
}

// refreshClusterVersions updates the internal `clusterVersions` field
// with the current view of the cluster version in each of the nodes
// of the cluster.
func (tr *testRunner) refreshClusterVersions() error {
	newClusterVersions := make([]roachpb.Version, 0, len(tr.crdbNodes))
	for _, node := range tr.crdbNodes {
		cv, err := clusterupgrade.ClusterVersion(tr.ctx, tr.conn(node))
		if err != nil {
			return fmt.Errorf("failed to get cluster version for node %d: %w", node, err)
		}
		newClusterVersions = append(newClusterVersions, cv)
	}

	tr.clusterVersions.Store(newClusterVersions)
	return nil
}

func (tr *testRunner) ensureInitializationStep(ss singleStep) error {
	_, isInit := ss.(startFromCheckpointStep)
	if !isInit {
		return fmt.Errorf("unexpected initialization type in mixed-version test: %T", ss)
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

func (tr *testRunner) newHelper(ctx context.Context, l *logger.Logger) *Helper {
	return &Helper{
		ctx:        ctx,
		runner:     tr,
		stepLogger: l,
	}
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

func newBackgroundRunner(ctx context.Context) *backgroundRunner {
	g := ctxgroup.WithContext(ctx)
	return &backgroundRunner{
		group:  g,
		ctx:    ctx,
		events: make(chan backgroundEvent),
	}
}

// Start will run the function `fn` in a goroutine. Any errors
// returned by that function are observable by reading from the
// channel returned by the `Events()` function.
func (br *backgroundRunner) Start(name string, fn func(context.Context) error) {
	br.group.Go(func() error {
		err := fn(br.ctx)
		br.events <- backgroundEvent{
			Name: name,
			Err:  err,
		}
		return err
	})
}

func (br *backgroundRunner) CompletedEvents() <-chan backgroundEvent {
	return br.events
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
	node, db := h.RandomDB(rng, h.runner.crdbNodes)
	h.stepLogger.Printf("running SQL statement:\n%s\nArgs: %v\nNode: %d", query, args, node)
	_, err := db.ExecContext(h.ctx, query, args...)
	return err
}

func (h *Helper) Connect(node int) *gosql.DB {
	return h.runner.conn(node)
}

// SetContext should be called by steps that need access to the test
// context, as that is only visible to them.
func (h *Helper) SetContext(c *Context) {
	h.testContext = c
}

// Context returns the test context associated with a certain step. It
// is made available for user-functions (see runHookStep).
func (h *Helper) Context() *Context {
	return h.testContext
}

// Background allows test authors to create functions that run in the
// background in mixed-version hooks.
func (h *Helper) Background(name string, fn func(context.Context, *logger.Logger) error) {
	h.runner.background.Start(name, func(ctx context.Context) error {
		bgLogger, err := h.loggerFor(name)
		if err != nil {
			return fmt.Errorf("failed to create logger for background function %q: %w", name, err)
		}

		err = fn(ctx, bgLogger)
		if err != nil {
			if isContextCanceled(err) {
				bgLogger.Printf("background function terminated (context canceled)")
				return nil
			}

			desc := fmt.Sprintf("error in background function %s: %s", name, err)
			return h.runner.testFailure(desc, bgLogger)
		}

		return nil
	})
}

// BackgroundCommand has the same semantics of `Background()`; the
// command passed will run and the test will fail if the command is
// not successful.
func (h *Helper) BackgroundCommand(cmd string, nodes option.NodeListOption) {
	desc := fmt.Sprintf("run command: %q", cmd)
	h.Background(desc, func(ctx context.Context, l *logger.Logger) error {
		l.Printf("running command `%s` on nodes %v in the background", cmd, nodes)
		return h.runner.cluster.RunE(ctx, nodes, cmd)
	})
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

func (tf *testFailure) Error() string {
	if tf.summarized {
		return tf.description
	}

	tf.summarized = true
	debugInfo := func(label, value string) string {
		return fmt.Sprintf("%-40s%s", label+":", value)
	}
	seedInfo := debugInfo("test random seed", strconv.FormatInt(tf.seed, 10))
	binaryVersions := debugInfo("binary versions", formatVersions(tf.binaryVersions))
	clusterVersionsBefore := debugInfo(
		"cluster versions before failure",
		formatVersions(tf.clusterVersionsBefore),
	)
	var clusterVersionsAfter string
	if cv := tf.clusterVersionsAfter; cv != nil {
		clusterVersionsBefore += "\n"
		clusterVersionsAfter = debugInfo("cluster versions after failure", formatVersions(cv))
	}

	return fmt.Sprintf(
		"%s\n%s\n%s\n%s%s",
		tf.description, seedInfo, binaryVersions, clusterVersionsBefore, clusterVersionsAfter,
	)
}

func renameFailedLogger(l *logger.Logger) error {
	currentFileName := l.File.Name()
	newLogName := path.Join(
		filepath.Dir(currentFileName),
		"FAILED_"+filepath.Base(currentFileName),
	)
	return os.Rename(currentFileName, newLogName)
}

func loadAtomicVersions(v atomic.Value) []roachpb.Version {
	if v.Load() == nil {
		return nil
	}

	return v.Load().([]roachpb.Version)
}

func formatVersions(versions []roachpb.Version) string {
	var pairs []string
	for idx, version := range versions {
		pairs = append(pairs, fmt.Sprintf("%d: %s", idx+1, version))
	}

	return fmt.Sprintf("[%s]", strings.Join(pairs, ", "))
}

// isContextCanceled returns a boolean indicating whether the error
// given happened because some context was canceled.
func isContextCanceled(err error) bool {
	// TODO(renato): unfortunately, we have to resort to string
	// comparison here. The most common use case for this function is
	// detecting cluster commands that fail when the test context is
	// canceled (after test success or failure), and roachtest does not
	// return an error that wraps the context cancelation (in other
	// words, `errors.Is` doesn't work). Once we fix this behavior, we
	// should use structured errors here.
	return strings.Contains(err.Error(), context.Canceled.Error())
}
