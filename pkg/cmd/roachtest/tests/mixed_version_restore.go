// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

var (
	// retry options while waiting for a restore to complete
	restoreCompletionRetryOptions = retry.Options{
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     1 * time.Minute,
		Multiplier:     1.5,
		MaxRetries:     50,
	}
)

type (
	// TODO: this
	// backupCollection wraps a backup collection (which may or may not
	// contain incremental backups). The associated fingerprint is the
	// expected fingerprint when the corresponding table is restored.
	restoredDatabase struct {
		database string
	}

	// backupSpec indicates where backups are supposed to be planned
	// (`BACKUP` statement sent to); and where they are supposed to be
	// executed (where the backup job will be picked up).
	restoreSpec struct {
		Plan    labeledNodes
		Execute labeledNodes
	}
)

// backupCollectionDesc builds a string that describes how a backup
// collection comprised of a full backup and a follow-up incremental
// backup was generated (in terms of which versions planned vs
// executed the backup). Used to generate descriptive backup names.
func restoreDesc(s restoreSpec) string {
	if s.Plan.Version == s.Execute.Version {
		return fmt.Sprintf("restore planned and executed on %s", s.Plan.Version)
	}

	return fmt.Sprintf("restore planned on %s executed on %s", s.Plan.Version, s.Execute.Version)
}

type mixedVersionRestore struct {
	cluster       cluster.Cluster
	roachNodes    option.NodeListOption
	collectionURI string
	// databases containing the restored table that are created in the test.
	restoreDBs []*restoredDatabase
	// the database with the backed up table
	database string
	// the table being backed up/restored
	table string
	// counter that is incremented atomically to provide unique
	// identifiers to backups created during the test
	currentBackupID int64
}

func newMixedVersionRestore(
	c cluster.Cluster, roachNodes option.NodeListOption, database, table string,
) *mixedVersionRestore {
	mvr := &mixedVersionRestore{cluster: c, database: database, table: table, roachNodes: roachNodes}
	return mvr
}

// setShortJobIntervals increases the frequency of the adopt and
// cancel loops in the job registry. This enables changes to job state
// to be observed faster, and the test to run quicker.
func (*mixedVersionRestore) setShortJobIntervals(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	return setShortJobIntervalsCommon(func(query string, args ...interface{}) error {
		return h.Exec(rng, query, args...)
	})
}

// createBackupToRestore starts a CSV server in the background and runs
// imports a bank fixture. Blocks until the importing is finished, at
// which point the CSV server is terminated.
func (mvr *mixedVersionRestore) createBackupToRestore(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	var rows int
	if mvr.cluster.IsLocal() {
		rows = 100
		l.Printf("importing only %d rows (as this is a local run)", rows)
	} else {
		rows = rows3GiB
		l.Printf("importing %d rows", rows)
	}

	csvPort := 8081
	importNode := h.RandomNode(rng, mvr.roachNodes)
	l.Printf("decided to run import on node %d", importNode)

	// TODO: should this be current or previous?
	currentRoach := mixedversion.CurrentCockroachPath
	var stopCSVServer context.CancelFunc
	h.Background("csv server", func(bgCtx context.Context, bgLogger *logger.Logger) error {
		cmd := importBankCSVServerCommand(currentRoach, csvPort)
		bgLogger.Printf("running CSV server in the background: %q", cmd)
		var csvServerCtx context.Context
		csvServerCtx, stopCSVServer = context.WithCancel(bgCtx)
		err := mvr.cluster.RunE(csvServerCtx, mvr.roachNodes, cmd)

		if err == nil || errors.Is(err, context.Canceled) {
			bgLogger.Printf("CSV server terminated")
			return nil
		}

		return fmt.Errorf("error while running csv server: %w", err)
	})

	l.Printf("start wait for port", importNode)
	if err := waitForPort(ctx, mvr.roachNodes, csvPort, mvr.cluster); err != nil {
		return err
	}
	l.Printf("done wait for port", importNode)

	err := mvr.cluster.RunE(
		ctx,
		mvr.cluster.Node(importNode),
		importBankCommand(currentRoach, rows, 0 /* ranges */, csvPort, importNode),
	)
	if err != nil {
		return err
	}
	stopCSVServer()
	l.Printf("done running import on node %d", importNode)

	destNode := h.RandomNode(rng, mvr.roachNodes)
	uri := fmt.Sprintf("nodelocal://%d/%s", destNode, "test_backup")
	mvr.collectionURI = uri
	stmt := fmt.Sprintf("BACKUP TABLE %s.%s INTO '%s'", mvr.database, mvr.table, uri)

	l.Printf("running %s", stmt)

	db := h.Connect(importNode)
	_, err = db.ExecContext(ctx, stmt)
	if err != nil {
		return err
	}

	return err
}

func (mvr *mixedVersionRestore) nextID() int64 {
	return atomic.AddInt64(&mvr.currentBackupID, 1)
}

// backupName returns a descriptive name for a backup depending on the
// state of the test we are in. The given label is also used to
// provide more context. Example: '3_22.2.4-to-current_final'
func (mvr *mixedVersionRestore) restoreDatabaseName(id int64, h *mixedversion.Helper, label string) string {
	testContext := h.Context()
	var finalizing string
	if testContext.Finalizing {
		finalizing = "_finalizing"
	}

	fromVersion := sanitizeVersionForBackup(testContext.FromVersion)
	toVersion := sanitizeVersionForBackup(testContext.ToVersion)
	sanitizedLabel := strings.ReplaceAll(label, " ", "_")
	sanitizedLabel = strings.ReplaceAll(sanitizedLabel, ".", "_")

	return fmt.Sprintf("%d_%s-to-%s_%s%s", id, fromVersion, toVersion, sanitizedLabel, finalizing)
}

// waitForJobSuccess waits for the given job with the given ID to
// succeed (according to `backupCompletionRetryOptions`). Returns an
// error if the job doesn't succeed within the attempted retries.
func (mvr *mixedVersionRestore) waitForJobSuccess(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper, jobID int,
) error {
	var lastErr error
	node, db := h.RandomDB(rng, mvr.roachNodes)
	l.Printf("querying job status through node %d", node)
	for r := retry.StartWithCtx(ctx, restoreCompletionRetryOptions); r.Next(); {
		var status string
		var payloadBytes []byte
		err := db.QueryRow(`SELECT status, payload FROM system.jobs WHERE id = $1`, jobID).Scan(&status, &payloadBytes)
		if err != nil {
			lastErr = fmt.Errorf("error reading (status, payload) for job %d: %w", jobID, err)
			l.Printf("%v", lastErr)
			continue
		}

		if jobs.Status(status) == jobs.StatusFailed {
			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
				lastErr = fmt.Errorf("job %d failed with error: %s", jobID, payload.Error)
			} else {
				lastErr = fmt.Errorf("job %d failed, and could not unmarshal payload: %w", jobID, err)
			}

			l.Printf("%v", lastErr)
			break
		}

		if expected, actual := jobs.StatusSucceeded, jobs.Status(status); expected != actual {
			lastErr = fmt.Errorf("job %d: current status %q, waiting for %q", jobID, actual, expected)
			l.Printf("%v", lastErr)
			continue
		}

		l.Printf("job %d: success", jobID)
		return nil
	}

	return fmt.Errorf("waiting for job to finish: %w", lastErr)
}

// computeFingerprint returns the fingerprint for a given table; if a
// non-empty `timestamp` is passed, the fingerprints is calculated as
// of that timestamp.
func (mvr *mixedVersionRestore) computeFingerprint(
	database, table string, rng *rand.Rand, h *mixedversion.Helper,
) (string, error) {
	var fprint string
	query := fmt.Sprintf("SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s.%s]", database, table)
	if err := h.QueryRow(rng, query).Scan(&fprint); err != nil {
		return "", err
	}

	return fprint, nil
}

// runBackup runs a `BACKUP` statement; the backup type `bType` needs
// to be an instance of either `fullBackup` or
// `incrementalBackup`. Returns when the backup job has completed.
func (mvr *mixedVersionRestore) runRestore(
	ctx context.Context,
	l *logger.Logger,
	label string,
	rng *rand.Rand,
	nodes option.NodeListOption,
	h *mixedversion.Helper,
) (restoredDatabase, error) {
	// NB: we need to run with the `detached` option + poll the
	// `system.jobs` table because we are intentionally disabling job
	// adoption in some nodes in the mixed-version test. Running the job
	// without the `detached` option will cause a `node liveness` error
	// to be returned when running the `RESTORE` statement.
	var restoreDB restoredDatabase
	name := mvr.restoreDatabaseName(mvr.nextID(), h, label)

	restoreDB = restoredDatabase{
		database: name,
	}
	options := []string{"detached", fmt.Sprintf("into_db='%s'", restoreDB.database)}

	node, db := h.RandomDB(rng, nodes)
	_, err := db.ExecContext(ctx, "CREATE DATABASE $1", name)
	if err != nil {
		return restoredDatabase{}, errors.Wrapf(err, "creating database %s", name)
	}

	stmt := fmt.Sprintf(
		"RESTORE TABLE %s FROM latest IN '%s' WITH %s",
		mvr.table, mvr.collectionURI, strings.Join(options, ", "),
	)
	l.Printf("restoring via node %d: %s", node, stmt)
	var jobID int
	if err := db.QueryRowContext(ctx, stmt).Scan(&jobID); err != nil {
		return restoredDatabase{}, errors.Wrapf(err, "error while restoring into %s.%s", restoreDB.database, mvr.table, err)
	}

	l.Printf("waiting for job %d (%s)", jobID, restoreDB.database)
	if err := mvr.waitForJobSuccess(ctx, l, rng, h, jobID); err != nil {
		return restoredDatabase{}, err
	}

	return restoreDB, nil
}

// runJobOnOneOf disables job adoption on cockroach nodes that are not
// in the `nodes` list. The function passed is then executed and job
// adoption is re-enabled at the end of the function. The function
// passed is expected to run statements that trigger job creation.
func (mvr *mixedVersionRestore) runJobOnOneOf(
	ctx context.Context,
	l *logger.Logger,
	nodes option.NodeListOption,
	h *mixedversion.Helper,
	fn func() error,
) error {
	sort.Ints(nodes)
	var disabledNodes option.NodeListOption
	for _, node := range mvr.roachNodes {
		idx := sort.SearchInts(nodes, node)
		if idx == len(nodes) || nodes[idx] != node {
			disabledNodes = append(disabledNodes, node)
		}
	}

	if err := mvr.disableJobAdoption(ctx, l, disabledNodes, h); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	return mvr.enableJobAdoption(ctx, l, disabledNodes)
}

func (mvr *mixedVersionRestore) restoreTable(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	spec restoreSpec,
	h *mixedversion.Helper,
) error {
	var restoreDB restoredDatabase
	if err := mvr.runJobOnOneOf(ctx, l, spec.Execute.Nodes, h, func() error {
		var err error
		label := restoreDesc(spec)
		restoreDB, err = mvr.runRestore(ctx, l, label, rng, spec.Plan.Nodes, h)
		return err
	}); err != nil {
		return err
	}

	mvr.restoreDBs = append(mvr.restoreDBs, &restoreDB)
	return nil
}

// sentinelFilePath returns the path to the file that prevents job
// adoption on the given node.
func (mvb *mixedVersionRestore) sentinelFilePath(
	ctx context.Context, l *logger.Logger, node int,
) (string, error) {
	result, err := mvb.cluster.RunWithDetailsSingleNode(
		ctx, l, mvb.cluster.Node(node), "echo -n {store-dir}",
	)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve store directory from node %d: %w", node, err)
	}
	return filepath.Join(result.Stdout, jobs.PreventAdoptionFile), nil
}

// disableJobAdoption disables job adoption on the given nodes by
// creating an empty file in `jobs.PreventAdoptionFile`. The function
// returns once any currently running jobs on the nodes terminate.
func (mvb *mixedVersionRestore) disableJobAdoption(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, h *mixedversion.Helper,
) error {
	l.Printf("disabling job adoption on nodes %v", nodes)
	eg, _ := errgroup.WithContext(ctx)
	for _, node := range nodes {
		node := node // capture range variable
		eg.Go(func() error {
			l.Printf("node %d: disabling job adoption", node)
			sentinelFilePath, err := mvb.sentinelFilePath(ctx, l, node)
			if err != nil {
				return err
			}
			if err := mvb.cluster.RunE(ctx, mvb.cluster.Node(node), "touch", sentinelFilePath); err != nil {
				return fmt.Errorf("node %d: failed to touch sentinel file %q: %w", node, sentinelFilePath, err)
			}

			// Wait for no jobs to be running on the node that we have halted
			// adoption on.
			l.Printf("node %d: waiting for all running jobs to terminate", node)
			if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
				db := h.Connect(node)
				var count int
				err := db.QueryRow(`SELECT count(*) FROM [SHOW JOBS] WHERE status = 'running'`).Scan(&count)
				if err != nil {
					l.Printf("node %d: error querying running jobs (%s)", node, err)
					return err
				}

				if count != 0 {
					l.Printf("node %d: %d running jobs...", node, count)
					return fmt.Errorf("node %d is still running %d jobs", node, count)
				}
				return nil
			}); err != nil {
				return err
			}

			l.Printf("node %d: job adoption disabled", node)
			return nil
		})
	}

	return eg.Wait()
}

// enableJobAdoption (re-)enables job adoption on the given nodes.
func (mvb *mixedVersionRestore) enableJobAdoption(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) error {
	l.Printf("enabling job adoption on nodes %v", nodes)
	eg, _ := errgroup.WithContext(ctx)
	for _, node := range nodes {
		node := node // capture range variable
		eg.Go(func() error {
			l.Printf("node %d: enabling job adoption", node)
			sentinelFilePath, err := mvb.sentinelFilePath(ctx, l, node)
			if err != nil {
				return err
			}

			if err := mvb.cluster.RunE(ctx, mvb.cluster.Node(node), "rm -f", sentinelFilePath); err != nil {
				return fmt.Errorf("node %d: failed to remove sentinel file %q: %w", node, sentinelFilePath, err)
			}

			l.Printf("node %d: job adoption enabled", node)
			return nil
		})
	}

	return eg.Wait()
}

// planAndRunBackups is the function that can be passed to the
// mixed-version test's `InMixedVersion` function. If the cluster is
// in mixed-binary state, four backup collections are created (see
// four cases in the function body). If all nodes are running the next
// version (which is different from the cluster version), then a
// backup collection is created in an arbitrary node.
func (mvr *mixedVersionRestore) planAndRunRestores(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	tc := h.Context() // test context
	l.Printf("current context: %#v", tc)

	onPrevious := labeledNodes{
		Nodes: tc.FromVersionNodes, Version: sanitizeVersionForBackup(tc.FromVersion),
	}
	onNext := labeledNodes{
		Nodes: tc.ToVersionNodes, Version: sanitizeVersionForBackup(tc.ToVersion),
	}

	if len(tc.FromVersionNodes) > 0 {
		// Case 1: plan backups    -> previous node
		//         execute backups -> next node
		spec := restoreSpec{Plan: onPrevious, Execute: onNext}
		l.Printf("planning restore: %s", restoreDesc(spec))
		if err := mvr.restoreTable(ctx, l, rng, spec, h); err != nil {
			return err
		}

		// Case 2: plan backups   -> next node
		//         execute backups -> previous node
		spec = restoreSpec{Plan: onNext, Execute: onPrevious}
		l.Printf("planning restore: %s", restoreDesc(spec))
		if err := mvr.restoreTable(ctx, l, rng, spec, h); err != nil {
			return err
		}

		// Case 3: plan & execute full backup        -> previous node
		//         plan & execute incremental backup -> next node
		spec = restoreSpec{Plan: onPrevious, Execute: onPrevious}
		l.Printf("planning restore: %s", restoreDesc(spec))
		if err := mvr.restoreTable(ctx, l, rng, spec, h); err != nil {
			return err
		}

		// Case 4: plan & execute full backup        -> next node
		//         plan & execute incremental backup -> previous node
		spec = restoreSpec{Plan: onNext, Execute: onNext}
		l.Printf("planning restore: %s", restoreDesc(spec))
		if err := mvr.restoreTable(ctx, l, rng, spec, h); err != nil {
			return err
		}
		return nil
	}

	l.Printf("all nodes running next version, running restore on arbitrary node")
	spec := restoreSpec{Plan: onNext, Execute: onNext}
	return mvr.restoreTable(ctx, l, rng, spec, h)
}

// verifyBackups cycles through all the backup collections created for
// the duration of the test, and verifies that restoring the backups
// in a new database results in the same data as when the backup was
// taken. This is accomplished by comparing the fingerprints after
// restoring with the expected fingerpring associated with the backup
// collection.
func (mvb *mixedVersionRestore) verifyBackups(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	l.Printf("verifying %d restored tables from this test", len(mvb.restoreDBs))
	originalFingerprint, err := mvb.computeFingerprint(mvb.database, mvb.table, rng, h)
	if err != nil {
		return errors.Wrapf(err, "backup %s: error computing fingerprint for %s.%s", mvb.database, mvb.table)
	}

	for _, restoredDB := range mvb.restoreDBs {
		l.Printf("verifying %s.%s", restoredDB.database, mvb.table)

		restoredFingerprint, err := mvb.computeFingerprint(restoredDB.database, mvb.table, rng, h)
		if err != nil {
			return errors.Wrapf(err, "backup %s: error computing fingerprint for %s.%s", restoredDB.database, mvb.table)
		}

		if restoredFingerprint != originalFingerprint {
			return fmt.Errorf(
				"restore %s.%s: mismatched fingerprints (expected=%s | actual=%s)",
				restoredDB.database, mvb.table, originalFingerprint, restoredFingerprint,
			)
		}

		l.Printf("%s: OK", restoredDB.database)
	}

	return nil
}

func registerRestoreMixedVersion(r registry.Registry) {
	// backup/mixed-version tests different states of backup in a mixed
	// version cluster. The actual state of the cluster when a backup is
	// executed is randomized, so each run of the test will exercise a
	// different set of events. Reusing the same seed will produce the
	// same test.
	r.Add(registry.TestSpec{
		Name:              "restore/mixed-version",
		Owner:             registry.OwnerDisasterRecovery,
		Cluster:           r.MakeClusterSpec(5),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Timeout:           4 * time.Hour,
		RequiresLicense:   true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.IsLocal() && runtime.GOARCH == "arm64" {
				t.Skip("Skip under ARM64. See https://github.com/cockroachdb/cockroach/issues/89268")
			}

			roachNodes := c.Range(1, c.Spec().NodeCount-1)
			workloadNode := c.Node(c.Spec().NodeCount)

			uploadVersion(ctx, t, c, workloadNode, clusterupgrade.MainVersion)
			bankRun := roachtestutil.NewCommand("./cockroach workload run bank").
				Arg("{pgurl%s}", roachNodes).
				Option("tolerate-errors")

			mvt := mixedversion.NewTest(ctx, t, t.L(), c, roachNodes)
			mvb := newMixedVersionRestore(c, roachNodes, "bank", "bank")
			mvt.OnStartup("set short job interval", mvb.setShortJobIntervals)
			mvt.OnStartup("load backup data", mvb.createBackupToRestore)
			mvt.Workload("bank", workloadNode, nil /* initCmd */, bankRun)

			mvt.InMixedVersion("plan and run backups", mvb.planAndRunRestores)
			mvt.AfterUpgradeFinalized("verify backups", mvb.verifyBackups)

			mvt.Run()
		},
	})
}
