// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
)

var (
	// clusterSettingValues is a mapping from cluster setting names to
	// possible values they can assume in this test. The system settings
	// listed here are chosen based on availability of documentation
	// (see below), meaning a customer could have reasonably set them;
	// and relationship to backup/restore. The list of possible values
	// are generally 50-100% smaller or larger than the documented
	// default.
	//
	// Documentation:
	// https://www.cockroachlabs.com/docs/stable/cluster-settings.html
	clusterSettingValues = map[string]metamorphicSetting{
		"bulkio.backup.file_size":                        tenantSetting("8MiB", "32MiB", "512MiB", "750MiB"),
		"bulkio.backup.read_timeout":                     tenantSetting("2m0s", "10m0s"),
		"bulkio.backup.read_with_priority_after":         tenantSetting("20s", "5m0s"),
		"bulkio.stream_ingestion.minimum_flush_interval": systemSetting("1s", "10s", "30s"),
		"kv.bulk_io_write.max_rate":                      systemSetting("250MiB", "500MiB", "2TiB"),
		"kv.bulk_sst.max_allowed_overage":                systemSetting("16MiB", "256MiB"),
		"kv.bulk_sst.target_size":                        systemSetting("4MiB", "64MiB", "128MiB"),
		// The default is currently 384 MB, which was set to be about 75% of a
		// range's worth of data. This configuration will reduce the size of this
		// setting to test restore_span_covering correctness, at the cost of a
		// performance dip.
		//
		// Note that a size of 0 indicates that target_size will not be used while
		// constructing restore span entries.
		"backup.restore_span.target_size": tenantSetting("0 B", "4 MiB", "32 MiB", "128 MiB"),
	}

	clusterSettingNames = func() []string {
		names := make([]string, 0, len(clusterSettingValues))
		for name := range clusterSettingValues {
			names = append(names, name)
		}

		// Sort the settings names so that picking a random setting is
		// deterministic, given the same random seed.
		sort.Strings(names)
		return names
	}()

	fewBankRows      = 100
	bankPossibleRows = []int{
		fewBankRows, // creates keys with long revision history (not valid with largeBankPayload)
		1_000,       // small backup
		10_000,      // larger backups (a few GiB when using 128 KiB payloads)
	}

	bankPossiblePayloadBytes = []int{
		0,        // workload default
		9,        // 1 random byte (`initial-` + 1)
		500,      // 5x default at the time of writing
		16 << 10, // 16 KiB
	}

	schemaChangeDB = "schemachange"

	v231CV = "23.1"
)

type CommonTestUtils struct {
	t                 test.Test
	cluster           cluster.Cluster
	roachNodes        option.NodeListOption
	mock              bool
	onlineRestore     bool
	compactionEnabled bool

	connCache struct {
		mu    syncutil.Mutex
		cache []*gosql.DB
	}
}

type commonTestOption func(*CommonTestUtils)

func withMock(mock bool) commonTestOption {
	return func(c *CommonTestUtils) {
		c.mock = mock
	}
}

func withOnlineRestore(or bool) commonTestOption {
	return func(c *CommonTestUtils) {
		c.onlineRestore = or
	}
}

func withCompaction(c bool) commonTestOption {
	return func(cu *CommonTestUtils) {
		cu.compactionEnabled = c
	}
}

// Change the function signature
func newCommonTestUtils(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	connectFunc func(int) (*gosql.DB, error),
	nodes option.NodeListOption,
	opts ...commonTestOption,
) (*CommonTestUtils, error) {
	cc := make([]*gosql.DB, len(nodes))
	for _, node := range nodes {
		conn, err := connectFunc(node)
		if err != nil {
			return nil, err
		}
		cc[node-1] = conn
	}

	if len(cc) == 0 {
		return nil, errors.New("cannot test using 0 nodes")
	}

	u := &CommonTestUtils{
		t:          t,
		cluster:    c,
		roachNodes: nodes,
	}
	u.connCache.cache = cc

	// Apply all options
	for _, opt := range opts {
		opt(u)
	}
	return u, nil
}

// setShortJobIntervals increases the frequency of the adopt and
// cancel loops in the job registry. This enables changes to job state
// to be observed faster, and the test to run quicker.
func (u *CommonTestUtils) setShortJobIntervals(ctx context.Context, rng *rand.Rand) error {
	return setShortJobIntervalsCommon(func(query string, args ...interface{}) error {
		return u.Exec(ctx, rng, query, args...)
	})
}

// systemTableWriter will run random statements that lead to data
// being written to system.* tables. The frequency of these writes
// (and the data written) are randomized. This function is expected to
// be run in the background throughout the entire test duration: any
// errors found while writing data will be logged but will not be
// considered fatal (after all, nodes are restarted multiple times
// during this test).
//
// The goal is to exercise the backup/restore functionality of some
// system tables that are typically empty in most tests.
//
// TODO(renato): this should be a `workload`.
func (u *CommonTestUtils) systemTableWriter(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, dbs []string, tables [][]string,
) error {
	type systemOperation func() error
	// addComment will run a `COMMENT ON (DATABASE|TABLE)` statement. It
	// may also randomly set the comment to NULL (which is equivalent ot
	// removing an existing comment, if any).
	addComment := func() error {
		const nullProbability = 0.2
		object, name := newCommentTarget(rng, dbs, tables)

		removeComment := rng.Float64() < nullProbability
		var prefix, commentContents string
		if removeComment {
			prefix = fmt.Sprintf("removing comment from %s", object)
			commentContents = "NULL"
		} else {
			prefix = fmt.Sprintf("adding comment to %s", object)
			commentLen := randIntBetween(rng, 64, 1024)
			commentContents = fmt.Sprintf("'%s'", randString(rng, commentLen))
		}

		l.Printf("%s: %s", prefix, name)
		return u.Exec(ctx, rng, fmt.Sprintf("COMMENT ON %s %s IS %s", strings.ToUpper(object), name, commentContents))
	}

	// addExternalConnection runs a `CREATE EXTERNAL CONNECTION`
	// statement, creating a named external connection to a nodelocal
	// location.
	addExternalConnection := func() error {
		node := u.RandomNode(rng, u.roachNodes)
		l.Printf("adding external connection to node %d", node)
		nodeLocal := fmt.Sprintf("nodelocal://%d/%s", node, randString(rng, 16))
		name := randString(rng, 8)
		return u.Exec(ctx, rng, fmt.Sprintf("CREATE EXTERNAL CONNECTION %q AS '%s'", name, nodeLocal))
	}

	// addRoleOrUser creates a new user or a role. The logic for both is
	// the same: both users and roles may have a password associated
	// with them, password expiration, associated roles, etc.
	addRoleOrUser := func() error {
		const roleProbability = 0.5
		isRole := rng.Float64() < roleProbability
		entity := "USER"
		if isRole {
			entity = "ROLE"
		}

		name := randString(rng, 6)
		l.Printf("creating %s %s", strings.ToLower(entity), name)

		const nullPasswordProbability = 0.2
		const expirationProbability = 0.5
		numRoles := rng.Intn(3) // up to 3 roles
		var options []string

		if rng.Float64() < nullPasswordProbability {
			options = append(options, "PASSWORD NULL")
		} else {
			password := randString(rng, randIntBetween(rng, 4, 32))
			options = append(options, fmt.Sprintf("LOGIN PASSWORD '%s'", password))
		}

		if rng.Float64() < expirationProbability {
			possibleExpirations := []time.Duration{
				10 * time.Second, 1 * time.Minute, 30 * time.Minute,
			}
			dur := possibleExpirations[rng.Intn(len(possibleExpirations))]
			expiresAt := timeutil.Now().Add(dur)
			options = append(options, fmt.Sprintf("VALID UNTIL '%s'", expiresAt.Format(time.RFC3339)))
		}

		possibleRoles := []string{
			"CANCELQUERY", "CONTROLCHANGEFEED", "CONTROLJOB", "CREATEDB", "CREATELOGIN",
			"CREATEROLE", "MODIFYCLUSTERSETTING",
		}
		rng.Shuffle(len(possibleRoles), func(i, j int) {
			possibleRoles[i], possibleRoles[j] = possibleRoles[j], possibleRoles[i]
		})
		options = append(options, possibleRoles[:numRoles]...)
		return u.Exec(ctx, rng, fmt.Sprintf("CREATE %s %q WITH %s", entity, name, strings.Join(options, " ")))
	}

	possibleOps := []systemOperation{
		addComment, addExternalConnection, addRoleOrUser,
	}
	for {
		nextDur := randWaitDuration(rng)
		l.Printf("will attempt a random insert in %s", nextDur)

		select {
		case <-time.After(nextDur):
			op := possibleOps[rng.Intn(len(possibleOps))]
			if err := op(); err != nil {
				l.Printf("error running operation: %v", err)
			}
		case <-ctx.Done():
			l.Printf("context is canceled, finishing")
			return ctx.Err()
		}
	}
}

// loadTables returns a list of tables that are part of the database
// with the given name.
func (u *CommonTestUtils) loadTablesForDBs(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, dbs ...string,
) ([][]string, error) {
	allTables := make([][]string, len(dbs))
	eg := u.t.NewErrorGroup(task.WithContext(ctx), task.Logger(l))
	for j, dbName := range dbs {
		eg.Go(func(ctx context.Context, l *logger.Logger) error {
			node, db := u.RandomDB(rng, u.roachNodes)
			l.Printf("loading table information for DB %q via node %d", dbName, node)
			query := fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s]", dbName)
			rows, err := db.QueryContext(ctx, query)
			if err != nil {
				return fmt.Errorf("failed to read tables for database %s: %w", dbName, err)
			}
			defer rows.Close()

			var tables []string
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					return fmt.Errorf("error scanning table_name for db %s: %w", dbName, err)
				}
				tables = append(tables, name)
			}

			if err := rows.Err(); err != nil {
				return fmt.Errorf("error iterating over table_name rows for database %s: %w", dbName, err)
			}

			allTables[j] = tables
			l.Printf("database %q has %d tables", dbName, len(tables))
			return nil
		})
	}

	if err := eg.WaitE(); err != nil {
		return nil, err
	}

	return allTables, nil
}

// setMaxRangeSizeAndDependentSettings chooses a random default range size from
// maxRangeSize bytes and scales the cluster settings in
// clusterSettingsScaledOnRangeSize such that rangeSize/settingValue remains the
// same.
func (u *CommonTestUtils) setMaxRangeSizeAndDependentSettings(
	ctx context.Context, t test.Test, rng *rand.Rand, dbs []string,
) error {
	const defaultRangeMinBytes = 1024
	const defaultRangeSize int64 = 512 << 20

	rangeSize := maxRangeSizeBytes[rng.Intn(len(maxRangeSizeBytes))]
	t.L().Printf("Set max range rangeSize to %s", humanizeutil.IBytes(rangeSize))

	scale := func(current int64) int64 {
		currentF := float64(current)
		ratio := float64(rangeSize) / float64(defaultRangeSize)
		return int64(currentF * ratio)
	}
	for _, dbName := range dbs {
		query := fmt.Sprintf("ALTER DATABASE %s CONFIGURE ZONE USING range_max_bytes=%d, range_min_bytes=%d",
			dbName, rangeSize, defaultRangeMinBytes)
		if err := u.Exec(ctx, rng, query); err != nil {
			return err
		}
	}

	for _, setting := range clusterSettingsScaledOnRangeSize {
		var humanizedCurrentValue string
		if err := u.QueryRow(ctx, rng, fmt.Sprintf("SHOW CLUSTER SETTING %s", setting)).Scan(&humanizedCurrentValue); err != nil {
			return err
		}
		currentValue, err := humanizeutil.ParseBytes(humanizedCurrentValue)
		if err != nil {
			return err
		}
		newValue := scale(currentValue)
		t.L().Printf("changing cluster setting %s from %s to %s", setting, humanizedCurrentValue, humanizeutil.IBytes(newValue))
		stmt := fmt.Sprintf("SET CLUSTER SETTING %s = '%d'", setting, newValue)
		if err := u.Exec(ctx, rng, stmt); err != nil {
			return err
		}
	}
	// Ensure ranges have been properly replicated.
	_, dbConn := u.RandomDB(rng, u.roachNodes)
	return roachtestutil.WaitFor3XReplication(ctx, t.L(), dbConn)
}

// setClusterSettings may set up to numCustomSettings cluster settings
// as defined in `clusterSettingValues`. The system settings changed
// are logged. This function should be called *before* the upgrade
// begins; the cockroach documentation says explicitly that changing
// cluster settings is not supported in mixed-version, so we don't
// test that scenario.
func (u *CommonTestUtils) setClusterSettings(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, rng *rand.Rand,
) error {
	const numCustomSettings = 3
	const defaultSettingsProbability = 0.2

	if rng.Float64() < defaultSettingsProbability {
		l.Printf("not setting any custom cluster settings (using defaults)")
		return nil
	}

	// Make sure to use a connection to the system tenant as it will be
	// necessary when changing system-only cluster settings.
	systemDB, err := c.ConnE(ctx, l, 1, option.VirtualClusterName(install.SystemInterfaceName))
	if err != nil {
		return errors.Wrap(err, "failed to connect to system tenant")
	}
	defer systemDB.Close()

	appDB := u.Connect(1) // no need to Close() this as it's a cached connection

	for j := 0; j < numCustomSettings; j++ {
		settingName := clusterSettingNames[rng.Intn(len(clusterSettingNames))]
		setting := clusterSettingValues[settingName]
		value := setting.Values[rng.Intn(len(setting.Values))]

		l.Printf("setting %s cluster setting %q to %q", setting.Scope, settingName, value)
		stmt := fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", settingName, value)

		var db *gosql.DB
		if setting.IsSystemOnly() {
			db = systemDB
		} else {
			db = appDB
		}

		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}

	return nil
}

// waitForJobSuccess waits for the given job with the given ID to
// succeed (according to `backupCompletionRetryOptions`). Returns an
// error if the job doesn't succeed within the attempted retries.
func (u *CommonTestUtils) waitForJobSuccess(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, jobID int, internalSystemJobs bool,
) error {
	return u.waitForJobSuccessWithNode(
		ctx, l, u.RandomNode(rng, u.roachNodes), jobID, internalSystemJobs,
	)
}

// waitForJobSuccessWithNode waits for the given job with the given ID to
// succeed (according to `backupCompletionRetryOptions`) via the provided node.
// Returns an error if the job doesn't succeed within the attempted retries.
func (u *CommonTestUtils) waitForJobSuccessWithNode(
	ctx context.Context, l *logger.Logger, node int, jobID int, internalSystemJobs bool,
) (resErr error) {
	l.Printf("querying job status through node %d", node)

	db, err := u.cluster.ConnE(ctx, l, node, option.DBName("system"))
	if err != nil {
		l.Printf("error connecting to node %d: %v", node, err)
		return err
	}
	defer func() {
		err := db.Close()
		resErr = errors.CombineErrors(resErr, err)
	}()

	jobsQuery := "system.jobs WHERE id = $1"
	if internalSystemJobs {
		jobsQuery = fmt.Sprintf("(%s)", jobutils.InternalSystemJobsBaseQuery)
	}

	var lastStatus jobs.State
	logThrottler := util.EveryMono(30 * time.Second)
	r := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: time.Second,
		MaxBackoff:     time.Second,
		MaxDuration:    80 * time.Minute,
	})
	for r.Next() {
		var status string
		var payloadBytes []byte
		err := db.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT status, payload FROM %s`, jobsQuery), jobID,
		).Scan(&status, &payloadBytes)
		if err != nil {
			l.Printf("error reading (status, payload) for job %d: %v", jobID, err)
			continue
		}

		if jobs.State(status) == jobs.StateFailed {
			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
				return fmt.Errorf("job %d failed with error: %s", jobID, payload.Error)
			} else {
				return fmt.Errorf("job %d failed, and could not unmarshal payload: %w", jobID, err)
			}
		}

		if expected, actual := jobs.StateSucceeded, jobs.State(status); expected != actual {
			// Log current status if there has been a change or if it has been long
			// enough since the last update.
			if logThrottler.ShouldProcess(crtime.NowMono()) || lastStatus != actual {
				l.Printf("job %d: current status %q, waiting for %q", jobID, actual, expected)
			}
			lastStatus = actual
			continue
		}

		l.Printf("job %d: success", jobID)
		return nil
	}

	if err := ctx.Err(); err != nil {
		return errors.Wrapf(err, "error waiting for job %d to finish", jobID)
	}

	return errors.Newf("retries exhausted waiting for job %d to finish", jobID)
}

// waitForJobFractionCompletedWithNode waits for the given job to reach at the
// least the provided progress fraction, querying through the provided node.
func (u *CommonTestUtils) waitForJobFractionCompletedWithNode(
	ctx context.Context, l *logger.Logger, node int, jobID int, fraction float64,
) (resErr error) {
	l.Printf("querying job progress through node %d", node)

	db, err := u.cluster.ConnE(ctx, l, node, option.DBName("system"))
	if err != nil {
		l.Printf("error connecting to node %d: %v", node, err)
		return err
	}
	defer func() {
		err := db.Close()
		resErr = errors.CombineErrors(resErr, err)
	}()

	logThrottler := util.EveryMono(30 * time.Second)
	const progQuery = `SELECT coordinator_id, fraction_completed, status FROM crdb_internal.jobs WHERE job_id = $1`
	var claimInstanceID gosql.NullInt64
	var fractionCompleted gosql.NullFloat64
	var status string
	for r := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: time.Second,
		MaxBackoff:     time.Second,
		MaxDuration:    60 * time.Minute,
	}); r.Next(); {
		unclaimed := !claimInstanceID.Valid
		err := db.QueryRowContext(ctx, progQuery, jobID).Scan(
			&claimInstanceID, &fractionCompleted, &status,
		)
		if err != nil {
			l.Printf("error querying progress for job %d: %v", jobID, err)
			continue
		}
		if unclaimed && claimInstanceID.Valid {
			l.Printf("job %d claimed by instance %d", jobID, claimInstanceID.Int64)
		}

		if fractionCompleted.Valid {
			if fractionCompleted.Float64 >= fraction {
				l.Printf(
					"job %d passed %.2f%% progress (current progress: %.2f%%)",
					jobID, fraction*100, fractionCompleted.Float64*100,
				)
				return nil
			}
			if logThrottler.ShouldProcess(crtime.NowMono()) {
				l.Printf(
					"waiting for job %d to pass %.2f%% (current progress: %.2f%%)",
					jobID, fraction*100, fractionCompleted.Float64*100,
				)
			}
		}

		jobStatus := jobs.State(status)
		if jobStatus != jobs.StateSucceeded && jobStatus != jobs.StateRunning {
			return errors.Newf("job %d unexpectedly in state %s while tracking progress", jobID, jobStatus)
		}
	}

	if err := ctx.Err(); err != nil {
		return errors.Wrapf(
			err, "error waiting for job %d to pass %.2f%% progress", jobID, fraction*100,
		)
	}

	return errors.Newf(
		"retries exhausted waiting for job %d to pass %.2f%% progress", jobID, fraction*100,
	)
}

// runJobOnOneOf disables job adoption on cockroach nodes that are not
// in the `nodes` list. The function passed is then executed and job
// adoption is re-enabled at the end of the function. The function
// passed is expected to run statements that trigger job creation.
func (u *CommonTestUtils) runJobOnOneOf(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, fn func() error,
) error {
	sort.Ints(nodes)
	var disabledNodes option.NodeListOption
	for _, node := range u.roachNodes {
		idx := sort.SearchInts(nodes, node)
		if idx == len(nodes) || nodes[idx] != node {
			disabledNodes = append(disabledNodes, node)
		}
	}

	if err := u.disableJobAdoption(ctx, l, disabledNodes); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	return u.enableJobAdoption(ctx, l, disabledNodes)
}

// sentinelFilePath returns the path to the file that prevents job
// adoption on the given node.
func (u *CommonTestUtils) sentinelFilePath(
	ctx context.Context, l *logger.Logger, node int,
) (string, error) {
	result, err := u.cluster.RunWithDetailsSingleNode(
		ctx, l, option.WithNodes(u.cluster.Node(node)), "echo -n {store-dir}",
	)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve store directory from node %d: %w", node, err)
	}
	return filepath.Join(result.Stdout, jobs.PreventAdoptionFile), nil
}

// disableJobAdoption disables job adoption on the given nodes by
// creating an empty file in `jobs.PreventAdoptionFile`. The function
// returns once any currently running jobs on the nodes terminate.
func (u *CommonTestUtils) disableJobAdoption(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) error {
	l.Printf("disabling job adoption on nodes %v", nodes)
	eg := u.t.NewErrorGroup(task.WithContext(ctx), task.Logger(l))
	for _, node := range nodes {
		eg.Go(func(ctx context.Context, l *logger.Logger) error {
			l.Printf("node %d: disabling job adoption", node)
			sentinelFilePath, err := u.sentinelFilePath(ctx, l, node)
			if err != nil {
				return err
			}
			if err := u.cluster.RunE(ctx, option.WithNodes(u.cluster.Node(node)), "touch", sentinelFilePath); err != nil {
				return fmt.Errorf("node %d: failed to touch sentinel file %q: %w", node, sentinelFilePath, err)
			}

			// Wait for no jobs to be running on the node that we have halted
			// adoption on.
			l.Printf("node %d: waiting for all running jobs to terminate", node)
			if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
				db := u.Connect(node)
				var count int
				err := db.QueryRow(fmt.Sprintf(
					`SELECT count(*) FROM [SHOW JOBS] WHERE status = 'running' AND coordinator_id = %d`,
					node,
				)).Scan(&count)
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
				l.Printf("giving up (probably a mixed-version restore running concurently)")
			}

			l.Printf("node %d: job adoption disabled", node)
			return nil
		})
	}

	return eg.WaitE()
}

// enableJobAdoption (re-)enables job adoption on the given nodes.
func (u *CommonTestUtils) enableJobAdoption(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) error {
	l.Printf("enabling job adoption on nodes %v", nodes)
	eg := u.t.NewErrorGroup(task.WithContext(ctx), task.Logger(l))
	for _, node := range nodes {
		eg.Go(func(ctx context.Context, l *logger.Logger) error {
			l.Printf("node %d: enabling job adoption", node)
			sentinelFilePath, err := u.sentinelFilePath(ctx, l, node)
			if err != nil {
				return err
			}

			if err := u.cluster.RunE(ctx, option.WithNodes(u.cluster.Node(node)), "rm -f", sentinelFilePath); err != nil {
				return fmt.Errorf("node %d: failed to remove sentinel file %q: %w", node, sentinelFilePath, err)
			}

			l.Printf("node %d: job adoption enabled", node)
			return nil
		})
	}

	return eg.WaitE()
}

// checkFiles uses the `check_files` option of `SHOW BACKUP` to verify
// that the latest backup in the collection passed is valid. This step
// is skipped if the feature is not available.
func (u *CommonTestUtils) checkFiles(
	ctx context.Context, rng *rand.Rand, collection *backupCollection,
) error {
	options := []string{"check_files"}
	if opt := collection.encryptionOption(); opt != nil {
		options = append(options, opt.String())
	}

	checkFilesStmt := fmt.Sprintf(
		"SHOW BACKUP LATEST IN '%s' WITH %s",
		collection.uri(), strings.Join(options, ", "),
	)
	return u.Exec(ctx, rng, checkFilesStmt)
}

func supportsCheckFiles(rng *rand.Rand, h *mixedversion.Helper) (bool, error) {
	return h.ClusterVersionAtLeast(rng, v231CV)
}

// collectFailureArtifacts fetches cockroach logs and a debug.zip and
// saves them to a directory in the test's artifacts dir. This is done
// so that we can report multiple restore failures in the same test,
// and make each failure actionable. If artifacts cannot be collected,
// the original restore error is returned, along with the error
// encountered while fetching the artifacts.
func (u *CommonTestUtils) collectFailureArtifacts(
	ctx context.Context, l *logger.Logger, restoreErr error, errID int,
) (error, error) {
	dirName := fmt.Sprintf("restore_failure_%d", errID)
	rootDir := filepath.Join(u.t.ArtifactsDir(), dirName)
	logsDir := filepath.Join(rootDir, "logs")
	if err := os.MkdirAll(filepath.Dir(logsDir), 0755); err != nil {
		return restoreErr, fmt.Errorf("could not create directory %s: %w", rootDir, err)
	}

	if err := u.cluster.Get(ctx, l, "logs" /* src */, logsDir, u.roachNodes); err != nil {
		return restoreErr, fmt.Errorf("could not fetch logs: %w", err)
	}
	zipLocation := filepath.Join(dirName, "debug.zip")
	if err := u.cluster.FetchDebugZip(ctx, l, zipLocation); err != nil {
		return restoreErr, err
	}

	return fmt.Errorf("%w (artifacts collected in %s)", restoreErr, dirName), nil
}

// takeDebugZip fetches a debug.zip from the cluster and saves it with the
// current timestamp in the test artifacts.
func (u *CommonTestUtils) takeDebugZip(ctx context.Context, l *logger.Logger) {
	zipPath := fmt.Sprintf("debug-%d.zip", timeutil.Now().Unix())
	err := u.cluster.FetchDebugZip(ctx, l, zipPath)
	if err != nil {
		l.Printf("failed to fetch debug zip: %v", err)
	}
}

// resetCluster wipes the entire cluster and starts it again with the
// specified version binary. This is done before we attempt restoring a
// full cluster backup.
func (u *CommonTestUtils) resetCluster(
	ctx context.Context,
	l *logger.Logger,
	version *clusterupgrade.Version,
	expectDeathsFn func(int),
	settings []install.ClusterSettingOption,
) error {
	l.Printf("resetting cluster using version %q", version.String())
	// TODO (kev-cao): Once we've migrated off of the deprecated cluster Monitor,
	// we can remove expectDeathsFn entirely.
	if expectDeathsFn != nil {
		expectDeathsFn(len(u.roachNodes))
	}
	if err := u.cluster.WipeE(ctx, l, u.roachNodes); err != nil {
		return fmt.Errorf("failed to wipe cluster: %w", err)
	}

	var opts = []option.StartStopOption{option.NoBackupSchedule}
	if !version.AtLeast(clusterupgrade.MustParseVersion("v24.1.0")) {
		opts = append(opts, option.DisableWALFailover)
	}

	cockroachPath := clusterupgrade.CockroachPathForVersion(u.t, version)
	settings = append(settings, install.BinaryOption(cockroachPath), install.SimpleSecureOption(true))
	return clusterupgrade.StartWithSettings(
		ctx, l, u.cluster, u.roachNodes, option.NewStartOpts(opts...), settings...,
	)
}

// newCommentTarget returns either a database or a table to be used as
// a target for a `COMMENT ON` statement. Returns the object being
// commented (either 'database' or 'table'), and the name of the
// object itself.
func newCommentTarget(rng *rand.Rand, dbs []string, tables [][]string) (string, string) {
	const dbCommentProbability = 0.4

	targetDBIdx := rng.Intn(len(dbs))
	targetDB := dbs[targetDBIdx]
	if rng.Float64() < dbCommentProbability {
		return "database", targetDB
	}

	dbTables := tables[targetDBIdx]
	targetTable := dbTables[rng.Intn(len(dbTables))]
	return "table", fmt.Sprintf("%s.%s", targetDB, targetTable)
}

// hasInternalSystemJobs returns true if the cluster is expected to
// have the `crdb_internal.system_jobs` vtable. If so, it should be
// used instead of `system.jobs` when querying job status.
func hasInternalSystemJobs(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, db *gosql.DB,
) (bool, error) {
	cv, err := clusterupgrade.ClusterVersion(ctx, l, db)
	if err != nil {
		return false, fmt.Errorf("failed to query cluster version: %w", err)
	}

	internalSystemJobsCV, err := roachpb.ParseVersion(v231CV)
	if err != nil {
		return false, err
	}

	return cv.AtLeast(internalSystemJobsCV), nil
}

func randIntBetween(rng *rand.Rand, min, max int) int {
	return rng.Intn(max-min) + min
}

func randFloatBetween(rng *rand.Rand, min, max float64) float64 {
	return rng.Float64()*(max-min) + min
}

func randString(rng *rand.Rand, strLen int) string {
	return randutil.RandString(rng, strLen, randutil.PrintableKeyAlphabet)
}

func randWaitDuration(rng *rand.Rand) time.Duration {
	durations := []int{1, 10, 60}
	return time.Duration(durations[rng.Intn(len(durations))]) * time.Second
}

func tpccWorkloadCmd(
	l *logger.Logger,
	testRNG *rand.Rand,
	seed int64,
	numWarehouses int,
	roachNodes option.NodeListOption,
) (init *roachtestutil.Command, run *roachtestutil.Command) {
	init = roachtestutil.NewCommand("./cockroach workload init tpcc").
		MaybeOption(testRNG.Intn(2) == 0, "families").
		Arg("{pgurl%s}", roachNodes).
		Flag("warehouses", numWarehouses).
		MaybeFlag(seed != 0, "seed", seed)
	run = roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", roachNodes).
		Flag("warehouses", numWarehouses).
		MaybeFlag(seed != 0, "seed", seed).
		Option("tolerate-errors")
	l.Printf("tpcc init: %s", init)
	l.Printf("tpcc run: %s", run)
	return init, run
}

func bankWorkloadCmd(
	l *logger.Logger, testRNG *rand.Rand, seed int64, roachNodes option.NodeListOption, mock bool,
) (init *roachtestutil.Command, run *roachtestutil.Command) {
	bankRows := bankPossibleRows[testRNG.Intn(len(bankPossibleRows))]
	possiblePayloads := bankPossiblePayloadBytes
	// force smaller row counts to use smaller payloads too to avoid making lots
	// of large revisions of a handful of keys.
	if bankRows < 1000 {
		possiblePayloads = []int{16, 64}
	}
	bankPayload := possiblePayloads[testRNG.Intn(len(possiblePayloads))]

	if mock {
		bankPayload = 9
		bankRows = 10
	}
	init = roachtestutil.NewCommand("./cockroach workload init bank").
		Flag("rows", bankRows).
		MaybeFlag(bankPayload != 0, "payload-bytes", bankPayload).
		MaybeFlag(seed != 0, "seed", seed).
		Flag("ranges", 0).
		Arg("{pgurl%s}", roachNodes)
	run = roachtestutil.NewCommand("./cockroach workload run bank").
		Arg("{pgurl%s}", roachNodes).
		MaybeFlag(seed != 0, "seed", seed).
		Option("tolerate-errors")
	l.Printf("bank init: %s", init)
	l.Printf("bank run: %s", run)
	return init, run
}

func schemaChangeWorkloadCmd(
	l *logger.Logger, testRNG *rand.Rand, seed int64, roachNodes option.NodeListOption, mock bool,
) (init *roachtestutil.Command, run *roachtestutil.Command) {
	maxOps := 1000
	concurrency := 5
	if mock {
		maxOps = 10
		concurrency = 2
	}
	if seed == 0 {
		seed = testRNG.Int63()
	}
	initCmd := roachtestutil.NewCommand("COCKROACH_RANDOM_SEED=%d ./workload init schemachange", seed).
		Arg("{pgurl%s}", roachNodes)
	// TODO (msbutler): ideally we'd use the `db` flag to explicitly set the
	// database, but it is currently broken:
	// https://github.com/cockroachdb/cockroach/issues/115545
	runCmd := roachtestutil.NewCommand("COCKROACH_RANDOM_SEED=%d ./workload run schemachange", seed).
		Flag("verbose", 1).
		Flag("max-ops", maxOps).
		Flag("concurrency", concurrency).
		Arg("{pgurl%s}", roachNodes)
	l.Printf("sc init: %s", initCmd)
	l.Printf("sc run: %s", runCmd)
	return initCmd, runCmd
}

// prepSchemaChangeWorkload creates the schemaChange workload database and a non
// empty table within it, so the test can properly fingerprint the database.
// Without a non-empty table, the test's fingerprint logic erroneously fails.
func prepSchemaChangeWorkload(
	ctx context.Context,
	workloadNode option.NodeListOption,
	testUtils *CommonTestUtils,
	testRNG *rand.Rand,
) error {
	if err := testUtils.Exec(ctx, testRNG, fmt.Sprintf("CREATE DATABASE %s", schemaChangeDB)); err != nil {
		return err
	}
	if err := testUtils.Exec(ctx, testRNG, fmt.Sprintf("CREATE TABLE %s.%s (x INT)", schemaChangeDB, "dummy")); err != nil {
		return err
	}
	if err := testUtils.Exec(ctx, testRNG, fmt.Sprintf("INSERT INTO %s.%s VALUES (1)", schemaChangeDB, "dummy")); err != nil {
		return err
	}
	return nil
}
