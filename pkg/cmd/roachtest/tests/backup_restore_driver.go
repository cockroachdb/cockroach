// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const SystemDatabaseName = "system"

// nonceLen is the length of the randomly generated string appended
// to the backup collection path to ensure uniqueness of the backup collection
// name.
const nonceLen = 4

// systemTablesInFullClusterBackup includes all system tables that
// are included as part of a full cluster backup. It should include
// every table that opts-in to cluster backup (see `system_schema.go`).
// It should be updated as system tables are added or removed from
// cluster backups.
//
// Note that we don't verify the `system.zones` table as there's no
// general mechanism to verify its correctness due to #100852. We
// may change that if the issue is fixed.
var systemTablesInFullClusterBackup = []string{
	"users", "settings", "locations", "role_members", "role_options", "ui",
	"comments", "scheduled_jobs", "database_role_settings", "tenant_settings",
	"privileges", "external_connections",
}

type (
	// BackupRestoreEnv provides an environment for running backup/restores.
	//
	// TODO (kev-cao): This is the successor to the BackupRestoreTestDriver, which
	// should eventually be deprecated and have all usages migbrated to this new
	// API.
	BackupRestoreEnv struct {
		t              test.Test
		log            *logger.Logger
		cluster        cluster.Cluster
		rng            *rand.Rand
		testUtils      *CommonTestUtils
		planNodes      labeledNodes
		executionNodes labeledNodes
	}

	// BackupCollectionHandle provides a handle to a collection of backups that
	// are all taken to the same backup URI. This handle can be used to create new
	// backups.
	//
	// NB: This is not thread safe, although making it thread safe is trivial.
	BackupCollectionHandle struct {
		env *BackupRestoreEnv
		// Collections under the same prefix will be grouped together under the same
		// directory in the backup bucket.
		collectionPrefix string
		// nonce is used to generate a unique name for a backup collection, used when
		// sharing a global namespace represented by the BACKUP_TESTING_BUCKET bucket.
		// The nonce allows multiple people (or TeamCity builds) to be running this
		// test without interfering with one another.
		nonce   string
		scope   BackupScope
		options []backupOption

		// nodelocal is set if the backup collection should be stored in a nodelocal
		// path instead of the shared BACKUP_TESTING_BUCKET. This is automatically set
		// for local clusters.
		nodelocal bool
		// If true, the backup collection is expected to be able to be online
		// restorable. This allows us to maximize test coverage for features that are
		// not yet supported with online restore, but can be used in classic restore.
		onlineRestorable bool

		backups []Backup
	}

	Backup struct {
		id   string
		aost string
		// Stores database-qualified table names that are included in the backup.
		storedTables []string
		// Maps fully-qualified table names to their corresponding fingerprints.
		fingerprints map[string]map[string]string
	}

	// labeledNodes allows us to label a set of nodes with the version
	// they are running, to allow for more descriptive backup collection
	// descriptions.
	labeledNodes struct {
		Nodes   option.NodeListOption
		Version string
	}
)

func NewBackupRestoreEnv(
	t test.Test,
	l *logger.Logger,
	cluster cluster.Cluster,
	rng *rand.Rand,
	testUtils *CommonTestUtils,
	options ...BackupRestoreEnvOption,
) *BackupRestoreEnv {
	env := &BackupRestoreEnv{
		t:         t,
		log:       l,
		cluster:   cluster,
		rng:       rng,
		testUtils: testUtils,
		planNodes: labeledNodes{
			Nodes:   cluster.CRDBNodes(),
			Version: clusterupgrade.CurrentVersion().String(),
		},
		executionNodes: labeledNodes{
			Nodes:   cluster.CRDBNodes(),
			Version: clusterupgrade.CurrentVersion().String(),
		},
	}
	for _, opt := range options {
		opt(env)
	}
	return env
}

type BackupRestoreEnvOption func(*BackupRestoreEnv)

// WithPlanNodes configures which nodes can be used to plan backup/restore. This
// API assumes that all specified nodes are running the same version, which is
// used for labeling purposes. By default, all nodes are eligible for planning.
func WithPlanNodes(nodes option.NodeListOption, version string) BackupRestoreEnvOption {
	return func(env *BackupRestoreEnv) {
		env.planNodes = labeledNodes{
			Nodes:   nodes,
			Version: version,
		}
	}
}

// WithExecutionNodes configures which nodes can be used to in the execution of
// the backup/restore job. This API assumes that all specified nodes are running
// the same version, which is used for labeling purposes. By default, all nodes
// are eligible for execution.
func WithExecutionNodes(nodes option.NodeListOption, version string) BackupRestoreEnvOption {
	return func(env *BackupRestoreEnv) {
		env.executionNodes = labeledNodes{
			Nodes:   nodes,
			Version: version,
		}
	}
}

func InitBackupCollection(
	ctx context.Context,
	env *BackupRestoreEnv,
	collectionPrefix string,
	scope initBackupScope,
	onlineRestorable bool,
	opts ...InitBackupCollectionOption,
) *BackupCollectionHandle {
	require.NotEmpty(env.t, collectionPrefix, "collection prefix for backup collection cannot be empty")
	// Use a different seed for generating the collection's nonce to
	// allow for multiple concurrent runs of this test using the same
	// COCKROACH_RANDOM_SEED, making it easier to reproduce failures
	// that are more likely to occur with certain test plans.
	nonceRng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	backupScope := scope(ctx, env)
	collection := &BackupCollectionHandle{
		env:              env,
		collectionPrefix: collectionPrefix,
		nonce:            randString(nonceRng, nonceLen),
		scope:            backupScope,
		onlineRestorable: onlineRestorable,
		nodelocal:        env.cluster.IsLocal(),
		options:          []backupOption{detached{}},
	}
	for _, opt := range opts {
		opt(env, collection)
	}
	return collection
}

func (c *BackupCollectionHandle) Scope() BackupScope {
	return c.scope
}

// Name returns a globally unique name for the backup collection.
func (c *BackupCollectionHandle) Name() string {
	return fmt.Sprintf(
		"%s_%s",
		c.scope.Desc(),
		c.nonce,
	)
}

func (c *BackupCollectionHandle) URI() string {
	scheme := "gs://"
	if c.nodelocal {
		scheme = "nodelocal://1/"
	}
	escapedName := url.PathEscape(c.Name())
	return fmt.Sprintf(
		"%s%s/%s/%s?AUTH=implicit",
		scheme,
		testutils.BackupTestingBucketLongTTL(),
		c.collectionPrefix,
		escapedName,
	)
}

func (c *BackupCollectionHandle) TakeFull(
	ctx context.Context, fingerprint bool,
) (jobspb.JobID, error) {
	backupTime := c.env.testUtils.now()
	stmt := fmt.Sprintf(
		"BACKUP %s INTO '%s' AS OF SYSTEM TIME '%s'",
		c.scope.AsTargetCmd(),
		c.URI(),
		backupTime,
	)
	if len(c.options) > 0 {
		var optionStrs []string
		for _, opt := range c.options {
			optionStrs = append(optionStrs, opt.OptionString())
		}
		stmt += " WITH " + strings.Join(optionStrs, ", ")
	}

	node, db := c.env.testUtils.RandomDB(c.env.rng, c.env.planNodes.Nodes)
	var jobID jobspb.JobID
	c.env.log.Printf("creating %s backup via node %d: %s", c.scope.Desc(), node, stmt)
	if err := c.env.testUtils.runJobOnOneOf(
		ctx, c.env.log, c.env.executionNodes.Nodes, func() error {
			return errors.Wrapf(
				db.QueryRowContext(ctx, stmt).Scan(&jobID),
				"error while planning %s backup in collection %s",
				c.scope.Desc(), c.Name(),
			)
		},
	); err != nil {
		return 0, err
	}
	if err := c.appendBackup(ctx, db, backupTime, fingerprint); err != nil {
		return 0, err
	}
	return jobID, nil
}

func (c *BackupCollectionHandle) TakeFullSync(
	ctx context.Context, fingerprint bool,
) (jobspb.JobID, error) {
	jobID, err := c.TakeFull(ctx, fingerprint)
	if err != nil {
		return 0, err
	}
	execErr := c.env.testUtils.waitForJobSuccess(
		ctx, c.env.log, c.env.rng, int(jobID), true, /* internalSystemJobs */
	)
	return jobID, execErr
}

func (c *BackupCollectionHandle) TakeIncremental(
	ctx context.Context, fingerprint bool,
) (jobspb.JobID, error) {
	backupTime := c.env.testUtils.now()
	stmt := fmt.Sprintf(
		"BACKUP %s INTO LATEST IN '%s' AS OF SYSTEM TIME '%s'",
		c.scope.AsTargetCmd(),
		c.URI(),
		backupTime,
	)
	if len(c.options) > 0 {
		var optionStrs []string
		for _, opt := range c.options {
			optionStrs = append(optionStrs, opt.OptionString())
		}
		stmt += " WITH " + strings.Join(optionStrs, ", ")
	}

	node, db := c.env.testUtils.RandomDB(c.env.rng, c.env.planNodes.Nodes)
	var jobID jobspb.JobID
	c.env.log.Printf("creating incremental %s backup via node %d: %s", c.scope.Desc(), node, stmt)
	if err := c.env.testUtils.runJobOnOneOf(
		ctx, c.env.log, c.env.executionNodes.Nodes, func() error {
			return errors.Wrapf(
				db.QueryRowContext(ctx, stmt).Scan(&jobID),
				"error while planning incremental %s backup in collection %s",
				c.scope.Desc(), c.Name(),
			)
		},
	); err != nil {
		return 0, err
	}

	if err := c.appendBackup(ctx, db, backupTime, fingerprint); err != nil {
		return 0, err
	}
	return jobID, nil
}

func (c *BackupCollectionHandle) TakeIncrementalSync(
	ctx context.Context, fingerprint bool,
) (jobspb.JobID, error) {
	jobID, err := c.TakeIncremental(ctx, fingerprint)
	if err != nil {
		return 0, err
	}
	execErr := c.env.testUtils.waitForJobSuccess(
		ctx, c.env.log, c.env.rng, int(jobID), true, /* internalSystemJobs */
	)
	return jobID, execErr
}

func (c *BackupCollectionHandle) TakeCompacted(ctx context.Context) (jobspb.JobID, error) {
	stmt := fmt.Sprintf(
		"BACKUP %s INTO LATEST IN '%s'",
		c.scope.AsTargetCmd(),
		c.URI(),
	)
	if len(c.options) > 0 {
		var optionStrs []string
		for _, opt := range c.options {
			optionStrs = append(optionStrs, opt.OptionString())
		}
		stmt += " WITH " + strings.Join(optionStrs, ", ")
	}
	node, db := c.env.testUtils.RandomDB(c.env.rng, c.env.planNodes.Nodes)

	showOpts := ""
	for _, opt := range c.options {
		if encOpt, ok := opt.(encryptionPassphrase); ok {
			showOpts = " WITH " + encOpt.OptionString()
			break
		}
	}
	// Since the compaction builtin still relies on the old SHOW BACKUP UX,
	// there's a decent amount of logic here to figure out the backups to compact.
	// Once we update the builtin to use the new SHOW BACKUP UX outputs, this
	// logic should be simplified.
	var latestSubdir string
	err := db.QueryRowContext(
		ctx,
		fmt.Sprintf(
			`SELECT path FROM [SHOW BACKUPS IN '%s'] ORDER BY path DESC LIMIT 1`,
			c.URI(),
		),
	).Scan(&latestSubdir)
	if err != nil {
		return 0, errors.Wrapf(err, "error querying for latest backup subdir")
	}

	var times [][2]string
	rows, err := db.QueryContext(
		ctx,
		fmt.Sprintf(
			` SELECT DISTINCT start_time, end_time FROM
			[SHOW BACKUP FROM LATEST IN '%s'%s]`,
			c.URI(), showOpts,
		),
	)
	if err != nil {
		return 0, errors.Wrapf(err, "error querying for backup times")
	}
	defer rows.Close()
	for rows.Next() {
		var startTime, endTime gosql.NullString
		if err := rows.Scan(&startTime, &endTime); err != nil {
			return 0, errors.Wrapf(err, "error scanning backup times")
		}
		times = append(times, [2]string{startTime.String, endTime.String})
	}
	if err := rows.Err(); err != nil {
		return 0, errors.Wrapf(err, "error iterating over backup times")
	}
	if chainLen := len(times); chainLen < 3 {
		return 0, errors.Newf(
			"chain length of %d in %s not long enough for compaction", c.Name(), chainLen,
		)
	}
	// Drop the full backup since it does not get compacted.
	times = times[1:]
	var compactWindow int
	if err := db.QueryRowContext(
		ctx, "SHOW CLUSTER SETTING backup.compaction.window_size",
	).Scan(&compactWindow); err != nil {
		return 0, errors.Wrapf(err, "error querying for compaction window setting")
	}
	compactWindow = min(compactWindow, len(times))

	// Pick a random window to compact.
	windowStartIdx := c.env.rng.Intn(len(times) - compactWindow + 1)
	compactStart := times[windowStartIdx][0]
	compactEnd := times[windowStartIdx+compactWindow-1][1]

	c.env.log.Printf(
		"compacting backups from %s to %s in subdir %s of collection %s via node %d",
		compactStart, compactEnd, latestSubdir, c.Name(), node,
	)
	var jobID jobspb.JobID
	if err := c.env.testUtils.runJobOnOneOf(
		ctx, c.env.log, c.env.executionNodes.Nodes, func() error {
			return errors.Wrapf(
				db.QueryRowContext(
					ctx,
					`SELECT crdb_internal.backup_compaction(
						0,
						$1,
						$2,
						$3::TIMESTAMP::DECIMAL * 1e9,
						$4::TIMESTAMP::DECIMAL * 1e9
					)`,
					stmt, latestSubdir, compactStart, compactEnd,
				).Scan(&jobID),
				"error while planning compaction of %s backup in collection %s",
				c.scope.Desc(), c.Name(),
			)
		},
	); err != nil {
		return 0, err
	}

	return jobID, nil
}

func (c *BackupCollectionHandle) TakeCompactedSync(ctx context.Context) (jobspb.JobID, error) {
	jobID, err := c.TakeCompacted(ctx)
	if err != nil {
		return 0, err
	}
	execErr := c.env.testUtils.waitForJobSuccess(
		ctx, c.env.log, c.env.rng, int(jobID), true, /* internalSystemJobs */
	)
	return jobID, execErr
}

func (c *BackupCollectionHandle) appendBackup(
	ctx context.Context, db *gosql.DB, aost string, fingerprint bool,
) error {
	storedTables := c.scope.ScopedTables(ctx, c.env.t, db, aost)
	var fingerprints map[string]map[string]string
	if fingerprint {
		var err error
		c.env.log.Printf(
			"fingerprinting %d tables for backup at %s in collection %s", len(storedTables), aost, c.Name(),
		)
		fingerprints, err = fingerprintTables(
			ctx, c.env.t, db, storedTables, aost,
		)
		if err != nil {
			return errors.Wrapf(
				err,
				"error fingerprinting tables for backup at %s in collection %s", aost, c.Name(),
			)
		}
	}
	backup := Backup{
		aost:         aost,
		storedTables: storedTables,
		fingerprints: fingerprints,
	}
	idx, _ := slices.BinarySearchFunc(c.backups, backup, func(a, b Backup) int {
		return strings.Compare(a.aost, b.aost)
	})
	c.backups = slices.Insert(c.backups, idx, backup)
	return nil
}

// SyncBackups links the backups taken by the collection with their associated IDs.
// SyncBackups should be called before any restore operations are performed to ensure
// all backups have been ID'd.
func (c *BackupCollectionHandle) SyncBackups(ctx context.Context) error {
	c.env.log.Printf("syncing backup collection %s to link tracked backups to IDs", c.Name())
	_, db := c.env.testUtils.RandomDB(c.env.rng, c.env.planNodes.Nodes)
	_, err := db.ExecContext(ctx, "SET use_backups_with_ids = true")
	if err != nil {
		return errors.Wrapf(err, "error setting use_backups_with_ids")
	}
	type showOutput struct {
		id      string
		endTime time.Time
	}
	var showOutputs []showOutput
	rows, err := db.QueryContext(
		ctx,
		fmt.Sprintf(
			`SELECT id, backup_time FROM [SHOW BACKUPS IN '%s']`,
			c.URI(),
		),
	)
	if err != nil {
		return errors.Wrapf(err, "error querying SHOW BACKUPS for collection %s", c.Name())
	}
	defer rows.Close()
	for rows.Next() {
		var show showOutput
		if err := rows.Scan(&show.id, &show.endTime); err != nil {
			return errors.Wrapf(err, "error scanning SHOW BACKUPS output for collection %s", c.Name())
		}
		showOutputs = append(showOutputs, show)
	}
	if err := rows.Err(); err != nil {
		return errors.Wrapf(err, "error iterating over SHOW BACKUPS output for collection %s", c.Name())
	}
	for idx, backup := range c.backups {
		if backup.id != "" {
			continue
		}
		unixNano, err := strconv.ParseInt(strings.Split(backup.aost, ".")[0], 10, 64)
		if err != nil {
			return errors.Wrapf(err, "error parsing AOST %s", backup.aost)
		}
		// SHOW BACKUPS outputs end times in seconds, so we round the AOST to
		// seconds.
		recordedEndTime := timeutil.Unix(0, unixNano).Round(time.Second)
		showIdx := slices.IndexFunc(showOutputs, func(output showOutput) bool {
			return output.endTime.Equal(recordedEndTime)
		})
		if showIdx == -1 {
			return errors.Newf(
				"could not find SHOW BACKUPS record for backup with AOST %s in collection %s",
				backup.aost, c.Name(),
			)
		}
		c.backups[idx].id = showOutputs[showIdx].id
		showOutputs = slices.Delete(showOutputs, showIdx, showIdx+1)
	}
	return nil
}

// Restore restores a backup with the given backupID from the backup collection.
// If the collection is scoped to the entire cluster, the cluster will be reset
// in order to allow for a full cluster restore.
// NB: Prior to calling Restore, SyncBackups should be called to ensure that all
// backups in the collection have been linked to their corresponding IDs.
func (c *BackupCollectionHandle) Restore(
	ctx context.Context, backupID string,
) (jobspb.JobID, error) {
	node, db := c.env.testUtils.RandomDB(c.env.rng, c.env.planNodes.Nodes)
	if _, err := db.ExecContext(
		ctx,
		fmt.Sprintf(`DROP DATABASE IF EXISTS "restored_%s" CASCADE`, backupID),
	); err != nil {
		return 0, err
	}

	var optionStrs []string
	switch c.scope.(type) {
	case tableScope:
		if _, err := db.ExecContext(
			ctx,
			fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS "restored_%s"`, backupID),
		); err != nil {
			return 0, err
		}
		optionStrs = append(optionStrs, fmt.Sprintf("into_db='restored_%s'", backupID))
	case databaseScope:
		optionStrs = append(optionStrs, fmt.Sprintf("new_db_name='restored_%s'", backupID))
	case clusterScope:
		if err := resetCluster(c.env.t, ctx, c.env.testUtils); err != nil {
			return 0, errors.Wrapf(
				err, "error resetting cluster before restoring backup %s", backupID,
			)
		}
	}
	for _, opt := range c.options {
		if _, ok := opt.(encryptionPassphrase); ok {
			optionStrs = append(optionStrs, opt.OptionString())
		}
	}

	stmt := fmt.Sprintf(
		"RESTORE %s FROM '%s' IN '%s'",
		c.scope.AsTargetCmd(),
		backupID,
		c.URI(),
	)
	if len(optionStrs) > 0 {
		stmt += " WITH " + strings.Join(optionStrs, ", ")
	}

	var jobID jobspb.JobID
	c.env.log.Printf("restoring backup %s via node %d: %s", backupID, node, stmt)
	if err := c.env.testUtils.runJobOnOneOf(
		ctx, c.env.log, c.env.executionNodes.Nodes, func() error {
			return errors.Wrapf(
				db.QueryRowContext(ctx, stmt).Scan(&jobID),
				"error while planning restore of backup %s in collection %s",
				backupID, c.Name(),
			)
		},
	); err != nil {
		return 0, err
	}

	return jobID, nil
}

// RestoreSync restores a backup and waits for the restore job to complete
// before returning. If the collection is scoped to the entire cluster, the
// cluster will be reset in order to allow for a full cluster restore.
func (c *BackupCollectionHandle) RestoreSync(
	ctx context.Context, backupID string,
) (jobspb.JobID, error) {
	jobID, err := c.Restore(ctx, backupID)
	if err != nil {
		return 0, err
	}
	execErr := c.env.testUtils.waitForJobSuccess(
		ctx, c.env.log, c.env.rng, int(jobID), true, /* internalSystemJobs */
	)
	return jobID, execErr
}

// VerifyBackup validates that contents of a backup after a restore match the
// contents at the time of the backup. This assumes that a restore of the backup
// has been completed, either via RestoreSync or Restore followed by a wait for
// job completion.
func (c *BackupCollectionHandle) VerifyBackup(ctx context.Context, backupID string) error {
	backupIdx := slices.IndexFunc(c.backups, func(b Backup) bool {
		return b.id == backupID
	})
	require.NotEqual(
		c.env.t, -1, backupIdx, "backup %s not found in collection %s", backupID, c.Name(),
	)
	backup := c.backups[backupIdx]
	require.NotEmpty(
		c.env.t, backup.fingerprints,
		"backup %s in collection %s was not fingerprinted, cannot be verified", backupID, c.Name(),
	)
	node, db := c.env.testUtils.RandomDB(c.env.rng, c.env.planNodes.Nodes)
	c.env.log.Printf("verifying backup %s via node %d", backupID, node)
	grp := c.env.t.NewErrorGroup()
	dbPattern := regexp.MustCompile(`^.+?\.`)
	for table, backupFp := range backup.fingerprints {
		grp.Go(func(ctx context.Context, l *logger.Logger) error {
			restoreFp := make(map[string]string)
			restoredTable := dbPattern.ReplaceAllString(table, fmt.Sprintf(`"restored_%s".`, backupID))
			fpRows, err := db.QueryContext(
				ctx, fmt.Sprintf(`SELECT * FROM [SHOW FINGERPRINTS FROM TABLE %s]`, restoredTable),
			)
			if err != nil {
				return err
			}
			defer fpRows.Close()
			for fpRows.Next() {
				var idxName string
				var idxFp gosql.NullString
				if err := fpRows.Scan(&idxName, &idxFp); err != nil {
					return err
				}
				restoreFp[idxName] = idxFp.String
			}
			if err := fpRows.Err(); err != nil {
				return err
			}
			if !reflect.DeepEqual(backupFp, restoreFp) {
				return errors.Newf(
					"fingerprint mismatch for table %s: expected %v, got %v",
					table, backupFp, restoreFp,
				)
			}
			return nil
		})
	}
	if err := grp.WaitE(); err != nil {
		return errors.Wrapf(
			err, "error during fingerprint of backup %s in collection %s", backupID, c.Name(),
		)
	}
	c.env.log.Printf("backup %s in collection %s successfully verified", backupID, c.Name())
	return nil
}

func (c *BackupCollectionHandle) Backups() []Backup {
	return c.backups
}

func (c *BackupCollectionHandle) LatestBackup() Backup {
	return c.backups[len(c.backups)-1]
}

func (B Backup) ID() string {
	return B.id
}

type InitBackupCollectionOption func(*BackupRestoreEnv, *BackupCollectionHandle)

func WithRevisionHistory() InitBackupCollectionOption {
	return func(env *BackupRestoreEnv, bc *BackupCollectionHandle) {
		bc.options = append(bc.options, revisionHistory{})
	}
}

func WithEncryptionPassphrase(passphrase string) InitBackupCollectionOption {
	return func(env *BackupRestoreEnv, bc *BackupCollectionHandle) {
		bc.options = append(bc.options, encryptionPassphrase{passphrase: passphrase})
	}
}

// WithMetamorphicBackupOptions randomly adds backup options that are compatible
// with the backup collection.
func WithMetamorphicBackupOptions() InitBackupCollectionOption {
	return func(env *BackupRestoreEnv, bc *BackupCollectionHandle) {
		possibleOpts := []backupOption{
			revisionHistory{},
		}
		if !bc.onlineRestorable {
			possibleOpts = append(
				possibleOpts,
				randEncryptionPassphrase(env.rng),
			)
		}

		for _, opt := range possibleOpts {
			if env.rng.Intn(2) == 0 {
				// Don't include the option if we've already included it.
				included := false
				optType := reflect.TypeOf(opt)
				for _, existingOpt := range bc.options {
					if reflect.TypeOf(existingOpt) == optType {
						included = true
					}
				}

				if !included {
					bc.options = append(bc.options, opt)
				}
			}
		}
	}
}

type initBackupScope func(context.Context, *BackupRestoreEnv) BackupScope

// TableScope creates a BackupScope for backing up specific tables. The provided
// tables should be fully qualified and belong to the same database.
func TableScope(tables []string) initBackupScope {
	return func(_ context.Context, env *BackupRestoreEnv) BackupScope {
		require.NotEmpty(env.t, tables, "table scope must include at least one table")
		require.False(
			env.t, slices.ContainsFunc(tables, func(table string) bool {
				return strings.HasPrefix(table, SystemDatabaseName+".")
			}),
			"table level backups of the system tables are not supported",
		)
		parentDB := strings.Split(tables[0], ".")[0]
		for _, table := range tables {
			parts := strings.Split(table, ".")
			require.Len(
				env.t, parts, 3, "table %s is not fully qualified with database and schema", table,
			)
			require.Equal(
				env.t, parentDB, parts[0],
				"all tables in a table scoped backup must belong to the same database",
			)
		}
		return tableScope{tables: tables}
	}
}

// DatabaseScope creates a BackupScope for backing up a specific database.
func DatabaseScope(db string) initBackupScope {
	return func(ctx context.Context, env *BackupRestoreEnv) BackupScope {
		require.NotEqual(
			env.t, SystemDatabaseName, db,
			"database level backups of the system database are not supported",
		)
		return databaseScope{db: db}
	}
}

// ClusterScope creates a BackupScope for backing up the entire cluster.
func ClusterScope() initBackupScope {
	return func(ctx context.Context, env *BackupRestoreEnv) BackupScope {
		return clusterScope{}
	}
}

// RandomTableScope creates a BackupScope for backing up a random table from a
// random database chosen from the list of eligible databases passed in.
func RandomTableScope(dbs []string) initBackupScope {
	return func(ctx context.Context, env *BackupRestoreEnv) BackupScope {
		dbs = filterDBsForTableScope(dbs)
		require.NotEmpty(env.t, dbs, "no eligible databases to select from for random table backup")
		targetDB := dbs[env.rng.Intn(len(dbs))]
		_, conn := env.testUtils.RandomDB(env.rng, env.planNodes.Nodes)
		tables, err := loadQualifiedTablesForDBs(env.t, ctx, conn, "now", targetDB)
		require.NoError(env.t, err, "error loading tables for database %s", targetDB)
		return tableScope{tables: tables}
	}
}

// RandomDatabaseScope creates a BackupScope for backing up a random database.
// It takes in an eligible list of databases and randomly selects one to back up.
func RandomDatabaseScope(dbs []string) initBackupScope {
	return func(_ context.Context, env *BackupRestoreEnv) BackupScope {
		dbs = filterDBsForDatabaseScope(dbs)
		require.NotEmpty(env.t, dbs, "no eligible databases to select from for random database backup")
		return databaseScope{db: dbs[env.rng.Intn(len(dbs))]}
	}
}

// RandomBackupScope chooses a random BackupScope. If a database or table level
// scope is chosen, the database(s) and table(s) included in the scope are
// randomly selected from the list of eligible databases passed in.
func RandomBackupScope(dbs []string) initBackupScope {
	return func(ctx context.Context, env *BackupRestoreEnv) BackupScope {
		validScopes := []initBackupScope{ClusterScope()}
		if len(filterDBsForDatabaseScope(dbs)) > 0 {
			validScopes = append(validScopes, RandomDatabaseScope(dbs))
		}
		if len(filterDBsForTableScope(dbs)) > 0 {
			validScopes = append(validScopes, RandomTableScope(dbs))
		}
		return validScopes[env.rng.Intn(len(validScopes))](ctx, env)
	}
}

func filterDBsForTableScope(dbs []string) []string {
	// Avoid creating table backups for the tpcc database, as they often
	// have foreign keys to other tables, making restoring them
	// difficult. We could pass the `skip_missing_foreign_keys` option,
	// but that would be a less interesting test.
	//
	// Avoid creating table backups for the schemachange database, as it inits
	// with 0 tables.
	//
	// Table level backups of the system database are not supported.
	return util.Filter(dbs, func(db string) bool {
		return db != "tpcc" && db != schemaChangeDB && db != SystemDatabaseName
	})
}

func filterDBsForDatabaseScope(dbs []string) []string {
	// Database level backups of the system database are not supported.
	return util.Filter(dbs, func(db string) bool {
		return db != SystemDatabaseName
	})
}

type (
	BackupScope interface {
		// Desc returns a string describing the backup type, and is used when creating
		// the name for a backup collection.
		//
		// NB: While not strictly necessary, using URL path safe characters is
		// recommended as the description will be sanitized to be used in the backup
		// collection URI.
		Desc() string
		// AsTargetCmd returns the string to be used in a backup/restore command. It
		// specifically represents the `target` portion of the `BACKUP {target}` and
		// `RESTORE {target}`. For example, for a table backup, this would return
		// `TABLE {databaseName}.{tableName}`.
		AsTargetCmd() string
		// ScopedTables returns a list of fully-qualified table names that
		// were in scope at a given AOST. This is used to determine which tables
		// were stored in a backup and are eligible for verification.
		ScopedTables(ctx context.Context, t test.Test, conn *gosql.DB, aost string) []string
	}

	// NB: Due to the fact that restore operations are limited to single-database
	// restores when restoring into a separate database, our table/database scopes
	// are also limited to single-database scopes. Restoring into a separate
	// database allows for better observability into fingerprint discrepancies
	// when running roachtest --debug.

	tableScope struct {
		// tables is a list of fully-qualified table names. The tables should all
		// belong to the same database.
		tables []string
	}

	databaseScope struct {
		db string
	}

	clusterScope struct{}
)

func (ts tableScope) Desc() string {
	return "table-" + strings.Join(ts.tables, "+")
}

func (ts tableScope) AsTargetCmd() string {
	return "TABLE " + strings.Join(ts.tables, ", ")
}

func (ts tableScope) ScopedTables(_ context.Context, _ test.Test, _ *gosql.DB, _ string) []string {
	return ts.tables
}

func (ds databaseScope) Desc() string {
	return "database-" + ds.db
}

func (ds databaseScope) AsTargetCmd() string {
	return "DATABASE " + ds.db
}

func (ds databaseScope) ScopedTables(
	ctx context.Context, t test.Test, conn *gosql.DB, aost string,
) []string {
	tables, err := loadQualifiedTablesForDBs(t, ctx, conn, aost, ds.db)
	require.NoError(t, err, "error loading tables for databases %v", ds.db)
	return tables
}

func (clusterScope) Desc() string {
	return "cluster"
}

func (clusterScope) AsTargetCmd() string {
	return ""
}

func (clusterScope) ScopedTables(
	ctx context.Context, t test.Test, conn *gosql.DB, aost string,
) []string {
	var dbs []string
	rows, err := conn.QueryContext(
		ctx,
		fmt.Sprintf("SELECT database_name FROM [SHOW DATABASES] AS OF SYSTEM TIME %s", aost),
	)
	require.NoError(t, err, "error querying for databases at AOST %s", aost)
	defer rows.Close()
	for rows.Next() {
		var dbName string
		require.NoError(t, rows.Scan(&dbName), "error scanning database name")
		dbs = append(dbs, dbName)
	}

	var targets []string
	if idx := slices.Index(dbs, SystemDatabaseName); idx != -1 {
		// A full cluster backup doesn't include every table, so we manually handle
		// the system database.
		dbs = slices.Delete(dbs, idx, idx+1)
		targets = append(
			targets,
			util.Map(systemTablesInFullClusterBackup, func(table string) string {
				return fmt.Sprintf("%s.%s", SystemDatabaseName, table)
			})...,
		)
	}

	dbTables, err := loadQualifiedTablesForDBs(t, ctx, conn, aost, dbs...)
	require.NoError(t, err, "error loading tables for databases %v", dbs)
	return append(targets, dbTables...)
}

type (
	// backupOption is an option passed to the `BACKUP` command (i.e.,
	// `WITH ...` portion).
	backupOption interface {
		// OptionString returns the string representation of the backup option,
		// which is used in the `WITH ...` portion of the `BACKUP` command.
		OptionString() string
	}

	detached struct{}

	revisionHistory struct{}

	encryptionPassphrase struct {
		passphrase string
	}
)

// All new backup options should be added here. If your option can be
// metamorphically enabled with some sensible defaults, consider adding it to

func (detached) OptionString() string {
	return "detached"
}

func (revisionHistory) OptionString() string {
	return "revision_history"
}

func (ep encryptionPassphrase) OptionString() string {
	return fmt.Sprintf("encryption_passphrase = '%s'", ep.passphrase)
}

// fingerprintTables returns a mapping from database-qualified table names to a
// mapping of their indexes to their corresponding fingerprints.
func fingerprintTables(
	ctx context.Context, t test.Test, conn *gosql.DB, tables []string, aost string,
) (map[string]map[string]string, error) {
	var mu syncutil.Mutex
	fingerprints := make(map[string]map[string]string)
	grp := t.NewErrorGroup()
	for _, table := range tables {
		grp.Go(func(ctx context.Context, l *logger.Logger) error {
			tableFps := make(map[string]string)
			fpRows, err := conn.QueryContext(
				ctx,
				fmt.Sprintf(
					`SELECT * FROM
					[SHOW FINGERPRINTS FROM TABLE %s]
					AS OF SYSTEM TIME '%s'
					`,
					table, aost,
				),
			)
			if err != nil {
				return err
			}
			defer fpRows.Close()
			for fpRows.Next() {
				var idxName string
				var idxFp gosql.NullString
				if err := fpRows.Scan(&idxName, &idxFp); err != nil {
					return err
				}
				tableFps[idxName] = idxFp.String
			}
			if err := fpRows.Err(); err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			fingerprints[table] = tableFps
			return nil
		})
	}
	if err := grp.WaitE(); err != nil {
		return nil, err
	}
	return fingerprints, nil
}

func loadQualifiedTablesForDBs(
	t test.Test, ctx context.Context, conn *gosql.DB, aost string, dbs ...string,
) ([]string, error) {
	var mu syncutil.Mutex
	var tables []string

	grp := t.NewErrorGroup(task.WithContext(ctx))
	for _, db := range dbs {
		grp.Go(func(ctx context.Context, l *logger.Logger) error {
			l.Printf("loading table information for DB %q", db)
			rows, err := conn.QueryContext(
				ctx,
				fmt.Sprintf(
					`SELECT schema_name, table_name FROM [SHOW TABLES FROM %s] WHERE type = 'table'`, db,
				),
			)
			if err != nil {
				return fmt.Errorf("failed to read tables for database %s: %w", db, err)
			}
			defer rows.Close()

			var dbTables []string
			for rows.Next() {
				var schemaName, tableName string
				if err := rows.Scan(&schemaName, &tableName); err != nil {
					return fmt.Errorf("error scanning table_name for db %s: %w", db, err)
				}
				dbTables = append(dbTables, fmt.Sprintf("%s.%s.%s", db, schemaName, tableName))
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("error iterating over table_name rows for database %s: %w", db, err)
			}
			mu.Lock()
			defer mu.Unlock()
			tables = append(tables, dbTables...)
			l.Printf("database %q has %d tables", db, len(dbTables))
			return nil
		})
	}
	if err := grp.WaitE(); err != nil {
		return nil, err
	}
	return tables, nil
}

// resetCluster takes a debug zip and resets the cluster, expecting all nodes to restart.
func resetCluster(t test.Test, ctx context.Context, testUtils *CommonTestUtils) error {
	expectDeathsFn := func(n int) {
		t.Monitor().ExpectProcessDead(testUtils.cluster.All())
	}
	// Before we reset the cluster, grab a debug zip.
	zipPath := fmt.Sprintf("debug-%d.zip", timeutil.Now().Unix())
	if err := testUtils.cluster.FetchDebugZip(ctx, t.L(), zipPath); err != nil {
		t.L().Printf("failed to fetch a debug zip: %v", err)
	}
	if err := testUtils.resetCluster(
		ctx, t.L(), clusterupgrade.CurrentVersion(), expectDeathsFn, []install.ClusterSettingOption{},
	); err != nil {
		return err
	}
	return nil
}
