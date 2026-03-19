// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"path"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	apd "github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// nonceLen is the length of the randomly generated string appended
	// to the backup name when creating a backup in GCS for this test.
	nonceLen = 4

	// probabilities that the test will attempt to pause a backup job.
	neverPause  = 0
	alwaysPause = 1

	// the test will not pause a backup job more than `maxPauses` times.
	maxPauses = 3

	// probability that we will attempt to run an AOST restore.
	restoreFromAOSTProbability = 0.5

	// systemTableSentinel is a placeholder value for system table
	// columns that cannot be validated after restore, for various
	// reasons. See `handleSpecialCases` for more details.
	systemTableSentinel = "{SENTINEL}"
)

var (
	// systemTablesInFullClusterBackup includes all system tables that
	// are included as part of a full cluster backup. It should include
	// every table that opts-in to cluster backup (see `system_schema.go`).
	// It should be updated as system tables are added or removed from
	// cluster backups.
	//
	// Note that we don't verify the `system.zones` table as there's no
	// general mechanism to verify its correctness due to #100852. We
	// may change that if the issue is fixed.
	systemTablesInFullClusterBackup = []string{
		"users", "settings", "locations", "role_members", "role_options", "ui",
		"comments", "scheduled_jobs", "database_role_settings", "tenant_settings",
		"privileges", "external_connections",
	}

	// showSystemQueries maps system table names to `SHOW` statements
	// that reflect their contents in a more human readable format. When
	// the contents of a system table after restore does not match the
	// values present when the backup was taken, we show the output of
	// the "SHOW" startements to make debugging failures easier.
	showSystemQueries = map[string]string{
		"privileges":   "SHOW GRANTS",
		"role_members": "SHOW ROLES",
		"settings":     "SHOW CLUSTER SETTINGS",
		"users":        "SHOW USERS",
		"zones":        "SHOW ZONE CONFIGURATIONS",
	}

	possibleNumIncrementalBackups = []int{
		2,
		3,
	}

	invalidDBNameRE = regexp.MustCompile(`[\-\.:/]`)
)

type BackupRestoreTestDriver struct {
	ctx     context.Context
	t       test.Test
	cluster cluster.Cluster

	roachNodes option.NodeListOption

	// databases where user data is being inserted
	dbs    []string
	tables [][]string

	// counter of restores, incremented atomically. Provides unique
	// database names that are used as restore targets when table and
	// database backups are restored.
	currentRestoreID int64

	testUtils *CommonTestUtils
}

func newBackupRestoreTestDriver(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	testUtils *CommonTestUtils,
	nodes option.NodeListOption,
	dbs []string,
	tables [][]string,
) (*BackupRestoreTestDriver, error) {
	d := &BackupRestoreTestDriver{
		ctx:        ctx,
		t:          t,
		cluster:    c,
		testUtils:  testUtils,
		roachNodes: nodes,
		dbs:        dbs,
		tables:     tables,
	}

	return d, nil
}

// createBackupCollection creates a new backup collection to be
// restored/verified at the end of the test. A full backup is created,
// and an incremental one is created on top of it. Both backups are
// created according to their respective `backupSpec`, indicating
// where they should be planned and executed.
func (d *BackupRestoreTestDriver) createBackupCollection(
	ctx context.Context,
	l *logger.Logger,
	tasker task.Tasker,
	rng *rand.Rand,
	fullBackupSpec backupSpec,
	incBackupSpec backupSpec,
	backupNamePrefix string,
	internalSystemJobs bool,
	isMultitenant bool,
) (*backupCollection, error) {
	builder := d.NewCollectionBuilder(
		l, tasker, rng, backupNamePrefix,
		fullBackupSpec, incBackupSpec,
		internalSystemJobs, isMultitenant,
	)

	// Create full backup.
	if _, err := builder.TakeFullSync(ctx); err != nil {
		return nil, err
	}

	// Create incremental backups.
	numIncrementals := possibleNumIncrementalBackups[rng.Intn(len(possibleNumIncrementalBackups))]
	if d.testUtils.mock {
		numIncrementals = 2
	}
	l.Printf("creating %d incremental backups", numIncrementals)

	for range numIncrementals {
		d.randomWait(l, rng)
		if _, err := builder.TakeIncSync(ctx); err != nil {
			return nil, err
		}

		if d.testUtils.compactionEnabled && builder.BackupCount() >= 3 {
			// Compact a random range of incremental backups.
			// startIdx must be >= 1 since full backup (index 0) cannot be compacted.
			// Example: with 3 backups [full=0, inc1=1, inc2=2], we can compact [1,2].
			startIdx := 1 + rng.Intn(builder.BackupCount()-2)
			endIdx := builder.BackupCount() - 1

			if _, err := builder.TakeCompactedSync(ctx, startIdx, endIdx); err != nil {
				return nil, err
			}
		}
	}

	return builder.Finalize(ctx)
}

// verifyBackupCollection restores the backup collection passed and verifies
// that the contents after the restore match the contents when the backup was
// taken. For cluster level backups, the cluster needs to be wiped before
// verifyBackupCollection is called or else the cluster restore will fail.
//
// NB: Deprecated in favor of using Restore/RestoreSync and ValidateRestore
// directly.
func (d *BackupRestoreTestDriver) verifyBackupCollection(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	bc *backupCollection,
	checkFiles bool,
	internalSystemJobs bool,
) error {
	rj, err := d.RestoreSync(ctx, l, rng, bc, checkFiles, internalSystemJobs)
	if err != nil {
		return fmt.Errorf("error restoring backup: %w", err)
	}
	return rj.ValidateRestore(ctx)
}

// getRestoredContents loads the contents (e.g. non system table fingerprints)
// of the tables that were restored from the backup collection. This should be
// called after runRestore has been called.
func (d *BackupRestoreTestDriver) getRestoredContents(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	restoredTables []string,
	bc *backupCollection,
	internalSystemJobs bool,
) ([]tableContents, error) {
	var restoredContents []tableContents
	var err error
	if d.testUtils.onlineRestore {
		waitForDownloadJob := rng.Intn(3) == 0
		restoredContents, err = d.getOnlineRestoredContents(
			ctx, l, rng, bc, restoredTables, internalSystemJobs, waitForDownloadJob,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"backup %s: error loading online restore contents: %w (waitForDownloadJob: %v)",
				bc.name, err, waitForDownloadJob,
			)
		}
	} else {
		restoredContents, err = d.computeTableContents(
			ctx, l, rng, restoredTables, bc.contents, "", /* timestamp */
		)
		if err != nil {
			return nil, fmt.Errorf("backup %s: error loading restored contents: %w", bc.name, err)
		}
	}
	return restoredContents, nil
}

// buildRestoreStatement builds the restore statement for the backup collection
// and returns the command, the tables being restored and the database being
// restored.
// Note: the database being restored into may not exist and may need to be
// created (e.g. when restoring a table backup).
func (d *BackupRestoreTestDriver) buildRestoreStatement(
	bc *backupCollection,
) (restoreStmt string, tables []string, database string) {
	tables, database = d.getRestoredTablesAndDB(bc)
	restoreCmd, options := bc.btype.RestoreCommand(database)
	restoreOptions := append([]string{"detached"}, options...)
	// If the backup was created with an encryption passphrase, we
	// need to include it when restoring as well.
	if opt := bc.encryptionOption(); opt != nil {
		restoreOptions = append(restoreOptions, opt.String())
	}
	if d.testUtils.onlineRestore {
		restoreOptions = append(restoreOptions, "experimental deferred copy")
	}

	var optionsStr string
	if len(restoreOptions) > 0 {
		optionsStr = fmt.Sprintf(" WITH %s", strings.Join(restoreOptions, ", "))
	}
	restoreStmt = fmt.Sprintf(
		"%s FROM LATEST IN '%s'%s %s",
		restoreCmd, bc.uri(), aostFor(bc.restoreAOST), optionsStr,
	)
	return restoreStmt, tables, database
}

// getRestoredTablesAndDB returns the name of the database being restored *into*
// along with the names of the tables being restored.
// Note: the database being restored into may not exist and may need to be
// created (e.g. when restoring a table backup).
func (d *BackupRestoreTestDriver) getRestoredTablesAndDB(bc *backupCollection) ([]string, string) {
	// Defaults for the database where the backup will be restored,
	// along with the expected names of the tables after restore.
	restoreDB := fmt.Sprintf(
		"restore_%s_%d", invalidDBNameRE.ReplaceAllString(bc.name, "_"), d.nextRestoreID(),
	)
	restoredTables := tableNamesWithDB(restoreDB, bc.tables)
	_, ok := bc.btype.(*clusterBackup)
	if ok {
		// For cluster backups, the restored tables are always the same
		// as the tables we backed up. In addition, we need to wipe the
		// cluster before attempting a restore.
		restoredTables = bc.tables
	}
	return restoredTables, restoreDB
}

// randomWait waits from 1s to 3m, to allow for the background
// workloads to update the databases we are backing up.
func (d *BackupRestoreTestDriver) randomWait(l *logger.Logger, rng *rand.Rand) {
	dur := randWaitDuration(rng)
	if d.testUtils.mock {
		dur = time.Millisecond
	}
	l.Printf("waiting for %s", dur)
	time.Sleep(dur)
}

func (d *BackupRestoreTestDriver) nextRestoreID() int64 {
	return atomic.AddInt64(&d.currentRestoreID, 1)
}

// computeTableContents will generate a list of `tableContents`
// implementations for each table in the `tables` parameter. If we are
// computing table contents after a restore, the `previousContents`
// should include the contents of the same tables at the time the
// backup was taken.
func (d *BackupRestoreTestDriver) computeTableContents(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	tables []string,
	previousContents []tableContents,
	timestamp string,
) ([]tableContents, error) {
	previousTableContents := func(j int) tableContents {
		if len(previousContents) == 0 {
			return nil
		}
		return previousContents[j]
	}

	result := make([]tableContents, len(tables))
	eg := d.t.NewErrorGroup(task.WithContext(ctx), task.Logger(l))
	for j, table := range tables {
		eg.Go(func(ctx context.Context, l *logger.Logger) error {
			node, db := d.testUtils.RandomDB(rng, d.roachNodes)
			l.Printf("querying table contents for %s through node %d", table, node)
			var contents tableContents
			var err error
			if strings.HasPrefix(table, "system.") {
				node := d.testUtils.RandomNode(rng, d.roachNodes)
				contents, err = newSystemTableContents(ctx, d.cluster, node, db, table, timestamp)
				if err != nil {
					return err
				}
			} else {
				contents = newFingerprintContents(db, table)
			}

			if err := contents.Load(ctx, l, timestamp, previousTableContents(j)); err != nil {
				return err
			}
			result[j] = contents
			l.Printf("loaded contents for %s", table)
			return nil
		})
	}

	if err := eg.WaitE(); err != nil {
		l.ErrorfCtx(ctx, "Error loading system table content %s", err)
		return nil, err
	}

	return result, nil
}

// saveContents computes the contents for every table in the
// collection passed, and caches it in the `contents` field of the
// collection.
func (d *BackupRestoreTestDriver) saveContents(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	collection *backupCollection,
	timestamp string,
) (*backupCollection, error) {
	l.Printf("backup %s: loading table contents at timestamp '%s'", collection.name, timestamp)
	contents, err := d.computeTableContents(
		ctx, l, rng, collection.tables, nil /* previousContents */, timestamp,
	)
	if err != nil {
		return nil, fmt.Errorf("error computing contents for backup %s: %w", collection.name, err)
	}

	collection.contents = contents
	l.Printf("computed contents for %d tables as part of %s", len(collection.contents), collection.name)

	return collection, nil
}

func (d BackupRestoreTestDriver) getOnlineRestoredContents(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	bc *backupCollection,
	restoredTables []string,
	internalSystemJobs bool,
	waitForDownloadJob bool,
) ([]tableContents, error) {
	downloadJobID, err := d.getORDownloadJobID(ctx, l, rng)
	if err != nil {
		return nil, fmt.Errorf("backup %s: error getting download job ID: %w", bc.name, err)
	}

	if waitForDownloadJob {
		// Sometimes wait for the download job to complete before fingerprinting.
		if err := d.testUtils.waitForJobSuccess(ctx, l, rng, downloadJobID, internalSystemJobs); err != nil {
			return nil, err
		}
	}
	restoredContents, err := d.computeTableContents(
		ctx, l, rng, restoredTables, bc.contents, "", /* timestamp */
	)
	if err != nil {
		return nil, fmt.Errorf("backup %s: error loading online restored contents: %w", bc.name, err)
	}

	// Verify the download job did indeed download all the data.
	if err := d.testUtils.waitForJobSuccess(ctx, l, rng, downloadJobID, internalSystemJobs); err != nil {
		return nil, err
	}
	db := d.testUtils.cluster.Conn(ctx, l, d.roachNodes[0])
	defer db.Close()
	if err := checkNoExternalBytesRemaining(ctx, db); err != nil {
		return nil, fmt.Errorf("download job %d did not download all data", downloadJobID)
	}
	return restoredContents, nil
}

// checkNoExternalBytesRemaining checks that there are no external bytes
// remaining of the tenant that runs the query. If there are, it returns an
// error.
func checkNoExternalBytesRemaining(ctx context.Context, db *gosql.DB) error {
	var externalBytes uint64
	if err := db.QueryRowContext(ctx, jobutils.GetExternalBytesForConnectedTenant).Scan(&externalBytes); err != nil {
		return errors.Wrapf(err, "could not get external bytes")
	}
	if externalBytes != 0 {
		return fmt.Errorf("cluster has %d external bytes remaining", externalBytes)
	}
	return nil
}

func (d BackupRestoreTestDriver) getORDownloadJobID(
	ctx context.Context, l *logger.Logger, rng *rand.Rand,
) (int, error) {
	var downloadJobID int
	if err := d.testUtils.QueryRow(
		ctx,
		rng,
		`SELECT job_id FROM [SHOW JOBS]
		WHERE description LIKE '%Background Data Download%' AND job_type = 'RESTORE'
		ORDER BY created DESC LIMIT 1`,
	).Scan(&downloadJobID); err != nil {
		return 0, err
	}
	return downloadJobID, nil
}

// deleteSSTFromBackupLayers deletes one SST from each layer of a backup
// collection. This is used to test online restore recovery. Deleting an SST
// from each layer ensures that at least one SST required for a restore will be
// missing.
func (d *BackupRestoreTestDriver) deleteSSTFromBackupLayers(
	ctx context.Context, l *logger.Logger, db *gosql.DB, collection *backupCollection,
) error {
	type sstInfo struct {
		path, start, end string
		bytes, rows      int
	}
	rows, err := db.QueryContext(
		ctx,
		// We delete the last SST in each directory just to ensure that we always
		// delete user data. If a system table data is deleted, it is possible for
		// the temporary system table to be GC'd before the download phase detects
		// the missing file.
		`WITH with_dir AS (
			SELECT *, regexp_replace(path, '/[^/]+\.sst$', '') AS dir
			FROM [SHOW BACKUP FILES FROM LATEST IN $1]
		)
		SELECT path, start_pretty, end_pretty, size_bytes, rows
		FROM (
			SELECT *, row_number() OVER (PARTITION BY dir ORDER BY path DESC) AS rn
			FROM with_dir
		)
		WHERE rn = 1`,
		collection.uri(),
	)
	if err != nil {
		return errors.Wrapf(err, "failed to query for backup files in %q", collection.uri())
	}
	defer rows.Close()
	var ssts []sstInfo
	for rows.Next() {
		var sst sstInfo
		if err := rows.Scan(&sst.path, &sst.start, &sst.end, &sst.bytes, &sst.rows); err != nil {
			return errors.Wrap(err, "error scanning SHOW BACKUP FILES row")
		}
		ssts = append(ssts, sst)
	}
	if rows.Err() != nil {
		return errors.Wrap(rows.Err(), "error iterating over SHOW BACKUP FILES")
	}
	if len(ssts) == 0 {
		return errors.Newf("unexpectedly found no SST files to delete in %q", collection.uri())
	}
	uri, err := url.Parse(collection.uri())
	if err != nil {
		return errors.Wrapf(err, "failed to parse backup collection URI %q", collection.uri())
	}
	storage, err := cloud.ExternalStorageFromURI(
		ctx,
		collection.uri(),
		base.ExternalIODirConfig{},
		clustersettings.MakeTestingClusterSettings(),
		blobs.TestBlobServiceClient(uri.Path),
		username.RootUserName(),
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	if err != nil {
		return errors.Wrapf(err, "failed to create external storage for %q", collection.uri())
	}
	defer storage.Close()
	for _, sst := range ssts {
		l.Printf(
			"deleting sst %s at %s: covering %d bytes and %d rows in [%s, %s)",
			sst.path, uri, sst.bytes, sst.rows, sst.start, sst.end,
		)
		err := renameSSTToDeleted(ctx, l, storage, sst.path)
		if err != nil {
			return errors.Wrapf(
				err, "failed to rename sst %s from backup %s in collection %s",
				sst.path,
				collection.name,
				uri,
			)
		}
	}
	return nil
}

// Restore starts a restore of the given backup collection and returns a
// RestoreJob that can be used to wait for completion and validate the results.
// For cluster level backups, the cluster needs to be wiped before calling
// Restore or else the cluster restore will fail.
func (d *BackupRestoreTestDriver) Restore(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	bc *backupCollection,
	checkFiles bool,
	internalSystemJobs bool,
) (*RestoreJob, error) {
	restoreStmt, restoredTables, restoreDB := d.buildRestoreStatement(bc)
	if _, isTableBackup := bc.btype.(*tableBackup); isTableBackup {
		if err := d.testUtils.Exec(
			ctx, rng, fmt.Sprintf("CREATE DATABASE %s", restoreDB),
		); err != nil {
			return nil, fmt.Errorf(
				"backup %s: error creating database %s: %w", bc.name, restoreDB, err,
			)
		}
	}
	if checkFiles {
		if err := d.testUtils.checkFiles(ctx, rng, bc); err != nil {
			return nil, fmt.Errorf("backup %s: check_files failed: %w", bc.name, err)
		}
	}
	l.Printf("Running restore: %s", restoreStmt)
	var jobID int
	if err := d.testUtils.QueryRow(ctx, rng, restoreStmt).Scan(&jobID); err != nil {
		return nil, fmt.Errorf("backup %s: error in restore statement: %w", bc.name, err)
	}

	return &RestoreJob{
		driver:             d,
		l:                  l,
		rng:                rng,
		bc:                 bc,
		jobID:              jobID,
		restoredTables:     restoredTables,
		internalSystemJobs: internalSystemJobs,
	}, nil
}

// RestoreSync starts a restore and waits for it to complete successfully.
func (d *BackupRestoreTestDriver) RestoreSync(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	bc *backupCollection,
	checkFiles bool,
	internalSystemJobs bool,
) (*RestoreJob, error) {
	rj, err := d.Restore(ctx, l, rng, bc, checkFiles, internalSystemJobs)
	if err != nil {
		return nil, err
	}
	if err := rj.WaitForJobSuccess(ctx); err != nil {
		return nil, err
	}
	return rj, nil
}

type RestoreJob struct {
	driver             *BackupRestoreTestDriver
	l                  *logger.Logger
	rng                *rand.Rand
	bc                 *backupCollection
	jobID              int
	restoredTables     []string
	internalSystemJobs bool
	succeeded          bool
}

func (rj *RestoreJob) ID() int {
	return rj.jobID
}

// WaitForJobSuccess waits for the restore job to complete successfully.
// Returns an error if the job does not succeed.
func (rj *RestoreJob) WaitForJobSuccess(ctx context.Context) error {
	if rj.succeeded {
		return nil
	}

	if err := rj.driver.testUtils.waitForJobSuccess(
		ctx, rj.l, rj.rng, rj.jobID, rj.internalSystemJobs,
	); err != nil {
		return err
	}

	rj.succeeded = true
	return nil
}

// ValidateRestore validates that the contents of the restored tables match
// the contents that were captured when the backup was taken. For an
// asynchronous restore, WaitForJobSuccess must be called successfully before
// ValidateRestore.
func (rj *RestoreJob) ValidateRestore(ctx context.Context) error {
	if !rj.succeeded {
		return errors.New(
			"cannot validate restore: job has not succeeded; call WaitForJobSuccess first",
		)
	}

	restoredContents, err := rj.driver.getRestoredContents(
		ctx, rj.l, rj.rng, rj.restoredTables, rj.bc, rj.internalSystemJobs,
	)
	if err != nil {
		return err
	}

	for j, contents := range rj.bc.contents {
		table := rj.bc.tables[j]
		restoredTableContents := restoredContents[j]
		rj.l.Printf("%s: verifying %s", rj.bc.name, table)
		if err := contents.ValidateRestore(ctx, rj.l, restoredTableContents); err != nil {
			return fmt.Errorf("backup %s: %w", rj.bc.name, err)
		}
	}
	_, db := rj.driver.testUtils.RandomDB(rj.rng, rj.driver.testUtils.roachNodes)
	if err := roachtestutil.CheckInvalidDescriptors(ctx, rj.l, db); err != nil {
		return fmt.Errorf("failed descriptor check: %w", err)
	}

	rj.l.Printf("%s: OK", rj.bc.name)
	return nil
}

func renameSSTToDeleted(
	ctx context.Context, l *logger.Logger, storage cloud.ExternalStorage, sstPath string,
) error {
	dir, basename := path.Split(sstPath)
	basename = "DELETED_" + basename
	deletedPath := path.Join(dir, basename)
	writer, err := storage.Writer(ctx, deletedPath)
	if err != nil {
		return errors.Wrap(err, "opening writer for deleted path")
	}
	defer writer.Close()

	reader, _, err := storage.ReadFile(ctx, sstPath, cloud.ReadOptions{NoFileSize: true})
	if err != nil {
		return errors.Wrapf(err, "opening reader for sst %s", sstPath)
	}
	defer reader.Close(ctx)

	scratch := make([]byte, 512)
	for {
		n, err := reader.Read(ctx, scratch)
		if n > 0 {
			if _, werr := writer.Write(scratch[:n]); werr != nil {
				return errors.Wrap(werr, "writing to deleted sst")
			}
		}

		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "reading from sst")
		}
	}

	return errors.Wrap(storage.Delete(ctx, sstPath), "deleting original sst")
}

// CollectionBuilder provides a stateful builder for constructing backup collections.
//
// This type is NOT thread-safe. All methods should be called from a single goroutine.
//
// Usage:
//
//	builder := driver.NewCollectionBuilder(l, tasker, rng, namePrefix, fullSpec, incSpec, internalSystemJobs, isMultitenant)
//	jobID, err := builder.TakeFull(ctx)
//	incJobID, err := builder.TakeInc(ctx)
//	collection, err := builder.Finalize(ctx)
type CollectionBuilder struct {
	// Immutable configuration set at construction.
	driver             *BackupRestoreTestDriver
	l                  *logger.Logger
	tasker             task.Tasker
	rng                *rand.Rand
	namePrefix         string
	fullBackupSpec     backupSpec
	incBackupSpec      backupSpec
	internalSystemJobs bool
	isMultitenant      bool
	cfg                collCfg

	// Mutable state tracking backup progress.

	// finalized indicates whether Finalize() has been called.
	// Once true, no further backups can be taken.
	finalized bool

	// fullStarted indicates whether a full backup has been started.
	fullStarted bool

	// collection holds the backup metadata, initialized after full backup.
	collection backupCollection

	// backupEndTimes tracks the end time of each backup in order:
	// [0] = full backup end time
	// [1] = first incremental end time
	// etc.
	backupEndTimes []string

	// incCount tracks the number of incremental backups taken.
	incCount int

	lastJobID int
}

// NewCollectionBuilder creates a new builder for constructing a backup collection.
func (d *BackupRestoreTestDriver) NewCollectionBuilder(
	l *logger.Logger,
	tasker task.Tasker,
	rng *rand.Rand,
	namePrefix string,
	fullBackupSpec backupSpec,
	incBackupSpec backupSpec,
	internalSystemJobs bool,
	isMultitenant bool,
	cfgs ...CollectionConfig,
) *CollectionBuilder {
	cb := &CollectionBuilder{
		driver:             d,
		l:                  l,
		tasker:             tasker,
		rng:                rng,
		namePrefix:         namePrefix,
		fullBackupSpec:     fullBackupSpec,
		incBackupSpec:      incBackupSpec,
		internalSystemJobs: internalSystemJobs,
		isMultitenant:      isMultitenant,
	}
	cb.initCollCfg(cfgs...)
	return cb
}

// WaitForLastJob waits for the last backup job to complete.
// This must be called after every async backup call (TakeFull, TakeInc, TakeCompacted)
// before starting another backup, as the builder does not support multiple backup jobs
// running concurrently.
func (cb *CollectionBuilder) WaitForLastJob(ctx context.Context) error {
	if cb.lastJobID == 0 {
		return nil
	}

	if err := cb.driver.testUtils.waitForJobSuccess(
		ctx, cb.l, cb.rng, cb.lastJobID, cb.internalSystemJobs,
	); err != nil {
		return err
	}

	cb.lastJobID = 0
	return nil
}

// TakeFull creates a full backup and returns immediately without waiting for completion.
func (cb *CollectionBuilder) TakeFull(ctx context.Context) (jobID int, err error) {
	if cb.finalized {
		return 0, errors.New("cannot take backup: builder has been finalized")
	}
	if cb.fullStarted {
		return 0, errors.New("full backup already started for this collection")
	}

	var collection backupCollection
	var endTime string
	if err := cb.driver.testUtils.runJobOnOneOf(
		ctx, cb.l, cb.fullBackupSpec.Execute.Nodes, func() error {
			var execErr error
			collection, endTime, jobID, execErr = cb.executeBackup(
				ctx,
				cb.fullBackupSpec.Plan.Nodes,
				cb.fullBackupSpec.PauseProbability,
				fullBackup{},
			)
			return execErr
		},
	); err != nil {
		return 0, err
	}

	cb.fullStarted = true
	cb.collection = collection
	cb.backupEndTimes = []string{endTime}
	cb.lastJobID = jobID

	return jobID, nil
}

// TakeFullSync creates a full backup and waits for it to complete.
func (cb *CollectionBuilder) TakeFullSync(ctx context.Context) (jobID int, err error) {
	jobID, err = cb.TakeFull(ctx)
	if err != nil {
		return 0, err
	}
	return jobID, cb.WaitForLastJob(ctx)
}

// TakeInc creates an incremental backup and returns immediately without waiting for completion.
func (cb *CollectionBuilder) TakeInc(ctx context.Context) (jobID int, err error) {
	if cb.finalized {
		return 0, errors.New("cannot take backup: builder has been finalized")
	}
	if !cb.fullStarted {
		return 0, errors.New("must start full backup before incremental backup")
	}
	if cb.lastJobID != 0 {
		return 0, errors.Newf("cannot start incremental backup: job %d still running", cb.lastJobID)
	}

	incNum := cb.incCount + 1
	var endTime string
	if err := cb.driver.testUtils.runJobOnOneOf(
		ctx, cb.l, cb.incBackupSpec.Execute.Nodes, func() error {
			var execErr error
			cb.collection, endTime, jobID, execErr = cb.executeBackup(
				ctx,
				cb.incBackupSpec.Plan.Nodes,
				cb.incBackupSpec.PauseProbability,
				incrementalBackup{collection: cb.collection, incNum: incNum},
			)
			return execErr
		},
	); err != nil {
		return 0, err
	}

	cb.incCount = incNum
	cb.backupEndTimes = append(cb.backupEndTimes, endTime)
	cb.lastJobID = jobID

	return jobID, nil
}

// TakeIncSync creates an incremental backup and waits for it to complete.
func (cb *CollectionBuilder) TakeIncSync(ctx context.Context) (jobID int, err error) {
	jobID, err = cb.TakeInc(ctx)
	if err != nil {
		return 0, err
	}
	return jobID, cb.WaitForLastJob(ctx)
}

// TakeCompacted creates a compacted backup and returns immediately without waiting for completion.
// The indices refer to positions in the backup chain (0 = full, 1 = first inc, etc.).
// NB: startIdx must be >= 1 since the full backup (index 0) cannot be compacted.
func (cb *CollectionBuilder) TakeCompacted(
	ctx context.Context, startIdx, endIdx int,
) (jobID int, err error) {
	if cb.finalized {
		return 0, errors.New("cannot take backup: builder has been finalized")
	}
	if !cb.fullStarted {
		return 0, errors.New("must take full backup before compacted backup")
	}
	if cb.lastJobID != 0 {
		return 0, errors.Newf("cannot start compacted backup: job %d still running", cb.lastJobID)
	}
	if startIdx <= 0 || endIdx >= len(cb.backupEndTimes) {
		return 0, errors.Newf(
			"invalid compaction range [%d, %d]: must be in range [1, %d] (full backup at index 0 cannot be compacted)",
			startIdx, endIdx, len(cb.backupEndTimes)-1,
		)
	}
	if startIdx >= endIdx {
		return 0, errors.Newf(
			"invalid compaction range [%d, %d]: startIdx must be < endIdx (range is inclusive)",
			startIdx, endIdx,
		)
	}

	var fullPath string
	_, db := cb.driver.testUtils.RandomDB(cb.rng, cb.driver.roachNodes)
	row := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT path FROM [SHOW BACKUPS IN '%s'] ORDER BY path DESC LIMIT 1`,
		cb.collection.uri(),
	))
	if err := row.Scan(&fullPath); err != nil {
		return 0, errors.Wrapf(err, "error while getting full backup path for %s", cb.collection.name)
	}

	compact := compactedBackup{
		collection: cb.collection,
		// Compacted backup spans from end of previous backup to end of last backup.
		// Example: backups [full=0, inc1=1, inc2=2] with times [t0, t1, t2]
		// Compacting [1,2] spans [t0, t2] where t0 = endTimes[0], t2 = endTimes[2]
		startTime:  cb.backupEndTimes[startIdx-1],
		endTime:    cb.backupEndTimes[endIdx],
		fullSubdir: fullPath,
	}

	if err := cb.driver.testUtils.runJobOnOneOf(
		ctx, cb.l, cb.incBackupSpec.Execute.Nodes, func() error {
			var execErr error
			cb.collection, _, jobID, execErr = cb.executeBackup(
				ctx,
				cb.incBackupSpec.Plan.Nodes,
				cb.incBackupSpec.PauseProbability,
				compact,
			)
			return execErr
		},
	); err != nil {
		return 0, err
	}

	// Since a compacted backup was made, then the backup end times of the
	// backups it compacted should be removed from the slice. This prevents a
	// scenario where a later compaction attempts to pick a start time from
	// one of the backups that were compacted. Since compaction looks at the
	// elided backup chain, these compacted backups are replaced by the
	// compacted backup and compaction will fail due to being unable to find
	// the starting backup.
	cb.backupEndTimes = slices.Delete(cb.backupEndTimes, startIdx, endIdx)
	cb.lastJobID = jobID

	return jobID, nil
}

// TakeCompactedSync creates a compacted backup and waits for it to complete.
// The indices refer to positions in the backup chain (0 = full, 1 = first inc, etc.).
// NB: startIdx must be >= 1 since the full backup (index 0) cannot be compacted.
func (cb *CollectionBuilder) TakeCompactedSync(
	ctx context.Context, startIdx, endIdx int,
) (jobID int, err error) {
	jobID, err = cb.TakeCompacted(ctx, startIdx, endIdx)
	if err != nil {
		return 0, err
	}
	return jobID, cb.WaitForLastJob(ctx)
}

// BackupCount returns the shortest length of the shortest chain to the latest
// end time in the backup collection.
func (cb *CollectionBuilder) BackupCount() int {
	return len(cb.backupEndTimes)
}

// Finalize completes the backup collection by:
//  1. Optionally selecting a restore AOST
//  2. Computing and saving table fingerprints
//
// After calling Finalize(), no further backups can be taken with this builder.
func (cb *CollectionBuilder) Finalize(ctx context.Context) (*backupCollection, error) {
	if cb.finalized {
		return nil, errors.New("builder has already been finalized")
	}
	if !cb.fullStarted {
		return nil, errors.New("cannot finalize: no full backup taken")
	}

	if err := cb.WaitForLastJob(ctx); err != nil {
		return nil, err
	}

	cb.finalized = true

	collection := cb.collection
	fullBackupEndTime := cb.backupEndTimes[0]
	var latestIncBackupEndTime string
	if len(cb.backupEndTimes) > 1 {
		latestIncBackupEndTime = cb.backupEndTimes[len(cb.backupEndTimes)-1]
	}

	if err := collection.maybeUseRestoreAOST(
		cb.l, cb.rng, fullBackupEndTime, latestIncBackupEndTime,
	); err != nil {
		return nil, err
	}

	fingerprintAOST := latestIncBackupEndTime
	if fingerprintAOST == "" {
		fingerprintAOST = fullBackupEndTime
	}
	if collection.restoreAOST != "" {
		fingerprintAOST = collection.restoreAOST
	}

	return cb.driver.saveContents(ctx, cb.l, cb.rng, &collection, fingerprintAOST)
}

// backupCollectionName creates a backup collection name based on the prefix
// and the target scope of the backup. Uniqueness is provided by the nonce in
// the backupCollection struct.
func (cb *CollectionBuilder) backupCollectionName(btype backupScope) string {
	sanitizedType := strings.ReplaceAll(btype.Desc(), " ", "-")
	return fmt.Sprintf("%s_%s", cb.namePrefix, sanitizedType)
}

// executeBackup executes a backup and returns the collection, end time, and jobID.
//
// Note: This method must be called within runJobOnOneOf to handle node selection.
func (cb *CollectionBuilder) executeBackup(
	ctx context.Context,
	planNodes option.NodeListOption,
	pauseProbability float64,
	bType fmt.Stringer,
) (backupCollection, string, int, error) {
	// NB: we need to run with the `detached` option + poll the
	// `system.jobs` table because we are intentionally disabling job
	// adoption in some nodes in the mixed-version test. Running the job
	// without the `detached` option will cause a `node liveness` error
	// to be returned when running the `BACKUP` statement.
	options := []string{"detached"}

	var latest string
	var collection backupCollection
	switch b := bType.(type) {
	case fullBackup:
		name := cb.backupCollectionName(cb.cfg.scope)
		collection = newBackupCollection(name, cb.cfg.scope, cb.cfg.options, cb.driver.cluster.IsLocal())
		cb.l.Printf("creating full backup for %s", collection.name)
	case incrementalBackup:
		collection = b.collection
		latest = " LATEST IN"
		cb.l.Printf("creating incremental backup num %d for %s", b.incNum, collection.name)
	case compactedBackup:
		collection = b.collection
		latest = " LATEST IN"
		cb.l.Printf(
			"creating compacted backup for %s from start %s to end %s",
			collection.name, b.startTime, b.endTime,
		)
	}

	for _, opt := range collection.options {
		options = append(options, opt.String())
	}

	backupTime := cb.driver.testUtils.now()
	node, db := cb.driver.testUtils.RandomDB(cb.rng, planNodes)

	stmt := fmt.Sprintf(
		"%s INTO%s '%s' AS OF SYSTEM TIME '%s' WITH %s",
		collection.btype.BackupCommand(),
		latest,
		collection.uri(),
		backupTime,
		strings.Join(options, ", "),
	)

	var jobID int
	if compactData, ok := bType.(compactedBackup); ok {
		backupTime = compactData.endTime
		if err := db.QueryRowContext(ctx,
			`SELECT crdb_internal.backup_compaction(0, $1, $2, $3::DECIMAL, $4::DECIMAL)`,
			stmt, compactData.fullSubdir, compactData.startTime, compactData.endTime,
		).Scan(&jobID); err != nil {
			return backupCollection{}, "", 0, errors.Wrapf(
				err, "error while creating %s compacted backup %s", bType, collection.name,
			)
		}
	} else {
		cb.l.Printf("creating %s backup via node %d: %s", bType, node, stmt)
		if err := db.QueryRowContext(ctx, stmt).Scan(&jobID); err != nil {
			return backupCollection{}, "", 0, errors.Wrapf(
				err, "error while creating %s backup %s", bType, collection.name,
			)
		}
	}
	cb.l.Printf("started %s backup job %d for %s", bType, jobID, collection.name)

	cb.maybePauseResumeLoop(ctx, cb.l, jobID, pauseProbability)
	return collection, backupTime, jobID, nil
}

// maybePauseResumeLoop randomly decides whether to start a pause/resume loop on
// the given job ID.
func (cb *CollectionBuilder) maybePauseResumeLoop(
	ctx context.Context, l *logger.Logger, jobID int, pauseProbability float64,
) {
	if cb.rng.Float64() >= pauseProbability {
		return
	}
	possibleDurations := []time.Duration{
		10 * time.Second, 30 * time.Second, 2 * time.Minute,
	}
	pauseAfter := possibleDurations[cb.rng.Intn(len(possibleDurations))]

	var node int
	node, db := cb.driver.testUtils.RandomDB(cb.rng, cb.driver.roachNodes)
	cb.l.Printf("attempting pauses in %s through node %d", pauseAfter, node)

	cb.tasker.Go(
		func(ctx context.Context, l *logger.Logger) error {
			var numPauses int
			ticker := time.NewTicker(pauseAfter)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
					if numPauses >= maxPauses {
						continue
					}

					var status string
					if err := db.QueryRow(`SELECT status FROM [SHOW JOB $1]`, jobID).Scan(&status); err != nil {
						l.Printf("error querying job %d status: %s", jobID, err)
						continue
					}
					if jobs.State(status) == jobs.StateFailed || jobs.State(status) == jobs.StateSucceeded {
						return nil
					}

					pauseDur := 5 * time.Second
					l.Printf("pausing job %d for %s", jobID, pauseDur)
					if _, err := db.Exec(fmt.Sprintf("PAUSE JOB %d", jobID)); err != nil {
						l.Printf("error pausing job %d: %s", jobID, err)
						continue
					}

					if err := testutils.SucceedsSoonError(func() error {
						var status string
						if err := db.QueryRow(`SELECT status FROM [SHOW JOB $1]`, jobID).Scan(&status); err != nil {
							return err
						}
						if status != "paused" {
							return errors.Newf("expected status `paused` but found %s", status)
						}
						return nil
					}); err != nil {
						return err
					}

					time.Sleep(pauseDur)

					l.Printf("resuming job %d", jobID)
					if _, err := db.Exec(fmt.Sprintf("RESUME JOB %d", jobID)); err != nil {
						return errors.Wrapf(err, "error resuming job %d", jobID)
					}

					numPauses++
				}
			}
		},
		task.Logger(cb.l),
		task.Name(fmt.Sprintf("pause-resume monitor for job %d", jobID)),
	)
}

// newBackupScope chooses a random backup type (table, database, cluster) with
// equal probability.
func (cb *CollectionBuilder) newBackupScope(isMultitenant bool) backupScope {
	possibleTypes := []backupScope{
		newTableBackup(cb.rng, cb.driver.dbs, cb.driver.tables),
		newDatabaseBackup(cb.rng, cb.driver.dbs, cb.driver.tables),
	}
	if !cb.driver.cluster.IsLocal() && !isMultitenant {
		// Cluster backups cannot be restored on nodelocal because the
		// cluster is wiped before cluster restore. Cluster restores in
		// multitenant deployments are also not supported by this test at
		// the moment.
		possibleTypes = append(
			possibleTypes, newClusterBackup(cb.rng, cb.driver.dbs, cb.driver.tables),
		)
	}

	return possibleTypes[cb.rng.Intn(len(possibleTypes))]
}

// initCollCfg applies the provided backup collection configuration
// along with defaults for any missing configuration.
func (cb *CollectionBuilder) initCollCfg(cfgs ...CollectionConfig) {
	for _, c := range cfgs {
		c(cb)
	}

	// Set defaults
	if cb.cfg.scope == nil {
		cb.cfg.scope = cb.newBackupScope(cb.isMultitenant)
	}

	if cb.cfg.options == nil {
		cb.cfg.options = newBackupOptions(cb.rng, cb.driver.testUtils.onlineRestore)
	}
}

// collCfg holds the backup configuration for the backup collection and is used
// to build the BACKUP statement when taking backups.
type collCfg struct {
	scope   backupScope
	options []backupOption
}

type CollectionConfig func(*CollectionBuilder)

func WithClusterScope() CollectionConfig {
	return func(cb *CollectionBuilder) {
		cb.cfg.scope = newClusterBackup(cb.rng, cb.driver.dbs, cb.driver.tables)
	}
}

type (
	// backupOption is an option passed to the `BACKUP` command (i.e.,
	// `WITH ...` portion).
	backupOption interface {
		fmt.Stringer
	}

	// revisionHistory wraps the `revision_history` backup option.
	revisionHistory struct{}

	// encryptionPassphrase is the `encryption_passphrase` backup
	// option. If passed when a backup is created, the same passphrase
	// needs to be passed for incremental backups and for restoring the
	// backup as well.
	encryptionPassphrase struct {
		passphrase string
	}

	// metamorphicSetting encodes the space of possible values a cluster
	// setting may assume during this test, along with the scope where
	// it applies (system or tenant).
	metamorphicSetting struct {
		Scope  string
		Values []string
	}

	// backupScope is the interface to be implemented by each backup scope
	// (table, database, cluster).
	backupScope interface {
		// Desc returns a string describing the backup type, and is used
		// when creating the name for a backup collection.
		Desc() string
		// BackupCommand returns the `BACKUP {target}` part of the backup
		// command used by the test to create the backup, up until the
		// `INTO` keyword.
		BackupCommand() string
		// RestoreCommand returns the `RESTORE {target}` part of the
		// restore command used by the test to restore a backup, up until
		// the `FROM` keyword. This function can also return any options
		// to be used when restoring. The string parameter is the name of
		// a unique database created by the test harness that restores
		// should use to restore their data, if applicable.
		RestoreCommand(string) (string, []string)
		// TargetTables returns a list of tables that are part of the
		// backup collection. These tables will be verified when the
		// backup is restored, and they should have the same contents as
		// they did when the backup was taken.
		TargetTables() []string
	}

	// tableBackup -- BACKUP TABLE ...
	tableBackup struct {
		db     string
		target string
	}

	// databaseBackup -- BACKUP DATABASE ...
	databaseBackup struct {
		db     string
		tables []string
	}

	// clusterBackup -- BACKUP ...
	clusterBackup struct {
		dbBackups    []*databaseBackup
		systemTables []string
	}

	// tableContents is an interface to be implemented by different
	// methods of storing and comparing the contents of a table. A lot
	// of backup-related tests use the `EXPERIMENTAL_FINGERPRINTS`
	// functionality to cheaply compare the contents of a table before a
	// backup and after restore; however, the semantics of system tables
	// does not allow for that comparison as changes in system table
	// contents after an upgrade are expected.
	tableContents interface {
		// Load computes the contents of a table with the given name
		// (passed as argument) and stores its representation internally.
		// If the contents of the table are being loaded from a restore,
		// the corresponding `tableContents` are passed as parameter as
		// well (`nil` otherwise).
		Load(context.Context, *logger.Logger, string, tableContents) error
		// ValidateRestore validates that a restored table with contents
		// passed as argument is valid according to the table contents
		// previously `Load`ed.
		ValidateRestore(context.Context, *logger.Logger, tableContents) error
	}

	// fingerprintContents implements the `tableContents` interface
	// using the `EXPERIMENTAL_FINGERPRINTS` functionality. This should
	// be the implementation for all user tables.
	fingerprintContents struct {
		db           *gosql.DB
		table        string
		fingerprints string
	}

	// showSystemResults stores the results of a SHOW statement when the
	// contents of a system table are inspected. Both the specific query
	// used and the output are stored.
	showSystemResults struct {
		query  string
		output string
	}

	// systemTableRow encapsulates the contents of a row in a system
	// table and provides a more declarative API for the test to
	// override certain columns with sentinel values when we cannot
	// expect the restored value to match the value at the time the
	// backup was taken.
	systemTableRow struct {
		table   string
		values  []interface{}
		columns []string
		matches bool
		err     error
	}

	// systemTableContents implements the `tableContents` interface for
	// system tables. Since the contents and the schema of system tables
	// may change as the database upgrades, we can't use fingerprints.
	// Instead, this struct validates that any data that existed in a
	// system table at the time of the backup should continue to exist
	// when the backup is restored.
	systemTableContents struct {
		cluster     cluster.Cluster
		roachNode   int
		db          *gosql.DB
		table       string
		columns     []string
		rows        map[string]struct{}
		showResults *showSystemResults
	}

	// backupCollection wraps a backup collection (which may or may not
	// contain incremental backups). The associated fingerprint is the
	// expected fingerprint when the corresponding table is restored.
	backupCollection struct {
		btype   backupScope
		name    string
		options []backupOption
		nonce   string

		// tables is the list of tables that we are going to verify once
		// this backup is restored. There should be a 1:1 mapping between
		// the table names in `tables` and the `tableContents` implementations
		// in the `contents` field.
		tables   []string
		contents []tableContents

		// restoreAOST contains the AOST used in restore, if non-empty. It also
		// determines the system time used to grab fingerprints.
		restoreAOST string

		nodelocal bool
	}

	fullBackup struct{}

	incrementalBackup struct {
		collection backupCollection
		incNum     int
	}

	compactedBackup struct {
		collection backupCollection
		startTime  string
		endTime    string

		fullSubdir string
	}

	// labeledNodes allows us to label a set of nodes with the version
	// they are running, to allow for human-readable backup names
	labeledNodes struct {
		Nodes   option.NodeListOption
		Version string
	}

	// backupSpec indicates where backups are supposed to be planned
	// (`BACKUP` statement sent to); and where they are supposed to be
	// executed (where the backup job will be picked up).
	backupSpec struct {
		PauseProbability float64
		Plan             labeledNodes
		Execute          labeledNodes
	}
)

func newBackupCollection(
	name string, btype backupScope, options []backupOption, nodelocal bool,
) backupCollection {
	// Use a different seed for generating the collection's nonce to
	// allow for multiple concurrent runs of this test using the same
	// COCKROACH_RANDOM_SEED, making it easier to reproduce failures
	// that are more likely to occur with certain test plans.
	nonceRng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	return backupCollection{
		btype:     btype,
		name:      name,
		tables:    btype.TargetTables(),
		options:   options,
		nonce:     randString(nonceRng, nonceLen),
		nodelocal: nodelocal,
	}
}

func (bc *backupCollection) uri() string {
	// Append the `nonce` to the backup name since we are now sharing a
	// global namespace represented by the BACKUP_TESTING_BUCKET
	// bucket. The nonce allows multiple people (or TeamCity builds) to
	// be running this test without interfering with one another.
	externalStorage := "gs://"
	if bc.nodelocal {
		externalStorage = "nodelocal://1/"
	}
	return fmt.Sprintf("%s%s/mixed-version/%s_%s?AUTH=implicit", externalStorage, testutils.BackupTestingBucketLongTTL(), bc.name, bc.nonce)
}

func (bc *backupCollection) encryptionOption() *encryptionPassphrase {
	for _, option := range bc.options {
		if ep, ok := option.(encryptionPassphrase); ok {
			return &ep
		}
	}

	return nil
}

func (bc *backupCollection) withRevisionHistory() bool {
	for _, option := range bc.options {
		if _, ok := option.(revisionHistory); ok {
			return true
		}
	}
	return false
}

// maybeUseRestoreAOST potentially picks a restore AOST between the
// full backup end time and the last incremental backup end time.
//
// We don't bother choosing an AOST before the full backup endtime since the
// restore may fail. We lack good observability for choosing a valid AOST within
// the revision history full backup.
func (bc *backupCollection) maybeUseRestoreAOST(
	l *logger.Logger, rng *rand.Rand, fullBackupEndTime, lastBackupEndTime string,
) error {
	// TODO(msbutler): pick AOST restore for non revision history backups by
	// randomly choosing a backup end time.
	if !bc.withRevisionHistory() || rng.Float64() > restoreFromAOSTProbability {
		return nil
	}

	parseAOST := func(aost string) (hlc.Timestamp, error) {
		d, _, err := apd.NewFromString(aost)
		if err != nil {
			return hlc.Timestamp{}, err
		}
		ts, err := hlc.DecimalToHLC(d)
		if err != nil {
			return hlc.Timestamp{}, err
		}
		return ts, nil
	}

	min, err := parseAOST(fullBackupEndTime)
	if err != nil {
		return err
	}
	max, err := parseAOST(lastBackupEndTime)
	if err != nil {
		return err
	}

	// Choose a random AOST between min and max with the following approach:
	// divide the interval between min and max over 100 bins and randomly choose a
	// bin. Randomly choosing a bin is more reproducible than randomly picking a
	// time between min and max.
	interval := max.WallTime - min.WallTime
	binCount := int64(100)
	bin := rng.Int63n(binCount)

	restoreAOST := hlc.Timestamp{
		WallTime: (bin*interval)/binCount + min.WallTime,
	}

	l.Printf("preparing for an AOST restore at %s, between full backup end time %s and last incremental backup end time %s", restoreAOST.GoTime(), min.GoTime(), max.GoTime())
	bc.restoreAOST = restoreAOST.AsOfSystemTime()
	return nil
}

// backupCollectionDesc builds a string that describes how a backup
// collection comprised of a full backup and a follow-up incremental
// backup was generated (in terms of which versions planned vs
// executed the backup). Used to generate descriptive backup names.
func backupCollectionDesc(fullSpec, incSpec backupSpec) string {
	specMsg := func(label string, s backupSpec) string {
		if s.Plan.Version == s.Execute.Version {
			return fmt.Sprintf("%s planned and executed on %s", label, s.Plan.Version)
		}

		return fmt.Sprintf("%s planned on %s executed on %s", label, s.Plan.Version, s.Execute.Version)
	}

	if reflect.DeepEqual(fullSpec, incSpec) {
		return specMsg("all", fullSpec)
	}

	return fmt.Sprintf("%s %s", specMsg("full", fullSpec), specMsg("incremental", incSpec))
}

func (fb fullBackup) String() string        { return "full" }
func (ib incrementalBackup) String() string { return "incremental" }
func (cb compactedBackup) String() string   { return "compacted" }

func (rh revisionHistory) String() string {
	return "revision_history"
}

// newBackupOptions returns a list of backup options to be used when
// creating a new backup. Each backup option has a 50% chance of being
// included.
func newBackupOptions(rng *rand.Rand, onlineRestoreExpected bool) []backupOption {
	possibleOpts := []backupOption{}
	if !onlineRestoreExpected {
		possibleOpts = append(possibleOpts, revisionHistory{})
		possibleOpts = append(possibleOpts, newEncryptionPassphrase(rng))
	}

	var options []backupOption
	for _, opt := range possibleOpts {
		if rng.Float64() < 0.5 {
			options = append(options, opt)
		}
	}

	return options
}

func newEncryptionPassphrase(rng *rand.Rand) encryptionPassphrase {
	return encryptionPassphrase{randString(rng, randIntBetween(rng, 32, 64))}
}

func (ep encryptionPassphrase) String() string {
	return fmt.Sprintf("encryption_passphrase = '%s'", ep.passphrase)
}

func newTableBackup(rng *rand.Rand, dbs []string, tables [][]string) *tableBackup {
	var targetDBIdx int
	var targetDB string
	// Avoid creating table backups for the tpcc database, as they often
	// have foreign keys to other tables, making restoring them
	// difficult. We could pass the `skip_missing_foreign_keys` option,
	// but that would be a less interesting test.
	//
	// Avoid creating table backups for the schemachange database, as it inits with 0 tables.
	for targetDB == "" || targetDB == "tpcc" || targetDB == schemaChangeDB {
		targetDBIdx = rng.Intn(len(dbs))
		targetDB = dbs[targetDBIdx]
	}

	dbTables := tables[targetDBIdx]
	targetTable := dbTables[rng.Intn(len(dbTables))]
	return &tableBackup{dbs[targetDBIdx], targetTable}
}

func (tb *tableBackup) Desc() string {
	return fmt.Sprintf("table %s.%s", tb.db, tb.target)
}

func (tb *tableBackup) BackupCommand() string {
	return fmt.Sprintf("BACKUP TABLE %s", tb.TargetTables()[0])
}

func (tb *tableBackup) RestoreCommand(restoreDB string) (string, []string) {
	options := []string{fmt.Sprintf("into_db = '%s'", restoreDB)}
	return fmt.Sprintf("RESTORE TABLE %s", tb.TargetTables()[0]), options
}

func (tb *tableBackup) TargetTables() []string {
	return []string{fmt.Sprintf("%s.%s", tb.db, tb.target)}
}

func newDatabaseBackup(rng *rand.Rand, dbs []string, tables [][]string) *databaseBackup {
	targetDBIdx := rng.Intn(len(dbs))
	return &databaseBackup{dbs[targetDBIdx], tables[targetDBIdx]}
}

func (dbb *databaseBackup) Desc() string {
	return fmt.Sprintf("database %s", dbb.db)
}

func (dbb *databaseBackup) BackupCommand() string {
	return fmt.Sprintf("BACKUP DATABASE %s", dbb.db)
}

func (dbb *databaseBackup) RestoreCommand(restoreDB string) (string, []string) {
	options := []string{fmt.Sprintf("new_db_name = '%s'", restoreDB)}
	return fmt.Sprintf("RESTORE DATABASE %s", dbb.db), options
}

func (dbb *databaseBackup) TargetTables() []string {
	return tableNamesWithDB(dbb.db, dbb.tables)
}

func newClusterBackup(rng *rand.Rand, dbs []string, tables [][]string) *clusterBackup {
	dbBackups := make([]*databaseBackup, 0, len(dbs))
	for j, db := range dbs {
		dbBackups = append(dbBackups, newDatabaseBackup(rng, []string{db}, [][]string{tables[j]}))
	}

	return &clusterBackup{
		dbBackups:    dbBackups,
		systemTables: systemTablesInFullClusterBackup,
	}
}

func (cb *clusterBackup) Desc() string                               { return "cluster" }
func (cb *clusterBackup) BackupCommand() string                      { return "BACKUP" }
func (cb *clusterBackup) RestoreCommand(_ string) (string, []string) { return "RESTORE", nil }

func (cb *clusterBackup) TargetTables() []string {
	var dbTargetTables []string
	for _, dbb := range cb.dbBackups {
		dbTargetTables = append(dbTargetTables, dbb.TargetTables()...)
	}
	return append(dbTargetTables, tableNamesWithDB("system", cb.systemTables)...)
}

func newFingerprintContents(db *gosql.DB, table string) *fingerprintContents {
	return &fingerprintContents{db: db, table: table}
}

// Load computes the fingerprints for the underlying table and stores the
// contents in the `fingeprints` field. If timestamp is not set, computes
// the fingerprint for the current time.
func (fc *fingerprintContents) Load(
	ctx context.Context, l *logger.Logger, timestamp string, _ tableContents,
) error {
	l.Printf("computing fingerprints for table %s", fc.table)
	query := fmt.Sprintf(
		"SELECT index_name, COALESCE(fingerprint, '') FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s]%s ORDER BY index_name",
		fc.table, aostFor(timestamp),
	)
	rows, err := fc.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("error when running query [%s]: %w", query, err)
	}
	defer rows.Close()

	var fprints []string
	for rows.Next() {
		var indexName, fprint string
		if err := rows.Scan(&indexName, &fprint); err != nil {
			return fmt.Errorf("error computing fingerprint for table %s, index %s: %w", fc.table, indexName, err)
		}

		fprints = append(fprints, fmt.Sprintf("%s:%s", indexName, fprint /* actualFingerprint */))
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating over fingerprint rows for table %s: %w", fc.table, err)
	}

	fc.fingerprints = strings.Join(fprints, "\n")
	return nil
}

// ValidateRestore compares the fingerprints associated with the given
// restored contents (which are assumed to be
// `fingerprintContents`). Returns an error if the fingerprints don't
// match.
func (fc *fingerprintContents) ValidateRestore(
	ctx context.Context, l *logger.Logger, contents tableContents,
) error {
	restoredContents := contents.(*fingerprintContents)
	if fc.fingerprints != restoredContents.fingerprints {
		l.Printf(
			"mismatched fingerprints for table %s\n--- Expected:\n%s\n--- Actual:\n%s",
			fc.table, fc.fingerprints, restoredContents.fingerprints,
		)
		return fmt.Errorf("mismatched fingerprints for table %s", fc.table)
	}

	return nil
}

func aostFor(timestamp string) string {
	var stmt string
	if timestamp != "" {
		stmt = fmt.Sprintf(" AS OF SYSTEM TIME '%s'", timestamp)
	}

	return stmt
}

// quoteColumnsForSelect quotes every column passed as parameter and
// returns a string that can be passed to a `SELECT` statement. Since
// we are dynamically loading the columns for a number of system
// tables, we need to quote the column names to avoid SQL syntax
// errors.
func quoteColumnsForSelect(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, c := range columns {
		quoted = append(quoted, fmt.Sprintf("%q", c))
	}

	return strings.Join(quoted, ", ")
}

// tableNamesWithDB returns a list of qualified table names where the
// database name is `db`. This can be useful in situations where we
// backup a "bank.bank" table, for example, and then restore it into a
// "some_db" database. The table name in the restored database would
// be "some_db.bank".
//
// Example:
//
//	tableNamesWithDB("restored_db", []string{"bank.bank", "stock"})
//	=> []string{"restored_db.bank", "restored_db.stock"}
func tableNamesWithDB(db string, tables []string) []string {
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		parts := strings.Split(t, ".")
		var tableName string
		if len(parts) == 1 {
			tableName = parts[0]
		} else {
			tableName = parts[1]
		}

		names = append(names, fmt.Sprintf("%s.%s", db, tableName))
	}

	return names
}

func newSystemTableRow(table string, values []interface{}, columns []string) *systemTableRow {
	return &systemTableRow{table: table, values: values, columns: columns, matches: true}
}

// skip determines whether we should continue applying filters or
// replacing columns value in an instance of systemTableRow. `matches`
// will be false if the caller used `Matches` and the column value did
// not match. `err` includes any error found while performing changes
// in this system table row, in which case the error should be
// returned to the caller.
func (sr *systemTableRow) skip() bool {
	return !sr.matches || sr.err != nil
}

// processColumn takes a column name and a callback function; the
// callback will be passed the value associated with that column, if
// found, and its return value overwrites the value for that
// column. The callback is not called if the column name is invalid.
func (sr *systemTableRow) processColumn(
	column string, skipMissing bool, fn func(interface{}) interface{},
) {
	if sr.skip() {
		return
	}

	colIdx := sort.SearchStrings(sr.columns, column)
	hasCol := colIdx < len(sr.columns) && sr.columns[colIdx] == column

	if !hasCol {
		if skipMissing {
			return
		}
		sr.err = fmt.Errorf("could not find column %s on %s", column, sr.table)
		return
	}

	sr.values[colIdx] = fn(sr.values[colIdx])
}

// Matches allows the caller to only apply certain changes if the
// value of a certain column matches a given value.
func (sr *systemTableRow) Matches(column string, value interface{}) *systemTableRow {
	sr.processColumn(column, false /*skipMissing */, func(actualValue interface{}) interface{} {
		sr.matches = reflect.DeepEqual(actualValue, value)
		return actualValue
	})

	return sr
}

// WithSentinelIfExists replaces the contents of the given columns with a
// fixed sentinel value, if the column exists.
func (sr *systemTableRow) WithSentinelIfExists(columns ...string) *systemTableRow {
	for _, column := range columns {
		sr.processColumn(column, true /* skipMissing */, func(value interface{}) interface{} {
			return systemTableSentinel
		})
	}

	return sr
}

// Delete marks any matched rows as deleted. When loading system table
// values, this will cause the corresponding row to not be loaded.
func (sr *systemTableRow) Delete() *systemTableRow {
	if sr.skip() {
		return sr
	}

	sr.values = nil
	return sr
}

// Values must be called when all column manipulations have been
// made. It returns the final set of values to be used for the system
// table row, and any error found along the way.
func (sr *systemTableRow) Values() ([]interface{}, error) {
	return sr.values, sr.err
}

func newSystemTableContents(
	ctx context.Context, c cluster.Cluster, node int, db *gosql.DB, name, timestamp string,
) (*systemTableContents, error) {
	// Dynamically load column names for the corresponding system
	// table. We use an AOST clause as this may be happening while
	// upgrade migrations are in-flight (which may change the schema of
	// system tables).
	query := fmt.Sprintf(
		"SELECT column_name FROM [SHOW COLUMNS FROM %s]%s ORDER BY column_name",
		name, aostFor(timestamp),
	)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error querying column names for %s: %w", name, err)
	}

	var columnNames []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("error scanning column_name for table %s: %w", name, err)
		}

		columnNames = append(columnNames, colName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over columns of %s: %w", name, err)
	}

	return &systemTableContents{
		cluster: c, db: db, roachNode: node, table: name, columns: columnNames,
	}, nil
}

// RawFormat displays the contents of a system table as serialized
// during `Load`. The contents of the system table may not be
// immediately understandable as they might include protobuf payloads
// and other binary-encoded data.
func (sc *systemTableContents) RawFormat() string {
	rows := make([]string, 0, len(sc.rows))
	for r := range sc.rows {
		rows = append(rows, r)
	}

	return strings.Join(rows, "\n")
}

// ShowSystemOutput displays the contents of the the `SHOW` statement
// for the underlying system table, if available.
func (sc *systemTableContents) ShowSystemOutput(label string) string {
	if sc.showResults == nil {
		return ""
	}

	return fmt.Sprintf("\n--- %q %s:\n%s", sc.showResults.query, label, sc.showResults.output)
}

// settingsHandler replaces the contents of every column in the
// system.settings table for the `version` setting. This setting is
// expected to contain the current cluster version, so it should not
// match the value when the backup was taken.
func (sc *systemTableContents) settingsHandler(
	values []interface{}, columns []string,
) ([]interface{}, error) {
	return newSystemTableRow(sc.table, values, columns).
		// `name` column equals 'version'
		Matches("name", "version").
		// use the sentinel value for every column in the settings table
		WithSentinelIfExists(columns...).
		Values()
}

// scheduleJobsHandler replaces the contents of some columns in the
// `scheduled_jobs` system table since we cannot ensure that they will
// match their values in the backup. Specifically, there's a race in
// the case when a scheduled jobs's next_run is in the past. In this
// scenario, the jobs subssytem will schedule the job for execution as
// soon as the restore is finished, and by the time the test attempts
// to compare the restored contents of scheduled_jobs with the
// original contents, they will have already diverged.
//
// Note that this same race was also identified in unit tests (see #100094).
func (sc *systemTableContents) scheduledJobsHandler(
	values []interface{}, columns []string,
) ([]interface{}, error) {
	return newSystemTableRow(sc.table, values, columns).
		WithSentinelIfExists("next_run", "schedule_details", "schedule_state").
		// Skip row-level TTL jobs to avoid false-positives, as they are reinserted
		// with new scheduled job IDs on restore.
		Matches("executor_type", tree.ScheduledRowLevelTTLExecutor.InternalName()).
		Delete().
		Values()
}

// usersHandler replaces the contents of "estimated_last_login_time" which gets
// updated on every user login attempt, which means the post restore fingerprint
// could observe an updated log in time value.
func (sc *systemTableContents) usersHandler(
	values []interface{}, columns []string,
) ([]interface{}, error) {
	return newSystemTableRow(sc.table, values, columns).
		WithSentinelIfExists("estimated_last_login_time").
		Values()
}

func (sc *systemTableContents) commentsHandler(
	values []interface{}, columns []string,
) ([]interface{}, error) {
	return newSystemTableRow(sc.table, values, columns).
		WithSentinelIfExists("object_id"). // object_id is rekeyed
		Values()
}

// tenantSettingsHandler deletes a `version` key from the
// system.tenant_settings table, if any. This row is not restored as
// of 24.2+ so it shouldn't be validated after restore.
func (sc *systemTableContents) tenantSettingsHandler(
	values []interface{}, columns []string,
) ([]interface{}, error) {
	return newSystemTableRow(sc.table, values, columns).
		Matches("name", "version").
		Delete().
		Values()
}

// handleSpecialCases exists because there are still cases where we
// can't assume that the contents of a system table are the same after
// a RESTORE. Columns that cannot be expected to be the same are
// replaced with a sentinel value in this function. If a row shouldn't
// be considered when validating a restore, `nil` is returned.
func (sc *systemTableContents) handleSpecialCases(
	l *logger.Logger, row []interface{}, columns []string,
) ([]interface{}, error) {
	switch sc.table {
	case "system.settings":
		return sc.settingsHandler(row, columns)
	case "system.scheduled_jobs":
		return sc.scheduledJobsHandler(row, columns)
	case "system.comments":
		return sc.commentsHandler(row, columns)
	case "system.tenant_settings":
		return sc.tenantSettingsHandler(row, columns)
	case "system.users":
		return sc.usersHandler(row, columns)
	default:
		return row, nil
	}
}

// loadShowResults loads the `showResults` field of the
// `systemTableContents` struct if the system table being analyzed has
// a matching `SHOW` statement (see `showSystemQueries` mapping). We
// run `cockroach sql` directly here to leverage the existing
// formatting logic in that command. This is only done to facilitate
// debugging in the event of failures.
func (sc *systemTableContents) loadShowResults(
	ctx context.Context, l *logger.Logger, timestamp string,
) error {
	if sc.cluster.IsLocal() {
		// T query below fails on local roachtest, so skip system table fingerprinting.
		return nil
	}
	systemName := strings.TrimPrefix(sc.table, "system.")
	showStmt, ok := showSystemQueries[systemName]
	if !ok {
		return nil
	}

	query := fmt.Sprintf("SELECT * FROM [%s]%s", showStmt, aostFor(timestamp))
	showCmd := roachtestutil.NewCommand("%s sql", test.DefaultCockroachPath).
		Flag("certs-dir", install.CockroachNodeCertsDir).
		Flag("e", fmt.Sprintf("%q", query)).
		Flag("port", fmt.Sprintf("{pgport:%d}", sc.roachNode)).
		String()

	node := sc.cluster.Node(sc.roachNode)
	result, err := sc.cluster.RunWithDetailsSingleNode(ctx, l, option.WithNodes(node), showCmd)
	if err != nil {
		return fmt.Errorf("error running command (%s): %w", showCmd, err)
	}

	sc.showResults = &showSystemResults{query: query, output: result.Stdout}
	return nil
}

// Load loads the contents of the underlying system table in memory.
// The contents of every row in the system table are marshaled using
// `json_agg` in the database. This function then converts that to a
// JSON array and uses that serialized representation in a set of rows
// that are kept in memory.
//
// Some assumptions this function makes:
// * all the contents of a system table can be marshaled to JSON;
// * the entire contents of system tables can be kept in memory.
//
// For the purposes of this test, both of these assumptions should
// hold without problems at the time of writing (~v23.1). We may need
// to revisit them if that ever changes.
func (sc *systemTableContents) Load(
	ctx context.Context, l *logger.Logger, timestamp string, previous tableContents,
) error {
	var loadColumns []string
	// If we are loading the contents of a system table after a restore,
	// we should only be querying the columns that were loaded when the
	// corresponding backup happened. We can't verify the correctness of
	// data added by migrations.
	if previous == nil {
		loadColumns = sc.columns
	} else {
		previousSystemTableContents := previous.(*systemTableContents)
		loadColumns = previousSystemTableContents.columns
	}

	l.Printf("loading data in table %s", sc.table)
	inner := fmt.Sprintf("SELECT %s FROM %s", quoteColumnsForSelect(loadColumns), sc.table)
	query := fmt.Sprintf(
		"SELECT coalesce(json_agg(s), '[]') AS encoded_data FROM (%s) s%s",
		inner, aostFor(timestamp),
	)
	var data []byte
	if err := sc.db.QueryRowContext(ctx, query).Scan(&data); err != nil {
		return fmt.Errorf("error when running query [%s]: %w", query, err)
	}

	// opaqueRows should contain a list of JSON-encoded rows:
	// [{"col1": "val11", "col2": "val12"}, {"col1": "val21", "col2": "val22"}, ...]
	var opaqueRows []map[string]interface{}
	if err := json.Unmarshal(data, &opaqueRows); err != nil {
		return fmt.Errorf("error unmarshaling data from table %s: %w", sc.table, err)
	}

	// rowSet should be a set of rows where each row is encoded as a
	// JSON array that matches the sorted list of columns in
	// `sc.columns`:
	// {["val11", "val12"], ["val21", "val22"], ...}
	rowSet := make(map[string]struct{})
	for _, r := range opaqueRows {
		opaqueRow := make([]interface{}, 0, len(loadColumns))
		for _, c := range loadColumns {
			opaqueRow = append(opaqueRow, r[c])
		}
		processedRow, err := sc.handleSpecialCases(l, opaqueRow, loadColumns)
		if err != nil {
			return fmt.Errorf("error processing row %v: %w", opaqueRow, err)
		}

		if processedRow == nil {
			continue
		}

		encodedRow, err := json.Marshal(processedRow)
		if err != nil {
			return fmt.Errorf("error marshaling processed row for table %s: %w", sc.table, err)
		}
		rowSet[string(encodedRow)] = struct{}{}
	}

	sc.rows = rowSet
	if err := sc.loadShowResults(ctx, l, timestamp); err != nil {
		return err
	}

	l.Printf("loaded %d rows from %s", len(sc.rows), sc.table)
	return nil
}

// ValidateRestore validates that every row that existed in a system
// table when the backup was taken continues to exist when the backup
// is restored.
func (sc *systemTableContents) ValidateRestore(
	ctx context.Context, l *logger.Logger, contents tableContents,
) error {
	restoredContents := contents.(*systemTableContents)
	for originalRow := range sc.rows {
		_, exists := restoredContents.rows[originalRow]
		if !exists {
			// Log the missing row and restored table contents here and
			// avoid including it in the error itself as the error message
			// from a test is displayed multiple times in a roachtest
			// failure, and having a long, multi-line error message adds a
			// lot of noise to the logs.
			l.Printf(
				"--- Missing row in table %s:\n%s\n--- Original rows:\n%s\n--- Restored contents:\n%s%s%s",
				sc.table, originalRow, sc.RawFormat(), restoredContents.RawFormat(),
				sc.ShowSystemOutput("when backup was taken"), restoredContents.ShowSystemOutput("after restore"),
			)

			return fmt.Errorf("restored system table %s is missing a row: %s", sc.table, originalRow)
		}
	}

	return nil
}
