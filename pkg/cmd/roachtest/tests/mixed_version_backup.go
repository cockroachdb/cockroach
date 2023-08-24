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
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
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
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const (
	// nonceLen is the length of the randomly generated string appended
	// to the backup name when creating a backup in GCS for this test.
	nonceLen = 4

	// systemTableSentinel is a placeholder value for system table
	// columns that cannot be validated after restore, for various
	// reasons. See `handleSpecialCases` for more details.
	systemTableSentinel = "{SENTINEL}"

	// probability that we will attempt to restore a backup in
	// mixed-version state.
	mixedVersionRestoreProbability = 0.5

	// string label added to the names of backups taken while the cluster is
	// upgrading.
	finalizingLabel = "_finalizing"

	// probabilities that the test will attempt to pause a backup job.
	neverPause  = 0
	alwaysPause = 1

	// the test will not pause a backup job more than `maxPauses` times.
	maxPauses = 3
)

var (
	invalidVersionRE = regexp.MustCompile(`[^a-zA-Z0-9\.]`)
	invalidDBNameRE  = regexp.MustCompile(`[\-\.:/]`)

	// retry options while waiting for a backup to complete
	backupCompletionRetryOptions = retry.Options{
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     1 * time.Minute,
		Multiplier:     1.5,
		MaxRetries:     80,
	}

	v231 = version.MustParse("v23.1.0")
	v222 = version.MustParse("v22.2.0")

	// minActivelySupportedVersion is the minimum cluster version that
	// should be active for this test to perform any backups or
	// restores. We are only interested in releases where we are still
	// actively fixing bugs in patch releases.
	minActivelySupportedVersion = v222

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

	// systemSettingValues is a mapping from system setting names to
	// possible values they can assume in this test. The system settings
	// listed here are chosen based on availability of documentation
	// (see below), meaning a customer could have reasonably set them;
	// and relationship to backup/restore. The list of possible values
	// are generally 50-100% smaller or larger than the documented
	// default.
	//
	// Documentation:
	// https://www.cockroachlabs.com/docs/stable/cluster-settings.html
	systemSettingValues = map[string][]string{
		"bulkio.backup.file_size":                        {"8MiB", "32MiB", "512MiB", "750MiB"},
		"bulkio.backup.read_timeout":                     {"2m0s", "10m0s"},
		"bulkio.backup.read_with_priority_after":         {"20s", "5m0s"},
		"bulkio.stream_ingestion.minimum_flush_interval": {"1s", "10s", "30s"},
		"kv.bulk_io_write.max_rate":                      {"250MiB", "500MiB", "2TiB"},
		"kv.bulk_sst.max_allowed_overage":                {"16MiB", "256MiB"},
		"kv.bulk_sst.target_size":                        {"4MiB", "64MiB", "128MiB"},
	}

	systemSettingNames = func() []string {
		names := make([]string, 0, len(systemSettingValues))
		for name := range systemSettingValues {
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

	largeBankPayload         = 128 << 10 // 128 KiB
	bankPossiblePayloadBytes = []int{
		0,                // workload default
		9,                // 1 random byte (`initial-` + 1)
		500,              // 5x default at the time of writing
		16 << 10,         // 16 KiB
		largeBankPayload, // 128 KiB
	}

	possibleNumIncrementalBackups = []int{
		1,
		3,
		5,
	}
)

// sanitizeVersionForBackup takes the string representation of a
// version and removes any characters that would not be allowed in a
// backup destination.
func sanitizeVersionForBackup(v string) string {
	return invalidVersionRE.ReplaceAllString(clusterupgrade.VersionMsg(v), "")
}

// hasInternalSystemJobs returns true if the cluster is expected to
// have the `crdb_internal.system_jobs` vtable in the mixed-version
// context passed. If so, it should be used instead of `system.jobs`
// when querying job status.
func hasInternalSystemJobs(h *mixedversion.Helper) bool {
	return h.LowestBinaryVersion().AtLeast(v231)
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
	}

	fullBackup struct {
		namePrefix string
	}
	incrementalBackup struct {
		collection backupCollection
		incNum     int
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

func (fb fullBackup) String() string        { return "full" }
func (ib incrementalBackup) String() string { return "incremental" }

func (rh revisionHistory) String() string {
	return "revision_history"
}

func randIntBetween(rng *rand.Rand, min, max int) int {
	return rng.Intn(max-min) + min
}

func randString(rng *rand.Rand, strLen int) string {
	return randutil.RandString(rng, strLen, randutil.PrintableKeyAlphabet)
}

func randWaitDuration(rng *rand.Rand) time.Duration {
	durations := []int{1, 10, 60, 5 * 60}
	return time.Duration(durations[rng.Intn(len(durations))]) * time.Second
}

func newEncryptionPassphrase(rng *rand.Rand) encryptionPassphrase {
	return encryptionPassphrase{randString(rng, randIntBetween(rng, 32, 64))}
}

func (ep encryptionPassphrase) String() string {
	return fmt.Sprintf("encryption_passphrase = '%s'", ep.passphrase)
}

// newBackupOptions returns a list of backup options to be used when
// creating a new backup. Each backup option has a 50% chance of being
// included.
func newBackupOptions(rng *rand.Rand) []backupOption {
	possibleOpts := []backupOption{
		revisionHistory{},
		newEncryptionPassphrase(rng),
	}

	var options []backupOption
	for _, opt := range possibleOpts {
		if rng.Float64() < 0.5 {
			options = append(options, opt)
		}
	}

	return options
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

func newTableBackup(rng *rand.Rand, dbs []string, tables [][]string) *tableBackup {
	var targetDBIdx int
	var targetDB string
	// Avoid creating table backups for the tpcc database, as they often
	// have foreign keys to other tables, making restoring them
	// difficult. We could pass the `skip_missing_foreign_keys` option,
	// but that would be a less interesting test.
	for targetDB == "" || targetDB == "tpcc" {
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

func newClusterBackup(
	rng *rand.Rand, dbs []string, tables [][]string,
) *clusterBackup {
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

// Load computes the fingerprints for the underlying table and stores
// the contents in the `fingeprints` field.
func (fc *fingerprintContents) Load(
	ctx context.Context, l *logger.Logger, timestamp string, _ tableContents,
) error {
	l.Printf("computing fingerprints for table %s", fc.table)
	query := fmt.Sprintf(
		"SELECT index_name, fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s]%s ORDER BY index_name",
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
func (sr *systemTableRow) processColumn(column string, fn func(interface{}) interface{}) {
	if sr.skip() {
		return
	}

	colIdx := sort.SearchStrings(sr.columns, column)
	hasCol := colIdx < len(sr.columns) && sr.columns[colIdx] == column

	if !hasCol {
		sr.err = fmt.Errorf("could not find column %s on %s", column, sr.table)
		return
	}

	sr.values[colIdx] = fn(sr.values[colIdx])
}

// Matches allows the caller to only apply certain changes if the
// value of a certain column matches a given value.
func (sr *systemTableRow) Matches(column string, value interface{}) *systemTableRow {
	sr.processColumn(column, func(actualValue interface{}) interface{} {
		sr.matches = reflect.DeepEqual(actualValue, value)
		return actualValue
	})

	return sr
}

// WithSentinel replaces the contents of the given columns with a
// fixed sentinel value.
func (sr *systemTableRow) WithSentinel(columns ...string) *systemTableRow {
	for _, column := range columns {
		sr.processColumn(column, func(value interface{}) interface{} {
			return systemTableSentinel
		})
	}

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
		WithSentinel(columns...).
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
		WithSentinel("next_run", "schedule_details", "schedule_state").
		Values()
}

func (sc *systemTableContents) commentsHandler(
	values []interface{}, columns []string,
) ([]interface{}, error) {
	return newSystemTableRow(sc.table, values, columns).
		WithSentinel("object_id"). // object_id is rekeyed
		Values()
}

// handleSpecialCases exists because there are still cases where we
// can't assume that the contents of a system table are the same after
// a RESTORE. Columns that cannot be expected to be the same are
// replaced with a sentinel value in this function.
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
	systemName := strings.TrimPrefix(sc.table, "system.")
	showStmt, ok := showSystemQueries[systemName]
	if !ok {
		return nil
	}

	query := fmt.Sprintf("SELECT * FROM [%s]%s", showStmt, aostFor(timestamp))
	showCmd := roachtestutil.NewCommand("%s sql", test.DefaultCockroachPath).
		Flag("certs-dir", "certs").
		Flag("e", fmt.Sprintf("%q", query)).
		String()

	node := sc.cluster.Node(sc.roachNode)
	result, err := sc.cluster.RunWithDetailsSingleNode(ctx, l, node, showCmd)
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

func newBackupCollection(name string, btype backupScope, options []backupOption) backupCollection {
	// Use a different seed for generating the collection's nonce to
	// allow for multiple concurrent runs of this test using the same
	// COCKROACH_RANDOM_SEED, making it easier to reproduce failures
	// that are more likely to occur with certain test plans.
	nonceRng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	return backupCollection{
		btype:   btype,
		name:    name,
		tables:  btype.TargetTables(),
		options: options,
		nonce:   randString(nonceRng, nonceLen),
	}
}

func (bc *backupCollection) uri() string {
	// Append the `nonce` to the backup name since we are now sharing a
	// global namespace represented by the BACKUP_TESTING_BUCKET
	// bucket. The nonce allows multiple people (or TeamCity builds) to
	// be running this test without interfering with one another.
	gcsBackupTestingBucket := os.Getenv("BACKUP_TESTING_BUCKET")
	if gcsBackupTestingBucket == "" {
		gcsBackupTestingBucket = "cockroachdb-backup-testing"
	}
	return fmt.Sprintf("gs://"+gcsBackupTestingBucket+"/mixed-version/%s_%s?AUTH=implicit", bc.name, bc.nonce)
}

func (bc *backupCollection) encryptionOption() *encryptionPassphrase {
	for _, option := range bc.options {
		if ep, ok := option.(encryptionPassphrase); ok {
			return &ep
		}
	}

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

// mixedVersionBackup is the struct that contains all the necessary
// state involved in the mixed-version backup test.
type mixedVersionBackup struct {
	cluster    cluster.Cluster
	t          test.Test
	roachNodes option.NodeListOption
	// backup collections that are created along the test
	collections []*backupCollection

	// databases where user data is being inserted
	dbs          []string
	tables       [][]string
	tablesLoaded *atomic.Bool

	// stopBackground can be called to stop any background functions
	// (including workloads) started in this test. Useful when restoring
	// cluster backups, as we don't want a stream of errors in the
	// these functions due to the nodes stopping.
	stopBackground mixedversion.StopFunc

	backupRestoreTestDriver *BackupRestoreTestDriver

	// commonTestUtils contains test utilities that can be shared between tests.
	// Do not use this field directly, use the CommonTestUtils method instead.
	commonTestUtils *CommonTestUtils
	utilsOnce       sync.Once
}

func newMixedVersionBackup(
	t test.Test,
	c cluster.Cluster,
	roachNodes option.NodeListOption,
	dbs []string,
) (*mixedVersionBackup, error) {
	var tablesLoaded atomic.Bool
	tablesLoaded.Store(false)

	return &mixedVersionBackup{
		t: t, cluster: c, roachNodes: roachNodes, tablesLoaded: &tablesLoaded, dbs: dbs,
	}, nil
}

// TODO(rui): move the driver to its own file or consolidate its contents in
// this file. Currently all of its methods are scattered to make the diff that
// introduced the driver easier to review.
type BackupRestoreTestDriver struct {
	ctx     context.Context
	t       test.Test
	cluster cluster.Cluster

	roachNodes option.NodeListOption

	// counter that is incremented atomically to provide unique
	// identifiers to backups created during the test
	currentBackupID int64

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
	ctx context.Context, t test.Test, c cluster.Cluster, testUtils *CommonTestUtils, nodes option.NodeListOption, dbs []string, tables [][]string,
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

func (mvb *mixedVersionBackup) initBackupRestoreTestDriver(ctx context.Context, l *logger.Logger, rng *rand.Rand) error {
	u, err := mvb.CommonTestUtils(ctx)
	if err != nil {
		return err
	}
	tables, err := u.loadTablesForDBs(ctx, l, rng, mvb.dbs...)
	if err != nil {
		return err
	}
	mvb.tables = tables
	mvb.tablesLoaded.Store(true)

	mvb.backupRestoreTestDriver, err = newBackupRestoreTestDriver(ctx, mvb.t, mvb.cluster, u, mvb.roachNodes, mvb.dbs, tables)
	if err != nil {
		return err
	}

	return nil
}

// newBackupScope chooses a random backup type (table, database,
// cluster) with equal probability.
func (d *BackupRestoreTestDriver) newBackupScope(rng *rand.Rand) backupScope {
	possibleTypes := []backupScope{
		newTableBackup(rng, d.dbs, d.tables),
		newDatabaseBackup(rng, d.dbs, d.tables),
		newClusterBackup(rng, d.dbs, d.tables),
	}

	return possibleTypes[rng.Intn(len(possibleTypes))]
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
	eg, _ := errgroup.WithContext(ctx)
	for j, dbName := range dbs {
		j, dbName := j, dbName // capture range variables
		eg.Go(func() error {
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

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return allTables, nil
}

// setClusterSettings may set up to numCustomSettings cluster settings
// as defined in `systemSettingValues`. The system settings changed
// are logged. This function should be called *before* the upgrade
// begins; the cockroach documentation says explicitly that changing
// cluster settings is not supported in mixed-version, so we don't
// test that scenario.
func (u *CommonTestUtils) setClusterSettings(ctx context.Context, l *logger.Logger, rng *rand.Rand) error {
	const numCustomSettings = 3
	const defaultSettingsProbability = 0.2

	if rng.Float64() < defaultSettingsProbability {
		l.Printf("not setting any custom cluster settings (using defaults)")
		return nil
	}

	for j := 0; j < numCustomSettings; j++ {
		setting := systemSettingNames[rng.Intn(len(systemSettingNames))]
		possibleValues := systemSettingValues[setting]
		value := possibleValues[rng.Intn(len(possibleValues))]

		l.Printf("setting cluster setting %q to %q", setting, value)
		stmt := fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", setting, value)
		if err := u.Exec(ctx, rng, stmt); err != nil {
			return err
		}
	}

	return nil
}

// skipBackups returns `true` when the cluster is running at a version
// older than the minimum actively supported version. In this case, we
// don't want to verify the correctness of backups or restores since
// the releases are already past their non-security support
// window. Crucially, this also stops this test from hitting bugs
// already fixed in later releases.
func (mvb *mixedVersionBackup) skipBackups(l *logger.Logger, h *mixedversion.Helper) bool {
	if lv := h.LowestBinaryVersion(); !lv.AtLeast(minActivelySupportedVersion) {
		l.Printf(
			"skipping step because %s is lower than minimum actively supported version %s",
			lv, minActivelySupportedVersion,
		)
		return true
	}

	return false
}

func (mvb *mixedVersionBackup) setShortJobIntervals(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	u, err := mvb.CommonTestUtils(ctx)
	if err != nil {
		return err
	}
	return u.setShortJobIntervals(ctx, rng)
}

func (mvb *mixedVersionBackup) systemTableWriter(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
	for !mvb.tablesLoaded.Load() {
		l.Printf("waiting for user tables to be loaded...")
		time.Sleep(10 * time.Second)
	}
	l.Printf("user tables loaded, starting random inserts")

	u, err := mvb.CommonTestUtils(ctx)
	if err != nil {
		return err
	}

	return u.systemTableWriter(ctx, l, rng, mvb.dbs, mvb.backupRestoreTestDriver.tables)
}

func (mvb *mixedVersionBackup) setClusterSettings(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	u, err := mvb.CommonTestUtils(ctx)
	if err != nil {
		return err
	}
	return u.setClusterSettings(ctx, l, rng)
}

// maybeTakePreviousVersionBackup creates a backup collection (full +
// incremental), and is supposed to be called before any nodes are
// upgraded. This ensures that we are able to restore this backup
// later, when we are in mixed version, and also after the upgrade is
// finalized.
func (mvb *mixedVersionBackup) maybeTakePreviousVersionBackup(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	// Wait here for a few minutes to allow the workloads (which are
	// initializing concurrently with this step) to store some data in
	// the cluster by the time the backup is taken. The actual wait time
	// chosen is somewhat arbitrary: it's less time than workloads
	// typically need to finish initializing (especially tpcc), so the
	// backup is taken while data is still being inserted. The actual
	// time is irrelevant as far as correctness is concerned: we should
	// be able to restore this backup after upgrading regardless of the
	// amount of data backed up.
	wait := 3 * time.Minute
	l.Printf("waiting for %s", wait)
	time.Sleep(wait)

	if err := mvb.initBackupRestoreTestDriver(ctx, l, rng); err != nil {
		return err
	}

	if mvb.skipBackups(l, h) {
		return nil
	}

	previousVersion := h.Context().FromVersion
	label := fmt.Sprintf("before upgrade in %s", sanitizeVersionForBackup(previousVersion))
	allPrevVersionNodes := labeledNodes{Nodes: mvb.roachNodes, Version: previousVersion}
	executeOnAllNodesSpec := backupSpec{PauseProbability: neverPause, Plan: allPrevVersionNodes, Execute: allPrevVersionNodes}
	return mvb.createBackupCollection(ctx, l, rng, executeOnAllNodesSpec, executeOnAllNodesSpec, h, label)
}

// randomWait waits from 1s to 5m, to allow for the background
// workloads to update the databases we are backing up.
func (d *BackupRestoreTestDriver) randomWait(l *logger.Logger, rng *rand.Rand) {
	dur := randWaitDuration(rng)
	l.Printf("waiting for %s", dur)
	time.Sleep(dur)
}

func (mvb *mixedVersionBackup) now() string {
	return hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}.AsOfSystemTime()
}

func (d *BackupRestoreTestDriver) nextBackupID() int64 {
	return atomic.AddInt64(&d.currentBackupID, 1)
}

func (d *BackupRestoreTestDriver) nextRestoreID() int64 {
	return atomic.AddInt64(&d.currentRestoreID, 1)
}

// backupNamePrefix returns a descriptive prefix for the name of a backup
// depending on the state of the test we are in. The given label is also used to
// provide more context. Example: '22.2.4-to-current_final'
func (mvb *mixedVersionBackup) backupNamePrefix(
	h *mixedversion.Helper, label string,
) string {
	testContext := h.Context()
	var finalizing string
	if testContext.Finalizing {
		finalizing = finalizingLabel
	}

	fromVersion := sanitizeVersionForBackup(testContext.FromVersion)
	toVersion := sanitizeVersionForBackup(testContext.ToVersion)
	sanitizedLabel := strings.ReplaceAll(label, " ", "-")

	return fmt.Sprintf(
		"%s-to-%s_%s%s",
		fromVersion, toVersion, sanitizedLabel, finalizing,
	)
}

// backupCollectionName creates a backup collection name based on an unique ID,
// prefix, and the target scope of the backup.
func (d *BackupRestoreTestDriver) backupCollectionName(
	id int64, prefix string, btype backupScope,
) string {
	sanitizedType := strings.ReplaceAll(btype.Desc(), " ", "-")
	return fmt.Sprintf("%d_%s_%s", id, prefix, sanitizedType)
}

func (u *CommonTestUtils) waitForJobSuccess(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, jobID int, internalSystemJobs bool,
) error {
	var lastErr error
	node, db := u.RandomDB(rng, u.roachNodes)
	l.Printf("querying job status through node %d", node)

	jobsQuery := "system.jobs WHERE id = $1"
	if internalSystemJobs {
		jobsQuery = fmt.Sprintf("(%s)", jobutils.InternalSystemJobsBaseQuery)
	}
	for r := retry.StartWithCtx(ctx, backupCompletionRetryOptions); r.Next(); {
		var status string
		var payloadBytes []byte
		err := db.QueryRow(
			fmt.Sprintf(`SELECT status, payload FROM %s`, jobsQuery), jobID,
		).Scan(&status, &payloadBytes)
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
	eg, _ := errgroup.WithContext(ctx)
	for j, table := range tables {
		j, table := j, table // capture range variables
		eg.Go(func() error {
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
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
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

func (d *BackupRestoreTestDriver) runBackup(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	nodes option.NodeListOption,
	pauseProbability float64,
	bType fmt.Stringer,
	internalSystemJobs bool,
) (backupCollection, string, error) {
	pauseAfter := 1024 * time.Hour // infinity
	var pauseResumeDB *gosql.DB
	if rng.Float64() < pauseProbability {
		possibleDurations := []time.Duration{
			10 * time.Second, 30 * time.Second, 2 * time.Minute,
		}
		pauseAfter = possibleDurations[rng.Intn(len(possibleDurations))]

		var node int
		node, pauseResumeDB = d.testUtils.RandomDB(rng, d.roachNodes)
		l.Printf("attempting pauses in %s through node %d", pauseAfter, node)
	}

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
		btype := d.newBackupScope(rng)
		name := d.backupCollectionName(d.nextBackupID(), b.namePrefix, btype)
		createOptions := newBackupOptions(rng)
		collection = newBackupCollection(name, btype, createOptions)
		l.Printf("creating full backup for %s", collection.name)
	case incrementalBackup:
		collection = b.collection
		latest = " LATEST IN"
		l.Printf("creating incremental backup num %d for %s", b.incNum, collection.name)
	}

	for _, opt := range collection.options {
		options = append(options, opt.String())
	}

	backupTime := d.testUtils.now()
	node, db := d.testUtils.RandomDB(rng, nodes)

	stmt := fmt.Sprintf(
		"%s INTO%s '%s' AS OF SYSTEM TIME '%s' WITH %s",
		collection.btype.BackupCommand(),
		latest,
		collection.uri(),
		backupTime,
		strings.Join(options, ", "),
	)
	l.Printf("creating %s backup via node %d: %s", bType, node, stmt)
	var jobID int
	if err := db.QueryRowContext(ctx, stmt).Scan(&jobID); err != nil {
		return backupCollection{}, "", fmt.Errorf("error while creating %s backup %s: %w", bType, collection.name, err)
	}

	backupErr := make(chan error)
	go func() {
		defer close(backupErr)
		l.Printf("waiting for job %d (%s)", jobID, collection.name)
		if err := d.testUtils.waitForJobSuccess(ctx, l, rng, jobID, internalSystemJobs); err != nil {
			backupErr <- err
		}
	}()

	var numPauses int
	for {
		select {
		case err := <-backupErr:
			if err != nil {
				return backupCollection{}, "", err
			}
			return collection, backupTime, nil

		case <-time.After(pauseAfter):
			if numPauses >= maxPauses {
				continue
			}

			pauseDur := 5 * time.Second
			l.Printf("pausing job %d for %s", jobID, pauseDur)
			if _, err := pauseResumeDB.Exec(fmt.Sprintf("PAUSE JOB %d", jobID)); err != nil {
				// We just log the error if pausing the job fails since we
				// cannot guarantee the job is still running by the time we
				// attempt to pause it. If that's the case, the next iteration
				// of the loop should read from the backupErr channel.
				l.Printf("error pausing job %d: %s", jobID, err)
				continue
			}

			time.Sleep(pauseDur)

			l.Printf("resuming job %d", jobID)
			if _, err := pauseResumeDB.Exec(fmt.Sprintf("RESUME JOB %d", jobID)); err != nil {
				return backupCollection{}, "", fmt.Errorf("error resuming job %d: %w", jobID, err)
			}

			numPauses++
		}
	}
}

// runBackup runs a `BACKUP` statement; the backup type `bType` needs
// to be an instance of either `fullBackup` or
// `incrementalBackup`. Returns when the backup job has completed.
func (mvb *mixedVersionBackup) runBackup(
	ctx context.Context,
	l *logger.Logger,
	bType fmt.Stringer,
	rng *rand.Rand,
	nodes option.NodeListOption,
	pauseProbability float64,
	h *mixedversion.Helper,
) (backupCollection, string, error) {
	tc := h.Context()
	if !tc.Finalizing {
		// don't wait if upgrade is finalizing to increase the chances of
		// creating a backup while upgrade migrations are being run.
		mvb.backupRestoreTestDriver.randomWait(l, rng)
	}

	internalSytemJobs := hasInternalSystemJobs(h)
	return mvb.backupRestoreTestDriver.runBackup(ctx, l, rng, nodes, pauseProbability, bType, internalSytemJobs)
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

func (mvb *mixedVersionBackup) createBackupCollection(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	fullBackupSpec backupSpec,
	incBackupSpec backupSpec,
	h *mixedversion.Helper,
	labelOverride string,
) error {
	label := backupCollectionDesc(fullBackupSpec, incBackupSpec)
	if labelOverride != "" {
		label = labelOverride
	}
	backupNamePrefix := mvb.backupNamePrefix(h, label)
	internalSystemJobs := hasInternalSystemJobs(h)

	collection, err := mvb.backupRestoreTestDriver.createBackupCollection(ctx, l, rng, fullBackupSpec, incBackupSpec, backupNamePrefix, internalSystemJobs)
	if err != nil {
		return err
	}

	mvb.collections = append(mvb.collections, collection)
	return nil
}

// createBackupCollection creates a new backup collection to be
// restored/verified at the end of the test. A full backup is created,
// and an incremental one is created on top of it. Both backups are
// created according to their respective `backupSpec`, indicating
// where they should be planned and executed.
func (d *BackupRestoreTestDriver) createBackupCollection(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	fullBackupSpec backupSpec,
	incBackupSpec backupSpec,
	backupNamePrefix string,
	internalSystemJobs bool,
) (*backupCollection, error) {
	var collection backupCollection
	var timestamp string

	// Create full backup.
	if err := d.testUtils.runJobOnOneOf(ctx, l, fullBackupSpec.Execute.Nodes, func() error {
		var err error
		collection, _, err = d.runBackup(
			ctx, l, rng, fullBackupSpec.Plan.Nodes, fullBackupSpec.PauseProbability, fullBackup{backupNamePrefix}, internalSystemJobs,
		)
		return err
	}); err != nil {
		return nil, err
	}

	// Create incremental backups.
	numIncrementals := possibleNumIncrementalBackups[rng.Intn(len(possibleNumIncrementalBackups))]
	l.Printf("creating %d incremental backups", numIncrementals)
	for i := 0; i < numIncrementals; i++ {
		d.randomWait(l, rng)
		if err := d.testUtils.runJobOnOneOf(ctx, l, incBackupSpec.Execute.Nodes, func() error {
			var err error
			collection, timestamp, err = d.runBackup(
				ctx, l, rng, incBackupSpec.Plan.Nodes, incBackupSpec.PauseProbability, incrementalBackup{collection: collection, incNum: i + 1}, internalSystemJobs,
			)
			return err
		}); err != nil {
			return nil, err
		}
	}

	return d.saveContents(ctx, l, rng, &collection, timestamp)
}

// sentinelFilePath returns the path to the file that prevents job
// adoption on the given node.
func (u *CommonTestUtils) sentinelFilePath(
	ctx context.Context, l *logger.Logger, node int,
) (string, error) {
	result, err := u.cluster.RunWithDetailsSingleNode(
		ctx, l, u.cluster.Node(node), "echo -n {store-dir}",
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
	eg, _ := errgroup.WithContext(ctx)
	for _, node := range nodes {
		node := node // capture range variable
		eg.Go(func() error {
			l.Printf("node %d: disabling job adoption", node)
			sentinelFilePath, err := u.sentinelFilePath(ctx, l, node)
			if err != nil {
				return err
			}
			if err := u.cluster.RunE(ctx, u.cluster.Node(node), "touch", sentinelFilePath); err != nil {
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

	return eg.Wait()
}

// enableJobAdoption (re-)enables job adoption on the given nodes.
func (u *CommonTestUtils) enableJobAdoption(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) error {
	l.Printf("enabling job adoption on nodes %v", nodes)
	eg, _ := errgroup.WithContext(ctx)
	for _, node := range nodes {
		node := node // capture range variable
		eg.Go(func() error {
			l.Printf("node %d: enabling job adoption", node)
			sentinelFilePath, err := u.sentinelFilePath(ctx, l, node)
			if err != nil {
				return err
			}

			if err := u.cluster.RunE(ctx, u.cluster.Node(node), "rm -f", sentinelFilePath); err != nil {
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
// in mixed-binary state, 3 (`numCollections`) backup collections are
// created (see possible setups in `collectionSpecs`). If all nodes
// are running the next version (which is different from the cluster
// version), then a backup collection is created in an arbitrary node.
func (mvb *mixedVersionBackup) planAndRunBackups(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	if mvb.skipBackups(l, h) {
		// If this function is called while an unsupported version is
		// running, we sleep for a few minutes to let the workloads run in
		// this older version.
		possibleWaitMinutes := []int{0, 10, 30}
		waitDur := time.Duration(possibleWaitMinutes[rng.Intn(len(possibleWaitMinutes))]) * time.Minute

		l.Printf("doing nothing for %s to let workloads run in this version", waitDur)
		select {
		case <-time.After(waitDur):
		case <-ctx.Done():
		}

		return nil
	}

	tc := h.Context() // test context
	l.Printf("current context: %#v", tc)

	onPrevious := labeledNodes{
		Nodes: tc.FromVersionNodes, Version: sanitizeVersionForBackup(tc.FromVersion),
	}
	onNext := labeledNodes{
		Nodes: tc.ToVersionNodes, Version: sanitizeVersionForBackup(tc.ToVersion),
	}
	onRandom := labeledNodes{Nodes: mvb.roachNodes, Version: "random node"}
	defaultPauseProbability := 0.2

	collectionSpecs := [][2]backupSpec{
		// Case 1: plan backups    -> previous node
		//         execute backups -> next node
		{
			{Plan: onPrevious, Execute: onNext, PauseProbability: defaultPauseProbability}, // full
			{Plan: onPrevious, Execute: onNext, PauseProbability: defaultPauseProbability}, // incremental
		},
		// Case 2: plan backups    -> next node
		//         execute backups -> previous node
		{
			{Plan: onNext, Execute: onPrevious, PauseProbability: defaultPauseProbability}, // full
			{Plan: onNext, Execute: onPrevious, PauseProbability: defaultPauseProbability}, // incremental
		},
		// Case 3: plan & execute full backup        -> previous node
		//         plan & execute incremental backup -> next node
		{
			{Plan: onPrevious, Execute: onPrevious, PauseProbability: defaultPauseProbability}, // full
			{Plan: onNext, Execute: onNext, PauseProbability: defaultPauseProbability},         // incremental
		},
		// Case 4: plan & execute full backup        -> next node
		//         plan & execute incremental backup -> previous node
		{
			{Plan: onNext, Execute: onNext, PauseProbability: defaultPauseProbability},         // full
			{Plan: onPrevious, Execute: onPrevious, PauseProbability: defaultPauseProbability}, // incremental
		},
		// Case 5: plan backups    -> random node
		//         execute backups -> random node (with pause + resume)
		{
			{Plan: onRandom, Execute: onRandom, PauseProbability: alwaysPause}, // full
			{Plan: onRandom, Execute: onRandom, PauseProbability: alwaysPause}, // incremental
		},
	}

	if len(tc.FromVersionNodes) > 0 {
		const numCollections = 2
		rng.Shuffle(len(collectionSpecs), func(i, j int) {
			collectionSpecs[i], collectionSpecs[j] = collectionSpecs[j], collectionSpecs[i]
		})

		for _, specPair := range collectionSpecs[:numCollections] {
			fullSpec, incSpec := specPair[0], specPair[1]
			l.Printf("planning backup: %s", backupCollectionDesc(fullSpec, incSpec))
			if err := mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h, ""); err != nil {
				return err
			}
		}
		return nil
	}

	l.Printf("all nodes running next version, running backup on arbitrary node")
	fullSpec := backupSpec{Plan: onNext, Execute: onNext, PauseProbability: defaultPauseProbability}
	incSpec := fullSpec
	return mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h, "")
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

func supportsCheckFiles(h *mixedversion.Helper) bool {
	return h.LowestBinaryVersion().AtLeast(v231)
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

// verifyBackupCollection restores the backup collection passed and verifies
// that the contents after the restore match the contents when the backup was
// taken. For cluster level backups, the cluster needs to be wiped before
// verifyBackupCollection is called or else the cluster restore will fail.
func (bc *backupCollection) verifyBackupCollection(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, d *BackupRestoreTestDriver, checkFiles bool, internalSystemJobs bool,
) error {
	// Defaults for the database where the backup will be restored,
	// along with the expected names of the tables after restore.
	restoreDB := fmt.Sprintf(
		"restore_%s_%d", invalidDBNameRE.ReplaceAllString(bc.name, "_"), d.nextRestoreID(),
	)
	restoredTables := tableNamesWithDB(restoreDB, bc.tables)

	// Pre-requisites:
	switch bc.btype.(type) {
	case *clusterBackup:
		// For cluster backups, the restored tables are always the same
		// as the tables we backed up. In addition, we need to wipe the
		// cluster before attempting a restore.
		restoredTables = bc.tables

	case *tableBackup:
		// If we are restoring a table backup , we need to create it
		// first.
		if err := d.testUtils.Exec(ctx, rng, fmt.Sprintf("CREATE DATABASE %s", restoreDB)); err != nil {
			return fmt.Errorf("backup %s: error creating database %s: %w", bc.name, restoreDB, err)
		}
	}

	// As a sanity check, make sure that a `check_files` check passes
	// before attempting a restore.
	if checkFiles {
		if err := d.testUtils.checkFiles(ctx, rng, bc); err != nil {
			return fmt.Errorf("backup %s: check_files failed: %w", bc.name, err)
		}
	}

	restoreCmd, options := bc.btype.RestoreCommand(restoreDB)
	restoreOptions := append([]string{"detached"}, options...)
	// If the backup was created with an encryption passphrase, we
	// need to include it when restoring as well.
	if opt := bc.encryptionOption(); opt != nil {
		restoreOptions = append(restoreOptions, opt.String())
	}

	var optionsStr string
	if len(restoreOptions) > 0 {
		optionsStr = fmt.Sprintf(" WITH %s", strings.Join(restoreOptions, ", "))
	}
	restoreStmt := fmt.Sprintf(
		"%s FROM LATEST IN '%s'%s",
		restoreCmd, bc.uri(), optionsStr,
	)
	var jobID int
	if err := d.testUtils.QueryRow(ctx, rng, restoreStmt).Scan(&jobID); err != nil {
		return fmt.Errorf("backup %s: error in restore statement: %w", bc.name, err)
	}

	if err := d.testUtils.waitForJobSuccess(ctx, l, rng, jobID, internalSystemJobs); err != nil {
		return err
	}

	restoredContents, err := d.computeTableContents(
		ctx, l, rng, restoredTables, bc.contents, "", /* timestamp */
	)
	if err != nil {
		return fmt.Errorf("backup %s: error loading restored contents: %w", bc.name, err)
	}

	for j, contents := range bc.contents {
		table := bc.tables[j]
		restoredTableContents := restoredContents[j]
		l.Printf("%s: verifying %s", bc.name, table)
		if err := contents.ValidateRestore(ctx, l, restoredTableContents); err != nil {
			return fmt.Errorf("backup %s: %w", bc.name, err)
		}
	}

	l.Printf("%s: OK", bc.name)
	return nil
}

// resetCluster wipes the entire cluster and starts it again with the
// specified version binary. This is done before we attempt restoring a
// full cluster backup.
func (u *CommonTestUtils) resetCluster(
	ctx context.Context, l *logger.Logger, version string, expectDeathsFn func(int),
) error {
	l.Printf("resetting cluster using version %q", clusterupgrade.VersionMsg(version))
	expectDeathsFn(len(u.roachNodes))
	if err := u.cluster.WipeE(ctx, l, true /* preserveCerts */, u.roachNodes); err != nil {
		return fmt.Errorf("failed to wipe cluster: %w", err)
	}

	cockroachPath := clusterupgrade.BinaryPathForVersion(u.t, version)
	return clusterupgrade.StartWithSettings(
		ctx, l, u.cluster, u.roachNodes, option.DefaultStartOptsNoBackups(),
		install.BinaryOption(cockroachPath), install.SecureOption(true),
	)
}

// verifySomeBackups is supposed to be called in mixed-version
// state. It will randomly pick a sample of the backups taken by the
// test so far (according to `mixedVersionRestoreProbability`), and
// validate the restore.
func (mvb *mixedVersionBackup) verifySomeBackups(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	var toBeRestored []*backupCollection
	for _, collection := range mvb.collections {
		if _, isCluster := collection.btype.(*clusterBackup); isCluster {
			continue
		}
		if rng.Float64() < mixedVersionRestoreProbability {
			toBeRestored = append(toBeRestored, collection)
		}
	}

	l.Printf("verifying %d out of %d backups in mixed version", len(toBeRestored), len(mvb.collections))
	checkFiles := supportsCheckFiles(h)
	if !checkFiles {
		l.Printf("skipping check_files as it is not supported")
	}

	internalSystemJobs := hasInternalSystemJobs(h)

	for _, collection := range toBeRestored {
		l.Printf("mixed-version: verifying %s", collection.name)
		if err := collection.verifyBackupCollection(ctx, l, rng, mvb.backupRestoreTestDriver, checkFiles, internalSystemJobs); err != nil {
			return errors.Wrap(err, "mixed-version")
		}
	}

	return nil
}

// verifyAllBackups cycles through all the backup collections created
// for the duration of the test, and verifies that restoring the
// backups results in the same data as when the backup was taken. We
// attempt to restore all backups taken both in the previous version,
// as well as in the current (latest) version, returning all errors
// found in the process.
func (mvb *mixedVersionBackup) verifyAllBackups(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	l.Printf("stopping background functions and workloads")
	mvb.stopBackground()

	u, err := mvb.CommonTestUtils(ctx)
	if err != nil {
		return err
	}

	var restoreErrors []error
	verify := func(version string) {
		v := clusterupgrade.VersionMsg(version)
		l.Printf("%s: verifying %d collections created during this test", v, len(mvb.collections))

		checkFiles := supportsCheckFiles(h)
		if !checkFiles {
			l.Printf("skipping check_files as it is not supported")
		}
		internalSystemJobs := hasInternalSystemJobs(h)

		for _, collection := range mvb.collections {
			if version != clusterupgrade.MainVersion && strings.Contains(collection.name, finalizingLabel) {
				// Do not attempt to restore, in the previous version, a
				// backup that was taken while the cluster was finalizing, as
				// that will most likely fail (the backup version will be past
				// the cluster version).
				continue
			}

			if _, ok := collection.btype.(*clusterBackup); ok {
				err := u.resetCluster(ctx, l, version, h.ExpectDeaths)
				if err != nil {
					err := errors.Wrapf(err, "%s", version)
					l.Printf("error resetting cluster: %v", err)
					restoreErrors = append(restoreErrors, err)
					continue
				}
			}

			if err := collection.verifyBackupCollection(ctx, l, rng, mvb.backupRestoreTestDriver, checkFiles, internalSystemJobs); err != nil {
				err := errors.Wrapf(err, "%s", version)
				l.Printf("restore error: %v", err)
				// Attempt to collect logs and debug.zip at the time of this
				// restore failure; if we can't, log the error encountered and
				// move on.
				restoreErr, collectionErr := u.collectFailureArtifacts(ctx, l, err, len(restoreErrors)+1)
				if collectionErr != nil {
					l.Printf("could not collect failure artifacts: %v", collectionErr)
				}
				restoreErrors = append(restoreErrors, restoreErr)
			}
		}
	}

	verify(h.Context().FromVersion)
	verify(h.Context().ToVersion)

	// If the context was canceled (most likely due to a test timeout),
	// return early. In these cases, it's likely that `restoreErrors`
	// will have a number of "restore failures" that all happened
	// because the underlying context was canceled, so proceeding with
	// the error reporting logic below is confusing, as it makes it look
	// like multiple failures occurred. It also makes the actually
	// important "timed out" message less prominent.
	if err := ctx.Err(); err != nil {
		return err
	}

	if len(restoreErrors) > 0 {
		if len(restoreErrors) == 1 {
			// Simplify error reporting if only one error was found.
			return restoreErrors[0]
		}

		msgs := make([]string, 0, len(restoreErrors))
		for j, err := range restoreErrors {
			msgs = append(msgs, fmt.Sprintf("%d: %s", j+1, err.Error()))
		}
		return fmt.Errorf("%d errors during restore:\n%s", len(restoreErrors), strings.Join(msgs, "\n"))
	}

	// Reset collections -- if this test run is performing multiple
	// upgrades, we just want to test restores from the previous version
	// to the current one.
	//
	// TODO(renato): it would be nice if this automatically followed
	// `binaryMinSupportedVersion` instead.
	mvb.collections = nil
	return nil
}

func registerBackupMixedVersion(r registry.Registry) {
	// backup/mixed-version tests different states of backup in a mixed
	// version cluster. The actual state of the cluster when a backup is
	// executed is randomized, so each run of the test will exercise a
	// different set of events. Reusing the same seed will produce the
	// same test.
	r.Add(registry.TestSpec{
		Name:              "backup-restore/mixed-version",
		Timeout:           8 * time.Hour,
		Owner:             registry.OwnerDisasterRecovery,
		Cluster:           r.MakeClusterSpec(5),
		EncryptionSupport: registry.EncryptionMetamorphic,
		RequiresLicense:   true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			//if c.Spec().Cloud != spec.GCE && !c.IsLocal() {
			//	t.Skip("uses gs://cockroachdb-backup-testing; see https://github.com/cockroachdb/cockroach/issues/105968")
			//}

			roachNodes := c.Range(1, c.Spec().NodeCount-1)
			workloadNode := c.Node(c.Spec().NodeCount)
			mvt := mixedversion.NewTest(
				ctx, t, t.L(), c, roachNodes,
				// We use a longer upgrade timeout in this test to give the
				// migrations enough time to finish considering all the data
				// that might exist in the cluster by the time the upgrade is
				// attempted.
				mixedversion.UpgradeTimeout(30*time.Minute),
				mixedversion.AlwaysUseLatestPredecessors(),
			)
			testRNG := mvt.RNG()

			uploadVersion(ctx, t, c, workloadNode, clusterupgrade.MainVersion)

			dbs := []string{"bank", "tpcc"}
			backupTest, err := newMixedVersionBackup(t, c, roachNodes, dbs)
			if err != nil {
				t.Fatal(err)
			}

			defer func() {
				err := backupTest.cleanUp(ctx)
				if err != nil {
					t.L().Printf("encountered error while cleaning up: %v", err)
				}
			}()

			// numWarehouses is picked as a number that provides enough work
			// for the cluster used in this test without overloading it,
			// which can make the backups take much longer to finish.
			const numWarehouses = 100
			bankInit, bankRun := bankWorkloadCmd(testRNG, roachNodes)
			tpccInit, tpccRun := tpccWorkloadCmd(numWarehouses, roachNodes)

			mvt.OnStartup("set short job interval", backupTest.setShortJobIntervals)
			mvt.OnStartup("take backup in previous version", backupTest.maybeTakePreviousVersionBackup)
			mvt.OnStartup("maybe set custom cluster settings", backupTest.setClusterSettings)

			// We start two workloads in this test:
			// - bank: the main purpose of this workload is to test some
			//   edge cases, such as columns with small or large payloads,
			//   keys with long revision history, etc.
			// - tpcc: tpcc is a much more complex workload, and should keep
			//   the cluster relatively busy while the backup and restores
			//   take place. Its schema is also more complex, and the
			//   operations more closely resemble a customer workload.
			stopBank := mvt.Workload("bank", workloadNode, bankInit, bankRun)
			stopTPCC := mvt.Workload("tpcc", workloadNode, tpccInit, tpccRun)
			stopSystemWriter := mvt.BackgroundFunc("system table writer", backupTest.systemTableWriter)

			mvt.InMixedVersion("plan and run backups", backupTest.planAndRunBackups)
			mvt.InMixedVersion("verify some backups", backupTest.verifySomeBackups)
			mvt.AfterUpgradeFinalized("verify all backups", backupTest.verifyAllBackups)

			backupTest.stopBackground = func() {
				stopBank()
				stopTPCC()
				stopSystemWriter()
			}
			mvt.Run()
		},
	})
}

func tpccWorkloadCmd(
	numWarehouses int, roachNodes option.NodeListOption,
) (init *roachtestutil.Command, run *roachtestutil.Command) {
	init = roachtestutil.NewCommand("./cockroach workload init tpcc").
		Arg("{pgurl%s}", roachNodes).
		Flag("warehouses", numWarehouses)
	run = roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", roachNodes).
		Flag("warehouses", numWarehouses).
		Option("tolerate-errors")
	return init, run
}

func bankWorkloadCmd(
	testRNG *rand.Rand, roachNodes option.NodeListOption,
) (init *roachtestutil.Command, run *roachtestutil.Command) {
	bankPayload := bankPossiblePayloadBytes[testRNG.Intn(len(bankPossiblePayloadBytes))]
	bankRows := bankPossibleRows[testRNG.Intn(len(bankPossibleRows))]
	// A small number of rows with large payloads will typically
	// lead to really large ranges that may cause the test to fail
	// (e.g., `split failed... cannot find valid split key`). We
	// avoid this combination.
	//
	// TODO(renato): consider reintroducing this combination when
	// #102284 is fixed.
	for bankPayload == largeBankPayload && bankRows == fewBankRows {
		bankPayload = bankPossiblePayloadBytes[testRNG.Intn(len(bankPossiblePayloadBytes))]
		bankRows = bankPossibleRows[testRNG.Intn(len(bankPossibleRows))]
	}

	init = roachtestutil.NewCommand("./cockroach workload init bank").
		Flag("rows", bankRows).
		MaybeFlag(bankPayload != 0, "payload-bytes", bankPayload).
		Flag("ranges", 0).
		Arg("{pgurl%s}", roachNodes)
	run = roachtestutil.NewCommand("./cockroach workload run bank").
		Arg("{pgurl%s}", roachNodes).
		Option("tolerate-errors")

	return init, run
}

type CommonTestUtils struct {
	t          test.Test
	cluster    cluster.Cluster
	roachNodes option.NodeListOption

	connCache struct {
		mu    syncutil.Mutex
		cache []*gosql.DB
	}
}

func newCommonTestUtils(ctx context.Context, t test.Test, c cluster.Cluster, nodes option.NodeListOption) (*CommonTestUtils, error) {
	cc := make([]*gosql.DB, len(nodes))
	for _, node := range nodes {
		conn, err := c.ConnE(ctx, t.L(), node)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to node %d: %w", node, err)
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
	return u, nil
}

func (mvb *mixedVersionBackup) CommonTestUtils(ctx context.Context) (*CommonTestUtils, error) {
	var err error
	mvb.utilsOnce.Do(func() {
		mvb.commonTestUtils, err = newCommonTestUtils(ctx, mvb.t, mvb.cluster, mvb.roachNodes)
	})
	return mvb.commonTestUtils, err
}

func (mvb *mixedVersionBackup) cleanUp(ctx context.Context) error {
	u, err := mvb.CommonTestUtils(ctx)
	if err != nil {
		return err
	}

	u.CloseConnections()
	return nil
}
