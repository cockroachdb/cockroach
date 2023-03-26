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
	"math/rand"
	"path/filepath"
	"reflect"
	"regexp"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
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
)

var (
	invalidVersionRE = regexp.MustCompile(`[^a-zA-Z0-9\.]`)
	invalidDBNameRE  = regexp.MustCompile(`[\-\.:/]`)

	// retry options while waiting for a backup to complete
	backupCompletionRetryOptions = retry.Options{
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     1 * time.Minute,
		Multiplier:     1.5,
		MaxRetries:     50,
	}

	v231 = func() *version.Version {
		v, err := version.Parse("v23.1.0")
		if err != nil {
			panic(fmt.Sprintf("failure parsing version: %v", err))
		}
		return v
	}()

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
func hasInternalSystemJobs(tc *mixedversion.Context) bool {
	lowestVersion := tc.FromVersion // upgrades
	if tc.FromVersion == clusterupgrade.MainVersion {
		lowestVersion = tc.ToVersion // downgrades
	}

	// Add 'v' prefix expected by `version` package.
	lowestVersion = "v" + lowestVersion
	sv, err := version.Parse(lowestVersion)
	if err != nil {
		panic(fmt.Errorf("internal error: test context version (%s) expected to be parseable: %w", lowestVersion, err))
	}
	return sv.AtLeast(v231)
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

	// backupType is the interface to be implemented by each backup type
	// (table, database, cluster).
	backupType interface {
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
		dbBackup     *databaseBackup
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
		name        string
		table       string
		columns     []string
		rows        map[string]struct{}
		showResults *showSystemResults
	}

	// backupCollection wraps a backup collection (which may or may not
	// contain incremental backups). The associated fingerprint is the
	// expected fingerprint when the corresponding table is restored.
	backupCollection struct {
		btype   backupType
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
		label string
	}
	incrementalBackup struct {
		collection backupCollection
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
		Plan    labeledNodes
		Execute labeledNodes
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

func newEncryptionPassphrase(rng *rand.Rand) encryptionPassphrase {
	minLen := 32
	maxLen := 64
	pwdLen := rng.Intn(maxLen-minLen) + minLen

	return encryptionPassphrase{randutil.RandString(rng, pwdLen, randutil.PrintableKeyAlphabet)}
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

func newTableBackup(rng *rand.Rand, db string, tables []string) *tableBackup {
	target := tables[rng.Intn(len(tables))]
	return &tableBackup{db, target}
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

func newDatabaseBackup(db string, tables []string) *databaseBackup {
	return &databaseBackup{db, tables}
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

func newClusterBackup(db string, tables []string) *clusterBackup {
	return &clusterBackup{
		dbBackup:     newDatabaseBackup(db, tables),
		systemTables: systemTablesInFullClusterBackup,
	}
}

func (cb *clusterBackup) Desc() string                               { return "cluster" }
func (cb *clusterBackup) BackupCommand() string                      { return "BACKUP" }
func (cb *clusterBackup) RestoreCommand(_ string) (string, []string) { return "RESTORE", nil }

func (cb *clusterBackup) TargetTables() []string {
	return append(cb.dbBackup.TargetTables(), tableNamesWithDB("system", cb.systemTables)...)
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
	showCmd := roachtestutil.NewCommand("%s sql", mixedversion.CurrentCockroachPath).
		Option("insecure").
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

func newBackupCollection(name string, btype backupType, options []backupOption) backupCollection {
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
		nonce:   randutil.RandString(nonceRng, nonceLen, randutil.PrintableKeyAlphabet),
	}
}

func (bc *backupCollection) uri() string {
	// Append the `nonce` to the backup name since we are now sharing a
	// global namespace represented by the cockroachdb-backup-testing
	// bucket. The nonce allows multiple people (or TeamCity builds) to
	// be running this test without interfering with one another.
	return fmt.Sprintf("gs://cockroachdb-backup-testing/%s_%s?AUTH=implicit", bc.name, bc.nonce)
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
	roachNodes option.NodeListOption
	// backup collections that are created along the test
	collections []*backupCollection
	// the db being backed up/restored
	db     string
	tables []string
	// counter that is incremented atomically to provide unique
	// identifiers to backups created during the test
	currentBackupID int64

	// stopWorkload can be called to stop the `bank` workload started in
	// this test. Useful when restoring cluster backups, as we don't
	// want a stream of errors in the workload due to the nodes
	// stopping.
	stopWorkload mixedversion.StopFunc
}

func newMixedVersionBackup(
	c cluster.Cluster, roachNodes option.NodeListOption, db string,
) *mixedVersionBackup {
	mvb := &mixedVersionBackup{cluster: c, db: db, roachNodes: roachNodes}
	return mvb
}

// newBackupType chooses a random backup type (table, database,
// cluster) with equal probability.
func (mvb *mixedVersionBackup) newBackupType(rng *rand.Rand) backupType {
	possibleTypes := []backupType{
		newTableBackup(rng, mvb.db, mvb.tables),
		newDatabaseBackup(mvb.db, mvb.tables),
		newClusterBackup(mvb.db, mvb.tables),
	}

	return possibleTypes[rng.Intn(len(possibleTypes))]
}

// setShortJobIntervals increases the frequency of the adopt and
// cancel loops in the job registry. This enables changes to job state
// to be observed faster, and the test to run quicker.
func (*mixedVersionBackup) setShortJobIntervals(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	return setShortJobIntervalsCommon(func(query string, args ...interface{}) error {
		return h.Exec(rng, query, args...)
	})
}

// loadTables returns a list of tables that are part of the database
// with the given name.
func (mvb *mixedVersionBackup) loadTables(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) ([]string, error) {
	node, db := h.RandomDB(rng, mvb.roachNodes)
	l.Printf("loading table information for DB %q via node %d", mvb.db, node)
	query := fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s]", mvb.db)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to read tables for database %s: %w", mvb.db, err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("error scanning table_name for db %s: %w", mvb.db, err)
		}
		tables = append(tables, name)
	}

	l.Printf("database %q has %d tables", mvb.db, len(tables))
	return tables, nil
}

// loadBackupData starts a CSV server in the background and runs
// imports a bank fixture. Blocks until the importing is finished, at
// which point the CSV server is terminated.
func (mvb *mixedVersionBackup) loadBackupData(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	var rows int
	if mvb.cluster.IsLocal() {
		rows = 100
		l.Printf("importing only %d rows (as this is a local run)", rows)
	} else {
		rows = rows3GiB
		l.Printf("importing %d rows", rows)
	}

	csvPort := 8081
	importNode := h.RandomNode(rng, mvb.roachNodes)
	l.Printf("decided to run import on node %d", importNode)

	currentRoach := mixedversion.CurrentCockroachPath
	stopCSVServer := h.Background("csv server", func(bgCtx context.Context, bgLogger *logger.Logger) error {
		cmd := importBankCSVServerCommand(currentRoach, csvPort)
		bgLogger.Printf("running CSV server in the background: %q", cmd)
		if err := mvb.cluster.RunE(bgCtx, mvb.roachNodes, cmd); err != nil {
			return fmt.Errorf("error while running csv server: %w", err)
		}

		return nil
	})
	defer stopCSVServer()
	if err := waitForPort(ctx, l, mvb.roachNodes, csvPort, mvb.cluster); err != nil {
		return err
	}

	err := mvb.cluster.RunE(
		ctx,
		mvb.cluster.Node(importNode),
		importBankCommand(currentRoach, rows, 0 /* ranges */, csvPort, importNode),
	)
	if err != nil {
		return err
	}

	mvb.tables, err = mvb.loadTables(ctx, l, rng, h)
	return err
}

// randomWait waits from 1s to 5m, to allow for the background
// workload to update the underlying table we are backing up.
func (mvb *mixedVersionBackup) randomWait(l *logger.Logger, rng *rand.Rand) {
	durations := []int{1, 10, 60, 5 * 60}
	dur := time.Duration(durations[rng.Intn(len(durations))]) * time.Second
	l.Printf("waiting for %s", dur)
	time.Sleep(dur)
}

func (mvb *mixedVersionBackup) now() string {
	return hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}.AsOfSystemTime()
}

func (mvb *mixedVersionBackup) nextID() int64 {
	return atomic.AddInt64(&mvb.currentBackupID, 1)
}

// backupName returns a descriptive name for a backup depending on the
// state of the test we are in. The given label is also used to
// provide more context. Example: '3_22.2.4-to-current_final'
func (mvb *mixedVersionBackup) backupName(
	id int64, h *mixedversion.Helper, label string, btype backupType,
) string {
	testContext := h.Context()
	var finalizing string
	if testContext.Finalizing {
		finalizing = "_finalizing"
	}

	fromVersion := sanitizeVersionForBackup(testContext.FromVersion)
	toVersion := sanitizeVersionForBackup(testContext.ToVersion)
	sanitizedLabel := strings.ReplaceAll(label, " ", "-")
	sanitizedType := strings.ReplaceAll(btype.Desc(), " ", "-")

	return fmt.Sprintf(
		"%d_%s-to-%s_%s_%s%s",
		id, fromVersion, toVersion, sanitizedType, sanitizedLabel, finalizing,
	)
}

// waitForJobSuccess waits for the given job with the given ID to
// succeed (according to `backupCompletionRetryOptions`). Returns an
// error if the job doesn't succeed within the attempted retries.
func (mvb *mixedVersionBackup) waitForJobSuccess(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper, jobID int,
) error {
	var lastErr error
	node, db := h.RandomDB(rng, mvb.roachNodes)
	l.Printf("querying job status through node %d", node)

	jobsQuery := "system.jobs WHERE id = $1"
	if hasInternalSystemJobs(h.Context()) {
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
// computing tbale contents after a restore, the `previousContents`
// should include the contents of the same tables at the time the
// backup was taken.
func (mvb *mixedVersionBackup) computeTableContents(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	tables []string,
	previousContents []tableContents,
	timestamp string,
	h *mixedversion.Helper,
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
			node, db := h.RandomDB(rng, mvb.roachNodes)
			l.Printf("querying table contents for %s through node %d", table, node)
			var contents tableContents
			var err error
			if strings.HasPrefix(table, "system.") {
				node := h.RandomNode(rng, mvb.roachNodes)
				contents, err = newSystemTableContents(ctx, mvb.cluster, node, db, table, timestamp)
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
func (mvb *mixedVersionBackup) saveContents(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	collection *backupCollection,
	timestamp string,
	h *mixedversion.Helper,
) error {
	contents, err := mvb.computeTableContents(
		ctx, l, rng, collection.tables, nil /* previousContents */, timestamp, h,
	)
	if err != nil {
		return fmt.Errorf("error computing contents for backup %s: %w", collection.name, err)
	}

	collection.contents = contents
	l.Printf("computed contents for %d tables as part of %s", len(collection.contents), collection.name)
	mvb.collections = append(mvb.collections, collection)
	return nil
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
	h *mixedversion.Helper,
) (backupCollection, string, error) {
	tc := h.Context()
	if !tc.Finalizing {
		// don't wait if upgrade is finalizing to increase the chances of
		// creating a backup while upgrade migrations are being run.
		mvb.randomWait(l, rng)
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
		btype := mvb.newBackupType(rng)
		name := mvb.backupName(mvb.nextID(), h, b.label, btype)
		createOptions := newBackupOptions(rng)
		collection = newBackupCollection(name, btype, createOptions)
		l.Printf("creating full backup for %s", collection.name)
	case incrementalBackup:
		collection = b.collection
		latest = " LATEST IN"
		l.Printf("creating incremental backup for %s", collection.name)
	}

	for _, opt := range collection.options {
		options = append(options, opt.String())
	}

	backupTime := mvb.now()
	node, db := h.RandomDB(rng, nodes)

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

	l.Printf("waiting for job %d (%s)", jobID, collection.name)
	if err := mvb.waitForJobSuccess(ctx, l, rng, h, jobID); err != nil {
		return backupCollection{}, "", err
	}

	return collection, backupTime, nil
}

// runJobOnOneOf disables job adoption on cockroach nodes that are not
// in the `nodes` list. The function passed is then executed and job
// adoption is re-enabled at the end of the function. The function
// passed is expected to run statements that trigger job creation.
func (mvb *mixedVersionBackup) runJobOnOneOf(
	ctx context.Context,
	l *logger.Logger,
	nodes option.NodeListOption,
	h *mixedversion.Helper,
	fn func() error,
) error {
	sort.Ints(nodes)
	var disabledNodes option.NodeListOption
	for _, node := range mvb.roachNodes {
		idx := sort.SearchInts(nodes, node)
		if idx == len(nodes) || nodes[idx] != node {
			disabledNodes = append(disabledNodes, node)
		}
	}

	if err := mvb.disableJobAdoption(ctx, l, disabledNodes, h); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	return mvb.enableJobAdoption(ctx, l, disabledNodes)
}

// createBackupCollection creates a new backup collection to be
// restored/verified at the end of the test. A full backup is created,
// and an incremental one is created on top of it. Both backups are
// created according to their respective `backupSpec`, indicating
// where they should be planned and executed.
func (mvb *mixedVersionBackup) createBackupCollection(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	fullBackupSpec backupSpec,
	incBackupSpec backupSpec,
	h *mixedversion.Helper,
) error {
	var collection backupCollection
	var timestamp string

	// Create full backup.
	if err := mvb.runJobOnOneOf(ctx, l, fullBackupSpec.Execute.Nodes, h, func() error {
		var err error
		label := backupCollectionDesc(fullBackupSpec, incBackupSpec)
		collection, timestamp, err = mvb.runBackup(ctx, l, fullBackup{label}, rng, fullBackupSpec.Plan.Nodes, h)
		return err
	}); err != nil {
		return err
	}

	// Create incremental backup.
	if err := mvb.runJobOnOneOf(ctx, l, incBackupSpec.Execute.Nodes, h, func() error {
		var err error
		collection, timestamp, err = mvb.runBackup(ctx, l, incrementalBackup{collection}, rng, incBackupSpec.Plan.Nodes, h)
		return err
	}); err != nil {
		return err
	}

	return mvb.saveContents(ctx, l, rng, &collection, timestamp, h)
}

// sentinelFilePath returns the path to the file that prevents job
// adoption on the given node.
func (mvb *mixedVersionBackup) sentinelFilePath(
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
func (mvb *mixedVersionBackup) disableJobAdoption(
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
func (mvb *mixedVersionBackup) enableJobAdoption(
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
func (mvb *mixedVersionBackup) planAndRunBackups(
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
		fullSpec := backupSpec{Plan: onPrevious, Execute: onNext}
		incSpec := fullSpec
		l.Printf("planning backup: %s", backupCollectionDesc(fullSpec, incSpec))
		if err := mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h); err != nil {
			return err
		}

		// Case 2: plan backups   -> next node
		//         execute backups -> previous node
		fullSpec = backupSpec{Plan: onNext, Execute: onPrevious}
		incSpec = fullSpec
		l.Printf("planning backup: %s", backupCollectionDesc(fullSpec, incSpec))
		if err := mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h); err != nil {
			return err
		}

		// Case 3: plan & execute full backup        -> previous node
		//         plan & execute incremental backup -> next node
		fullSpec = backupSpec{Plan: onPrevious, Execute: onPrevious}
		incSpec = backupSpec{Plan: onNext, Execute: onNext}
		l.Printf("planning backup: %s", backupCollectionDesc(fullSpec, incSpec))
		if err := mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h); err != nil {
			return err
		}

		// Case 4: plan & execute full backup        -> next node
		//         plan & execute incremental backup -> previous node
		fullSpec = backupSpec{Plan: onNext, Execute: onNext}
		incSpec = backupSpec{Plan: onPrevious, Execute: onPrevious}
		l.Printf("planning backup: %s", backupCollectionDesc(fullSpec, incSpec))
		if err := mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h); err != nil {
			return err
		}
		return nil
	}

	l.Printf("all nodes running next version, running backup on arbitrary node")
	fullSpec := backupSpec{Plan: onNext, Execute: onNext}
	incSpec := fullSpec
	return mvb.createBackupCollection(ctx, l, rng, fullSpec, incSpec, h)
}

// resetCluster wipes the entire cluster and starts it again with the
// current latest binary. This is done before we attempt restoring a
// full cluster backup.
func (mvb *mixedVersionBackup) resetCluster(ctx context.Context, l *logger.Logger) error {
	if err := mvb.cluster.WipeE(ctx, l, mvb.roachNodes); err != nil {
		return fmt.Errorf("failed to wipe cluster: %w", err)
	}

	return clusterupgrade.StartWithBinary(
		ctx, l, mvb.cluster, mvb.roachNodes, mixedversion.CurrentCockroachPath, option.DefaultStartOptsNoBackups(),
	)
}

// verifyBackups cycles through all the backup collections created for
// the duration of the test, and verifies that restoring the backups
// results in the same data as when the backup was taken.
func (mvb *mixedVersionBackup) verifyBackups(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	l.Printf("stopping workload")
	mvb.stopWorkload()

	l.Printf("verifying %d collections created during this test", len(mvb.collections))
	for _, collection := range mvb.collections {
		l.Printf("verifying %s", collection.name)

		// Defaults for the database where the backup will be restored,
		// along with the expected names of the tables after restore.
		restoreDB := fmt.Sprintf("restore_%s", invalidDBNameRE.ReplaceAllString(collection.name, "_"))
		restoredTables := tableNamesWithDB(restoreDB, collection.tables)

		// Pre-requisites:
		switch collection.btype.(type) {
		case *clusterBackup:
			// For cluster backups, the restored tables are always the same
			// as the tables we backed up. In addition, we need to wipe the
			// cluster before attempting a restore.
			restoredTables = collection.tables
			if err := mvb.resetCluster(ctx, l); err != nil {
				return err
			}
		case *tableBackup:
			// If we are restoring a table backup , we need to create it
			// first.
			if err := h.Exec(rng, fmt.Sprintf("CREATE DATABASE %s", restoreDB)); err != nil {
				return fmt.Errorf("backup %s: error creating database %s: %w", collection.name, restoreDB, err)
			}
		}

		restoreCmd, options := collection.btype.RestoreCommand(restoreDB)
		restoreOptions := append([]string{}, options...)
		// If the backup was created with an encryption passphrase, we
		// need to include it when restoring as well.
		for _, option := range collection.options {
			if ep, ok := option.(encryptionPassphrase); ok {
				restoreOptions = append(restoreOptions, ep.String())
			}
		}

		var optionsStr string
		if len(restoreOptions) > 0 {
			optionsStr = fmt.Sprintf(" WITH %s", strings.Join(restoreOptions, ", "))
		}
		restoreStmt := fmt.Sprintf(
			"%s FROM LATEST IN '%s'%s",
			restoreCmd, collection.uri(), optionsStr,
		)
		if err := h.Exec(rng, restoreStmt); err != nil {
			return fmt.Errorf("backup %s: error restoring: %w", collection.name, err)
		}

		restoredContents, err := mvb.computeTableContents(
			ctx, l, rng, restoredTables, collection.contents, "" /* timestamp */, h,
		)
		if err != nil {
			return fmt.Errorf("backup %s: error loading restored contents: %w", collection.name, err)
		}

		for j, contents := range collection.contents {
			table := collection.tables[j]
			restoredTableContents := restoredContents[j]
			l.Printf("%s: verifying %s", collection.name, table)
			if err := contents.ValidateRestore(ctx, l, restoredTableContents); err != nil {
				return fmt.Errorf("backup %s: %w", collection.name, err)
			}
		}

		l.Printf("%s: OK", collection.name)
	}

	return nil
}

func registerBackupMixedVersion(r registry.Registry) {
	// backup/mixed-version tests different states of backup in a mixed
	// version cluster. The actual state of the cluster when a backup is
	// executed is randomized, so each run of the test will exercise a
	// different set of events. Reusing the same seed will produce the
	// same test.
	r.Add(registry.TestSpec{
		Name:              "backup/mixed-version",
		Timeout:           5 * time.Hour,
		Owner:             registry.OwnerDisasterRecovery,
		Cluster:           r.MakeClusterSpec(5),
		EncryptionSupport: registry.EncryptionMetamorphic,
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
			mvb := newMixedVersionBackup(c, roachNodes, "bank")
			mvt.OnStartup("set short job interval", mvb.setShortJobIntervals)
			mvt.OnStartup("load backup data", mvb.loadBackupData)
			mvb.stopWorkload = mvt.Workload("bank", workloadNode, nil /* initCmd */, bankRun)

			mvt.InMixedVersion("plan and run backups", mvb.planAndRunBackups)
			mvt.AfterUpgradeFinalized("verify backups", mvb.verifyBackups)

			mvt.Run()
		},
	})
}
