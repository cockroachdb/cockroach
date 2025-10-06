// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fingerprintutils

import (
	"context"
	gosql "database/sql"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/gostdlib/x/sync/errgroup"
)

type FingerprintOption struct {
	AOST            hlc.Timestamp
	Stripped        bool
	RevisionHistory bool
	StartTime       hlc.Timestamp
}

func AOST(aost hlc.Timestamp) func(*FingerprintOption) {
	return func(opt *FingerprintOption) {
		opt.AOST = aost
	}
}

func Stripped() func(*FingerprintOption) {
	return func(opt *FingerprintOption) {
		opt.Stripped = true
	}
}

func RevisionHistory() func(*FingerprintOption) {
	return func(opt *FingerprintOption) {
		opt.RevisionHistory = true
	}
}

func StartTime(startTime hlc.Timestamp) func(*FingerprintOption) {
	return func(opt *FingerprintOption) {
		opt.StartTime = startTime
	}
}

func getOpts(optFuncs ...func(*FingerprintOption)) (*FingerprintOption, error) {
	fingerprintOpts := &FingerprintOption{}
	for _, opt := range optFuncs {
		opt(fingerprintOpts)
	}

	if fingerprintOpts.Stripped && !fingerprintOpts.StartTime.IsEmpty() {
		return nil, errors.New("cannot specify stripped and a start time")
	}
	if fingerprintOpts.Stripped && fingerprintOpts.RevisionHistory {
		return nil, errors.New("cannot specify stripped and revision history")
	}
	return fingerprintOpts, nil
}

func FingerprintTable(
	ctx context.Context, db *gosql.DB, tableID uint32, optFuncs ...func(*FingerprintOption),
) (int64, error) {
	opts, err := getOpts(optFuncs...)
	if err != nil {
		return 0, err
	}

	aostCmd := ""
	if !opts.AOST.IsEmpty() {
		aostCmd = fmt.Sprintf("AS OF SYSTEM TIME '%s'", opts.AOST.AsOfSystemTime())
	}

	cmd := fmt.Sprintf(`SELECT * FROM crdb_internal.fingerprint(crdb_internal.table_span(%d),true) %s`, tableID, aostCmd)

	if !opts.Stripped {
		startTime := opts.StartTime.AsOfSystemTime()
		cmd = fmt.Sprintf(`SELECT * FROM crdb_internal.fingerprint(crdb_internal.table_span(%d),'%s'::DECIMAL,%t) %s`, tableID, startTime, opts.RevisionHistory, aostCmd)
	}
	var fingerprint int64
	row := db.QueryRowContext(ctx, cmd)
	if err := row.Scan(&fingerprint); err != nil {
		return 0, errors.Wrapf(err, "fingerprint command failed on table id %d", tableID)
	}
	return fingerprint, nil
}

// FingerprintTables concurrently computes the fingerprints of the passed in tables.
func FingerprintTables(
	ctx context.Context, db *gosql.DB, tableIDs []uint32, optFuncs ...func(*FingerprintOption),
) ([]int64, error) {
	result := make([]int64, len(tableIDs))
	eg, _ := errgroup.WithContext(ctx)
	for j, id := range tableIDs {
		eg.Go(func() error {
			fingerprint, err := FingerprintTable(ctx, db, id, optFuncs...)
			if err != nil {
				return err
			}
			result[j] = fingerprint
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

// FingerprintDatabase fingerprints every table in the database and returns a
// map from tableName to fingerprint.
func FingerprintDatabase(
	ctx context.Context, db *gosql.DB, dbName string, optFuncs ...func(*FingerprintOption),
) (map[string]int64, error) {
	var databaseID int
	row := db.QueryRowContext(ctx, `SELECT id FROM system.namespace WHERE name = $1`, dbName)
	if err := row.Scan(&databaseID); err != nil {
		return nil, errors.New("could not get database descriptor id")
	}
	rows, err := db.QueryContext(ctx, `SELECT id, name FROM system.namespace where "parentID" = $1`, databaseID)
	if err != nil {
		return nil, errors.New("could not get database table name and tableIDs")
	}
	defer rows.Close()
	idToName := make(map[uint32]string)
	tableIDs := make([]uint32, 0)

	for rows.Next() {
		var name string
		var id uint32
		if err := rows.Scan(&id, &name); err != nil {
			return nil, errors.Wrapf(err, "error scanning for table names for database %q", dbName)
		}
		tableIDs = append(tableIDs, id)
		idToName[id] = name
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrapf(err, "error iterating over table_name rows for database %q",
			dbName)
	}
	nameToFingerprint := make(map[string]int64, len(tableIDs))
	fingerprints, err := FingerprintTables(ctx, db, tableIDs, optFuncs...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed fingerprint tables in database %q", dbName)
	}
	for i, fingerprint := range fingerprints {
		id := tableIDs[i]
		name := idToName[id]
		nameToFingerprint[name] = fingerprint
	}
	return nameToFingerprint, err
}

// FingerprintAllDatabases fingerprints all databases in the cluster.
func FingerprintAllDatabases(
	ctx context.Context, db *gosql.DB, includeSystemDB bool, optFuncs ...func(*FingerprintOption),
) (map[string]map[string]int64, error) {

	dbNames := make([]string, 0)
	rows, err := db.QueryContext(ctx, `SELECT database_name FROM [SHOW DATABASES]`)
	if err != nil {
		return nil, errors.Wrap(err, "could not get database names")
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, errors.Wrap(err, "error scanning for db names")
		}
		dbNames = append(dbNames, name)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrapf(err, "error iterating over database names")
	}
	dbFingerprints := make(map[string]map[string]int64)
	for _, dbName := range dbNames {
		if dbName == "system" && !includeSystemDB {
			continue
		}
		dbFingerprint, err := FingerprintDatabase(ctx, db, dbName, optFuncs...)
		if err != nil {
			return nil, err
		}
		dbFingerprints[dbName] = dbFingerprint
	}
	return dbFingerprints, nil
}

// CompareDatabaseFingerprints returns an error if the databases have a fingerprint mismatch.
// The input for each database is map[tableName]fingerprintValue.
func CompareDatabaseFingerprints(db1, db2 map[string]int64) error {
	for tableName, tableFingerprint := range db1 {
		table2Fingerprint, ok := db2[tableName]
		if !ok {
			return errors.Newf("%q table not in second database", tableName)
		}
		if tableFingerprint != table2Fingerprint {
			return errors.Newf("fingerprint mismatch on %q table: %d != %d", tableName,
				tableFingerprint, table2Fingerprint)
		}
		delete(db2, tableName)
	}
	if len(db2) > 0 {
		return errors.Newf("second database has more tables: %s", db2)
	}
	return nil
}

// CompareMultipleDatabaseFingerprints returns an error if the two sets of
// databases have a fingerprint mismatch. The input for each collection of
// databases is map[databaseName]map[tableName]fingerprintValue.
func CompareMultipleDatabaseFingerprints(c1, c2 map[string]map[string]int64) error {
	for dbName, dbFingerprints := range c1 {
		db2Fingerprints, ok := c2[dbName]
		if !ok {
			return errors.Newf("%q table not in second database", dbName)
		}
		if err := CompareDatabaseFingerprints(dbFingerprints, db2Fingerprints); err != nil {
			return errors.Wrapf(err, "failed comparing fingerprints for database %q", dbName)
		}
		delete(c2, dbName)
	}
	if len(c2) > 0 {
		return errors.Newf("second collection has more databases: %s", c2)
	}
	return nil
}
