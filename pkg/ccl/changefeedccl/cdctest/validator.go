// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdctest

import (
	"bytes"
	"context"
	gosql "database/sql"
	gojson "encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// Validator checks for violations of our changefeed ordering and delivery
// guarantees in a single table.
type Validator interface {
	// NoteRow accepts a changed row entry.
	NoteRow(partition, key, value string, updated hlc.Timestamp, topic string) error
	// NoteResolved accepts a resolved timestamp entry.
	NoteResolved(partition string, resolved hlc.Timestamp) error
	// Failures returns any violations seen so far.
	Failures() []string
}

// StreamValidator wraps a Validator and exposes additional methods for
// introspection.
type StreamValidator interface {
	Validator
	// GetValuesForKeyBelowTimestamp returns the streamed KV updates for `key`
	// with a ts less than equal to timestamp`.
	GetValuesForKeyBelowTimestamp(key string, timestamp hlc.Timestamp) ([]roachpb.KeyValue, error)
}

type timestampValue struct {
	ts    hlc.Timestamp
	value string
}

type orderValidator struct {
	topic                 string
	partitionForKey       map[string]string
	keyTimestampAndValues map[string][]timestampValue
	resolved              map[string]hlc.Timestamp

	failures []string
}

// NoOpValidator is a validator that does nothing. Useful for
// composition.
var NoOpValidator = &noOpValidator{}

var _ Validator = &orderValidator{}
var _ Validator = &noOpValidator{}
var _ StreamValidator = &orderValidator{}

type noOpValidator struct{}

// NoteRow accepts a changed row entry.
func (v *noOpValidator) NoteRow(string, string, string, hlc.Timestamp, string) error { return nil }

// NoteResolved accepts a resolved timestamp entry.
func (v *noOpValidator) NoteResolved(string, hlc.Timestamp) error { return nil }

// Failures returns any violations seen so far.
func (v *noOpValidator) Failures() []string { return nil }

// NewOrderValidator returns a Validator that checks the row and resolved
// timestamp ordering guarantees. It also asserts that keys have an affinity to
// a single partition.
//
// Once a row with has been emitted with some timestamp, no previously unseen
// versions of that row will be emitted with a lower timestamp.
//
// Once a resolved timestamp has been emitted, no previously unseen rows with a
// lower update timestamp will be emitted on that partition.
func NewOrderValidator(topic string) Validator {
	return &orderValidator{
		topic:                 topic,
		partitionForKey:       make(map[string]string),
		keyTimestampAndValues: make(map[string][]timestampValue),
		resolved:              make(map[string]hlc.Timestamp),
	}
}

// NewStreamOrderValidator wraps an orderValidator as described above, and
// exposes additional methods for introspection.
func NewStreamOrderValidator() StreamValidator {
	return &orderValidator{
		topic:                 "unused",
		partitionForKey:       make(map[string]string),
		keyTimestampAndValues: make(map[string][]timestampValue),
		resolved:              make(map[string]hlc.Timestamp),
	}
}

// GetValuesForKeyBelowTimestamp implements the StreamValidator interface.
func (v *orderValidator) GetValuesForKeyBelowTimestamp(
	key string, timestamp hlc.Timestamp,
) ([]roachpb.KeyValue, error) {
	timestampValueTuples := v.keyTimestampAndValues[key]
	timestampsIdx := sort.Search(len(timestampValueTuples), func(i int) bool {
		return timestamp.Less(timestampValueTuples[i].ts)
	})
	var kv []roachpb.KeyValue
	for _, tsValue := range timestampValueTuples[:timestampsIdx] {
		byteRep := []byte(key)
		kv = append(kv, roachpb.KeyValue{
			Key: byteRep,
			Value: roachpb.Value{
				RawBytes:  []byte(tsValue.value),
				Timestamp: tsValue.ts,
			},
		})
	}

	return kv, nil
}

// NoteRow implements the Validator interface.
func (v *orderValidator) NoteRow(
	partition, key, value string, updated hlc.Timestamp, topic string,
) error {
	if prev, ok := v.partitionForKey[key]; ok && prev != partition {
		v.failures = append(v.failures, fmt.Sprintf(
			`key [%s] received on two partitions: %s and %s`, key, prev, partition,
		))
		return nil
	}
	v.partitionForKey[key] = partition

	timestampValueTuples := v.keyTimestampAndValues[key]
	timestampsIdx := sort.Search(len(timestampValueTuples), func(i int) bool {
		return updated.LessEq(timestampValueTuples[i].ts)
	})
	seen := timestampsIdx < len(timestampValueTuples) &&
		timestampValueTuples[timestampsIdx].ts == updated
	if seen {
		return nil
	}

	if len(timestampValueTuples) > 0 &&
		updated.Less(timestampValueTuples[len(timestampValueTuples)-1].ts) {
		v.failures = append(v.failures, fmt.Sprintf(
			`topic %s partition %s: saw new row timestamp %s after %s was seen`,
			v.topic, partition,
			updated.AsOfSystemTime(),
			timestampValueTuples[len(timestampValueTuples)-1].ts.AsOfSystemTime(),
		))
	}
	latestResolved := v.resolved[partition]
	if updated.Less(latestResolved) {
		v.failures = append(v.failures, fmt.Sprintf(
			`topic %s partition %s: saw new row timestamp %s after %s was resolved`,
			v.topic, partition, updated.AsOfSystemTime(), latestResolved.AsOfSystemTime(),
		))
	}

	v.keyTimestampAndValues[key] = append(
		append(timestampValueTuples[:timestampsIdx], timestampValue{
			ts:    updated,
			value: value,
		}),
		timestampValueTuples[timestampsIdx:]...)
	return nil
}

// NoteResolved implements the Validator interface.
func (v *orderValidator) NoteResolved(partition string, resolved hlc.Timestamp) error {
	prev := v.resolved[partition]
	if prev.Less(resolved) {
		v.resolved[partition] = resolved
	}
	return nil
}

// Failures implements the Validator interface.
func (v *orderValidator) Failures() []string {
	return v.failures
}

type beforeAfterValidator struct {
	sqlDB          *gosql.DB
	table          string
	primaryKeyCols []string
	resolved       map[string]hlc.Timestamp
	diff           bool

	failures []string
}

// NewBeforeAfterValidator returns a Validator verifies that the "before" and
// "after" fields in each row agree with the source table when performing AS OF
// SYSTEM TIME lookups before and at the row's timestamp.
func NewBeforeAfterValidator(sqlDB *gosql.DB, table string, diff bool) (Validator, error) {
	primaryKeyCols, err := fetchPrimaryKeyCols(sqlDB, table)
	if err != nil {
		return nil, errors.Wrap(err, "fetchPrimaryKeyCols failed")
	}

	return &beforeAfterValidator{
		sqlDB:          sqlDB,
		table:          table,
		diff:           diff,
		primaryKeyCols: primaryKeyCols,
		resolved:       make(map[string]hlc.Timestamp),
	}, nil
}

// NoteRow implements the Validator interface.
func (v *beforeAfterValidator) NoteRow(
	partition, key, value string, updated hlc.Timestamp, topic string,
) error {
	keyJSON, err := json.ParseJSON(key)
	if err != nil {
		return err
	}
	keyJSONAsArray, notArray := keyJSON.AsArray()
	if !notArray || len(keyJSONAsArray) != len(v.primaryKeyCols) {
		return errors.Errorf(
			`notArray: %t expected primary key columns %s got datums %s`,
			notArray, v.primaryKeyCols, keyJSONAsArray)
	}

	valueJSON, err := json.ParseJSON(value)
	if err != nil {
		return err
	}

	afterJSON, err := valueJSON.FetchValKey("after")
	if err != nil {
		return err
	}

	beforeJson, err := valueJSON.FetchValKey("before")
	if err != nil {
		return err
	}

	// Check that the "after" field agrees with the row in the table at the
	// updated timestamp.
	if err := v.checkRowAt("after", keyJSONAsArray, afterJSON, updated); err != nil {
		return err
	}

	if !v.diff {
		// If the diff option is not specified in the change feed,
		// we don't expect the rows to contain a "before" field.
		if beforeJson != nil {
			return errors.Errorf(`expected before to be nil, got %s`, beforeJson.String())
		}
		return nil
	}

	if v.resolved[partition].IsEmpty() && (beforeJson == nil || beforeJson.Type() == json.NullJSONType) {
		// If the initial scan hasn't completed for this partition,
		// we don't require the rows to contain a "before" field.
		return nil
	}

	// Check that the "before" field agrees with the row in the table immediately
	// before the updated timestamp.
	return v.checkRowAt("before", keyJSONAsArray, beforeJson, updated.Prev())
}

func (v *beforeAfterValidator) checkRowAt(
	field string, primaryKeyDatums []json.JSON, rowDatums json.JSON, ts hlc.Timestamp,
) error {
	var stmtBuf bytes.Buffer
	var args []interface{}
	if rowDatums == nil || rowDatums.Type() == json.NullJSONType {
		// We expect the row to be missing ...
		stmtBuf.WriteString(`SELECT count(*) = 0 `)
	} else {
		// We expect the row to be present ...
		stmtBuf.WriteString(`SELECT count(*) = 1 `)
	}
	fmt.Fprintf(&stmtBuf, `FROM %s AS OF SYSTEM TIME '%s' WHERE `, v.table, ts.AsOfSystemTime())
	if rowDatums == nil || rowDatums.Type() == json.NullJSONType {
		// ... with the primary key.
		for i, datum := range primaryKeyDatums {
			if len(args) != 0 {
				stmtBuf.WriteString(` AND `)
			}
			if datum == nil || datum.Type() == json.NullJSONType {
				fmt.Fprintf(&stmtBuf, `%s IS NULL`, v.primaryKeyCols[i])
			} else {
				fmt.Fprintf(&stmtBuf, `to_json(%s)::TEXT = $%d`, v.primaryKeyCols[i], i+1)
				args = append(args, datum.String())
			}
		}
	} else {
		// ... and match the specified datums.
		iter, err := rowDatums.ObjectIter()
		if err != nil {
			return err
		}

		colNames := make([]string, 0)
		for iter.Next() {
			colNames = append(colNames, iter.Key())
		}
		sort.Strings(colNames)
		for i, col := range colNames {
			if len(args) != 0 {
				stmtBuf.WriteString(` AND `)
			}
			jsonCol, err := rowDatums.FetchValKey(col)

			if jsonCol == nil || jsonCol.Type() == json.NullJSONType {
				fmt.Fprintf(&stmtBuf, `%s IS NULL`, col)
			} else {
				fmt.Fprintf(&stmtBuf, `to_json(%s)::TEXT = $%d`, col, i+1)
				if err != nil {
					return err
				}
				args = append(args, jsonCol.String())
			}
		}
	}

	var valid bool
	stmt := stmtBuf.String()
	if err := v.sqlDB.QueryRow(stmt, args...).Scan(&valid); err != nil {
		return errors.Wrapf(err, "while executing %s", stmt)
	}
	if !valid {
		v.failures = append(v.failures, fmt.Sprintf(
			"%q field did not agree with row at %s: %s %v",
			field, ts.AsOfSystemTime(), stmt, args))
	}
	return nil
}

// NoteResolved implements the Validator interface.
func (v *beforeAfterValidator) NoteResolved(partition string, resolved hlc.Timestamp) error {
	prev := v.resolved[partition]
	if prev.Less(resolved) {
		v.resolved[partition] = resolved
	}
	return nil
}

// Failures implements the Validator interface.
func (v *beforeAfterValidator) Failures() []string {
	return v.failures
}

type mvccTimestampValidator struct {
	failures []string
}

// NewMvccTimestampValidator returns a Validator that verifies that the emitted
// row includes the mvcc_timestamp field and that its value is not later than
// the updated timestamp
func NewMvccTimestampValidator() Validator {
	return &mvccTimestampValidator{}
}

// NoteRow implements the Validator interface.
func (v *mvccTimestampValidator) NoteRow(
	partition, key, value string, updated hlc.Timestamp, topic string,
) error {
	valueJSON, err := json.ParseJSON(value)
	if err != nil {
		return err
	}

	mvccJSON, err := valueJSON.FetchValKey("mvcc_timestamp")
	if err != nil {
		return err
	}
	if mvccJSON == nil {
		v.failures = append(v.failures, "expected MVCC timestamp, got nil")
		return nil
	}

	mvccJSONText, err := mvccJSON.AsText()
	if err != nil {
		return err
	}

	if *mvccJSONText > updated.AsOfSystemTime() {
		v.failures = append(v.failures, fmt.Sprintf(
			`expected MVCC timestamp to be earlier or equal to updated timestamp (%s), got %s`,
			updated.AsOfSystemTime(), *mvccJSONText))
	}

	// This check is primarily a sanity check. Even in tests, we see the mvcc
	// timestamp regularly differ from the updated timestamp by over 5 seconds.
	// Asserting that the two are within an hour of each other should always pass.
	if *mvccJSONText < updated.AddDuration(-time.Hour).AsOfSystemTime() {
		v.failures = append(v.failures, fmt.Sprintf(
			`expected MVCC timestamp to be within an hour of updated timestamp (%s), got %s`,
			updated.AsOfSystemTime(), *mvccJSONText))
	}

	return nil
}

// NoteResolved implements the Validator interface.
func (v *mvccTimestampValidator) NoteResolved(partition string, resolved hlc.Timestamp) error {
	return nil
}

// Failures implements the Validator interface.
func (v *mvccTimestampValidator) Failures() []string {
	return v.failures
}

type keyInValueValidator struct {
	primaryKeyCols []string
	failures       []string
}

// NewKeyInValueValidator returns a Validator that verifies that the emitted row
// includes the key inside a field named "key" inside the value. It should be
// used only when key_in_value is specified in the changefeed.
func NewKeyInValueValidator(sqlDB *gosql.DB, table string) (Validator, error) {
	primaryKeyCols, err := fetchPrimaryKeyCols(sqlDB, table)
	if err != nil {
		return nil, errors.Wrap(err, "fetchPrimaryKeyCols failed")
	}

	return &keyInValueValidator{
		primaryKeyCols: primaryKeyCols,
	}, nil
}

// NoteRow implements the Validator interface.
func (v *keyInValueValidator) NoteRow(
	partition, key, value string, updated hlc.Timestamp, topic string,
) error {
	keyJSON, err := json.ParseJSON(key)
	if err != nil {
		return err
	}

	valueJSON, err := json.ParseJSON(value)
	if err != nil {
		return err
	}

	keyString := keyJSON.String()
	keyInValueJSON, err := valueJSON.FetchValKey("key")
	if err != nil {
		return err
	}

	if keyInValueJSON == nil {
		v.failures = append(v.failures, fmt.Sprintf(
			"no key in value, expected key value %s", keyString))
	} else {
		keyInValueString := keyInValueJSON.String()
		if keyInValueString != keyString {
			v.failures = append(v.failures, fmt.Sprintf(
				"key in value %s does not match expected key value %s",
				keyInValueString, keyString))
		}
	}

	return nil
}

// NoteResolved implements the Validator interface.
func (v *keyInValueValidator) NoteResolved(partition string, resolved hlc.Timestamp) error {
	return nil
}

// Failures implements the Validator interface.
func (v *keyInValueValidator) Failures() []string {
	return v.failures
}

type topicValidator struct {
	table         string
	fullTableName bool

	failures []string
}

// NewTopicValidator returns a Validator that verifies that the topic field of
// the row agrees with the name of the table. In the case the full_table_name
// option is specified, it checks the topic includes the db and schema name.
func NewTopicValidator(table string, fullTableName bool) Validator {
	return &topicValidator{
		table:         table,
		fullTableName: fullTableName,
	}
}

// NoteRow implements the Validator interface.
func (v *topicValidator) NoteRow(
	partition, key, value string, updated hlc.Timestamp, topic string,
) error {
	if v.fullTableName {
		if topic != fmt.Sprintf(`d.public.%s`, v.table) {
			v.failures = append(v.failures, fmt.Sprintf(
				"topic %s does not match expected table d.public.%s", topic, v.table))
		}
	} else {
		if topic != v.table {
			v.failures = append(v.failures, fmt.Sprintf(
				"topic %s does not match expected table %s", topic, v.table))
		}
	}
	return nil
}

// NoteResolved implements the Validator interface.
func (v *topicValidator) NoteResolved(partition string, resolved hlc.Timestamp) error {
	return nil
}

// Failures implements the Validator interface.
func (v *topicValidator) Failures() []string {
	return v.failures
}

type validatorRow struct {
	key, value string
	updated    hlc.Timestamp
}

// FingerprintValidator verifies that recreating a table from its changefeed
// will fingerprint the same at all "interesting" points in time.
type FingerprintValidator struct {
	sqlDBFunc              func(func(*gosql.DB) error) error
	origTable, fprintTable string
	primaryKeyCols         []string
	partitionResolved      map[string]hlc.Timestamp
	resolved               hlc.Timestamp
	// It's possible to get a resolved timestamp from before the table even
	// exists, which is valid but complicates the way FingerprintValidator works.
	// Don't create a fingerprint earlier than the first seen row.
	firstRowTimestamp hlc.Timestamp
	// previousRowUpdateTs keeps track of the timestamp of the most recently processed row
	// update. Before starting to process row updates belonging to a particular timestamp
	// X, we want to fingerprint at `X.Prev()` to catch any "missed" row updates.
	// Maintaining `previousRowUpdateTs` allows us to do this. See `NoteResolved()` for
	// more details.
	previousRowUpdateTs hlc.Timestamp

	// `fprintOrigColumns` keeps track of the number of non test columns in `fprint`.
	fprintOrigColumns int
	fprintTestColumns int
	buffer            []validatorRow

	failures []string
}

// defaultSQLDBFunc is the default function passed the FingerprintValidator's
// `sqlDBFunc`. It is sufficient in cases when the database is not expected to
// fail while the validator is using it.
func defaultSQLDBFunc(db *gosql.DB) func(func(*gosql.DB) error) error {
	return func(f func(*gosql.DB) error) error {
		return f(db)
	}
}

// NewFingerprintValidator returns a new FingerprintValidator that uses `fprintTable` as
// scratch space to recreate `origTable`. `fprintTable` must exist before calling this
// constructor. `maxTestColumnCount` indicates the maximum number of columns that can be
// expected in `origTable` due to test-related schema changes. This fingerprint validator
// will modify `fprint`'s schema to add `maxTestColumnCount` columns to avoid having to
// accommodate schema changes on the fly.
func NewFingerprintValidator(
	db *gosql.DB, origTable, fprintTable string, partitions []string, maxTestColumnCount int,
) (*FingerprintValidator, error) {
	// Fetch the primary keys though information_schema schema inspections so we
	// can use them to construct the SQL for DELETEs and also so we can verify
	// that the key in a message matches what's expected for the value.
	primaryKeyCols, err := fetchPrimaryKeyCols(db, fprintTable)
	if err != nil {
		return nil, err
	}

	// Record the non-test%d columns in `fprint`.
	var fprintOrigColumns int
	if err := db.QueryRow(`
		SELECT count(column_name)
		FROM information_schema.columns
		WHERE table_name=$1
	`, fprintTable).Scan(&fprintOrigColumns); err != nil {
		return nil, err
	}

	// Add test columns to fprint.
	if maxTestColumnCount > 0 {
		var addColumnStmt bytes.Buffer
		addColumnStmt.WriteString(`ALTER TABLE fprint `)
		for i := 0; i < maxTestColumnCount; i++ {
			if i != 0 {
				addColumnStmt.WriteString(`, `)
			}
			fmt.Fprintf(&addColumnStmt, `ADD COLUMN test%d STRING`, i)
		}
		_, err = db.Exec(addColumnStmt.String())
		if err != nil {
			return nil, err
		}
	}

	v := &FingerprintValidator{
		sqlDBFunc:         defaultSQLDBFunc(db),
		origTable:         origTable,
		fprintTable:       fprintTable,
		primaryKeyCols:    primaryKeyCols,
		fprintOrigColumns: fprintOrigColumns,
		fprintTestColumns: maxTestColumnCount,
	}
	v.partitionResolved = make(map[string]hlc.Timestamp)
	for _, partition := range partitions {
		v.partitionResolved[partition] = hlc.Timestamp{}
	}
	return v, nil
}

// DBFunc sets the database function used when the validator needs to
// perform database operations (updating the scratch table, computing
// fingerprints, etc)
func (v *FingerprintValidator) DBFunc(
	dbFunc func(func(*gosql.DB) error) error,
) *FingerprintValidator {
	v.sqlDBFunc = dbFunc
	return v
}

// NoteRow implements the Validator interface.
func (v *FingerprintValidator) NoteRow(
	partition, key, value string, updated hlc.Timestamp, topic string,
) error {
	if v.firstRowTimestamp.IsEmpty() || updated.Less(v.firstRowTimestamp) {
		v.firstRowTimestamp = updated
	}

	row := validatorRow{key: key, value: value, updated: updated}

	// if this row's timestamp is earlier than the last resolved
	// timestamp we processed, we can skip it as it is a duplicate
	if row.updated.Less(v.resolved) {
		return nil
	}

	v.buffer = append(v.buffer, row)
	return nil
}

// fetchTableColTypes fetches the column types for the given table tableName.
// Note that the table has to exist at the updated timestamp.
func (v *FingerprintValidator) fetchTableColTypes(
	tableName string, updated hlc.Timestamp,
) (map[string]*types.T, error) {
	parts := strings.Split(tableName, ".")
	var dbName string
	if len(parts) == 2 {
		dbName = parts[0] + "."
	}

	colToType := make(map[string]*types.T)
	if err := v.sqlDBFunc(func(sqlDB *gosql.DB) error {
		var rows *gosql.Rows
		queryStr := fmt.Sprintf(`SELECT a.attname AS column_name, t.oid AS type_oid, t.typname AS type_name
		FROM %spg_catalog.pg_attribute a JOIN %spg_catalog.pg_type t ON a.atttypid = t.oid AS OF SYSTEM TIME '%s'
		WHERE a.attrelid = $1::regclass AND a.attnum > 0`, dbName, dbName, updated.AsOfSystemTime())

		rows, err := sqlDB.Query(queryStr, tableName)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		type result struct {
			keyColumn string
			oid       string
			typeName  string
		}

		var results []result
		for rows.Next() {
			var keyColumn, oidStr, typeName string
			if err := rows.Scan(&keyColumn, &oidStr, &typeName); err != nil {
				return err
			}
			if err := rows.Err(); err != nil {
				return err
			}
			results = append(results, result{
				keyColumn: keyColumn,
				oid:       oidStr,
				typeName:  typeName,
			})
			oidNum, err := strconv.Atoi(oidStr)
			if err != nil {
				return err
			}
			colToType[keyColumn] = types.OidToType[oid.Oid(oidNum)]
		}
		if len(results) == 0 {
			return errors.Errorf("no columns found for table %s", tableName)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return colToType, nil
}

// applyRowUpdate applies the update represented by `row` to the scratch table.
func (v *FingerprintValidator) applyRowUpdate(row validatorRow) (_err error) {
	defer func() {
		_err = errors.Wrap(_err, "FingerprintValidator failed")
	}()

	var args []interface{}
	keyJSON, err := json.ParseJSON(row.key)
	if err != nil {
		return err
	}
	keyJSONAsArray, ok := keyJSON.AsArray()
	if !ok || len(keyJSONAsArray) != len(v.primaryKeyCols) {
		return errors.Errorf(
			`notArray: %t expected primary key columns %s got datums %s`,
			ok, v.primaryKeyCols, keyJSONAsArray)
	}

	var stmtBuf bytes.Buffer
	valueJSON, err := json.ParseJSON(row.value)
	if err != nil {
		return err
	}
	afterJSON, err := valueJSON.FetchValKey("after")
	if err != nil {
		return err
	}

	if afterJSON != nil && afterJSON.Type() != json.NullJSONType {
		// UPDATE or INSERT
		fmt.Fprintf(&stmtBuf, `UPSERT INTO %s (`, v.fprintTable)
		iter, err := afterJSON.ObjectIter()
		if err != nil {
			return err
		}
		colNames := make([]string, 0)
		for iter.Next() {
			colNames = append(colNames, iter.Key())
		}

		typeofCol, err := v.fetchTableColTypes(v.origTable, row.updated)
		for _, colValue := range colNames {
			if len(args) != 0 {
				stmtBuf.WriteString(`,`)
			}
			stmtBuf.WriteString(colValue)
			if err != nil {
				return err
			}
			colType, exists := typeofCol[colValue]
			if !exists {
				return errors.Errorf("column %s not found in table %s", colValue, v.origTable)
			}
			jsonValue, err := afterJSON.FetchValKey(colValue)
			if err != nil {
				return err
			}
			str, err := jsonValue.AsText()
			if err != nil {
				return err
			}
			if str != nil {
				datum, _ := rowenc.ParseDatumStringAs(context.Background(), colType, *str,
					eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings()), nil)
				args = append(args, datum)
			} else {
				args = append(args, nil)
			}
		}

		for i := len(colNames) - v.fprintOrigColumns; i < v.fprintTestColumns; i++ {
			fmt.Fprintf(&stmtBuf, `, test%d`, i)
			args = append(args, nil)
		}
		stmtBuf.WriteString(`) VALUES (`)
		for i := range args {
			if i != 0 {
				stmtBuf.WriteString(`,`)
			}
			fmt.Fprintf(&stmtBuf, `$%d`, i+1)
		}
		stmtBuf.WriteString(`)`)

		// Also verify that the key matches the value.
		for idx, primaryKeyCol := range v.primaryKeyCols {
			valueJSON, err := afterJSON.FetchValKey(primaryKeyCol)
			if err != nil {
				return err
			}
			if rowKeyValue := keyJSONAsArray[idx]; valueJSON != rowKeyValue {
				v.failures = append(v.failures,
					fmt.Sprintf(`key %s did not match expected key %s for value %s`,
						primaryKeyCol, rowKeyValue, valueJSON))
			}
		}
	} else {
		// DELETE
		fmt.Fprintf(&stmtBuf, `DELETE FROM %s WHERE `, v.fprintTable)
		for i, datum := range keyJSONAsArray {
			if len(args) != 0 {
				stmtBuf.WriteString(` AND `)
			}
			fmt.Fprintf(&stmtBuf, `to_json(%s)::text = $%d`, v.primaryKeyCols[i], i+1)
			args = append(args, datum.String())
		}
	}

	return v.sqlDBFunc(func(db *gosql.DB) error {
		_, err := db.Exec(stmtBuf.String(), args...)
		return err
	})
}

// NoteResolved implements the Validator interface.
func (v *FingerprintValidator) NoteResolved(partition string, resolved hlc.Timestamp) error {
	if r, ok := v.partitionResolved[partition]; !ok {
		return errors.Errorf(`unknown partition: %s`, partition)
	} else if resolved.LessEq(r) {
		return nil
	}
	v.partitionResolved[partition] = resolved

	// Check if this partition's resolved timestamp advancing has advanced the
	// overall topic resolved timestamp. This is O(n^2) but could be better with
	// a heap, if necessary.
	newResolved := resolved
	for _, r := range v.partitionResolved {
		if r.Less(newResolved) {
			newResolved = r
		}
	}
	if newResolved.LessEq(v.resolved) {
		return nil
	}
	v.resolved = newResolved

	// NB: Intentionally not stable sort because it shouldn't matter.
	sort.Slice(v.buffer, func(i, j int) bool {
		return v.buffer[i].updated.Less(v.buffer[j].updated)
	})

	var lastFingerprintedAt hlc.Timestamp
	// We apply all the row updates we received in the time window between the last
	// resolved timestamp and this one. We process all row updates belonging to a given
	// timestamp and then `fingerprint` to ensure the scratch table and the original table
	// match.
	for len(v.buffer) > 0 {
		if v.resolved.Less(v.buffer[0].updated) {
			break
		}
		row := v.buffer[0]
		// NOTE: changes to the validator's state before `applyRowUpdate`
		// are safe because if database calls can fail, they should be
		// retried by passing a custom function to DBFunction
		v.buffer = v.buffer[1:]

		// If we've processed all row updates belonging to the previous row's timestamp,
		// we fingerprint at `updated.Prev()` since we want to catch cases where one or
		// more row updates are missed. For example: If k1 was written at t1, t2, t3 and
		// the update for t2 was missed.
		if !v.previousRowUpdateTs.IsEmpty() && v.previousRowUpdateTs.Less(row.updated) {
			if err := v.fingerprint(row.updated.Prev()); err != nil {
				return err
			}
		}
		if err := v.applyRowUpdate(row); err != nil {
			return err
		}

		// If any updates have exactly the same timestamp, we have to apply them all
		// before fingerprinting.
		if len(v.buffer) == 0 || v.buffer[0].updated != row.updated {
			lastFingerprintedAt = row.updated
			if err := v.fingerprint(row.updated); err != nil {
				return err
			}
		}
		v.previousRowUpdateTs = row.updated
	}

	if !v.firstRowTimestamp.IsEmpty() && v.firstRowTimestamp.LessEq(resolved) &&
		lastFingerprintedAt != resolved {
		return v.fingerprint(resolved)
	}
	return nil
}

func (v *FingerprintValidator) fingerprint(ts hlc.Timestamp) error {
	var orig string
	if err := v.sqlDBFunc(func(db *gosql.DB) error {
		return db.QueryRow(`SELECT IFNULL(fingerprint, 'EMPTY') FROM [
		SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ` + v.origTable + `
	] AS OF SYSTEM TIME '` + ts.AsOfSystemTime() + `'`).Scan(&orig)
	}); err != nil {
		return err
	}
	var check string
	if err := v.sqlDBFunc(func(db *gosql.DB) error {
		return db.QueryRow(`SELECT IFNULL(fingerprint, 'EMPTY') FROM [
		SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ` + v.fprintTable + `
	]`).Scan(&check)
	}); err != nil {
		return err
	}
	if orig != check {
		v.failures = append(v.failures, fmt.Sprintf(
			`fingerprints did not match at %s: %s vs %s`, ts.AsOfSystemTime(), orig, check))
	}
	return nil
}

// Failures implements the Validator interface.
func (v *FingerprintValidator) Failures() []string {
	return v.failures
}

// Validators abstracts over running multiple `Validator`s at once on the same
// feed.
type Validators []Validator

// NoteRow implements the Validator interface.
func (vs Validators) NoteRow(
	partition, key, value string, updated hlc.Timestamp, topic string,
) error {
	for _, v := range vs {
		if err := v.NoteRow(partition, key, value, updated, topic); err != nil {
			return err
		}
	}
	return nil
}

// NoteResolved implements the Validator interface.
func (vs Validators) NoteResolved(partition string, resolved hlc.Timestamp) error {
	for _, v := range vs {
		if err := v.NoteResolved(partition, resolved); err != nil {
			return err
		}
	}
	return nil
}

// Failures implements the Validator interface.
func (vs Validators) Failures() []string {
	var f []string
	for _, v := range vs {
		f = append(f, v.Failures()...)
	}
	return f
}

// CountValidator wraps a Validator and keeps count of how many rows and
// resolved timestamps have been seen.
type CountValidator struct {
	v Validator

	NumRows, NumResolved                 int
	NumResolvedRows, NumResolvedWithRows int
	rowsSinceResolved                    int
}

// NewCountValidator returns a CountValidator wrapping the given Validator.
func NewCountValidator(v Validator) *CountValidator {
	return &CountValidator{v: v}
}

// NoteRow implements the Validator interface.
func (v *CountValidator) NoteRow(
	partition, key, value string, updated hlc.Timestamp, topic string,
) error {
	v.NumRows++
	v.rowsSinceResolved++
	return v.v.NoteRow(partition, key, value, updated, topic)
}

// NoteResolved implements the Validator interface.
func (v *CountValidator) NoteResolved(partition string, resolved hlc.Timestamp) error {
	v.NumResolved++
	if v.rowsSinceResolved > 0 {
		v.NumResolvedWithRows++
		v.NumResolvedRows += v.rowsSinceResolved
		v.rowsSinceResolved = 0
	}
	return v.v.NoteResolved(partition, resolved)
}

// Failures implements the Validator interface.
func (v *CountValidator) Failures() []string {
	return v.v.Failures()
}

// ParseJSONValueTimestamps returns the updated or resolved timestamp set in the
// provided `format=json` value. Exported for acceptance testing.
func ParseJSONValueTimestamps(v []byte) (updated, resolved hlc.Timestamp, err error) {
	var valueRaw struct {
		Resolved string `json:"resolved"`
		Updated  string `json:"updated"`
	}
	if err := gojson.Unmarshal(v, &valueRaw); err != nil {
		return hlc.Timestamp{}, hlc.Timestamp{}, errors.Wrapf(err, "parsing [%s] as json", v)
	}
	if valueRaw.Updated != `` {
		var err error
		updated, err = hlc.ParseHLC(valueRaw.Updated)
		if err != nil {
			return hlc.Timestamp{}, hlc.Timestamp{}, err
		}
	}
	if valueRaw.Resolved != `` {
		var err error
		resolved, err = hlc.ParseHLC(valueRaw.Resolved)
		if err != nil {
			return hlc.Timestamp{}, hlc.Timestamp{}, err
		}
	}
	return updated, resolved, nil
}

// fetchPrimaryKeyCols fetches the names of the primary key columns for the
// specified table.
func fetchPrimaryKeyCols(sqlDB *gosql.DB, tableStr string) ([]string, error) {
	parts := strings.Split(tableStr, ".")
	var db, table string
	switch len(parts) {
	case 1:
		table = parts[0]
	case 2:
		db = parts[0] + "."
		table = parts[1]
	default:
		return nil, errors.Errorf("could not parse table %s", parts)
	}
	rows, err := sqlDB.Query(fmt.Sprintf(`
		SELECT column_name
		FROM %sinformation_schema.key_column_usage
		WHERE table_name=$1
			AND constraint_name=($1||'_pkey')
		ORDER BY ordinal_position`, db),
		table,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var primaryKeyCols []string
	for rows.Next() {
		var primaryKeyCol string
		if err := rows.Scan(&primaryKeyCol); err != nil {
			return nil, err
		}
		primaryKeyCols = append(primaryKeyCols, primaryKeyCol)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(primaryKeyCols) == 0 {
		return nil, errors.Errorf("no primary key information found for %s", tableStr)
	}
	return primaryKeyCols, nil
}
