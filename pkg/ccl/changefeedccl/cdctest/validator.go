// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdctest

import (
	"bytes"
	gosql "database/sql"
	gojson "encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// Validator checks for violations of our changefeed ordering and delivery
// guarantees in a single table.
type Validator interface {
	// NoteRow accepts a changed row entry.
	NoteRow(partition string, key, value string, updated hlc.Timestamp) error
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

var _ Validator = &orderValidator{}
var _ StreamValidator = &orderValidator{}

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
func (v *orderValidator) NoteRow(partition string, key, value string, updated hlc.Timestamp) error {
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

	if !seen && len(timestampValueTuples) > 0 &&
		updated.Less(timestampValueTuples[len(timestampValueTuples)-1].ts) {
		v.failures = append(v.failures, fmt.Sprintf(
			`topic %s partition %s: saw new row timestamp %s after %s was seen`,
			v.topic, partition,
			updated.AsOfSystemTime(),
			timestampValueTuples[len(timestampValueTuples)-1].ts.AsOfSystemTime(),
		))
	}
	if !seen && updated.Less(v.resolved[partition]) {
		v.failures = append(v.failures, fmt.Sprintf(
			`topic %s partition %s: saw new row timestamp %s after %s was resolved`,
			v.topic, partition, updated.AsOfSystemTime(), v.resolved[partition].AsOfSystemTime(),
		))
	}

	if !seen {
		v.keyTimestampAndValues[key] = append(
			append(timestampValueTuples[:timestampsIdx], timestampValue{
				ts:    updated,
				value: value,
			}),
			timestampValueTuples[timestampsIdx:]...)
	}
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

	failures []string
}

// NewBeforeAfterValidator returns a Validator verifies that the "before" and
// "after" fields in each row agree with the source table when performing AS OF
// SYSTEM TIME lookups before and at the row's timestamp.
func NewBeforeAfterValidator(sqlDB *gosql.DB, table string) (Validator, error) {
	primaryKeyCols, err := fetchPrimaryKeyCols(sqlDB, table)
	if err != nil {
		return nil, errors.Wrap(err, "fetchPrimaryKeyCols failed")
	}

	return &beforeAfterValidator{
		sqlDB:          sqlDB,
		table:          table,
		primaryKeyCols: primaryKeyCols,
		resolved:       make(map[string]hlc.Timestamp),
	}, nil
}

// NoteRow implements the Validator interface.
func (v *beforeAfterValidator) NoteRow(
	partition string, key, value string, updated hlc.Timestamp,
) error {
	var primaryKeyDatums []interface{}
	if err := gojson.Unmarshal([]byte(key), &primaryKeyDatums); err != nil {
		return err
	}
	if len(primaryKeyDatums) != len(v.primaryKeyCols) {
		return errors.Errorf(
			`expected primary key columns %s got datums %s`, v.primaryKeyCols, primaryKeyDatums)
	}

	type wrapper struct {
		After  map[string]interface{} `json:"after"`
		Before map[string]interface{} `json:"before"`
	}
	var rowJSON wrapper
	if err := gojson.Unmarshal([]byte(value), &rowJSON); err != nil {
		return err
	}

	// Check that the "after" field agrees with the row in the table at the
	// updated timestamp.
	if err := v.checkRowAt("after", primaryKeyDatums, rowJSON.After, updated); err != nil {
		return err
	}

	if v.resolved[partition].IsEmpty() && rowJSON.Before == nil {
		// If the initial scan hasn't completed for this partition,
		// we don't require the rows to contain a "before" field.
		return nil
	}

	// Check that the "before" field agrees with the row in the table immediately
	// before the updated timestamp.
	return v.checkRowAt("before", primaryKeyDatums, rowJSON.Before, updated.Prev())
}

func (v *beforeAfterValidator) checkRowAt(
	field string, primaryKeyDatums []interface{}, rowDatums map[string]interface{}, ts hlc.Timestamp,
) error {
	var stmtBuf bytes.Buffer
	var args []interface{}
	if rowDatums == nil {
		// We expect the row to be missing ...
		stmtBuf.WriteString(`SELECT count(*) = 0 `)
	} else {
		// We expect the row to be present ...
		stmtBuf.WriteString(`SELECT count(*) = 1 `)
	}
	fmt.Fprintf(&stmtBuf, `FROM %s AS OF SYSTEM TIME '%s' WHERE `, v.table, ts.AsOfSystemTime())
	if rowDatums == nil {
		// ... with the primary key.
		for i, datum := range primaryKeyDatums {
			if len(args) != 0 {
				stmtBuf.WriteString(` AND `)
			}
			fmt.Fprintf(&stmtBuf, `%s = $%d`, v.primaryKeyCols[i], i+1)
			args = append(args, datum)
		}
	} else {
		// ... and match the specified datums.
		colNames := make([]string, 0, len(rowDatums))
		for col := range rowDatums {
			colNames = append(colNames, col)
		}
		sort.Strings(colNames)
		for i, col := range colNames {
			if len(args) != 0 {
				stmtBuf.WriteString(` AND `)
			}
			fmt.Fprintf(&stmtBuf, `%s = $%d`, col, i+1)
			args = append(args, rowDatums[col])
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

type validatorRow struct {
	key, value string
	updated    hlc.Timestamp
}

// fingerprintValidator verifies that recreating a table from its changefeed
// will fingerprint the same at all "interesting" points in time.
type fingerprintValidator struct {
	sqlDB                  *gosql.DB
	origTable, fprintTable string
	primaryKeyCols         []string
	partitionResolved      map[string]hlc.Timestamp
	resolved               hlc.Timestamp
	// It's possible to get a resolved timestamp from before the table even
	// exists, which is valid but complicates the way fingerprintValidator works.
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

// NewFingerprintValidator returns a new FingerprintValidator that uses `fprintTable` as
// scratch space to recreate `origTable`. `fprintTable` must exist before calling this
// constructor. `maxTestColumnCount` indicates the maximum number of columns that can be
// expected in `origTable` due to test-related schema changes. This fingerprint validator
// will modify `fprint`'s schema to add `maxTestColumnCount` columns to avoid having to
// accommodate schema changes on the fly.
func NewFingerprintValidator(
	sqlDB *gosql.DB, origTable, fprintTable string, partitions []string, maxTestColumnCount int,
) (Validator, error) {
	// Fetch the primary keys though information_schema schema inspections so we
	// can use them to construct the SQL for DELETEs and also so we can verify
	// that the key in a message matches what's expected for the value.
	primaryKeyCols, err := fetchPrimaryKeyCols(sqlDB, fprintTable)
	if err != nil {
		return nil, err
	}

	// Record the non-test%d columns in `fprint`.
	var fprintOrigColumns int
	if err := sqlDB.QueryRow(`
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
		if _, err := sqlDB.Exec(addColumnStmt.String()); err != nil {
			return nil, err
		}
	}

	v := &fingerprintValidator{
		sqlDB:             sqlDB,
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

// NoteRow implements the Validator interface.
func (v *fingerprintValidator) NoteRow(
	ignoredPartition string, key, value string, updated hlc.Timestamp,
) error {
	if v.firstRowTimestamp.IsEmpty() || updated.Less(v.firstRowTimestamp) {
		v.firstRowTimestamp = updated
	}
	v.buffer = append(v.buffer, validatorRow{
		key:     key,
		value:   value,
		updated: updated,
	})
	return nil
}

// applyRowUpdate applies the update represented by `row` to the scratch table.
func (v *fingerprintValidator) applyRowUpdate(row validatorRow) (_err error) {
	defer func() {
		_err = errors.Wrap(_err, "fingerprintValidator failed")
	}()

	var args []interface{}
	var primaryKeyDatums []interface{}
	if err := gojson.Unmarshal([]byte(row.key), &primaryKeyDatums); err != nil {
		return err
	}
	if len(primaryKeyDatums) != len(v.primaryKeyCols) {
		return errors.Errorf(`expected primary key columns %s got datums %s`,
			v.primaryKeyCols, primaryKeyDatums)
	}

	var stmtBuf bytes.Buffer
	type wrapper struct {
		After map[string]interface{} `json:"after"`
	}
	var value wrapper
	if err := gojson.Unmarshal([]byte(row.value), &value); err != nil {
		return err
	}
	if value.After != nil {
		// UPDATE or INSERT
		fmt.Fprintf(&stmtBuf, `UPSERT INTO %s (`, v.fprintTable)
		for col, colValue := range value.After {
			if len(args) != 0 {
				stmtBuf.WriteString(`,`)
			}
			stmtBuf.WriteString(col)
			args = append(args, colValue)
		}
		for i := len(value.After) - v.fprintOrigColumns; i < v.fprintTestColumns; i++ {
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
		primaryKeyDatums = make([]interface{}, len(v.primaryKeyCols))
		for idx, primaryKeyCol := range v.primaryKeyCols {
			primaryKeyDatums[idx] = value.After[primaryKeyCol]
		}
		primaryKeyJSON, err := gojson.Marshal(primaryKeyDatums)
		if err != nil {
			return err
		}

		if string(primaryKeyJSON) != row.key {
			v.failures = append(v.failures,
				fmt.Sprintf(`key %s did not match expected key %s for value %s`,
					row.key, primaryKeyJSON, row.value))
		}
	} else {
		// DELETE
		fmt.Fprintf(&stmtBuf, `DELETE FROM %s WHERE `, v.fprintTable)
		for i, datum := range primaryKeyDatums {
			if len(args) != 0 {
				stmtBuf.WriteString(` AND `)
			}
			fmt.Fprintf(&stmtBuf, `%s = $%d`, v.primaryKeyCols[i], i+1)
			args = append(args, datum)
		}
	}
	_, err := v.sqlDB.Exec(stmtBuf.String(), args...)
	return err
}

// NoteResolved implements the Validator interface.
func (v *fingerprintValidator) NoteResolved(partition string, resolved hlc.Timestamp) error {
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

func (v *fingerprintValidator) fingerprint(ts hlc.Timestamp) error {
	var orig string
	if err := v.sqlDB.QueryRow(`SELECT IFNULL(fingerprint, 'EMPTY') FROM [
		SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ` + v.origTable + `
	] AS OF SYSTEM TIME '` + ts.AsOfSystemTime() + `'`).Scan(&orig); err != nil {
		return err
	}
	var check string
	if err := v.sqlDB.QueryRow(`SELECT IFNULL(fingerprint, 'EMPTY') FROM [
		SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ` + v.fprintTable + `
	]`).Scan(&check); err != nil {
		return err
	}
	if orig != check {
		v.failures = append(v.failures, fmt.Sprintf(
			`fingerprints did not match at %s: %s vs %s`, ts.AsOfSystemTime(), orig, check))
	}
	return nil
}

// Failures implements the Validator interface.
func (v *fingerprintValidator) Failures() []string {
	return v.failures
}

// Validators abstracts over running multiple `Validator`s at once on the same
// feed.
type Validators []Validator

// NoteRow implements the Validator interface.
func (vs Validators) NoteRow(partition string, key, value string, updated hlc.Timestamp) error {
	for _, v := range vs {
		if err := v.NoteRow(partition, key, value, updated); err != nil {
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

// MakeCountValidator returns a CountValidator wrapping the given Validator.
func MakeCountValidator(v Validator) *CountValidator {
	return &CountValidator{v: v}
}

// NoteRow implements the Validator interface.
func (v *CountValidator) NoteRow(partition string, key, value string, updated hlc.Timestamp) error {
	v.NumRows++
	v.rowsSinceResolved++
	return v.v.NoteRow(partition, key, value, updated)
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
		updated, err = sql.ParseHLC(valueRaw.Updated)
		if err != nil {
			return hlc.Timestamp{}, hlc.Timestamp{}, err
		}
	}
	if valueRaw.Resolved != `` {
		var err error
		resolved, err = sql.ParseHLC(valueRaw.Resolved)
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
			AND constraint_name='primary'
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
