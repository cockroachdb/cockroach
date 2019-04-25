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

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

// Validator checks for violations of our changefeed ordering and delivery
// guarantees in a single table.
type Validator interface {
	// NoteRow accepts a changed row entry.
	NoteRow(partition string, key, value string, updated hlc.Timestamp)
	// NoteResolved accepts a resolved timestamp entry.
	NoteResolved(partition string, resolved hlc.Timestamp) error
	// Failures returns any violations seen so far.
	Failures() []string
}

type orderValidator struct {
	topic           string
	partitionForKey map[string]string
	keyTimestamps   map[string][]hlc.Timestamp
	resolved        map[string]hlc.Timestamp

	failures []string
}

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
		topic:           topic,
		partitionForKey: make(map[string]string),
		keyTimestamps:   make(map[string][]hlc.Timestamp),
		resolved:        make(map[string]hlc.Timestamp),
	}
}

// NoteRow implements the Validator interface.
func (v *orderValidator) NoteRow(
	partition string, key, ignoredValue string, updated hlc.Timestamp,
) {
	if prev, ok := v.partitionForKey[key]; ok && prev != partition {
		v.failures = append(v.failures, fmt.Sprintf(
			`key [%s] received on two partitions: %s and %s`, key, prev, partition,
		))
		return
	}
	v.partitionForKey[key] = partition

	timestamps := v.keyTimestamps[key]
	timestampsIdx := sort.Search(len(timestamps), func(i int) bool {
		return !timestamps[i].Less(updated)
	})
	seen := timestampsIdx < len(timestamps) && timestamps[timestampsIdx] == updated

	if !seen && len(timestamps) > 0 && updated.Less(timestamps[len(timestamps)-1]) {
		v.failures = append(v.failures, fmt.Sprintf(
			`topic %s partition %s: saw new row timestamp %s after %s was seen`,
			v.topic, partition,
			updated.AsOfSystemTime(), timestamps[len(timestamps)-1].AsOfSystemTime(),
		))
	}
	if !seen && updated.Less(v.resolved[partition]) {
		v.failures = append(v.failures, fmt.Sprintf(
			`topic %s partition %s: saw new row timestamp %s after %s was resolved`,
			v.topic, partition, updated.AsOfSystemTime(), v.resolved[partition].AsOfSystemTime(),
		))
	}

	if !seen {
		v.keyTimestamps[key] = append(
			append(timestamps[:timestampsIdx], updated), timestamps[timestampsIdx:]...)
	}
}

// NoteResolved implements the Validator interface.
func (v *orderValidator) NoteResolved(partition string, resolved hlc.Timestamp) error {
	prev := v.resolved[partition]
	if prev.Less(resolved) {
		v.resolved[partition] = resolved
	}
	return nil
}

func (v *orderValidator) Failures() []string {
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

	buffer []validatorRow

	failures []string
}

// NewFingerprintValidator returns a new FingerprintValidator that uses
// `fprintTable` as scratch space to recreate `origTable`. `fprintTable` must
// exist before calling this constructor.
func NewFingerprintValidator(
	sqlDB *gosql.DB, origTable, fprintTable string, partitions []string,
) (Validator, error) {
	// Fetch the primary keys though information_schema schema inspections so we
	// can use them to construct the SQL for DELETEs and also so we can verify
	// that the key in a message matches what's expected for the value.
	var primaryKeyCols []string
	rows, err := sqlDB.Query(`
		SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_name=$1
			AND constraint_name='primary'
		ORDER BY ordinal_position`,
		fprintTable,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var primaryKeyCol string
		if err := rows.Scan(&primaryKeyCol); err != nil {
			return nil, err
		}
		primaryKeyCols = append(primaryKeyCols, primaryKeyCol)
	}
	if len(primaryKeyCols) == 0 {
		return nil, errors.Errorf("no primary key information found for %s", fprintTable)
	}

	v := &fingerprintValidator{
		sqlDB:          sqlDB,
		origTable:      origTable,
		fprintTable:    fprintTable,
		primaryKeyCols: primaryKeyCols,
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
) {
	if v.firstRowTimestamp.IsEmpty() || updated.Less(v.firstRowTimestamp) {
		v.firstRowTimestamp = updated
	}
	v.buffer = append(v.buffer, validatorRow{
		key:     key,
		value:   value,
		updated: updated,
	})
}

// NoteResolved implements the Validator interface.
func (v *fingerprintValidator) NoteResolved(partition string, resolved hlc.Timestamp) error {
	if r, ok := v.partitionResolved[partition]; !ok {
		return errors.Errorf(`unknown partition: %s`, partition)
	} else if !r.Less(resolved) {
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
	if !v.resolved.Less(newResolved) {
		return nil
	}
	initialScanComplete := v.resolved != (hlc.Timestamp{})
	v.resolved = newResolved

	// NB: Intentionally not stable sort because it shouldn't matter.
	sort.Slice(v.buffer, func(i, j int) bool {
		return v.buffer[i].updated.Less(v.buffer[j].updated)
	})

	var lastUpdated hlc.Timestamp
	for len(v.buffer) > 0 {
		if v.resolved.Less(v.buffer[0].updated) {
			break
		}
		row := v.buffer[0]
		v.buffer = v.buffer[1:]

		// If we have already completed the initial scan, verify the fingerprint at
		// every point in time. Before the initial scan is complete, the fingerprint
		// table might not have the earliest version of every row present in the
		// table.
		if initialScanComplete {
			if row.updated != lastUpdated {
				if lastUpdated != (hlc.Timestamp{}) {
					if err := v.fingerprint(lastUpdated); err != nil {
						return err
					}
				}
				if err := v.fingerprint(row.updated.Prev()); err != nil {
					return err
				}
			}
			lastUpdated = row.updated
		}

		type wrapper struct {
			After map[string]interface{} `json:"after"`
		}
		var value wrapper
		if err := gojson.Unmarshal([]byte(row.value), &value); err != nil {
			return err
		}

		var stmtBuf bytes.Buffer
		var args []interface{}
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
			stmtBuf.WriteString(`) VALUES (`)
			for i := range args {
				if i != 0 {
					stmtBuf.WriteString(`,`)
				}
				fmt.Fprintf(&stmtBuf, `$%d`, i+1)
			}
			stmtBuf.WriteString(`)`)

			// Also verify that the key matches the value.
			primaryKeyDatums := make([]interface{}, len(v.primaryKeyCols))
			for idx, primaryKeyCol := range v.primaryKeyCols {
				primaryKeyDatums[idx] = value.After[primaryKeyCol]
			}
			primaryKeyJSON, err := gojson.Marshal(primaryKeyDatums)
			if err != nil {
				return err
			}
			if string(primaryKeyJSON) != row.key {
				v.failures = append(v.failures, fmt.Sprintf(
					`key %s did not match expected key %s for value %s`, row.key, primaryKeyJSON, row.value))
			}
		} else {
			// DELETE
			var primaryKeyDatums []interface{}
			if err := gojson.Unmarshal([]byte(row.key), &primaryKeyDatums); err != nil {
				return err
			}
			if len(primaryKeyDatums) != len(v.primaryKeyCols) {
				return errors.Errorf(
					`expected primary key columns %s got datums %s`, v.primaryKeyCols, primaryKeyDatums)
			}
			fmt.Fprintf(&stmtBuf, `DELETE FROM %s WHERE `, v.fprintTable)
			for i, datum := range primaryKeyDatums {
				if len(args) != 0 {
					stmtBuf.WriteString(`,`)
				}
				fmt.Fprintf(&stmtBuf, `%s = $%d`, v.primaryKeyCols[i], i+1)
				args = append(args, datum)
			}
		}
		if len(args) > 0 {
			if _, err := v.sqlDB.Exec(stmtBuf.String(), args...); err != nil {
				return errors.Wrap(err, stmtBuf.String())
			}
		}
	}

	if !v.firstRowTimestamp.IsEmpty() && !resolved.Less(v.firstRowTimestamp) {
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
func (vs Validators) NoteRow(partition string, key, value string, updated hlc.Timestamp) {
	for _, v := range vs {
		v.NoteRow(partition, key, value, updated)
	}
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
func (v *CountValidator) NoteRow(partition string, key, value string, updated hlc.Timestamp) {
	v.NumRows++
	v.rowsSinceResolved++
	v.v.NoteRow(partition, key, value, updated)
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
