// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

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
	partitionResolved      map[string]hlc.Timestamp
	resolved               hlc.Timestamp

	buffer []validatorRow

	failures []string
}

// NewFingerprintValidator returns a new FingerprintValidator that uses
// `fprintTable` as scratch space to recreate `origTable`.
func NewFingerprintValidator(
	sqlDB *gosql.DB, origTable, fprintTable string, partitions []string,
) Validator {
	v := &fingerprintValidator{
		sqlDB:       sqlDB,
		origTable:   origTable,
		fprintTable: fprintTable,
	}
	v.partitionResolved = make(map[string]hlc.Timestamp)
	for _, partition := range partitions {
		v.partitionResolved[partition] = hlc.Timestamp{}
	}
	return v
}

// NoteRow implements the Validator interface.
func (v *fingerprintValidator) NoteRow(
	ignoredPartition string, key, value string, updated hlc.Timestamp,
) {
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

		value := make(map[string]interface{})
		if err := gojson.Unmarshal([]byte(row.value), &value); err != nil {
			return err
		}

		var stmtBuf bytes.Buffer
		var args []interface{}
		fmt.Fprintf(&stmtBuf, `UPSERT INTO %s (`, v.fprintTable)
		for col, colValue := range value {
			if col == `__crdb__` {
				continue
			}
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
		if len(args) > 0 {
			if _, err := v.sqlDB.Exec(stmtBuf.String(), args...); err != nil {
				return errors.Wrap(err, stmtBuf.String())
			}
		}
	}

	return v.fingerprint(v.resolved)
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

// ParseJSONValueTimestamps returns the updated or resolved timestamp set in the
// provided `format=json` value. Exported for acceptance testing.
func ParseJSONValueTimestamps(v []byte) (updated, resolved hlc.Timestamp, err error) {
	var valueRaw struct {
		CRDB struct {
			Resolved string `json:"resolved"`
			Updated  string `json:"updated"`
		} `json:"__crdb__"`
	}
	if err := gojson.Unmarshal(v, &valueRaw); err != nil {
		return hlc.Timestamp{}, hlc.Timestamp{}, err
	}
	if valueRaw.CRDB.Updated != `` {
		var err error
		updated, err = sql.ParseHLC(valueRaw.CRDB.Updated)
		if err != nil {
			return hlc.Timestamp{}, hlc.Timestamp{}, err
		}
	}
	if valueRaw.CRDB.Resolved != `` {
		var err error
		resolved, err = sql.ParseHLC(valueRaw.CRDB.Resolved)
		if err != nil {
			return hlc.Timestamp{}, hlc.Timestamp{}, err
		}
	}
	return updated, resolved, nil
}
