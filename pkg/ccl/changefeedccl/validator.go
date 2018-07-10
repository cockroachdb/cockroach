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
	"strings"

	"github.com/cockroachdb/apd"
	"github.com/pkg/errors"
)

// OrderValidator checks the row and resolved timestamp ordering guarantees.
//
// Once a row with has been emitted with some timestamp, no previously unseen
// versions of that row will be emitted with a lower timestamp.
//
// Once a resolved timestamp has been emitted, no previously unseen rows with a
// lower update timestamp will be emitted on that partition.
type OrderValidator struct {
	topic           string
	partitionForKey map[string]string
	keyTimestamps   map[string][]string
	resolved        map[string]string

	failures []string
}

// NewOrderValidator returns an OrderValidator for the given topic.
func NewOrderValidator(topic string) *OrderValidator {
	return &OrderValidator{
		topic:           topic,
		partitionForKey: make(map[string]string),
		keyTimestamps:   make(map[string][]string),
		resolved:        make(map[string]string),
	}
}

// NoteRow accepts a changed row entry.
func (v *OrderValidator) NoteRow(partition string, key string, ts string) {
	if prev, ok := v.partitionForKey[key]; ok && prev != partition {
		v.failures = append(v.failures, fmt.Sprintf(
			`key [%s] received on two partitions: %s and %s`, key, prev, partition,
		))
	}
	v.partitionForKey[key] = partition

	timestamps := v.keyTimestamps[key]
	timestampsIdx := sort.SearchStrings(timestamps, ts)
	seen := timestampsIdx < len(timestamps) && timestamps[timestampsIdx] == ts

	if !seen && len(timestamps) > 0 && strings.Compare(ts, timestamps[len(timestamps)-1]) < 0 {
		v.failures = append(v.failures, fmt.Sprintf(
			`topic %s partition %s: saw new row timestamp %s after %s was seen`,
			v.topic, partition, ts, timestamps[len(timestamps)-1],
		))
	}
	if !seen && strings.Compare(ts, v.resolved[partition]) < 0 {
		v.failures = append(v.failures, fmt.Sprintf(
			`topic %s partition %s: saw new row timestamp %s after %s was resolved`,
			v.topic, partition, ts, v.resolved[partition],
		))
	}

	if !seen {
		v.keyTimestamps[key] = append(append(timestamps[:timestampsIdx], ts), timestamps[timestampsIdx:]...)
	}
}

// NoteResolved accepts a resolved timestamp entry.
func (v *OrderValidator) NoteResolved(partition string, ts string) {
	prev := v.resolved[partition]
	if strings.Compare(ts, prev) > 0 {
		v.resolved[partition] = ts
	}
}

// Failures returns any violations seen so far.
func (v *OrderValidator) Failures() []string {
	return v.failures
}

type validatorRow struct {
	key, value string
	ts         string
}

// FingerprintValidator verifies that recreating a table from its changefeed
// will fingerprint the same at all "interesting" points in time.
type FingerprintValidator struct {
	sqlDB                  *gosql.DB
	origTable, fprintTable string
	partitionResolved      map[string]string
	resolved               string

	buffer []validatorRow

	failures []string
}

// NewFingerprintValidator returns a new FingerprintValidator that uses
// `fprintTable` as scratch space to recreate `origTable`.
func NewFingerprintValidator(
	sqlDB *gosql.DB,
	origTable, fprintTable string,
	partitions []string,
) *FingerprintValidator {
	v := &FingerprintValidator{
		sqlDB:       sqlDB,
		origTable:   origTable,
		fprintTable: fprintTable,
	}
	v.partitionResolved = make(map[string]string)
	for _, partition := range partitions {
		v.partitionResolved[partition] = ``
	}
	return v
}

// NoteRow accepts a changed row entry.
func (v *FingerprintValidator) NoteRow(key, value string, ts string) {
	v.buffer = append(v.buffer, validatorRow{
		key:   key,
		value: value,
		ts:    ts,
	})
}

// NoteResolved accepts a resolved timestamp entry.
func (v *FingerprintValidator) NoteResolved(partition string, resolved string) error {
	if r, ok := v.partitionResolved[partition]; !ok {
		return errors.Errorf(`unknown partition: %s`, partition)
	} else if strings.Compare(r, resolved) >= 0 {
		return nil
	}
	v.partitionResolved[partition] = resolved

	// Check if this partition's resolved timestamp advancing has advanced the
	// overall topic resolved timestamp. This is O(n^2) but could be better with
	// a heap, if necessary.
	newResolved := resolved
	for _, r := range v.partitionResolved {
		if strings.Compare(r, newResolved) < 0 {
			newResolved = r
		}
	}
	if strings.Compare(v.resolved, newResolved) >= 0 {
		return nil
	}
	v.resolved = newResolved

	// NB: Intentionally not stable sort because it shouldn't matter.
	sort.Slice(v.buffer, func(i, j int) bool {
		return strings.Compare(v.buffer[i].ts, v.buffer[j].ts) < 0
	})

	var ts string
	for len(v.buffer) > 0 {
		if strings.Compare(v.buffer[0].ts, v.resolved) > 0 {
			break
		}
		row := v.buffer[0]
		v.buffer = v.buffer[1:]

		if row.ts != ts {
			beforeRow, _, _ := apd.NewFromString(row.ts)
			_, _ = apd.BaseContext.Add(beforeRow, beforeRow, apd.New(-1, 0))

			// TODO(dan): The following should be enabled (and the tests
			// unskipped once #27101 is fixed.
			//
			// if ts != `` {
			// 	if err := v.fingerprint(ts); err != nil {
			// 		return err
			// 	}
			// }
			// if err := v.fingerprint(beforeRow.String()); err != nil {
			// 	return err
			// }
		}
		ts = row.ts

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

func (v *FingerprintValidator) fingerprint(ts string) error {
	var orig string
	if err := v.sqlDB.QueryRow(`SELECT IFNULL(fingerprint, '') FROM [
		SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ` + v.origTable + `
	] AS OF SYSTEM TIME '` + ts + `'`).Scan(&orig); err != nil {
		return err
	}
	var check string
	if err := v.sqlDB.QueryRow(`SELECT IFNULL(fingerprint, '') FROM [
		SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ` + v.fprintTable + `
	]`).Scan(&check); err != nil {
		return err
	}
	if orig != check {
		v.failures = append(v.failures, fmt.Sprintf(
			`fingerprints did not match at %s: %s vs %s`, ts, orig, check))
	}
	return nil
}

// Failures returns any violations seen so far.
func (v *FingerprintValidator) Failures() []string {
	return v.failures
}
