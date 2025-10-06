// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/errors"
)

type ieWriter struct {
	insertQuery      string
	deleteQuery      string
	ie               isql.Executor
	sessionBasedMode SessionBasedLeasingMode
}

func newInternalExecutorWriter(
	ie isql.Executor, tableName string, mode SessionBasedLeasingMode,
) *ieWriter {
	var (
		deleteLease = `
DELETE FROM %s
      WHERE (crdb_region, "descID", version, "nodeID", expiration)
            = ($1, $2, $3, $4, $5);`
		insertLease = `
INSERT
  INTO %s (crdb_region, "descID", version, "nodeID", expiration)
VALUES ($1, $2, $3, $4, $5)`
	)
	if mode == SessionBasedOnly {
		deleteLease = `
DELETE FROM %s
      WHERE (crdb_region, desc_id, version, sql_instance_id, session_id)
            = ($1, $2, $3, $4, $5);`
		insertLease = `
INSERT
  INTO %s (crdb_region, desc_id, version, sql_instance_id, session_id)
VALUES ($1, $2, $3, $4, $5)`
	}
	return &ieWriter{
		ie:               ie,
		insertQuery:      fmt.Sprintf(insertLease, tableName),
		deleteQuery:      fmt.Sprintf(deleteLease, tableName),
		sessionBasedMode: mode,
	}
}

func (w *ieWriter) deleteLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	var err error
	if w.sessionBasedMode == SessionBasedLeasingOff {
		_, err = w.ie.Exec(
			ctx,
			"lease-release",
			nil, /* txn */
			w.deleteQuery,
			l.regionPrefix, l.descID, l.version, l.instanceID, &l.expiration,
		)
	} else {
		_, err = w.ie.Exec(
			ctx,
			"lease-release",
			nil, /* txn */
			w.deleteQuery,
			l.regionPrefix, l.descID, l.version, l.instanceID, l.sessionID,
		)
	}

	return err
}

func (w *ieWriter) insertLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	var count int
	var err error
	if w.sessionBasedMode == SessionBasedLeasingOff {
		count, err = w.ie.Exec(ctx, "lease-insert", txn, w.insertQuery,
			l.regionPrefix, l.descID, l.version, l.instanceID, &l.expiration,
		)
	} else {
		count, err = w.ie.Exec(ctx, "lease-insert", txn, w.insertQuery,
			l.regionPrefix, l.descID, l.version, l.instanceID, l.sessionID,
		)
	}
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.Errorf("%s: expected 1 result, found %d", w.insertQuery, count)
	}
	return nil
}

var _ writer = (*ieWriter)(nil)
