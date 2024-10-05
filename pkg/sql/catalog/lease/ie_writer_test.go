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
	insertQuery string
	deleteQuery string
	ie          isql.Executor
}

func newInternalExecutorWriter(ie isql.Executor, tableName string) *ieWriter {
	const (
		deleteLease = `
DELETE FROM %s
      WHERE (crdb_region, "descID", version, "nodeID", expiration)
            = ($1, $2, $3, $4, $5);`
		insertLease = `
INSERT
  INTO %s (crdb_region, "descID", version, "nodeID", expiration)
VALUES ($1, $2, $3, $4, $5)`
	)
	return &ieWriter{
		ie:          ie,
		insertQuery: fmt.Sprintf(insertLease, tableName),
		deleteQuery: fmt.Sprintf(deleteLease, tableName),
	}
}

func (w *ieWriter) deleteLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	_, err := w.ie.Exec(
		ctx,
		"lease-release",
		nil, /* txn */
		w.deleteQuery,
		l.regionPrefix, l.descID, l.version, l.instanceID, &l.expiration,
	)
	return err
}

func (w *ieWriter) insertLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	count, err := w.ie.Exec(ctx, "lease-insert", txn, w.insertQuery,
		l.regionPrefix, l.descID, l.version, l.instanceID, &l.expiration,
	)
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.Errorf("%s: expected 1 result, found %d", w.insertQuery, count)
	}
	return nil
}

var _ writer = (*ieWriter)(nil)
