// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lease

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

type ieWriter struct {
	ie sqlutil.InternalExecutor
}

func (w *ieWriter) deleteLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	const deleteLease = `
DELETE FROM system.public.lease
      WHERE ("descID", version, "nodeID", expiration)
            = ($1, $2, $3, $4);`
	_, err := w.ie.Exec(
		ctx,
		"lease-release",
		nil, /* txn */
		deleteLease,
		l.descID, l.version, l.instanceID, &l.expiration,
	)
	return err
}

func (w *ieWriter) insertLease(ctx context.Context, txn *kv.Txn, l leaseFields) error {
	const insertLease = `
INSERT
  INTO system.public.lease ("descID", version, "nodeID", expiration)
VALUES ($1, $2, $3, $4)`
	count, err := w.ie.Exec(ctx, "lease-insert", txn, insertLease,
		l.descID, l.version, l.instanceID, &l.expiration,
	)
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.Errorf("%s: expected 1 result, found %d", insertLease, count)
	}
	return nil
}

var _ writer = (*ieWriter)(nil)
