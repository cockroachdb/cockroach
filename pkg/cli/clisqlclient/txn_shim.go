// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlclient

import (
	"context"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
)

// sqlTxnShim implements the crdb.Tx interface.
//
// It exists to support crdb.ExecuteInTxn. Normally, we'd hand crdb.ExecuteInTxn
// a sql.Txn, but sqlConn predates go1.8's support for multiple result sets and
// so deals directly with the lib/pq driver. See #14964.
//
// TODO(knz): This code is incorrect, see
// https://github.com/cockroachdb/cockroach/issues/67261
type sqlTxnShim struct {
	conn *sqlConn
}

var _ crdb.Tx = sqlTxnShim{}

func (t sqlTxnShim) Commit(ctx context.Context) error {
	return t.conn.Exec(ctx, `COMMIT`)
}

func (t sqlTxnShim) Rollback(ctx context.Context) error {
	return t.conn.Exec(ctx, `ROLLBACK`)
}

func (t sqlTxnShim) Exec(ctx context.Context, query string, values ...interface{}) error {
	if len(values) != 0 {
		panic("sqlTxnShim.ExecContext must not be called with values")
	}
	return t.conn.Exec(ctx, query)
}
