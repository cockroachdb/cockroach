// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package crdbpgx

import (
	"context"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/jackc/pgx/v4"
)

// ExecuteTx runs fn inside a transaction and retries it as needed. On
// non-retryable failures, the transaction is aborted and rolled back; on
// success, the transaction is committed.
//
// See crdb.ExecuteTx() for more information.
//
// conn can be a pgx.Conn or a pgxpool.Pool.
func ExecuteTx(
	ctx context.Context, conn Conn, txOptions pgx.TxOptions, fn func(pgx.Tx) error,
) error {
	tx, err := conn.BeginTx(ctx, txOptions)
	if err != nil {
		return err
	}
	return crdb.ExecuteInTx(ctx, pgxTxAdapter{tx}, func() error { return fn(tx) })
}

// Conn abstracts pgx transactions creators: pgx.Conn and pgxpool.Pool.
type Conn interface {
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type pgxTxAdapter struct {
	tx pgx.Tx
}

var _ crdb.Tx = pgxTxAdapter{}

func (tx pgxTxAdapter) Commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

func (tx pgxTxAdapter) Rollback(ctx context.Context) error {
	return tx.tx.Rollback(ctx)
}

// Exec is part of the crdb.Tx interface.
func (tx pgxTxAdapter) Exec(ctx context.Context, q string, args ...interface{}) error {
	_, err := tx.tx.Exec(ctx, q, args...)
	return err
}
