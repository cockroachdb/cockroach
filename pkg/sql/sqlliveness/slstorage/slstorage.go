// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/errors"
)

// Storage implements sqlliveness.Storage.
type Storage struct {
	db *kv.DB
	ie tree.InternalExecutor
}

// NewStorage creates a new storage struct.
func NewStorage(db *kv.DB, ie tree.InternalExecutor) sqlliveness.Storage {
	return &Storage{db: db, ie: ie}
}

// IsAlive returns whether the query session is currently alive. It may return
// true for a session which is no longer alive but will never return false for
// a session which is alive.
func (s *Storage) IsAlive(
	ctx context.Context, txn *kv.Txn, sid sqlliveness.SessionID,
) (alive bool, err error) {
	row, err := s.ie.QueryRow(
		ctx, "expire-single-session", txn, `
SELECT session_id FROM system.sqlliveness WHERE session_id = $1`, sid,
	)
	if err != nil {
		return true, errors.Wrapf(err, "Could not query session id: %s", sid)
	}
	return row != nil, nil
}
