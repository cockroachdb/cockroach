// Copyright 2022 The Cockroach Authors.
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
	"bytes"
	"context"
	"database/sql/driver"
	"io"

	"github.com/jackc/pgconn"
)

// CopyFromState represents an in progress COPY FROM.
type CopyFromState struct {
	conn  *pgconn.PgConn
	query string
}

// BeginCopyFrom starts a COPY FROM query.
func BeginCopyFrom(ctx context.Context, conn Conn, query string) (*CopyFromState, error) {
	copyConn := conn.(*sqlConn).conn.PgConn()
	// Run the initial query, but don't use the result so that we can get any
	// errors early.
	if _, err := copyConn.CopyFrom(ctx, bytes.NewReader([]byte{}), query); err != nil {
		return nil, err
	}
	return &CopyFromState{
		conn:  copyConn,
		query: query,
	}, nil
}

// copyFromRows is a mock Rows interface for COPY results.
type copyFromRows struct {
	t pgconn.CommandTag
}

func (c copyFromRows) Close() error {
	return nil
}

func (c copyFromRows) Columns() []string {
	return nil
}

func (c copyFromRows) ColumnTypeDatabaseTypeName(index int) string {
	return ""
}

func (c copyFromRows) Tag() (CommandTag, error) {
	return c.t, nil
}

func (c copyFromRows) Next(values []driver.Value) error {
	return io.EOF
}

func (c copyFromRows) NextResultSet() (bool, error) {
	return false, nil
}

// Cancel cancels a COPY FROM query from completing.
func (c *CopyFromState) Cancel() error {
	return nil
}

// Commit completes a COPY FROM query by committing lines to the database.
func (c *CopyFromState) Commit(ctx context.Context, cleanupFunc func(), lines string) QueryFn {
	return func(ctx context.Context, conn Conn) (Rows, bool, error) {
		defer cleanupFunc()
		rows, isMulti, err := func() (Rows, bool, error) {
			r := bytes.NewReader([]byte(lines))
			tag, err := c.conn.CopyFrom(ctx, r, c.query)
			if err != nil {
				return nil, false, err
			}
			return copyFromRows{tag}, false, nil
		}()
		return rows, isMulti, err
	}
}
