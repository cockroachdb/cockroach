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
	"database/sql/driver"
	"reflect"

	"github.com/cockroachdb/errors"
)

type sqlRows struct {
	rows sqlRowsI
	conn *sqlConn
}

var _ Rows = (*sqlRows)(nil)

type sqlRowsI interface {
	driver.RowsColumnTypeScanType
	driver.RowsColumnTypeDatabaseTypeName
	Result() driver.Result
	Tag() string

	// Go 1.8 multiple result set interfaces.
	// TODO(mjibson): clean this up after 1.8 is released.
	HasNextResultSet() bool
	NextResultSet() error
}

func (r *sqlRows) Columns() []string {
	return r.rows.Columns()
}

func (r *sqlRows) Result() driver.Result {
	return r.rows.Result()
}

func (r *sqlRows) Tag() string {
	return r.rows.Tag()
}

func (r *sqlRows) Close() error {
	r.conn.flushNotices()
	err := r.rows.Close()
	if errors.Is(err, driver.ErrBadConn) {
		r.conn.reconnecting = true
		r.conn.silentClose()
	}
	return err
}

// Next implements the Rows interface.
func (r *sqlRows) Next(values []driver.Value) error {
	err := r.rows.Next(values)
	if errors.Is(err, driver.ErrBadConn) {
		r.conn.reconnecting = true
		r.conn.silentClose()
	}
	for i, v := range values {
		if b, ok := v.([]byte); ok {
			values[i] = append([]byte{}, b...)
		}
	}
	// After the first row was received, we want to delay all
	// further notices until the end of execution.
	r.conn.delayNotices = true
	return err
}

// NextResultSet prepares the next result set for reading.
func (r *sqlRows) NextResultSet() (bool, error) {
	if !r.rows.HasNextResultSet() {
		return false, nil
	}
	return true, r.rows.NextResultSet()
}

func (r *sqlRows) ColumnTypeScanType(index int) reflect.Type {
	return r.rows.ColumnTypeScanType(index)
}

func (r *sqlRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.rows.ColumnTypeDatabaseTypeName(index)
}

func (r *sqlRows) ColumnTypeNames() []string {
	colTypes := make([]string, len(r.Columns()))
	for i := range colTypes {
		colTypes[i] = r.ColumnTypeDatabaseTypeName(i)
	}
	return colTypes
}
