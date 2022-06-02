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
	"io"
	"reflect"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

type sqlRows struct {
	rows     pgx.Rows
	connInfo *pgtype.ConnInfo
	conn     *sqlConn
	colNames []string
}

var _ Rows = (*sqlRows)(nil)

func (r *sqlRows) Columns() []string {
	if r.colNames == nil {
		fields := r.rows.FieldDescriptions()
		r.colNames = make([]string, len(fields))
		for i, fd := range fields {
			r.colNames[i] = string(fd.Name)
		}
	}
	return r.colNames
}

func (r *sqlRows) Tag() (CommandTag, error) {
	return r.rows.CommandTag(), r.rows.Err()
}

func (r *sqlRows) Close() error {
	r.conn.flushNotices()
	r.rows.Close()
	if r.conn.conn.IsClosed() {
		r.conn.reconnecting = true
		r.conn.silentClose()
		return ErrConnectionClosed
	}
	return r.rows.Err()
}

// Next implements the Rows interface.
func (r *sqlRows) Next(values []driver.Value) error {
	if r.conn.conn.IsClosed() {
		r.conn.reconnecting = true
		r.conn.silentClose()
		return ErrConnectionClosed
	}
	if !r.rows.Next() {
		return io.EOF
	}
	rawVals, err := r.rows.Values()
	if err != nil {
		return err
	}
	for i, v := range rawVals {
		if b, ok := (v).([]byte); ok {
			// Copy byte slices as per the comment on Rows.Next.
			values[i] = append([]byte{}, b...)
		} else {
			values[i] = v
		}
	}
	// After the first row was received, we want to delay all
	// further notices until the end of execution.
	r.conn.delayNotices = true
	return err
}

// NextResultSet prepares the next result set for reading.
func (r *sqlRows) NextResultSet() (bool, error) {
	return false, nil
}

func (r *sqlRows) ColumnTypeScanType(index int) reflect.Type {
	o := r.rows.FieldDescriptions()[index].DataTypeOID
	n := r.ColumnTypeDatabaseTypeName(index)
	return scanType(o, n)
}

func (r *sqlRows) ColumnTypeDatabaseTypeName(index int) string {
	fieldOID := r.rows.FieldDescriptions()[index].DataTypeOID
	return databaseTypeName(r.connInfo, fieldOID)
}

func (r *sqlRows) ColumnTypeNames() []string {
	colTypes := make([]string, len(r.Columns()))
	for i := range colTypes {
		colTypes[i] = r.ColumnTypeDatabaseTypeName(i)
	}
	return colTypes
}
