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
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

type sqlRows struct {
	rows     pgx.Rows
	connInfo *pgtype.ConnInfo
	conn     *sqlConn
}

var _ Rows = (*sqlRows)(nil)

func (r *sqlRows) Columns() []string {
	fields := r.rows.FieldDescriptions()
	columnNames := make([]string, len(fields))
	for i, fd := range fields {
		columnNames[i] = string(fd.Name)
	}
	return columnNames
}

func (r *sqlRows) Tag() (pgconn.CommandTag, error) {
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
	return nil
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
	if n == "" || strings.HasPrefix(n, "_") {
		// User-defined types and array types are scanned into []byte.
		return reflect.TypeOf([]byte(nil))
	}
	switch o {
	case pgtype.Int8OID:
		return reflect.TypeOf(int64(0))
	case pgtype.Int4OID:
		return reflect.TypeOf(int32(0))
	case pgtype.Int2OID:
		return reflect.TypeOf(int16(0))
	case pgtype.VarcharOID, pgtype.TextOID, pgtype.NameOID,
		pgtype.ByteaOID, pgtype.NumericOID, pgtype.RecordOID,
		pgtype.QCharOID, pgtype.BPCharOID:
		return reflect.TypeOf("")
	case pgtype.BoolOID:
		return reflect.TypeOf(false)
	case pgtype.DateOID, pgtype.TimeOID, 1266, pgtype.TimestampOID, pgtype.TimestamptzOID:
		// 1266 is the OID for TimeTZ.
		// TODO(rafi): Add TimetzOID to pgtype.
		return reflect.TypeOf(time.Time{})
	default:
		return reflect.TypeOf(new(interface{})).Elem()
	}
}

func (r *sqlRows) ColumnTypeDatabaseTypeName(index int) string {
	fieldOID := r.rows.FieldDescriptions()[index].DataTypeOID
	dataType, ok := r.connInfo.DataTypeForOID(fieldOID)
	if !ok {
		// TODO(rafi): remove special logic once jackc/pgtype includes these types.
		switch fieldOID {
		case 1002:
			return "_CHAR"
		case 1003:
			return "_NAME"
		case 1266:
			return "TIMETZ"
		case 1270:
			return "_TIMETZ"
		default:
			return ""
		}
	}
	return strings.ToUpper(dataType.Name)
}

func (r *sqlRows) ColumnTypeNames() []string {
	colTypes := make([]string, len(r.Columns()))
	for i := range colTypes {
		colTypes[i] = r.ColumnTypeDatabaseTypeName(i)
	}
	return colTypes
}
