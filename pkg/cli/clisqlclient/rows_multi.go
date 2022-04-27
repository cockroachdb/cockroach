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

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

type sqlRowsMultiResultSet struct {
	rows     *pgconn.MultiResultReader
	connInfo *pgtype.ConnInfo

	conn *sqlConn
}

var _ Rows = (*sqlRowsMultiResultSet)(nil)

func (r *sqlRowsMultiResultSet) Columns() []string {
	rr := r.rows.ResultReader()
	if rr == nil {
		// ResultReader may be nil if an empty query was executed.
		return nil
	}
	fields := rr.FieldDescriptions()
	columnNames := make([]string, len(fields))
	for i, fd := range fields {
		columnNames[i] = string(fd.Name)
	}
	return columnNames
}

func (r *sqlRowsMultiResultSet) Tag() (CommandTag, error) {
	if rr := r.rows.ResultReader(); rr != nil {
		// ResultReader may be nil if an empty query was executed.
		return r.rows.ResultReader().Close()
	}
	return pgconn.CommandTag(""), nil
}

func (r *sqlRowsMultiResultSet) Close() (retErr error) {
	r.conn.flushNotices()
	if rr := r.rows.ResultReader(); rr != nil {
		// ResultReader may be nil if an empty query was executed.
		_, retErr = r.rows.ResultReader().Close()
	}
	retErr = errors.CombineErrors(retErr, r.rows.Close())
	if r.conn.conn.IsClosed() {
		r.conn.reconnecting = true
		r.conn.silentClose()
	}
	return retErr
}

// Next implements the Rows interface.
func (r *sqlRowsMultiResultSet) Next(values []driver.Value) error {
	if r.conn.conn.IsClosed() {
		r.conn.reconnecting = true
		r.conn.silentClose()
	}
	rd := r.rows.ResultReader()
	if rd == nil {
		// ResultReader may be nil if an empty query was executed.
		return io.EOF
	}
	if !rd.NextRow() {
		if _, err := rd.Close(); err != nil {
			return err
		}
		return io.EOF
	}
	if len(rd.FieldDescriptions()) != len(values) {
		return errors.AssertionFailedf(
			"number of field descriptions must equal number of destinations, got %d and %d",
			len(rd.FieldDescriptions()),
			len(values),
		)
	}
	for i := range values {
		rowVal := rd.Values()[i]
		if rowVal == nil {
			values[i] = nil
			continue
		}
		fieldOID := rd.FieldDescriptions()[i].DataTypeOID
		fieldFormat := rd.FieldDescriptions()[i].Format
		if dt, ok := r.connInfo.DataTypeForOID(fieldOID); !ok || strings.HasPrefix(dt.Name, "_") {
			// User-defined types and array types are all decoded as raw bytes.
			var b []byte
			err := r.connInfo.Scan(fieldOID, fieldFormat, rowVal, &b)
			if err != nil {
				return pgx.ScanArgError{ColumnIndex: i, Err: err}
			}
			// Copy byte slices as per the comment on Rows.Next.
			values[i] = append([]byte{}, b...)
		} else if fieldOID == pgtype.ByteaOID ||
			fieldOID == pgtype.QCharOID ||
			fieldOID == pgtype.NumericOID ||
			fieldOID == pgtype.RecordOID ||
			fieldOID == pgtype.IntervalOID ||
			fieldOID == pgtype.UUIDOID {
			// BYTEA values are already sent according to the bytea_output setting.
			// QChar and Record values can't be decoded using the default decoder.
			// INTERVAL values are already sent according to the intervalstyle setting.
			// Numeric and UUID values are sent in the correct format, and we can't
			// use the default pgconn type since it uses custom structs that make
			// formatting harder.
			var s string
			err := r.connInfo.Scan(fieldOID, fieldFormat, rowVal, &s)
			if err != nil {
				return pgx.ScanArgError{ColumnIndex: i, Err: err}
			}
			values[i] = s
		} else if fieldOID == pgtype.TimeOID {
			// Use time.Time explicitly, since pgconn defaults to using int64
			// (for microseconds), which complicates formatting logic.
			var t time.Time
			err := r.connInfo.Scan(fieldOID, fieldFormat, rowVal, &t)
			if err != nil {
				return pgx.ScanArgError{ColumnIndex: i, Err: err}
			}
			values[i] = t
		} else {
			// For all other SQL types, let pgconn figure out the go type.
			var v interface{}
			err := r.connInfo.Scan(fieldOID, fieldFormat, rowVal, &v)
			if err != nil {
				return pgx.ScanArgError{ColumnIndex: i, Err: err}
			}
			values[i] = v
		}
	}
	// After the first row was received, we want to delay all
	// further notices until the end of execution.
	r.conn.delayNotices = true
	return nil
}

// NextResultSet prepares the next result set for reading.
func (r *sqlRowsMultiResultSet) NextResultSet() (bool, error) {
	next := r.rows.NextResult()
	if !next {
		if err := r.rows.Close(); err != nil {
			return false, err
		}
	}
	return next, nil
}

func (r *sqlRowsMultiResultSet) ColumnTypeScanType(index int) reflect.Type {
	rd := r.rows.ResultReader()
	o := rd.FieldDescriptions()[index].DataTypeOID
	switch o {
	case pgtype.Int8OID:
		return reflect.TypeOf(int64(0))
	case pgtype.Int4OID:
		return reflect.TypeOf(int32(0))
	case pgtype.Int2OID:
		return reflect.TypeOf(int16(0))
	case pgtype.VarcharOID, pgtype.TextOID:
		return reflect.TypeOf("")
	case pgtype.BoolOID:
		return reflect.TypeOf(false)
	case pgtype.DateOID, pgtype.TimeOID, 1266, pgtype.TimestampOID, pgtype.TimestamptzOID:
		// 1266 is the OID for TimeTZ.
		// TODO(rafi): Add TimetzOID to pgtype.
		return reflect.TypeOf(time.Time{})
	case pgtype.ByteaOID:
		return reflect.TypeOf([]byte(nil))
	default:
		return reflect.TypeOf(new(interface{})).Elem()
	}
}

func (r *sqlRowsMultiResultSet) ColumnTypeDatabaseTypeName(index int) string {
	rd := r.rows.ResultReader()
	fieldOID := rd.FieldDescriptions()[index].DataTypeOID
	dataType, ok := r.connInfo.DataTypeForOID(fieldOID)
	if !ok {
		// TODO(rafi): remove special logic once jackc/pgtype supports these types.
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

func (r *sqlRowsMultiResultSet) ColumnTypeNames() []string {
	colTypes := make([]string, len(r.Columns()))
	for i := range colTypes {
		colTypes[i] = r.ColumnTypeDatabaseTypeName(i)
	}
	return colTypes
}
