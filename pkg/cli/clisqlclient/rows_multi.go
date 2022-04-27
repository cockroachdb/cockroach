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
	"strconv"
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
	fields := r.rows.ResultReader().FieldDescriptions()
	columnNames := make([]string, len(fields))
	for i, fd := range fields {
		columnNames[i] = string(fd.Name)
	}
	return columnNames
}

func (r *sqlRowsMultiResultSet) Result() driver.Result {
	rd := r.rows.ResultReader()
	tag, _ := rd.Close()
	return driver.RowsAffected(tag.RowsAffected())
}

func (r *sqlRowsMultiResultSet) Tag() string {
	rd := r.rows.ResultReader()
	tag, _ := rd.Close()
	parts := strings.Split(tag.String(), " ")
	if _, err := strconv.Atoi(parts[len(parts)-1]); err == nil {
		parts = parts[:len(parts)-1]
	}
	return strings.Join(parts, " ")
}

func (r *sqlRowsMultiResultSet) Close() error {
	r.conn.flushNotices()
	_, retErr := r.rows.ResultReader().Close()
	if closeErr := r.rows.Close(); closeErr != nil && retErr == nil {
		retErr = closeErr
	}
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
	rawVals := make([]interface{}, len(values))
	for i := range rawVals {
		err := r.connInfo.Scan(
			rd.FieldDescriptions()[i].DataTypeOID,
			rd.FieldDescriptions()[i].Format,
			rd.Values()[i],
			&rawVals[i],
		)
		if err != nil {
			return pgx.ScanArgError{ColumnIndex: i, Err: err}
		}
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
	dataType, ok := r.connInfo.DataTypeForOID(rd.FieldDescriptions()[index].DataTypeOID)
	if !ok {
		return "UNKNOWN"
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
