// Copyright 2015 The Cockroach Authors.
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
//
// Author: Ben Darnell

package pgwire

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/lib/pq/oid"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

//go:generate stringer -type=formatCode
type formatCode int16

const (
	formatText   formatCode = 0
	formatBinary formatCode = 1
)

// pgType contains type metadata used in RowDescription messages.
type pgType struct {
	oid oid.Oid

	// Variable-size types have size=-1.
	// Note that the protocol has both int16 and int32 size fields,
	// so this attribute is an unsized int and should be cast
	// as needed.
	// This field does *not* correspond to the encoded length of a
	// data type, so it's unclear what, if anything, it is used for.
	// To get the right value, "SELECT oid, typlen FROM pg_type"
	// on a postgres server.
	size int
}

func typeForDatum(d parser.Datum) pgType {
	if d == parser.DNull {
		return pgType{}
	}
	switch d.(type) {
	case parser.DBool:
		return pgType{oid.T_bool, 1}

	case parser.DInt:
		return pgType{oid.T_int8, 8}

	case parser.DFloat:
		return pgType{oid.T_float8, 8}

	case parser.DDecimal:
		return pgType{oid.T_numeric, -1}

	case parser.DBytes, parser.DString:
		return pgType{oid.T_text, -1}

	case parser.DDate:
		return pgType{oid.T_date, 8}

	case parser.DTimestamp:
		return pgType{oid.T_timestamp, 8}

	case parser.DInterval:
		return pgType{oid.T_interval, 8}

	default:
		panic(fmt.Sprintf("unsupported type %T", d))
	}
}

const secondsInDay = 24 * 60 * 60

func (b *writeBuffer) writeTextDatum(d parser.Datum) error {
	if log.V(2) {
		log.Infof("pgwire writing TEXT datum of type: %T, %#v", d, d)
	}
	if d == parser.DNull {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return nil
	}
	switch v := d.(type) {
	case parser.DBool:
		b.putInt32(1)
		if v {
			return b.WriteByte('t')
		}
		return b.WriteByte('f')

	case parser.DInt:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		// TODO(tamird): @petermattis sez this allocates. Investigate.
		s := strconv.AppendInt(b.putbuf[4:4], int64(v), 10)
		b.putInt32(int32(len(s)))
		_, err := b.Write(s)
		return err

	case parser.DFloat:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := strconv.AppendFloat(b.putbuf[4:4], float64(v), 'f', -1, 64)
		b.putInt32(int32(len(s)))
		_, err := b.Write(s)
		return err

	case parser.DDecimal:
		vs := v.Dec.String()
		b.putInt32(int32(len(vs)))
		_, err := b.WriteString(vs)
		return err

	case parser.DBytes:
		b.putInt32(int32(len(v)))
		_, err := b.Write([]byte(v))
		return err

	case parser.DString:
		b.putInt32(int32(len(v)))
		_, err := b.WriteString(string(v))
		return err

	case parser.DDate:
		t := time.Unix(int64(v)*secondsInDay, 0).UTC()
		s := formatTs(t)
		b.putInt32(int32(len(s)))
		_, err := b.Write(s)
		return err

	case parser.DTimestamp:
		t := v.UTC()
		s := formatTs(t)
		b.putInt32(int32(len(s)))
		_, err := b.Write(s)
		return err

	case parser.DInterval:
		s := v.String()
		b.putInt32(int32(len(s)))
		_, err := b.WriteString(s)
		return err

	default:
		return util.Errorf("unsupported type %T", d)
	}
}

func (b *writeBuffer) writeBinaryDatum(d parser.Datum) error {
	if log.V(2) {
		log.Infof("pgwire writing BINARY datum of type: %T, %#v", d, d)
	}
	if d == parser.DNull {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return nil
	}
	switch v := d.(type) {
	case parser.DInt:
		b.putInt32(8)
		b.putInt64(int64(v))
		return nil

	default:
		return util.Errorf("unsupported type %T", d)
	}
}

const pgTimeStampFormat = "2006-01-02 15:04:05.999999999-07:00"

// formatTs formats t into a format lib/pq understands.
// Mostly cribbed from github.com/lib/pq.
func formatTs(t time.Time) (b []byte) {
	// Need to send dates before 0001 A.D. with " BC" suffix, instead of the
	// minus sign preferred by Go.
	// Beware, "0000" in ISO is "1 BC", "-0001" is "2 BC" and so on
	bc := false
	if t.Year() <= 0 {
		// flip year sign, and add 1, e.g: "0" will be "1", and "-10" will be "11"
		t = t.AddDate((-t.Year())*2+1, 0, 0)
		bc = true
	}
	b = []byte(t.Format(pgTimeStampFormat))

	_, offset := t.Zone()
	offset = offset % 60
	if offset != 0 {
		// RFC3339Nano already printed the minus sign
		if offset < 0 {
			offset = -offset
		}

		b = append(b, ':')
		if offset < 10 {
			b = append(b, '0')
		}
		b = strconv.AppendInt(b, int64(offset), 10)
	}

	if bc {
		b = append(b, " BC"...)
	}
	return b
}

var (
	oidToDatum = map[oid.Oid]parser.Datum{
		oid.T_bool:      parser.DummyBool,
		oid.T_date:      parser.DummyDate,
		oid.T_float4:    parser.DummyFloat,
		oid.T_float8:    parser.DummyFloat,
		oid.T_int2:      parser.DummyInt,
		oid.T_int4:      parser.DummyInt,
		oid.T_int8:      parser.DummyInt,
		oid.T_interval:  parser.DummyInterval,
		oid.T_numeric:   parser.DummyDecimal,
		oid.T_text:      parser.DummyString,
		oid.T_timestamp: parser.DummyTimestamp,
	}
	// Using reflection to support unhashable types.
	datumToOid = map[reflect.Type]oid.Oid{
		reflect.TypeOf(parser.DummyBool):      oid.T_bool,
		reflect.TypeOf(parser.DummyBytes):     oid.T_text,
		reflect.TypeOf(parser.DummyDate):      oid.T_date,
		reflect.TypeOf(parser.DummyFloat):     oid.T_float8,
		reflect.TypeOf(parser.DummyInt):       oid.T_int8,
		reflect.TypeOf(parser.DummyInterval):  oid.T_interval,
		reflect.TypeOf(parser.DummyDecimal):   oid.T_numeric,
		reflect.TypeOf(parser.DummyString):    oid.T_text,
		reflect.TypeOf(parser.DummyTimestamp): oid.T_timestamp,
	}
)

// decodeOidDatum decodes bytes with specified Oid and format code into
// a datum.
func decodeOidDatum(id oid.Oid, code formatCode, b []byte) (parser.Datum, error) {
	var d parser.Datum
	switch id {
	case oid.T_bool:
		switch code {
		case formatText:
			v, err := strconv.ParseBool(string(b))
			if err != nil {
				return d, err
			}
			d = parser.DBool(v)
		default:
			return d, fmt.Errorf("unsupported bool format code: %d", code)
		}
	case oid.T_int2:
		switch code {
		case formatText:
			i, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return d, err
			}
			d = parser.DInt(i)
		case formatBinary:
			var i int16
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &i)
			if err != nil {
				return d, err
			}
			d = parser.DInt(i)
		default:
			return d, fmt.Errorf("unsupported int2 format code: %d", code)
		}
	case oid.T_int4:
		switch code {
		case formatText:
			i, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return d, err
			}
			d = parser.DInt(i)
		case formatBinary:
			var i int32
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &i)
			if err != nil {
				return d, err
			}
			d = parser.DInt(i)
		default:
			return d, fmt.Errorf("unsupported int4 format code: %d", code)
		}
	case oid.T_int8:
		switch code {
		case formatText:
			i, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return d, err
			}
			d = parser.DInt(i)
		case formatBinary:
			var i int64
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &i)
			if err != nil {
				return d, err
			}
			d = parser.DInt(i)
		default:
			return d, fmt.Errorf("unsupported int8 format code: %d", code)
		}
	case oid.T_float4:
		switch code {
		case formatText:
			f, err := strconv.ParseFloat(string(b), 64)
			if err != nil {
				return d, err
			}
			d = parser.DFloat(f)
		case formatBinary:
			var f float32
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &f)
			if err != nil {
				return d, err
			}
			d = parser.DFloat(f)
		default:
			return d, fmt.Errorf("unsupported float4 format code: %d", code)
		}
	case oid.T_float8:
		switch code {
		case formatText:
			f, err := strconv.ParseFloat(string(b), 64)
			if err != nil {
				return d, err
			}
			d = parser.DFloat(f)
		case formatBinary:
			var f float64
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &f)
			if err != nil {
				return d, err
			}
			d = parser.DFloat(f)
		default:
			return d, fmt.Errorf("unsupported float8 format code: %d", code)
		}
	case oid.T_numeric:
		switch code {
		case formatText:
			dd := parser.DDecimal{}
			if _, ok := dd.SetString(string(b)); !ok {
				return nil, fmt.Errorf("could not parse string %q as decimal", b)
			}
			d = dd
		default:
			return d, fmt.Errorf("unsupported numeric format code: %d", code)
		}
	case oid.T_text:
		switch code {
		case formatText:
			d = parser.DString(b)
		default:
			return d, fmt.Errorf("unsupported text format code: %d", code)
		}
	// TODO(mjibson): implement date/time types
	default:
		return d, fmt.Errorf("unsupported OID: %v", id)
	}
	return d, nil
}
