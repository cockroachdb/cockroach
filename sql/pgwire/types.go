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
	"fmt"
	"strconv"
	"time"

	"github.com/lib/pq/oid"

	"github.com/cockroachdb/cockroach/sql/driver"
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

func typeForDatum(d driver.Datum) pgType {
	switch d.Payload.(type) {
	case nil:
		return pgType{}

	case *driver.Datum_BoolVal:
		return pgType{oid.T_bool, 1}

	case *driver.Datum_IntVal:
		return pgType{oid.T_int8, 8}

	case *driver.Datum_FloatVal:
		return pgType{oid.T_float8, 8}

	case *driver.Datum_BytesVal, *driver.Datum_StringVal:
		return pgType{oid.T_text, -1}

	case *driver.Datum_DateVal:
		return pgType{oid.T_date, 8}

	case *driver.Datum_TimeVal:
		return pgType{oid.T_timestamp, 8}

	case *driver.Datum_IntervalVal:
		return pgType{oid.T_interval, 8}

	default:
		panic(fmt.Sprintf("unsupported type %T", d.Payload))
	}
}

const secondsInDay = 24 * 60 * 60

func (b *writeBuffer) writeTextDatum(d driver.Datum) error {
	if log.V(2) {
		log.Infof("pgwire writing TEXT datum of type: %T, %#v", d.Payload, d.Payload)
	}
	switch v := d.Payload.(type) {
	case nil:
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return nil
	case *driver.Datum_BoolVal:
		b.putInt32(1)
		if v.BoolVal {
			return b.WriteByte('t')
		}
		return b.WriteByte('f')

	case *driver.Datum_IntVal:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		// TODO(tamird): @petermattis sez this allocates. Investigate.
		s := strconv.AppendInt(b.putbuf[4:4], v.IntVal, 10)
		b.putInt32(int32(len(s)))
		_, err := b.Write(s)
		return err

	case *driver.Datum_FloatVal:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := strconv.AppendFloat(b.putbuf[4:4], v.FloatVal, 'f', -1, 64)
		b.putInt32(int32(len(s)))
		_, err := b.Write(s)
		return err

	case *driver.Datum_BytesVal:
		b.putInt32(int32(len(v.BytesVal)))
		_, err := b.Write(v.BytesVal)
		return err

	case *driver.Datum_StringVal:
		b.putInt32(int32(len(v.StringVal)))
		_, err := b.WriteString(v.StringVal)
		return err

	case *driver.Datum_DateVal:
		t := time.Unix(v.DateVal*secondsInDay, 0).UTC()
		s := formatTs(t)
		b.putInt32(int32(len(s)))
		_, err := b.Write(s)
		return err

	case *driver.Datum_TimeVal:
		t := v.TimeVal.GoTime().UTC()
		s := formatTs(t)
		b.putInt32(int32(len(s)))
		_, err := b.Write(s)
		return err

	case *driver.Datum_IntervalVal:
		s := time.Duration(v.IntervalVal).String()
		b.putInt32(int32(len(s)))
		_, err := b.WriteString(s)
		return err

	default:
		return util.Errorf("unsupported type %T", d.Payload)
	}
}

func (b *writeBuffer) writeBinaryDatum(d driver.Datum) error {
	if log.V(2) {
		log.Infof("pgwire writing BINARY datum of type: %T, %#v", d.Payload, d.Payload)
	}
	switch v := d.Payload.(type) {
	case nil:
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return nil

	case *driver.Datum_IntVal:
		b.putInt32(8)
		b.putInt64(v.IntVal)
		return nil

	default:
		return util.Errorf("unsupported type %T", d.Payload)
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
		oid.T_int8:      parser.DummyInt,
		oid.T_float8:    parser.DummyFloat,
		oid.T_text:      parser.DummyString,
		oid.T_date:      parser.DummyDate,
		oid.T_timestamp: parser.DummyTimestamp,
		oid.T_interval:  parser.DummyInterval,
	}
	datumToOid = map[parser.Datum]oid.Oid{
		parser.DummyBytes:     oid.T_text,
		parser.DummyBool:      oid.T_bool,
		parser.DummyInt:       oid.T_int8,
		parser.DummyFloat:     oid.T_float8,
		parser.DummyString:    oid.T_text,
		parser.DummyDate:      oid.T_date,
		parser.DummyTimestamp: oid.T_timestamp,
		parser.DummyInterval:  oid.T_interval,
	}
)

// decodeOidDatum decodes bytes with specified Oid and format code into
// a datum.
func decodeOidDatum(id oid.Oid, code formatCode, b []byte) (driver.Datum, error) {
	var d driver.Datum
	switch id {
	case oid.T_bool:
		switch code {
		case formatText:
			v, err := strconv.ParseBool(string(b))
			if err != nil {
				return d, fmt.Errorf("unknown bool value")
			}
			d.Payload = &driver.Datum_BoolVal{BoolVal: v}
		default:
			return d, fmt.Errorf("unsupported: binary bool parameter")
		}
	case oid.T_int8:
		switch code {
		case formatText:
			i, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return d, fmt.Errorf("unknown int value")
			}
			d.Payload = &driver.Datum_IntVal{IntVal: i}
		default:
			return d, fmt.Errorf("unsupported: binary int parameter")
		}
	case oid.T_float8:
		switch code {
		case formatText:
			f, err := strconv.ParseFloat(string(b), 64)
			if err != nil {
				return d, fmt.Errorf("unknown float value")
			}
			d.Payload = &driver.Datum_FloatVal{FloatVal: f}
		default:
			return d, fmt.Errorf("unsupported: binary float parameter")
		}
	case oid.T_text:
		switch code {
		case formatText:
			d.Payload = &driver.Datum_StringVal{StringVal: string(b)}
		default:
			return d, fmt.Errorf("unsupported: binary string parameter")
		}
	// TODO(mjibson): implement date/time types
	default:
		return d, fmt.Errorf("unsupported: %v", id)
	}
	return d, nil
}
