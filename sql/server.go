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
// Author: Peter Mattis (peter@cockroachlabs.com)
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/gogo/protobuf/proto"
)

func protoFromResult(r Result) driver.Response_Result {
	drr := driver.Response_Result{}
	if r.PErr != nil {
		drr.Error = proto.String(r.PErr.String())
	}
	switch r.Type {
	case parser.DDL:
		drr.Union = &driver.Response_Result_DDL_{
			DDL: &driver.Response_Result_DDL{},
		}
	case parser.RowsAffected:
		drr.Union = &driver.Response_Result_RowsAffected{
			RowsAffected: uint32(r.RowsAffected),
		}
	case parser.Rows:
		rows := &driver.Response_Result_Rows{
			Columns: make([]*driver.Response_Result_Rows_Column, 0, len(r.Columns)),
			Rows:    make([]driver.Response_Result_Rows_Row, 0, len(r.Rows)),
		}
		for _, col := range r.Columns {
			rows.Columns = append(rows.Columns, protoFromColumn(col))
		}
		for _, row := range r.Rows {
			rows.Rows = append(rows.Rows, protoFromRow(row))
		}
		drr.Union = &driver.Response_Result_Rows_{
			Rows: rows,
		}
	}
	return drr
}

func protoFromColumn(c ResultColumn) *driver.Response_Result_Rows_Column {
	return &driver.Response_Result_Rows_Column{
		Name: c.Name,
		Typ:  protoFromDatum(c.Typ),
	}
}

func protoFromRow(r ResultRow) driver.Response_Result_Rows_Row {
	rr := driver.Response_Result_Rows_Row{
		Values: make([]driver.Datum, 0, len(r.Values)),
	}
	for _, v := range r.Values {
		rr.Values = append(rr.Values, protoFromDatum(v))
	}
	return rr
}

func datumFromProto(d driver.Datum) parser.Datum {
	arg := d.Payload
	if arg == nil {
		return parser.DNull
	}
	switch t := arg.(type) {
	case *driver.Datum_BoolVal:
		return parser.DBool(t.BoolVal)
	case *driver.Datum_IntVal:
		return parser.DInt(t.IntVal)
	case *driver.Datum_FloatVal:
		return parser.DFloat(t.FloatVal)
	case *driver.Datum_DecimalVal:
		dd := &parser.DDecimal{}
		if _, ok := dd.SetString(t.DecimalVal); !ok {
			panic(fmt.Sprintf("could not parse string %q as decimal", t.DecimalVal))
		}
		return dd
	case *driver.Datum_BytesVal:
		return parser.DBytes(t.BytesVal)
	case *driver.Datum_StringVal:
		return parser.DString(t.StringVal)
	case *driver.Datum_DateVal:
		return parser.DDate(t.DateVal)
	case *driver.Datum_TimeVal:
		return parser.DTimestamp{Time: t.TimeVal.GoTime()}
	case *driver.Datum_IntervalVal:
		return parser.DInterval{Duration: time.Duration(t.IntervalVal)}
	default:
		panic(fmt.Sprintf("unexpected type %T", t))
	}
}

func protoFromDatum(datum parser.Datum) driver.Datum {
	if datum == parser.DNull {
		return driver.Datum{}
	}

	switch vt := datum.(type) {
	case parser.DBool:
		return driver.Datum{
			Payload: &driver.Datum_BoolVal{BoolVal: bool(vt)},
		}
	case parser.DInt:
		return driver.Datum{
			Payload: &driver.Datum_IntVal{IntVal: int64(vt)},
		}
	case parser.DFloat:
		return driver.Datum{
			Payload: &driver.Datum_FloatVal{FloatVal: float64(vt)},
		}
	case *parser.DDecimal:
		return driver.Datum{
			Payload: &driver.Datum_DecimalVal{DecimalVal: vt.Dec.String()},
		}
	case parser.DBytes:
		return driver.Datum{
			Payload: &driver.Datum_BytesVal{BytesVal: []byte(vt)},
		}
	case parser.DString:
		return driver.Datum{
			Payload: &driver.Datum_StringVal{StringVal: string(vt)},
		}
	case parser.DDate:
		return driver.Datum{
			Payload: &driver.Datum_DateVal{DateVal: int64(vt)},
		}
	case parser.DTimestamp:
		wireTimestamp := driver.Timestamp(vt.Time)
		return driver.Datum{
			Payload: &driver.Datum_TimeVal{
				TimeVal: &wireTimestamp,
			},
		}
	case parser.DInterval:
		return driver.Datum{
			Payload: &driver.Datum_IntervalVal{IntervalVal: vt.Nanoseconds()},
		}
	default:
		panic(util.Errorf("unsupported result type: %s", datum.Type()))
	}
}
