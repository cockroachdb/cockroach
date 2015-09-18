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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package driver

import (
	"database/sql/driver"
	"time"

	"github.com/cockroachdb/cockroach/util"
)

var _ driver.Valuer = Datum{}

const (
	// Endpoint is the URL path prefix which accepts incoming
	// HTTP requests for the SQL API.
	Endpoint = "/sql/"
)

func makeDatum(val driver.Value) (Datum, error) {
	var datum Datum

	if val == nil {
		return datum, nil
	}
	switch t := val.(type) {
	case bool:
		datum.Payload = &Datum_BoolVal{t}
	case int64:
		datum.Payload = &Datum_IntVal{t}
	case float64:
		datum.Payload = &Datum_FloatVal{t}
	case []byte:
		datum.Payload = &Datum_BytesVal{t}
	case string:
		datum.Payload = &Datum_StringVal{t}
	case time.Time:
		// Send absolute time devoid of time-zone.
		datum.Payload = &Datum_TimeVal{
			&Datum_Timestamp{
				Sec:  t.Unix(),
				Nsec: uint32(t.Nanosecond()),
			},
		}
	default:
		return datum, util.Errorf("unsupported type %T", t)
	}

	return datum, nil
}

// Value implements the driver.Valuer interface.
func (d Datum) Value() (driver.Value, error) {
	var val driver.Value

	switch t := d.Payload.(type) {
	case *Datum_BoolVal:
		val = t.BoolVal
	case *Datum_IntVal:
		val = t.IntVal
	case *Datum_FloatVal:
		val = t.FloatVal
	case *Datum_BytesVal:
		val = t.BytesVal
	case *Datum_StringVal:
		val = t.StringVal
	case *Datum_TimeVal:
		val = t.TimeVal.GoTime().UTC()
	case *Datum_IntervalVal:
		val = t.IntervalVal
	}

	if driver.IsValue(val) {
		return val, nil
	}
	return nil, util.Errorf("unsupported type %T", val)
}

// GoTime converts the timestamp to a time.Time.
func (t Datum_Timestamp) GoTime() time.Time {
	return time.Unix(t.Sec, int64(t.Nsec))
}

// Method returns the method.
func (Request) Method() Method {
	return Execute
}

// CreateReply creates an empty response for the request.
func (Request) CreateReply() Response {
	return Response{}
}
