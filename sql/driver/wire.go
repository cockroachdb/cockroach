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
	"fmt"
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
	case int64:
		datum.IntVal = &t
	case float64:
		datum.FloatVal = &t
	case bool:
		datum.BoolVal = &t
	case []byte:
		datum.BytesVal = t
	case string:
		datum.StringVal = &t
	case time.Time:
		// Send absolute time devoid of time-zone.
		datum.TimeVal = &Datum_Timestamp{
			Sec:  t.Unix(),
			Nsec: uint32(t.Nanosecond()),
		}
	default:
		return datum, util.Errorf("unsupported type %T", t)
	}

	return datum, nil
}

// Value implements the driver.Valuer interface.
func (d Datum) Value() (driver.Value, error) {
	val := d.GetValue()

	switch t := val.(type) {
	case *bool:
		val = *t
	case *int64:
		val = *t
	case *float64:
		val = *t
	case []byte:
		val = t
	case *string:
		val = *t
	case *Datum_Timestamp:
		val = t.GoTime().UTC()
	}

	if driver.IsValue(val) {
		return val, nil
	}
	return nil, util.Errorf("unsupported type %T", val)
}

func (d Datum) String() string {
	v, err := d.Value()
	if err != nil {
		panic(err)
	}

	if v == nil {
		return "NULL"
	}

	if bytes, ok := v.([]byte); ok {
		return string(bytes)
	}

	return fmt.Sprint(v)
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
