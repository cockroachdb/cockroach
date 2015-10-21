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

	// secondsInDay is the number of seconds in a day.
	secondsInDay = 24 * 60 * 60
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
		timestamp := Timestamp(t)
		datum.Payload = &Datum_TimeVal{
			&timestamp,
		}
	case Date:
		datum.Payload = &Datum_DateVal{int64(t)}
	default:
		return datum, util.Errorf("unsupported type %T", t)
	}

	return datum, nil
}

// Date is the number of days since the Unix epoch.
// It provides a custom String() method.
type Date int64

// MakeDate constructs a Date from a time.Time.
func MakeDate(t time.Time) Date {
	year, month, day := t.Date()
	return Date(time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Unix() / secondsInDay)
}

// String returns the underlying time formatted using the format string
// "2006-01-02".
func (d Date) String() string {
	return time.Unix(int64(d)*secondsInDay, 0).UTC().Format("2006-01-02")
}

// Value implements the driver.Valuer interface.
func (d Datum) Value() (driver.Value, error) {
	var val driver.Value

	switch vt := d.Payload.(type) {
	case nil:
		val = vt
	case *Datum_BoolVal:
		val = vt.BoolVal
	case *Datum_IntVal:
		val = vt.IntVal
	case *Datum_FloatVal:
		val = vt.FloatVal
	case *Datum_BytesVal:
		val = vt.BytesVal
	case *Datum_StringVal:
		val = vt.StringVal
	case *Datum_DateVal:
		val = Date(vt.DateVal)
	case *Datum_TimeVal:
		t, err := vt.TimeVal.GoTime()
		if err != nil {
			return nil, err
		}
		val = t
	case *Datum_IntervalVal:
		val = time.Duration(vt.IntervalVal)
	default:
		return nil, util.Errorf("unsupported type %T", vt)
	}

	return val, nil
}

// GoTime converts the timestamp to a time.Time.
func (t Datum_Timestamp) GoTime() (time.Time, error) {
	loc, err := time.LoadLocation(t.Location)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(t.Sec, int64(t.Nsec)).In(loc), nil
}

// Timestamp converts a time.Time to a timestamp.
func Timestamp(t time.Time) Datum_Timestamp {
	return Datum_Timestamp{
		Sec:      t.Unix(),
		Nsec:     uint32(t.Nanosecond()),
		Location: t.Location().String(),
	}
}

// Method returns the method.
func (Request) Method() Method {
	return Execute
}

// CreateReply creates an empty response for the request.
func (Request) CreateReply() Response {
	return Response{}
}
