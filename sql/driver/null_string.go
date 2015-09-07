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
// Author: Tamir Duberstein (tamird@gmail.com)

package driver

import (
	"database/sql"
	"fmt"
	"time"
)

const (
	dateFormat                    = "2006-01-02"
	timestampWithOffsetZoneFormat = "2006-01-02 15:04:05.999999999-07:00"
)

var _ sql.Scanner = &NullString{}
var _ fmt.Stringer = &NullString{}

// NullString is a thin wrapper around sql.NullString that supports
// scanning from time.Time.
type NullString struct {
	nullString sql.NullString
}

// Scan implements the sql.Scanner interface.
func (ss *NullString) Scan(value interface{}) error {
	if t, ok := value.(time.Time); ok {
		if year, month, day := t.Date(); t.Equal(time.Date(year, month, day, 0, 0, 0, 0, time.UTC)) {
			ss.nullString.String = t.Format(dateFormat)
		} else {
			ss.nullString.String = t.Format(timestampWithOffsetZoneFormat)
		}
		ss.nullString.Valid = true
		return nil
	}
	return ss.nullString.Scan(value)
}

// String implements the fmt.Stringer interface.
func (ss *NullString) String() string {
	if ss.nullString.Valid {
		return ss.nullString.String
	}
	return "NULL"
}
