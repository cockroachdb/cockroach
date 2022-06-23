/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqltypes

import (
	"errors"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	// ErrNoSuchField indicates a search for a value by an unknown field/column name
	ErrNoSuchField = errors.New("No such field in RowNamedValues")
)

// RowNamedValues contains a row's values as a map based on Field (aka table column) name
type RowNamedValues map[string]Value

// ToString returns the named field as string
func (r RowNamedValues) ToString(fieldName string) (string, error) {
	if v, ok := r[fieldName]; ok {
		return v.ToString(), nil
	}
	return "", ErrNoSuchField
}

// AsString returns the named field as string, or default value if nonexistent/error
func (r RowNamedValues) AsString(fieldName string, def string) string {
	if v, err := r.ToString(fieldName); err == nil {
		return v
	}
	return def
}

// ToInt64 returns the named field as int64
func (r RowNamedValues) ToInt64(fieldName string) (int64, error) {
	if v, ok := r[fieldName]; ok {
		return v.ToInt64()
	}
	return 0, ErrNoSuchField
}

// AsInt64 returns the named field as int64, or default value if nonexistent/error
func (r RowNamedValues) AsInt64(fieldName string, def int64) int64 {
	if v, err := r.ToInt64(fieldName); err == nil {
		return v
	}
	return def
}

// ToUint64 returns the named field as uint64
func (r RowNamedValues) ToUint64(fieldName string) (uint64, error) {
	if v, ok := r[fieldName]; ok {
		return v.ToUint64()
	}
	return 0, ErrNoSuchField
}

// AsUint64 returns the named field as uint64, or default value if nonexistent/error
func (r RowNamedValues) AsUint64(fieldName string, def uint64) uint64 {
	if v, err := r.ToUint64(fieldName); err == nil {
		return v
	}
	return def
}

// ToBool returns the named field as bool
func (r RowNamedValues) ToBool(fieldName string) (bool, error) {
	if v, ok := r[fieldName]; ok {
		return v.ToBool()
	}
	return false, ErrNoSuchField
}

// AsBool returns the named field as bool, or default value if nonexistent/error
func (r RowNamedValues) AsBool(fieldName string, def bool) bool {
	if v, err := r.ToBool(fieldName); err == nil {
		return v
	}
	return def
}

// NamedResult represents a query result with named values as opposed to ordinal values.
type NamedResult struct {
	Fields       []*querypb.Field `json:"fields"`
	RowsAffected uint64           `json:"rows_affected"`
	InsertID     uint64           `json:"insert_id"`
	Rows         []RowNamedValues `json:"rows"`
}

// ToNamedResult converts a Result struct into a new NamedResult struct
func ToNamedResult(result *Result) (r *NamedResult) {
	if result == nil {
		return r
	}
	r = &NamedResult{
		Fields:       result.Fields,
		RowsAffected: result.RowsAffected,
		InsertID:     result.InsertID,
	}
	columnOrdinals := make(map[int]string)
	for i, field := range result.Fields {
		columnOrdinals[i] = field.Name
	}
	r.Rows = make([]RowNamedValues, len(result.Rows))
	for rowIndex, row := range result.Rows {
		namedRow := make(RowNamedValues)
		for i, value := range row {
			namedRow[columnOrdinals[i]] = value
		}
		r.Rows[rowIndex] = namedRow
	}
	return r
}

// Row assumes this result has exactly one row, and returns it, or else returns nil.
// It is useful for queries like:
// - select count(*) from ...
// - select @@read_only
// - select UNIX_TIMESTAMP() from dual
func (r *NamedResult) Row() RowNamedValues {
	if len(r.Rows) != 1 {
		return nil
	}
	return r.Rows[0]
}
