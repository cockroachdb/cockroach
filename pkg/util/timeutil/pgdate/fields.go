// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgdate

//go:generate stringer -type=field

// A field is some piece of information that we can newFieldExtract out of a
// date or time string.
type field uint

// The field values here are used in index into the fieldExtract.data
// array, otherwise we'd use 1 << iota.
const (
	fieldYear field = iota
	fieldMonth
	fieldDay
	fieldEra
	fieldHour
	fieldMinute
	fieldSecond
	fieldNanos
	fieldMeridian
	fieldTZHour
	fieldTZMinute
	fieldTZSecond

	fieldMinimum = fieldYear
	fieldMaximum = fieldTZSecond
)

const (
	fieldValueAM  = -1
	fieldValuePM  = 1
	fieldValueBCE = -1
	fieldValueCE  = 1
)

// AsSet returns a singleton set containing the field.
func (f field) AsSet() fieldSet {
	return 1 << f
}

// Pretty wraps the generated String() function to return only the
// name: "Year" vs "fieldYear".
func (f field) Pretty() string {
	return f.String()[5:]
}

// A fieldSet is an immutable aggregate of fields.
// The zero value is an empty set.
type fieldSet int

// newFieldSet constructs a fieldSet from zero or more fields.
func newFieldSet(fields ...field) fieldSet {
	var ret fieldSet
	for _, f := range fields {
		ret |= f.AsSet()
	}
	return ret
}

// Add returns a fieldSet containing the field.
func (s fieldSet) Add(field field) fieldSet {
	return s.AddAll(field.AsSet())
}

// AddAll returns a fieldSet combining the other fieldSet.
func (s fieldSet) AddAll(other fieldSet) fieldSet {
	return s | other
}

// Clear removes the field from the set.  It is not an error to
// clear a field which is not set.
func (s fieldSet) Clear(field field) fieldSet {
	return s.ClearAll(field.AsSet())
}

// ClearAll removes all fields from other in the set.  It is not an
// error to clear fields which are not set.
func (s fieldSet) ClearAll(other fieldSet) fieldSet {
	return s & ^other
}

// Has returns true if the given field is present in the set.
func (s fieldSet) Has(field field) bool {
	return s&field.AsSet() != 0
}

// HasAll returns true if the field set contains all of the
// other fields.
func (s fieldSet) HasAll(other fieldSet) bool {
	return s&other == other
}

// HasAny returns true if the field set contains any of the
// other fields.
func (s fieldSet) HasAny(other fieldSet) bool {
	return s&other != 0
}

func (s *fieldSet) String() string {
	ret := "[ "
	for f := fieldMinimum; f <= fieldMaximum; f++ {
		if s.Has(f) {
			ret += f.Pretty() + " "
		}
	}
	ret += "]"
	return ret
}
