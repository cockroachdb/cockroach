// Copyright 2018 The Cockroach Authors.
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

package pgdate

// A field is some piece of information that we can newFieldExtract out of a
// date or time string.
//go:generate stringer -type=field
type field uint

const (
	fieldYear field = iota
	fieldMonth
	fieldDay
	fieldEra
	fieldHour
	fieldMinute
	fieldSecond
	// fieldFraction will store microsecond offsets.
	fieldFraction
	fieldMeridian
	fieldTZ1
	// fieldTZ2 is used for half-hour timezones specified as `+8:30`.
	fieldTZ2

	fieldMinimum = fieldYear
	fieldMaximum = fieldTZ2
)

const (
	fieldValueAM  = -1
	fieldValuePM  = 1
	fieldValueBCE = -1
	fieldValueCE  = 1
)

// Add returns a fieldSet containing the reciver and other.
func (f field) Add(other field) fieldSet {
	return f.AsSet().Add(other)
}

// AsSet returns a singleton set containing the field.
func (f field) AsSet() fieldSet {
	return 1 << f
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

// AsSlice returns the contents of the set as a slice.
func (s fieldSet) AsSlice() []field {
	var ret []field

	for field := fieldMinimum; field <= fieldMaximum; field++ {
		if s.Has(field) {
			ret = append(ret, field)
		}
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

// MaskEquals compares a subset of fields in the set.  This is
// useful for looking as a specific pattern of a subset of all
// of the fields.  The mask parameter specifies the fields to
// compare between the receiver and the other set.
func (s fieldSet) MaskEquals(mask fieldSet, other fieldSet) bool {
	return s&mask == other&mask
}

func (s *fieldSet) String() string {
	ret := "[ "
	for f := fieldMinimum; f <= fieldMaximum; f++ {
		if s.Has(f) {
			ret += f.String()[5:] + " "
		}
	}
	ret += "]"
	return ret
}
