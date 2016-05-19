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

package parser

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/duration"
)

var (
	constDBoolTrue  DBool = true
	constDBoolFalse DBool = false

	// DBoolTrue is a pointer to the DBool(true) value and can be used in
	// comparisons against Datum types.
	DBoolTrue = &constDBoolTrue
	// DBoolFalse is a pointer to the DBool(false) value and can be used in
	// comparisons against Datum types.
	DBoolFalse = &constDBoolFalse

	// DNull is the NULL Datum.
	DNull Datum = dNull{}
)

// A Datum holds either a bool, int64, float64, string or []Datum.
type Datum interface {
	TypedExpr
	// Type returns the (user-friendly) name of the type.
	Type() string
	// TypeEqual determines if the receiver and the other Datum have the same
	// type or not. This method should be used for asserting the type of all
	// Datum, with the exception of DNull, where it is safe/encouraged to perform
	// a direct equivalence check.
	TypeEqual(other Datum) bool
	// Compare returns -1 if the receiver is less than other, 0 if receiver is
	// equal to other and +1 if receiver is greater than other.
	// TODO(nvanbenschoten) Should we look into merging this with cmpOps?
	Compare(other Datum) int
	// HasPrev specifies if Prev() can be used to compute a previous value for
	// a datum. For example, DBytes doesn't support it (the previous for BB is BAZZZ..).
	HasPrev() bool
	// Prev returns the previous datum. If the receiver is "b" and the returned datum
	// is "a", then "a < b" and no other datum will compare such that "a < c <
	// b".
	// The return value is undefined if `IsMin()`.
	Prev() Datum
	// HasNext specifies if Next() can be used to compute a next value for a
	// datum. For example, DDecimal doesn't support it (the next for 1.0 is 1.00..1).
	HasNext() bool
	// Next returns the next datum. If the receiver is "a" and the returned datum
	// is "b", then "a < b" and no other datum will compare such that "a < c <
	// b".
	// The return value is undefined if `IsMax()`.
	Next() Datum
	// IsMax returns true if the datum is equal to the maximum value the datum
	// type can hold.
	IsMax() bool
	// IsMin returns true if the datum is equal to the minimum value the datum
	// type can hold.
	IsMin() bool
}

// DBool is the boolean Datum.
type DBool bool

// MakeDBool converts its argument to a *DBool, returning either DBoolTrue or
// DBoolFalse.
func MakeDBool(d DBool) *DBool {
	if d {
		return DBoolTrue
	}
	return DBoolFalse
}

// GetBool gets DBool or an error (also treats NULL as false, not an error).
func GetBool(d Datum) (DBool, error) {
	if v, ok := d.(*DBool); ok {
		return *v, nil
	}
	if d == DNull {
		return DBool(false), nil
	}
	return false, fmt.Errorf("cannot convert %s to bool", d.Type())
}

// ReturnType implements the TypedExpr interface.
func (*DBool) ReturnType() Datum {
	return TypeBool
}

// Type implements the Datum interface.
func (*DBool) Type() string {
	return "bool"
}

// TypeEqual implements the Datum interface.
func (d *DBool) TypeEqual(other Datum) bool {
	_, ok := other.(*DBool)
	return ok
}

// Compare implements the Datum interface.
func (d *DBool) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DBool)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if !*d && *v {
		return -1
	}
	if *d && !*v {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (*DBool) HasPrev() bool {
	return true
}

// Prev implements the Datum interface.
func (*DBool) Prev() Datum {
	return DBoolFalse
}

// HasNext implements the Datum interface.
func (*DBool) HasNext() bool {
	return true
}

// Next implements the Datum interface.
func (*DBool) Next() Datum {
	return DBoolTrue
}

// IsMax implements the Datum interface.
func (d *DBool) IsMax() bool {
	return *d == true
}

// IsMin implements the Datum interface.
func (d *DBool) IsMin() bool {
	return *d == false
}

// Format implements the NodeFormatter interface.
func (d *DBool) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(strconv.FormatBool(bool(*d)))
}

// DInt is the int Datum.
type DInt int64

// NewDInt is a helper routine to create a *DInt initialized from its argument.
func NewDInt(d DInt) *DInt {
	return &d
}

// ReturnType implements the TypedExpr interface.
func (*DInt) ReturnType() Datum {
	return TypeInt
}

// Type implements the Datum interface.
func (*DInt) Type() string {
	return "int"
}

// TypeEqual implements the Datum interface.
func (d *DInt) TypeEqual(other Datum) bool {
	_, ok := other.(*DInt)
	return ok
}

// Compare implements the Datum interface.
func (d *DInt) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DInt)
	if !ok {
		cmp, ok := mixedTypeCompare(d, other)
		if !ok {
			panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
		}
		return cmp
	}
	if *d < *v {
		return -1
	}
	if *d > *v {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (*DInt) HasPrev() bool {
	return true
}

// Prev implements the Datum interface.
func (d *DInt) Prev() Datum {
	return NewDInt(*d - 1)
}

// HasNext implements the Datum interface.
func (*DInt) HasNext() bool {
	return true
}

// Next implements the Datum interface.
func (d *DInt) Next() Datum {
	return NewDInt(*d + 1)
}

// IsMax implements the Datum interface.
func (d *DInt) IsMax() bool {
	return *d == math.MaxInt64
}

// IsMin implements the Datum interface.
func (d *DInt) IsMin() bool {
	return *d == math.MinInt64
}

// Format implements the NodeFormatter interface.
func (d *DInt) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(strconv.FormatInt(int64(*d), 10))
}

// DFloat is the float Datum.
type DFloat float64

// NewDFloat is a helper routine to create a *DFloat initialized from its
// argument.
func NewDFloat(d DFloat) *DFloat {
	return &d
}

// ReturnType implements the TypedExpr interface.
func (*DFloat) ReturnType() Datum {
	return TypeFloat
}

// Type implements the Datum interface.
func (*DFloat) Type() string {
	return "float"
}

// TypeEqual implements the Datum interface.
func (d *DFloat) TypeEqual(other Datum) bool {
	_, ok := other.(*DFloat)
	return ok
}

// Compare implements the Datum interface.
func (d *DFloat) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DFloat)
	if !ok {
		cmp, ok := mixedTypeCompare(d, other)
		if !ok {
			panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
		}
		return cmp
	}
	if *d < *v {
		return -1
	}
	if *d > *v {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (*DFloat) HasPrev() bool {
	return true
}

// Prev implements the Datum interface.
func (d *DFloat) Prev() Datum {
	return NewDFloat(DFloat(math.Nextafter(float64(*d), math.Inf(-1))))
}

// HasNext implements the Datum interface.
func (*DFloat) HasNext() bool {
	return true
}

// Next implements the Datum interface.
func (d *DFloat) Next() Datum {
	return NewDFloat(DFloat(math.Nextafter(float64(*d), math.Inf(1))))
}

// IsMax implements the Datum interface.
func (d *DFloat) IsMax() bool {
	// Using >= accounts for +inf as well.
	return *d >= math.MaxFloat64
}

// IsMin implements the Datum interface.
func (d *DFloat) IsMin() bool {
	// Using <= accounts for -inf as well.
	return *d <= -math.MaxFloat64
}

// Format implements the NodeFormatter interface.
func (d *DFloat) Format(buf *bytes.Buffer, f FmtFlags) {
	fl := float64(*d)
	if _, frac := math.Modf(fl); frac == 0 && -1000000 < *d && *d < 1000000 {
		// d is a small whole number. Ensure it is printed using a decimal point.
		fmt.Fprintf(buf, "%.1f", fl)
	} else {
		fmt.Fprintf(buf, "%g", fl)
	}
}

// DDecimal is the decimal Datum.
type DDecimal struct {
	inf.Dec
}

// ReturnType implements the TypedExpr interface.
func (d *DDecimal) ReturnType() Datum {
	return TypeDecimal
}

// Type implements the Datum interface.
func (*DDecimal) Type() string {
	return "decimal"
}

// TypeEqual implements the Datum interface.
func (d *DDecimal) TypeEqual(other Datum) bool {
	_, ok := other.(*DDecimal)
	return ok
}

// Compare implements the Datum interface.
func (d *DDecimal) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DDecimal)
	if !ok {
		cmp, ok := mixedTypeCompare(d, other)
		if !ok {
			panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
		}
		return cmp
	}
	return d.Cmp(&v.Dec)
}

// HasPrev implements the Datum interface.
func (*DDecimal) HasPrev() bool {
	return false
}

// Prev implements the Datum interface.
func (d *DDecimal) Prev() Datum {
	panic(d.Type() + ".Prev() not supported")
}

// HasNext implements the Datum interface.
func (*DDecimal) HasNext() bool {
	return false
}

// Next implements the Datum interface.
func (d *DDecimal) Next() Datum {
	panic(d.Type() + ".Next() not supported")
}

// IsMax implements the Datum interface.
func (*DDecimal) IsMax() bool {
	return false
}

// IsMin implements the Datum interface.
func (*DDecimal) IsMin() bool {
	return false
}

// Format implements the NodeFormatter interface.
func (d *DDecimal) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(d.Dec.String())
}

// DString is the string Datum.
type DString string

// NewDString is a helper routine to create a *DString initialized from its
// argument.
func NewDString(d string) *DString {
	r := DString(d)
	return &r
}

// ReturnType implements the TypedExpr interface.
func (*DString) ReturnType() Datum {
	return TypeString
}

// Type implements the Datum interface.
func (*DString) Type() string {
	return "string"
}

// TypeEqual implements the Datum interface.
func (d *DString) TypeEqual(other Datum) bool {
	_, ok := other.(*DString)
	return ok
}

// Compare implements the Datum interface.
func (d *DString) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DString)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if *d < *v {
		return -1
	}
	if *d > *v {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (*DString) HasPrev() bool {
	return false
}

// Prev implements the Datum interface.
func (d *DString) Prev() Datum {
	panic(d.Type() + ".Prev() not supported")
}

// HasNext implements the Datum interface.
func (*DString) HasNext() bool {
	return true
}

// Next implements the Datum interface.
func (d *DString) Next() Datum {
	return NewDString(string(roachpb.Key(*d).Next()))
}

// IsMax implements the Datum interface.
func (*DString) IsMax() bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DString) IsMin() bool {
	return len(*d) == 0
}

// Format implements the NodeFormatter interface.
func (d *DString) Format(buf *bytes.Buffer, f FmtFlags) {
	encodeSQLString(buf, string(*d))
}

// DBytes is the bytes Datum. The underlying type is a string because we want
// the immutability, but this may contain arbitrary bytes.
type DBytes string

// NewDBytes is a helper routine to create a *DBytes initialized from its
// argument.
func NewDBytes(d DBytes) *DBytes {
	return &d
}

// ReturnType implements the TypedExpr interface.
func (d *DBytes) ReturnType() Datum {
	return TypeBytes
}

// Type implements the Datum interface.
func (*DBytes) Type() string {
	return "bytes"
}

// TypeEqual implements the Datum interface.
func (d *DBytes) TypeEqual(other Datum) bool {
	_, ok := other.(*DBytes)
	return ok
}

// Compare implements the Datum interface.
func (d *DBytes) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DBytes)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if *d < *v {
		return -1
	}
	if *d > *v {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (*DBytes) HasPrev() bool {
	return false
}

// Prev implements the Datum interface.
func (d *DBytes) Prev() Datum {
	panic(d.Type() + ".Prev() not supported")
}

// HasNext implements the Datum interface.
func (*DBytes) HasNext() bool {
	return true
}

// Next implements the Datum interface.
func (d *DBytes) Next() Datum {
	return NewDBytes(DBytes(roachpb.Key(*d).Next()))
}

// IsMax implements the Datum interface.
func (*DBytes) IsMax() bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DBytes) IsMin() bool {
	return len(*d) == 0
}

// Format implements the NodeFormatter interface.
func (d *DBytes) Format(buf *bytes.Buffer, f FmtFlags) {
	encodeSQLBytes(buf, string(*d))
}

// DDate is the date Datum represented as the number of days after
// the Unix epoch.
type DDate int64

// NewDDate is a helper routine to create a *DDate initialized from its
// argument.
func NewDDate(d DDate) *DDate {
	return &d
}

// ReturnType implements the TypedExpr interface.
func (*DDate) ReturnType() Datum {
	return TypeDate
}

// Type implements the Datum interface.
func (*DDate) Type() string {
	return "date"
}

// TypeEqual implements the Datum interface.
func (d *DDate) TypeEqual(other Datum) bool {
	_, ok := other.(*DDate)
	return ok
}

// Compare implements the Datum interface.
func (d *DDate) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DDate)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if *d < *v {
		return -1
	}
	if *v < *d {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (*DDate) HasPrev() bool {
	return true
}

// Prev implements the Datum interface.
func (d *DDate) Prev() Datum {
	return NewDDate(*d - 1)
}

// HasNext implements the Datum interface.
func (*DDate) HasNext() bool {
	return true
}

// Next implements the Datum interface.
func (d *DDate) Next() Datum {
	return NewDDate(*d + 1)
}

// IsMax implements the Datum interface.
func (d *DDate) IsMax() bool {
	return *d == math.MaxInt64
}

// IsMin implements the Datum interface.
func (d *DDate) IsMin() bool {
	return *d == math.MinInt64
}

// Format implements the NodeFormatter interface.
func (d *DDate) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(time.Unix(int64(*d)*secondsInDay, 0).UTC().Format(dateFormat))
}

// DTimestamp is the timestamp Datum.
type DTimestamp struct {
	time.Time
}

// MakeDTimestamp creates a DTimestamp with specified precision.
func MakeDTimestamp(t time.Time, precision time.Duration) *DTimestamp {
	return &DTimestamp{Time: t.Round(precision)}
}

// ReturnType implements the TypedExpr interface.
func (*DTimestamp) ReturnType() Datum {
	return TypeTimestamp
}

// Type implements the Datum interface.
func (*DTimestamp) Type() string {
	return "timestamp"
}

// TypeEqual implements the Datum interface.
func (d *DTimestamp) TypeEqual(other Datum) bool {
	_, ok := other.(*DTimestamp)
	return ok
}

// Compare implements the Datum interface.
func (d *DTimestamp) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DTimestamp)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if d.Before(v.Time) {
		return -1
	}
	if v.Before(d.Time) {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (*DTimestamp) HasPrev() bool {
	return true
}

// Prev implements the Datum interface.
func (d *DTimestamp) Prev() Datum {
	return &DTimestamp{Time: d.Add(-1)}
}

// HasNext implements the Datum interface.
func (*DTimestamp) HasNext() bool {
	return true
}

// Next implements the Datum interface.
func (d *DTimestamp) Next() Datum {
	return &DTimestamp{Time: d.Add(1)}
}

// IsMax implements the Datum interface.
func (d *DTimestamp) IsMax() bool {
	// Adding 1 overflows to a smaller value
	return d.After(d.Next().(*DTimestamp).Time)
}

// IsMin implements the Datum interface.
func (d *DTimestamp) IsMin() bool {
	// Subtracting 1 underflows to a larger value.
	return d.Before(d.Add(-1))
}

// Format implements the NodeFormatter interface.
func (d *DTimestamp) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(d.UTC().Format(timestampNodeFormat))
}

// DTimestampTZ is the timestamp Datum that is rendered with session offset.
type DTimestampTZ struct {
	time.Time
}

// MakeDTimestampTZ creates a DTimestampTZ with specified precision.
func MakeDTimestampTZ(t time.Time, precision time.Duration) *DTimestampTZ {
	return &DTimestampTZ{Time: t.Round(precision)}
}

// ReturnType implements the TypedExpr interface.
func (*DTimestampTZ) ReturnType() Datum {
	return TypeTimestampTZ
}

// Type implements the Datum interface.
func (d *DTimestampTZ) Type() string {
	return "timestamptz"
}

// TypeEqual implements the Datum interface.
func (d *DTimestampTZ) TypeEqual(other Datum) bool {
	_, ok := other.(*DTimestampTZ)
	return ok
}

// Compare implements the Datum interface.
func (d *DTimestampTZ) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DTimestampTZ)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if d.Before(v.Time) {
		return -1
	}
	if v.Before(d.Time) {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (d *DTimestampTZ) HasPrev() bool {
	return true
}

// Prev implements the Datum interface.
func (d *DTimestampTZ) Prev() Datum {
	return &DTimestampTZ{Time: d.Add(-1)}
}

// HasNext implements the Datum interface.
func (d *DTimestampTZ) HasNext() bool {
	return true
}

// Next implements the Datum interface.
func (d *DTimestampTZ) Next() Datum {
	return &DTimestampTZ{Time: d.Add(1)}
}

// IsMax implements the Datum interface.
func (d *DTimestampTZ) IsMax() bool {
	// Adding 1 overflows to a smaller value
	return d.After(d.Next().(*DTimestampTZ).Time)
}

// IsMin implements the Datum interface.
func (d *DTimestampTZ) IsMin() bool {
	// Subtracting 1 underflows to a larger value.
	return d.Before(d.Add(-1))
}

// Format implements the NodeFormatter interface.
func (d *DTimestampTZ) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(d.UTC().Format(timestampNodeFormat))
}

// DInterval is the interval Datum.
type DInterval struct {
	duration.Duration
}

// ReturnType implements the TypedExpr interface.
func (d *DInterval) ReturnType() Datum {
	return TypeInterval
}

// Type implements the Datum interface.
func (*DInterval) Type() string {
	return "interval"
}

// TypeEqual implements the Datum interface.
func (d *DInterval) TypeEqual(other Datum) bool {
	_, ok := other.(*DInterval)
	return ok
}

// Compare implements the Datum interface.
func (d *DInterval) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DInterval)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	return d.Duration.Compare(v.Duration)
}

// HasPrev implements the Datum interface.
func (*DInterval) HasPrev() bool {
	return false
}

// Prev implements the Datum interface.
func (d *DInterval) Prev() Datum {
	panic(d.Type() + ".Prev() not supported")
}

// HasNext implements the Datum interface.
func (*DInterval) HasNext() bool {
	return false
}

// Next implements the Datum interface.
func (d *DInterval) Next() Datum {
	panic(d.Type() + ".Next() not supported")
}

// IsMax implements the Datum interface.
func (d *DInterval) IsMax() bool {
	return d.Months == math.MaxInt64 && d.Days == math.MaxInt64 && d.Nanos == math.MaxInt64
}

// IsMin implements the Datum interface.
func (d *DInterval) IsMin() bool {
	return d.Months == math.MinInt64 && d.Days == math.MinInt64 && d.Nanos == math.MinInt64
}

// Format implements the NodeFormatter interface.
func (d *DInterval) Format(buf *bytes.Buffer, f FmtFlags) {
	if d.Months != 0 || d.Days != 0 {
		buf.WriteString(d.Duration.String())
	} else {
		buf.WriteString((time.Duration(d.Duration.Nanos) * time.Nanosecond).String())
	}
}

// DTuple is the tuple Datum.
type DTuple []Datum

// ReturnType implements the TypedExpr interface.
func (d *DTuple) ReturnType() Datum {
	return d
}

// Type implements the Datum interface.
func (*DTuple) Type() string {
	return "tuple"
}

// TypeEqual implements the Datum interface.
func (d *DTuple) TypeEqual(other Datum) bool {
	t, ok := other.(*DTuple)
	if !ok {
		return false
	}
	if len(*d) != len(*t) {
		return false
	}
	for i := 0; i < len(*d); i++ {
		if !(*d)[i].TypeEqual((*t)[i]) {
			return false
		}
	}
	return true
}

// Compare implements the Datum interface.
func (d *DTuple) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DTuple)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	n := len(*d)
	if n > len(*v) {
		n = len(*v)
	}
	for i := 0; i < n; i++ {
		c := (*d)[i].Compare((*v)[i])
		if c != 0 {
			return c
		}
	}
	if len(*d) < len(*v) {
		return -1
	}
	if len(*d) > len(*v) {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (d *DTuple) HasPrev() bool {
	for i := len(*d) - 1; i >= 0; i-- {
		if (*d)[i].HasPrev() {
			return true
		}
	}
	return false
}

// Prev implements the Datum interface.
func (d *DTuple) Prev() Datum {
	n := make(DTuple, len(*d))
	copy(n, *d)
	for i := len(n) - 1; i >= 0; i-- {
		if n[i].HasPrev() {
			n[i] = n[i].Prev()
			return &n
		}
	}
	panic(fmt.Errorf("Prev() cannot be computed on a tuple whose datum does not support it"))
}

// HasNext implements the Datum interface.
func (d *DTuple) HasNext() bool {
	for i := len(*d) - 1; i >= 0; i-- {
		if (*d)[i].HasNext() {
			return true
		}
	}
	return false
}

// Next implements the Datum interface.
func (d *DTuple) Next() Datum {
	n := make(DTuple, len(*d))
	copy(n, *d)
	for i := len(n) - 1; i >= 0; i-- {
		if n[i].HasNext() {
			n[i] = n[i].Next()
			return &n
		}
	}
	panic(fmt.Errorf("Next() cannot be computed on a tuple whose datum does not support it"))
}

// IsMax implements the Datum interface.
func (*DTuple) IsMax() bool {
	// Unimplemented for DTuple. Seems possible to provide an implementation
	// which called IsMax for each of the elements, but currently this isn't
	// needed.
	return false
}

// IsMin implements the Datum interface.
func (*DTuple) IsMin() bool {
	// Unimplemented for DTuple. Seems possible to provide an implementation
	// which called IsMin for each of the elements, but currently this isn't
	// needed.
	return false
}

// Format implements the NodeFormatter interface.
func (d *DTuple) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('(')
	for i, v := range *d {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, v)
	}
	buf.WriteByte(')')
}

func (d *DTuple) Len() int {
	return len(*d)
}

func (d *DTuple) Less(i, j int) bool {
	return (*d)[i].Compare((*d)[j]) < 0
}

func (d *DTuple) Swap(i, j int) {
	(*d)[i], (*d)[j] = (*d)[j], (*d)[i]
}

// Normalize sorts and uniques the datum tuple.
func (d *DTuple) Normalize() {
	sort.Sort(d)
	d.makeUnique()
}

func (d *DTuple) makeUnique() {
	n := 0
	for i := 0; i < len(*d); i++ {
		if (*d)[i] == DNull {
			continue
		}
		if n == 0 || (*d)[n-1].Compare((*d)[i]) < 0 {
			(*d)[n] = (*d)[i]
			n++
		}
	}
	*d = (*d)[:n]
}

// SortedDifference finds the elements of d which are not in other,
// assuming that d and other are already sorted.
func (d *DTuple) SortedDifference(other *DTuple) *DTuple {
	var r DTuple
	a := *d
	b := *other
	for len(a) > 0 && len(b) > 0 {
		switch a[0].Compare(b[0]) {
		case -1:
			r = append(r, a[0])
			a = a[1:]
		case 0:
			a = a[1:]
			b = b[1:]
		case 1:
			b = b[1:]
		}
	}
	return &r
}

type dNull struct{}

// ReturnType implements the TypedExpr interface.
func (dNull) ReturnType() Datum {
	return DNull
}

// Type implements the Datum interface.
func (d dNull) Type() string {
	return "NULL"
}

// TypeEqual implements the Datum interface.
func (d dNull) TypeEqual(other Datum) bool {
	_, ok := other.(dNull)
	return ok
}

// Compare implements the Datum interface.
func (d dNull) Compare(other Datum) int {
	if other == DNull {
		return 0
	}
	return -1
}

// HasPrev implements the Datum interface.
func (dNull) HasPrev() bool {
	return false
}

// Prev implements the Datum interface.
func (d dNull) Prev() Datum {
	panic(d.Type() + ".Prev not supported")
}

// HasNext implements the Datum interface.
func (dNull) HasNext() bool {
	return false
}

// Next implements the Datum interface.
func (d dNull) Next() Datum {
	panic(d.Type() + ".Next not supported")
}

// IsMax implements the Datum interface.
func (dNull) IsMax() bool {
	return true
}

// IsMin implements the Datum interface.
func (dNull) IsMin() bool {
	return true
}

// Format implements the NodeFormatter interface.
func (dNull) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("NULL")
}

var _ VariableExpr = &DValArg{}

// DValArg is the named bind var argument Datum.
type DValArg struct {
	name string
	typeAnnotation
}

// ReturnType implements the TypedExpr interface.
func (d *DValArg) ReturnType() Datum {
	if d.typeAnnotation.typ != nil {
		return d.typeAnnotation.typ
	}
	return d
}

// Variable implements the VariableExpr interface.
func (*DValArg) Variable() {}

// Type implements the Datum interface.
func (*DValArg) Type() string {
	return "parameter"
}

// TypeEqual implements the Datum interface.
func (d *DValArg) TypeEqual(other Datum) bool {
	_, ok := other.(*DValArg)
	return ok
}

// Compare implements the Datum interface.
func (d *DValArg) Compare(other Datum) int {
	panic(d.Type() + ".Compare not supported")
}

// HasPrev implements the Datum interface.
func (*DValArg) HasPrev() bool {
	return false
}

// Prev implements the Datum interface.
func (d *DValArg) Prev() Datum {
	panic(d.Type() + ".Prev not supported")
}

// HasNext implements the Datum interface.
func (*DValArg) HasNext() bool {
	return false
}

// Next implements the Datum interface.
func (d *DValArg) Next() Datum {
	panic(d.Type() + ".Next not supported")
}

// IsMax implements the Datum interface.
func (*DValArg) IsMax() bool {
	return true
}

// IsMin implements the Datum interface.
func (*DValArg) IsMin() bool {
	return true
}

// Format implements the NodeFormatter interface.
func (d *DValArg) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('$')
	buf.WriteString(d.name)
}

// Temporary workaround for #3633, allowing comparisons between
// heterogeneous types.
// TODO(nvanbenschoten) Now that typing is improved, can we get rid of this?
func mixedTypeCompare(l, r Datum) (int, bool) {
	// Check equality.
	eqOp, ok := CmpOps[EQ].lookupImpl(l, r)
	if !ok {
		return 0, false
	}
	eq, err := eqOp.fn(EvalContext{}, l, r)
	if err != nil {
		panic(err)
	}
	if eq {
		return 0, true
	}

	// Check less than.
	ltOp, ok := CmpOps[LT].lookupImpl(l, r)
	if !ok {
		return 0, false
	}
	lt, err := ltOp.fn(EvalContext{}, l, r)
	if err != nil {
		panic(err)
	}
	if lt {
		return -1, true
	}
	return 1, true
}
