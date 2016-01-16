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
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/shopspring/decimal"
)

var (
	// DummyBool is a placeholder DBool value.
	DummyBool Datum = DBool(false)
	// DummyInt is a placeholder DInt value.
	DummyInt Datum = DInt(0)
	// DummyFloat is a placeholder DFloat value.
	DummyFloat Datum = DFloat(0)
	// DummyDecimal is a placeholder DDecimal value.
	DummyDecimal Datum = DDecimal{}
	// DummyString is a placeholder DString value.
	DummyString Datum = DString("")
	// DummyBytes is a placeholder DBytes value.
	DummyBytes Datum = DBytes("")
	// DummyDate is a placeholder DDate value.
	DummyDate Datum = DDate(0)
	// DummyTimestamp is a placeholder DTimestamp value.
	DummyTimestamp Datum = DTimestamp{}
	// DummyInterval is a placeholder DInterval value.
	DummyInterval Datum = DInterval{}
	// dummyTuple is a placeholder DTuple value.
	dummyTuple Datum = DTuple{}
	// DNull is the NULL Datum.
	DNull Datum = dNull{}

	boolType      = reflect.TypeOf(DummyBool)
	intType       = reflect.TypeOf(DummyInt)
	floatType     = reflect.TypeOf(DummyFloat)
	decimalType   = reflect.TypeOf(DummyDecimal)
	stringType    = reflect.TypeOf(DummyString)
	bytesType     = reflect.TypeOf(DummyBytes)
	dateType      = reflect.TypeOf(DummyDate)
	timestampType = reflect.TypeOf(DummyTimestamp)
	intervalType  = reflect.TypeOf(DummyInterval)
	tupleType     = reflect.TypeOf(dummyTuple)
	nullType      = reflect.TypeOf(DNull)
	valargType    = reflect.TypeOf(DValArg{})
)

// A Datum holds either a bool, int64, float64, string or []Datum.
type Datum interface {
	Expr
	Type() string
	// Compare returns -1 if the receiver is less than other, 0 if receiver is
	// equal to other and +1 if receiver is greater than other.
	Compare(other Datum) int
	// HasPrev is a type trait specifying if Prev() can be used to compute a
	// previous value for a datum. For example, DBytes doesn't support it
	// (the previous for BB is BAZZZ..).
	HasPrev() bool
	// Prev returns the previous datum. If the receiver is "b" and the returned datum
	// is "a", then "a < b" and no other datum will compare such that "a < c <
	// b".
	// The return value is undefined is `IsMin()`.
	Prev() Datum
	// Next returns the next datum. If the receiver is "a" and the returned datum
	// is "b", then "a < b" and no other datum will compare such that "a < c <
	// b".
	// The return value is undefined is `IsMax()`.
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

// GetBool gets DBool or an error (also treats NULL as false, not an error).
func GetBool(d Datum) (DBool, error) {
	if v, ok := d.(DBool); ok {
		return v, nil
	}
	if d == DNull {
		return DBool(false), nil
	}
	return false, fmt.Errorf("cannot convert %s to bool", d.Type())
}

// Type implements the Datum interface.
func (d DBool) Type() string {
	return "bool"
}

// Compare implements the Datum interface.
func (d DBool) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DBool)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if !d && v {
		return -1
	}
	if d && !v {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (d DBool) HasPrev() bool {
	return true
}

// Prev implements the Datum interface.
func (d DBool) Prev() Datum {
	return DBool(false)
}

// Next implements the Datum interface.
func (d DBool) Next() Datum {
	return DBool(true)
}

// IsMax implements the Datum interface.
func (d DBool) IsMax() bool {
	return d == true
}

// IsMin implements the Datum interface.
func (d DBool) IsMin() bool {
	return d == false
}

func (d DBool) String() string {
	return strconv.FormatBool(bool(d))
}

// DInt is the int Datum.
type DInt int64

// Type implements the Datum interface.
func (d DInt) Type() string {
	return "int"
}

// Compare implements the Datum interface.
func (d DInt) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DInt)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if d < v {
		return -1
	}
	if d > v {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (d DInt) HasPrev() bool {
	return true
}

// Prev implements the Datum interface.
func (d DInt) Prev() Datum {
	return d - 1
}

// Next implements the Datum interface.
func (d DInt) Next() Datum {
	return d + 1
}

// IsMax implements the Datum interface.
func (d DInt) IsMax() bool {
	return d == math.MaxInt64
}

// IsMin implements the Datum interface.
func (d DInt) IsMin() bool {
	return d == math.MinInt64
}

func (d DInt) String() string {
	return strconv.FormatInt(int64(d), 10)
}

// DFloat is the float Datum.
type DFloat float64

// Type implements the Datum interface.
func (d DFloat) Type() string {
	return "float"
}

// Compare implements the Datum interface.
func (d DFloat) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DFloat)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if d < v {
		return -1
	}
	if d > v {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (d DFloat) HasPrev() bool {
	return true
}

// Prev implements the Datum interface.
func (d DFloat) Prev() Datum {
	return DFloat(math.Nextafter(float64(d), math.Inf(-1)))
}

// Next implements the Datum interface.
func (d DFloat) Next() Datum {
	return DFloat(math.Nextafter(float64(d), math.Inf(1)))
}

// IsMax implements the Datum interface.
func (d DFloat) IsMax() bool {
	// Using >= accounts for +inf as well.
	return d >= math.MaxFloat64
}

// IsMin implements the Datum interface.
func (d DFloat) IsMin() bool {
	// Using <= accounts for -inf as well.
	return d <= -math.MaxFloat64
}

func (d DFloat) String() string {
	fmt := byte('g')
	prec := -1
	if _, frac := math.Modf(float64(d)); frac == 0 {
		// d is a whole number.
		if -1000000 < d && d < 1000000 {
			// Small whole numbers are printed without exponents, and with one
			// digit of decimal precision to ensure they will parse as floats.
			fmt = 'f'
			prec = 1
		} else {
			// Large whole numbers are printed with exponents.
			fmt = 'e'
		}
	}
	return strconv.FormatFloat(float64(d), fmt, prec, 64)
}

// DDecimal is the decimal Datum.
type DDecimal struct {
	decimal.Decimal
}

// Type implements the Datum interface.
func (d DDecimal) Type() string {
	return "decimal"
}

// Compare implements the Datum interface.
func (d DDecimal) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DDecimal)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	return d.Cmp(v.Decimal)
}

// HasPrev implements the Datum interface.
func (d DDecimal) HasPrev() bool {
	return false
}

// Prev implements the Datum interface.
func (d DDecimal) Prev() Datum {
	panic(d.Type() + ".Prev() not supported")
}

// Next implements the Datum interface.
func (d DDecimal) Next() Datum {
	// TODO(nvanbenschoten) How can we get this? decimal.Decimal
	// supports up to 2^31 digits after the decimal point. Probably
	// going to based on the roachpb.Value encoding we use.
	panic(d.Type() + ".Next() not supported")
}

// IsMax implements the Datum interface.
func (d DDecimal) IsMax() bool {
	return false
}

// IsMin implements the Datum interface.
func (d DDecimal) IsMin() bool {
	return false
}

func (d DDecimal) String() string {
	return d.Decimal.String()
}

// DString is the string Datum.
type DString string

// Type implements the Datum interface.
func (d DString) Type() string {
	return "string"
}

// Compare implements the Datum interface.
func (d DString) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DString)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if d < v {
		return -1
	}
	if d > v {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (d DString) HasPrev() bool {
	return false
}

// Prev implements the Datum interface.
func (d DString) Prev() Datum {
	panic(d.Type() + ".Prev() not supported")
}

// Next implements the Datum interface.
func (d DString) Next() Datum {
	return DString(roachpb.Key(d).Next())
}

// IsMax implements the Datum interface.
func (d DString) IsMax() bool {
	return false
}

// IsMin implements the Datum interface.
func (d DString) IsMin() bool {
	return len(d) == 0
}

func (d DString) String() string {
	return encodeSQLString(string(d))
}

// DBytes is the bytes Datum. The underlying type is a string because we want
// the immutability, but this may contain arbitrary bytes.
type DBytes string

// Type implements the Datum interface.
func (d DBytes) Type() string {
	return "bytes"
}

// Compare implements the Datum interface.
func (d DBytes) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DBytes)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if d < v {
		return -1
	}
	if d > v {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (d DBytes) HasPrev() bool {
	return false
}

// Prev implements the Datum interface.
func (d DBytes) Prev() Datum {
	panic(d.Type() + ".Prev() not supported")
}

// Next implements the Datum interface.
func (d DBytes) Next() Datum {
	return DBytes(roachpb.Key(d).Next())
}

// IsMax implements the Datum interface.
func (d DBytes) IsMax() bool {
	return false
}

// IsMin implements the Datum interface.
func (d DBytes) IsMin() bool {
	return len(d) == 0
}

func (d DBytes) String() string {
	return encodeSQLBytes(string(d))
}

// DDate is the date Datum represented as the number of days after
// the Unix epoch.
type DDate int64

// Type implements the Datum interface.
func (d DDate) Type() string {
	return "date"
}

// Compare implements the Datum interface.
func (d DDate) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DDate)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if d < v {
		return -1
	}
	if v < d {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (d DDate) HasPrev() bool {
	return true
}

// Prev implements the Datum interface.
func (d DDate) Prev() Datum {
	return d - 1
}

// Next implements the Datum interface.
func (d DDate) Next() Datum {
	return d + 1
}

// IsMax implements the Datum interface.
func (d DDate) IsMax() bool {
	return d == math.MaxInt64
}

// IsMin implements the Datum interface.
func (d DDate) IsMin() bool {
	return d == math.MinInt64
}

func (d DDate) String() string {
	return time.Unix(int64(d)*secondsInDay, 0).UTC().Format(dateFormat)
}

// DTimestamp is the timestamp Datum.
type DTimestamp struct {
	time.Time
}

// Type implements the Datum interface.
func (d DTimestamp) Type() string {
	return "timestamp"
}

// Compare implements the Datum interface.
func (d DTimestamp) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DTimestamp)
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
func (d DTimestamp) HasPrev() bool {
	return true
}

// Prev implements the Datum interface.
func (d DTimestamp) Prev() Datum {
	return DTimestamp{Time: d.Add(-1)}
}

// Next implements the Datum interface.
func (d DTimestamp) Next() Datum {
	return DTimestamp{Time: d.Add(1)}
}

// IsMax implements the Datum interface.
func (d DTimestamp) IsMax() bool {
	// Adding 1 overflows to a smaller value
	return d.After(d.Next().(DTimestamp).Time)
}

// IsMin implements the Datum interface.
func (d DTimestamp) IsMin() bool {
	// Subtracting 1 underflows to a larger value.
	return d.Before(d.Add(-1))
}

func (d DTimestamp) String() string {
	return d.UTC().Format(TimestampWithOffsetZoneFormat)
}

// DInterval is the interval Datum.
type DInterval struct {
	time.Duration
}

// Type implements the Datum interface.
func (d DInterval) Type() string {
	return "interval"
}

// Compare implements the Datum interface.
func (d DInterval) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DInterval)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	if d.Duration < v.Duration {
		return -1
	}
	if v.Duration < d.Duration {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (d DInterval) HasPrev() bool {
	return true
}

// Prev implements the Datum interface.
func (d DInterval) Prev() Datum {
	return DInterval{Duration: d.Duration - 1}
}

// Next implements the Datum interface.
func (d DInterval) Next() Datum {
	return DInterval{Duration: d.Duration + 1}
}

// IsMax implements the Datum interface.
func (d DInterval) IsMax() bool {
	return d.Duration == math.MaxInt64
}

// IsMin implements the Datum interface.
func (d DInterval) IsMin() bool {
	return d.Duration == math.MinInt64
}

// DTuple is the tuple Datum.
type DTuple []Datum

// Type implements the Datum interface.
func (d DTuple) Type() string {
	return "tuple"
}

// Compare implements the Datum interface.
func (d DTuple) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(DTuple)
	if !ok {
		panic(fmt.Sprintf("unsupported comparison: %s to %s", d.Type(), other.Type()))
	}
	n := len(d)
	if n > len(v) {
		n = len(v)
	}
	for i := 0; i < n; i++ {
		c := d[i].Compare(v[i])
		if c != 0 {
			return c
		}
	}
	if len(d) < len(v) {
		return -1
	}
	if len(d) > len(v) {
		return 1
	}
	return 0
}

// HasPrev implements the Datum interface.
func (d DTuple) HasPrev() bool {
	return false
}

// Prev implements the Datum interface.
func (d DTuple) Prev() Datum {
	// We could implement it depending on the constituents of the tuple, but it's
	// not needed.
	panic(fmt.Errorf("can't compute Prev() on a tuple"))
}

// Next implements the Datum interface.
func (d DTuple) Next() Datum {
	n := make(DTuple, len(d))
	copy(n, d)
	n[len(n)-1] = n[len(n)-1].Next()
	return n
}

// IsMax implements the Datum interface.
func (d DTuple) IsMax() bool {
	// Unimplemented for DTuple. Seems possible to provide an implementation
	// which called IsMax for each of the elements, but currently this isn't
	// needed.
	return false
}

// IsMin implements the Datum interface.
func (d DTuple) IsMin() bool {
	// Unimplemented for DTuple. Seems possible to provide an implementation
	// which called IsMin for each of the elements, but currently this isn't
	// needed.
	return false
}

func (d DTuple) String() string {
	var buf bytes.Buffer
	_ = buf.WriteByte('(')
	for i, v := range d {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(v.String())
	}
	_ = buf.WriteByte(')')
	return buf.String()
}

func (d DTuple) Len() int {
	return len(d)
}

func (d DTuple) Less(i, j int) bool {
	return d[i].Compare(d[j]) < 0
}

func (d DTuple) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
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

type dNull struct{}

// Type implements the Datum interface.
func (d dNull) Type() string {
	return "NULL"
}

// Compare implements the Datum interface.
func (d dNull) Compare(other Datum) int {
	if other == DNull {
		return 0
	}
	return -1
}

// HasPrev implements the Datum interface.
func (d dNull) HasPrev() bool {
	return false
}

// Prev implements the Datum interface.
func (d dNull) Prev() Datum {
	panic(fmt.Errorf("cannot compute Prev() on Null"))
}

// Next implements the Datum interface.
func (d dNull) Next() Datum {
	panic("dNull.Next not supported")
}

// IsMax implements the Datum interface.
func (d dNull) IsMax() bool {
	return true
}

// IsMin implements the Datum interface.
func (d dNull) IsMin() bool {
	return true
}

func (d dNull) String() string {
	return "NULL"
}

var _ VariableExpr = DValArg{}

// DValArg is the named bind var argument Datum.
type DValArg struct {
	name string
}

// Variable implements the VariableExpr interface.
func (DValArg) Variable() {}

// Type implements the Datum interface.
func (DValArg) Type() string {
	return "valarg"
}

// Compare implements the Datum interface.
func (d DValArg) Compare(other Datum) int {
	panic(d.Type() + ".Compare not supported")
}

// HasPrev implements the Datum interface.
func (d DValArg) HasPrev() bool {
	return false
}

// Prev implements the Datum interface.
func (d DValArg) Prev() Datum {
	panic(d.Type() + ".Prev not supported")
}

// Next implements the Datum interface.
func (d DValArg) Next() Datum {
	panic(d.Type() + ".Next not supported")
}

// IsMax implements the Datum interface.
func (DValArg) IsMax() bool {
	return true
}

// IsMin implements the Datum interface.
func (DValArg) IsMin() bool {
	return true
}

func (d DValArg) String() string {
	return "$" + d.name
}
