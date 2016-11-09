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
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/pkg/errors"
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

// Datum represents a SQL value.
type Datum interface {
	TypedExpr
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

	// Size returns a lower bound on the total size of the receiver in bytes,
	// including memory that is pointed at (even if shared between Datum
	// instances) but excluding allocation overhead.
	//
	// It holds for every Datum d that d.Size() >= d.ResolvedType().Size().
	Size() uintptr
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

// makeParseError returns a parse error using the provided string and type. An
// optional error can be provided, which will be appended to the end of the
// error string.
func makeParseError(s string, typ Type, err error) error {
	var suffix string
	if err != nil {
		suffix = fmt.Sprintf(": %v", err)
	}
	return fmt.Errorf("could not parse '%s' as type %s%s", s, typ, suffix)
}

func makeUnsupportedComparisonMessage(d1, d2 Datum) string {
	return fmt.Sprintf("unsupported comparison: %s to %s", d1.ResolvedType(), d2.ResolvedType())
}

func makeUnsupportedMethodMessage(d Datum, methodName string) string {
	return fmt.Sprintf("%s.%s not supported", d.ResolvedType(), methodName)
}

// ParseDBool parses and returns the *DBool Datum value represented by the provided
// string, or an error if parsing is unsuccessful.
func ParseDBool(s string) (*DBool, error) {
	// TODO(pmattis): strconv.ParseBool is more permissive than the SQL
	// spec. Is that ok?
	b, err := strconv.ParseBool(s)
	if err != nil {
		return nil, makeParseError(s, TypeBool, err)
	}
	return MakeDBool(DBool(b)), nil
}

// GetBool gets DBool or an error (also treats NULL as false, not an error).
func GetBool(d Datum) (DBool, error) {
	if v, ok := d.(*DBool); ok {
		return *v, nil
	}
	if d == DNull {
		return DBool(false), nil
	}
	return false, fmt.Errorf("cannot convert %s to type %s", d.ResolvedType(), TypeBool)
}

// ResolvedType implements the TypedExpr interface.
func (*DBool) ResolvedType() Type {
	return TypeBool
}

// Compare implements the Datum interface.
func (d *DBool) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DBool)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
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
	return bool(*d)
}

// IsMin implements the Datum interface.
func (d *DBool) IsMin() bool {
	return !bool(*d)
}

// Format implements the NodeFormatter interface.
func (d *DBool) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(strconv.FormatBool(bool(*d)))
}

// Size implements the Datum interface.
func (d *DBool) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DInt is the int Datum.
type DInt int64

// NewDInt is a helper routine to create a *DInt initialized from its argument.
func NewDInt(d DInt) *DInt {
	return &d
}

// ParseDInt parses and returns the *DInt Datum value represented by the provided
// string, or an error if parsing is unsuccessful.
func ParseDInt(s string) (*DInt, error) {
	i, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		return nil, makeParseError(s, TypeInt, err)
	}
	return NewDInt(DInt(i)), nil
}

// ResolvedType implements the TypedExpr interface.
func (*DInt) ResolvedType() Type {
	return TypeInt
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
			panic(makeUnsupportedComparisonMessage(d, other))
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

// Size implements the Datum interface.
func (d *DInt) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DFloat is the float Datum.
type DFloat float64

// NewDFloat is a helper routine to create a *DFloat initialized from its
// argument.
func NewDFloat(d DFloat) *DFloat {
	return &d
}

// ParseDFloat parses and returns the *DFloat Datum value represented by the provided
// string, or an error if parsing is unsuccessful.
func ParseDFloat(s string) (*DFloat, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, makeParseError(s, TypeFloat, err)
	}
	return NewDFloat(DFloat(f)), nil
}

// ResolvedType implements the TypedExpr interface.
func (*DFloat) ResolvedType() Type {
	return TypeFloat
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
			panic(makeUnsupportedComparisonMessage(d, other))
		}
		return cmp
	}
	if *d < *v {
		return -1
	}
	if *d > *v {
		return 1
	}
	// NaN sorts before non-NaN (#10109).
	if *d == *v {
		return 0
	}
	if math.IsNaN(float64(*d)) {
		if math.IsNaN(float64(*v)) {
			return 0
		}
		return -1
	}
	return 1
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

// Size implements the Datum interface.
func (d *DFloat) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DDecimal is the decimal Datum.
type DDecimal struct {
	inf.Dec
}

// ParseDDecimal parses and returns the *DDecimal Datum value represented by the
// provided string, or an error if parsing is unsuccessful.
func ParseDDecimal(s string) (*DDecimal, error) {
	dd := &DDecimal{}
	if _, ok := dd.SetString(s); !ok {
		return nil, makeParseError(s, TypeDecimal, nil)
	}
	return dd, nil
}

// ResolvedType implements the TypedExpr interface.
func (*DDecimal) ResolvedType() Type {
	return TypeDecimal
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
			panic(makeUnsupportedComparisonMessage(d, other))
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
	panic(makeUnsupportedMethodMessage(d, "Prev"))
}

// HasNext implements the Datum interface.
func (*DDecimal) HasNext() bool {
	return false
}

// Next implements the Datum interface.
func (d *DDecimal) Next() Datum {
	panic(makeUnsupportedMethodMessage(d, "Next"))
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

// Size implements the Datum interface.
func (d *DDecimal) Size() uintptr {
	intVal := d.Dec.UnscaledBig()
	return unsafe.Sizeof(*d) + uintptr(cap(intVal.Bits()))*unsafe.Sizeof(big.Word(0))
}

// DString is the string Datum.
type DString string

// NewDString is a helper routine to create a *DString initialized from its
// argument.
func NewDString(d string) *DString {
	r := DString(d)
	return &r
}

// ResolvedType implements the TypedExpr interface.
func (*DString) ResolvedType() Type {
	return TypeString
}

// Compare implements the Datum interface.
func (d *DString) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DString)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
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
	panic(makeUnsupportedMethodMessage(d, "Prev"))
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

// Size implements the Datum interface.
func (d *DString) Size() uintptr {
	return unsafe.Sizeof(*d) + uintptr(len(*d))
}

// DBytes is the bytes Datum. The underlying type is a string because we want
// the immutability, but this may contain arbitrary bytes.
type DBytes string

// NewDBytes is a helper routine to create a *DBytes initialized from its
// argument.
func NewDBytes(d DBytes) *DBytes {
	return &d
}

// ResolvedType implements the TypedExpr interface.
func (*DBytes) ResolvedType() Type {
	return TypeBytes
}

// Compare implements the Datum interface.
func (d *DBytes) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DBytes)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
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
	panic(makeUnsupportedMethodMessage(d, "Prev"))
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

// Size implements the Datum interface.
func (d *DBytes) Size() uintptr {
	return unsafe.Sizeof(*d) + uintptr(len(*d))
}

// DDate is the date Datum represented as the number of days after
// the Unix epoch.
type DDate int64

// NewDDate is a helper routine to create a *DDate initialized from its
// argument.
func NewDDate(d DDate) *DDate {
	return &d
}

// NewDDateFromTime constructs a *DDate from a time.Time in the provided time zone.
func NewDDateFromTime(t time.Time, loc *time.Location) *DDate {
	year, month, day := t.In(loc).Date()
	secs := time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Unix()
	return NewDDate(DDate(secs / secondsInDay))
}

// time.Time formats.
const (
	dateFormat                = "2006-01-02"
	dateFormatWithOffset      = "2006-01-02 -07:00:00"
	dateFormatNoPadWithOffset = "2006-1-2 -07:00:00"
)

var dateFormats = []string{
	dateFormat,
	dateFormatWithOffset,
	dateFormatNoPadWithOffset,
	time.RFC3339Nano,
}

// ParseDDate parses and returns the *DDate Datum value represented by the provided
// string in the provided location, or an error if parsing is unsuccessful.
func ParseDDate(s string, loc *time.Location) (*DDate, error) {
	// No need to ParseInLocation here because we're only parsing dates.

	// HACK: go doesn't handle offsets that are not zero-padded from psql/jdbc.
	// Thus, if we see `2015-10-05 +0:0:0` we need to change it to `+00:00:00`.
	if l := len(s); l > 6 && s[l-2] == ':' && s[l-4] == ':' && (s[l-6] == '+' || s[l-6] == '-') {
		s = fmt.Sprintf("%s %c0%c:0%c:0%c", s[:l-6], s[l-6], s[l-5], s[l-3], s[l-1])
	}

	for _, format := range dateFormats {
		if t, err := time.Parse(format, s); err == nil {
			return NewDDateFromTime(t, loc), nil
		}
	}

	return nil, makeParseError(s, TypeDate, nil)
}

// ResolvedType implements the TypedExpr interface.
func (*DDate) ResolvedType() Type {
	return TypeDate
}

// Compare implements the Datum interface.
func (d *DDate) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DDate)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
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

// Size implements the Datum interface.
func (d *DDate) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DTimestamp is the timestamp Datum.
type DTimestamp struct {
	time.Time
}

// MakeDTimestamp creates a DTimestamp with specified precision.
func MakeDTimestamp(t time.Time, precision time.Duration) *DTimestamp {
	return &DTimestamp{Time: t.Round(precision)}
}

// time.Time formats.
const (
	timestampFormat                       = "2006-01-02 15:04:05"
	timestampWithOffsetZoneFormat         = timestampFormat + "-07"
	timestampWithOffsetSecondsZoneFormat  = timestampWithOffsetZoneFormat + ":00"
	timestampWithNamedZoneFormat          = timestampFormat + " MST"
	timestampRFC3339NanoWithoutZoneFormat = "2006-01-02T15:04:05"

	timestampNodeFormat = timestampFormat + ".999999-07:00"
)

var timeFormats = []string{
	dateFormat,
	time.RFC3339Nano,
	timestampWithOffsetZoneFormat,
	timestampWithOffsetSecondsZoneFormat,
	timestampFormat,
	timestampWithNamedZoneFormat,
	timestampRFC3339NanoWithoutZoneFormat,
	timestampNodeFormat,
}

func parseTimestampInLocation(s string, loc *time.Location) (time.Time, error) {
	for _, format := range timeFormats {
		if t, err := time.ParseInLocation(format, s, loc); err == nil {
			if err := checkForMissingZone(t, loc); err != nil {
				return time.Time{}, makeParseError(s, TypeTimestamp, err)
			}
			return t, nil
		}
	}
	return time.Time{}, makeParseError(s, TypeTimestamp, nil)
}

// Unfortunately Go is very strict when parsing abbreviated zone names -- with
// the exception of 'UTC' and 'GMT', it only supports abbreviations that are
// defined in the local in which it is parsing. Worse, it doesn't return any
// sort of error for unresolved zones, but rather simply pretends they have a
// zero offset. This means changing the session zone such that an abbreviation
// like 'CET' stops being resolved *silently* changes the offsets of parsed
// strings with 'CET' offsets to zero.
// We attempt to detect when this has happened and return an error instead.
//
// Postgres does its own parsing and just maintains a list of zone abbreviations
// that are always recognized, regardless of the session location. If this check
// ends up catching too many users, we may need to do the same.
func checkForMissingZone(t time.Time, parseLoc *time.Location) error {
	if z, off := t.Zone(); off == 0 && t.Location() != parseLoc && z != "UTC" && !strings.HasPrefix(z, "GMT") {
		return errors.Errorf("unknown zone %q", z)
	}
	return nil
}

// ParseDTimestamp parses and returns the *DTimestamp Datum value represented by
// the provided string in UTC, or an error if parsing is unsuccessful.
func ParseDTimestamp(s string, precision time.Duration) (*DTimestamp, error) {
	// `ParseInLocation` uses the location provided both for resolving an explicit
	// abbreviated zone as well as for the default zone if not specified
	// explicitly. For non-'WITH TIME ZONE' strings (which this is used to parse),
	// we do not want to add a non-UTC zone if one is not explicitly stated, so we
	// use time.UTC rather than the sesion location. Unfortunately this also means
	// we do not use the session zone for resolving abbreviations.
	t, err := parseTimestampInLocation(s, time.UTC)
	if err != nil {
		return nil, err
	}
	return MakeDTimestamp(t, precision), nil
}

// ResolvedType implements the TypedExpr interface.
func (*DTimestamp) ResolvedType() Type {
	return TypeTimestamp
}

// Compare implements the Datum interface.
func (d *DTimestamp) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DTimestamp)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
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

// Size implements the Datum interface.
func (d *DTimestamp) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DTimestampTZ is the timestamp Datum that is rendered with session offset.
type DTimestampTZ struct {
	time.Time
}

// MakeDTimestampTZ creates a DTimestampTZ with specified precision.
func MakeDTimestampTZ(t time.Time, precision time.Duration) *DTimestampTZ {
	return &DTimestampTZ{Time: t.Round(precision)}
}

// ParseDTimestampTZ parses and returns the *DTimestampTZ Datum value represented by
// the provided string in the provided location, or an error if parsing is unsuccessful.
func ParseDTimestampTZ(
	s string, loc *time.Location, precision time.Duration,
) (*DTimestampTZ, error) {
	t, err := parseTimestampInLocation(s, loc)
	if err != nil {
		return nil, err
	}
	return MakeDTimestampTZ(t, precision), nil
}

// ResolvedType implements the TypedExpr interface.
func (*DTimestampTZ) ResolvedType() Type {
	return TypeTimestampTZ
}

// Compare implements the Datum interface.
func (d *DTimestampTZ) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DTimestampTZ)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
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

// Size implements the Datum interface.
func (d *DTimestampTZ) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DInterval is the interval Datum.
type DInterval struct {
	duration.Duration
}

// ParseDInterval parses and returns the *DInterval Datum value represented by the provided
// string, or an error if parsing is unsuccessful.
func ParseDInterval(s string) (*DInterval, error) {
	// At this time the only supported interval formats are:
	// - SQL stardard.
	// - Postgres compatible.
	// - iso8601 format (with designators only), see interval.go for
	//   sources of documentation.
	// - Golang time.parseDuration compatible.

	// If it's a blank string, exit early.
	if len(s) == 0 {
		return nil, makeParseError(s, TypeInterval, nil)
	}

	if s[0] == 'P' {
		// If it has a leading P we're most likely working with an iso8601
		// interval.
		dur, err := iso8601ToDuration(s)
		if err != nil {
			return nil, makeParseError(s, TypeInterval, err)
		}
		return &DInterval{Duration: dur}, nil
	} else if ((s[0] != '-') && strings.ContainsRune(s, '-')) || (strings.ContainsRune(s, ' ') && strings.ContainsRune(s, ':')) {
		// No leading by '-' but containing '-', for example '1-2'
		// Or, Containing both space and ':', for example '3 4:05:06'
		parts := strings.Split(s, " ")
		dur, err := dateToDuration(parts[0])
		if err != nil {
			return nil, makeParseError(s, TypeInterval, err)
		}
		switch len(parts) {
		case 1:
			return &DInterval{Duration: dur}, nil
		case 2:
			ret, err := ParseDInterval(parts[1])
			if err != nil {
				return nil, makeParseError(s, TypeInterval, err)
			}
			return &DInterval{Duration: ret.Add(dur)}, nil
		default:
			return nil, makeParseError(s, TypeInterval, fmt.Errorf("unknown format"))
		}
	} else if strings.ContainsRune(s, ' ') {
		// If it has a space, then we're most likely a postgres string,
		// as neither iso8601 nor golang permit spaces.
		dur, err := postgresToDuration(s)
		if err != nil {
			return nil, makeParseError(s, TypeInterval, err)
		}
		return &DInterval{Duration: dur}, nil
	} else if strings.ContainsRune(s, ':') {
		// Colon-separated intervals in Postgres are odd. They have day, hour,
		// minute, or second parts depending on number of fields and if the field
		// is an int or float.
		//
		// Instead of supporting unit changing based on int or float, use the
		// following rules:
		// - One field is S.
		// - Two fields is H:M.
		// - Three fields is H:M:S.
		// - All fields support both int and float.
		parts := strings.Split(s, ":")
		var err error
		var dur time.Duration
		switch len(parts) {
		case 2:
			dur, err = time.ParseDuration(parts[0] + "h" + parts[1] + "m")
		case 3:
			dur, err = time.ParseDuration(parts[0] + "h" + parts[1] + "m" + parts[2] + "s")
		default:
			return nil, makeParseError(s, TypeInterval, fmt.Errorf("unknown format"))
		}
		if err != nil {
			return nil, makeParseError(s, TypeInterval, err)
		}
		return &DInterval{Duration: duration.Duration{Nanos: dur.Nanoseconds()}}, nil
	}

	// An interval that's just a number is seconds.
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		s += "s"
	}
	// Fallback to golang durations.
	dur, err := time.ParseDuration(s)
	if err != nil {
		return nil, makeParseError(s, TypeInterval, err)
	}
	return &DInterval{Duration: duration.Duration{Nanos: dur.Nanoseconds()}}, nil
}

// ResolvedType implements the TypedExpr interface.
func (*DInterval) ResolvedType() Type {
	return TypeInterval
}

// Compare implements the Datum interface.
func (d *DInterval) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DInterval)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	return d.Duration.Compare(v.Duration)
}

// HasPrev implements the Datum interface.
func (*DInterval) HasPrev() bool {
	return false
}

// Prev implements the Datum interface.
func (d *DInterval) Prev() Datum {
	panic(makeUnsupportedMethodMessage(d, "Prev"))
}

// HasNext implements the Datum interface.
func (*DInterval) HasNext() bool {
	return false
}

// Next implements the Datum interface.
func (d *DInterval) Next() Datum {
	panic(makeUnsupportedMethodMessage(d, "Next"))
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

// Size implements the Datum interface.
func (d *DInterval) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DTuple is the tuple Datum.
type DTuple []Datum

// ResolvedType implements the TypedExpr interface.
func (d *DTuple) ResolvedType() Type {
	typ := make(TTuple, len(*d))
	for i, v := range *d {
		typ[i] = v.ResolvedType()
	}
	return typ
}

// Compare implements the Datum interface.
func (d *DTuple) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DTuple)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
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

// Size implements the Datum interface.
func (d *DTuple) Size() uintptr {
	sz := unsafe.Sizeof(*d)
	for _, e := range *d {
		dsz := e.Size()
		sz += dsz
	}
	return sz
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

// ResolvedType implements the TypedExpr interface.
func (dNull) ResolvedType() Type {
	return TypeNull
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
	panic(makeUnsupportedMethodMessage(d, "Prev"))
}

// HasNext implements the Datum interface.
func (dNull) HasNext() bool {
	return false
}

// Next implements the Datum interface.
func (d dNull) Next() Datum {
	panic(makeUnsupportedMethodMessage(d, "Next"))
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

// Size implements the Datum interface.
func (d dNull) Size() uintptr {
	return unsafe.Sizeof(d)
}

// DArray is the array Datum. Currently, all arrays are text arrays. Any Datum
// inserted into a DArray are treated as text during serialization. This is
// probably sufficient to provide basic ORM (ActiveRecord) support, but we'll
// need to support different array types (e.g. int[]) when we implement
// persistent storage and other features of arrays.
type DArray []Datum

// ResolvedType implements the TypedExpr interface.
func (d *DArray) ResolvedType() Type {
	return TypeArray
}

// Compare implements the Datum interface.
func (d *DArray) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DArray)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
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
func (d *DArray) HasPrev() bool {
	for i := len(*d) - 1; i >= 0; i-- {
		if (*d)[i].HasPrev() {
			return true
		}
	}
	return false
}

// Prev implements the Datum interface.
func (d *DArray) Prev() Datum {
	n := make(DArray, len(*d))
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
func (d *DArray) HasNext() bool {
	for i := len(*d) - 1; i >= 0; i-- {
		if (*d)[i].HasNext() {
			return true
		}
	}
	return false
}

// Next implements the Datum interface.
func (d *DArray) Next() Datum {
	n := make(DArray, len(*d))
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
func (*DArray) IsMax() bool {
	// Unimplemented for DArray. Seems possible to provide an implementation
	// which called IsMax for each of the elements, but currently this isn't
	// needed.
	return false
}

// IsMin implements the Datum interface.
func (*DArray) IsMin() bool {
	// Unimplemented for DArray. Seems possible to provide an implementation
	// which called IsMin for each of the elements, but currently this isn't
	// needed.
	return false
}

// Format implements the NodeFormatter interface.
func (d *DArray) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('{')
	for i, v := range *d {
		if i > 0 {
			buf.WriteString(",")
		}
		FormatNode(buf, f, v)
	}
	buf.WriteByte('}')
}

func (d *DArray) Len() int {
	return len(*d)
}

func (d *DArray) Less(i, j int) bool {
	return (*d)[i].Compare((*d)[j]) < 0
}

func (d *DArray) Swap(i, j int) {
	(*d)[i], (*d)[j] = (*d)[j], (*d)[i]
}

// Size implements the Datum interface.
func (d *DArray) Size() uintptr {
	sz := unsafe.Sizeof(*d)
	for _, e := range *d {
		dsz := e.Size()
		sz += dsz
	}
	return sz
}

// Temporary workaround for #3633, allowing comparisons between
// heterogeneous types.
// TODO(nvanbenschoten) Now that typing is improved, can we get rid of this?
func mixedTypeCompare(l, r Datum) (int, bool) {
	ltype := l.ResolvedType()
	rtype := r.ResolvedType()
	// Check equality.
	eqOp, ok := CmpOps[EQ].lookupImpl(ltype, rtype)
	if !ok {
		return 0, false
	}
	ctx := &EvalContext{}
	eq, err := eqOp.fn(ctx, l, r)
	if err != nil {
		panic(err)
	}
	if eq {
		return 0, true
	}

	// Check less than.
	ltOp, ok := CmpOps[LT].lookupImpl(ltype, rtype)
	if !ok {
		return 0, false
	}
	lt, err := ltOp.fn(ctx, l, r)
	if err != nil {
		panic(err)
	}
	if lt {
		return -1, true
	}
	return 1, true
}
