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

	"golang.org/x/text/collate"
	"golang.org/x/text/language"

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
	// Prev returns the previous datum and true, if one exists, or nil
	// and false.  The previous datum satisfied the following
	// definition: if the receiver is "b" and the returned datum is "a",
	// then "a < b" and no other datum will compare such that "a < c <
	// b".
	// The return value is undefined if `IsMin()` returns true.
	Prev() (Datum, bool)
	// IsMin returns true if the datum is equal to the minimum value the datum
	// type can hold.
	IsMin() bool

	// Next returns the next datum and true, if one exists, or nil
	// and false otherwise. The next datum satisfied the following
	// definition: if the receiver is "a" and the returned datum is "b",
	// then "a < b" and no other datum will compare such that "a < c <
	// b".
	// The return value is undefined if `IsMax()` returns true.
	Next() (Datum, bool)
	// IsMax returns true if the datum is equal to the maximum value the datum
	// type can hold.
	IsMax() bool

	// max() returns the upper value and true, if one exists, otherwise
	// nil and false. Used By Prev().
	max() (Datum, bool)

	// min() returns the lower value, if one exists, otherwise nil and
	// false. Used by Next().
	min() (Datum, bool)

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

// Prev implements the Datum interface.
func (*DBool) Prev() (Datum, bool) {
	return DBoolFalse, true
}

// Next implements the Datum interface.
func (*DBool) Next() (Datum, bool) {
	return DBoolTrue, true
}

// IsMax implements the Datum interface.
func (d *DBool) IsMax() bool {
	return bool(*d)
}

// IsMin implements the Datum interface.
func (d *DBool) IsMin() bool {
	return !bool(*d)
}

// min implements the Datum interface.
func (d *DBool) min() (Datum, bool) {
	return DBoolFalse, true
}

// max implements the Datum interface.
func (d *DBool) max() (Datum, bool) {
	return DBoolTrue, true
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

// Prev implements the Datum interface.
func (d *DInt) Prev() (Datum, bool) {
	return NewDInt(*d - 1), true
}

// Next implements the Datum interface.
func (d *DInt) Next() (Datum, bool) {
	return NewDInt(*d + 1), true
}

// IsMax implements the Datum interface.
func (d *DInt) IsMax() bool {
	return *d == math.MaxInt64
}

// IsMin implements the Datum interface.
func (d *DInt) IsMin() bool {
	return *d == math.MinInt64
}

var dMaxInt = NewDInt(math.MaxInt64)
var dMinInt = NewDInt(math.MinInt64)

// max implements the Datum interface.
func (d *DInt) max() (Datum, bool) {
	return dMaxInt, true
}

// min implements the Datum interface.
func (d *DInt) min() (Datum, bool) {
	return dMinInt, true
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

// Prev implements the Datum interface.
func (d *DFloat) Prev() (Datum, bool) {
	return NewDFloat(DFloat(math.Nextafter(float64(*d), math.Inf(-1)))), true
}

// Next implements the Datum interface.
func (d *DFloat) Next() (Datum, bool) {
	return NewDFloat(DFloat(math.Nextafter(float64(*d), math.Inf(1)))), true
}

var dMaxFloat = NewDFloat(DFloat(math.Inf(1)))
var dMinFloat = NewDFloat(DFloat(math.Inf(-1)))

// IsMax implements the Datum interface.
func (d *DFloat) IsMax() bool {
	return *d == *dMaxFloat
}

// IsMin implements the Datum interface.
func (d *DFloat) IsMin() bool {
	return *d == *dMinFloat
}

// max implements the Datum interface.
func (d *DFloat) max() (Datum, bool) {
	return dMaxFloat, true
}

// min implements the Datum interface.
func (d *DFloat) min() (Datum, bool) {
	return dMinFloat, true
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

// Prev implements the Datum interface.
func (d *DDecimal) Prev() (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DDecimal) Next() (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (*DDecimal) IsMax() bool {
	return false
}

// IsMin implements the Datum interface.
func (*DDecimal) IsMin() bool {
	return false
}

// max implements the Datum interface.
func (d *DDecimal) max() (Datum, bool) {
	return nil, false
}

// min implements the Datum interface.
func (d *DDecimal) min() (Datum, bool) {
	return nil, false
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

// DStringBacked is an interface implemented by types that are comparable by a
// backing DString.
type DStringBacked interface {
	BackingDString() *DString
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
	v, ok := other.(DStringBacked)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d.BackingDString() < *v.BackingDString() {
		return -1
	}
	if *d.BackingDString() > *v.BackingDString() {
		return 1
	}
	return 0
}

// Prev implements the Datum interface.
func (d *DString) Prev() (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DString) Next() (Datum, bool) {
	return NewDString(string(roachpb.Key(*d).Next())), true
}

// IsMax implements the Datum interface.
func (*DString) IsMax() bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DString) IsMin() bool {
	return len(*d) == 0
}

var dEmptyString = NewDString("")

// min implements the Datum interface.
func (d *DString) min() (Datum, bool) {
	return dEmptyString, true
}

// max implements the Datum interface.
func (d *DString) max() (Datum, bool) {
	return nil, false
}

// Format implements the NodeFormatter interface.
func (d *DString) Format(buf *bytes.Buffer, f FmtFlags) {
	encodeSQLString(buf, string(*d))
}

// Size implements the Datum interface.
func (d *DString) Size() uintptr {
	return unsafe.Sizeof(*d) + uintptr(len(*d))
}

// BackingDString implements the DStringBacked interface.
func (d *DString) BackingDString() *DString {
	return d
}

// DName is the Datum for column name literals.
type DName struct {
	DString DString
}

// NewDName is a helper routine to create a *DName initialized from its
// argument.
func NewDName(d string) *DName {
	r := DName{DString: DString(d)}
	return &r
}

// nameify is a convenience method to turn the result of (*DString, Bool)
// methods into (*DName, bool).
func nameify(d Datum, b bool) (Datum, bool) {
	return &DName{DString: *d.(*DString)}, b
}

// ResolvedType implements the TypedExpr interface.
func (d *DName) ResolvedType() Type { return TypeName }

// Prev implements the Datum interface.
func (d *DName) Prev() (Datum, bool) { return nameify(d.DString.Prev()) }

// Next implements the Datum interface.
func (d *DName) Next() (Datum, bool) { return nameify(d.DString.Next()) }

// Compare implements the Datum interface.
func (d *DName) Compare(other Datum) int {
	return d.DString.Compare(other)
}

// Format implements the NodeFormatter interface.
func (d *DName) Format(buf *bytes.Buffer, f FmtFlags) {
	d.DString.Format(buf, f)
}

// IsMax implements the Datum interface.
func (d *DName) IsMax() bool { return d.DString.IsMax() }

// IsMin implements the Datum interface.
func (d *DName) IsMin() bool { return d.DString.IsMin() }

// Size implements the Datum interface.
func (d *DName) Size() uintptr { return d.DString.Size() }

// min implements the Datum interface.
func (d *DName) min() (Datum, bool) { return nameify(d.DString.min()) }

// max implements the Datum interface.
func (d *DName) max() (Datum, bool) { return nameify(d.DString.max()) }

// BackingDString implements the DStringBacked interface.
func (d *DName) BackingDString() *DString {
	return &d.DString
}

// DCollatedString is the Datum for strings with a locale. The struct members
// are intended to be immutable.
type DCollatedString struct {
	Contents string
	Locale   string
	// key is the collation key.
	key []byte
}

// CollationEnvironment stores the state needed by NewDCollatedString to
// construct collation keys efficiently.
type CollationEnvironment struct {
	cache  map[string]collationEnvironmentCacheEntry
	buffer collate.Buffer
}

type collationEnvironmentCacheEntry struct {
	// locale is interned.
	locale string
	// collator is an expensive factory.
	collator *collate.Collator
}

func (env *CollationEnvironment) getCacheEntry(locale string) collationEnvironmentCacheEntry {
	entry, ok := env.cache[locale]
	if !ok {
		if env.cache == nil {
			env.cache = make(map[string]collationEnvironmentCacheEntry)
		}
		entry = collationEnvironmentCacheEntry{locale, collate.New(language.MustParse(locale))}
		env.cache[locale] = entry
	}
	return entry
}

// NewDCollatedString is a helper routine to create a *DCollatedString. Panics
// if locale is invalid. Not safe for concurrent use.
func NewDCollatedString(contents string, locale string, env *CollationEnvironment) *DCollatedString {
	entry := env.getCacheEntry(locale)
	key := entry.collator.KeyFromString(&env.buffer, contents)
	d := DCollatedString{contents, entry.locale, make([]byte, len(key))}
	copy(d.key, key)
	env.buffer.Reset()
	return &d
}

// Format implements the NodeFormatter interface.
func (d *DCollatedString) Format(buf *bytes.Buffer, f FmtFlags) {
	encodeSQLString(buf, d.Contents)
}

// ResolvedType implements the TypedExpr interface.
func (d *DCollatedString) ResolvedType() Type {
	return TCollatedString{d.Locale}
}

// Compare implements the Datum interface.
func (d *DCollatedString) Compare(other Datum) int {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1
	}
	v, ok := other.(*DCollatedString)
	if !ok || d.Locale != v.Locale {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	return bytes.Compare(d.key, v.key)
}

// Prev implements the Datum interface.
func (d *DCollatedString) Prev() (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DCollatedString) Next() (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (*DCollatedString) IsMax() bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DCollatedString) IsMin() bool {
	return d.Contents == ""
}

// min implements the Datum interface.
func (d *DCollatedString) min() (Datum, bool) {
	return &DCollatedString{"", d.Locale, nil}, true
}

// max implements the Datum interface.
func (d *DCollatedString) max() (Datum, bool) {
	return nil, false
}

// Size implements the Datum interface.
func (d *DCollatedString) Size() uintptr {
	return unsafe.Sizeof(*d) + uintptr(len(d.Contents)) + uintptr(len(d.Locale)) + uintptr(len(d.key))
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

// Prev implements the Datum interface.
func (d *DBytes) Prev() (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DBytes) Next() (Datum, bool) {
	return NewDBytes(DBytes(roachpb.Key(*d).Next())), true
}

// IsMax implements the Datum interface.
func (*DBytes) IsMax() bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DBytes) IsMin() bool {
	return len(*d) == 0
}

var dEmptyBytes = NewDBytes(DBytes(""))

// min implements the Datum interface.
func (d *DBytes) min() (Datum, bool) {
	return dEmptyBytes, true
}

// max implements the Datum interface.
func (d *DBytes) max() (Datum, bool) {
	return nil, false
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

// Prev implements the Datum interface.
func (d *DDate) Prev() (Datum, bool) {
	return NewDDate(*d - 1), true
}

// Next implements the Datum interface.
func (d *DDate) Next() (Datum, bool) {
	return NewDDate(*d + 1), true
}

// IsMax implements the Datum interface.
func (d *DDate) IsMax() bool {
	return *d == math.MaxInt64
}

// IsMin implements the Datum interface.
func (d *DDate) IsMin() bool {
	return *d == math.MinInt64
}

// max implements the Datum interface.
func (d *DDate) max() (Datum, bool) {
	// TODO(knz) figure a good way to find a maximum.
	return nil, false
}

// min implements the Datum interface.
func (d *DDate) min() (Datum, bool) {
	// TODO(knz) figure a good way to find a minimum.
	return nil, false
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
	// use time.UTC rather than the session location. Unfortunately this also means
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

// Prev implements the Datum interface.
func (d *DTimestamp) Prev() (Datum, bool) {
	return &DTimestamp{Time: d.Add(-time.Microsecond)}, true
}

// Next implements the Datum interface.
func (d *DTimestamp) Next() (Datum, bool) {
	return &DTimestamp{Time: d.Add(time.Microsecond)}, true
}

// IsMax implements the Datum interface.
func (d *DTimestamp) IsMax() bool {
	// Adding 1 overflows to a smaller value
	tNext := d.Time.Add(time.Microsecond)
	return d.After(tNext)
}

// IsMin implements the Datum interface.
func (d *DTimestamp) IsMin() bool {
	// Subtracting 1 underflows to a larger value.
	tPrev := d.Time.Add(-time.Microsecond)
	return d.Before(tPrev)
}

// min implements the Datum interface.
func (d *DTimestamp) min() (Datum, bool) {
	// TODO(knz) figure a good way to find a minimum.
	return nil, false
}

// max implements the Datum interface.
func (d *DTimestamp) max() (Datum, bool) {
	// TODO(knz) figure a good way to find a minimum.
	return nil, false
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

// Prev implements the Datum interface.
func (d *DTimestampTZ) Prev() (Datum, bool) {
	return &DTimestampTZ{Time: d.Add(-time.Microsecond)}, true
}

// Next implements the Datum interface.
func (d *DTimestampTZ) Next() (Datum, bool) {
	return &DTimestampTZ{Time: d.Add(time.Microsecond)}, true
}

// IsMax implements the Datum interface.
func (d *DTimestampTZ) IsMax() bool {
	// Adding 1 overflows to a smaller value
	tNext := d.Time.Add(time.Microsecond)
	return d.After(tNext)
}

// IsMin implements the Datum interface.
func (d *DTimestampTZ) IsMin() bool {
	// Subtracting 1 underflows to a larger value.
	tPrev := d.Time.Add(-time.Microsecond)
	return d.Before(tPrev)
}

// min implements the Datum interface.
func (d *DTimestampTZ) min() (Datum, bool) {
	// TODO(knz) figure a good way to find a minimum.
	return nil, false
}

// max implements the Datum interface.
func (d *DTimestampTZ) max() (Datum, bool) {
	// TODO(knz) figure a good way to find a minimum.
	return nil, false
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
	// - SQL standard.
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

// Prev implements the Datum interface.
func (d *DInterval) Prev() (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DInterval) Next() (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (d *DInterval) IsMax() bool {
	return d.Months == math.MaxInt64 && d.Days == math.MaxInt64 && d.Nanos == math.MaxInt64
}

// IsMin implements the Datum interface.
func (d *DInterval) IsMin() bool {
	return d.Months == math.MinInt64 && d.Days == math.MinInt64 && d.Nanos == math.MinInt64
}

var dMaxInterval = &DInterval{
	duration.Duration{
		Months: math.MaxInt64,
		Days:   math.MaxInt64,
		Nanos:  math.MaxInt64,
	}}
var dMinInterval = &DInterval{
	duration.Duration{
		Months: math.MinInt64,
		Days:   math.MinInt64,
		Nanos:  math.MinInt64,
	}}

// max implements the Datum interface.
func (d *DInterval) max() (Datum, bool) {
	return dMaxInterval, true
}

// min implements the Datum interface.
func (d *DInterval) min() (Datum, bool) {
	return dMinInterval, true
}

// ValueAsString returns the interval as a string (e.g. "1h2m").
func (d *DInterval) ValueAsString() string {
	if d.Months != 0 || d.Days != 0 {
		return d.Duration.String()
	}
	// TODO(radu): we should use Postgres syntax instead of converting to nanoseconds.
	return (time.Duration(d.Duration.Nanos) * time.Nanosecond).String()
}

// Format implements the NodeFormatter interface. Example: "INTERVAL `1h2m`".
func (d *DInterval) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("INTERVAL '")
	buf.WriteString(d.ValueAsString())
	buf.WriteByte('\'')
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

// Prev implements the Datum interface.
func (d *DTuple) Prev() (Datum, bool) {
	// Note: (a:decimal, b:int, c:int) has a prev value; that's (a, b,
	// c-1). With an exception if c is MinInt64, in which case the prev
	// value is (a, b-1, max()). However, (a:int, b:decimal) does not
	// have a prev value, because decimal doesn't have one.
	//
	// In general, a tuple has a prev value if and only if it ends with
	// zero or more values that are a minimum and a maximum value of the
	// same type exists, and the first element before that has a prev
	// value.
	n := make(DTuple, len(*d))
	copy(n, *d)
	for i := len(n) - 1; i >= 0; i-- {
		if !n[i].IsMin() {
			prevVal, ok := n[i].Prev()
			if !ok {
				return nil, false
			}
			n[i] = prevVal
			break
		}
		maxVal, ok := n[i].max()
		if !ok {
			return nil, false
		}
		n[i] = maxVal
	}
	return &n, true
}

// Next implements the Datum interface.
func (d *DTuple) Next() (Datum, bool) {
	// Note: (a:decimal, b:int, c:int) has a next value; that's (a, b,
	// c+1). With an exception if c is MaxInt64, in which case the next
	// value is (a, b+1, min()). However, (a:int, b:decimal) does not
	// have a next value, because decimal doesn't have one.
	//
	// In general, a tuple has a next value if and only if it ends with
	// zero or more values that are a maximum and a minimum value of the
	// same type exists, and the first element before that has a next
	// value.
	n := make(DTuple, len(*d))
	copy(n, *d)
	for i := len(n) - 1; i >= 0; i-- {
		if !n[i].IsMax() {
			nextVal, ok := n[i].Next()
			if !ok {
				return nil, false
			}
			n[i] = nextVal
			break
		}
		minVal, ok := n[i].min()
		if !ok {
			return nil, false
		}
		n[i] = minVal
	}
	return &n, true
}

// max implements the Datum interface.
func (d *DTuple) max() (Datum, bool) {
	res := make(DTuple, len(*d))
	for i, v := range *d {
		m, ok := v.max()
		if !ok {
			return nil, false
		}
		res[i] = m
	}
	return &res, true
}

// max implements the Datum interface.
func (d *DTuple) min() (Datum, bool) {
	res := make(DTuple, len(*d))
	for i, v := range *d {
		m, ok := v.min()
		if !ok {
			return nil, false
		}
		res[i] = m
	}
	return &res, true
}

// IsMax implements the Datum interface.
func (d *DTuple) IsMax() bool {
	for _, v := range *d {
		if !v.IsMax() {
			return false
		}
	}
	return true
}

// IsMin implements the Datum interface.
func (d *DTuple) IsMin() bool {
	for _, v := range *d {
		if !v.IsMin() {
			return false
		}
	}
	return true
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

// Prev implements the Datum interface.
func (d dNull) Prev() (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d dNull) Next() (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (dNull) IsMax() bool {
	return true
}

// IsMin implements the Datum interface.
func (dNull) IsMin() bool {
	return true
}

// max implements the Datum interface.
func (dNull) max() (Datum, bool) {
	return DNull, true
}

// min implements the Datum interface.
func (dNull) min() (Datum, bool) {
	return DNull, true
}

// Format implements the NodeFormatter interface.
func (dNull) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("NULL")
}

// Size implements the Datum interface.
func (d dNull) Size() uintptr {
	return unsafe.Sizeof(d)
}

// DArray is the array Datum. Any Datum inserted into a DArray are treated as
// text during serialization.
type DArray struct {
	ParamTyp Type
	Array    []Datum
}

// NewDArray returns a DArray containing elements of the specified type.
func NewDArray(paramTyp Type) *DArray {
	return &DArray{ParamTyp: paramTyp}
}

// ResolvedType implements the TypedExpr interface.
func (d *DArray) ResolvedType() Type {
	return tArray{Typ: d.ParamTyp}
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
	n := d.Len()
	if n > v.Len() {
		n = v.Len()
	}
	for i := 0; i < n; i++ {
		c := d.Array[i].Compare(v.Array[i])
		if c != 0 {
			return c
		}
	}
	if d.Len() < v.Len() {
		return -1
	}
	if d.Len() > v.Len() {
		return 1
	}
	return 0
}

// Prev implements the Datum interface.
func (d *DArray) Prev() (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DArray) Next() (Datum, bool) {
	a := DArray{ParamTyp: d.ParamTyp, Array: make([]Datum, d.Len()+1)}
	copy(a.Array, d.Array)
	a.Array[len(a.Array)-1] = DNull
	return &a, true
}

// max implements the Datum interface.
func (d *DArray) max() (Datum, bool) {
	return nil, false
}

// min implements the Datum interface.
func (d *DArray) min() (Datum, bool) {
	return &DArray{ParamTyp: d.ParamTyp}, true
}

// IsMax implements the Datum interface.
func (d *DArray) IsMax() bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DArray) IsMin() bool {
	return d.Len() == 0
}

// Format implements the NodeFormatter interface.
func (d *DArray) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('{')
	for i, v := range d.Array {
		if i > 0 {
			buf.WriteString(",")
		}
		FormatNode(buf, f, v)
	}
	buf.WriteByte('}')
}

func (d *DArray) Len() int {
	return len(d.Array)
}

func (d *DArray) Less(i, j int) bool {
	return d.Array[i].Compare(d.Array[j]) < 0
}

func (d *DArray) Swap(i, j int) {
	d.Array[i], d.Array[j] = d.Array[j], d.Array[i]
}

// Size implements the Datum interface.
func (d *DArray) Size() uintptr {
	sz := unsafe.Sizeof(*d)
	for _, e := range d.Array {
		dsz := e.Size()
		sz += dsz
	}
	return sz
}

var errNonHomogeneousArray = errors.New("multidimensional arrays must have array expressions with matching dimensions")

// Append appends a Datum to the array, whose parameterized type must be
// consistent with the type of the Datum.
func (d *DArray) Append(v Datum) error {
	if v != DNull && !d.ParamTyp.Equal(v.ResolvedType()) {
		return errors.Errorf("cannot append %s to array containing %s", d.ParamTyp,
			v.ResolvedType())
	}
	if _, ok := d.ParamTyp.(tArray); ok {
		if v == DNull {
			return errNonHomogeneousArray
		}
		if d.Len() > 0 {
			prevItem := d.Array[d.Len()-1]
			if prevItem == DNull {
				return errNonHomogeneousArray
			}
			expectedLen := prevItem.(*DArray).Len()
			if v.(*DArray).Len() != expectedLen {
				return errNonHomogeneousArray
			}
		}
	}
	d.Array = append(d.Array, v)
	return nil
}

// DTable is the table Datum. It is used for datums that hold an
// entire table generator. See the comments in generator_builtins.go
// for details.
type DTable struct {
	ValueGenerator
}

// Format implements the NodeFormatter interface.
func (t *DTable) Format(buf *bytes.Buffer, _ FmtFlags) {
	buf.WriteString("<generated>")
}

// ResolvedType implements the TypedExpr interface.
func (t *DTable) ResolvedType() Type {
	return TTable{t.ValueGenerator.ColumnTypes()}
}

// Compare implements the Datum interface.
func (t *DTable) Compare(other Datum) int {
	if o, ok := other.(*DTable); ok {
		if o.ValueGenerator == t.ValueGenerator {
			return 0
		}
	}
	return -1
}

// Prev implements the Datum interface.
func (*DTable) Prev() (Datum, bool) { return nil, false }

// Next implements the Datum interface.
func (*DTable) Next() (Datum, bool) { return nil, false }

// IsMax implements the Datum interface.
func (*DTable) IsMax() bool { return false }

// IsMin implements the Datum interface.
func (*DTable) IsMin() bool { return false }

// max implements the Datum interface.
func (*DTable) max() (Datum, bool) { return nil, false }

// min implements the Datum interface.
func (*DTable) min() (Datum, bool) { return nil, false }

// Size implements the Datum interface.
func (*DTable) Size() uintptr { return unsafe.Sizeof(DTable{}) }

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
