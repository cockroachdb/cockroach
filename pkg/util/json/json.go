// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	uniq "github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/errors"
)

// Type represents a JSON type.
type Type int

// This enum defines the ordering of types. It should not be reordered.
const (
	_ Type = iota
	NullJSONType
	StringJSONType
	NumberJSONType
	FalseJSONType
	TrueJSONType
	ArrayJSONType
	ObjectJSONType
)

const (
	wordSize          = unsafe.Sizeof(big.Word(0))
	decimalSize       = unsafe.Sizeof(apd.Decimal{})
	stringHeaderSize  = unsafe.Sizeof(reflect.StringHeader{})
	sliceHeaderSize   = unsafe.Sizeof(reflect.SliceHeader{})
	keyValuePairSize  = unsafe.Sizeof(jsonKeyValuePair{})
	jsonInterfaceSize = unsafe.Sizeof((JSON)(nil))
)

const (
	msgModifyAfterBuild = "modify after Build()"
)

// JSON represents a JSON value.
type JSON interface {
	fmt.Stringer

	Compare(JSON) (int, error)
	// Type returns the JSON type.
	Type() Type
	// Format writes out the JSON document to the specified buffer.
	Format(buf *bytes.Buffer)
	// Size returns the size of the JSON document in bytes.
	Size() uintptr

	// encodeInvertedIndexKeys takes in a key prefix and returns a slice of
	// inverted index keys, one per path through the receiver.
	encodeInvertedIndexKeys(b []byte) ([][]byte, error)

	// encodeContainingInvertedIndexSpans takes in a key prefix and returns the
	// spans that must be scanned in the inverted index to evaluate a contains (@>)
	// predicate with the given JSON (i.e., find the objects in the index that
	// contain the given JSON).
	//
	// The spans are returned in an inverted.SpanExpression, which represents the
	// set operations that must be applied on the spans read during execution. See
	// comments in the SpanExpression definition for details.
	//
	// If isRoot is true, this function is being called at the root level of the
	// JSON hierarchy. If isObjectValue is true, the given JSON is the value of a
	// JSON object key. Note that isRoot and isObjectValue cannot both be true at
	// the same time.
	encodeContainingInvertedIndexSpans(
		b []byte, isRoot, isObjectValue bool,
	) (invertedExpr inverted.Expression, err error)

	// encodeContainedInvertedIndexSpans takes in a key prefix and returns the
	// spans that must be scanned in the inverted index to evaluate a contained
	// by (<@) predicate with the given JSON (i.e., find the objects in the index
	// that are contained by the given JSON).
	//
	// The spans are returned in an inverted.SpanExpression, which represents the
	// set operations that must be applied on the spans read during execution. See
	// comments in the SpanExpression definition for details.
	//
	// If isRoot is true, this function is being called at the root level of the
	// JSON hierarchy. If isObjectValue is true, the given JSON is the value of a
	// JSON object key. Note that isRoot and isObjectValue cannot both be true at
	// the same time.
	encodeContainedInvertedIndexSpans(
		b []byte, isRoot, isObjectValue bool,
	) (invertedExpr inverted.Expression, err error)

	// numInvertedIndexEntries returns the number of entries that will be
	// produced if this JSON gets included in an inverted index.
	numInvertedIndexEntries() (int, error)

	// allPaths returns a slice of new JSON documents, each a path to a leaf
	// through the receiver. Note that leaves include the empty object and array
	// in addition to scalars.
	allPaths() ([]JSON, error)

	// FetchValKey implements the `->` operator for strings, returning nil if the
	// key is not found.
	FetchValKey(key string) (JSON, error)

	// FetchValIdx implements the `->` operator for ints, returning nil if the
	// key is not found.
	FetchValIdx(idx int) (JSON, error)

	// FetchValKeyOrIdx is used for path access, if obj is an object, it tries to
	// access the given field. If it's an array, it interprets the key as an int
	// and tries to access the given index.
	FetchValKeyOrIdx(key string) (JSON, error)

	// RemoveString implements the `-` operator for strings, returning JSON after removal,
	// whether removal is valid and error message.
	RemoveString(s string) (JSON, bool, error)

	// RemoveIndex implements the `-` operator for ints, returning JSON after removal,
	// whether removal is valid and error message.
	RemoveIndex(idx int) (JSON, bool, error)

	// RemovePath and doRemovePath implement the `#-` operator for strings, returning JSON after removal,
	// whether removal is valid and error message.
	RemovePath(path []string) (JSON, bool, error)
	doRemovePath(path []string) (JSON, bool, error)

	// Concat implements the `||` operator.
	Concat(other JSON) (JSON, error)

	// AsText returns the JSON document as a string, with quotes around strings removed, and null as nil.
	AsText() (*string, error)

	// AsDecimal returns the JSON document as a apd.Decimal if it is a numeric
	// type, and a boolean inidicating if this JSON document is a numeric type.
	AsDecimal() (*apd.Decimal, bool)

	// Exists implements the `?` operator: does the string exist as a top-level
	// key within the JSON value?
	//
	// If the object is a JSON array, returns true when the key is a top-level
	// element of the array.
	Exists(string) (bool, error)

	// StripNulls returns the JSON document with all object fields that have null values omitted
	// and whether it needs to strip nulls. Stripping nulls is needed only if it contains some
	// object fields having null values.
	StripNulls() (JSON, bool, error)

	// ObjectIter returns an *ObjectKeyIterator, nil if json is not an object.
	ObjectIter() (*ObjectIterator, error)

	// isScalar returns whether the JSON document is null, true, false, a string,
	// or a number.
	isScalar() bool

	// preprocessForContains converts a JSON document to an internal interface
	// which is used to efficiently implement the @> operator.
	preprocessForContains() (containsable, error)

	// encode appends the encoding of the JSON document to appendTo, returning
	// the result alongside the JEntry for the document. Note that some values
	// (true/false/null) are encoded with 0 bytes and are purely defined by their
	// JEntry.
	encode(appendTo []byte) (jEntry jEntry, b []byte, err error)

	// MaybeDecode returns an equivalent JSON which is not a jsonEncoded.
	MaybeDecode() JSON

	// toGoRepr returns the Go-style representation of this JSON value
	// (map[string]interface{} for objects, etc.).
	toGoRepr() (interface{}, error)

	// tryDecode returns an equivalent JSON which is not a jsonEncoded, returning
	// an error if the encoded data was corrupt.
	tryDecode() (JSON, error)

	// Len returns the number of outermost elements in the JSON document if it is an object or an array.
	// Otherwise, Len returns 0.
	Len() int

	// HasContainerLeaf returns whether this document contains in it somewhere
	// either the empty array or the empty object.
	HasContainerLeaf() (bool, error)
}

type jsonTrue struct{}

// TrueJSONValue is JSON `true`
var TrueJSONValue = jsonTrue{}

type jsonFalse struct{}

// FalseJSONValue is JSON `false`
var FalseJSONValue = jsonFalse{}

type jsonNull struct{}

// NullJSONValue is JSON `null`
var NullJSONValue = jsonNull{}

type jsonNumber apd.Decimal
type jsonString string

type jsonArray []JSON

type jsonKeyValuePair struct {
	k jsonString
	v JSON
}

// ArrayBuilder builds JSON Array by a JSON sequence.
type ArrayBuilder struct {
	jsons []JSON
}

// NewArrayBuilder returns an ArrayBuilder. The builder will reserve spaces
// based on hint about number of adds to reduce times of growing capacity.
func NewArrayBuilder(numAddsHint int) *ArrayBuilder {
	return &ArrayBuilder{
		jsons: make([]JSON, 0, numAddsHint),
	}
}

// Add appends JSON to the sequence.
func (b *ArrayBuilder) Add(j JSON) {
	b.jsons = append(b.jsons, j)
}

// Build returns the constructed JSON array. A caller may not modify the array,
// and the ArrayBuilder reserves the right to re-use the array returned (though
// the data will not be modified).  This is important in the case of a window
// function, which might want to incrementally update an aggregation.
func (b *ArrayBuilder) Build() JSON {
	return jsonArray(b.jsons)
}

// ArrayBuilderWithCounter builds JSON Array by a JSON sequence with a size counter.
type ArrayBuilderWithCounter struct {
	ab   *ArrayBuilder
	size uintptr
}

// NewArrayBuilderWithCounter returns an ArrayBuilderWithCounter.
func NewArrayBuilderWithCounter() *ArrayBuilderWithCounter {
	return &ArrayBuilderWithCounter{
		ab:   NewArrayBuilder(0),
		size: sliceHeaderSize,
	}
}

// Add appends JSON to the sequence and updates the size counter.
func (b *ArrayBuilderWithCounter) Add(j JSON) {
	oldCap := cap(b.ab.jsons)
	b.ab.Add(j)
	b.size += j.Size() + (uintptr)(cap(b.ab.jsons)-oldCap)*jsonInterfaceSize
}

// Build returns a JSON array built from a JSON sequence. After that, it should
// not be modified any longer.
func (b *ArrayBuilderWithCounter) Build() JSON {
	return b.ab.Build()
}

// Size returns the size in bytes of the JSON Array the builder is going to build.
func (b *ArrayBuilderWithCounter) Size() uintptr {
	return b.size
}

// ObjectBuilderWithCounter builds a JSON object a key/value pair at a time, keeping the memory usage of the object.
type ObjectBuilderWithCounter struct {
	ob   *ObjectBuilder
	size uintptr
}

// NewObjectBuilderWithCounter creates and instantiates ObjectBuilder with memory counter.
func NewObjectBuilderWithCounter() *ObjectBuilderWithCounter {
	ob := NewObjectBuilder(0)
	return &ObjectBuilderWithCounter{
		ob: ob,
		// initial memory allocation
		size: unsafe.Sizeof(ob) + jsonInterfaceSize,
	}
}

// Add appends key value pair to the sequence and updates
// amount of memory allocated for the overall keys and values.
func (b *ObjectBuilderWithCounter) Add(k string, v JSON) {
	b.ob.Add(k, v)
	// Size of added JSON + overhead of storing key/value pair + the size of the key.
	b.size += v.Size() + keyValuePairSize + uintptr(len(k))
}

// Build returns a JSON object built from a key value pair sequence. After that,
// it should not be modified any longer.
func (b *ObjectBuilderWithCounter) Build() JSON {
	return b.ob.Build()
}

// Size returns the size in bytes of the JSON object the builder is going to build.
func (b *ObjectBuilderWithCounter) Size() uintptr {
	return b.size
}

// ObjectBuilder builds JSON Object by a key value pair sequence.
type ObjectBuilder struct {
	pairs []jsonKeyValuePair
}

// NewObjectBuilder returns an ObjectBuilder. The builder will reserve spaces
// based on hint about number of adds to reduce times of growing capacity.
func NewObjectBuilder(numAddsHint int) *ObjectBuilder {
	return &ObjectBuilder{
		pairs: make([]jsonKeyValuePair, 0, numAddsHint),
	}
}

// Add appends key value pair to the sequence.
func (b *ObjectBuilder) Add(k string, v JSON) {
	if b.pairs == nil {
		panic(errors.AssertionFailedf(msgModifyAfterBuild))
	}
	b.pairs = append(b.pairs, jsonKeyValuePair{k: jsonString(k), v: v})
}

// Build returns a JSON object built from a key value pair sequence. After that,
// it should not be modified any longer.
func (b *ObjectBuilder) Build() JSON {
	if b.pairs == nil {
		panic(errors.AssertionFailedf(msgModifyAfterBuild))
	}
	orders := make([]int, len(b.pairs))
	for i := range orders {
		orders[i] = i
	}
	sorter := pairSorter{
		pairs:        b.pairs,
		orders:       orders,
		hasNonUnique: false,
	}
	b.pairs = nil
	sort.Sort(&sorter)
	sorter.unique()
	return jsonObject(sorter.pairs)
}

// pairSorter sorts and uniqueifies JSON pairs. In order to keep
// the last one for pairs with the same key while sort.Sort is
// not stable, pairSorter uses []int orders to maintain order and
// bool hasNonUnique to skip unnecessary uniqueifying.
type pairSorter struct {
	pairs        []jsonKeyValuePair
	orders       []int
	hasNonUnique bool
}

func (s *pairSorter) Len() int {
	return len(s.pairs)
}

func (s *pairSorter) Less(i, j int) bool {
	cmp := strings.Compare(string(s.pairs[i].k), string(s.pairs[j].k))
	if cmp != 0 {
		return cmp == -1
	}
	s.hasNonUnique = true
	// The element with greater order has lower rank when their keys
	// are same, since unique algorithm will prefer first element.
	return s.orders[i] > s.orders[j]
}

func (s *pairSorter) Swap(i, j int) {
	s.pairs[i], s.orders[i], s.pairs[j], s.orders[j] = s.pairs[j], s.orders[j], s.pairs[i], s.orders[i]
}

func (s *pairSorter) unique() {
	// If there are any duplicate keys, then in sorted order it will have
	// two pairs with rank i and i + 1 whose keys are same.
	// For sorting based on comparisons, if two unique elements (pair.k, order)
	// have rank i and i + 1, they have to compare once to figure out their
	// relative order in the final position i and i + 1. So if there are any
	// equal elements, then the sort must have compared them at some point.
	if !s.hasNonUnique {
		return
	}
	top := 0
	for i := 1; i < len(s.pairs); i++ {
		if s.pairs[top].k != s.pairs[i].k {
			top++
			if top != i {
				s.pairs[top] = s.pairs[i]
			}
		}
	}
	s.pairs = s.pairs[:top+1]
}

// jsonObject represents a JSON object as a sorted-by-key list of key-value
// pairs, which are unique by key.
type jsonObject []jsonKeyValuePair

var emptyJSONObject = jsonObject(nil)
var emptyJSONArray = jsonArray(nil)

func (jsonNull) Type() Type   { return NullJSONType }
func (jsonFalse) Type() Type  { return FalseJSONType }
func (jsonTrue) Type() Type   { return TrueJSONType }
func (jsonNumber) Type() Type { return NumberJSONType }
func (jsonString) Type() Type { return StringJSONType }
func (jsonArray) Type() Type  { return ArrayJSONType }
func (jsonObject) Type() Type { return ObjectJSONType }

func (j jsonNull) MaybeDecode() JSON   { return j }
func (j jsonFalse) MaybeDecode() JSON  { return j }
func (j jsonTrue) MaybeDecode() JSON   { return j }
func (j jsonNumber) MaybeDecode() JSON { return j }
func (j jsonString) MaybeDecode() JSON { return j }
func (j jsonArray) MaybeDecode() JSON  { return j }
func (j jsonObject) MaybeDecode() JSON { return j }

func (j jsonNull) AsDecimal() (*apd.Decimal, bool)   { return nil, false }
func (j jsonFalse) AsDecimal() (*apd.Decimal, bool)  { return nil, false }
func (j jsonTrue) AsDecimal() (*apd.Decimal, bool)   { return nil, false }
func (j jsonString) AsDecimal() (*apd.Decimal, bool) { return nil, false }
func (j jsonArray) AsDecimal() (*apd.Decimal, bool)  { return nil, false }
func (j jsonObject) AsDecimal() (*apd.Decimal, bool) { return nil, false }
func (j jsonNumber) AsDecimal() (*apd.Decimal, bool) {
	d := apd.Decimal(j)
	return &d, true
}

func (j jsonNull) tryDecode() (JSON, error)   { return j, nil }
func (j jsonFalse) tryDecode() (JSON, error)  { return j, nil }
func (j jsonTrue) tryDecode() (JSON, error)   { return j, nil }
func (j jsonNumber) tryDecode() (JSON, error) { return j, nil }
func (j jsonString) tryDecode() (JSON, error) { return j, nil }
func (j jsonArray) tryDecode() (JSON, error)  { return j, nil }
func (j jsonObject) tryDecode() (JSON, error) { return j, nil }

func cmpJSONTypes(a Type, b Type) int {
	if b > a {
		return -1
	}
	if b < a {
		return 1
	}
	return 0
}

func (j jsonNull) Compare(other JSON) (int, error)  { return cmpJSONTypes(j.Type(), other.Type()), nil }
func (j jsonFalse) Compare(other JSON) (int, error) { return cmpJSONTypes(j.Type(), other.Type()), nil }
func (j jsonTrue) Compare(other JSON) (int, error)  { return cmpJSONTypes(j.Type(), other.Type()), nil }

func decodeIfNeeded(j JSON) (JSON, error) {
	if enc, ok := j.(*jsonEncoded); ok {
		var err error
		j, err = enc.decode()
		if err != nil {
			return nil, err
		}
	}
	return j, nil
}

func (j jsonNumber) Compare(other JSON) (int, error) {
	cmp := cmpJSONTypes(j.Type(), other.Type())
	if cmp != 0 {
		return cmp, nil
	}
	var err error
	if other, err = decodeIfNeeded(other); err != nil {
		return 0, err
	}
	dec := apd.Decimal(j)
	o := apd.Decimal(other.(jsonNumber))
	return dec.Cmp(&o), nil
}

func (j jsonString) Compare(other JSON) (int, error) {
	cmp := cmpJSONTypes(j.Type(), other.Type())
	if cmp != 0 {
		return cmp, nil
	}
	// TODO(justin): we should optimize this, we don't have to decode the whole thing.
	var err error
	if other, err = decodeIfNeeded(other); err != nil {
		return 0, err
	}
	o := other.(jsonString)
	if o > j {
		return -1, nil
	}
	if o < j {
		return 1, nil
	}
	return 0, nil
}

func (j jsonArray) Compare(other JSON) (int, error) {
	cmp := cmpJSONTypes(j.Type(), other.Type())
	if cmp != 0 {
		return cmp, nil
	}
	lenJ := j.Len()
	lenO := other.Len()
	if lenJ < lenO {
		return -1, nil
	}
	if lenJ > lenO {
		return 1, nil
	}
	// TODO(justin): we should optimize this, we don't have to decode the whole thing.
	var err error
	if other, err = decodeIfNeeded(other); err != nil {
		return 0, err
	}
	o := other.(jsonArray)
	for i := 0; i < lenJ; i++ {
		cmp, err := j[i].Compare(o[i])
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

func (j jsonObject) Compare(other JSON) (int, error) {
	cmp := cmpJSONTypes(j.Type(), other.Type())
	if cmp != 0 {
		return cmp, nil
	}
	lenJ := j.Len()
	lenO := other.Len()
	if lenJ < lenO {
		return -1, nil
	}
	if lenJ > lenO {
		return 1, nil
	}
	// TODO(justin): we should optimize this, we don't have to decode the whole thing.
	var err error
	if other, err = decodeIfNeeded(other); err != nil {
		return 0, err
	}
	o := other.(jsonObject)
	for i := 0; i < lenJ; i++ {
		cmpKey, err := j[i].k.Compare(o[i].k)
		if err != nil {
			return 0, err
		}
		if cmpKey != 0 {
			return cmpKey, nil
		}
		cmpVal, err := j[i].v.Compare(o[i].v)
		if err != nil {
			return 0, err
		}
		if cmpVal != 0 {
			return cmpVal, nil
		}
	}
	return 0, nil
}

var errTrailingCharacters = pgerror.WithCandidateCode(errors.New("trailing characters after JSON document"), pgcode.InvalidTextRepresentation)

func (jsonNull) Format(buf *bytes.Buffer) { buf.WriteString("null") }

func (jsonFalse) Format(buf *bytes.Buffer) { buf.WriteString("false") }

func (jsonTrue) Format(buf *bytes.Buffer) { buf.WriteString("true") }

func (j jsonNumber) Format(buf *bytes.Buffer) {
	dec := apd.Decimal(j)
	// Make sure non-finite values are encoded as valid strings by
	// quoting them. Unfortunately, since this is JSON, there's no
	// defined way to express the three special numeric values (+inf,
	// -inf, nan) except as a string. This means that the decoding
	// side can't tell whether the field should be a float or a
	// string. Testing for exact types is thus tricky. As of this
	// comment, our current tests for this behavior happen it the SQL
	// package, not here in the JSON package.
	nonfinite := dec.Form != apd.Finite
	if nonfinite {
		buf.WriteByte('"')
	}
	buf.WriteString(dec.String())
	if nonfinite {
		buf.WriteByte('"')
	}
}

func (j jsonString) Format(buf *bytes.Buffer) {
	encodeJSONString(buf, string(j))
}

func asString(j JSON) string {
	var buf bytes.Buffer
	j.Format(&buf)
	return buf.String()
}

func (j jsonNull) String() string   { return asString(j) }
func (j jsonTrue) String() string   { return asString(j) }
func (j jsonFalse) String() string  { return asString(j) }
func (j jsonString) String() string { return asString(j) }
func (j jsonNumber) String() string { return asString(j) }
func (j jsonArray) String() string  { return asString(j) }
func (j jsonObject) String() string { return asString(j) }

const hexAlphabet = "0123456789abcdef"

// encodeJSONString writes a string literal to buf as a JSON string.
// Cribbed from https://github.com/golang/go/blob/7badae85f20f1bce4cc344f9202447618d45d414/src/encoding/json/encode.go.
func encodeJSONString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if safeSet[b] {
				i++
				continue
			}
			if start < i {
				buf.WriteString(s[start:i])
			}
			switch b {
			case '\\', '"':
				buf.WriteByte('\\')
				buf.WriteByte(b)
			case '\n':
				buf.WriteByte('\\')
				buf.WriteByte('n')
			case '\r':
				buf.WriteByte('\\')
				buf.WriteByte('r')
			case '\t':
				buf.WriteByte('\\')
				buf.WriteByte('t')
			default:
				// This encodes bytes < 0x20 except for \t, \n and \r.
				// If escapeHTML is set, it also escapes <, >, and &
				// because they can lead to security holes when
				// user-controlled strings are rendered into JSON
				// and served to some browsers.
				buf.WriteString(`\u00`)
				buf.WriteByte(hexAlphabet[b>>4])
				buf.WriteByte(hexAlphabet[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				buf.WriteString(s[start:i])
			}
			buf.WriteString(`\ufffd`)
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(s) {
		buf.WriteString(s[start:])
	}
	buf.WriteByte('"')
}

func (j jsonArray) Format(buf *bytes.Buffer) {
	buf.WriteByte('[')
	for i := range j {
		if i != 0 {
			buf.WriteString(", ")
		}
		j[i].Format(buf)
	}
	buf.WriteByte(']')
}

func (j jsonObject) Format(buf *bytes.Buffer) {
	buf.WriteByte('{')
	for i := range j {
		if i != 0 {
			buf.WriteString(", ")
		}
		encodeJSONString(buf, string(j[i].k))
		buf.WriteString(": ")
		j[i].v.Format(buf)
	}
	buf.WriteByte('}')
}

func (jsonNull) Size() uintptr { return 0 }

func (jsonFalse) Size() uintptr { return 0 }

func (jsonTrue) Size() uintptr { return 0 }

func (j jsonNumber) Size() uintptr {
	intVal := j.Coeff
	return decimalSize + uintptr(cap(intVal.Bits()))*wordSize
}

func (j jsonString) Size() uintptr {
	return stringHeaderSize + uintptr(len(j))
}

func (j jsonArray) Size() uintptr {
	valSize := sliceHeaderSize + uintptr(cap(j))*jsonInterfaceSize
	for _, elem := range j {
		valSize += elem.Size()
	}
	return valSize
}

func (j jsonObject) Size() uintptr {
	valSize := sliceHeaderSize + uintptr(cap(j))*keyValuePairSize
	// jsonKeyValuePair consists of jsonString(i.e. string header) k and JSON interface v.
	// Since elem.k.Size() has already taken stringHeaderSize into account, we should
	// reduce len(j) * stringHeaderSize to avoid counting the size of string headers twice
	valSize -= uintptr(len(j)) * stringHeaderSize
	for _, elem := range j {
		valSize += elem.k.Size()
		valSize += elem.v.Size()
	}
	return valSize
}

// ParseJSON takes a string of JSON and returns a JSON value.
func ParseJSON(s string) (JSON, error) {
	// This goes in two phases - first it parses the string into raw interface{}s
	// using the Go encoding/json package, then it transforms that into a JSON.
	// This could be faster if we wrote a parser to go directly into the JSON.
	var result interface{}
	decoder := json.NewDecoder(strings.NewReader(s))
	// We want arbitrary size/precision decimals, so we call UseNumber() to tell
	// the decoder to decode numbers into strings instead of float64s (which we
	// later parse using apd).
	decoder.UseNumber()
	err := decoder.Decode(&result)
	if err != nil {
		err = errors.Handled(err)
		err = errors.Wrap(err, "unable to decode JSON")
		err = pgerror.WithCandidateCode(err, pgcode.InvalidTextRepresentation)
		return nil, err
	}
	if decoder.More() {
		return nil, errTrailingCharacters
	}
	return MakeJSON(result)
}

// EncodeInvertedIndexKeys takes in a key prefix and returns a slice of inverted index keys,
// one per unique path through the receiver.
func EncodeInvertedIndexKeys(b []byte, json JSON) ([][]byte, error) {
	return json.encodeInvertedIndexKeys(encoding.EncodeJSONAscending(b))
}

// EncodeContainingInvertedIndexSpans takes in a key prefix and returns the
// spans that must be scanned in the inverted index to evaluate a contains (@>)
// predicate with the given JSON (i.e., find the objects in the index that
// contain the given JSON).
//
// The spans are returned in an inverted.SpanExpression, which represents the
// set operations that must be applied on the spans read during execution. See
// comments in the SpanExpression definition for details.
//
// The input inKey is prefixed to the keys in all returned spans.
func EncodeContainingInvertedIndexSpans(
	b []byte, json JSON,
) (invertedExpr inverted.Expression, err error) {
	return json.encodeContainingInvertedIndexSpans(
		encoding.EncodeJSONAscending(b), true /* isRoot */, false, /* isObjectValue */
	)
}

// EncodeContainedInvertedIndexSpans takes in a key prefix and returns the
// spans that must be scanned in the inverted index to evaluate a contained by
// (<@) predicate with the given JSON (i.e., find the objects in the index that
// could be contained by the given JSON).
//
// The spans are returned in an inverted.SpanExpression, which represents the
// set operations that must be applied on the spans read during execution. See
// comments in the SpanExpression definition for details.
//
// The input inKey is prefixed to the keys in all returned spans.
func EncodeContainedInvertedIndexSpans(
	b []byte, json JSON,
) (invertedExpr inverted.Expression, err error) {
	invertedExpr, err = json.encodeContainedInvertedIndexSpans(
		encoding.EncodeJSONAscending(b), true /* isRoot */, false, /* isObjectValue */
	)
	if err != nil {
		return nil, err
	}
	// The produced inverted expression will never be tight. This is because the
	// span expression produced will match all objects that contain at least one
	// of the keys, which does not guarantee they only contain the keys.
	// In other words, there may be false positives included that will need to
	// pass through an additional filter.
	// For example, the spans produced for '{"a": "b"}' will include both
	// '{"a": "b"}' and '{"a": "b", "c", "d"}', but the second row should be
	// filtered out since '{"a": "b", "c", "d"}' <@ '{"a": "b"}' is false.
	invertedExpr.SetNotTight()
	return invertedExpr, nil
}

func (j jsonNull) encodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	b = encoding.AddJSONPathTerminator(b)
	return [][]byte{encoding.EncodeNullAscending(b)}, nil
}

func (j jsonNull) encodeContainingInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (inverted.Expression, error) {
	return encodeContainingInvertedIndexSpansFromLeaf(j, b, isRoot, isObjectValue)
}

func (j jsonNull) encodeContainedInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (inverted.Expression, error) {
	invertedExpr, err := encodeContainedInvertedIndexSpansFromLeaf(j, b, isRoot)
	return invertedExpr, err
}

func (jsonTrue) encodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	b = encoding.AddJSONPathTerminator(b)
	return [][]byte{encoding.EncodeTrueAscending(b)}, nil
}

func (j jsonTrue) encodeContainingInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (inverted.Expression, error) {
	return encodeContainingInvertedIndexSpansFromLeaf(j, b, isRoot, isObjectValue)
}

func (j jsonTrue) encodeContainedInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (inverted.Expression, error) {
	invertedExpr, err := encodeContainedInvertedIndexSpansFromLeaf(j, b, isRoot)
	return invertedExpr, err
}

func (jsonFalse) encodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	b = encoding.AddJSONPathTerminator(b)
	return [][]byte{encoding.EncodeFalseAscending(b)}, nil
}

func (j jsonFalse) encodeContainingInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (inverted.Expression, error) {
	return encodeContainingInvertedIndexSpansFromLeaf(j, b, isRoot, isObjectValue)
}

func (j jsonFalse) encodeContainedInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (inverted.Expression, error) {
	invertedExpr, err := encodeContainedInvertedIndexSpansFromLeaf(j, b, isRoot)
	return invertedExpr, err
}

func (j jsonString) encodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	b = encoding.AddJSONPathTerminator(b)
	return [][]byte{encoding.EncodeStringAscending(b, string(j))}, nil
}

func (j jsonString) encodeContainingInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (inverted.Expression, error) {
	return encodeContainingInvertedIndexSpansFromLeaf(j, b, isRoot, isObjectValue)
}

func (j jsonString) encodeContainedInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (inverted.Expression, error) {
	invertedExpr, err := encodeContainedInvertedIndexSpansFromLeaf(j, b, isRoot)
	return invertedExpr, err
}

func (j jsonNumber) encodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	b = encoding.AddJSONPathTerminator(b)
	var dec = apd.Decimal(j)
	return [][]byte{encoding.EncodeDecimalAscending(b, &dec)}, nil
}

func (j jsonNumber) encodeContainingInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (inverted.Expression, error) {
	return encodeContainingInvertedIndexSpansFromLeaf(j, b, isRoot, isObjectValue)
}

func (j jsonNumber) encodeContainedInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (inverted.Expression, error) {
	invertedExpr, err := encodeContainedInvertedIndexSpansFromLeaf(j, b, isRoot)
	return invertedExpr, err
}

func (j jsonArray) encodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	// Checking for an empty array.
	if len(j) == 0 {
		return [][]byte{encoding.EncodeJSONEmptyArray(b)}, nil
	}

	prefix := encoding.EncodeArrayAscending(b)
	var outKeys [][]byte
	for i := range j {
		children, err := j[i].encodeInvertedIndexKeys(prefix[:len(prefix):len(prefix)])
		if err != nil {
			return nil, err
		}
		outKeys = append(outKeys, children...)
	}

	// Deduplicate the entries, since arrays can have duplicates - we don't want
	// to emit duplicate keys from this method, as it's more expensive to
	// deduplicate keys via KV (which will actually write the keys) than to do
	// it now (just an in-memory sort and distinct).
	outKeys = uniq.UniquifyByteSlices(outKeys)
	return outKeys, nil
}

func (j jsonArray) encodeContainingInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (invertedExpr inverted.Expression, err error) {
	// Checking for an empty array.
	if len(j) == 0 {
		return encodeContainingInvertedIndexSpansFromLeaf(j, b, isRoot, isObjectValue)
	}

	prefix := encoding.EncodeArrayAscending(b)
	for i := range j {
		child, err := j[i].encodeContainingInvertedIndexSpans(
			prefix[:len(prefix):len(prefix)], false /* isRoot */, false, /* isObjectValue */
		)
		if err != nil {
			return nil, err
		}

		if invertedExpr == nil {
			invertedExpr = child
		} else {
			invertedExpr = inverted.And(invertedExpr, child)
		}
	}

	// If this array is not at the root and has more than one element,
	// we cannot produce tight spans. This is because we cannot rely on the keys
	// alone to determine whether the top level JSON is contained in another JSON.
	// For example, '[[1], [2]]' and '[[1, 2]]' have exactly the same keys, but
	// '[[1, 2]]' @> '[[1], [2]]' is true, while '[[1], [2]]' @> '[[1, 2]]' is
	// false. We will return an expression with Tight=false for the second case,
	// which will signal the need to filter out false positives.
	//
	// Note that in addition to checking that the original array had length > 1,
	// we also check that the spanExpr is an intersection. The inverted.And
	// function performs some deduplication, so it's possible that the original
	// array had duplicates that were removed, causing the intersection to be
	// removed.
	if spanExpr, ok := invertedExpr.(*inverted.SpanExpression); ok &&
		!isRoot && j.Len() > 1 && spanExpr.Operator == inverted.SetIntersection {
		invertedExpr.SetNotTight()
	}

	return invertedExpr, nil
}

func (j jsonArray) encodeContainedInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (invertedExpr inverted.Expression, err error) {
	if !isObjectValue || len(j) == 0 {
		// The empty array should always be added to the spans, since it is contained
		// by everything. Empty array values are already accounted for when getting
		// the spans for a non-empty object value, so they should be excluded.
		emptyArrSpanExpr := inverted.ExprForSpan(
			inverted.MakeSingleValSpan(encoding.EncodeJSONEmptyArray(b[:len(b):len(b)])), false, /* tight */
		)
		emptyArrSpanExpr.Unique = true
		invertedExpr = emptyArrSpanExpr
	}

	// If the given jsonArray is empty, we return the SpanExpression.
	if len(j) == 0 {
		return invertedExpr, nil
	}

	prefix := encoding.EncodeArrayAscending(b[:len(b):len(b)])
	for i := range j {
		childWithPrefix, err := j[i].encodeContainedInvertedIndexSpans(
			prefix[:len(prefix):len(prefix)], false /* isRoot */, false, /* isObjectValue */
		)
		if err != nil {
			return nil, err
		}
		if invertedExpr == nil {
			invertedExpr = childWithPrefix
		} else {
			invertedExpr = inverted.Or(invertedExpr, childWithPrefix)
		}

		// Scalars inside the array should also be included in the spans
		// without the array prefix, since they are contained by the array. This
		// only applies to arrays not nested inside other arrays or objects, so
		// isRoot must be true.
		// For example, '1' <@ '[1]' is true, but '1' <@ '[[1]]' is false.
		// However for non-scalar types, the structure of nesting must match.
		// For example, '{"a": "a"}' <@ '[{"a": "a"}, {"b": "c"}]' is false, and
		// '[{"a": "a"}]' <@ '[{"a": "a"}, {"b": "c"}]' is true.
		// Therefore, we only include childWithoutPrefix for non-nested primitives
		// and empty arrays/objects.
		if isRoot && isEnd(j[i]) {
			childWithoutPrefix, err := j[i].encodeContainedInvertedIndexSpans(
				b[:len(b):len(b)], false /* isRoot */, false, /* isObjectValue */
			)
			if err != nil {
				return nil, err
			}
			invertedExpr = inverted.Or(invertedExpr, childWithoutPrefix)
		}
	}

	return invertedExpr, nil
}

func (j jsonObject) encodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	// Checking for an empty object.
	if len(j) == 0 {
		return [][]byte{encoding.EncodeJSONEmptyObject(b)}, nil
	}

	var outKeys [][]byte
	for i := range j {
		children, err := j[i].v.encodeInvertedIndexKeys(nil)
		if err != nil {
			return nil, err
		}

		// We're trying to see if this is the end of the JSON path. If it is, then we don't want to
		// add an extra separator.
		end := isEnd(j[i].v)

		for _, childBytes := range children {
			encodedKey := bytes.Join([][]byte{b,
				encoding.EncodeJSONKeyStringAscending(nil, string(j[i].k), end),
				childBytes}, nil)

			outKeys = append(outKeys, encodedKey)
		}
	}
	return outKeys, nil
}

func (j jsonObject) encodeContainingInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (invertedExpr inverted.Expression, err error) {
	if len(j) == 0 {
		return encodeContainingInvertedIndexSpansFromLeaf(j, b, isRoot, isObjectValue)
	}

	for i := range j {
		// We're trying to see if this is the end of the JSON path. If it is, then
		// we don't want to add an extra separator.
		end := isEnd(j[i].v)

		prefix := encoding.EncodeJSONKeyStringAscending(b[:len(b):len(b)], string(j[i].k), end)
		child, err := j[i].v.encodeContainingInvertedIndexSpans(
			prefix, false /* isRoot */, true, /* isObjectValue */
		)
		if err != nil {
			return nil, err
		}

		if invertedExpr == nil {
			invertedExpr = child
		} else {
			invertedExpr = inverted.And(invertedExpr, child)
		}
	}

	// If this object is not at the root and has more than one element,
	// we cannot produce tight spans. This is because we cannot rely on the keys
	// alone to determine whether the top level JSON is contained in another JSON.
	// For example, '[{"a": "b"}, {"c": "d"}]' and '[{"a": "b", "c": "d"}]'
	// have exactly the same keys, but
	// '[{"a": "b", "c": "d"}]' @> '[{"a": "b"}, {"c": "d"}]' is true, while
	// '[{"a": "b"}, {"c": "d"}]' @> '[{"a": "b", "c": "d"}]' is false.
	// We will return an expression with Tight=false for the second case, which
	// will signal the need to filter out false positives.
	if !isRoot && j.Len() > 1 {
		invertedExpr.SetNotTight()
	}

	return invertedExpr, nil
}

func (j jsonObject) encodeContainedInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (invertedExpr inverted.Expression, err error) {
	// The empty object should always be added to the spans, since it is contained
	// by everything. Empty object values are already accounted for when getting
	// the spans for a non-empty object value, so they should be excluded.
	if !isObjectValue || len(j) == 0 {
		emptyObjSpanExpr := inverted.ExprForSpan(
			inverted.MakeSingleValSpan(encoding.EncodeJSONEmptyObject(b[:len(b):len(b)])), false, /* tight */
		)
		emptyObjSpanExpr.Unique = true
		invertedExpr = emptyObjSpanExpr
	}
	// If the given jsonObject is empty, we return the SpanExpression.
	if len(j) == 0 {
		return invertedExpr, nil
	}

	for i := range j {
		// We're trying to see if this is the end of the JSON path. If it is, then
		// we don't want to add an extra separator.
		end := isEnd(j[i].v)

		prefix := encoding.EncodeJSONKeyStringAscending(b[:len(b):len(b)], string(j[i].k), end)
		child, err := j[i].v.encodeContainedInvertedIndexSpans(
			prefix, false /* isRoot */, true, /* isObjectValue */
		)
		if err != nil {
			return nil, err
		}

		if invertedExpr == nil {
			invertedExpr = child
		} else {
			invertedExpr = inverted.Or(invertedExpr, child)
		}

		// When we have a nested object or array, we want to include the empty
		// object or array span with the prefix. For example, '{"a": {"b": "c"}}'
		// should include the span for '{"a": {}}', and '{"a": [1]}' should include
		// '{"a": []}'.
		if !end {
			v := emptyJSONForType(j[i].v)
			if v != nil {
				prefixWithEnd := encoding.EncodeJSONKeyStringAscending(b[:len(b):len(b)], string(j[i].k), true)
				childWithEnd, err := v.encodeContainedInvertedIndexSpans(
					prefixWithEnd, false /* isRoot */, true, /* isObjectValue */
				)
				if err != nil {
					return nil, err
				}
				invertedExpr = inverted.Or(invertedExpr, childWithEnd)
			}
		}
	}

	return invertedExpr, nil
}

// isEnd returns true if a JSON value is the end of the JSON path.
// If it is, then we don't want to add an extra separator when encoding
// the keys.
func isEnd(json JSON) bool {
	end := true
	switch t := json.(type) {
	case jsonArray, jsonObject:
		if t.Len() != 0 {
			end = false
		}

	case *jsonEncoded:
		switch t.typ {
		case ArrayJSONType, ObjectJSONType:
			if t.containerLen != 0 {
				end = false
			}
		}
	}
	return end
}

// emptyJSONForType returns either an empty JSON array or object corresponding
// to the input JSON type. If the provided JSON is not an object or array, it
// returns nil.
func emptyJSONForType(json JSON) JSON {
	switch t := json.(type) {
	case jsonArray:
		return emptyJSONArray

	case jsonObject:
		return emptyJSONObject

	case *jsonEncoded:
		switch t.typ {
		case ArrayJSONType:
			return emptyJSONArray

		case ObjectJSONType:
			return emptyJSONObject
		}
	}
	return nil
}

// encodeContainingInvertedIndexSpansFromLeaf encodes the spans that must be
// scanned in an inverted index to find the JSON objects that contain the given
// leaf JSON value. A leaf is any scalar json such as '1', 'true', or 'null',
// or an empty object or array.
//
// If isRoot is true, this function is being called at the root level of the
// JSON hierarchy. If isObjectValue is true, the given JSON is the value of a
// JSON object key. Note that isRoot and isObjectValue cannot both be true at
// the same time.
func encodeContainingInvertedIndexSpansFromLeaf(
	j JSON, b []byte, isRoot, isObjectValue bool,
) (invertedExpr inverted.Expression, err error) {
	keys, err := j.encodeInvertedIndexKeys(b)
	if err != nil {
		return nil, err
	}

	invertedExpr = &inverted.SpanExpression{Tight: true}
	var unique bool
	prefix := b[:len(b):len(b)]
	if isObjectValue {
		if isRoot {
			return nil, errors.AssertionFailedf(
				"isObjectValue and isRoot should not both be true",
			)
		}
		// Since isObjectValue is true, we know that at this point prefix contains
		// the encoded JSON object key. It does not yet contain the
		// escape + escapedJSONObjectKeyTerm separator, which is used to separate
		// the key from the value when the value is a non-empty object or array.
		// In order for '{}' and '[]' to match non-empty objects and arrays in the
		// switch that follows, we must add that separator.
		//
		// EncodeJSONKeyStringAscending(..., false) adds the separator, and
		// since the key is already encoded in prefix, we pass the empty string as
		// the key.
		prefix = encoding.EncodeJSONKeyStringAscending(prefix, "", false /* end */)
	}

	switch t := j.(type) {
	case jsonArray:
		if t.Len() != 0 {
			return nil, errors.AssertionFailedf(
				"encodeContainingInvertedIndexSpansFromLeaf called on a non-empty jsonArray",
			)
		}

		// At this point, `keys` contains the empty array, which ensures that
		// '{"a": []}' matches '{"a": []}' and '[]' matches '[]'. This is correct
		// because a JSON object or array always contains itself.

		// Add a key to cover all non-empty arrays. It is needed for JSON arrays
		// such as '[]' to match '[1]' and '{"a": []}' to match '{"a": [1]}'
		// (i.e., '[1]' @> '[]' and '{"a": [1]}' @> '{"a": []}' are true).
		// EncodeArrayAscending generates the prefix that is used for all non-empty
		// arrays.
		//
		// The span that will be generated for this key can have duplicate PKs, so
		// unique=false.
		keys = append(keys, encoding.EncodeArrayAscending(prefix))

	case jsonObject:
		if t.Len() != 0 {
			return nil, errors.AssertionFailedf(
				"encodeContainingInvertedIndexSpansFromLeaf called on a non-empty jsonObject",
			)
		}

		// At this point, `keys` contains the empty object, which ensures that
		// '{"a": {}}' matches '{"a": {}}' and '{}' matches '{}'. This is correct
		// because a JSON object always contains itself. This key will be converted
		// into a span below.

		// Add a span to cover keys for non-empty objects. It is needed for
		// JSON objects such as '{}' to match '{"a": "b"}', but not '[1]',
		// and '{"a": {}}' to match '{"a": {"b": "c"}}', but not '{"a": [1]}' or
		// ["a"]. (i.e., '{"a": "b"}' @> '{}' and '{"a": {"b": "c"}}' @> '{"a": {}}'
		// are true, but '[1]' @> '{}', '{"a": [1]}' @> '{"a": {}}', and
		// '["a"]' @> '{"a": {}}' are false)
		//
		// This span can have duplicate PKs, so unique=false.
		invertedExpr = inverted.Or(invertedExpr, inverted.ExprForSpan(inverted.Span{
			// EncodeJSONObjectSpanStartAscending generates the first possible value
			// for JSON objects.
			Start: inverted.EncVal(encoding.EncodeJSONObjectSpanStartAscending(prefix)),
			// This end key is equal to jsonInvertedIndex + 1.
			End: inverted.EncVal(roachpb.Key(prefix).PrefixEnd()),
		}, true /* tight */))

	default:
		if isRoot {
			// If we find a scalar on the right side of the @> operator it means that
			// we need to find both matching scalars and arrays that contain that value.
			// In order to do this we generate two logical spans, one for the original
			// scalar (which we have already done above) and one for arrays containing
			// the scalar. This is *only* the case if the scalar is the root element.
			// For example, '1' @> '1' and '[1]' @> '1' are both true, but
			// '[1]' @> '[1]' is true, while '[[1]]' @> '[1]' is false.
			arr := NewArrayBuilder(1)
			arr.Add(j)
			jArr := arr.Build()
			arrKeys, err := jArr.encodeInvertedIndexKeys(prefix)
			if err != nil {
				return nil, err
			}
			keys = append(keys, arrKeys...)

			// Even though we now have two spans, we can guarantee no PK duplicates
			// because in JSON something can either be an array or a scalar -- not
			// both. So the spans are guaranteed not to overlap when mapped onto the
			// primary key space. Therefore there won't be any duplicate primary keys
			// when we retrieve rows for both sets, and setting unique to true
			// below is valid.
		}

		// We can guarantee that there will be no duplicate primary keys produced
		// for paths ending in a scalar, regardless of whether or not this is the
		// root.
		unique = true
	}

	for _, key := range keys {
		invertedExpr = inverted.Or(invertedExpr, inverted.ExprForSpan(
			inverted.MakeSingleValSpan(inverted.EncVal(key)), true, /* tight */
		))
	}
	if spanExpr, ok := invertedExpr.(*inverted.SpanExpression); ok {
		spanExpr.Unique = unique
	}

	return invertedExpr, nil
}

// encodeContainedInvertedIndexSpansFromLeaf encodes the spans that must be
// scanned in an inverted index to find the JSON objects that are contained by
// the given leaf JSON value. A leaf is any scalar json such as '1', 'true', or
// 'null'. The resulting span should only include the key for the leaf.
// Unlike encodeContainingInvertedIndexSpansFromLeaf, empty JSON objects and
// array spans are not encoded by this function.
//
// If isRoot is true, this function is being called at the root level of the
// JSON hierarchy.
func encodeContainedInvertedIndexSpansFromLeaf(
	j JSON, b []byte, isRoot bool,
) (invertedExpr inverted.Expression, err error) {
	keys, err := j.encodeInvertedIndexKeys(b)
	if err != nil {
		return nil, err
	}

	invertedExpr = &inverted.SpanExpression{Tight: false}
	var unique bool
	switch j.(type) {
	case jsonArray:
		return nil, errors.AssertionFailedf(
			"encodeContainedInvertedIndexSpansFromLeaf called on a jsonArray",
		)

	case jsonObject:
		return nil, errors.AssertionFailedf(
			"encodeContainedInvertedIndexSpansFromLeaf called on a jsonObject",
		)

	default:
		// The leaf is a scalar, so we only want the single span that contains the
		// key for it, because the scalar can only contain itself.
		if isRoot {
			// We can guarantee that there will not be duplicate primary keys produced
			// for paths ending in a scalar if it is the root.
			unique = true
		}
		for _, key := range keys {
			invertedExpr = inverted.Or(invertedExpr, inverted.ExprForSpan(
				inverted.MakeSingleValSpan(key), false, /* tight */
			))
		}
		if spanExpr, ok := invertedExpr.(*inverted.SpanExpression); ok {
			spanExpr.Unique = unique
		}

		return invertedExpr, nil
	}
}

// NumInvertedIndexEntries returns the number of inverted index entries that
// would be created for the given JSON value. Since identical elements of an
// array are encoded identically in the inverted index, the total number of
// distinct index entries may be less than the total number of paths.
func NumInvertedIndexEntries(j JSON) (int, error) {
	return j.numInvertedIndexEntries()
}

func (j jsonNull) numInvertedIndexEntries() (int, error) {
	return 1, nil
}
func (jsonTrue) numInvertedIndexEntries() (int, error) {
	return 1, nil
}
func (jsonFalse) numInvertedIndexEntries() (int, error) {
	return 1, nil
}
func (j jsonString) numInvertedIndexEntries() (int, error) {
	return 1, nil
}
func (j jsonNumber) numInvertedIndexEntries() (int, error) {
	return 1, nil
}
func (j jsonArray) numInvertedIndexEntries() (int, error) {
	if len(j) == 0 {
		return 1, nil
	}
	keys, err := j.encodeInvertedIndexKeys(nil)
	if err != nil {
		return 0, err
	}
	return len(keys), nil
}

func (j jsonObject) numInvertedIndexEntries() (int, error) {
	if len(j) == 0 {
		return 1, nil
	}
	count := 0
	for _, kv := range j {
		n, err := kv.v.numInvertedIndexEntries()
		if err != nil {
			return 0, err
		}
		count += n
	}
	return count, nil
}

// AllPaths returns a slice of new JSON documents, each a path to a leaf
// through the input. Note that leaves include the empty object and array
// in addition to scalars.
func AllPaths(j JSON) ([]JSON, error) {
	return j.allPaths()
}

func (j jsonNull) allPaths() ([]JSON, error) {
	return []JSON{j}, nil
}

func (j jsonTrue) allPaths() ([]JSON, error) {
	return []JSON{j}, nil
}

func (j jsonFalse) allPaths() ([]JSON, error) {
	return []JSON{j}, nil
}

func (j jsonString) allPaths() ([]JSON, error) {
	return []JSON{j}, nil
}

func (j jsonNumber) allPaths() ([]JSON, error) {
	return []JSON{j}, nil
}

func (j jsonArray) allPaths() ([]JSON, error) {
	if len(j) == 0 {
		return []JSON{j}, nil
	}
	ret := make([]JSON, 0, len(j))
	for i := range j {
		paths, err := j[i].allPaths()
		if err != nil {
			return nil, err
		}
		for _, path := range paths {
			ret = append(ret, jsonArray{path})
		}
	}
	return ret, nil
}

func (j jsonObject) allPaths() ([]JSON, error) {
	if len(j) == 0 {
		return []JSON{j}, nil
	}
	ret := make([]JSON, 0, len(j))
	for i := range j {
		paths, err := j[i].v.allPaths()
		if err != nil {
			return nil, err
		}
		for _, path := range paths {
			ret = append(ret, jsonObject{jsonKeyValuePair{k: j[i].k, v: path}})
		}
	}
	return ret, nil
}

// FromSpatialObject transforms a SpatialObject into the json.JSON type.
func FromSpatialObject(so geopb.SpatialObject, numDecimalDigits int) (JSON, error) {
	j, err := geo.SpatialObjectToGeoJSON(so, numDecimalDigits, geo.SpatialObjectToGeoJSONFlagZero)
	if err != nil {
		return nil, err
	}
	return ParseJSON(string(j))
}

// FromDecimal returns a JSON value given a apd.Decimal.
func FromDecimal(v apd.Decimal) JSON {
	return jsonNumber(v)
}

// FromNumber returns a JSON value given a json.Number.
func FromNumber(v json.Number) (JSON, error) {
	// The JSON decoder has already verified that the string `v` represents a
	// valid JSON number, and the set of valid JSON numbers is a [proper] subset
	// of the set of valid apd.Decimal values.
	dec := apd.Decimal{}
	_, _, err := dec.SetString(string(v))
	return jsonNumber(dec), err
}

// FromString returns a JSON value given a string.
func FromString(v string) JSON {
	return jsonString(v)
}

// FromBool returns a JSON value given a bool.
func FromBool(v bool) JSON {
	if v {
		return TrueJSONValue
	}
	return FalseJSONValue
}

func fromArray(v []interface{}) (JSON, error) {
	elems := make([]JSON, len(v))
	for i := range v {
		var err error
		elems[i], err = MakeJSON(v[i])
		if err != nil {
			return nil, err
		}
	}
	return jsonArray(elems), nil
}

func fromMap(v map[string]interface{}) (JSON, error) {
	keys := make([]string, len(v))
	i := 0
	for k := range v {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	result := make([]jsonKeyValuePair, len(v))
	for i := range keys {
		v, err := MakeJSON(v[keys[i]])
		if err != nil {
			return nil, err
		}
		result[i] = jsonKeyValuePair{
			k: jsonString(keys[i]),
			v: v,
		}
	}
	return jsonObject(result), nil
}

// FromInt returns a JSON value given a int.
func FromInt(v int) JSON {
	dec := apd.Decimal{}
	dec.SetInt64(int64(v))
	return jsonNumber(dec)
}

// FromInt64 returns a JSON value given a int64.
func FromInt64(v int64) JSON {
	dec := apd.Decimal{}
	dec.SetInt64(v)
	return jsonNumber(dec)
}

// FromFloat64 returns a JSON value given a float64.
func FromFloat64(v float64) (JSON, error) {
	dec := apd.Decimal{}
	_, err := dec.SetFloat64(v)
	if err != nil {
		return nil, err
	}
	return jsonNumber(dec), nil
}

// MakeJSON returns a JSON value given a Go-style representation of JSON.
// * JSON null is Go `nil`,
// * JSON true is Go `true`,
// * JSON false is Go `false`,
// * JSON numbers are json.Number | int | int64 | float64,
// * JSON string is a Go string,
// * JSON array is a Go []interface{},
// * JSON object is a Go map[string]interface{}.
func MakeJSON(d interface{}) (JSON, error) {
	switch v := d.(type) {
	case json.Number:
		return FromNumber(v)
	case string:
		return FromString(v), nil
	case bool:
		return FromBool(v), nil
	case nil:
		return NullJSONValue, nil
	case []interface{}:
		return fromArray(v)
	case map[string]interface{}:
		return fromMap(v)
		// The below are not used by ParseJSON, but are provided for ease-of-use when
		// constructing Datums.
	case int:
		return FromInt(v), nil
	case int64:
		return FromInt64(v), nil
	case float64:
		return FromFloat64(v)
	case JSON:
		// If we get passed a JSON, just accept it. This is useful in cases like the
		// random JSON generator.
		return v, nil
	}
	return nil, errors.AssertionFailedf("unknown value type passed to MakeJSON: %T", d)
}

// This value was determined through some rough experimental results as a good
// place to start doing binary search over a linear scan.
const bsearchCutoff = 20

func findPairIndexByKey(j jsonObject, key string) (int, bool) {
	// For small objects, the overhead of binary search is significant and so
	// it's faster to just do a linear scan.
	var i int
	if len(j) < bsearchCutoff {
		for i = range j {
			if string(j[i].k) >= key {
				break
			}
		}
	} else {
		i = sort.Search(len(j), func(i int) bool { return string(j[i].k) >= key })
	}
	if i < len(j) && string(j[i].k) == key {
		return i, true
	}
	return -1, false
}

func (j jsonObject) FetchValKey(key string) (JSON, error) {
	i, ok := findPairIndexByKey(j, key)
	if ok {
		return j[i].v, nil
	}
	return nil, nil
}

func (jsonNull) FetchValKey(string) (JSON, error)   { return nil, nil }
func (jsonTrue) FetchValKey(string) (JSON, error)   { return nil, nil }
func (jsonFalse) FetchValKey(string) (JSON, error)  { return nil, nil }
func (jsonString) FetchValKey(string) (JSON, error) { return nil, nil }
func (jsonNumber) FetchValKey(string) (JSON, error) { return nil, nil }
func (jsonArray) FetchValKey(string) (JSON, error)  { return nil, nil }

func fetchValIdxForScalar(j JSON, idx int) JSON {
	// The 0'th element (and -1'st element with negative indexing) of a scalar
	// JSON value is the scalar itself. This effectively treats scalar values as
	// single element arrays.
	if idx == 0 || idx == -1 {
		return j
	}
	return nil
}

func (j jsonNull) FetchValIdx(idx int) (JSON, error)   { return fetchValIdxForScalar(j, idx), nil }
func (j jsonTrue) FetchValIdx(idx int) (JSON, error)   { return fetchValIdxForScalar(j, idx), nil }
func (j jsonFalse) FetchValIdx(idx int) (JSON, error)  { return fetchValIdxForScalar(j, idx), nil }
func (j jsonString) FetchValIdx(idx int) (JSON, error) { return fetchValIdxForScalar(j, idx), nil }
func (j jsonNumber) FetchValIdx(idx int) (JSON, error) { return fetchValIdxForScalar(j, idx), nil }

func (j jsonArray) FetchValIdx(idx int) (JSON, error) {
	if idx < 0 {
		idx = len(j) + idx
	}
	if idx >= 0 && idx < len(j) {
		return j[idx], nil
	}
	return nil, nil
}

func (jsonObject) FetchValIdx(int) (JSON, error) { return nil, nil }

// FetchPath implements the #> operator.
func FetchPath(j JSON, path []string) (JSON, error) {
	var next JSON
	var err error
	for _, v := range path {
		next, err = j.FetchValKeyOrIdx(v)
		if next == nil {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		j = next
	}
	return j, nil
}

var errCannotSetPathInScalar = pgerror.WithCandidateCode(errors.New("cannot set path in scalar"), pgcode.InvalidParameterValue)

// setValKeyOrIdx sets a key or index within a JSON object or array. If the
// provided value is neither an object or array the value is returned
// unchanged.
// If the value is an object which does not have the provided key and
// createMissing is true, the key is inserted into the object with the value `to`.
// If the value is an array and the provided index is negative, it counts from the back of the array.
// Further, if the value is an array, and createMissing is true:
// * if the provided index points to before the start of the array, `to` is prepended to the array.
// * if the provided index points to after the end of the array, `to` is appended to the array.
func setValKeyOrIdx(j JSON, key string, to JSON, createMissing bool) (JSON, error) {
	switch v := j.(type) {
	case *jsonEncoded:
		n, err := v.shallowDecode()
		if err != nil {
			return nil, err
		}
		return setValKeyOrIdx(n, key, to, createMissing)
	case jsonObject:
		return v.SetKey(key, to, createMissing)
	case jsonArray:
		idx, err := strconv.Atoi(key)
		if err != nil {
			return nil, err
		}
		if idx < 0 {
			idx = len(v) + idx
		}
		if !createMissing && (idx < 0 || idx >= len(v)) {
			return v, nil
		}
		var result jsonArray
		if idx < 0 {
			result = make(jsonArray, len(v)+1)
			copy(result[1:], v)
			result[0] = to
		} else if idx >= len(v) {
			result = make(jsonArray, len(v)+1)
			copy(result, v)
			result[len(result)-1] = to
		} else {
			result = make(jsonArray, len(v))
			copy(result, v)
			result[idx] = to
		}
		return result, nil
	}
	return j, nil
}

// DeepSet sets a path to a value in a JSON document.
// Largely follows the same semantics as setValKeyOrIdx, but with a path.
// Implements the jsonb_set builtin.
func DeepSet(j JSON, path []string, to JSON, createMissing bool) (JSON, error) {
	if j.isScalar() {
		return nil, errCannotSetPathInScalar
	}
	return deepSet(j, path, to, createMissing)
}

func deepSet(j JSON, path []string, to JSON, createMissing bool) (JSON, error) {
	switch len(path) {
	case 0:
		return j, nil
	case 1:
		return setValKeyOrIdx(j, path[0], to, createMissing)
	default:
		switch v := j.(type) {
		case *jsonEncoded:
			n, err := v.shallowDecode()
			if err != nil {
				return nil, err
			}
			return deepSet(n, path, to, createMissing)
		default:
			fetched, err := j.FetchValKeyOrIdx(path[0])
			if err != nil {
				return nil, err
			}
			if fetched == nil {
				return j, nil
			}
			sub, err := deepSet(fetched, path[1:], to, createMissing)
			if err != nil {
				return nil, err
			}
			return setValKeyOrIdx(j, path[0], sub, createMissing)
		}
	}
}

var errCannotReplaceExistingKey = pgerror.WithCandidateCode(errors.New("cannot replace existing key"), pgcode.InvalidParameterValue)

func insertValKeyOrIdx(j JSON, key string, newVal JSON, insertAfter bool) (JSON, error) {
	switch v := j.(type) {
	case *jsonEncoded:
		n, err := v.shallowDecode()
		if err != nil {
			return nil, err
		}
		return insertValKeyOrIdx(n, key, newVal, insertAfter)
	case jsonObject:
		result, err := v.SetKey(key, newVal, true)
		if err != nil {
			return nil, err
		}
		if len(result) == len(v) {
			return nil, errCannotReplaceExistingKey
		}
		return result, nil
	case jsonArray:
		idx, err := strconv.Atoi(key)
		if err != nil {
			return nil, err
		}
		if idx < 0 {
			idx = len(v) + idx
		}
		if insertAfter {
			idx++
		}

		var result = make(jsonArray, len(v)+1)
		if idx <= 0 {
			copy(result[1:], v)
			result[0] = newVal
		} else if idx >= len(v) {
			copy(result, v)
			result[len(result)-1] = newVal
		} else {
			copy(result[:idx], v[:idx])
			copy(result[idx+1:], v[idx:])
			result[idx] = newVal
		}
		return result, nil
	}
	return j, nil
}

// DeepInsert inserts a value at a path in a JSON document.
// Implements the jsonb_insert builtin.
func DeepInsert(j JSON, path []string, to JSON, insertAfter bool) (JSON, error) {
	if j.isScalar() {
		return nil, errCannotSetPathInScalar
	}
	return deepInsert(j, path, to, insertAfter)
}

func deepInsert(j JSON, path []string, to JSON, insertAfter bool) (JSON, error) {
	switch len(path) {
	case 0:
		return j, nil
	case 1:
		return insertValKeyOrIdx(j, path[0], to, insertAfter)
	default:
		switch v := j.(type) {
		case *jsonEncoded:
			n, err := v.shallowDecode()
			if err != nil {
				return nil, err
			}
			return deepInsert(n, path, to, insertAfter)
		default:
			fetched, err := j.FetchValKeyOrIdx(path[0])
			if err != nil {
				return nil, err
			}
			if fetched == nil {
				return j, nil
			}
			sub, err := deepInsert(fetched, path[1:], to, insertAfter)
			if err != nil {
				return nil, err
			}
			return setValKeyOrIdx(j, path[0], sub, true)
		}
	}
}

func (j jsonObject) FetchValKeyOrIdx(key string) (JSON, error) {
	return j.FetchValKey(key)
}

func (j jsonArray) FetchValKeyOrIdx(key string) (JSON, error) {
	idx, err := strconv.Atoi(key)
	if err != nil {
		// We shouldn't return this error because it means we couldn't parse the
		// number, meaning it was a string and that just means we can't find the
		// value in an array.
		return nil, nil //nolint:returnerrcheck
	}
	return j.FetchValIdx(idx)
}

func (jsonNull) FetchValKeyOrIdx(string) (JSON, error)   { return nil, nil }
func (jsonTrue) FetchValKeyOrIdx(string) (JSON, error)   { return nil, nil }
func (jsonFalse) FetchValKeyOrIdx(string) (JSON, error)  { return nil, nil }
func (jsonString) FetchValKeyOrIdx(string) (JSON, error) { return nil, nil }
func (jsonNumber) FetchValKeyOrIdx(string) (JSON, error) { return nil, nil }

var errCannotDeleteFromScalar = pgerror.WithCandidateCode(errors.New("cannot delete from scalar"), pgcode.InvalidParameterValue)
var errCannotDeleteFromObject = pgerror.WithCandidateCode(errors.New("cannot delete from object using integer index"), pgcode.InvalidParameterValue)

func (j jsonObject) SetKey(key string, to JSON, createMissing bool) (jsonObject, error) {
	result := make(jsonObject, 0, len(j)+1)
	curIdx := 0

	for curIdx < len(j) && string(j[curIdx].k) < key {
		result = append(result, j[curIdx])
		curIdx++
	}

	keyAlreadyExists := curIdx < len(j) && string(j[curIdx].k) == key
	if createMissing || keyAlreadyExists {
		result = append(result, jsonKeyValuePair{
			k: jsonString(key),
			v: to,
		})
	}
	if keyAlreadyExists {
		curIdx++
	}

	for curIdx < len(j) {
		result = append(result, j[curIdx])
		curIdx++
	}

	return result, nil
}

func (j jsonArray) RemoveString(s string) (JSON, bool, error) {
	b := NewArrayBuilder(j.Len())
	removed := false
	for _, el := range j {
		// We want to remove only elements of string type.
		if el.Type() == StringJSONType {
			t, err := el.AsText()
			if err != nil {
				return nil, false, err
			}
			if *t != s {
				b.Add(el)
			} else {
				removed = true
			}
		} else {
			b.Add(el)
		}
	}
	if removed {
		return b.Build(), removed, nil
	}
	return j, false, nil
}

func (j jsonObject) RemoveString(s string) (JSON, bool, error) {
	idx, ok := findPairIndexByKey(j, s)
	if !ok {
		return j, false, nil
	}

	newVal := make([]jsonKeyValuePair, len(j)-1)
	for i, elem := range j[:idx] {
		newVal[i] = elem
	}
	for i, elem := range j[idx+1:] {
		newVal[idx+i] = elem
	}
	return jsonObject(newVal), true, nil
}

func (jsonNull) RemoveString(string) (JSON, bool, error) {
	return nil, false, errCannotDeleteFromScalar
}
func (jsonTrue) RemoveString(string) (JSON, bool, error) {
	return nil, false, errCannotDeleteFromScalar
}
func (jsonFalse) RemoveString(string) (JSON, bool, error) {
	return nil, false, errCannotDeleteFromScalar
}
func (jsonString) RemoveString(string) (JSON, bool, error) {
	return nil, false, errCannotDeleteFromScalar
}
func (jsonNumber) RemoveString(string) (JSON, bool, error) {
	return nil, false, errCannotDeleteFromScalar
}

func (j jsonArray) RemoveIndex(idx int) (JSON, bool, error) {
	if idx < 0 {
		idx = len(j) + idx
	}
	if idx < 0 || idx >= len(j) {
		return j, false, nil
	}
	result := make(jsonArray, len(j)-1)
	for i := 0; i < idx; i++ {
		result[i] = j[i]
	}
	for i := idx + 1; i < len(j); i++ {
		result[i-1] = j[i]
	}
	return result, true, nil
}

func (j jsonObject) RemoveIndex(int) (JSON, bool, error) {
	return nil, false, errCannotDeleteFromObject
}

func (jsonNull) RemoveIndex(int) (JSON, bool, error)   { return nil, false, errCannotDeleteFromScalar }
func (jsonTrue) RemoveIndex(int) (JSON, bool, error)   { return nil, false, errCannotDeleteFromScalar }
func (jsonFalse) RemoveIndex(int) (JSON, bool, error)  { return nil, false, errCannotDeleteFromScalar }
func (jsonString) RemoveIndex(int) (JSON, bool, error) { return nil, false, errCannotDeleteFromScalar }
func (jsonNumber) RemoveIndex(int) (JSON, bool, error) { return nil, false, errCannotDeleteFromScalar }

var errInvalidConcat = pgerror.WithCandidateCode(errors.New("invalid concatenation of jsonb objects"), pgcode.InvalidParameterValue)

func scalarConcat(left, other JSON) (JSON, error) {
	switch other.Type() {
	case ArrayJSONType:
		decoded, err := other.tryDecode()
		if err != nil {
			return nil, err
		}
		right := decoded.(jsonArray)
		result := make(jsonArray, len(right)+1)
		result[0] = left
		for i := range right {
			result[i+1] = right[i]
		}
		return result, nil
	case ObjectJSONType:
		return nil, errInvalidConcat
	default:
		return jsonArray{left, other}, nil
	}
}

func (jsonNull) Concat(other JSON) (JSON, error)     { return scalarConcat(NullJSONValue, other) }
func (jsonTrue) Concat(other JSON) (JSON, error)     { return scalarConcat(TrueJSONValue, other) }
func (jsonFalse) Concat(other JSON) (JSON, error)    { return scalarConcat(FalseJSONValue, other) }
func (j jsonString) Concat(other JSON) (JSON, error) { return scalarConcat(j, other) }
func (j jsonNumber) Concat(other JSON) (JSON, error) { return scalarConcat(j, other) }

func (j jsonArray) Concat(other JSON) (JSON, error) {
	left := j
	switch other.Type() {
	case ArrayJSONType:
		decoded, err := other.tryDecode()
		if err != nil {
			return nil, err
		}
		right := decoded.(jsonArray)
		result := make(jsonArray, len(left)+len(right))
		copy(result, left)
		copy(result[len(left):], right)
		return result, nil
	default:
		result := make(jsonArray, len(left)+1)
		copy(result, left)
		result[len(left)] = other
		return result, nil
	}
}

func (j jsonObject) Concat(other JSON) (JSON, error) {
	switch other.Type() {
	case ArrayJSONType:
		return scalarConcat(j, other)
	case ObjectJSONType:
		right := other.MaybeDecode().(jsonObject)
		// Since both objects are sorted, we can do the merge sort thing to
		// "concatenate" them.
		// The capacity here is an overestimate if the two objects share keys.
		result := make(jsonObject, 0, len(j)+len(right))
		rightIdx := 0
		for _, kv := range j {
			for rightIdx < len(right) && right[rightIdx].k < kv.k {
				result = append(result, right[rightIdx])
				rightIdx++
			}
			// If we have any matching keys, the value in the right object takes
			// precedence (this allows || to work as a setter).
			if rightIdx < len(right) && right[rightIdx].k == kv.k {
				result = append(result, right[rightIdx])
				rightIdx++
			} else {
				result = append(result, kv)
			}
		}
		// We've exhausted all of the key-value pairs on the left, so just dump in
		// the remaining ones from the right.
		for i := rightIdx; i < len(right); i++ {
			result = append(result, right[i])
		}
		return result, nil
	default:
		return nil, errInvalidConcat
	}
}

func (j jsonString) AsText() (*string, error) {
	s := string(j)
	return &s, nil
}
func (j jsonNull) AsText() (*string, error) { return nil, nil }
func (j jsonTrue) AsText() (*string, error) {
	s := j.String()
	return &s, nil
}
func (j jsonFalse) AsText() (*string, error) {
	s := j.String()
	return &s, nil
}
func (j jsonNumber) AsText() (*string, error) {
	s := j.String()
	return &s, nil
}
func (j jsonArray) AsText() (*string, error) {
	s := j.String()
	return &s, nil
}
func (j jsonObject) AsText() (*string, error) {
	s := j.String()
	return &s, nil
}

func (jsonNull) Exists(string) (bool, error)   { return false, nil }
func (jsonTrue) Exists(string) (bool, error)   { return false, nil }
func (jsonFalse) Exists(string) (bool, error)  { return false, nil }
func (jsonNumber) Exists(string) (bool, error) { return false, nil }

func (j jsonString) Exists(s string) (bool, error) {
	return string(j) == s, nil
}

func (j jsonArray) Exists(s string) (bool, error) {
	for i := 0; i < len(j); i++ {
		if elem, ok := j[i].(jsonString); ok && string(elem) == s {
			return true, nil
		}
	}
	return false, nil
}
func (j jsonObject) Exists(s string) (bool, error) {
	v, err := j.FetchValKey(s)
	if err != nil {
		return false, err
	}
	return v != nil, nil
}

func (j jsonNull) StripNulls() (JSON, bool, error) {
	return j, false, nil
}
func (j jsonTrue) StripNulls() (JSON, bool, error) {
	return j, false, nil
}
func (j jsonFalse) StripNulls() (JSON, bool, error) {
	return j, false, nil
}
func (j jsonNumber) StripNulls() (JSON, bool, error) {
	return j, false, nil
}
func (j jsonString) StripNulls() (JSON, bool, error) {
	return j, false, nil
}
func (j jsonArray) StripNulls() (JSON, bool, error) {
	for i, e := range j {
		json, needToStrip, err := e.StripNulls()
		if err != nil {
			return nil, false, err
		}
		if needToStrip {
			// Cannot return the original content, need to return the result
			// with new JSON array.
			newArr := make(jsonArray, 0, len(j))
			newArr = append(append(newArr, j[:i]...), json)
			for _, elem := range j[i+1:] {
				if json, _, err = elem.StripNulls(); err != nil {
					return nil, false, err
				}
				newArr = append(newArr, json)
			}
			return newArr, true, nil
		}
	}
	return j, false, nil
}
func (j jsonObject) StripNulls() (JSON, bool, error) {
	for i, e := range j {
		var json JSON
		var err error
		needToStrip := false
		hasNullValue := e.v.Type() == NullJSONType
		if !hasNullValue {
			json, needToStrip, err = e.v.StripNulls()
			if err != nil {
				return nil, false, err
			}
		}
		if hasNullValue || needToStrip {
			// Cannot return the original content, need to return the result
			// with new JSON object.
			numNotNulls := i
			for _, elem := range j[i:] {
				if elem.v.Type() != NullJSONType {
					numNotNulls++
				}
			}
			// Use number of fields not having null value to construct newObj
			// so that no need to grow the capacity of newObj.
			newObj := make(jsonObject, 0, numNotNulls)
			newObj = append(newObj, j[:i]...)
			if !hasNullValue {
				newObj = append(newObj, jsonKeyValuePair{
					k: e.k,
					v: json,
				})
			}
			for _, elem := range j[i+1:] {
				if elem.v.Type() != NullJSONType {
					if json, _, err = elem.v.StripNulls(); err != nil {
						return nil, false, err
					}
					newObj = append(newObj, jsonKeyValuePair{
						k: elem.k,
						v: json,
					})
				}
			}
			return newObj, true, nil
		}
	}
	return j, false, nil
}

func (jsonNull) ObjectIter() (*ObjectIterator, error) {
	return nil, nil
}
func (jsonTrue) ObjectIter() (*ObjectIterator, error) {
	return nil, nil
}
func (jsonFalse) ObjectIter() (*ObjectIterator, error) {
	return nil, nil
}
func (jsonNumber) ObjectIter() (*ObjectIterator, error) {
	return nil, nil
}
func (jsonString) ObjectIter() (*ObjectIterator, error) {
	return nil, nil
}
func (jsonArray) ObjectIter() (*ObjectIterator, error) {
	return nil, nil
}
func (j jsonObject) ObjectIter() (*ObjectIterator, error) {
	return newObjectIterator(j), nil
}

func (jsonNull) isScalar() bool   { return true }
func (jsonFalse) isScalar() bool  { return true }
func (jsonTrue) isScalar() bool   { return true }
func (jsonNumber) isScalar() bool { return true }
func (jsonString) isScalar() bool { return true }
func (jsonArray) isScalar() bool  { return false }
func (jsonObject) isScalar() bool { return false }

func (jsonNull) Len() int     { return 0 }
func (jsonTrue) Len() int     { return 0 }
func (jsonFalse) Len() int    { return 0 }
func (jsonNumber) Len() int   { return 0 }
func (jsonString) Len() int   { return 0 }
func (j jsonArray) Len() int  { return len(j) }
func (j jsonObject) Len() int { return len(j) }

func (jsonNull) toGoRepr() (interface{}, error)     { return nil, nil }
func (jsonTrue) toGoRepr() (interface{}, error)     { return true, nil }
func (jsonFalse) toGoRepr() (interface{}, error)    { return false, nil }
func (j jsonString) toGoRepr() (interface{}, error) { return string(j), nil }
func (j jsonNumber) toGoRepr() (interface{}, error) { return json.Number(j.String()), nil }
func (j jsonArray) toGoRepr() (interface{}, error) {
	result := make([]interface{}, len(j))
	for i, e := range j {
		next, err := e.toGoRepr()
		if err != nil {
			return nil, err
		}
		result[i] = next
	}
	return result, nil
}
func (j jsonObject) toGoRepr() (interface{}, error) {
	result := make(map[string]interface{})
	for _, e := range j {
		next, err := e.v.toGoRepr()
		if err != nil {
			return nil, err
		}
		result[string(e.k)] = next
	}
	return result, nil
}

// Pretty pretty-prints the given JSON document as required by jsonb_pretty.
func Pretty(j JSON) (string, error) {
	asGo, err := j.toGoRepr()
	if err != nil {
		return "", err
	}
	// Luckily for us, despite Go's random map ordering, MarshalIndent sorts the
	// keys of objects.
	res, err := json.MarshalIndent(asGo, "", "    ")
	return string(res), errors.Handled(err)
}

var errCannotDeletePathInScalar = pgerror.WithCandidateCode(errors.New("cannot delete path in scalar"), pgcode.InvalidParameterValue)

func (j jsonArray) RemovePath(path []string) (JSON, bool, error)  { return j.doRemovePath(path) }
func (j jsonObject) RemovePath(path []string) (JSON, bool, error) { return j.doRemovePath(path) }
func (jsonNull) RemovePath([]string) (JSON, bool, error) {
	return nil, false, errCannotDeletePathInScalar
}
func (jsonTrue) RemovePath([]string) (JSON, bool, error) {
	return nil, false, errCannotDeletePathInScalar
}
func (jsonFalse) RemovePath([]string) (JSON, bool, error) {
	return nil, false, errCannotDeletePathInScalar
}
func (jsonString) RemovePath([]string) (JSON, bool, error) {
	return nil, false, errCannotDeletePathInScalar
}
func (jsonNumber) RemovePath([]string) (JSON, bool, error) {
	return nil, false, errCannotDeletePathInScalar
}

func (j jsonArray) doRemovePath(path []string) (JSON, bool, error) {
	if len(path) == 0 {
		return j, false, nil
	}
	// In path-deletion we have to attempt to parse numbers (this is different
	// from the `-` operator, where strings just never match on arrays).
	idx, err := strconv.Atoi(path[0])
	if err != nil {
		// TODO(yuzefovich): give the position of the path element to match psql.
		err := errors.Newf("a path element is not an integer: %s", path[0])
		err = pgerror.WithCandidateCode(err, pgcode.InvalidTextRepresentation)
		return j, false, err
	}
	if len(path) == 1 {
		return j.RemoveIndex(idx)
	}

	if idx < -len(j) || idx >= len(j) {
		return j, false, nil
	}
	if idx < 0 {
		idx += len(j)
	}
	newVal, ok, err := j[idx].doRemovePath(path[1:])
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return j, false, nil
	}

	result := make(jsonArray, len(j))
	copy(result, j)
	result[idx] = newVal

	return result, true, nil
}

func (j jsonObject) doRemovePath(path []string) (JSON, bool, error) {
	if len(path) == 0 {
		return j, false, nil
	}
	if len(path) == 1 {
		return j.RemoveString(path[0])
	}
	idx, ok := findPairIndexByKey(j, path[0])
	if !ok {
		return j, false, nil
	}

	newVal, ok, err := j[idx].v.doRemovePath(path[1:])
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return j, false, nil
	}

	result := make(jsonObject, len(j))
	copy(result, j)
	result[idx].v = newVal

	return result, true, nil
}

// When we hit a scalar, we stop. #- only errors if there's a scalar at the
// very top level.
func (j jsonNull) doRemovePath([]string) (JSON, bool, error)   { return j, false, nil }
func (j jsonTrue) doRemovePath([]string) (JSON, bool, error)   { return j, false, nil }
func (j jsonFalse) doRemovePath([]string) (JSON, bool, error)  { return j, false, nil }
func (j jsonString) doRemovePath([]string) (JSON, bool, error) { return j, false, nil }
func (j jsonNumber) doRemovePath([]string) (JSON, bool, error) { return j, false, nil }

func (j jsonObject) HasContainerLeaf() (bool, error) {
	if j.Len() == 0 {
		return true, nil
	}
	for _, c := range j {
		child, err := c.v.HasContainerLeaf()
		if err != nil {
			return false, err
		}
		if child {
			return true, nil
		}
	}
	return false, nil
}

func (j jsonArray) HasContainerLeaf() (bool, error) {
	if j.Len() == 0 {
		return true, nil
	}
	for _, c := range j {
		child, err := c.HasContainerLeaf()
		if err != nil {
			return false, err
		}
		if child {
			return true, nil
		}
	}
	return false, nil
}

func (j jsonNull) HasContainerLeaf() (bool, error)   { return false, nil }
func (j jsonTrue) HasContainerLeaf() (bool, error)   { return false, nil }
func (j jsonFalse) HasContainerLeaf() (bool, error)  { return false, nil }
func (j jsonString) HasContainerLeaf() (bool, error) { return false, nil }
func (j jsonNumber) HasContainerLeaf() (bool, error) { return false, nil }
