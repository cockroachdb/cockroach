// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// overloadBase and other overload-related structs form a leveled hierarchy
// that is useful during the code generation. Structs have "up" and "down"
// references to each other so that the necessary fields for the templating are
// accessible via '.'. Only when we reach the bottom level (where
// lastArgWidthOverload is) do we have the access to "resolved" overload
// functions.
//
// The idea is that every argument of an overload function can have overloaded
// type family and then the width of the type. argTypeOverload describes an
// overloaded argument that is not the last among all functions' arguments. The
// struct itself is "separated" into two levels - argTypeOverload and
// argWidthOverload for ease of iterating over it during the code generation.
// argTypeOverload describes a single "family-width" pair, so from that point
// of view, it is concrete. However, since this argument is not the last, the
// struct does *not* have AssignFunc and/or other functions because it is not
// "resolved" - when we see it during code generation, we don't have full
// information yet.
//
// Here is the diagram of relationships for argTypeOverload struct:
//
//   argTypeOverloadBase            overloadBase
//            \          \              |
//            \           ------        |
//            ↓                 ↓       ↓
//   argWidthOverloadBase       argTypeOverload
//               \                /
//                \              | (single)
//                ↓              ↓
//                argWidthOverload
//
// lastArgTypeOverload is similar in nature to argTypeOverload in that it
// describes an overloaded argument, but that argument is the last one, so the
// overload is "resolved" - it has access to the AssignFunc and/or other
// functions. The struct still overloads a single type family, however, it can
// have multiple widths for that type family, so it supports multiple "family-
// width" pairs.
//
// Here is the diagram of relationships for lastArgTypeOverload struct:
//
//   argTypeOverloadBase            overloadBase
//            \          \              |
//            \           ------        |
//            ↓                 ↓       ↓
//   argWidthOverloadBase     lastArgTypeOverload
//               \                /
//                \              | (multiple)
//                ↓              ↓
//                lastArgWidthOverload
//
// Two argument overload consists of multiple corresponding to each other
// argTypeOverloads and lastArgTypeOverloads.
//
// The important point is that an overload is "resolved" only at
// lastArgWidthOverload level - only that struct has the information about the
// return type and has access to the functions that can be called.
//
// These structs (or their "resolved" equivalents) are intended to be used by
// the code generation with the following patterns:
//
//   switch canonicalTypeFamily {
//     switch width {
//       <resolved one arg overload>
//     }
//   }
//
//   switch leftCanonicalTypeFamily {
//     switch leftWidth {
//       switch rightCanonicalTypeFamily {
//         switch rightWidth {
//           <resolved two arg overload>
//         }
//       }
//     }
//   }
type overloadBase struct {
	kind overloadKind

	Name  string
	OpStr string
	// Only one of CmpOp and BinOp will be set, depending on whether the
	// overload is a binary operator or a comparison operator. Neither of the
	// fields will be set when it is a hash or cast overload.
	CmpOp tree.ComparisonOperator
	BinOp tree.BinaryOperatorSymbol
}

// overloadKind describes the type of an overload. The word "kind" was chosen
// to reduce possible confusion with "types" of arguments of an overload.
type overloadKind int

const (
	binaryOverload overloadKind = iota
	comparisonOverload
	hashOverload
	castOverload
)

func (b *overloadBase) String() string {
	return fmt.Sprintf("%s: %s", b.Name, b.OpStr)
}

func toString(family types.Family) string {
	switch family {
	case typeconv.DatumVecCanonicalTypeFamily:
		return "typeconv.DatumVecCanonicalTypeFamily"
	default:
		return "types." + family.String()
	}
}

type argTypeOverloadBase struct {
	CanonicalTypeFamily    types.Family
	CanonicalTypeFamilyStr string
}

func newArgTypeOverloadBase(canonicalTypeFamily types.Family) *argTypeOverloadBase {
	return &argTypeOverloadBase{
		CanonicalTypeFamily:    canonicalTypeFamily,
		CanonicalTypeFamilyStr: toString(canonicalTypeFamily),
	}
}

func (b *argTypeOverloadBase) String() string {
	return b.CanonicalTypeFamilyStr
}

// argTypeOverload describes an overloaded argument that is not the last among
// all functions' arguments. The struct itself is "separated" into two levels -
// argTypeOverload and argWidthOverload for ease of iterating over it during
// the code generation. argTypeOverload describes a single "family-width" pair,
// so from that point of view, it is concrete. However, since this argument is
// not the last, the struct does *not* have AssignFunc and/or other functions
// because it is not "resolved" - when we see it during code generation, we
// don't have full information yet.
type argTypeOverload struct {
	*overloadBase
	*argTypeOverloadBase

	WidthOverload *argWidthOverload
}

// newArgTypeOverload creates a new argTypeOverload.
func newArgTypeOverload(
	ob *overloadBase, canonicalTypeFamily types.Family, width int32,
) *argTypeOverload {
	typeOverload := &argTypeOverload{
		overloadBase:        ob,
		argTypeOverloadBase: newArgTypeOverloadBase(canonicalTypeFamily),
	}
	typeOverload.WidthOverload = newArgWidthOverload(typeOverload, width)
	return typeOverload
}

func (o *argTypeOverload) String() string {
	return fmt.Sprintf("%s\t%s", o.overloadBase, o.WidthOverload)
}

// lastArgTypeOverload is similar in nature to argTypeOverload in that it
// describes an overloaded argument, but that argument is the last one, so the
// overload is "resolved" - it has access to the AssignFunc and/or other
// functions. The struct still overloads a single type family, however, it can
// have multiple widths for that type family, so it supports multiple "family-
// width" pairs.
type lastArgTypeOverload struct {
	*overloadBase
	*argTypeOverloadBase

	WidthOverloads []*lastArgWidthOverload
}

// newLastArgTypeOverload creates a new lastArgTypeOverload. Note that
// WidthOverloads field is not populated and will be updated according when
// creating lastArgWidthOverloads.
func newLastArgTypeOverload(
	ob *overloadBase, canonicalTypeFamily types.Family,
) *lastArgTypeOverload {
	return &lastArgTypeOverload{
		overloadBase:        ob,
		argTypeOverloadBase: newArgTypeOverloadBase(canonicalTypeFamily),
	}
}

func (o *lastArgTypeOverload) String() string {
	s := fmt.Sprintf("%s\t%s", o.overloadBase, o.WidthOverloads[0])
	for _, wo := range o.WidthOverloads[1:] {
		s = fmt.Sprintf("%s\n%s", s, wo)
	}
	return s
}

type argWidthOverloadBase struct {
	*argTypeOverloadBase

	Width int32
	// VecMethod is the name of the method that should be called on coldata.Vec
	// to get access to the well-typed underlying memory.
	VecMethod string
	// GoType is the physical representation of a single element of the vector.
	GoType string
}

func newArgWidthOverloadBase(
	typeOverloadBase *argTypeOverloadBase, width int32,
) *argWidthOverloadBase {
	return &argWidthOverloadBase{
		argTypeOverloadBase: typeOverloadBase,
		Width:               width,
		VecMethod:           toVecMethod(typeOverloadBase.CanonicalTypeFamily, width),
		GoType:              toPhysicalRepresentation(typeOverloadBase.CanonicalTypeFamily, width),
	}
}

func (b *argWidthOverloadBase) IsBytesLike() bool {
	switch b.CanonicalTypeFamily {
	case types.JsonFamily, types.BytesFamily:
		return true
	}
	return false
}

func (b *argWidthOverloadBase) String() string {
	return fmt.Sprintf("%s\tWidth: %d\tVecMethod: %s", b.argTypeOverloadBase, b.Width, b.VecMethod)
}

type argWidthOverload struct {
	*argTypeOverload
	*argWidthOverloadBase
}

func newArgWidthOverload(typeOverload *argTypeOverload, width int32) *argWidthOverload {
	return &argWidthOverload{
		argTypeOverload:      typeOverload,
		argWidthOverloadBase: newArgWidthOverloadBase(typeOverload.argTypeOverloadBase, width),
	}
}

func (o *argWidthOverload) String() string {
	return o.argWidthOverloadBase.String()
}

type lastArgWidthOverload struct {
	*lastArgTypeOverload
	*argWidthOverloadBase

	RetType      *types.T
	RetVecMethod string
	RetGoType    string

	AssignFunc  assignFunc
	CompareFunc compareFunc
	CastFunc    castFunc
}

// newLastArgWidthOverload creates a new lastArgWidthOverload. Note that it
// updates the typeOverload to include the newly-created struct into
// WidthOverloads field.
func newLastArgWidthOverload(
	typeOverload *lastArgTypeOverload, width int32, retType *types.T,
) *lastArgWidthOverload {
	retCanonicalTypeFamily := typeconv.TypeFamilyToCanonicalTypeFamily(retType.Family())
	lawo := &lastArgWidthOverload{
		lastArgTypeOverload:  typeOverload,
		argWidthOverloadBase: newArgWidthOverloadBase(typeOverload.argTypeOverloadBase, width),
		RetType:              retType,
		RetVecMethod:         toVecMethod(retCanonicalTypeFamily, retType.Width()),
		RetGoType:            toPhysicalRepresentation(retCanonicalTypeFamily, retType.Width()),
	}
	typeOverload.WidthOverloads = append(typeOverload.WidthOverloads, lawo)
	return lawo
}

func (o *lastArgWidthOverload) String() string {
	return fmt.Sprintf("%s\tReturn: %s", o.argWidthOverloadBase, o.RetType.Name())
}

type oneArgOverload struct {
	*lastArgTypeOverload
}

func (o *oneArgOverload) String() string {
	return fmt.Sprintf("%s\n", o.lastArgTypeOverload.String())
}

// twoArgsResolvedOverload is a utility struct that represents an overload that
// takes it two arguments and that has been "resolved" (meaning it supports
// only a single type family and a single type width on both sides).
type twoArgsResolvedOverload struct {
	*overloadBase
	Left  *argWidthOverload
	Right *lastArgWidthOverload
}

// twoArgsResolvedOverloadsInfo contains all overloads that take in two
// arguments and stores them in a similar hierarchical structure to how
// twoArgsOverloads are stored, with the difference that on the "bottom" level
// we store the "resolved" overload.
var twoArgsResolvedOverloadsInfo struct {
	BinOps        []*twoArgsResolvedOverloadInfo
	CmpOps        []*twoArgsResolvedOverloadInfo
	CastOverloads *twoArgsResolvedOverloadInfo
}

var resolvedBinCmpOpsOverloads []*twoArgsResolvedOverload

type twoArgsResolvedOverloadInfo struct {
	*overloadBase
	LeftFamilies []*twoArgsResolvedOverloadLeftFamilyInfo
}

type twoArgsResolvedOverloadLeftFamilyInfo struct {
	LeftCanonicalFamilyStr string
	LeftWidths             []*twoArgsResolvedOverloadLeftWidthInfo
}

type twoArgsResolvedOverloadLeftWidthInfo struct {
	Width         int32
	RightFamilies []*twoArgsResolvedOverloadRightFamilyInfo
}

type twoArgsResolvedOverloadRightFamilyInfo struct {
	RightCanonicalFamilyStr string
	RightWidths             []*twoArgsResolvedOverloadRightWidthInfo
}

type twoArgsResolvedOverloadRightWidthInfo struct {
	Width int32
	*twoArgsResolvedOverload
}

type assignFunc func(op *lastArgWidthOverload, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string) string
type compareFunc func(targetElem, leftElem, rightElem, leftCol, rightCol string) string
type castFunc func(to, from, fromCol, toType string) string

// Assign produces a Go source string that assigns the "targetElem" variable to
// the result of applying the overload to the two inputs, "leftElem" and
// "rightElem". Some overload implementations might need access to the column
// variable names, and those are provided via the corresponding parameters.
// Note that these are not generic vectors (i.e. not coldata.Vec) but rather
// concrete columns (e.g. []int64).
//
// For example, an overload that implemented the float64 plus operation, when
// fed the inputs "x", "a", "b", "xCol", "aCol", "bCol", would produce the
// string "x = a + b".
func (o *lastArgWidthOverload) Assign(
	targetElem, leftElem, rightElem, targetCol, leftCol, rightCol string,
) string {
	if o.AssignFunc != nil {
		if ret := o.AssignFunc(
			o, targetElem, leftElem, rightElem, targetCol, leftCol, rightCol,
		); ret != "" {
			return ret
		}
	}
	// Default assign form assumes an infix operator.
	return fmt.Sprintf("%s = %s %s %s", targetElem, leftElem, o.overloadBase.OpStr, rightElem)
}

// Compare produces a Go source string that assigns the "targetElem" variable to
// the result of comparing the two inputs, "leftElem" and "rightElem". Some
// overload implementations might need access to the vector variable names, and
// those are provided via the corresponding parameters. Note that there is no
// "targetCol" variable because we know that the target column is []bool.
//
// The targetElem will be negative, zero, or positive depending on whether
// leftElem is less-than, equal-to, or greater-than rightElem.
func (o *lastArgWidthOverload) Compare(
	targetElem, leftElem, rightElem, leftCol, rightCol string,
) string {
	if o.CompareFunc != nil {
		if ret := o.CompareFunc(targetElem, leftElem, rightElem, leftCol, rightCol); ret != "" {
			return ret
		}
	}
	// Default compare form assumes an infix operator.
	return fmt.Sprintf(
		"if %s < %s { %s = -1 } else if %s > %s { %s = 1 } else { %s = 0 }",
		leftElem, rightElem, targetElem, leftElem, rightElem, targetElem, targetElem)
}

func (o *lastArgWidthOverload) Cast(to, from, fromCol, toType string) string {
	if o.CastFunc != nil {
		if ret := o.CastFunc(to, from, fromCol, toType); ret != "" {
			return ret
		}
	}
	// Default cast function is "identity" cast.
	return fmt.Sprintf("%s = %s", to, from)
}

func (o *lastArgWidthOverload) UnaryAssign(targetElem, vElem, targetCol, vVec string) string {
	if o.AssignFunc != nil {
		if ret := o.AssignFunc(o, targetElem, vElem, "", targetCol, vVec, ""); ret != "" {
			return ret
		}
	}
	// Default assign form assumes a function operator.
	return fmt.Sprintf("%s = %s(%s)", targetElem, o.overloadBase.OpStr, vElem)
}

func goTypeSliceName(canonicalTypeFamily types.Family, width int32) string {
	switch canonicalTypeFamily {
	case types.BoolFamily:
		return "coldata.Bools"
	case types.BytesFamily:
		return "*coldata.Bytes"
	case types.DecimalFamily:
		return "coldata.Decimals"
	case types.IntFamily:
		switch width {
		case 16:
			return "coldata.Int16s"
		case 32:
			return "coldata.Int32s"
		case 64, anyWidth:
			return "coldata.Int64s"
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unexpected int width %d", width))
			// This code is unreachable, but the compiler cannot infer that.
			return ""
		}
	case types.IntervalFamily:
		return "coldata.Durations"
	case types.JsonFamily:
		return "*coldata.JSONs"
	case types.FloatFamily:
		return "coldata.Float64s"
	case types.TimestampTZFamily:
		return "coldata.Times"
	case typeconv.DatumVecCanonicalTypeFamily:
		return "coldata.DatumVec"
	}
	colexecerror.InternalError(errors.AssertionFailedf("unsupported canonical type family %s", canonicalTypeFamily))
	return ""
}

func (b *argWidthOverloadBase) GoTypeSliceName() string {
	return goTypeSliceName(b.CanonicalTypeFamily, b.Width)
}

func copyVal(canonicalTypeFamily types.Family, dest, src string) string {
	switch canonicalTypeFamily {
	case types.BytesFamily:
		return fmt.Sprintf("%[1]s = append(%[1]s[:0], %[2]s...)", dest, src)
	case types.JsonFamily:
		return fmt.Sprintf(`
var _err error
var _bytes []byte
_bytes, _err = json.EncodeJSON(nil, %[1]s)
if _err != nil {
    colexecerror.ExpectedError(_err)
}
%[2]s, _err = json.FromEncoding(_bytes)
if _err != nil {
    colexecerror.ExpectedError(_err)
}
`, src, dest)
	case types.DecimalFamily:
		return fmt.Sprintf("%s.Set(&%s)", dest, src)
	}
	return fmt.Sprintf("%s = %s", dest, src)
}

// CopyVal is a function that should only be used in templates.
func (b *argWidthOverloadBase) CopyVal(dest, src string) string {
	return copyVal(b.CanonicalTypeFamily, dest, src)
}

// sliceable returns whether the vector of canonicalTypeFamily can be sliced
// (i.e. whether it is a Golang's slice).
func sliceable(canonicalTypeFamily types.Family) bool {
	switch canonicalTypeFamily {
	case types.BytesFamily, types.JsonFamily, typeconv.DatumVecCanonicalTypeFamily:
		return false
	default:
		return true
	}
}

// Sliceable is a function that should only be used in templates.
func (b *argWidthOverloadBase) Sliceable() bool {
	return sliceable(b.CanonicalTypeFamily)
}

// AppendSlice is a function that should only be used in templates.
func (b *argWidthOverloadBase) AppendSlice(
	target, src, destIdx, srcStartIdx, srcEndIdx string,
) string {
	var tmpl string
	switch b.CanonicalTypeFamily {
	case types.BytesFamily, types.JsonFamily, typeconv.DatumVecCanonicalTypeFamily:
		tmpl = `{{.Tgt}}.AppendSlice({{.Src}}, {{.TgtIdx}}, {{.SrcStart}}, {{.SrcEnd}})`
	case types.DecimalFamily:
		tmpl = `{
  __desiredCap := {{.TgtIdx}} + {{.SrcEnd}} - {{.SrcStart}}
  if cap({{.Tgt}}) >= __desiredCap {
  	{{.Tgt}} = {{.Tgt}}[:__desiredCap]
  } else {
    __prevCap := cap({{.Tgt}})
    __capToAllocate := __desiredCap
    if __capToAllocate < 2 * __prevCap {
      __capToAllocate = 2 * __prevCap
    }
    __new_slice := make([]apd.Decimal, __desiredCap, __capToAllocate)
    copy(__new_slice, {{.Tgt}}[:{{.TgtIdx}}])
    {{.Tgt}} = __new_slice
  }
  __src_slice := {{.Src}}[{{.SrcStart}}:{{.SrcEnd}}]
  __dst_slice := {{.Tgt}}[{{.TgtIdx}}:]
  _ = __dst_slice[len(__src_slice)-1]
  for __i := range __src_slice {
    //gcassert:bce
    __dst_slice[__i].Set(&__src_slice[__i])
  }
}`
	default:
		tmpl = `{{.Tgt}} = append({{.Tgt}}[:{{.TgtIdx}}], {{.Src}}[{{.SrcStart}}:{{.SrcEnd}}]...)`
	}
	args := map[string]string{
		"Tgt":      target,
		"Src":      src,
		"TgtIdx":   destIdx,
		"SrcStart": srcStartIdx,
		"SrcEnd":   srcEndIdx,
	}
	var buf strings.Builder
	if err := template.Must(template.New("").Parse(tmpl)).Execute(&buf, args); err != nil {
		colexecerror.InternalError(err)
	}
	return buf.String()
}

// AppendVal is a function that should only be used in templates.
func (b *argWidthOverloadBase) AppendVal(target, v string) string {
	switch b.CanonicalTypeFamily {
	case types.BytesFamily, types.JsonFamily, typeconv.DatumVecCanonicalTypeFamily:
		return fmt.Sprintf("%s.AppendVal(%s)", target, v)
	case types.DecimalFamily:
		return fmt.Sprintf(`%[1]s = append(%[1]s, apd.Decimal{})
%[1]s[len(%[1]s)-1].Set(&%[2]s)`, target, v)
	}
	return fmt.Sprintf("%[1]s = append(%[1]s, %[2]s)", target, v)
}

// setVariableSize is a function that should only be used in templates. It
// returns a string that contains a code snippet for computing the size of the
// object named 'value' if it has variable size and assigns it to the variable
// named 'target' (for fixed sizes the snippet will simply declare the 'target'
// variable). The value object must be of canonicalTypeFamily representation.
func setVariableSize(canonicalTypeFamily types.Family, target, value string) string {
	switch canonicalTypeFamily {
	case types.BytesFamily:
		return fmt.Sprintf(`%s := len(%s)`, target, value)
	case types.JsonFamily:
		return fmt.Sprintf(`var %[1]s uintptr
if %[2]s != nil {
    %[1]s = %[2]s.Size()
}`, target, value)
	case types.DecimalFamily:
		return fmt.Sprintf(`%s := tree.SizeOfDecimal(&%s)`, target, value)
	case typeconv.DatumVecCanonicalTypeFamily:
		return fmt.Sprintf(`
		var %[1]s uintptr
		if %[2]s != nil {
			%[1]s = %[2]s.(*coldataext.Datum).Size()
		}`, target, value)
	default:
		return fmt.Sprintf(`var %s uintptr`, target)
	}
}

// SetVariableSize is a function that should only be used in templates. See the
// comment on setVariableSize for more details.
func (b *argWidthOverloadBase) SetVariableSize(target, value string) string {
	return setVariableSize(b.CanonicalTypeFamily, target, value)
}

// Remove unused warnings.
var (
	lawo = &lastArgWidthOverload{}
	_    = lawo.Assign
	_    = lawo.Compare
	_    = lawo.Cast
	_    = lawo.UnaryAssign

	awob = &argWidthOverloadBase{}
	_    = awob.GoTypeSliceName
	_    = awob.CopyVal
	_    = awob.Sliceable
	_    = awob.AppendSlice
	_    = awob.AppendVal
	_    = awob.SetVariableSize
	_    = awob.IsBytesLike
)

func init() {
	registerTypeCustomizers()

	populateBinOpOverloads()
	populateCmpOpOverloads()
	populateHashOverloads()
	populateCastOverloads()
}

// typeCustomizer is a marker interface for something that implements one or
// more of binOpTypeCustomizer, cmpOpTypeCustomizer, hashTypeCustomizer,
// castTypeCustomizer.
//
// A type customizer allows custom templating behavior for a particular type
// that doesn't permit the ordinary Go assignment (x = y), comparison
// (==, <, etc) or binary operator (+, -, etc) semantics.
type typeCustomizer interface{}

// typePair is used to key a map that holds all typeCustomizers.
type typePair struct {
	leftTypeFamily  types.Family
	leftWidth       int32
	rightTypeFamily types.Family
	rightWidth      int32
}

var typeCustomizers map[typePair]typeCustomizer

// registerTypeCustomizer registers a particular type customizer to a
// pair of types, for usage by templates.
func registerTypeCustomizer(pair typePair, customizer typeCustomizer) {
	typeCustomizers[pair] = customizer
}

// boolCustomizer is necessary since bools don't support < <= > >= in Go.
type boolCustomizer struct{}

// bytesCustomizer is necessary since []byte doesn't support comparison ops in
// Go - bytes.Compare and so on have to be used.
type bytesCustomizer struct{}

// decimalCustomizer is necessary since apd.Decimal doesn't have infix operator
// support for binary or comparison operators, and also doesn't have normal
// variable-set semantics.
type decimalCustomizer struct{}

// floatCustomizers are used for hash functions.
type floatCustomizer struct{}

// intCustomizers are used for hash functions and overflow handling.
type intCustomizer struct{ width int32 }

// decimalFloatCustomizer supports mixed type expressions with a decimal
// left-hand side and a float right-hand side.
type decimalFloatCustomizer struct{}

// decimalIntCustomizer supports mixed type expressions with a decimal left-hand
// side and an int right-hand side.
type decimalIntCustomizer struct{}

// floatDecimalCustomizer supports mixed type expressions with a float left-hand
// side and a decimal right-hand side.
type floatDecimalCustomizer struct{}

// intDecimalCustomizer supports mixed type expressions with an int left-hand
// side and a decimal right-hand side.
type intDecimalCustomizer struct{}

// floatIntCustomizer supports mixed type expressions with a float left-hand
// side and an int right-hand side.
type floatIntCustomizer struct{}

// intFloatCustomizer supports mixed type expressions with an int left-hand
// side and a float right-hand side.
type intFloatCustomizer struct{}

// timestampCustomizer is necessary since time.Time doesn't have infix operators.
type timestampCustomizer struct{}

// intervalCustomizer is necessary since duration.Duration doesn't have infix
// operators.
type intervalCustomizer struct{}

// jsonCustomizer is necessary since json.JSON doesn't have infix operators.
type jsonCustomizer struct{}

// timestampIntervalCustomizer supports mixed type expression with a timestamp
// left-hand side and an interval right-hand side.
type timestampIntervalCustomizer struct{}

// intervalTimestampCustomizer supports mixed type expression with an interval
// left-hand side and a timestamp right-hand side.
type intervalTimestampCustomizer struct{}

// intervalIntCustomizer supports mixed type expression with an interval
// left-hand side and an int right-hand side.
type intervalIntCustomizer struct{}

// intIntervalCustomizer supports mixed type expression with an int left-hand
// side and an interval right-hand side.
type intIntervalCustomizer struct{}

// intervalFloatCustomizer supports mixed type expression with an interval
// left-hand side and a float right-hand side.
type intervalFloatCustomizer struct{}

// jsonBytesCustomizer supports mixed type expressions with a json left-hand
// side and a bytes right-hand side.
type jsonBytesCustomizer struct{}

// jsonIntCustomizer supports mixed type expressions with a json left-hand
// side and a int right-hand side.
type jsonIntCustomizer struct{}

// jsonDatumCustomizer supports mixed type expression with a JSON left-hand side
// and datum right-hand side.
type jsonDatumCustomizer struct{}

// floatIntervalCustomizer supports mixed type expression with a float
// left-hand side and an interval right-hand side.
type floatIntervalCustomizer struct{}

// intervalDecimalCustomizer supports mixed type expression with an interval
// left-hand side and a decimal right-hand side.
type intervalDecimalCustomizer struct{}

// decimalIntervalCustomizer supports mixed type expression with a decimal
// left-hand side and an interval right-hand side.
type decimalIntervalCustomizer struct{}

// datumCustomizer supports overloads on tree.Datums.
type datumCustomizer struct{}

// datumNonDatumCustomizer supports overloads of mixed type binary expressions
// with a datum left-hand side and non-datum right-hand side.
type datumNonDatumCustomizer struct{}

// nonDatumDatumCustomizer supports overloads of mixed type binary expressions
// with a non-datum left-hand side and datum right-hand side.
type nonDatumDatumCustomizer struct {
	leftCanonicalTypeFamily types.Family
}

// TODO(yuzefovich): add support for datums on both sides and non-datum result.

func registerTypeCustomizers() {
	typeCustomizers = make(map[typePair]typeCustomizer)
	registerTypeCustomizer(typePair{types.BoolFamily, anyWidth, types.BoolFamily, anyWidth}, boolCustomizer{})
	registerTypeCustomizer(typePair{types.BytesFamily, anyWidth, types.BytesFamily, anyWidth}, bytesCustomizer{})
	registerTypeCustomizer(typePair{types.DecimalFamily, anyWidth, types.DecimalFamily, anyWidth}, decimalCustomizer{})
	registerTypeCustomizer(typePair{types.FloatFamily, anyWidth, types.FloatFamily, anyWidth}, floatCustomizer{})
	registerTypeCustomizer(typePair{types.TimestampTZFamily, anyWidth, types.TimestampTZFamily, anyWidth}, timestampCustomizer{})
	registerTypeCustomizer(typePair{types.IntervalFamily, anyWidth, types.IntervalFamily, anyWidth}, intervalCustomizer{})
	registerTypeCustomizer(typePair{types.JsonFamily, anyWidth, types.JsonFamily, anyWidth}, jsonCustomizer{})
	registerTypeCustomizer(typePair{types.JsonFamily, anyWidth, types.BytesFamily, anyWidth}, jsonBytesCustomizer{})
	registerTypeCustomizer(typePair{types.JsonFamily, anyWidth, typeconv.DatumVecCanonicalTypeFamily, anyWidth}, jsonDatumCustomizer{})
	registerTypeCustomizer(typePair{typeconv.DatumVecCanonicalTypeFamily, anyWidth, typeconv.DatumVecCanonicalTypeFamily, anyWidth}, datumCustomizer{})
	for _, leftIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		for _, rightIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			registerTypeCustomizer(typePair{types.IntFamily, leftIntWidth, types.IntFamily, rightIntWidth}, intCustomizer{width: anyWidth})
		}
	}
	// Use a customizer of appropriate width when widths are the same.
	for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		registerTypeCustomizer(typePair{types.IntFamily, intWidth, types.IntFamily, intWidth}, intCustomizer{width: intWidth})
	}

	registerTypeCustomizer(typePair{types.DecimalFamily, anyWidth, types.FloatFamily, anyWidth}, decimalFloatCustomizer{})
	registerTypeCustomizer(typePair{types.IntervalFamily, anyWidth, types.FloatFamily, anyWidth}, intervalFloatCustomizer{})
	for _, rightIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		registerTypeCustomizer(typePair{types.DecimalFamily, anyWidth, types.IntFamily, rightIntWidth}, decimalIntCustomizer{})
		registerTypeCustomizer(typePair{types.FloatFamily, anyWidth, types.IntFamily, rightIntWidth}, floatIntCustomizer{})
		registerTypeCustomizer(typePair{types.IntervalFamily, anyWidth, types.IntFamily, rightIntWidth}, intervalIntCustomizer{})
		registerTypeCustomizer(typePair{types.JsonFamily, anyWidth, types.IntFamily, rightIntWidth}, jsonIntCustomizer{})
	}
	registerTypeCustomizer(typePair{types.FloatFamily, anyWidth, types.DecimalFamily, anyWidth}, floatDecimalCustomizer{})
	registerTypeCustomizer(typePair{types.FloatFamily, anyWidth, types.IntervalFamily, anyWidth}, floatIntervalCustomizer{})
	for _, leftIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		registerTypeCustomizer(typePair{types.IntFamily, leftIntWidth, types.DecimalFamily, anyWidth}, intDecimalCustomizer{})
		registerTypeCustomizer(typePair{types.IntFamily, leftIntWidth, types.FloatFamily, anyWidth}, intFloatCustomizer{})
		registerTypeCustomizer(typePair{types.IntFamily, leftIntWidth, types.IntervalFamily, anyWidth}, intIntervalCustomizer{})
	}
	registerTypeCustomizer(typePair{types.TimestampTZFamily, anyWidth, types.IntervalFamily, anyWidth}, timestampIntervalCustomizer{})
	registerTypeCustomizer(typePair{types.IntervalFamily, anyWidth, types.TimestampTZFamily, anyWidth}, intervalTimestampCustomizer{})
	registerTypeCustomizer(typePair{types.IntervalFamily, anyWidth, types.DecimalFamily, anyWidth}, intervalDecimalCustomizer{})
	registerTypeCustomizer(typePair{types.DecimalFamily, anyWidth, types.IntervalFamily, anyWidth}, decimalIntervalCustomizer{})

	for _, compatibleFamily := range compatibleCanonicalTypeFamilies[typeconv.DatumVecCanonicalTypeFamily] {
		if compatibleFamily != typeconv.DatumVecCanonicalTypeFamily {
			for _, width := range supportedWidthsByCanonicalTypeFamily[compatibleFamily] {
				registerTypeCustomizer(typePair{typeconv.DatumVecCanonicalTypeFamily, anyWidth, compatibleFamily, width}, datumNonDatumCustomizer{})
				registerTypeCustomizer(typePair{compatibleFamily, width, typeconv.DatumVecCanonicalTypeFamily, anyWidth}, nonDatumDatumCustomizer{compatibleFamily})
			}
		}
	}
}

var supportedCanonicalTypeFamilies = []types.Family{
	types.BoolFamily,
	types.BytesFamily,
	types.DecimalFamily,
	types.IntFamily,
	types.FloatFamily,
	types.TimestampTZFamily,
	types.IntervalFamily,
	types.JsonFamily,
	typeconv.DatumVecCanonicalTypeFamily,
}

// anyWidth is special "value" of width of a type that will be used to generate
// "case -1: default:" block that would match all widths that are not
// explicitly specified.
const anyWidth = -1

var typeWidthReplacement = fmt.Sprintf("{{.Width}}{{if eq .Width %d}}: default{{end}}", anyWidth)

// supportedWidthsByCanonicalTypeFamily is a mapping from a canonical type
// family to all widths that are supported by that family. Make sure that
// anyWidth value is the last one in every slice.
var supportedWidthsByCanonicalTypeFamily = map[types.Family][]int32{
	types.BoolFamily:                     {anyWidth},
	types.BytesFamily:                    {anyWidth},
	types.DecimalFamily:                  {anyWidth},
	types.IntFamily:                      {16, 32, anyWidth},
	types.FloatFamily:                    {anyWidth},
	types.TimestampTZFamily:              {anyWidth},
	types.IntervalFamily:                 {anyWidth},
	types.JsonFamily:                     {anyWidth},
	typeconv.DatumVecCanonicalTypeFamily: {anyWidth},
}

var numericCanonicalTypeFamilies = []types.Family{types.IntFamily, types.FloatFamily, types.DecimalFamily}

// toVecMethod returns the method name from coldata.Vec interface that can be
// used to get the well-typed underlying memory from a vector.
func toVecMethod(canonicalTypeFamily types.Family, width int32) string {
	switch canonicalTypeFamily {
	case types.BoolFamily:
		return "Bool"
	case types.BytesFamily:
		return "Bytes"
	case types.DecimalFamily:
		return "Decimal"
	case types.IntFamily:
		switch width {
		case 16:
			return "Int16"
		case 32:
			return "Int32"
		case 64, anyWidth:
			return "Int64"
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unexpected width of int type family: %d", width))
		}
	case types.FloatFamily:
		return "Float64"
	case types.TimestampTZFamily:
		return "Timestamp"
	case types.IntervalFamily:
		return "Interval"
	case types.JsonFamily:
		return "JSON"
	case typeconv.DatumVecCanonicalTypeFamily:
		return "Datum"
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unsupported canonical type family %s", canonicalTypeFamily))
	}
	// This code is unreachable, but the compiler cannot infer that.
	return ""
}

// toPhysicalRepresentation returns a string that describes how a single
// element from a vector of the provided family and width is represented
// physically.
func toPhysicalRepresentation(canonicalTypeFamily types.Family, width int32) string {
	switch canonicalTypeFamily {
	case types.BoolFamily:
		return "bool"
	case types.BytesFamily:
		return "[]byte"
	case types.DecimalFamily:
		return "apd.Decimal"
	case types.IntFamily:
		switch width {
		case 16:
			return "int16"
		case 32:
			return "int32"
		case 64, anyWidth:
			return "int64"
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unexpected width of int type family: %d", width))
		}
	case types.FloatFamily:
		return "float64"
	case types.TimestampTZFamily:
		return "time.Time"
	case types.IntervalFamily:
		return "duration.Duration"
	case types.JsonFamily:
		return "json.JSON"
	case typeconv.DatumVecCanonicalTypeFamily:
		// This is somewhat unfortunate, but we can neither use coldata.Datum
		// nor tree.Datum because we have generated files living in two
		// different packages (sql/colexec and col/coldata).
		return "interface{}"
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unsupported canonical type family %s", canonicalTypeFamily))
	}
	// This code is unreachable, but the compiler cannot infer that.
	return ""
}
