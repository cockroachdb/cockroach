// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coltypes

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/cockroachdb/apd"
)

// T represents an exec physical type - a bytes representation of a particular
// column type.
type T int

//go:generate stringer -type=T
const (
	// Bool is a column of type bool
	Bool T = iota
	// Bytes is a column of type []byte
	Bytes
	// Decimal is a column of type apd.Decimal
	Decimal
	// Int16 is a column of type int16
	Int16
	// Int32 is a column of type int32
	Int32
	// Int64 is a column of type int64
	Int64
	// Float64 is a column of type float64
	Float64

	// Unhandled is a temporary value that represents an unhandled type.
	// TODO(jordan): this should be replaced by a panic once all types are
	// handled.
	Unhandled
)

// AllTypes is slice of all exec types.
var AllTypes []T

// CompatibleTypes maps a type to a slice of types that can be used with that
// type in a binary expression.
var CompatibleTypes map[T][]T

// NumberTypes is a slice containing all numeric types.
var NumberTypes = []T{Int16, Int32, Int64, Float64, Decimal}

// IntTypes is a slice containing all int types.
var IntTypes = []T{Int16, Int32, Int64}

// FloatTypes is a slice containing all float types.
var FloatTypes = []T{Float64}

func init() {
	for i := Bool; i < Unhandled; i++ {
		AllTypes = append(AllTypes, i)
	}

	CompatibleTypes = make(map[T][]T)
	CompatibleTypes[Bool] = append(CompatibleTypes[Bool], Bool)
	CompatibleTypes[Bytes] = append(CompatibleTypes[Bytes], Bytes)
	CompatibleTypes[Decimal] = append(CompatibleTypes[Decimal], NumberTypes...)
	CompatibleTypes[Int16] = append(CompatibleTypes[Int16], NumberTypes...)
	CompatibleTypes[Int32] = append(CompatibleTypes[Int32], NumberTypes...)
	CompatibleTypes[Int64] = append(CompatibleTypes[Int64], NumberTypes...)
	CompatibleTypes[Float64] = append(CompatibleTypes[Float64], NumberTypes...)
}

// FromGoType returns the type for a Go value, if applicable. Shouldn't be used at
// runtime.
func FromGoType(v interface{}) T {
	switch t := v.(type) {
	case int16:
		return Int16
	case int32:
		return Int32
	case int, int64:
		return Int64
	case bool:
		return Bool
	case float64:
		return Float64
	case []byte:
		return Bytes
	case string:
		return Bytes
	case apd.Decimal:
		return Decimal
	default:
		panic(fmt.Sprintf("type %T not supported yet", t))
	}
}

// GoTypeName returns the stringified Go type for an exec type.
func (t T) GoTypeName() string {
	switch t {
	case Bool:
		return "bool"
	case Bytes:
		return "[]byte"
	case Decimal:
		return "apd.Decimal"
	case Int16:
		return "int16"
	case Int32:
		return "int32"
	case Int64:
		return "int64"
	case Float64:
		return "float64"
	default:
		panic(fmt.Sprintf("unhandled type %d", t))
	}
}

// Remove unused warnings for templating functions.
var (
	_ = Bool.GoTypeSliceName
	_ = Bool.Get
	_ = Bool.Set
	_ = Bool.Slice
	_ = Bool.CopySlice
	_ = Bool.CopyVal
	_ = Bool.AppendSlice
	_ = Bool.AppendVal
	_ = Bool.Len
	_ = Bool.Range
	_ = Bool.Zero
)

// GoTypeSliceName returns how a slice of the receiver type is represented.
func (t T) GoTypeSliceName() string {
	if t == Bytes {
		return "*coldata.Bytes"
	}
	return "[]" + t.GoTypeName()
}

// Get is a function that should only be used in templates.
func (t T) Get(target, i string) string {
	switch t {
	case Bytes:
		return fmt.Sprintf("%s.Get(%s)", target, i)
	}
	return fmt.Sprintf("%s[%s]", target, i)
}

// CopyVal is a function that should only be used in templates.
func (t T) CopyVal(dest, src string) string {
	switch t {
	case Bytes:
		return fmt.Sprintf("%[1]s = append(%[1]s[:0], %[2]s...)", dest, src)
	case Decimal:
		return fmt.Sprintf("%s.Set(&%s)", dest, src)
	}
	return fmt.Sprintf("%s = %s", dest, src)
}

// Set is a function that should only be used in templates.
func (t T) Set(target, i, new string) string {
	switch t {
	case Bytes:
		return fmt.Sprintf("%s.Set(%s, %s)", target, i, new)
	case Decimal:
		return fmt.Sprintf("%s[%s].Set(&%s)", target, i, new)
	}
	return fmt.Sprintf("%s[%s] = %s", target, i, new)
}

// Slice is a function that should only be used in templates.
func (t T) Slice(target, start, end string) string {
	if t == Bytes {
		return fmt.Sprintf("%s.Slice(%s, %s)", target, start, end)
	}
	return fmt.Sprintf("%s[%s:%s]", target, start, end)
}

// CopySlice is a function that should only be used in templates.
func (t T) CopySlice(target, src, destIdx, srcStartIdx, srcEndIdx string) string {
	var tmpl string
	switch t {
	case Bytes:
		tmpl = `{{.Tgt}}.CopySlice({{.Src}}, {{.TgtIdx}}, {{.SrcStart}}, {{.SrcEnd}})`
	case Decimal:
		tmpl = `{
  __tgt_slice := {{.Tgt}}[{{.TgtIdx}}:]
  __src_slice := {{.Src}}[{{.SrcStart}}:{{.SrcEnd}}]
  for __i := range __src_slice {
    __tgt_slice[__i].Set(&__src_slice[__i])
  }
}`
	default:
		tmpl = `copy({{.Tgt}}[{{.TgtIdx}}:], {{.Src}}[{{.SrcStart}}:{{.SrcEnd}}])`
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
		panic(err)
	}
	return buf.String()
}

// AppendSlice is a function that should only be used in templates.
func (t T) AppendSlice(target, src, destIdx, srcStartIdx, srcEndIdx string) string {
	var tmpl string
	switch t {
	case Bytes:
		tmpl = `{{.Tgt}}.AppendSlice({{.Src}}, {{.TgtIdx}}, {{.SrcStart}}, {{.SrcEnd}})`
	case Decimal:
		tmpl = `{
  __desiredCap := {{.TgtIdx}} + {{.SrcEnd}} - {{.SrcStart}}
  if cap({{.Tgt}}) >= __desiredCap {
  	{{.Tgt}} = {{.Tgt}}[:__desiredCap]
  } else {
    __new_slice := make([]apd.Decimal, __desiredCap)
    copy(__new_slice, {{.Tgt}})
    {{.Tgt}} = __new_slice
  }
  __src_slice := {{.Src}}[{{.SrcStart}}:{{.SrcEnd}}]
  __dst_slice := {{.Tgt}}[{{.TgtIdx}}:]
  for __i := range __src_slice {
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
		panic(err)
	}
	return buf.String()
}

// AppendVal is a function that should only be used in templates.
func (t T) AppendVal(target, v string) string {
	switch t {
	case Bytes:
		return fmt.Sprintf("%s.AppendVal(%s)", target, v)
	case Decimal:
		return fmt.Sprintf(`%[1]s = append(%[1]s, apd.Decimal{})
%[1]s[len(%[1]s)-1].Set(&%[2]s)`, target, v)
	}
	return fmt.Sprintf("%[1]s = append(%[1]s, %[2]s)", target, v)
}

// Len is a function that should only be used in templates.
func (t T) Len(target string) string {
	if t == Bytes {
		return fmt.Sprintf("%s.Len()", target)
	}
	return fmt.Sprintf("len(%s)", target)
}

// Range is a function that should only be used in templates.
func (t T) Range(loopVariableIdent string, target string) string {
	if t == Bytes {
		return fmt.Sprintf("%[1]s := 0; %[1]s < %[2]s.Len(); %[1]s++", loopVariableIdent, target)
	}
	return fmt.Sprintf("%[1]s := range %[2]s", loopVariableIdent, target)
}

// Zero is a function that should only be used in templates.
func (t T) Zero(target string) string {
	switch t {
	case Bytes:
		return fmt.Sprintf("%s.Zero()", target)
	case Decimal:
		return fmt.Sprintf(`for n := 0; n < len(%[1]s); n++ {
    %[1]s[n].SetInt64(0)
}`, target)
	}
	return fmt.Sprintf("for n := 0; n < len(%[1]s); n += copy(%[1]s[n:], zero%sColumn) {}", target, t.String())
}
