// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lang

import "bytes"

// AnyDataType is a data type about which nothing is known, and so could be any
// data type. Among other uses, it is assigned to custom functions:
//
//   (Scan $def:*) => (ConstrainScan $def)
//
// The ConstrainScan custom function has the AnyDataType assigned to it, since
// the return type of the function is not known.
var AnyDataType = &ExternalDataType{Name: "<any>"}

// ListDataType indicates that a pattern matches or constructs a list of
// expressions. For example:
//
//   (Tuple $list:[ $item:* ]) => $item
//
// The $list binding will have the ListDataType.
var ListDataType = &ExternalDataType{Name: "<list>"}

// StringDataType is a singleton instance of the "string" external data type.
var StringDataType = &ExternalDataType{Name: "<string>"}

// Int64DataType is a singleton instance of the "int64" external data type.
var Int64DataType = &ExternalDataType{Name: "<int64>"}

// DataType is a marker interface implemented by structs that describe the kind
// of data that will be returned by an expression in a match or replace pattern.
// See DataType implementations for more specific information.
type DataType interface {
	dataType()
	String() string
}

// DefineSetDataType indicates that a pattern matches or constructs one of
// several possible defined operators. For example:
//
//   (Eq | Ne $left:* $right:*) => (True)
//
// The top-level match pattern would have a DefineSetDataType that referenced
// the defines for the Eq and Ne operators.
type DefineSetDataType struct {
	Defines DefineSetExpr
}

// dataType is part of the DataType interface.
func (d *DefineSetDataType) dataType() {}

// String is part of the DataType interface.
func (d *DefineSetDataType) String() string {
	var buf bytes.Buffer
	if len(d.Defines) > 1 {
		buf.WriteRune('[')
	}
	for i, define := range d.Defines {
		if i != 0 {
			buf.WriteString(" | ")
		}
		buf.WriteString(string(define.Name))
	}
	if len(d.Defines) > 1 {
		buf.WriteRune(']')
	}
	return buf.String()
}

// ExternalDataType indicates that a pattern matches or constructs a non-
// operator type referenced in a Define. For example:
//
//   define Scan {
//     Def ScanDef
//   }
//
//   (Scan $def:*) => (ConstrainScan $def)
//
// Here, $def will have an ExternalDataType with Name equal to "ScanDef".
type ExternalDataType struct {
	Name string
}

// dataType is part of the DataType interface.
func (d *ExternalDataType) dataType() {}

// String is part of the DataType interface.
func (d *ExternalDataType) String() string {
	return d.Name
}

// DoTypesContradict returns true if two types cannot both be assigned to same
// expression.
func DoTypesContradict(dt1, dt2 DataType) bool {
	if dt1 == dt2 || dt1 == AnyDataType || dt2 == AnyDataType {
		return false
	}

	switch t1 := dt1.(type) {
	case *DefineSetDataType:
		switch t2 := dt2.(type) {
		case *DefineSetDataType:
			// Ensure that smaller set is a subset of the larger set.
			if len(t2.Defines) > len(t1.Defines) {
				t1, t2 = t2, t1
			}
			set := make(map[StringExpr]bool)
			for _, d := range t1.Defines {
				set[d.Name] = true
			}
			for _, d := range t2.Defines {
				if !set[d.Name] {
					return true
				}
			}
			return false

		case *ExternalDataType:
			// An operator type is not assignable to a list, string, or int64,
			// so return contradiction for builtin types. However, an operator
			// type might be assignable to other external data types (e.g.
			// opt.Expr), so no conclusions can be drawn there.
			return IsBuiltinType(dt2)
		}

	case *ExternalDataType:
		switch t2 := dt2.(type) {
		case *DefineSetDataType:
			// See above comment for flipped case.
			return IsBuiltinType(dt1)

		case *ExternalDataType:
			// If neither or both types are builtin, require names to match,
			// since it's known that list, string, and int64 types are not
			// assignable to one another.
			if !IsBuiltinType(dt1) && !IsBuiltinType(dt2) {
				return t1.Name != t2.Name
			}
			if IsBuiltinType(dt1) && IsBuiltinType(dt2) {
				return t1.Name != t2.Name
			}

			// Otherwise, assume types are compatible, since nothing more is
			// known about the external data type.
			return false
		}
	}
	panic("unhandled data type")
}

// IsBuiltinType returns true if the data type is one of List, String, or Int64.
func IsBuiltinType(dt DataType) bool {
	return dt == ListDataType || dt == StringDataType || dt == Int64DataType
}

// IsTypeMoreRestrictive returns true if the first data type gives more complete
// and detailed information than the second.
func IsTypeMoreRestrictive(dt1, dt2 DataType) bool {
	switch t1 := dt1.(type) {
	case *DefineSetDataType:
		if t2, ok := dt2.(*DefineSetDataType); ok {
			// More restrictive data type is assumed to be one that matches or
			// constructs fewer possible operators.
			return len(t1.Defines) < len(t2.Defines)
		}

		// DefineSetDataType is more restrictive than other concrete data types.
		return true

	case *ExternalDataType:
		if dt1 == AnyDataType {
			return false
		}
		if dt1 == ListDataType {
			return dt2 == AnyDataType
		}
		return dt2 == ListDataType || dt2 == AnyDataType
	}
	panic("unhandled data type")
}
