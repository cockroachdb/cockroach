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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package parser

import (
	"fmt"
	"go/constant"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/util/decimal"
	"gopkg.in/inf.v0"
)

// Constant is an constant literal expression which may be resolved to more than one type.
type Constant interface {
	Expr
	AvailableTypes() []Datum
	ResolveAsType(Datum) (TypedExpr, error)
}

var _ Constant = &NumVal{}
var _ Constant = &StrVal{}

func isNumericConstant(expr Expr) bool {
	if _, ok := expr.(*NumVal); ok {
		return true
	}
	return false
}

func typeCheckConstant(c Constant, desired Datum) (TypedExpr, error) {
	avail := c.AvailableTypes()
	if desired != nil {
		for _, typ := range avail {
			if desired.TypeEqual(typ) {
				return c.ResolveAsType(desired)
			}
		}
	}

	natural := avail[0]
	return c.ResolveAsType(natural)
}

func naturalConstantType(c Constant) Datum {
	return c.AvailableTypes()[0]
}

func canConstantBecome(c Constant, typ Datum) bool {
	avail := c.AvailableTypes()
	for _, availTyp := range avail {
		if availTyp.TypeEqual(typ) {
			return true
		}
	}
	return false
}

func shouldConstantBecome(c Constant, typ Datum) bool {
	if num, ok := c.(*NumVal); ok {
		if typ.TypeEqual(DummyInt) && num.Kind() == constant.Float {
			return false
		}
	}
	return canConstantBecome(c, typ)
}

// NumVal represents a constant numeric value.
type NumVal struct {
	constant.Value

	// We preserve the "original" string representation (before folding and normalization).
	OrigString string
}

func (expr *NumVal) String() string {
	if expr.OrigString != "" {
		return expr.OrigString
	}
	return expr.Value.String()
}

// canBeInt64 checks if it's possible for the value to become an int64:
//  1   = yes
//  1.0 = yes
//  1.1 = no
//  123...overflow...456 = no
func (expr *NumVal) canBeInt64() bool {
	_, err := expr.asInt()
	return err == nil
}

// shouldBeInt64 checks if the value naturally is an int64:
//  1   = yes
//  1.0 = no
//  1.1 = no
//  123...overflow...456 = no
//
// Currently unused so commented out, but useful even just for
// its documentation value.
// func (expr *NumVal) shouldBeInt64() bool {
// 	return expr.Kind() == constant.Int && expr.canBeInt64()
// }

// asInt returns the value as an integer if possible, or returns an
// error if not possible.
func (expr *NumVal) asInt() (int, error) {
	intVal := constant.ToInt(expr.Value)
	if intVal.Kind() == constant.Unknown {
		return 0, fmt.Errorf("cannot represent %v as an int", expr.Value)
	}
	i, exact := constant.Int64Val(intVal)
	if !exact {
		return 0, fmt.Errorf("representing %v as an int would overflow", intVal)
	}
	return int(i), nil
}

var numValAvailIntFloatDec = []Datum{DummyInt, DummyFloat, DummyDecimal}
var numValAvailFloatIntDec = []Datum{DummyFloat, DummyInt, DummyDecimal}
var numValAvailFloatDec = numValAvailIntFloatDec[1:]

// var numValAvailDec = numValAvailIntFloatDec[2:]

// AvailableTypes implements the Constant interface.
func (expr *NumVal) AvailableTypes() []Datum {
	switch {
	case expr.canBeInt64():
		if expr.Kind() == constant.Int {
			return numValAvailIntFloatDec
		}
		return numValAvailFloatIntDec
	default:
		return numValAvailFloatDec
	}
}

// ResolveAsType implements the Constant interface.
func (expr *NumVal) ResolveAsType(typ Datum) (TypedExpr, error) {
	switch typ {
	case DummyInt:
		i, exact := constant.Int64Val(constant.ToInt(expr.Value))
		if !exact {
			return nil, fmt.Errorf("integer value out of range: %v", expr.Value)
		}
		return NewDInt(DInt(i)), nil
	case DummyFloat:
		f, _ := constant.Float64Val(constant.ToFloat(expr.Value))
		return NewDFloat(DFloat(f)), nil
	case DummyDecimal:
		dd := &DDecimal{}
		s := expr.ExactString()
		if idx := strings.IndexRune(s, '/'); idx != -1 {
			// Handle constant.ratVal, which will return a rational string
			// like 6/7. If only we could call big.Rat.FloatString() on it...
			num, den := s[:idx], s[idx+1:]
			if _, ok := dd.SetString(num); !ok {
				return nil, fmt.Errorf("could not evaluate numerator of %v as Datum type DDecimal from string %q", expr, num)
			}
			denDec := new(inf.Dec)
			if _, ok := denDec.SetString(den); !ok {
				return nil, fmt.Errorf("could not evaluate denominator %v as Datum type DDecimal from string %q", expr, den)
			}
			dd.QuoRound(&dd.Dec, denDec, decimal.Precision, inf.RoundHalfUp)

			// Get rid of trailing zeros. We probaby want to remove this
			if s = dd.Dec.String(); strings.ContainsRune(s, '.') {
				for {
					switch s[len(s)-1] {
					case '0':
						s = s[:len(s)-1]
						continue
					case '.':
						s = s[:len(s)-1]
					}
					break
				}
				if _, ok := dd.SetString(s); !ok {
					return nil, fmt.Errorf("could not evaluate %v as Datum type DDecimal from string %q", expr, s)
				}
			}
		} else {
			if _, ok := dd.SetString(s); !ok {
				return nil, fmt.Errorf("could not evaluate %v as Datum type DDecimal from string %q", expr, s)
			}
		}
		return dd, nil
	default:
		return nil, fmt.Errorf("could not resolve %T %v into a %T", expr, expr, typ)
	}
}

var numValTypePriority = []Datum{DummyInt, DummyFloat, DummyDecimal}

// commonNumericConstantType returns the best constant type...
func commonNumericConstantType(vals ...*NumVal) Datum {
	bestType := 0
	for _, c := range vals {
		for {
			// This will not work if the available types are not strictly
			// supersets of their previous types in order of preference.
			if shouldConstantBecome(c, numValTypePriority[bestType]) {
				break
			}
			bestType++
			if bestType == len(numValTypePriority)-1 {
				return numValTypePriority[bestType]
			}
		}
	}
	return numValTypePriority[bestType]
}

// StrVal represents a constant string value.
type StrVal struct {
	s        string
	bytesEsc bool
}

func (expr *StrVal) String() string {
	if expr.bytesEsc {
		return encodeSQLBytes(expr.s)
	}
	return encodeSQLString(expr.s)
}

var strValAvailStringBytes = []Datum{DummyString, DummyBytes}
var strValAvailBytesString = []Datum{DummyBytes, DummyString}
var strValAvailBytes = strValAvailBytesString[:1]

// AvailableTypes implements the Constant interface.
func (expr *StrVal) AvailableTypes() []Datum {
	if !expr.bytesEsc {
		return strValAvailStringBytes
	}
	if utf8.ValidString(expr.s) {
		return strValAvailBytesString
	}
	return strValAvailBytes
}

// ResolveAsType implements the Constant interface.
func (expr *StrVal) ResolveAsType(typ Datum) (TypedExpr, error) {
	switch typ {
	case DummyString:
		return NewDString(expr.s), nil
	case DummyBytes:
		return NewDBytes(DBytes(expr.s)), nil
	default:
		return nil, fmt.Errorf("could not resolve %T %v into a %T", expr, expr, typ)
	}
}
