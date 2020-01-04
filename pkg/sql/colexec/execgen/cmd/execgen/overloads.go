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
	"errors"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var binaryOpName = map[tree.BinaryOperator]string{
	tree.Plus:  "Plus",
	tree.Minus: "Minus",
	tree.Mult:  "Mult",
	tree.Div:   "Div",
}

var comparisonOpName = map[tree.ComparisonOperator]string{
	tree.EQ: "EQ",
	tree.NE: "NE",
	tree.LT: "LT",
	tree.LE: "LE",
	tree.GT: "GT",
	tree.GE: "GE",
}

var binaryOpInfix = map[tree.BinaryOperator]string{
	tree.Plus:  "+",
	tree.Minus: "-",
	tree.Mult:  "*",
	tree.Div:   "/",
}

var binaryOpDecMethod = map[tree.BinaryOperator]string{
	tree.Plus:  "Add",
	tree.Minus: "Sub",
	tree.Mult:  "Mul",
	tree.Div:   "Quo",
}

var binaryOpDecCtx = map[tree.BinaryOperator]string{
	tree.Plus:  "ExactCtx",
	tree.Minus: "ExactCtx",
	tree.Mult:  "ExactCtx",
	tree.Div:   "DecimalCtx",
}

var comparisonOpInfix = map[tree.ComparisonOperator]string{
	tree.EQ: "==",
	tree.NE: "!=",
	tree.LT: "<",
	tree.LE: "<=",
	tree.GT: ">",
	tree.GE: ">=",
}

type overload struct {
	Name string
	// Only one of CmpOp and BinOp will be set, depending on whether the overload
	// is a binary operator or a comparison operator.
	CmpOp tree.ComparisonOperator
	BinOp tree.BinaryOperator
	// OpStr is the string form of whichever of CmpOp and BinOp are set.
	OpStr     string
	LTyp      coltypes.T
	RTyp      coltypes.T
	LGoType   string
	RGoType   string
	RetTyp    coltypes.T
	RetGoType string

	AssignFunc  assignFunc
	CompareFunc compareFunc

	// TODO(solon): These would not be necessary if we changed the zero values of
	// ComparisonOperator and BinaryOperator to be invalid.
	IsCmpOp  bool
	IsBinOp  bool
	IsHashOp bool
}

type assignFunc func(op overload, target, l, r string) string
type compareFunc func(target, l, r string) string

var binaryOpOverloads []*overload
var comparisonOpOverloads []*overload

// sameTypeBinaryOpToOverloads maps a binary operator to all of the overloads
// that implement the operator between two values of the same type.
var sameTypeBinaryOpToOverloads map[tree.BinaryOperator][]*overload

// anyTypeBinaryOpToOverloads maps a binary operator to all of the overloads
// that implement it, including all mixed input types. It also includes all
// entries from sameTypeBinaryOpToOverloads.
var anyTypeBinaryOpToOverloads map[tree.BinaryOperator][]*overload

// sameTypeComparisonOpToOverloads maps a comparison operator to all of the
// overloads that implement that comparison between two values of the same type.
var sameTypeComparisonOpToOverloads map[tree.ComparisonOperator][]*overload

// anyTypeComparisonOpToOverloads maps a comparison operator to all of the
// overloads that implement it, including all mixed type comparisons. It also
// includes all entries from sameTypeComparisonOpToOverloads.
var anyTypeComparisonOpToOverloads map[tree.ComparisonOperator][]*overload

// hashOverloads is a list of all of the overloads that implement the hash
// operation.
var hashOverloads []*overload

// Assign produces a Go source string that assigns the "target" variable to the
// result of applying the overload to the two inputs, l and r.
//
// For example, an overload that implemented the float64 plus operation, when fed
// the inputs "x", "a", "b", would produce the string "x = a + b".
func (o overload) Assign(target, l, r string) string {
	if o.AssignFunc != nil {
		if ret := o.AssignFunc(o, target, l, r); ret != "" {
			return ret
		}
	}
	// Default assign form assumes an infix operator.
	return fmt.Sprintf("%s = %s %s %s", target, l, o.OpStr, r)
}

// Compare produces a Go source string that assigns the "target" variable to the
// result of comparing the two inputs, l and r. The target will be negative,
// zero, or positive depending on whether l is less-than, equal-to, or
// greater-than r.
func (o overload) Compare(target, l, r string) string {
	if o.CompareFunc != nil {
		if ret := o.CompareFunc(target, l, r); ret != "" {
			return ret
		}
	}
	// Default compare form assumes an infix operator.
	return fmt.Sprintf(
		"if %s < %s { %s = -1 } else if %s > %s { %s = 1 } else { %s = 0 }",
		l, r, target, l, r, target, target)
}

func (o overload) UnaryAssign(target, v string) string {
	if o.AssignFunc != nil {
		if ret := o.AssignFunc(o, target, v, ""); ret != "" {
			return ret
		}
	}
	// Default assign form assumes a function operator.
	return fmt.Sprintf("%s = %s(%s)", target, o.OpStr, v)
}

type castOverload struct {
	FromTyp    coltypes.T
	ToTyp      coltypes.T
	ToGoTyp    string
	AssignFunc castAssignFunc
}

func (o castOverload) Assign(to, from string) string {
	return o.AssignFunc(to, from)
}

type castAssignFunc func(to, from string) string

func castIdentity(to, from string) string {
	return fmt.Sprintf("%s = %s", to, from)
}

func intToDecimal(to, from string) string {
	convStr := `
    %[1]s = *apd.New(int64(%[2]s), 0)
  `
	return fmt.Sprintf(convStr, to, from)
}

func intToFloat(floatSize int) func(string, string) string {
	return func(to, from string) string {
		convStr := `
			%[1]s = float%[3]d(%[2]s)
			`
		return fmt.Sprintf(convStr, to, from, floatSize)
	}
}

func intToInt32(to, from string) string {
	convStr := `
    %[1]s = int32(%[2]s)
  `
	return fmt.Sprintf(convStr, to, from)
}

func intToInt64(to, from string) string {
	convStr := `
    %[1]s = int64(%[2]s)
  `
	return fmt.Sprintf(convStr, to, from)
}

func floatToInt(intSize int, floatSize int) func(string, string) string {
	return func(to, from string) string {
		convStr := `
			if math.IsNaN(float64(%[2]s)) || %[2]s <= float%[4]d(math.MinInt%[3]d) || %[2]s >= float%[4]d(math.MaxInt%[3]d) {
				execerror.NonVectorizedPanic(tree.ErrIntOutOfRange)
			}
			%[1]s = int%[3]d(%[2]s)
		`
		return fmt.Sprintf(convStr, to, from, intSize, floatSize)
	}
}

func numToBool(to, from string) string {
	convStr := `
		%[1]s = %[2]s != 0
	`
	return fmt.Sprintf(convStr, to, from)
}

func floatToDecimal(to, from string) string {
	convStr := `
		{
			var tmpDec apd.Decimal
			_, tmpErr := tmpDec.SetFloat64(float64(%[2]s))
    	if tmpErr != nil {
    	  execerror.NonVectorizedPanic(tmpErr)
    	}
			%[1]s = tmpDec
		}
	`
	return fmt.Sprintf(convStr, to, from)
}

var castOverloads map[coltypes.T][]castOverload

func init() {
	registerTypeCustomizers()
	registerBinOpOutputTypes()

	// Build overload definitions for basic coltypes.
	inputTypes := coltypes.AllTypes
	binOps := []tree.BinaryOperator{tree.Plus, tree.Minus, tree.Mult, tree.Div}
	cmpOps := []tree.ComparisonOperator{tree.EQ, tree.NE, tree.LT, tree.LE, tree.GT, tree.GE}
	sameTypeBinaryOpToOverloads = make(map[tree.BinaryOperator][]*overload, len(binaryOpName))
	anyTypeBinaryOpToOverloads = make(map[tree.BinaryOperator][]*overload, len(binaryOpName))
	sameTypeComparisonOpToOverloads = make(map[tree.ComparisonOperator][]*overload, len(comparisonOpName))
	anyTypeComparisonOpToOverloads = make(map[tree.ComparisonOperator][]*overload, len(comparisonOpName))
	for _, leftType := range inputTypes {
		for _, rightType := range coltypes.CompatibleTypes[leftType] {
			customizer := typeCustomizers[coltypePair{leftType, rightType}]
			for _, op := range binOps {
				// Skip types that don't have associated binary ops.
				retType, ok := binOpOutputTypes[op][coltypePair{leftType, rightType}]
				if !ok {
					continue
				}
				ov := &overload{
					Name:      binaryOpName[op],
					BinOp:     op,
					IsBinOp:   true,
					OpStr:     binaryOpInfix[op],
					LTyp:      leftType,
					RTyp:      rightType,
					LGoType:   leftType.GoTypeName(),
					RGoType:   rightType.GoTypeName(),
					RetTyp:    retType,
					RetGoType: retType.GoTypeName(),
				}
				if customizer != nil {
					if b, ok := customizer.(binOpTypeCustomizer); ok {
						ov.AssignFunc = b.getBinOpAssignFunc()
					}
				}
				binaryOpOverloads = append(binaryOpOverloads, ov)
				anyTypeBinaryOpToOverloads[op] = append(anyTypeBinaryOpToOverloads[op], ov)
				if leftType == rightType {
					sameTypeBinaryOpToOverloads[op] = append(sameTypeBinaryOpToOverloads[op], ov)
				}
			}
		}
		for _, rightType := range coltypes.ComparableTypes[leftType] {
			customizer := typeCustomizers[coltypePair{leftType, rightType}]
			for _, op := range cmpOps {
				opStr := comparisonOpInfix[op]
				ov := &overload{
					Name:      comparisonOpName[op],
					CmpOp:     op,
					IsCmpOp:   true,
					OpStr:     opStr,
					LTyp:      leftType,
					RTyp:      rightType,
					LGoType:   leftType.GoTypeName(),
					RGoType:   rightType.GoTypeName(),
					RetTyp:    coltypes.Bool,
					RetGoType: coltypes.Bool.GoTypeName(),
				}
				if customizer != nil {
					if b, ok := customizer.(cmpOpTypeCustomizer); ok {
						ov.AssignFunc = func(op overload, target, l, r string) string {
							cmp := b.getCmpOpCompareFunc()("cmpResult", l, r)
							if cmp == "" {
								return ""
							}
							args := map[string]string{"Target": target, "Cmp": cmp, "Op": op.OpStr}
							buf := strings.Builder{}
							t := template.Must(template.New("").Parse(`
								{
									var cmpResult int
									{{.Cmp}}
									{{.Target}} = cmpResult {{.Op}} 0
								}
							`))
							if err := t.Execute(&buf, args); err != nil {
								execerror.VectorizedInternalPanic(err)
							}
							return buf.String()
						}
						ov.CompareFunc = b.getCmpOpCompareFunc()
					}
				}
				comparisonOpOverloads = append(comparisonOpOverloads, ov)
				anyTypeComparisonOpToOverloads[op] = append(anyTypeComparisonOpToOverloads[op], ov)
				if leftType == rightType {
					sameTypeComparisonOpToOverloads[op] = append(sameTypeComparisonOpToOverloads[op], ov)
				}
			}
		}
		sameTypeCustomizer := typeCustomizers[coltypePair{leftType, leftType}]
		ov := &overload{
			IsHashOp: true,
			LTyp:     leftType,
			OpStr:    "uint64",
		}
		if sameTypeCustomizer != nil {
			if b, ok := sameTypeCustomizer.(hashTypeCustomizer); ok {
				ov.AssignFunc = b.getHashAssignFunc()
			}
		}
		hashOverloads = append(hashOverloads, ov)
	}

	// Build cast overloads. We omit cases of type casts that we do not support.
	castOverloads = make(map[coltypes.T][]castOverload)
	for _, from := range inputTypes {
		switch from {
		case coltypes.Bool:
			for _, to := range inputTypes {
				ov := castOverload{FromTyp: from, ToTyp: to, ToGoTyp: to.GoTypeName()}
				switch to {
				case coltypes.Bool:
					ov.AssignFunc = castIdentity
				case coltypes.Int16, coltypes.Int32,
					coltypes.Int64, coltypes.Float64:
					ov.AssignFunc = func(to, from string) string {
						convStr := `
							%[1]s = 0
							if %[2]s {
								%[1]s = 1
							}
						`
						return fmt.Sprintf(convStr, to, from)
					}
				}
				castOverloads[from] = append(castOverloads[from], ov)
			}
		case coltypes.Bytes:
			// TODO (rohany): It's unclear what to do here in the bytes case.
			// There are different conversion rules for the multiple things
			// that a bytes type can implemented, but we don't know each of the
			// things is contained here. Additionally, we don't really know
			// what to do even if it is a bytes to bytes operation here.
			for _, to := range inputTypes {
				ov := castOverload{FromTyp: from, ToTyp: to, ToGoTyp: to.GoTypeName()}
				switch to {
				}
				castOverloads[from] = append(castOverloads[from], ov)
			}
		case coltypes.Decimal:
			for _, to := range inputTypes {
				ov := castOverload{FromTyp: from, ToTyp: to, ToGoTyp: to.GoTypeName()}
				switch to {
				case coltypes.Bool:
					ov.AssignFunc = func(to, from string) string {
						convStr := `
							%[1]s = %[2]s.Sign() != 0
						`
						return fmt.Sprintf(convStr, to, from)
					}
				case coltypes.Decimal:
					ov.AssignFunc = castIdentity
				}
				castOverloads[from] = append(castOverloads[from], ov)
			}
		case coltypes.Int16:
			for _, to := range inputTypes {
				ov := castOverload{FromTyp: from, ToTyp: to, ToGoTyp: to.GoTypeName()}
				switch to {
				case coltypes.Bool:
					ov.AssignFunc = numToBool
				case coltypes.Decimal:
					ov.AssignFunc = intToDecimal
				case coltypes.Int16:
					ov.AssignFunc = castIdentity
				case coltypes.Int32:
					ov.AssignFunc = intToInt32
				case coltypes.Int64:
					ov.AssignFunc = intToInt64
				case coltypes.Float64:
					ov.AssignFunc = intToFloat(64)
				}
				castOverloads[from] = append(castOverloads[from], ov)
			}
		case coltypes.Int32:
			for _, to := range inputTypes {
				ov := castOverload{FromTyp: from, ToTyp: to, ToGoTyp: to.GoTypeName()}
				switch to {
				case coltypes.Bool:
					ov.AssignFunc = numToBool
				case coltypes.Decimal:
					ov.AssignFunc = intToDecimal
				case coltypes.Int32:
					ov.AssignFunc = castIdentity
				case coltypes.Int64:
					ov.AssignFunc = intToInt64
				case coltypes.Float64:
					ov.AssignFunc = intToFloat(64)
				}
				castOverloads[from] = append(castOverloads[from], ov)
			}
		case coltypes.Int64:
			for _, to := range inputTypes {
				ov := castOverload{FromTyp: from, ToTyp: to, ToGoTyp: to.GoTypeName()}
				switch to {
				case coltypes.Bool:
					ov.AssignFunc = numToBool
				case coltypes.Decimal:
					ov.AssignFunc = intToDecimal
				case coltypes.Int64:
					ov.AssignFunc = castIdentity
				case coltypes.Float64:
					ov.AssignFunc = intToFloat(64)
				}
				castOverloads[from] = append(castOverloads[from], ov)
			}
		case coltypes.Float64:
			for _, to := range inputTypes {
				ov := castOverload{FromTyp: from, ToTyp: to, ToGoTyp: to.GoTypeName()}
				switch to {
				case coltypes.Bool:
					ov.AssignFunc = numToBool
				case coltypes.Decimal:
					ov.AssignFunc = floatToDecimal
				case coltypes.Int16:
					ov.AssignFunc = floatToInt(16, 64)
				case coltypes.Int32:
					ov.AssignFunc = floatToInt(32, 64)
				case coltypes.Int64:
					ov.AssignFunc = floatToInt(64, 64)
				case coltypes.Float64:
					ov.AssignFunc = castIdentity
				}
				castOverloads[from] = append(castOverloads[from], ov)
			}
		}
	}
}

// typeCustomizer is a marker interface for something that implements one or
// more of binOpTypeCustomizer and cmpOpTypeCustomizer.
//
// A type customizer allows custom templating behavior for a particular type
// that doesn't permit the ordinary Go assignment (x = y), comparison
// (==, <, etc) or binary operator (+, -, etc) semantics.
type typeCustomizer interface{}

// coltypePair is used to key a map that holds all typeCustomizers.
type coltypePair struct {
	leftType  coltypes.T
	rightType coltypes.T
}

var typeCustomizers map[coltypePair]typeCustomizer

var binOpOutputTypes map[tree.BinaryOperator]map[coltypePair]coltypes.T

// registerTypeCustomizer registers a particular type customizer to a
// pair of types, for usage by templates.
func registerTypeCustomizer(pair coltypePair, customizer typeCustomizer) {
	typeCustomizers[pair] = customizer
}

// binOpTypeCustomizer is a type customizer that changes how the templater
// produces binary operator output for a particular type.
type binOpTypeCustomizer interface {
	getBinOpAssignFunc() assignFunc
}

// cmpOpTypeCustomizer is a type customizer that changes how the templater
// produces comparison operator output for a particular type.
type cmpOpTypeCustomizer interface {
	getCmpOpCompareFunc() compareFunc
}

// hashTypeCustomizer is a type customizer that changes how the templater
// produces hash output for a particular type.
type hashTypeCustomizer interface {
	getHashAssignFunc() assignFunc
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
type floatCustomizer struct{ width int }

// intCustomizers are used for hash functions and overflow handling.
type intCustomizer struct{ width int }

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

// floatIntervalCustomizer supports mixed type expression with a float
// left-hand side and an interval right-hand side.
type floatIntervalCustomizer struct{}

// intervalDecimalCustomizer supports mixed type expression with an interval
// left-hand side and a decimal right-hand side.
type intervalDecimalCustomizer struct{}

// decimalIntervalCustomizer supports mixed type expression with a decimal
// left-hand side and an interval right-hand side.
type decimalIntervalCustomizer struct{}

func (boolCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(target, l, r string) string {
		args := map[string]string{"Target": target, "Left": l, "Right": r}
		buf := strings.Builder{}
		// Inline the code from tree.CompareBools
		t := template.Must(template.New("").Parse(`
			if !{{.Left}} && {{.Right}} {
				{{.Target}} = -1
			}	else if {{.Left}} && !{{.Right}} {
				{{.Target}} = 1
			}	else {
				{{.Target}} = 0
			}
		`))

		if err := t.Execute(&buf, args); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return buf.String()
	}
}

func (boolCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf(`
			x := 0
			if %[2]s {
    		x = 1
			}
			%[1]s = %[1]s*31 + uintptr(x)
		`, target, v)
	}
}

func (bytesCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(target, l, r string) string {
		return fmt.Sprintf("%s = bytes.Compare(%s, %s)", target, l, r)
	}
}

// hashByteSliceString is a templated code for hashing a byte slice. It is
// meant to be used as a format string for fmt.Sprintf with the first argument
// being the "target" (i.e. what variable to assign the hash to) and the second
// argument being the "value" (i.e. what is the name of a byte slice variable).
const hashByteSliceString = `
			sh := (*reflect.SliceHeader)(unsafe.Pointer(&%[2]s))
			%[1]s = memhash(unsafe.Pointer(sh.Data), %[1]s, uintptr(len(%[2]s)))
`

func (bytesCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf(hashByteSliceString, target, v)
	}
}

func (decimalCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(target, l, r string) string {
		return fmt.Sprintf("%s = tree.CompareDecimals(&%s, &%s)", target, l, r)
	}
}

func (decimalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		if op.BinOp == tree.Div {
			return fmt.Sprintf(`
			{
				cond, err := tree.%s.%s(&%s, &%s, &%s)
				if cond.DivisionByZero() {
					execerror.NonVectorizedPanic(tree.ErrDivByZero)
				}
				if err != nil {
					execerror.NonVectorizedPanic(err)
				}
			}
			`, binaryOpDecCtx[op.BinOp], binaryOpDecMethod[op.BinOp], target, l, r)
		}
		return fmt.Sprintf("if _, err := tree.%s.%s(&%s, &%s, &%s); err != nil { execerror.NonVectorizedPanic(err) }",
			binaryOpDecCtx[op.BinOp], binaryOpDecMethod[op.BinOp], target, l, r)
	}
}

func (decimalCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf(`
			// In order for equal decimals to hash to the same value we need to
			// remove the trailing zeroes if there are any.
			tmpDec := &decimalScratch.tmpDec1
			tmpDec.Reduce(&%[1]s)
			b := []byte(tmpDec.String())`, v) +
			fmt.Sprintf(hashByteSliceString, target, "b")
	}
}

func (c floatCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		// TODO(yuzefovich): think through whether this is appropriate way to hash
		// NaNs.
		return fmt.Sprintf(
			`
			f := %[2]s
			if math.IsNaN(float64(f)) {
				f = 0
			}
			%[1]s = f%[3]dhash(noescape(unsafe.Pointer(&f)), %[1]s)
`, target, v, c.width)
	}
}

func getFloatCmpOpCompareFunc(checkLeftNan, checkRightNan bool) compareFunc {
	return func(target, l, r string) string {
		args := map[string]interface{}{
			"Target":        target,
			"Left":          l,
			"Right":         r,
			"CheckLeftNan":  checkLeftNan,
			"CheckRightNan": checkRightNan}
		buf := strings.Builder{}
		// In SQL, NaN is treated as less than all other float values. In Go, any
		// comparison with NaN returns false. To allow floats of different sizes to
		// be compared, always upcast to float64. The CheckLeftNan and CheckRightNan
		// flags skip NaN checks when the input is an int (which is necessary to
		// pass linting.)
		t := template.Must(template.New("").Parse(`
			{
				a, b := float64({{.Left}}), float64({{.Right}})
				if a < b {
					{{.Target}} = -1
				} else if a > b {
					{{.Target}} = 1
				}	else if a == b {
					{{.Target}} = 0
				}	else if {{ if .CheckLeftNan }} math.IsNaN(a) {{ else }} false {{ end }} {
					if {{ if .CheckRightNan }} math.IsNaN(b) {{ else }} false {{ end }} {
						{{.Target}} = 0
					} else {
						{{.Target}} = -1
					}
				}	else {
					{{.Target}} = 1
				}
			}
		`))

		if err := t.Execute(&buf, args); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return buf.String()
	}
}

func (c floatCustomizer) getCmpOpCompareFunc() compareFunc {
	return getFloatCmpOpCompareFunc(true /* checkLeftNan */, true /* checkRightNan */)
}

func (c floatCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		// The float64 customizer handles binOps with floats of different widths (in
		// addition to handling float64-only arithmetic), so we must cast to float64
		// in this case.
		if c.width == 64 {
			return fmt.Sprintf("%s = float64(%s) %s float64(%s)", target, l, op.OpStr, r)
		}
		return fmt.Sprintf("%s = %s %s %s", target, l, op.OpStr, r)
	}
}

func (c intCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf(`
				// In order for integers with different widths but of the same value to
				// to hash to the same value, we upcast all of them to int64.
				asInt64 := int64(%[2]s)
				%[1]s = memhash64(noescape(unsafe.Pointer(&asInt64)), %[1]s)`,
			target, v)
	}
}

func (c intCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(target, l, r string) string {
		args := map[string]string{"Target": target, "Left": l, "Right": r}
		buf := strings.Builder{}
		// To allow ints of different sizes to be compared, always upcast to int64.
		t := template.Must(template.New("").Parse(`
			{
				a, b := int64({{.Left}}), int64({{.Right}})
				if a < b {
					{{.Target}} = -1
				} else if a > b {
					{{.Target}} = 1
				}	else {
					{{.Target}} = 0
				}
			}
		`))

		if err := t.Execute(&buf, args); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return buf.String()
	}
}

func (c intCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		args := map[string]string{"Target": target, "Left": l, "Right": r}
		// The int64 customizer handles binOps with integers of different widths (in
		// addition to handling int64-only arithmetic), so we must cast to int64 in
		// this case.
		if c.width == 64 {
			args["Left"] = fmt.Sprintf("int64(%s)", l)
			args["Right"] = fmt.Sprintf("int64(%s)", r)
		}
		buf := strings.Builder{}
		var t *template.Template

		switch op.BinOp {

		case tree.Plus:
			t = template.Must(template.New("").Parse(`
				{
					result := {{.Left}} + {{.Right}}
					if (result < {{.Left}}) != ({{.Right}} < 0) {
						execerror.NonVectorizedPanic(tree.ErrIntOutOfRange)
					}
					{{.Target}} = result
				}
			`))

		case tree.Minus:
			t = template.Must(template.New("").Parse(`
				{
					result := {{.Left}} - {{.Right}}
					if (result < {{.Left}}) != ({{.Right}} > 0) {
						execerror.NonVectorizedPanic(tree.ErrIntOutOfRange)
					}
					{{.Target}} = result
				}
			`))

		case tree.Mult:
			// If the inputs are small enough, then we don't have to do any further
			// checks. For the sake of legibility, upperBound and lowerBound are both
			// not set to their maximal/minimal values. An even more advanced check
			// (for positive values) might involve adding together the highest bit
			// positions of the inputs, and checking if the sum is less than the
			// integer width.
			var upperBound, lowerBound string
			switch c.width {
			case 16:
				upperBound = "math.MaxInt8"
				lowerBound = "math.MinInt8"
			case 32:
				upperBound = "math.MaxInt16"
				lowerBound = "math.MinInt16"
			case 64:
				upperBound = "math.MaxInt32"
				lowerBound = "math.MinInt32"
			default:
				execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled integer width %d", c.width))
			}

			args["UpperBound"] = upperBound
			args["LowerBound"] = lowerBound
			t = template.Must(template.New("").Parse(`
				{
					result := {{.Left}} * {{.Right}}
					if {{.Left}} > {{.UpperBound}} || {{.Left}} < {{.LowerBound}} || {{.Right}} > {{.UpperBound}} || {{.Right}} < {{.LowerBound}} {
						if {{.Left}} != 0 && {{.Right}} != 0 {
							sameSign := ({{.Left}} < 0) == ({{.Right}} < 0)
							if (result < 0) == sameSign {
								execerror.NonVectorizedPanic(tree.ErrIntOutOfRange)
							} else if result/{{.Right}} != {{.Left}} {
								execerror.NonVectorizedPanic(tree.ErrIntOutOfRange)
							}
						}
					}
					{{.Target}} = result
				}
			`))

		case tree.Div:
			// Note that this is the '/' operator, which has a decimal result.
			// TODO(rafi): implement the '//' floor division operator.
			args["Ctx"] = binaryOpDecCtx[op.BinOp]
			t = template.Must(template.New("").Parse(`
			{
				if {{.Right}} == 0 {
					execerror.NonVectorizedPanic(tree.ErrDivByZero)
				}
				leftTmpDec, rightTmpDec := &decimalScratch.tmpDec1, &decimalScratch.tmpDec2
				leftTmpDec.SetFinite(int64({{.Left}}), 0)
				rightTmpDec.SetFinite(int64({{.Right}}), 0)
				if _, err := tree.{{.Ctx}}.Quo(&{{.Target}}, leftTmpDec, rightTmpDec); err != nil {
					execerror.NonVectorizedPanic(err)
				}
			}
		`))

		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
		}

		if err := t.Execute(&buf, args); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return buf.String()
	}
}

func (c decimalFloatCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(target, l, r string) string {
		args := map[string]string{"Target": target, "Left": l, "Right": r}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				tmpDec := &decimalScratch.tmpDec1
				if _, err := tmpDec.SetFloat64(float64({{.Right}})); err != nil {
					execerror.NonVectorizedPanic(err)
				}
				{{.Target}} = tree.CompareDecimals(&{{.Left}}, tmpDec)
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return buf.String()
	}
}

func (c decimalIntCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(target, l, r string) string {
		args := map[string]string{"Target": target, "Left": l, "Right": r}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				tmpDec := &decimalScratch.tmpDec1
				tmpDec.SetFinite(int64({{.Right}}), 0)
				{{.Target}} = tree.CompareDecimals(&{{.Left}}, tmpDec)
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return buf.String()
	}
}

func (c decimalIntCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		isDivision := op.BinOp == tree.Div
		args := map[string]interface{}{
			"Ctx":        binaryOpDecCtx[op.BinOp],
			"Op":         binaryOpDecMethod[op.BinOp],
			"IsDivision": isDivision,
			"Target":     target, "Left": l, "Right": r,
		}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				{{ if .IsDivision }}
				if {{.Right}} == 0 {
					execerror.NonVectorizedPanic(tree.ErrDivByZero)
				}
				{{ end }}
				tmpDec := &decimalScratch.tmpDec1
				tmpDec.SetFinite(int64({{.Right}}), 0)
				if _, err := tree.{{.Ctx}}.{{.Op}}(&{{.Target}}, &{{.Left}}, tmpDec); err != nil {
					execerror.NonVectorizedPanic(err)
				}
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return buf.String()
	}
}

func (c floatDecimalCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(target, l, r string) string {
		args := map[string]string{"Target": target, "Left": l, "Right": r}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				tmpDec := &decimalScratch.tmpDec1
				if _, err := tmpDec.SetFloat64(float64({{.Left}})); err != nil {
					execerror.NonVectorizedPanic(err)
				}
				{{.Target}} = tree.CompareDecimals(tmpDec, &{{.Right}})
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return buf.String()
	}
}

func (c intDecimalCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(target, l, r string) string {
		args := map[string]string{"Target": target, "Left": l, "Right": r}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				tmpDec := &decimalScratch.tmpDec1
				tmpDec.SetFinite(int64({{.Left}}), 0)
				{{.Target}} = tree.CompareDecimals(tmpDec, &{{.Right}})
			}
		`))

		if err := t.Execute(&buf, args); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return buf.String()
	}
}

func (c intDecimalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		isDivision := op.BinOp == tree.Div
		args := map[string]interface{}{
			"Ctx":        binaryOpDecCtx[op.BinOp],
			"Op":         binaryOpDecMethod[op.BinOp],
			"IsDivision": isDivision,
			"Target":     target, "Left": l, "Right": r,
		}
		buf := strings.Builder{}
		t := template.Must(template.New("").Parse(`
			{
				tmpDec := &decimalScratch.tmpDec1
				tmpDec.SetFinite(int64({{.Left}}), 0)
				{{ if .IsDivision }}
				cond, err := tree.{{.Ctx}}.{{.Op}}(&{{.Target}}, tmpDec, &{{.Right}})
				if cond.DivisionByZero() {
					execerror.NonVectorizedPanic(tree.ErrDivByZero)
				}
				{{ else }}
				_, err := tree.{{.Ctx}}.{{.Op}}(&{{.Target}}, tmpDec, &{{.Right}})
				{{ end }}
				if err != nil {
					execerror.NonVectorizedPanic(err)
				}
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return buf.String()
	}
}

func (c floatIntCustomizer) getCmpOpCompareFunc() compareFunc {
	// floatCustomizer's comparison function can be reused since float-int
	// comparison works by casting the int.
	return getFloatCmpOpCompareFunc(true /* checkLeftNan */, false /* checkRightNan */)
}

func (c intFloatCustomizer) getCmpOpCompareFunc() compareFunc {
	// floatCustomizer's comparison function can be reused since int-float
	// comparison works by casting the int.
	return getFloatCmpOpCompareFunc(false /* checkLeftNan */, true /* checkRightNan */)
}

func (c timestampCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(target, l, r string) string {
		args := map[string]string{"Target": target, "Left": l, "Right": r}
		buf := strings.Builder{}
		// Inline the code from tree.compareTimestamps.
		t := template.Must(template.New("").Parse(`
      if {{.Left}}.Before({{.Right}}) {
				{{.Target}} = -1
			} else if {{.Right}}.Before({{.Left}}) {
				{{.Target}} = 1
			} else { 
        {{.Target}} = 0
      }`))

		if err := t.Execute(&buf, args); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return buf.String()
	}
}

func (c timestampCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf(`
		  s := %[2]s.UnixNano()
		  %[1]s = memhash64(noescape(unsafe.Pointer(&s)), %[1]s)
		`, target, v)
	}
}

func (c timestampCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.BinOp {
		case tree.Minus:
			return fmt.Sprintf(`
		  nanos := %[2]s.Sub(%[3]s).Nanoseconds()
		  %[1]s = duration.MakeDuration(nanos, 0, 0)
		  `,
				target, l, r)
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
		}
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (c intervalCustomizer) getCmpOpCompareFunc() compareFunc {
	return func(target, l, r string) string {
		return fmt.Sprintf("%s = %s.Compare(%s)", target, l, r)
	}
}

func (c intervalCustomizer) getHashAssignFunc() assignFunc {
	return func(op overload, target, v, _ string) string {
		return fmt.Sprintf(`
		  months, days, nanos := %[2]s.Months, %[2]s.Days, %[2]s.Nanos()
		  %[1]s = memhash64(noescape(unsafe.Pointer(&months)), %[1]s)
		  %[1]s = memhash64(noescape(unsafe.Pointer(&days)), %[1]s)
		  %[1]s = memhash64(noescape(unsafe.Pointer(&nanos)), %[1]s)
		`, target, v)
	}
}

func (c intervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.BinOp {
		case tree.Plus:
			return fmt.Sprintf(`%[1]s = %[2]s.Add(%[3]s)`,
				target, l, r)
		case tree.Minus:
			return fmt.Sprintf(`%[1]s = %[2]s.Sub(%[3]s)`,
				target, l, r)
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
		}
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (c timestampIntervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.BinOp {
		case tree.Plus:
			return fmt.Sprintf(`%[1]s = duration.Add(%[2]s, %[3]s)`,
				target, l, r)
		case tree.Minus:
			return fmt.Sprintf(`%[1]s = duration.Add(%[2]s, %[3]s.Mul(-1))`,
				target, l, r)
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
		}
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (c intervalTimestampCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.BinOp {
		case tree.Plus:
			return fmt.Sprintf(`%[1]s = duration.Add(%[3]s, %[2]s)`,
				target, l, r)
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
		}
		return ""
	}
}

func (c intervalIntCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.BinOp {
		case tree.Mult:
			return fmt.Sprintf(`%[1]s = %[2]s.Mul(int64(%[3]s))`,
				target, l, r)
		case tree.Div:
			return fmt.Sprintf(`
				if %[3]s == 0 {
					execerror.NonVectorizedPanic(tree.ErrDivByZero)
				}
				%[1]s = %[2]s.Div(int64(%[3]s))`,
				target, l, r)
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
		}
		return ""
	}
}

func (c intIntervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.BinOp {
		case tree.Mult:
			return fmt.Sprintf(`%[1]s = %[3]s.Mul(int64(%[2]s))`,
				target, l, r)
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
		}
		return ""
	}
}

func (c intervalFloatCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.BinOp {
		case tree.Mult:
			return fmt.Sprintf(`%[1]s = %[2]s.MulFloat(float64(%[3]s))`,
				target, l, r)
		case tree.Div:
			return fmt.Sprintf(`
				if %[3]s == 0.0 {
					execerror.NonVectorizedPanic(tree.ErrDivByZero)
				}
				%[1]s = %[2]s.DivFloat(float64(%[3]s))`,
				target, l, r)
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
		}
		return ""
	}
}

func (c floatIntervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.BinOp {
		case tree.Mult:
			return fmt.Sprintf(`%[1]s = %[3]s.MulFloat(float64(%[2]s))`,
				target, l, r)
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
		}
		return ""
	}
}

func (c intervalDecimalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.BinOp {
		case tree.Mult:
			return fmt.Sprintf(`
		  f, err := %[3]s.Float64()
		  if err != nil {
		    execerror.VectorizedInternalPanic(err)
		  }
		  %[1]s = %[2]s.MulFloat(f)`,
				target, l, r)
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
		}
		return ""
	}
}

func (c decimalIntervalCustomizer) getBinOpAssignFunc() assignFunc {
	return func(op overload, target, l, r string) string {
		switch op.BinOp {
		case tree.Mult:
			return fmt.Sprintf(`
		  f, err := %[2]s.Float64()
		  if err != nil {
		    execerror.VectorizedInternalPanic(err)
		  }
		  %[1]s = %[3]s.MulFloat(f)`,
				target, l, r)

		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
		}
		return ""
	}
}

func registerTypeCustomizers() {
	typeCustomizers = make(map[coltypePair]typeCustomizer)
	registerTypeCustomizer(coltypePair{coltypes.Bool, coltypes.Bool}, boolCustomizer{})
	registerTypeCustomizer(coltypePair{coltypes.Bytes, coltypes.Bytes}, bytesCustomizer{})
	registerTypeCustomizer(coltypePair{coltypes.Decimal, coltypes.Decimal}, decimalCustomizer{})
	registerTypeCustomizer(coltypePair{coltypes.Timestamp, coltypes.Timestamp}, timestampCustomizer{})
	registerTypeCustomizer(coltypePair{coltypes.Interval, coltypes.Interval}, intervalCustomizer{})
	for _, leftFloatType := range coltypes.FloatTypes {
		for _, rightFloatType := range coltypes.FloatTypes {
			registerTypeCustomizer(coltypePair{leftFloatType, rightFloatType}, floatCustomizer{width: 64})
		}
	}
	for _, leftIntType := range coltypes.IntTypes {
		for _, rightIntType := range coltypes.IntTypes {
			registerTypeCustomizer(coltypePair{leftIntType, rightIntType}, intCustomizer{width: 64})
		}
	}
	// Use a customizer of appropriate width when widths are the same.
	registerTypeCustomizer(coltypePair{coltypes.Float64, coltypes.Float64}, floatCustomizer{width: 64})
	registerTypeCustomizer(coltypePair{coltypes.Int16, coltypes.Int16}, intCustomizer{width: 16})
	registerTypeCustomizer(coltypePair{coltypes.Int32, coltypes.Int32}, intCustomizer{width: 32})
	registerTypeCustomizer(coltypePair{coltypes.Int64, coltypes.Int64}, intCustomizer{width: 64})

	for _, rightFloatType := range coltypes.FloatTypes {
		registerTypeCustomizer(coltypePair{coltypes.Decimal, rightFloatType}, decimalFloatCustomizer{})
		registerTypeCustomizer(coltypePair{coltypes.Interval, rightFloatType}, intervalFloatCustomizer{})
	}
	for _, rightIntType := range coltypes.IntTypes {
		registerTypeCustomizer(coltypePair{coltypes.Decimal, rightIntType}, decimalIntCustomizer{})
		registerTypeCustomizer(coltypePair{coltypes.Interval, rightIntType}, intervalIntCustomizer{})
	}
	for _, leftFloatType := range coltypes.FloatTypes {
		registerTypeCustomizer(coltypePair{leftFloatType, coltypes.Decimal}, floatDecimalCustomizer{})
		registerTypeCustomizer(coltypePair{leftFloatType, coltypes.Interval}, floatIntervalCustomizer{})
	}
	for _, leftIntType := range coltypes.IntTypes {
		registerTypeCustomizer(coltypePair{leftIntType, coltypes.Decimal}, intDecimalCustomizer{})
		registerTypeCustomizer(coltypePair{leftIntType, coltypes.Interval}, intIntervalCustomizer{})
	}
	for _, leftFloatType := range coltypes.FloatTypes {
		for _, rightIntType := range coltypes.IntTypes {
			registerTypeCustomizer(coltypePair{leftFloatType, rightIntType}, floatIntCustomizer{})
		}
	}
	for _, leftIntType := range coltypes.IntTypes {
		for _, rightFloatType := range coltypes.FloatTypes {
			registerTypeCustomizer(coltypePair{leftIntType, rightFloatType}, intFloatCustomizer{})
		}
	}
	registerTypeCustomizer(coltypePair{coltypes.Timestamp, coltypes.Interval}, timestampIntervalCustomizer{})
	registerTypeCustomizer(coltypePair{coltypes.Interval, coltypes.Timestamp}, intervalTimestampCustomizer{})
	registerTypeCustomizer(coltypePair{coltypes.Interval, coltypes.Decimal}, intervalDecimalCustomizer{})
	registerTypeCustomizer(coltypePair{coltypes.Decimal, coltypes.Interval}, decimalIntervalCustomizer{})
}

func registerBinOpOutputTypes() {
	binOpOutputTypes = make(map[tree.BinaryOperator]map[coltypePair]coltypes.T)
	for binOp := range binaryOpName {
		binOpOutputTypes[binOp] = make(map[coltypePair]coltypes.T)
		for _, leftFloatType := range coltypes.FloatTypes {
			for _, rightFloatType := range coltypes.FloatTypes {
				binOpOutputTypes[binOp][coltypePair{leftFloatType, rightFloatType}] = coltypes.Float64
			}
		}
		for _, leftIntType := range coltypes.IntTypes {
			for _, rightIntType := range coltypes.IntTypes {
				binOpOutputTypes[binOp][coltypePair{leftIntType, rightIntType}] = coltypes.Int64
			}
		}
		// Use an output type of the same width when input widths are the same.
		binOpOutputTypes[binOp][coltypePair{coltypes.Decimal, coltypes.Decimal}] = coltypes.Decimal
		binOpOutputTypes[binOp][coltypePair{coltypes.Float64, coltypes.Float64}] = coltypes.Float64
		binOpOutputTypes[binOp][coltypePair{coltypes.Int16, coltypes.Int16}] = coltypes.Int16
		binOpOutputTypes[binOp][coltypePair{coltypes.Int32, coltypes.Int32}] = coltypes.Int32
		binOpOutputTypes[binOp][coltypePair{coltypes.Int64, coltypes.Int64}] = coltypes.Int64
		for _, intType := range coltypes.IntTypes {
			binOpOutputTypes[binOp][coltypePair{coltypes.Decimal, intType}] = coltypes.Decimal
			binOpOutputTypes[binOp][coltypePair{intType, coltypes.Decimal}] = coltypes.Decimal
		}
	}

	// There is a special case for division with integers; it should have a
	// decimal result.
	for _, leftIntType := range coltypes.IntTypes {
		for _, rightIntType := range coltypes.IntTypes {
			binOpOutputTypes[tree.Div][coltypePair{leftIntType, rightIntType}] = coltypes.Decimal
		}
	}

	binOpOutputTypes[tree.Minus][coltypePair{coltypes.Timestamp, coltypes.Timestamp}] = coltypes.Interval
	binOpOutputTypes[tree.Plus][coltypePair{coltypes.Interval, coltypes.Interval}] = coltypes.Interval
	binOpOutputTypes[tree.Minus][coltypePair{coltypes.Interval, coltypes.Interval}] = coltypes.Interval
	for _, numberType := range coltypes.NumberTypes {
		binOpOutputTypes[tree.Mult][coltypePair{numberType, coltypes.Interval}] = coltypes.Interval
		binOpOutputTypes[tree.Mult][coltypePair{coltypes.Interval, numberType}] = coltypes.Interval
	}
	for _, rightIntType := range coltypes.IntTypes {
		binOpOutputTypes[tree.Div][coltypePair{coltypes.Interval, rightIntType}] = coltypes.Interval
	}
	for _, rightFloatType := range coltypes.FloatTypes {
		binOpOutputTypes[tree.Div][coltypePair{coltypes.Interval, rightFloatType}] = coltypes.Interval
	}
	binOpOutputTypes[tree.Plus][coltypePair{coltypes.Timestamp, coltypes.Interval}] = coltypes.Timestamp
	binOpOutputTypes[tree.Minus][coltypePair{coltypes.Timestamp, coltypes.Interval}] = coltypes.Timestamp
	binOpOutputTypes[tree.Plus][coltypePair{coltypes.Interval, coltypes.Timestamp}] = coltypes.Timestamp
}

// Avoid unused warning for functions which are only used in templates.
var _ = overload{}.Assign
var _ = overload{}.Compare
var _ = overload{}.UnaryAssign
var _ = castOverload{}.Assign

// buildDict is a template function that builds a dictionary out of its
// arguments. The argument to this function should be an alternating sequence of
// argument name strings and arguments (argName1, arg1, argName2, arg2, etc).
// This is needed because the template language only allows 1 argument to be
// passed into a defined template.
func buildDict(values ...interface{}) (map[string]interface{}, error) {
	if len(values)%2 != 0 {
		return nil, errors.New("invalid call to buildDict")
	}
	dict := make(map[string]interface{}, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			return nil, errors.New("buildDict keys must be strings")
		}
		dict[key] = values[i+1]
	}
	return dict, nil
}

// intersectOverloads takes in a slice of overloads and returns a new slice of
// overloads the corresponding intersected overloads at each position. The
// intersection is determined to be the maximum common set of LTyp types shared
// by each overloads.
func intersectOverloads(allOverloads ...[]*overload) [][]*overload {
	inputTypes := coltypes.AllTypes
	keepTypes := make(map[coltypes.T]bool, len(inputTypes))

	for _, t := range inputTypes {
		keepTypes[t] = true
		for _, overloads := range allOverloads {
			found := false
			for _, ov := range overloads {
				if ov.LTyp == t {
					found = true
				}
			}

			if !found {
				keepTypes[t] = false
			}
		}
	}

	for i, overloads := range allOverloads {
		newOverloads := make([]*overload, 0, cap(overloads))
		for _, ov := range overloads {
			if keepTypes[ov.LTyp] {
				newOverloads = append(newOverloads, ov)
			}
		}

		allOverloads[i] = newOverloads
	}

	return allOverloads
}

// makeFunctionRegex makes a regexp representing a function with a specified
// number of arguments. For example, a function with 3 arguments looks like
// `(?s)funcName\(\s*(.*?),\s*(.*?),\s*(.*?)\)`.
func makeFunctionRegex(funcName string, numArgs int) *regexp.Regexp {
	argsRegex := ""

	for i := 0; i < numArgs; i++ {
		if argsRegex != "" {
			argsRegex += ","
		}
		argsRegex += `\s*(.*?)`
	}

	return regexp.MustCompile(`(?s)` + funcName + `\(` + argsRegex + `\)`)
}

// makeTemplateFunctionCall makes a string representing a function call in the
// template language. For example, it will return
//   `{{.Assign "$1" "$2" "$3"}}`
// if funcName is `Assign` and numArgs is 3.
func makeTemplateFunctionCall(funcName string, numArgs int) string {
	res := "{{." + funcName
	for i := 1; i <= numArgs; i++ {
		res += fmt.Sprintf(" \"$%d\"", i)
	}
	res += "}}"
	return res
}
