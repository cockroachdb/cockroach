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
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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

// overloadBase and other overload-related structs form a leveled hierarchy
// that is useful during the code generation. Structs have "up" and "down"
// references to each other so that the necessary fields for the templating are
// accessible via '.'. Only when we reach the bottom level (where
// lastArgWidthOverload is) do we have the access to "resolved" overload
// functions.
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
// And for lastArgTypeOverload:
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
	Name  string
	OpStr string
	// Only one of CmpOp and BinOp will be set, depending on whether the
	// overload is a binary operator or a comparison operator. Neither of the
	// fields will be set when it is a hash or cast overload.
	CmpOp tree.ComparisonOperator
	BinOp tree.BinaryOperator

	IsCmpOp  bool
	IsBinOp  bool
	IsHashOp bool
	IsCastOp bool
}

func (b *overloadBase) String() string {
	return fmt.Sprintf("%s: %s", b.Name, b.OpStr)
}

func toString(family types.Family) string {
	return "types." + family.String()
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
	return b.CanonicalTypeFamily.String()
}

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
	lawo := &lastArgWidthOverload{
		lastArgTypeOverload:  typeOverload,
		argWidthOverloadBase: newArgWidthOverloadBase(typeOverload.argTypeOverloadBase, width),
		RetType:              retType,
		RetVecMethod:         toVecMethod(retType.Family(), retType.Width()),
		RetGoType:            toPhysicalRepresentation(retType.Family(), retType.Width()),
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

type assignFunc func(op *lastArgWidthOverload, target, l, r string) string
type compareFunc func(target, l, r string) string

// sameTypeBinaryOpToOverloads maps a binary operator to all of the overloads
// that implement that comparison between two values of the same type (meaning
// they have the same family and width).
var sameTypeBinaryOpToOverloads = make(map[tree.BinaryOperator][]*oneArgOverload, len(binaryOpName))

var binOpOutputTypes = make(map[tree.BinaryOperator]map[typePair]*types.T)

// sameTypeComparisonOpToOverloads maps a comparison operator to all of the
// overloads that implement that comparison between two values of the same type
// (meaning they have the same family and width).
var sameTypeComparisonOpToOverloads = make(map[tree.ComparisonOperator][]*oneArgOverload, len(comparisonOpName))

var cmpOpOutputTypes = make(map[typePair]*types.T)

// hashOverloads is a list of all of the overloads that implement the hash
// operation.
var hashOverloads []*oneArgOverload

// castOutputTypes contains "fake" output types of a cast operator. It is used
// just to make overloads initialization happy and is a bit of a hack - we
// represent cast overloads as twoArgsOverload (meaning it takes in two
// arguments and returns a third value), whereas it'll actually take in one
// argument (left) and will return the result of a cast to another argument's
// type (right), so the "true" return type is stored in right overload which is
// populated correctly.
var castOutputTypes = make(map[typePair]*types.T)

// Assign produces a Go source string that assigns the "target" variable to the
// result of applying the overload to the two inputs, l and r.
//
// For example, an overload that implemented the float64 plus operation, when fed
// the inputs "x", "a", "b", would produce the string "x = a + b".
func (o *lastArgWidthOverload) Assign(target, l, r string) string {
	if o.AssignFunc != nil {
		if ret := o.AssignFunc(o, target, l, r); ret != "" {
			return ret
		}
	}
	// Default assign form assumes an infix operator.
	return fmt.Sprintf("%s = %s %s %s", target, l, o.overloadBase.OpStr, r)
}

// Compare produces a Go source string that assigns the "target" variable to the
// result of comparing the two inputs, l and r. The target will be negative,
// zero, or positive depending on whether l is less-than, equal-to, or
// greater-than r.
func (o *lastArgWidthOverload) Compare(target, l, r string) string {
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

func (o *lastArgWidthOverload) UnaryAssign(target, v string) string {
	if o.AssignFunc != nil {
		if ret := o.AssignFunc(o, target, v, ""); ret != "" {
			return ret
		}
	}
	// Default assign form assumes a function operator.
	return fmt.Sprintf("%s = %s(%s)", target, o.overloadBase.OpStr, v)
}

type castFunc func(to, from string) string

func (o *lastArgWidthOverload) Cast(to, from string) string {
	if o.CastFunc != nil {
		if ret := o.CastFunc(to, from); ret != "" {
			return ret
		}
	}
	// Default cast function is "identity" cast.
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
				colexecerror.ExpectedError(tree.ErrIntOutOfRange)
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
				colexecerror.ExpectedError(tmpErr)
			}
			%[1]s = tmpDec
		}
	`
	return fmt.Sprintf(convStr, to, from)
}

// populateTwoArgsOverloads creates all overload structs related to a single
// binary, comparison, or cast operator. It takes in:
// - base - the overload base that will be shared among all new overloads.
// - opOutputTypes - mapping from a pair of types to the output type, it should
//   contain an entry for all supported type pairs.
// - overrideOverloadFuncs - a function that could update AssignFunc,
//   CompareFunc and/or CastFunc fields of a newly created lastArgWidthOverload
//   based on a typeCustomizer.
// It returns all new overloads that have the same type (which will be empty
// for cast operator).
func populateTwoArgsOverloads(
	base *overloadBase,
	opOutputTypes map[typePair]*types.T,
	overrideOverloadFuncs func(*lastArgWidthOverload, typeCustomizer),
	customizers map[typePair]typeCustomizer,
) (newSameTypeOverloads []*oneArgOverload) {
	var combinableCanonicalTypeFamilies map[types.Family][]types.Family
	if base.IsBinOp {
		combinableCanonicalTypeFamilies = compatibleCanonicalTypeFamilies
	} else if base.IsCmpOp {
		combinableCanonicalTypeFamilies = comparableCanonicalTypeFamilies
	} else if base.IsCastOp {
		combinableCanonicalTypeFamilies = castableCanonicalTypeFamilies
	} else {
		colexecerror.InternalError("unexpectedly overload is neither binary, comparison, nor cast")
	}
	for _, leftFamily := range supportedCanonicalTypeFamilies {
		leftWidths, found := supportedWidthsByCanonicalTypeFamily[leftFamily]
		if !found {
			colexecerror.InternalError(fmt.Sprintf("didn't find supported widths for %s", leftFamily))
		}
		leftFamilyStr := toString(leftFamily)
		for _, rightFamily := range combinableCanonicalTypeFamilies[leftFamily] {
			rightWidths, found := supportedWidthsByCanonicalTypeFamily[rightFamily]
			if !found {
				colexecerror.InternalError(fmt.Sprintf("didn't find supported widths for %s", rightFamily))
			}
			rightFamilyStr := toString(rightFamily)
			for _, leftWidth := range leftWidths {
				for _, rightWidth := range rightWidths {
					customizer, ok := customizers[typePair{leftFamily, leftWidth, rightFamily, rightWidth}]
					if !ok {
						colexecerror.InternalError("unexpectedly didn't find a type customizer")
					}
					// Skip overloads that don't have associated output types.
					retType, ok := opOutputTypes[typePair{leftFamily, leftWidth, rightFamily, rightWidth}]
					if !ok {
						continue
					}
					var info *twoArgsResolvedOverloadInfo
					if base.IsBinOp {
						for _, existingInfo := range twoArgsResolvedOverloadsInfo.BinOps {
							if existingInfo.Name == base.Name {
								info = existingInfo
								break
							}
						}
						if info == nil {
							info = &twoArgsResolvedOverloadInfo{
								overloadBase: base,
							}
							twoArgsResolvedOverloadsInfo.BinOps = append(twoArgsResolvedOverloadsInfo.BinOps, info)
						}
					} else if base.IsCmpOp {
						for _, existingInfo := range twoArgsResolvedOverloadsInfo.CmpOps {
							if existingInfo.Name == base.Name {
								info = existingInfo
								break
							}
						}
						if info == nil {
							info = &twoArgsResolvedOverloadInfo{
								overloadBase: base,
							}
							twoArgsResolvedOverloadsInfo.CmpOps = append(twoArgsResolvedOverloadsInfo.CmpOps, info)
						}
					} else if base.IsCastOp {
						info = twoArgsResolvedOverloadsInfo.CastOverloads
						if info == nil {
							info = &twoArgsResolvedOverloadInfo{
								overloadBase: base,
							}
							twoArgsResolvedOverloadsInfo.CastOverloads = info
						}
					}
					var leftFamilies *twoArgsResolvedOverloadLeftFamilyInfo
					for _, lf := range info.LeftFamilies {
						if lf.LeftCanonicalFamilyStr == leftFamilyStr {
							leftFamilies = lf
							break
						}
					}
					if leftFamilies == nil {
						leftFamilies = &twoArgsResolvedOverloadLeftFamilyInfo{
							LeftCanonicalFamilyStr: leftFamilyStr,
						}
						info.LeftFamilies = append(info.LeftFamilies, leftFamilies)
					}
					var leftWidths *twoArgsResolvedOverloadLeftWidthInfo
					for _, lw := range leftFamilies.LeftWidths {
						if lw.Width == leftWidth {
							leftWidths = lw
							break
						}
					}
					if leftWidths == nil {
						leftWidths = &twoArgsResolvedOverloadLeftWidthInfo{
							Width: leftWidth,
						}
						leftFamilies.LeftWidths = append(leftFamilies.LeftWidths, leftWidths)
					}
					var rightFamilies *twoArgsResolvedOverloadRightFamilyInfo
					for _, rf := range leftWidths.RightFamilies {
						if rf.RightCanonicalFamilyStr == rightFamilyStr {
							rightFamilies = rf
							break
						}
					}
					if rightFamilies == nil {
						rightFamilies = &twoArgsResolvedOverloadRightFamilyInfo{
							RightCanonicalFamilyStr: rightFamilyStr,
						}
						leftWidths.RightFamilies = append(leftWidths.RightFamilies, rightFamilies)
					}
					lawo := newLastArgWidthOverload(
						newLastArgTypeOverload(base, rightFamily),
						rightWidth, retType,
					)
					overrideOverloadFuncs(lawo, customizer)
					taro := &twoArgsResolvedOverload{
						overloadBase: base,
						Left: newArgWidthOverload(
							newArgTypeOverload(base, leftFamily, leftWidth),
							leftWidth,
						),
						Right: lawo,
					}
					rightFamilies.RightWidths = append(rightFamilies.RightWidths,
						&twoArgsResolvedOverloadRightWidthInfo{
							Width:                   rightWidth,
							twoArgsResolvedOverload: taro,
						})
					if base.IsBinOp || base.IsCmpOp {
						resolvedBinCmpOpsOverloads = append(resolvedBinCmpOpsOverloads, taro)
						if leftFamily == rightFamily && leftWidth == rightWidth {
							var oao *oneArgOverload
							for _, o := range newSameTypeOverloads {
								if o.CanonicalTypeFamily == leftFamily {
									oao = o
									break
								}
							}
							if oao == nil {
								oao = &oneArgOverload{
									// We're creating a separate lastArgTypeOverload
									// because we want to have a separate WidthOverloads
									// field for same type family and same width
									// overloads.
									lastArgTypeOverload: &lastArgTypeOverload{
										overloadBase:        base,
										argTypeOverloadBase: lawo.lastArgTypeOverload.argTypeOverloadBase,
									},
								}
								newSameTypeOverloads = append(newSameTypeOverloads, oao)
							}
							oao.WidthOverloads = append(oao.WidthOverloads, lawo)
						}
					}
				}
			}
		}
	}
	return newSameTypeOverloads
}

func populateBinOpOverloads() {
	registerBinOpOutputTypes()
	for _, op := range []tree.BinaryOperator{tree.Plus, tree.Minus, tree.Mult, tree.Div} {
		ob := &overloadBase{
			Name:    binaryOpName[op],
			BinOp:   op,
			IsBinOp: true,
			OpStr:   binaryOpInfix[op],
		}
		sameTypeBinaryOpToOverloads[op] = populateTwoArgsOverloads(
			ob, binOpOutputTypes[op],
			func(lawo *lastArgWidthOverload, customizer typeCustomizer) {
				if b, ok := customizer.(binOpTypeCustomizer); ok {
					lawo.AssignFunc = b.getBinOpAssignFunc()
				}
			},
			typeCustomizers,
		)
	}
}

func populateCmpOpOverloads() {
	registerCmpOpOutputTypes()
	for _, op := range []tree.ComparisonOperator{tree.EQ, tree.NE, tree.LT, tree.LE, tree.GT, tree.GE} {
		base := &overloadBase{
			Name:    comparisonOpName[op],
			CmpOp:   op,
			IsCmpOp: true,
			OpStr:   comparisonOpInfix[op],
		}
		sameTypeComparisonOpToOverloads[op] = populateTwoArgsOverloads(
			base,
			cmpOpOutputTypes,
			func(lawo *lastArgWidthOverload, customizer typeCustomizer) {
				if b, ok := customizer.(cmpOpTypeCustomizer); ok {
					lawo.AssignFunc = func(op *lastArgWidthOverload, target, l, r string) string {
						cmp := b.getCmpOpCompareFunc()("cmpResult", l, r)
						if cmp == "" {
							return ""
						}
						args := map[string]string{"Target": target, "Cmp": cmp, "Op": op.overloadBase.OpStr}
						buf := strings.Builder{}
						t := template.Must(template.New("").Parse(`
										{
											var cmpResult int
											{{.Cmp}}
											{{.Target}} = cmpResult {{.Op}} 0
										}
									`))
						if err := t.Execute(&buf, args); err != nil {
							colexecerror.InternalError(err)
						}
						return buf.String()
					}
					lawo.CompareFunc = b.getCmpOpCompareFunc()
				}
			},
			typeCustomizers,
		)
	}
}

func populateHashOverloads() {
	hashOverloadBase := &overloadBase{
		Name:     "hash",
		IsHashOp: true,
		OpStr:    "uint64",
	}
	for _, family := range supportedCanonicalTypeFamilies {
		widths, found := supportedWidthsByCanonicalTypeFamily[family]
		if !found {
			colexecerror.InternalError(fmt.Sprintf("didn't find supported widths for %s", family))
		}
		ov := newLastArgTypeOverload(hashOverloadBase, family)
		for _, width := range widths {
			// Note that we pass in types.Bool as the return type just to make
			//
			// Note that we pass in types.Bool as the return type just to make
			// overloads initialization happy. We don't actually care about the
			// return type since we know that it will be represented physically
			// as uint64.
			lawo := newLastArgWidthOverload(ov, width, types.Bool)
			sameTypeCustomizer := typeCustomizers[typePair{family, width, family, width}]
			if sameTypeCustomizer != nil {
				if b, ok := sameTypeCustomizer.(hashTypeCustomizer); ok {
					lawo.AssignFunc = b.getHashAssignFunc()
				}
			}
		}
		hashOverloads = append(hashOverloads, &oneArgOverload{
			lastArgTypeOverload: ov,
		})
	}
}

func populateCastOverloads() {
	registerCastTypeCustomizers()
	registerCastOutputTypes()
	populateTwoArgsOverloads(
		&overloadBase{
			Name:     "cast",
			IsCastOp: true,
		},
		castOutputTypes,
		func(lawo *lastArgWidthOverload, customizer typeCustomizer) {
			if b, ok := customizer.(castTypeCustomizer); ok {
				lawo.CastFunc = b.getCastFunc()
			}
		}, castTypeCustomizers)
}

func init() {
	registerTypeCustomizers()

	populateBinOpOverloads()
	populateCmpOpOverloads()
	populateHashOverloads()
	populateCastOverloads()
}

// typeCustomizer is a marker interface for something that implements one or
// more of binOpTypeCustomizer and cmpOpTypeCustomizer.
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

var typeCustomizers map[coltypePair]typeCustomizer

// registerTypeCustomizer registers a particular type customizer to a
// pair of types, for usage by templates.
func registerTypeCustomizer(pair coltypePair, customizer typeCustomizer) {
	typeCustomizers[pair] = customizer
}

var castTypeCustomizers = make(map[typePair]typeCustomizer)

func registerCastTypeCustomizer(pair typePair, customizer typeCustomizer) {
	if _, found := castTypeCustomizers[pair]; found {
		colexecerror.InternalError(fmt.Sprintf("unexpectedly cast type customizer already present for %v", pair))
	}
	castTypeCustomizers[pair] = customizer
	alreadyPresent := false
	for _, rightFamily := range castableCanonicalTypeFamilies[pair.leftTypeFamily] {
		if rightFamily == pair.rightTypeFamily {
			alreadyPresent = true
			break
		}
	}
	if !alreadyPresent {
		castableCanonicalTypeFamilies[pair.leftTypeFamily] = append(
			castableCanonicalTypeFamilies[pair.leftTypeFamily], pair.rightTypeFamily,
		)
	}
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
			colexecerror.InternalError(err)
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
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				if err != nil {
					colexecerror.ExpectedError(err)
				}
			}
			`, binaryOpDecCtx[op.BinOp], binaryOpDecMethod[op.BinOp], target, l, r)
		}
		return fmt.Sprintf("if _, err := tree.%s.%s(&%s, &%s, &%s); err != nil { colexecerror.ExpectedError(err) }",
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
				}	else if {{if .CheckLeftNan}} math.IsNaN(a) {{else}} false {{end}} {
					if {{if .CheckRightNan}} math.IsNaN(b) {{else}} false {{end}} {
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
			colexecerror.InternalError(err)
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
			colexecerror.InternalError(err)
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
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
					}
					{{.Target}} = result
				}
			`))

		case tree.Minus:
			t = template.Must(template.New("").Parse(`
				{
					result := {{.Left}} - {{.Right}}
					if (result < {{.Left}}) != ({{.Right}} > 0) {
						colexecerror.ExpectedError(tree.ErrIntOutOfRange)
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
				colexecerror.InternalError(fmt.Sprintf("unhandled integer width %d", c.width))
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
								colexecerror.ExpectedError(tree.ErrIntOutOfRange)
							} else if result/{{.Right}} != {{.Left}} {
								colexecerror.ExpectedError(tree.ErrIntOutOfRange)
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
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				leftTmpDec, rightTmpDec := &decimalScratch.tmpDec1, &decimalScratch.tmpDec2
				leftTmpDec.SetFinite(int64({{.Left}}), 0)
				rightTmpDec.SetFinite(int64({{.Right}}), 0)
				if _, err := tree.{{.Ctx}}.Quo(&{{.Target}}, leftTmpDec, rightTmpDec); err != nil {
					colexecerror.ExpectedError(err)
				}
			}
		`))

		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
		}

		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
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
					colexecerror.ExpectedError(err)
				}
				{{.Target}} = tree.CompareDecimals(&{{.Left}}, tmpDec)
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
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
			colexecerror.InternalError(err)
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
				{{if .IsDivision}}
				if {{.Right}} == 0 {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				{{end}}
				tmpDec := &decimalScratch.tmpDec1
				tmpDec.SetFinite(int64({{.Right}}), 0)
				if _, err := tree.{{.Ctx}}.{{.Op}}(&{{.Target}}, &{{.Left}}, tmpDec); err != nil {
					colexecerror.ExpectedError(err)
				}
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
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
					colexecerror.ExpectedError(err)
				}
				{{.Target}} = tree.CompareDecimals(tmpDec, &{{.Right}})
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
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
			colexecerror.InternalError(err)
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
				{{if .IsDivision}}
				cond, err := tree.{{.Ctx}}.{{.Op}}(&{{.Target}}, tmpDec, &{{.Right}})
				if cond.DivisionByZero() {
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				{{else}}
				_, err := tree.{{.Ctx}}.{{.Op}}(&{{.Target}}, tmpDec, &{{.Right}})
				{{end}}
				if err != nil {
					colexecerror.ExpectedError(err)
				}
			}
		`))
		if err := t.Execute(&buf, args); err != nil {
			colexecerror.InternalError(err)
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
			colexecerror.InternalError(err)
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
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
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
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
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
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
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
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
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
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				%[1]s = %[2]s.Div(int64(%[3]s))`,
				target, l, r)
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
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
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
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
					colexecerror.ExpectedError(tree.ErrDivByZero)
				}
				%[1]s = %[2]s.DivFloat(float64(%[3]s))`,
				target, l, r)
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
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
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
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
		    colexecerror.InternalError(err)
		  }
		  %[1]s = %[2]s.MulFloat(f)`,
				target, l, r)
		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
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
		    colexecerror.InternalError(err)
		  }
		  %[1]s = %[3]s.MulFloat(f)`,
				target, l, r)

		default:
			colexecerror.InternalError(fmt.Sprintf("unhandled binary operator %s", op.BinOp.String()))
		}
		return ""
	}
}

func registerTypeCustomizers() {
	typeCustomizers = make(map[typePair]typeCustomizer)
	registerTypeCustomizer(typePair{types.BoolFamily, anyWidth, types.BoolFamily, anyWidth}, boolCustomizer{})
	registerTypeCustomizer(typePair{types.BytesFamily, anyWidth, types.BytesFamily, anyWidth}, bytesCustomizer{})
	registerTypeCustomizer(typePair{types.DecimalFamily, anyWidth, types.DecimalFamily, anyWidth}, decimalCustomizer{})
	registerTypeCustomizer(typePair{types.FloatFamily, anyWidth, types.FloatFamily, anyWidth}, floatCustomizer{})
	registerTypeCustomizer(typePair{types.TimestampTZFamily, anyWidth, types.TimestampTZFamily, anyWidth}, timestampCustomizer{})
	registerTypeCustomizer(typePair{types.IntervalFamily, anyWidth, types.IntervalFamily, anyWidth}, intervalCustomizer{})
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
}

func registerBinOpOutputTypes() {
	for binOp := range binaryOpName {
		binOpOutputTypes[binOp] = make(map[typePair]*types.T)
		binOpOutputTypes[binOp][typePair{types.FloatFamily, anyWidth, types.FloatFamily, anyWidth}] = types.Float
		for _, leftIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			for _, rightIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
				binOpOutputTypes[binOp][typePair{types.IntFamily, leftIntWidth, types.IntFamily, rightIntWidth}] = types.Int
			}
		}
		binOpOutputTypes[binOp][typePair{types.DecimalFamily, anyWidth, types.DecimalFamily, anyWidth}] = types.Decimal
		binOpOutputTypes[binOp][typePair{types.FloatFamily, anyWidth, types.FloatFamily, anyWidth}] = types.Float
		// Use an output type of the same width when input widths are the same.
		// Note: keep output type of binary operations on integers of different
		// widths in line with planning in colexec/execplan.go.
		for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			var intType *types.T
			switch intWidth {
			case 16:
				intType = types.Int2
			case 32:
				intType = types.Int4
			case anyWidth:
				intType = types.Int
			default:
				colexecerror.InternalError(fmt.Sprintf("unexpected int width: %d", intWidth))
			}
			binOpOutputTypes[binOp][typePair{types.IntFamily, intWidth, types.IntFamily, intWidth}] = intType
		}
		for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			binOpOutputTypes[binOp][typePair{types.DecimalFamily, anyWidth, types.IntFamily, intWidth}] = types.Decimal
			binOpOutputTypes[binOp][typePair{types.IntFamily, intWidth, types.DecimalFamily, anyWidth}] = types.Decimal
		}
	}

	// There is a special case for division with integers; it should have a
	// decimal result.
	for _, leftIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		for _, rightIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			binOpOutputTypes[tree.Div][typePair{types.IntFamily, leftIntWidth, types.IntFamily, rightIntWidth}] = types.Decimal
		}
	}

	binOpOutputTypes[tree.Minus][typePair{types.TimestampTZFamily, anyWidth, types.TimestampTZFamily, anyWidth}] = types.Interval
	binOpOutputTypes[tree.Plus][typePair{types.IntervalFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.Interval
	binOpOutputTypes[tree.Minus][typePair{types.IntervalFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.Interval
	for _, numberTypeFamily := range numericCanonicalTypeFamilies {
		binOpOutputTypes[tree.Mult][typePair{numberTypeFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.Interval
		binOpOutputTypes[tree.Mult][typePair{types.IntervalFamily, anyWidth, numberTypeFamily, anyWidth}] = types.Interval
	}
	binOpOutputTypes[tree.Div][typePair{types.IntervalFamily, anyWidth, types.IntFamily, anyWidth}] = types.Interval
	binOpOutputTypes[tree.Div][typePair{types.IntervalFamily, anyWidth, types.FloatFamily, anyWidth}] = types.Interval
	binOpOutputTypes[tree.Plus][typePair{types.TimestampTZFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.TimestampTZ
	binOpOutputTypes[tree.Minus][typePair{types.TimestampTZFamily, anyWidth, types.IntervalFamily, anyWidth}] = types.TimestampTZ
	binOpOutputTypes[tree.Plus][typePair{types.IntervalFamily, anyWidth, types.TimestampTZFamily, anyWidth}] = types.TimestampTZ
}

// boolCastCustomizer specifies casts from booleans.
type boolCastCustomizer struct{}

// decimalCastCustomizer specifies casts from decimals.
type decimalCastCustomizer struct{}

// floatCastCustomizer specifies casts from floats.
type floatCastCustomizer struct {
	toFamily types.Family
	toWidth  int32
}

// intCastCustomizer specifies casts from ints to other types.
type intCastCustomizer struct {
	toFamily types.Family
	toWidth  int32
}

func (boolCastCustomizer) getCastFunc() castFunc {
	return func(to, from string) string {
		convStr := `
			%[1]s = 0
			if %[2]s {
				%[1]s = 1
			}
		`
		return fmt.Sprintf(convStr, to, from)
	}
}

func (decimalCastCustomizer) getCastFunc() castFunc {
	return func(to, from string) string {
		return fmt.Sprintf("%[1]s = %[2]s.Sign() != 0", to, from)
	}
}

func (c floatCastCustomizer) getCastFunc() castFunc {
	switch c.toFamily {
	case types.BoolFamily:
		return numToBool
	case types.DecimalFamily:
		return floatToDecimal
	case types.IntFamily:
		return floatToInt(c.toWidth, 64)
	}
	colexecerror.InternalError(fmt.Sprintf("unexpectedly didn't find a cast from float to %s with %d width", c.toFamily, c.toWidth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

func (c intCastCustomizer) getCastFunc() castFunc {
	switch c.toFamily {
	case types.BoolFamily:
		return numToBool
	case types.DecimalFamily:
		return intToDecimal
	case types.IntFamily:
		switch c.toWidth {
		case 16:
			return intToInt16
		case 32:
			return intToInt32
		case anyWidth:
			return intToInt64
		}
	case types.FloatFamily:
		return intToFloat()
	}
	colexecerror.InternalError(fmt.Sprintf("unexpectedly didn't find a cast from int to %s with %d width", c.toFamily, c.toWidth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

func registerCastTypeCustomizers() {
	// Identity casts.
	//
	// Note that we're using the same "vanilla" type customizers since identity
	// casts are the default behavior of the CastFunc.
	registerCastTypeCustomizer(typePair{types.BoolFamily, anyWidth, types.BoolFamily, anyWidth}, boolCustomizer{})
	// TODO(yuzefovich): add casts between types that have types.BytesFamily as
	// their canonical type family.
	registerCastTypeCustomizer(typePair{types.DecimalFamily, anyWidth, types.DecimalFamily, anyWidth}, decimalCustomizer{})
	registerCastTypeCustomizer(typePair{types.FloatFamily, anyWidth, types.FloatFamily, anyWidth}, floatCustomizer{})
	for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		registerCastTypeCustomizer(typePair{types.IntFamily, intWidth, types.IntFamily, intWidth}, intCustomizer{width: intWidth})
	}
	// TODO(yuzefovich): add casts for Timestamps and Intervals.

	// Casts from boolean.
	registerCastTypeCustomizer(typePair{types.BoolFamily, anyWidth, types.FloatFamily, anyWidth}, boolCastCustomizer{})
	for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		registerCastTypeCustomizer(typePair{types.BoolFamily, anyWidth, types.IntFamily, intWidth}, boolCastCustomizer{})
	}

	// Casts from decimal.
	registerCastTypeCustomizer(typePair{types.DecimalFamily, anyWidth, types.BoolFamily, anyWidth}, decimalCastCustomizer{})

	// Casts from ints.
	for _, fromIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		// Casts between ints.
		for _, toIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			if fromIntWidth != toIntWidth {
				registerCastTypeCustomizer(typePair{types.IntFamily, fromIntWidth, types.IntFamily, toIntWidth}, intCastCustomizer{toFamily: types.IntFamily, toWidth: toIntWidth})
			}
		}
		// Casts to other types.
		for _, toFamily := range []types.Family{types.BoolFamily, types.DecimalFamily, types.FloatFamily} {
			registerCastTypeCustomizer(typePair{types.IntFamily, fromIntWidth, toFamily, anyWidth}, intCastCustomizer{toFamily: toFamily, toWidth: anyWidth})
		}
	}

	// Casts from float.
	for _, toFamily := range []types.Family{types.BoolFamily, types.DecimalFamily, types.IntFamily} {
		for _, toWidth := range supportedWidthsByCanonicalTypeFamily[toFamily] {
			registerCastTypeCustomizer(typePair{types.FloatFamily, anyWidth, toFamily, toWidth}, floatCastCustomizer{toFamily: toFamily, toWidth: toWidth})
		}
	}
}

func registerCastOutputTypes() {
	for _, leftFamily := range supportedCanonicalTypeFamilies {
		for _, leftWidth := range supportedWidthsByCanonicalTypeFamily[leftFamily] {
			for _, rightFamily := range castableCanonicalTypeFamilies[leftFamily] {
				for _, rightWidth := range supportedWidthsByCanonicalTypeFamily[rightFamily] {
					castOutputTypes[typePair{leftFamily, leftWidth, rightFamily, rightWidth}] = types.Bool
				}
			}
		}
	}
}

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

// Although unused right now, this function might be helpful later.
var _ = intersectOverloads

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

func (b *argWidthOverloadBase) GoTypeSliceName() string {
	switch b.CanonicalTypeFamily {
	case types.BytesFamily:
		return "*coldata.Bytes"
	case types.IntFamily:
		switch b.Width {
		case 16:
			return "[]int16"
		case 32:
			return "[]int32"
		case 64, anyWidth:
			return "[]int64"
		default:
			colexecerror.InternalError(fmt.Sprintf("unexpected int width %d", b.Width))
			// This code is unreachable, but the compiler cannot infer that.
			return ""
		}
	default:
		return "[]" + toPhysicalRepresentation(b.CanonicalTypeFamily, b.Width)
	}
}

func get(family types.Family, target, i string) string {
	switch family {
	case types.BytesFamily:
		return fmt.Sprintf("%s.Get(%s)", target, i)
	}
	return fmt.Sprintf("%s[%s]", target, i)
}

// Get is a function that should only be used in templates.
func (b *argWidthOverloadBase) Get(target, i string) string {
	return get(b.CanonicalTypeFamily, target, i)
}

// ReturnGet is a function that should only be used in templates.
func (o *lastArgWidthOverload) ReturnGet(target, i string) string {
	return get(typeconv.TypeFamilyToCanonicalTypeFamily[o.RetType.Family()], target, i)
}

// CopyVal is a function that should only be used in templates.
func (b *argWidthOverloadBase) CopyVal(dest, src string) string {
	switch b.CanonicalTypeFamily {
	case types.BytesFamily:
		return fmt.Sprintf("%[1]s = append(%[1]s[:0], %[2]s...)", dest, src)
	case types.DecimalFamily:
		return fmt.Sprintf("%s.Set(&%s)", dest, src)
	}
	return fmt.Sprintf("%s = %s", dest, src)
}

// Set is a function that should only be used in templates.
func (b *argWidthOverloadBase) Set(target, i, new string) string {
	switch b.CanonicalTypeFamily {
	case types.BytesFamily:
		return fmt.Sprintf("%s.Set(%s, %s)", target, i, new)
	case types.DecimalFamily:
		return fmt.Sprintf("%s[%s].Set(&%s)", target, i, new)
	}
	return fmt.Sprintf("%s[%s] = %s", target, i, new)
}

// Slice is a function that should only be used in templates.
func (b *argWidthOverloadBase) Slice(target, start, end string) string {
	switch b.CanonicalTypeFamily {
	case types.BytesFamily:
		// Slice is a noop for Bytes. We also add a few lines to address "unused
		// variable" compiler errors.
		return fmt.Sprintf(`%s
_ = %s
_ = %s`, target, start, end)
	}
	return fmt.Sprintf("%s[%s:%s]", target, start, end)
}

// CopySlice is a function that should only be used in templates.
func (b *argWidthOverloadBase) CopySlice(
	target, src, destIdx, srcStartIdx, srcEndIdx string,
) string {
	var tmpl string
	switch b.CanonicalTypeFamily {
	case types.BytesFamily:
		tmpl = `{{.Tgt}}.CopySlice({{.Src}}, {{.TgtIdx}}, {{.SrcStart}}, {{.SrcEnd}})`
	case types.DecimalFamily:
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
		colexecerror.InternalError(err)
	}
	return buf.String()
}

// AppendSlice is a function that should only be used in templates.
func (b *argWidthOverloadBase) AppendSlice(
	target, src, destIdx, srcStartIdx, srcEndIdx string,
) string {
	var tmpl string
	switch b.CanonicalTypeFamily {
	case types.BytesFamily:
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
		colexecerror.InternalError(err)
	}
	return buf.String()
}

// AppendVal is a function that should only be used in templates.
func (b *argWidthOverloadBase) AppendVal(target, v string) string {
	switch b.CanonicalTypeFamily {
	case types.BytesFamily:
		return fmt.Sprintf("%s.AppendVal(%s)", target, v)
	case types.DecimalFamily:
		return fmt.Sprintf(`%[1]s = append(%[1]s, apd.Decimal{})
%[1]s[len(%[1]s)-1].Set(&%[2]s)`, target, v)
	}
	return fmt.Sprintf("%[1]s = append(%[1]s, %[2]s)", target, v)
}

// Len is a function that should only be used in templates.
// WARNING: combination of Slice and Len might not work correctly for Bytes
// type.
func (b *argWidthOverloadBase) Len(target string) string {
	switch b.CanonicalTypeFamily {
	case types.BytesFamily:
		return fmt.Sprintf("%s.Len()", target)
	}
	return fmt.Sprintf("len(%s)", target)
}

// Range is a function that should only be used in templates.
func (b *argWidthOverloadBase) Range(loopVariableIdent, target, start, end string) string {
	switch b.CanonicalTypeFamily {
	case types.BytesFamily:
		return fmt.Sprintf("%[1]s := %[2]s; %[1]s < %[3]s; %[1]s++", loopVariableIdent, start, end)
	}
	return fmt.Sprintf("%[1]s := range %[2]s", loopVariableIdent, target)
}

// Window is a function that should only be used in templates.
func (b *argWidthOverloadBase) Window(target, start, end string) string {
	switch b.CanonicalTypeFamily {
	case types.BytesFamily:
		return fmt.Sprintf(`%s.Window(%s, %s)`, target, start, end)
	}
	return fmt.Sprintf("%s[%s:%s]", target, start, end)
}

// Remove unused warnings.
var (
	lawo = &lastArgWidthOverload{}
	_    = lawo.Assign
	_    = lawo.Compare
	_    = lawo.Cast
	_    = lawo.UnaryAssign
	_    = lawo.ReturnGet

	awob = &argWidthOverloadBase{}
	_    = awob.GoTypeSliceName
	_    = awob.Get
	_    = awob.CopyVal
	_    = awob.Set
	_    = awob.Slice
	_    = awob.CopySlice
	_    = awob.AppendSlice
	_    = awob.AppendVal
	_    = awob.Len
	_    = awob.Range
	_    = awob.Window
)

var supportedCanonicalTypeFamilies = []types.Family{
	types.BoolFamily,
	types.BytesFamily,
	types.DecimalFamily,
	types.IntFamily,
	types.FloatFamily,
	types.TimestampTZFamily,
	types.IntervalFamily,
}

var compatibleCanonicalTypeFamilies = map[types.Family][]types.Family{
	types.BoolFamily:        {types.BoolFamily},
	types.BytesFamily:       {types.BytesFamily},
	types.DecimalFamily:     append(numericCanonicalTypeFamilies, types.IntervalFamily),
	types.IntFamily:         append(numericCanonicalTypeFamilies, types.IntervalFamily),
	types.FloatFamily:       append(numericCanonicalTypeFamilies, types.IntervalFamily),
	types.TimestampTZFamily: timeCanonicalTypeFamilies,
	types.IntervalFamily:    append(numericCanonicalTypeFamilies, timeCanonicalTypeFamilies...),
}

var comparableCanonicalTypeFamilies = map[types.Family][]types.Family{
	types.BoolFamily:        {types.BoolFamily},
	types.BytesFamily:       {types.BytesFamily},
	types.DecimalFamily:     numericCanonicalTypeFamilies,
	types.IntFamily:         numericCanonicalTypeFamilies,
	types.FloatFamily:       numericCanonicalTypeFamilies,
	types.TimestampTZFamily: {types.TimestampTZFamily},
	types.IntervalFamily:    {types.IntervalFamily},
}

var castableCanonicalTypeFamilies = make(map[types.Family][]types.Family)

// anyWidth is special "value" of width of a type that will be used to generate
// "case -1: default:" block that would match all widths that are not
// explicitly specified.
const anyWidth = -1

var typeWidthReplacement = fmt.Sprintf("{{.Width}}{{if eq .Width %d}}: default{{end}}", anyWidth)

// supportedWidthsByCanonicalTypeFamily is a mapping from a canonical type
// family to all widths that are supported by that family. Make sure that
// anyWidth value is the last one in every slice.
var supportedWidthsByCanonicalTypeFamily = map[types.Family][]int32{
	types.BoolFamily:        {anyWidth},
	types.BytesFamily:       {anyWidth},
	types.DecimalFamily:     {anyWidth},
	types.IntFamily:         {16, 32, anyWidth},
	types.FloatFamily:       {anyWidth},
	types.TimestampTZFamily: {anyWidth},
	types.IntervalFamily:    {anyWidth},
}

var numericCanonicalTypeFamilies = []types.Family{types.IntFamily, types.FloatFamily, types.DecimalFamily}

var timeCanonicalTypeFamilies = []types.Family{types.TimestampTZFamily, types.IntervalFamily}

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
			colexecerror.InternalError(fmt.Sprintf("unexpected width of int type family: %d", width))
		}
	case types.FloatFamily:
		return "Float64"
	case types.TimestampTZFamily:
		return "Timestamp"
	case types.IntervalFamily:
		return "Interval"
	default:
		colexecerror.InternalError(fmt.Sprintf("unsupported canonical type family %s", canonicalTypeFamily))
	}
	// This code is unreachable, but the compiler cannot infer that.
	return ""
}

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
			colexecerror.InternalError(fmt.Sprintf("unexpected width of int type family: %d", width))
		}
	case types.FloatFamily:
		return "float64"
	case types.TimestampTZFamily:
		return "time.Time"
	case types.IntervalFamily:
		return "duration.Duration"
	default:
		colexecerror.InternalError(fmt.Sprintf("unsupported canonical type family %s", canonicalTypeFamily))
	}
	// This code is unreachable, but the compiler cannot infer that.
	return ""
}
