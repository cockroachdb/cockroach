// Copyright 2020 The Cockroach Authors.
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
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

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
	switch base.kind {
	case binaryOverload:
		combinableCanonicalTypeFamilies = compatibleCanonicalTypeFamilies
	case comparisonOverload:
		combinableCanonicalTypeFamilies = comparableCanonicalTypeFamilies
	case castOverload:
		combinableCanonicalTypeFamilies = castableCanonicalTypeFamilies
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly overload is neither binary, comparison, nor cast"))
	}
	for _, leftFamily := range supportedCanonicalTypeFamilies {
		leftWidths, found := supportedWidthsByCanonicalTypeFamily[leftFamily]
		if !found {
			colexecerror.InternalError(errors.AssertionFailedf("didn't find supported widths for %s", leftFamily))
		}
		leftFamilyStr := toString(leftFamily)
		for _, rightFamily := range combinableCanonicalTypeFamilies[leftFamily] {
			rightWidths, found := supportedWidthsByCanonicalTypeFamily[rightFamily]
			if !found {
				colexecerror.InternalError(errors.AssertionFailedf("didn't find supported widths for %s", rightFamily))
			}
			rightFamilyStr := toString(rightFamily)
			for _, leftWidth := range leftWidths {
				for _, rightWidth := range rightWidths {
					customizer, ok := customizers[typePair{leftFamily, leftWidth, rightFamily, rightWidth}]
					if !ok {
						colexecerror.InternalError(errors.AssertionFailedf(
							"unexpectedly didn't find a type customizer for %s %d %s %d", leftFamily, leftWidth, rightFamily, rightWidth))
					}
					// Skip overloads that don't have associated output types.
					retType, ok := opOutputTypes[typePair{leftFamily, leftWidth, rightFamily, rightWidth}]
					if !ok {
						continue
					}
					var info *twoArgsResolvedOverloadInfo
					switch base.kind {
					case binaryOverload:
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
					case comparisonOverload:
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
					case castOverload:
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
					if base.kind == binaryOverload || base.kind == comparisonOverload {
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
