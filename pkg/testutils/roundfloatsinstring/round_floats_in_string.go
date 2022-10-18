// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roundfloatsinstring

import (
	"fmt"
	"regexp"
	"strconv"
)

// RoundFloatsInString rounds floats in a given string to the given number of significant figures.
func RoundFloatsInString(s string, significantFigures int) string {
	return string(regexp.MustCompile(`(\d+\.\d+)`).ReplaceAllFunc([]byte(s), func(x []byte) []byte {
		f, err := strconv.ParseFloat(string(x), 64)
		if err != nil {
			return []byte(err.Error())
		}
		formatSpecifier := "%." + fmt.Sprintf("%dg", significantFigures)
		return []byte(fmt.Sprintf(formatSpecifier, f))
	}))
}
