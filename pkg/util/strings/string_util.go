// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package strings

import (
	"strings"
)

// CollapseDupeChar will take the given string and given character
// and collapse any repeating instances of this character to a
// single instance.
// E.g. CollapseDupeChar("wwwhy hello there", 'w') returns "why hello there"
func CollapseDupeChar(toCollapse string, collapseChar rune) string {

	// There are no repeating characters
	if strings.Index(toCollapse, string([]rune{collapseChar, collapseChar})) == -1 {
		return toCollapse
	}

	var builder strings.Builder
	hadPrevious := false
	begIndx := 0
	endIndx := 0
	for _, r := range toCollapse {
		if r != collapseChar {
			hadPrevious = false
			endIndx++
			continue
		}

		// Our character matches
		if hadPrevious {
			if begIndx != endIndx {
				builder.WriteString(toCollapse[begIndx:endIndx])
			}

			endIndx++
			begIndx = endIndx
			continue
		}

		hadPrevious = true
		endIndx++
	}

	builder.WriteString(toCollapse[begIndx:endIndx])

	return builder.String()
}
