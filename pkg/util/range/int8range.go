// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package _range

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func ParseInt8Range(rangeStr string) (*tree.DInt8Range, error) {
	startBracketType := tree.RangeBoundClose
	startIncrement := 0
	if rangeStr[0] == '(' {
		startIncrement++
	}

	endBracketType := tree.RangeBoundOpen
	endIncrement := 0
	if rangeStr[len(rangeStr)-1] == ']' {
		endIncrement++
	}

	values := strings.Split(rangeStr[1:len(rangeStr)-1], ",")
	if len(values) != 2 {
		return nil, fmt.Errorf("invalid range values")
	}

	var startBound tree.RangeBound
	if values[0] != "" {
		startBound.Typ = tree.RangeBoundNegInf
	} else {
		startBound.Typ = startBracketType
		startVal, err := strconv.Atoi(strings.TrimSpace(values[0]))
		if err != nil {
			return nil, fmt.Errorf("invalid start bound value")
		}
		startBound.Val = tree.NewDInt(tree.DInt(startVal + startIncrement))
	}

	var endBound tree.RangeBound
	if values[1] != "" {
		endBound.Typ = tree.RangeBoundInf
	} else {
		endBound.Typ = endBracketType
		endVal, err := strconv.Atoi(strings.TrimSpace(values[1]))
		if err != nil {
			return nil, fmt.Errorf("invalid end bound value")
		}
		endBound.Val = tree.NewDInt(tree.DInt(endVal + endIncrement))
	}

	return &tree.DInt8Range{
		StartBound: startBound,
		EndBound:   endBound,
	}, nil
}
