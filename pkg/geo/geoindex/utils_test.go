// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geoindex

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/datadriven"
	"github.com/golang/geo/s2"
)

func nameArg(t *testing.T, d *datadriven.TestData) string {
	var name string
	d.ScanArgs(t, "name", &name)
	return name
}

func s2Config(t *testing.T, d *datadriven.TestData) S2Config {
	var minLevel, maxLevel, maxCells int
	d.ScanArgs(t, "minlevel", &minLevel)
	d.ScanArgs(t, "maxlevel", &maxLevel)
	d.ScanArgs(t, "maxcells", &maxCells)
	return S2Config{
		MinLevel: int32(minLevel),
		MaxLevel: int32(maxLevel),
		LevelMod: 1,
		MaxCells: int32(maxCells),
	}
}

func keysToString(keys []Key, bbox geopb.BoundingBox, err error) string {
	if err != nil {
		return err.Error()
	}
	if len(keys) == 0 {
		return ""
	}
	var cells []string
	for _, k := range keys {
		cells = append(cells, k.String())
	}
	return fmt.Sprintf("%s\nBoundingBox: %s", strings.Join(cells, ", "), bbox.String())
}

func cellUnionToString(cells s2.CellUnion) string {
	var strs []string
	for _, c := range cells {
		strs = append(strs, Key(c).String())
	}
	return strings.Join(strs, ", ")
}

func spansToString(spans UnionKeySpans, err error) string {
	if err != nil {
		return err.Error()
	}
	return spans.toString(60)
}

// Intersection of unioned keys.
type evaluatedExpr [][]Key

type unionSorter []Key

func (s unionSorter) Len() int {
	return len(s)
}

func (s unionSorter) Less(i, j int) bool {
	return s2.CellID(s[i]).Level() > s2.CellID(s[j]).Level()
}

func (s unionSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Checks that the factored expression does not repeat a cell
// and prints out the unfactored expression with each unioned
// sub-expression in high-to-low level order, so that it is
// easy to read and validate.
func checkExprAndToString(expr RPKeyExpr, err error) string {
	if err != nil {
		return err.Error()
	}
	if len(expr) == 0 {
		return ""
	}
	keys := make(map[Key]struct{})
	for _, elem := range expr {
		switch k := elem.(type) {
		case Key:
			if _, ok := keys[k]; ok {
				return fmt.Sprintf("duplicate key: %s", k)
			}
			keys[k] = struct{}{}
		}
	}
	var stack []evaluatedExpr
	for _, elem := range expr {
		switch e := elem.(type) {
		case Key:
			stack = append(stack, evaluatedExpr{[]Key{e}})
		case RPSetOperator:
			op0, op1 := stack[len(stack)-1], stack[len(stack)-2]
			stack = stack[:len(stack)-2]
			switch e {
			case RPSetIntersection:
				op0 = append(op0, op1...)
			case RPSetUnion:
				if len(op1) != 1 {
					op0, op1 = op1, op0
				}
				if len(op1) != 1 {
					return "error in expression"
				}
				for i := range op0 {
					op0[i] = append(op0[i], op1[0]...)
				}
			}
			stack = append(stack, op0)
		}
	}
	if len(stack) != 1 {
		return fmt.Sprintf("stack has unexpected length: %d", len(stack))
	}
	b := newStringBuilderWithWrap(&strings.Builder{}, 60)
	for i, cells := range stack[0] {
		sort.Sort(unionSorter(cells))
		fmt.Fprintf(b, "%d: ", i)
		for i, c := range cells {
			fmt.Fprintf(b, "%s", c)
			if i != len(cells)-1 {
				b.WriteString(", ")
			}
			b.tryWrap()
		}
		b.doWrap()
	}

	return b.String()
}
