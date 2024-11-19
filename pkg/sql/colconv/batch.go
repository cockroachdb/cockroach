// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package colconv exposes utilities for working with vectorized columns.
package colconv

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
)

func init() {
	coldata.VecsToStringWithRowPrefix = vecsToStringWithRowPrefix
}

// vecsToStringWithRowPrefix returns a pretty representation of the vectors with
// each row being in a separate string.
func vecsToStringWithRowPrefix(vecs []*coldata.Vec, length int, sel []int, prefix string) []string {
	var builder strings.Builder
	converter := NewAllVecToDatumConverter(len(vecs))
	defer converter.Release()
	converter.ConvertVecs(vecs, length, sel)
	result := make([]string, length)
	strs := make([]string, len(vecs))
	for i := 0; i < length; i++ {
		builder.Reset()
		rowIdx := i
		if sel != nil {
			rowIdx = sel[i]
		}
		builder.WriteString(prefix + "[")
		for colIdx := 0; colIdx < len(vecs); colIdx++ {
			strs[colIdx] = converter.GetDatumColumn(colIdx)[rowIdx].String()
		}
		builder.WriteString(strings.Join(strs, " "))
		builder.WriteString("]")
		result[i] = builder.String()
	}
	return result
}
