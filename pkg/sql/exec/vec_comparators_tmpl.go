// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for sort.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "tree" package.
var _ tree.Datum

// _COMPARE is the template equality function for assigning the first input
// to the result of comparing second and third inputs.
func _COMPARE(_, _, _ string) bool {
	panic("")
}

// */}}

// {{range .}}
type _TYPEVecComparator struct {
	vecs  [][]_GOTYPE
	nulls []*coldata.Nulls
}

func (c *_TYPEVecComparator) compare(vecIdx1, vecIdx2 int, valIdx1, valIdx2 uint16) int {
	n1 := c.nulls[vecIdx1].HasNulls() && c.nulls[vecIdx1].NullAt(valIdx1)
	n2 := c.nulls[vecIdx2].HasNulls() && c.nulls[vecIdx2].NullAt(valIdx2)
	if n1 && n2 {
		return 0
	} else if n1 {
		return -1
	} else if n2 {
		return 1
	}
	left := c.vecs[vecIdx1][valIdx1]
	right := c.vecs[vecIdx2][valIdx2]
	var cmp int
	_COMPARE("cmp", "left", "right")
	return cmp
}

func (c *_TYPEVecComparator) setVec(idx int, vec coldata.Vec) {
	c.vecs[idx] = vec._TYPE()
	c.nulls[idx] = vec.Nulls()
}

// {{end}}

func GetVecComparator(t types.T, numVecs int) vecComparator {
	switch t {
	// {{range .}}
	case types._TYPE:
		return &_TYPEVecComparator{
			vecs:  make([][]_GOTYPE, numVecs),
			nulls: make([]*coldata.Nulls, numVecs),
		}
		// {{end}}
	}
	panic(fmt.Sprintf("unhandled type %v", t))
}
