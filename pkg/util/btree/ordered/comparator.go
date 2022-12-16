// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ordered

import (
	"bytes"

	"golang.org/x/exp/constraints"
)

// Comparator is used to compare values.
type Comparator[K any] interface {
	Compare(a, b K) int
}

// ValueComparator is a Comparator for ordered values.
type ValueComparator[K constraints.Ordered] struct{}

// Compare implements Comparator.
func (c ValueComparator[K]) Compare(a, b K) int { return Compare(a, b) }

// KeyComparator is a Comparator for Key values.
type KeyComparator[K Key[K]] struct{}

// Compare implements Comparator.
func (c KeyComparator[K]) Compare(a, b K) int { return a.Compare(b) }

// BytesComparator is a Comparator for Bytes values.
type BytesComparator[K Bytes] struct{}

// Compare implements Comparator.
func (c BytesComparator[K]) Compare(a, b K) int { return bytes.Compare(a, b) }
